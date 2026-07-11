use futures::TryStreamExt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use varve_config::{BuildContext, ComponentFactory, ConfigSection, RegistryError};
use varve_engine::{BasisToken, Db, EngineError, NodeRole, Registries};
use varve_log::{Log, LogError, LogRecord};
use varve_types::LogPosition;

fn config(root: &TempDir, roles: &[&str], poll_ms: u64, batch: usize) -> varve_config::Config {
    let roles = roles
        .iter()
        .map(|role| format!("\"{role}\""))
        .collect::<Vec<_>>()
        .join(", ");
    varve_config::Config::from_toml_str(&format!(
        "[node]\nroles = [{roles}]\ntail_poll_interval_ms = {poll_ms}\n\
         tail_batch_records = {batch}\nbasis_timeout_ms = 1000\n\
         [log]\nbackend = \"local\"\ngroup_commit_window_ms = 0\n\
         [log.local]\ndir = {:?}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = 100000\n\
         [storage.local]\ndir = {:?}\n",
        root.path().join("log").display().to_string(),
        root.path().join("store").display().to_string(),
    ))
    .unwrap_or_else(|error| panic!("test config must parse: {error}"))
}

#[tokio::test]
async fn query_node_applies_resolved_effects_and_has_no_writer() {
    let root = TempDir::new().unwrap();
    let writer = Db::open(config(&root, &["writer", "query", "compactor"], 5, 2))
        .await
        .unwrap();
    let query = Db::open(config(&root, &["query"], 5, 2)).await.unwrap();

    let receipt = writer
        .execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();
    for _ in 0..200 {
        if query.status().await.unwrap().applied.tx_id >= receipt.tx_id {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(query.status().await.unwrap().applied.tx_id >= receipt.tx_id);

    let batches = query
        .query("MATCH (p:Person) RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        1
    );
    assert!(matches!(
        query.execute("INSERT (:X {_id: 2})").await,
        Err(EngineError::RoleDisabled(NodeRole::Writer))
    ));
    assert!(query.status().await.unwrap().follower_error.is_none());
}

#[tokio::test]
async fn node_roles_reject_invalid_combinations() {
    let root = TempDir::new().unwrap();
    assert!(matches!(
        Db::open(config(&root, &[], 5, 1)).await,
        Err(EngineError::InvalidNodeConfig(_))
    ));
    assert!(matches!(
        Db::open(config(&root, &["query"], 5, 0)).await,
        Err(EngineError::InvalidNodeConfig(_))
    ));
    assert!(matches!(
        Db::open(config(&root, &["query", "compactor"], 5, 1)).await,
        Err(EngineError::InvalidNodeConfig(_))
    ));
}

#[tokio::test]
async fn basis_wait_times_out_then_succeeds_after_writer_commit() {
    let root = TempDir::new().unwrap();
    let writer = Db::open(config(&root, &["writer", "query", "compactor"], 200, 16))
        .await
        .unwrap();
    let query = Db::open(config(&root, &["query"], 200, 16)).await.unwrap();

    assert!(matches!(
        query
            .wait_for_basis(BasisToken::TxId(1), Duration::from_millis(10))
            .await,
        Err(EngineError::BasisTimeout { .. })
    ));

    let receipt = writer.execute("INSERT (:X {_id: 1})").await.unwrap();
    let rows = query
        .query("MATCH (x:X) RETURN x._id AS id")
        .basis(receipt)
        .basis_timeout(Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|batch| batch.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn basis_at_accepts_the_already_applied_log_position() {
    let db = Db::memory();
    db.execute("INSERT (:X {_id: 1})").await.unwrap();
    let applied = db.status().await.unwrap().applied;

    db.wait_for_basis(
        BasisToken::At(applied.log_position),
        Duration::from_millis(1),
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn public_query_stream_yields_rows() {
    let db = Db::memory();
    db.execute("INSERT (:X {_id: 1})").await.unwrap();

    let stream = db
        .query("MATCH (x:X) RETURN x._id AS id")
        .stream()
        .await
        .unwrap();
    let rows = stream
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .iter()
        .map(|batch| batch.num_rows())
        .sum::<usize>();

    assert_eq!(rows, 1);
}

#[tokio::test]
async fn query_role_gate_precedes_basis_wait_and_parsing() {
    let root = TempDir::new().unwrap();
    let writer = Db::open(config(&root, &["writer"], 5, 1)).await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        writer
            .query("this is not valid GQL")
            .basis(BasisToken::TxId(u64::MAX))
            .basis_timeout(Duration::from_secs(5))
            .stream(),
    )
    .await
    .expect("role gate must run before the long basis wait");

    assert!(matches!(
        result,
        Err(EngineError::RoleDisabled(NodeRole::Query))
    ));
}

struct CorruptAfterRecoveryLog {
    reads: AtomicUsize,
}

#[async_trait::async_trait]
impl Log for CorruptAfterRecoveryLog {
    async fn append(&self, _records: Vec<LogRecord>) -> Result<LogPosition, LogError> {
        unreachable!("query-only test node never appends")
    }

    async fn read_range(
        &self,
        _from: LogPosition,
        _to: LogPosition,
    ) -> Result<Vec<(LogPosition, LogRecord)>, LogError> {
        if self.reads.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(Vec::new())
        } else {
            Err(LogError::Corrupt {
                path: "test-log".into(),
                offset: 0,
                reason: "terminal follower failure".into(),
            })
        }
    }

    async fn trim(&self, _up_to: LogPosition) -> Result<(), LogError> {
        unreachable!("query-only test node never trims")
    }
}

struct CorruptAfterRecoveryFactory;

impl ComponentFactory<dyn Log> for CorruptAfterRecoveryFactory {
    fn name(&self) -> &'static str {
        "corrupt-after-recovery"
    }

    fn build(
        &self,
        _cfg: &ConfigSection,
        _ctx: &BuildContext,
    ) -> Result<Arc<dyn Log>, RegistryError> {
        Ok(Arc::new(CorruptAfterRecoveryLog {
            reads: AtomicUsize::new(0),
        }))
    }
}

#[tokio::test]
async fn basis_wait_fails_immediately_on_terminal_follower_error() {
    let mut registries = Registries::with_builtins();
    registries
        .log
        .register(Box::new(CorruptAfterRecoveryFactory))
        .unwrap();
    let config = varve_config::Config::from_toml_str(
        "[node]\nroles = [\"query\"]\ntail_poll_interval_ms = 1\n\
         tail_batch_records = 1\nbasis_timeout_ms = 5000\n\
         [log]\nbackend = \"corrupt-after-recovery\"\n\
         [storage]\nbackend = \"memory\"\n",
    )
    .unwrap();
    let query = Db::open_with(&config, &registries).await.unwrap();

    let result = tokio::time::timeout(
        Duration::from_millis(250),
        query.wait_for_basis(BasisToken::TxId(1), Duration::from_secs(5)),
    )
    .await
    .expect("terminal follower failure must beat the basis timeout");

    assert!(matches!(result, Err(EngineError::FollowerFailed(_))));
}
