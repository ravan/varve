#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns
use std::path::Path;
use varve::{Config, Db};

fn local_config(dir: &Path) -> Config {
    let dir_toml = toml_escaped(dir);
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n[log.local]\ndir = {dir_toml}\n"
    ))
    .unwrap()
}

// tempdir paths are tame, but escape properly anyway.
fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string()) // Rust debug-quotes ⊇ TOML basic strings for these paths
}

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn acked_transactions_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let (ada, bob);
    {
        let db = Db::open(local_config(dir.path())).await.unwrap();
        ada = db
            .execute("INSERT (:Person {_id: 1, name: 'Ada'})")
            .await
            .unwrap();
        bob = db
            .execute("INSERT (:Person {_id: 2, name: 'Bob'})")
            .await
            .unwrap();
    } // drop closes the writer; every acked tx is already durable

    let db = Db::open(local_config(dir.path())).await.unwrap();
    let batches = db.query("MATCH (p:Person) RETURN p.name").await.unwrap();
    assert_eq!(rows(&batches), 2);

    // tx ids and system times continue past the replayed history.
    let cyd = db
        .execute("INSERT (:Person {_id: 3, name: 'Cyd'})")
        .await
        .unwrap();
    assert_eq!(cyd.tx_id, 3);
    assert!(cyd.system_time > ada.system_time);
    assert!(cyd.system_time > bob.system_time);
    assert_eq!(
        rows(&db.query("MATCH (p:Person) RETURN p.name").await.unwrap()),
        3
    );
}

#[tokio::test]
async fn bitemporal_history_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let before_delete;
    {
        let db = Db::open(local_config(dir.path())).await.unwrap();
        db.execute("INSERT (:P {_id: 1, name: 'Zoe'})")
            .await
            .unwrap();
        before_delete = db
            .execute("INSERT (:P {_id: 2, name: 'Amy'})")
            .await
            .unwrap();
        db.execute("MATCH (p:P) WHERE p.name = 'Zoe' DELETE p")
            .await
            .unwrap();
        // valid-time axis too
        db.execute("INSERT (:Q {_id: 9, name: 'Eve'}) VALID FROM DATE '2020-06-01'")
            .await
            .unwrap();
    }

    let db = Db::open(local_config(dir.path())).await.unwrap();
    // Delete replayed: only Amy now…
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p.name").await.unwrap()),
        1
    );
    // …but time travel to before the delete still sees both.
    let time_travel = format!(
        "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:P) RETURN p.name",
        before_delete.system_time
    );
    assert_eq!(rows(&db.query(&time_travel).await.unwrap()), 2);
    // Valid-time bounds replayed intact.
    assert_eq!(
        rows(
            &db.query("FOR VALID_TIME AS OF DATE '2019-01-01' MATCH (q:Q) RETURN q.name")
                .await
                .unwrap()
        ),
        0
    );
    assert_eq!(
        rows(
            &db.query("FOR VALID_TIME AS OF DATE '2021-01-01' MATCH (q:Q) RETURN q.name")
                .await
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn generated_ids_do_not_collide_across_restarts() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(local_config(dir.path())).await.unwrap();
        db.execute("INSERT (:G {v: 1})").await.unwrap(); // no _id → generated
    }
    let db = Db::open(local_config(dir.path())).await.unwrap();
    db.execute("INSERT (:G {v: 2})").await.unwrap(); // must NOT reuse the id
    assert_eq!(rows(&db.query("MATCH (g:G) RETURN g.v").await.unwrap()), 2);
}

#[tokio::test]
async fn replay_recovers_max_tx_id_across_a_burned_id_gap() {
    // The writer bumps `next_tx_id` and pulls a clock tick BEFORE resolving a
    // statement (writer.rs::resolve), so a statement that fails to resolve
    // still burns a tx_id that never reaches the log. Replay must recover
    // `next_tx_id` as a MAX over the log, not a count of records, or a
    // reopened DB will reissue an already-used tx_id and collide two
    // generated ids (`varve:gen:{tx_id}:{ordinal}`) into the same iid.
    let dir = tempfile::tempdir().unwrap();
    let burned_tx_first_id;
    {
        let db = Db::open(local_config(dir.path())).await.unwrap();
        // _id: 2.5 is a Float — Value::id_bytes() rejects Float/Null, so this
        // fails inside resolve_insert AFTER next_tx_id was incremented to 1.
        // tx_id 1 is burned: it never reaches the log.
        let bad = db.execute("INSERT (:X {_id: 2.5})").await;
        assert!(
            matches!(bad, Err(varve::EngineError::Type(_))),
            "expected a Type error from the Float _id, got {bad:?}"
        );

        // The next successful mutation gets tx_id 2 — the log now holds ONE
        // record whose tx_id is 2, so count(records) = 1 != max(tx_id) = 2.
        // This gap is exactly what makes the test non-vacuous.
        burned_tx_first_id = db.execute("INSERT (:G {v: 1})").await.unwrap();
        assert_eq!(burned_tx_first_id.tx_id, 2);
    } // drop closes the writer; the one durable record is tx_id 2.

    // Reopen: with correct `max()` recovery, next_tx_id floors at 2, so this
    // insert gets tx_id 3 and a distinct generated id
    // (`varve:gen:3:0` vs. the first insert's `varve:gen:2:0`). A regression
    // to `records.len()` recovery would floor at 1 (one record replayed),
    // reissue tx_id 2, and re-derive `varve:gen:2:0` — colliding the two
    // generated-id nodes into a single iid and losing a row.
    let db = Db::open(local_config(dir.path())).await.unwrap();
    db.execute("INSERT (:G {v: 2})").await.unwrap();
    assert_eq!(rows(&db.query("MATCH (g:G) RETURN g.v").await.unwrap()), 2);
}

#[tokio::test]
async fn db_local_convenience_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::local(dir.path()).await.unwrap();
        db.execute("INSERT (:L {_id: 1})").await.unwrap();
    }
    let db = Db::local(dir.path()).await.unwrap();
    assert_eq!(
        rows(&db.query("MATCH (l:L) RETURN l._id").await.unwrap()),
        1
    );
}

#[tokio::test]
async fn memory_backend_via_config_and_default() {
    // Explicit memory backend…
    let db = Db::open(Config::from_toml_str("[log]\nbackend = \"memory\"").unwrap())
        .await
        .unwrap();
    db.execute("INSERT (:M {_id: 1})").await.unwrap();
    // …and no [log] section at all defaults to memory.
    let db = Db::open(Config::from_toml_str("").unwrap()).await.unwrap();
    db.execute("INSERT (:M {_id: 1})").await.unwrap();
}

#[tokio::test]
async fn unknown_backend_error_lists_available() {
    let err = Db::open(Config::from_toml_str("[log]\nbackend = \"kafka\"").unwrap())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("kafka"), "{err}");
    assert!(err.contains("local"), "{err}");
    assert!(err.contains("memory"), "{err}");
}

#[tokio::test]
async fn open_from_config_file() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = dir.path().join("log");
    let config_path = dir.path().join("varve.toml");
    std::fs::write(
        &config_path,
        format!(
            "[log]\nbackend = \"local\"\n[log.local]\ndir = {}\n",
            toml_escaped(&log_dir)
        ),
    )
    .unwrap();

    let db = Db::open(Config::from_file(&config_path).unwrap())
        .await
        .unwrap();
    db.execute("INSERT (:F {_id: 1})").await.unwrap();
    drop(db);
    let db = Db::open(Config::from_file(&config_path).unwrap())
        .await
        .unwrap();
    assert_eq!(
        rows(&db.query("MATCH (f:F) RETURN f._id").await.unwrap()),
        1
    );
}
