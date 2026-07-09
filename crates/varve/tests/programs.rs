#![allow(clippy::unwrap_used)]

use arrow::array::{Array, StringArray};
use std::path::Path;
use varve::{Config, Db, EngineError};

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn strings(batches: &[varve::RecordBatch], col: &str) -> Vec<String> {
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            (0..col.len()).map(|idx| col.value(idx).to_string())
        })
        .collect();
    out.sort();
    out
}

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

fn local_config(dir: &Path) -> Config {
    let log_dir = toml_escaped(&dir.join("log"));
    let store_dir = toml_escaped(&dir.join("store"));
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\n\
         [storage.local]\ndir = {store_dir}\n"
    ))
    .unwrap()
}

#[tokio::test]
async fn program_is_one_tx_one_log_record() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Db::open(local_config(dir.path())).await.unwrap();
        let receipt = db
            .execute(
                "INSERT (:P {_id: 1}); \
                 INSERT (:P {_id: 2}); \
                 INSERT (:P {_id: 3});",
            )
            .await
            .unwrap();
        assert_eq!(receipt.tx_id, 1);
    }

    let db = Db::open(local_config(dir.path())).await.unwrap();
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p._id").await.unwrap()),
        3
    );

    let next = db.execute("INSERT (:P {_id: 4})").await.unwrap();
    assert_eq!(next.tx_id, 2);
}

#[tokio::test]
async fn later_statement_sees_earlier_effects() {
    let db = Db::memory();

    db.execute(
        "INSERT (:P {_id: 1, name: 'Ada'}); \
         MATCH (a:P {_id: 1}) INSERT (a)-[:KNOWS]->(:P {_id: 2, name: 'Bob'}); \
         MATCH (b:P {_id: 2}) SET b.name = 'Robert';",
    )
    .await
    .unwrap();

    assert_eq!(
        rows(
            &db.query("MATCH (a:P)-[k:KNOWS]->(b:P) RETURN k._iid")
                .await
                .unwrap()
        ),
        1
    );
    let names = db
        .query("MATCH (b:P {_id: 2}) RETURN b.name AS name")
        .await
        .unwrap();
    assert_eq!(strings(&names, "name"), vec!["Robert"]);
}

#[tokio::test]
async fn failed_statement_rolls_back_whole_program() {
    let db = Db::memory();

    let err = db
        .execute("INSERT (:P {_id: 1}); INSERT (:P {_id: 2.5});")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Type(_)), "{err:?}");

    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p._id").await.unwrap()),
        0
    );

    let receipt = db.execute("INSERT (:P {_id: 2})").await.unwrap();
    assert_eq!(receipt.tx_id, 2);
}

#[tokio::test]
async fn detach_delete_after_insert_in_same_program() {
    let db = Db::memory();

    db.execute(
        "INSERT (:P {_id: 1})-[:KNOWS]->(:P {_id: 2}); \
         MATCH (p:P {_id: 1}) DETACH DELETE p;",
    )
    .await
    .unwrap();

    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p._id").await.unwrap()),
        1
    );
    assert_eq!(
        rows(
            &db.query("MATCH (a:P)-[k:KNOWS]->(b:P) RETURN k._iid")
                .await
                .unwrap()
        ),
        0
    );
}

#[tokio::test]
async fn hundred_inserts_one_program_fewer_log_records_than_hundred_txs() {
    let db = Db::memory();
    let program = (0..100)
        .map(|i| format!("INSERT (:P {{_id: {i}}})"))
        .collect::<Vec<_>>()
        .join("; ");

    let receipt = db.execute(&program).await.unwrap();
    assert_eq!(receipt.tx_id, 1);
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p._id").await.unwrap()),
        100
    );

    let next = db.execute("INSERT (:P {_id: 100})").await.unwrap();
    assert_eq!(next.tx_id, 2);
}

#[tokio::test]
async fn generated_node_ids_are_unique_across_program_statements() {
    let db = Db::memory();

    db.execute("INSERT (:P); INSERT (:P);").await.unwrap();

    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p._iid").await.unwrap()),
        2
    );
}

#[tokio::test]
async fn generated_edge_ids_are_unique_across_program_statements() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1}), (:P {_id: 2})")
        .await
        .unwrap();

    db.execute(
        "MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:K]->(b); \
         MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:K]->(b);",
    )
    .await
    .unwrap();

    assert_eq!(
        rows(
            &db.query("MATCH (a:P)-[k:K]->(b:P) RETURN k._iid")
                .await
                .unwrap()
        ),
        2
    );
}

#[tokio::test]
async fn execute_rejects_query_inside_program() {
    let db = Db::memory();

    let err = db
        .execute("INSERT (:P {_id: 1}); MATCH (p:P) RETURN p._id;")
        .await
        .unwrap_err();

    assert!(matches!(err, EngineError::NotAMutation), "{err:?}");
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p._id").await.unwrap()),
        0
    );
}

#[tokio::test]
async fn query_rejects_multi_statement_program() {
    let db = Db::memory();

    let err = db
        .query("MATCH (p:P) RETURN p._id; MATCH (p:P) RETURN p._id;")
        .await
        .unwrap_err();

    assert!(matches!(err, EngineError::NotAQuery), "{err:?}");
}
