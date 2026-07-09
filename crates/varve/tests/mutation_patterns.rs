#![allow(clippy::unwrap_used)]

use datafusion::arrow::array::{Array, StringArray};
use std::path::Path;
use varve::{Config, Db, EngineError};

fn row_count(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

fn blocks_config(dir: &Path, max_block_rows: usize) -> Config {
    let log_dir = toml_escaped(&dir.join("log"));
    let store_dir = toml_escaped(&dir.join("store"));
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {max_block_rows}\n\
         [storage.local]\ndir = {store_dir}\n"
    ))
    .unwrap()
}

async fn wait_for_flush_count(dir: &Path, want: usize) {
    let blocks = dir.join("store").join("v1").join("blocks");
    for _ in 0..200 {
        let count = blocks
            .read_dir()
            .map(|entries| {
                entries
                    .filter_map(Result::ok)
                    .filter(|entry| entry.file_name().to_string_lossy().ends_with(".manifest"))
                    .count()
            })
            .unwrap_or(0);
        if count >= want {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("fewer than {want} manifest(s) under {blocks:?} within 5s");
}

fn string_pairs(batches: &[varve::RecordBatch], col_a: &str, col_b: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for batch in batches {
        let ia = batch.schema().column_with_name(col_a).unwrap().0;
        let ib = batch.schema().column_with_name(col_b).unwrap().0;
        let arr_a = batch
            .column(ia)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let arr_b = batch
            .column(ib)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..batch.num_rows() {
            out.push((arr_a.value(row).to_string(), arr_b.value(row).to_string()));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn match_insert_with_hop_binds_endpoints() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'Ada'})-[:KNOWS]->(:P {_id: 2, name: 'Bob'})")
        .await
        .unwrap();

    db.execute("MATCH (a:P)-[:KNOWS]->(b:P) INSERT (a)-[:MET]->(b)")
        .await
        .unwrap();

    let rows = db
        .query("MATCH (a:P)-[:MET]->(b:P) RETURN a.name AS a, b.name AS b")
        .await
        .unwrap();
    assert_eq!(
        string_pairs(&rows, "a", "b"),
        vec![("Ada".to_string(), "Bob".to_string())]
    );
}

#[tokio::test]
async fn match_insert_connected_pattern_not_cartesian() {
    let db = Db::memory();
    db.execute(
        "INSERT (:P {_id: 1, name: 'Ada'}),
                (:P {_id: 2, name: 'Bob'}),
                (:P {_id: 3, name: 'Cy'})",
    )
    .await
    .unwrap();
    db.execute("MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:KNOWS]->(b)")
        .await
        .unwrap();
    db.execute("MATCH (a:P {_id: 2}), (b:P {_id: 3}) INSERT (a)-[:KNOWS]->(b)")
        .await
        .unwrap();

    db.execute("MATCH (a:P)-[:KNOWS]->(b:P) INSERT (a)-[:MET]->(b)")
        .await
        .unwrap();

    let rows = db
        .query("MATCH (a:P)-[:MET]->(b:P) RETURN a.name AS a, b.name AS b")
        .await
        .unwrap();
    assert_eq!(
        string_pairs(&rows, "a", "b"),
        vec![
            ("Ada".to_string(), "Bob".to_string()),
            ("Bob".to_string(), "Cy".to_string()),
        ]
    );
}

#[tokio::test]
async fn delete_with_hop_pattern() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'Ada'})-[:KNOWS]->(:P {_id: 2, name: 'Bob'})")
        .await
        .unwrap();

    db.execute("MATCH (a:P)-[r:KNOWS]->(b:P) DELETE r")
        .await
        .unwrap();

    let edge_rows = db
        .query("MATCH (a:P)-[:KNOWS]->(b:P) RETURN a.name AS a, b.name AS b")
        .await
        .unwrap();
    assert_eq!(row_count(&edge_rows), 0);

    let node_rows = db.query("MATCH (p:P) RETURN p.name").await.unwrap();
    assert_eq!(row_count(&node_rows), 2);
}

#[tokio::test]
async fn match_part_quantified_hop_rejected() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1})-[:K]->(:P {_id: 2})")
        .await
        .unwrap();

    let quantified = db
        .execute("MATCH (a:P)-[:K]->{1,2}(b:P) INSERT (a)-[:MET]->(b)")
        .await
        .unwrap_err();
    assert!(
        matches!(quantified, EngineError::Unsupported(ref msg) if msg.contains("quantified")),
        "{quantified:?}"
    );

    let path_var = db
        .execute("MATCH p = (a:P)-[:K]->(b:P) INSERT (a)-[:MET]->(b)")
        .await
        .unwrap_err();
    assert!(
        matches!(path_var, EngineError::Unsupported(ref msg) if msg.contains("path variable")),
        "{path_var:?}"
    );
}

#[tokio::test]
async fn match_part_path_variable_target_rejected_by_engine() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'Ada'})-[:KNOWS]->(:P {_id: 2, name: 'Bob'})")
        .await
        .unwrap();

    let err = db
        .execute("MATCH p = (a:P)-[:KNOWS]->(b:P) DELETE p")
        .await
        .unwrap_err();

    assert!(
        matches!(err, EngineError::Unsupported(ref msg) if msg.contains("path variable")),
        "{err:?}"
    );
}

#[tokio::test]
async fn delete_edge_target_flushes_with_endpoints() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
        db.execute("INSERT (:P {_id: 1})-[:KNOWS]->(:P {_id: 2})")
            .await
            .unwrap();
        wait_for_flush_count(dir.path(), 1).await;

        db.execute("MATCH (a:P)-[r:KNOWS]->(b:P) DELETE r")
            .await
            .unwrap();
        wait_for_flush_count(dir.path(), 2).await;
    }

    let db = Db::local(dir.path()).await.unwrap();
    let rows = db
        .query("MATCH (a:P)-[:KNOWS]->(b:P) RETURN a._id AS a, b._id AS b")
        .await
        .unwrap();
    assert_eq!(row_count(&rows), 0);
}

#[tokio::test]
async fn match_insert_rejects_edge_binding_as_node_endpoint() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1})-[:R]->(:P {_id: 2})")
        .await
        .unwrap();

    let err = db
        .execute("MATCH (a:P)-[r:R]->(b:P) INSERT (r)-[:X]->(a)")
        .await
        .unwrap_err();

    assert!(
        matches!(
            err,
            EngineError::Unsupported(_) | EngineError::UnboundVariable(_)
        ),
        "{err:?}"
    );
}
