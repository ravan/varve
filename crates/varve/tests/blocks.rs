#![allow(clippy::unwrap_used)]
use std::path::Path;
use varve::{Config, Db, EngineError};

/// log + storage both local under `dir`, tiny block threshold so tests
/// actually flush, 1 ms group-commit window.
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

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

/// Flushes happen asynchronously after acks — wait until the store dir has
/// a manifest (or give up and let assertions fail loudly).
async fn wait_for_flush(dir: &Path) {
    let blocks = dir.join("store").join("v1").join("blocks");
    for _ in 0..200 {
        if blocks
            .read_dir()
            .map(|mut d| d.next().is_some())
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("no manifest appeared under {blocks:?} within 5s");
}

#[tokio::test]
async fn flushed_blocks_survive_restart_with_correct_queries() {
    let dir = tempfile::tempdir().unwrap();
    {
        let db = Db::open(blocks_config(dir.path(), 4)).await.unwrap();
        for (id, name) in [
            (1, "Ada"),
            (2, "Bob"),
            (3, "Cyd"),
            (4, "Dee"),
            (5, "Eve"),
            (6, "Fay"),
        ] {
            db.execute(&format!("INSERT (:Person {{_id: {id}, name: '{name}'}})"))
                .await
                .unwrap();
        }
        wait_for_flush(dir.path()).await; // 4 rows in block 0, 2 still live
    }

    let db = Db::open(blocks_config(dir.path(), 4)).await.unwrap();
    let all = db.query("MATCH (p:Person) RETURN p.name").await.unwrap();
    assert_eq!(rows(&all), 6);

    // Point lookup crosses the block/live split correctly (IID pushdown path).
    let point = db
        .query("MATCH (p:Person) WHERE p._id = 3 RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&point), 1);
    let names: &arrow::array::StringArray = point[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(names.value(0), "Cyd");
}

#[tokio::test]
async fn tx_and_clock_floors_survive_restart_with_a_trimmed_log() {
    let dir = tempfile::tempdir().unwrap();
    let last_receipt;
    {
        let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
        db.execute("INSERT (:P {_id: 1})").await.unwrap();
        last_receipt = db.execute("INSERT (:P {_id: 2})").await.unwrap();
        wait_for_flush(dir.path()).await; // flush + trim: the log is now empty
    }

    let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
    // With an empty log, floors MUST come from the manifest.
    let next = db.execute("INSERT (:P {_id: 3})").await.unwrap();
    assert_eq!(next.tx_id, 3, "tx counter continues past flushed history");
    assert!(
        next.system_time > last_receipt.system_time,
        "clock floored above flushed history"
    );
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p._id").await.unwrap()),
        3
    );
}

#[tokio::test]
async fn flushed_catalog_graphs_survive_restart_without_user_data() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
        db.execute("CREATE GRAPH tenant").await.unwrap();
        wait_for_flush(dir.path()).await;
    }

    let db = Db::open(blocks_config(dir.path(), 1)).await.unwrap();
    let rows_in_empty_graph = db
        .query("USE tenant; MATCH (p:Person) RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&rows_in_empty_graph), 0);
}

#[tokio::test]
async fn bitemporal_history_survives_flush_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    let v1;
    {
        let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
        v1 = db
            .execute("INSERT (:City {_id: 1, name: 'Oslo'})")
            .await
            .unwrap();
        db.execute("INSERT (:City {_id: 1, name: 'Osloo'})")
            .await
            .unwrap();
        wait_for_flush(dir.path()).await;
    }

    let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
    let now = db
        .query("MATCH (c:City) RETURN c.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&now), 1);
    let names: &arrow::array::StringArray = now[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(names.value(0), "Osloo");

    // Time travel to before the correction — served from the flushed block.
    let before = db
        .query(&format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (c:City) RETURN c.name AS name",
            v1.system_time
        ))
        .await
        .unwrap();
    assert_eq!(rows(&before), 1);
    let names: &arrow::array::StringArray = before[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(names.value(0), "Oslo");
}

#[tokio::test]
async fn flushed_l0_meta_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
        db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
            .await
            .unwrap();
        db.execute("INSERT (:Person {_id: 2, name: 'Bob'})")
            .await
            .unwrap();
        wait_for_flush(dir.path()).await;
    }

    let meta_path = dir
        .path()
        .join("store")
        .join("v1")
        .join("graphs")
        .join("default")
        .join("tables")
        .join("nodes")
        .join("meta")
        .join("l00-rc-b00.arrow");
    let meta = std::fs::read(&meta_path).unwrap();
    let pages = varve_index::decode_meta(&meta).unwrap();
    assert!(!pages.is_empty());

    let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
    let point = db
        .query("MATCH (p:Person) WHERE p._id = 2 RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&point), 1);
}

#[tokio::test]
async fn local_log_with_memory_storage_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let log_dir = toml_escaped(&dir.path().join("log"));
    let config = Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\n[log.local]\ndir = {log_dir}\n"
    ))
    .unwrap();
    let err = Db::open(config).await.unwrap_err();
    assert!(matches!(err, EngineError::VolatileBlockStore), "{err}");
    assert!(err.to_string().contains("[storage]"), "{err}");
}
