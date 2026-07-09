#![allow(clippy::unwrap_used)]

use arrow::array::{Array, StringArray};
use varve::{Config, Db, EngineError, RecordBatch};
use varve_testkit::db_harness::{
    local_blocks_config as blocks_config, row_count as rows, wait_for_manifest_count,
};

fn strings(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut out: Vec<String> = batches
        .iter()
        .flat_map(|b| {
            let a: &StringArray = b
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref()
                .unwrap();
            (0..a.len())
                .map(|i| a.value(i).to_string())
                .collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

fn group_commit_config() -> Config {
    Config::from_toml_str(
        "[log]\nbackend = \"memory\"\ngroup_commit_window_ms = 100\n\
         [storage]\nbackend = \"memory\"\nmax_block_rows = 1000\n",
    )
    .unwrap()
}

#[tokio::test]
async fn create_graph_then_use_inserts_isolated() {
    let db = Db::memory();

    db.execute("CREATE GRAPH tenant").await.unwrap();
    db.execute("INSERT (:Person {_id: 1, name: 'Default Ada'})")
        .await
        .unwrap();
    db.execute("USE tenant; INSERT (:Person {_id: 1, name: 'Tenant Ada'})")
        .await
        .unwrap();

    let default_rows = db
        .query("MATCH (p:Person) RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(strings(&default_rows, "name"), vec!["Default Ada"]);

    let tenant_rows = db
        .query("USE tenant; MATCH (p:Person) RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(strings(&tenant_rows, "name"), vec!["Tenant Ada"]);
}

#[tokio::test]
async fn create_graph_then_use_insert_in_same_group_commit_window() {
    let db = Db::open(group_commit_config()).await.unwrap();

    let create = db.execute("CREATE GRAPH tenant");
    let insert = db.execute("USE tenant; INSERT (:Person {_id: 1, name: 'Tenant Ada'})");
    let (create, insert) = tokio::join!(biased; create, insert);

    create.unwrap();
    insert.unwrap();
    let tenant_rows = db
        .query("USE tenant; MATCH (p:Person) RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(strings(&tenant_rows, "name"), vec!["Tenant Ada"]);
}

#[tokio::test]
async fn drop_graph_then_use_insert_in_same_group_commit_window_is_not_committed() {
    let db = Db::open(group_commit_config()).await.unwrap();
    db.execute("CREATE GRAPH tenant").await.unwrap();

    let drop_graph = db.execute("DROP GRAPH tenant");
    let insert = db.execute("USE tenant; INSERT (:Person {_id: 1, name: 'Tenant Ada'})");
    let (drop_graph, insert) = tokio::join!(biased; drop_graph, insert);

    drop_graph.unwrap();
    let err = insert.unwrap_err();
    assert!(matches!(err, EngineError::UnknownGraph(g) if g == "tenant"));
    let err = db
        .query("USE tenant; MATCH (p:Person) RETURN p.name AS name")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::UnknownGraph(g) if g == "tenant"));
}

#[tokio::test]
async fn use_unknown_graph_errors() {
    let db = Db::memory();

    let err = db
        .execute("USE missing; INSERT (:Person {_id: 1})")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::UnknownGraph(g) if g == "missing"));

    let err = db
        .query("USE missing; MATCH (p:Person) RETURN p.name")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::UnknownGraph(g) if g == "missing"));
}

#[tokio::test]
async fn create_existing_graph_errors() {
    let db = Db::memory();

    db.execute("CREATE GRAPH tenant").await.unwrap();
    let err = db.execute("CREATE GRAPH tenant").await.unwrap_err();
    assert!(matches!(err, EngineError::GraphExists(g) if g == "tenant"));
}

#[tokio::test]
async fn drop_graph_then_use_errors() {
    let db = Db::memory();

    db.execute("CREATE GRAPH tenant").await.unwrap();
    db.execute("USE tenant; INSERT (:Person {_id: 1})")
        .await
        .unwrap();
    db.execute("DROP GRAPH tenant").await.unwrap();

    let err = db
        .query("USE tenant; MATCH (p:Person) RETURN p.name")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::UnknownGraph(g) if g == "tenant"));
}

#[tokio::test]
async fn drop_default_rejected() {
    let db = Db::memory();

    let err = db.execute("DROP GRAPH default").await.unwrap_err();
    assert!(matches!(err, EngineError::Unsupported(msg) if msg.contains("default")));
}

#[tokio::test]
async fn meta_prefix_rejected_for_users() {
    let db = Db::memory();

    let err = db.execute("CREATE GRAPH __tenant").await.unwrap_err();
    assert!(matches!(err, EngineError::Unsupported(msg) if msg.contains("__")));
}

#[tokio::test]
async fn graphs_survive_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
        db.execute("CREATE GRAPH tenant").await.unwrap();
        db.execute("USE tenant; INSERT (:Person {_id: 1, name: 'Tenant Ada'})")
            .await
            .unwrap();
        wait_for_manifest_count(dir.path(), 1).await;
    }

    let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
    let tenant_rows = db
        .query("USE tenant; MATCH (p:Person) RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(strings(&tenant_rows, "name"), vec!["Tenant Ada"]);
}

#[tokio::test]
async fn create_graph_catalog_only_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Db::open(blocks_config(dir.path(), 100)).await.unwrap();
        db.execute("CREATE GRAPH tenant").await.unwrap();
    }

    let db = Db::open(blocks_config(dir.path(), 100)).await.unwrap();
    let rows_in_empty_graph = db
        .query("USE tenant; MATCH (p:Person) RETURN p.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&rows_in_empty_graph), 0);
}

#[tokio::test]
async fn drop_graph_survives_restart() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Db::open(blocks_config(dir.path(), 100)).await.unwrap();
        db.execute("CREATE GRAPH tenant").await.unwrap();
        db.execute("USE tenant; INSERT (:Person {_id: 1})")
            .await
            .unwrap();
        db.execute("DROP GRAPH tenant").await.unwrap();
    }

    let db = Db::open(blocks_config(dir.path(), 100)).await.unwrap();
    let err = db
        .query("USE tenant; MATCH (p:Person) RETURN p.name")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::UnknownGraph(g) if g == "tenant"));
}

#[tokio::test]
async fn per_graph_flush_one_manifest() {
    let dir = tempfile::tempdir().unwrap();

    {
        let db = Db::open(blocks_config(dir.path(), 3)).await.unwrap();
        db.execute("CREATE GRAPH tenant").await.unwrap();
        db.execute("INSERT (:Person {_id: 1, name: 'Default Ada'})")
            .await
            .unwrap();
        db.execute("USE tenant; INSERT (:Person {_id: 1, name: 'Tenant Ada'})")
            .await
            .unwrap();
        wait_for_manifest_count(dir.path(), 1).await;
    }

    let blocks = dir.path().join("store").join("v1").join("blocks");
    let manifest_count = blocks
        .read_dir()
        .unwrap()
        .flatten()
        .filter(|entry| entry.file_name().to_string_lossy().ends_with(".manifest"))
        .count();
    assert_eq!(manifest_count, 1);

    let db = Db::open(blocks_config(dir.path(), 3)).await.unwrap();
    assert_eq!(
        strings(
            &db.query("MATCH (p:Person) RETURN p.name AS name")
                .await
                .unwrap(),
            "name"
        ),
        vec!["Default Ada"]
    );
    assert_eq!(
        strings(
            &db.query("USE tenant; MATCH (p:Person) RETURN p.name AS name")
                .await
                .unwrap(),
            "name"
        ),
        vec!["Tenant Ada"]
    );
}
