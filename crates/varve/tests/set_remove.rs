#![allow(clippy::unwrap_used)]

use arrow::array::{Array, Int64Array, StringArray};
use std::collections::BTreeMap;
use std::path::Path;
use varve::{Config, Db, EngineError, RecordBatch};
use varve_types::Value;

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn strings(batches: &[RecordBatch], col: &str) -> Vec<String> {
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

fn ints(batches: &[RecordBatch], col: &str) -> Vec<i64> {
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            (0..col.len()).map(|idx| col.value(idx))
        })
        .collect();
    out.sort();
    out
}

fn one_param(name: &str, value: Value) -> BTreeMap<String, Value> {
    BTreeMap::from([(name.to_string(), value)])
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

async fn wait_for_flush(dir: &Path) {
    let blocks = dir.join("store").join("v1").join("blocks");
    for _ in 0..200 {
        if blocks
            .read_dir()
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false)
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("no manifest appeared under {blocks:?} within 5s");
}

#[tokio::test]
async fn set_prop_updates_current_state_only() {
    let db = Db::memory();
    let inserted = db
        .execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();

    db.execute("MATCH (n:Person {_id: 1}) SET n.name = 'Adele'")
        .await
        .unwrap();

    let current = db
        .query("MATCH (n:Person {_id: 1}) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(strings(&current, "name"), vec!["Adele"]);

    let before_valid = db
        .query(&format!(
            "FOR VALID_TIME AS OF TIMESTAMP '{}' MATCH (n:Person {{_id: 1}}) RETURN n.name AS name",
            inserted.system_time
        ))
        .await
        .unwrap();
    assert_eq!(strings(&before_valid, "name"), vec!["Ada"]);

    let before_system = db
        .query(&format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (n:Person {{_id: 1}}) RETURN n.name AS name",
            inserted.system_time
        ))
        .await
        .unwrap();
    assert_eq!(strings(&before_system, "name"), vec!["Ada"]);
}

#[tokio::test]
async fn set_prop_from_expression_per_row() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, age: 10}), (:Person {_id: 2, age: 20}), (:Person {_id: 3, age: 30})")
        .await
        .unwrap();

    db.execute("MATCH (n:Person) SET n.double = n.age * 2")
        .await
        .unwrap();

    let batches = db
        .query("MATCH (n:Person) RETURN n.double AS double")
        .await
        .unwrap();
    assert_eq!(ints(&batches, "double"), vec![20, 40, 60]);
}

#[tokio::test]
async fn set_add_label_and_match_by_it() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();

    db.execute("MATCH (n:Person {_id: 1}) SET n:Employee")
        .await
        .unwrap();

    let employees = db
        .query("MATCH (n:Employee) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(strings(&employees, "name"), vec!["Ada"]);
}

#[tokio::test]
async fn set_after_flush_updates_requested_iid_payload() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(blocks_config(dir.path(), 2)).await.unwrap();
    db.execute(
        "INSERT (:Person:Target {_id: 1, name: 'Ada', city: 'London'}), \
         (:Person:Other {_id: 2, name: 'Bob', city: 'Paris'})",
    )
    .await
    .unwrap();
    wait_for_flush(dir.path()).await;

    db.execute("MATCH (n:Person {_id: 1}) SET n.city = 'Oslo'")
        .await
        .unwrap();

    let target = db
        .query("MATCH (n:Target {_id: 1}) RETURN n.name AS name, n.city AS city")
        .await
        .unwrap();
    assert_eq!(rows(&target), 1);
    assert_eq!(strings(&target, "name"), vec!["Ada"]);
    assert_eq!(strings(&target, "city"), vec!["Oslo"]);

    let other = db
        .query("MATCH (n:Other {_id: 2}) RETURN n.name AS name, n.city AS city")
        .await
        .unwrap();
    assert_eq!(rows(&other), 1);
    assert_eq!(strings(&other, "name"), vec!["Bob"]);
    assert_eq!(strings(&other, "city"), vec!["Paris"]);
}

#[tokio::test]
async fn remove_prop_and_label() {
    let db = Db::memory();
    db.execute("INSERT (:Person:Legacy {_id: 1, name: 'Ada', city: 'London'})")
        .await
        .unwrap();

    db.execute("MATCH (n:Person {_id: 1}) REMOVE n.city, n:Legacy")
        .await
        .unwrap();

    let without_city = db
        .query("MATCH (n:Person) WHERE n.city IS NULL RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(strings(&without_city, "name"), vec!["Ada"]);

    let legacy = db
        .query("MATCH (n:Legacy) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&legacy), 0);
}

#[tokio::test]
async fn remove_missing_prop_is_noop_no_event() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();

    db.execute("MATCH (n:Person {_id: 1}) REMOVE n.missing")
        .await
        .unwrap();

    let history = db
        .query("FOR SYSTEM_TIME ALL MATCH (n:Person {_id: 1}) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&history), 1);
    assert_eq!(strings(&history, "name"), vec!["Ada"]);
}

#[tokio::test]
async fn set_merges_multiple_items_one_event() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada', age: 36})")
        .await
        .unwrap();

    db.execute("MATCH (n:Person {_id: 1}) SET n.age = 37, n.city = 'Oslo', n:Employee")
        .await
        .unwrap();

    let current = db
        .query("MATCH (n:Employee) RETURN n.age AS age, n.city AS city")
        .await
        .unwrap();
    assert_eq!(ints(&current, "age"), vec![37]);
    assert_eq!(strings(&current, "city"), vec!["Oslo"]);

    let history = db
        .query("FOR SYSTEM_TIME ALL MATCH (n:Person {_id: 1}) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&history), 2);
}

#[tokio::test]
async fn set_with_param_value() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();

    let params = one_param("name", Value::Str("Adele".to_string()));
    db.execute_with("MATCH (n:Person {_id: 1}) SET n.name = $name", &params)
        .await
        .unwrap();

    let batches = db
        .query("MATCH (n:Person {_id: 1}) RETURN n.name AS name")
        .await
        .unwrap();
    assert_eq!(strings(&batches, "name"), vec!["Adele"]);
}

#[tokio::test]
async fn set_unknown_target_var_errors() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();

    let err = db
        .execute("MATCH (n:Person {_id: 1}) SET missing.name = 'Adele'")
        .await
        .unwrap_err();

    assert!(matches!(err, EngineError::UnboundVariable(var) if var == "missing"));
}
