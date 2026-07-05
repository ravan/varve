#![allow(clippy::unwrap_used)]
use std::path::Path;
use varve::{Config, Db};

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

#[tokio::test]
async fn object_store_log_backend_works_end_to_end() {
    // memory storage + object-store log: everything volatile TOGETHER
    // (shared-fate, like Db::memory) — the decision-11 guard only rejects a
    // DURABLE log over a volatile block store.
    let config = Config::from_toml_str(
        "[log]\nbackend = \"object-store\"\n[storage]\nbackend = \"memory\"\n",
    )
    .unwrap();
    let db = Db::open(config).await.unwrap();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();
    let batches = db
        .query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&batches), 1);
}

#[tokio::test]
async fn object_store_log_replays_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let toml = format!(
        "[log]\nbackend = \"object-store\"\n\
         [storage]\nbackend = \"local\"\n[storage.local]\ndir = {}\n",
        toml_escaped(&dir.path().join("store"))
    );
    {
        let db = Db::open(Config::from_toml_str(&toml).unwrap()).await.unwrap();
        // execute() acks only after the durable PUT, so both records are in
        // v1/log/ the moment these return — dropping the Db is safe.
        db.execute("INSERT (:P {_id: 1, name: 'a'})").await.unwrap();
        db.execute("INSERT (:P {_id: 2, name: 'b'})").await.unwrap();
    }
    let db = Db::open(Config::from_toml_str(&toml).unwrap()).await.unwrap();
    let batches = db.query("MATCH (p:P) RETURN p.name").await.unwrap();
    assert_eq!(rows(&batches), 2, "slice-3 replay through the object log");
}
