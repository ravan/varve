#![allow(clippy::unwrap_used)]
use std::path::Path;
use varve::{Config, Db};

fn rows(batches: &[varve::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

/// local log + local storage + the named cache tiers under `root`.
fn tiers_config(root: &Path, tiers: &str, max_block_rows: usize) -> Config {
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {max_block_rows}\n\
         [storage.local]\ndir = {}\n\
         [cache]\ntiers = [{tiers}]\n\
         [cache.disk]\ndir = {}\n",
        toml_escaped(&root.join("log")),
        toml_escaped(&root.join("store")),
        toml_escaped(&root.join("cache")),
    ))
    .unwrap()
}

/// Flushes happen asynchronously after acks (same helper as blocks.rs).
async fn wait_for_flush(root: &Path) {
    let blocks = root.join("store").join("v1").join("blocks");
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
async fn disk_tier_selected_by_name_populates_and_survives_restart() {
    let root = tempfile::tempdir().unwrap();
    {
        let db = Db::open(tiers_config(root.path(), "\"disk\"", 3))
            .await
            .unwrap();
        for i in 1..=3 {
            db.execute(&format!("INSERT (:P {{_id: {i}, name: 'p{i}'}})"))
                .await
                .unwrap();
        }
        wait_for_flush(root.path()).await;
        // Reading persisted pages fills the disk tier.
        let all = db.query("MATCH (p:P) RETURN p.name").await.unwrap();
        assert_eq!(rows(&all), 3);
    }
    let cache_files = std::fs::read_dir(root.path().join("cache"))
        .unwrap()
        .count();
    assert!(cache_files > 0, "query filled the disk cache");

    // Restart: the SAME cache dir is rebuilt and correctness holds.
    let db = Db::open(tiers_config(root.path(), "\"disk\"", 3))
        .await
        .unwrap();
    let all = db.query("MATCH (p:P) RETURN p.name").await.unwrap();
    assert_eq!(rows(&all), 3);
}

#[tokio::test]
async fn memory_and_disk_chain_composes() {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(tiers_config(root.path(), "\"memory\", \"disk\"", 1000))
        .await
        .unwrap();
    db.execute("INSERT (:P {_id: 1, name: 'a'})").await.unwrap();
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p.name").await.unwrap()),
        1
    );
}

#[tokio::test]
async fn empty_tier_list_runs_uncached() {
    let root = tempfile::tempdir().unwrap();
    let db = Db::open(tiers_config(root.path(), "", 1000)).await.unwrap();
    db.execute("INSERT (:P {_id: 1, name: 'a'})").await.unwrap();
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p.name").await.unwrap()),
        1
    );
}

#[tokio::test]
async fn unknown_tier_error_lists_available() {
    let root = tempfile::tempdir().unwrap();
    let err = match Db::open(tiers_config(root.path(), "\"l2\"", 1000)).await {
        Ok(_) => panic!("expected unknown cache tier to fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("l2"), "{err}");
    assert!(err.contains("disk"), "{err}");
    assert!(err.contains("memory"), "{err}");
}

#[tokio::test]
async fn disk_tier_requires_its_dir() {
    let root = tempfile::tempdir().unwrap();
    let config = Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\n[log.local]\ndir = {}\n\
         [storage]\nbackend = \"local\"\n[storage.local]\ndir = {}\n\
         [cache]\ntiers = [\"disk\"]\n",
        toml_escaped(&root.path().join("log")),
        toml_escaped(&root.path().join("store")),
    ))
    .unwrap();
    // EngineError wraps RegistryError transparently, so the build error's
    // own message is the display text — no variant matching needed.
    let err = match Db::open(config).await {
        Ok(_) => panic!("expected disk tier without [cache.disk] to fail"),
        Err(e) => e.to_string(),
    };
    assert!(err.contains("[cache.disk]"), "{err}");
}
