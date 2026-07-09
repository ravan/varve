use std::path::Path;
use std::time::Duration;

use varve::{Config, Db, RecordBatch};

pub fn toml_escaped_path(path: &Path) -> String {
    format!("{:?}", path.display().to_string())
}

pub fn local_blocks_config(root: &Path, max_block_rows: usize) -> Config {
    let log_dir = toml_escaped_path(&root.join("log"));
    let store_dir = toml_escaped_path(&root.join("store"));
    Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 1\n\
         [log.local]\n\
         dir = {log_dir}\n\
         [storage]\n\
         backend = \"local\"\n\
         max_block_rows = {max_block_rows}\n\
         [storage.local]\n\
         dir = {store_dir}\n"
    ))
    .expect("local blocks config should parse")
}

pub fn local_gc_blocks_config(root: &Path, max_block_rows: usize) -> Config {
    let log_dir = toml_escaped_path(&root.join("log"));
    let store_dir = toml_escaped_path(&root.join("store"));
    Config::from_toml_str(&format!(
        "[log]\n\
         backend = \"local\"\n\
         group_commit_window_ms = 1\n\
         [log.local]\n\
         dir = {log_dir}\n\
         [storage]\n\
         backend = \"local\"\n\
         max_block_rows = {max_block_rows}\n\
         [storage.local]\n\
         dir = {store_dir}\n\
         [gc]\n\
         enabled = true\n\
         blocks_to_keep = 0\n\
         garbage_lifetime_hours = 0\n"
    ))
    .expect("local gc blocks config should parse")
}

pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

pub async fn wait_for_manifest_count(root: &Path, count: usize) {
    let blocks = root.join("store").join("v1").join("blocks");
    for _ in 0..200 {
        let got = blocks
            .read_dir()
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|entry| entry.file_name().to_string_lossy().ends_with(".manifest"))
                    .count()
            })
            .unwrap_or(0);
        if got >= count {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("expected at least {count} manifests under {blocks:?}");
}

pub async fn compact_until_idle(db: &Db) -> Result<usize, Box<dyn std::error::Error>> {
    let mut jobs = 0;
    for _ in 0..16 {
        let report = db.compact_once().await?;
        if report.jobs == 0 {
            return Ok(jobs);
        }
        jobs += report.jobs;
    }
    Err(std::io::Error::other("compaction did not become idle").into())
}
