//! Full-scan latency over flushed blocks: what a paged list (`LIMIT`) and a
//! `DISTINCT` facet cost on a label with a few hundred thousand nodes.
//!
//! Run: cargo run --release --example scan_bench -p varve
//! Env: VARVE_SCAN_NODES (default 200000), VARVE_SCAN_LIVE (default 0) extra
//! nodes left unflushed in the live tail.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;
use std::time::{Duration, Instant};
use varve::{Config, Db};

const BATCH: usize = 1000;
const SEVERITIES: [&str; 5] = ["critical", "high", "medium", "low", "none"];

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

fn config(root: &Path, max_block_rows: usize) -> Config {
    Config::from_toml_str(&format!(
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n[log.local]\ndir = {}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {max_block_rows}\n\
         [storage.local]\ndir = {}\n",
        toml_escaped(&root.join("log")),
        toml_escaped(&root.join("store")),
    ))
    .expect("valid bench config")
}

async fn insert_nodes(db: &Db, from: usize, to: usize) {
    let mut id = from;
    while id < to {
        let end = (id + BATCH).min(to);
        let mut stmt = String::from("INSERT ");
        for (n, i) in (id..end).enumerate() {
            if n > 0 {
                stmt.push_str(", ");
            }
            let sev = SEVERITIES[i % SEVERITIES.len()];
            stmt.push_str(&format!(
                "(:Vulnerability {{_id: {i}, vulnID: 'cve-2026-{i}', sev: '{sev}', \
                 score: {}.{}, published: {}, source: 'osv', \
                 desc: 'synthetic vulnerability number {i} for the scan bench'}})",
                i % 10,
                i % 7,
                1_700_000_000 + i
            ));
        }
        db.execute(&stmt).await.expect("insert batch");
        id = end;
    }
}

async fn wait_flushed(db: &Db, target_live: u64) {
    for _ in 0..600 {
        if db.metrics().live_rows <= target_live {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("flush did not land");
}

async fn timed(db: &Db, gql: &str) -> (Duration, usize) {
    let mut best = Duration::MAX;
    let mut rows = 0;
    for _ in 0..3 {
        let start = Instant::now();
        let batches = db.query(gql).await.expect("query");
        best = best.min(start.elapsed());
        rows = batches.iter().map(|b| b.num_rows()).sum();
    }
    (best, rows)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let nodes: usize = std::env::var("VARVE_SCAN_NODES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let live: usize = std::env::var("VARVE_SCAN_LIVE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let root = tempfile::tempdir().expect("tempdir");
    let db = Db::open(config(root.path(), 50_000)).await.expect("open");

    let start = Instant::now();
    insert_nodes(&db, 0, nodes).await;
    // Pad to the block boundary so the whole label is flushed.
    let pad = (50_000 - nodes % 50_000) % 50_000;
    if pad > 0 {
        let mut stmt = String::from("INSERT ");
        for i in 0..pad {
            if i > 0 {
                stmt.push_str(", ");
            }
            stmt.push_str(&format!("(:Pad {{_id: {i}}})"));
        }
        db.execute(&stmt).await.expect("pad");
    }
    wait_flushed(&db, 0).await;
    if live > 0 {
        insert_nodes(&db, nodes, nodes + live).await;
    }
    println!(
        "ingest {nodes} flushed + {live} live nodes: {:?}",
        start.elapsed()
    );

    let queries = [
        "MATCH (v:Vulnerability) RETURN v.vulnID",
        "MATCH (v:Vulnerability) RETURN v.vulnID LIMIT 5",
        "MATCH (v:Vulnerability) RETURN v.vulnID SKIP 100000 LIMIT 5",
        "MATCH (v:Vulnerability) RETURN DISTINCT v.sev",
        "MATCH (v:Vulnerability) RETURN DISTINCT v.sev LIMIT 5",
        "MATCH (v:Vulnerability) RETURN count(*)",
        "MATCH (v:Vulnerability) WHERE v.vulnID = 'cve-2026-777' RETURN v.sev",
        "MATCH (v:Vulnerability) RETURN v ORDER BY v.published DESC LIMIT 5",
    ];
    for gql in queries {
        let (best, rows) = timed(&db, gql).await;
        println!("{best:>10.1?}  {rows:>7} rows  {gql}");
    }
}
