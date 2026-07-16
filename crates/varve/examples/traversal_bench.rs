//! Roadmap slice-6 exit-criteria perf smoke (task 11): full 10k-node/60k-edge
//! deterministic social graph (`varve_testkit::fixture::social_graph`) →
//! ingest via GQL → force-flush via config (`max_block_rows`, mirrors
//! `block_bench.rs`) → reopen → timed 2-hop friend-of-friend and
//! `-[:KNOWS]->{1,3}` expansion from anchor `_id 0`: cold once, then 100 warm
//! iterations (avg + p50). Exit criterion: warm 2-hop < 50 ms (record the
//! printed numbers in STATUS.md).
//! Run: cargo run --release --example traversal_bench -p varve

use std::path::Path;
use std::time::{Duration, Instant};
use varve::{Config, Db, Doc, EdgePut, NodePut, Value};
use varve_testkit::fixture::{social_graph, SocialGraph, EDGE_PROGRAM_BATCH};

const DEFAULT_PEOPLE: usize = 10_000;
const DEFAULT_FRIENDSHIPS: usize = 60_000;
const DEFAULT_NODE_BATCH: usize = 1_000;
const DEFAULT_EDGE_BATCH: usize = EDGE_PROGRAM_BATCH;
const SEED: u64 = 42;
const PROGRESS_INTERVAL: Duration = Duration::from_secs(5);
// Small enough that a handful of block flushes happen over the course of
// ingest (70k events / 20,000 ≈ 3-4 flushes), so the persisted path is
// genuinely exercised (mirrors block_bench.rs's MAX_BLOCK_ROWS rationale).
const MAX_BLOCK_ROWS: usize = 20_000;
const WARM_ITERS: usize = 100;
const ANCHOR: i64 = 0;

/// How phase 1 loads the fixture: `gql` (default — comparable to every
/// recorded baseline run) drives per-program GQL through `db.execute`;
/// `bulk` drives the xtdb-style data-op path through `db.ingest` (no parse,
/// no plan, no per-edge endpoint MATCH).
#[derive(Clone, Copy, PartialEq, Eq)]
enum IngestMode {
    Gql,
    Bulk,
}

impl IngestMode {
    fn unit(self) -> &'static str {
        match self {
            IngestMode::Gql => "programs",
            IngestMode::Bulk => "entities",
        }
    }

    fn name(self) -> &'static str {
        match self {
            IngestMode::Gql => "gql",
            IngestMode::Bulk => "bulk",
        }
    }
}

fn ingest_mode() -> Result<IngestMode, Box<dyn std::error::Error>> {
    match std::env::var("VARVE_TRAVERSAL_INGEST") {
        Ok(value) if value == "gql" => Ok(IngestMode::Gql),
        Ok(value) if value == "bulk" => Ok(IngestMode::Bulk),
        Ok(value) => {
            Err(format!("VARVE_TRAVERSAL_INGEST must be 'gql' or 'bulk', got '{value}'").into())
        }
        Err(std::env::VarError::NotPresent) => Ok(IngestMode::Gql),
        Err(error) => Err(error.into()),
    }
}

/// Opt-in post-ingest compaction (bulk load → compact → serve): a freshly
/// bulk-loaded store is all L0 tries, and every L0 trie spans the full
/// hashed-iid space, so anchored point/set lookups still overlap nearly every
/// page. Compaction rewrites them into globally-sorted tries whose pages
/// partition the space — the shape the anchored fast path's pruning needs.
/// Default off so results stay comparable to the recorded baseline runs.
fn compact_enabled() -> Result<bool, Box<dyn std::error::Error>> {
    match std::env::var("VARVE_TRAVERSAL_COMPACT") {
        Ok(value) if value == "1" => Ok(true),
        Ok(value) if value == "0" => Ok(false),
        Ok(value) => {
            Err(format!("VARVE_TRAVERSAL_COMPACT must be '0' or '1', got '{value}'").into())
        }
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn benchmark_shape() -> Result<(usize, usize, usize, usize), Box<dyn std::error::Error>> {
    fn read(name: &str, default: usize) -> Result<usize, Box<dyn std::error::Error>> {
        let value = match std::env::var(name) {
            Ok(value) => value.parse::<usize>()?,
            Err(std::env::VarError::NotPresent) => default,
            Err(error) => return Err(error.into()),
        };
        if value == 0 {
            return Err(format!("{name} must be greater than zero").into());
        }
        Ok(value)
    }

    Ok((
        read("VARVE_TRAVERSAL_PEOPLE", DEFAULT_PEOPLE)?,
        read("VARVE_TRAVERSAL_FRIENDSHIPS", DEFAULT_FRIENDSHIPS)?,
        read("VARVE_TRAVERSAL_NODE_BATCH", DEFAULT_NODE_BATCH)?,
        read("VARVE_TRAVERSAL_EDGE_BATCH", DEFAULT_EDGE_BATCH)?,
    ))
}

#[allow(clippy::too_many_arguments)]
fn report_progress(
    phase: &str,
    phase_completed: usize,
    phase_total: usize,
    completed: usize,
    total: usize,
    stage_eta_known: bool,
    last_completed: &mut usize,
    last_report: &mut Instant,
) {
    let now = Instant::now();
    let interval = now.duration_since(*last_report);
    if phase_completed < phase_total && interval < PROGRESS_INTERVAL {
        return;
    }

    let completed_delta = phase_completed - *last_completed;
    let rate = completed_delta as f64 / interval.as_secs_f64();
    if stage_eta_known {
        let eta_seconds = (phase_total - phase_completed) as f64 / rate;
        println!(
            "progress completed={completed} total={total} phase={phase} phase_completed={phase_completed} phase_total={phase_total} rate={rate:.3} eta_seconds={eta_seconds:.1}"
        );
    } else {
        println!(
            "progress completed={completed} total={total} phase={phase} phase_completed={phase_completed} phase_total={phase_total} rate={rate:.3} eta_seconds=unknown"
        );
    }
    *last_completed = phase_completed;
    *last_report = now;
}

fn config(dir: &Path) -> Result<Config, Box<dyn std::error::Error>> {
    let log_dir = format!("{:?}", dir.join("log").display().to_string());
    let store_dir = format!("{:?}", dir.join("store").display().to_string());
    Ok(Config::from_toml_str(&format!(
        // group_commit_window_ms = 1 (mirrors tests/blocks.rs's blocks_config
        // helper): ingest here is a single sequential writer awaiting each
        // program's ack in turn, so there is never a second commit in flight to
        // batch with — every default 15ms window would be paid in full, per
        // program, for nothing. A tiny window keeps the write path honest
        // without that artificial latency floor.
        "[log]\nbackend = \"local\"\ngroup_commit_window_ms = 1\n\
         [log.local]\ndir = {log_dir}\n\
         [storage]\nbackend = \"local\"\nmax_block_rows = {MAX_BLOCK_ROWS}\n\
         [storage.local]\ndir = {store_dir}\n"
    ))?)
}

async fn two_hop(db: &Db) -> Result<usize, Box<dyn std::error::Error>> {
    let rows = db
        .query(format!(
            "MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) \
             WHERE a._id = {ANCHOR} RETURN c._id"
        ))
        .await?;
    Ok(rows.iter().map(|b| b.num_rows()).sum())
}

async fn quantified_1_3(db: &Db) -> Result<usize, Box<dyn std::error::Error>> {
    let rows = db
        .query(format!(
            "MATCH (a:Person)-[:KNOWS]->{{1,3}}(b:Person) WHERE a._id = {ANCHOR} RETURN b._id"
        ))
        .await?;
    Ok(rows.iter().map(|b| b.num_rows()).sum())
}

fn p50(mut xs: Vec<Duration>) -> Duration {
    xs.sort_unstable();
    xs[xs.len() / 2]
}

fn avg(xs: &[Duration]) -> Duration {
    xs.iter().sum::<Duration>() / xs.len() as u32
}

/// Times `WARM_ITERS` back-to-back calls to `f`, asserting every call
/// returns the same row count as `expect` (traversal answers over a static
/// graph must be stable across repeated warm reads).
async fn warm_timings<F, Fut>(
    f: F,
    expect: usize,
    label: &str,
) -> Result<Vec<Duration>, Box<dyn std::error::Error>>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<usize, Box<dyn std::error::Error>>>,
{
    let mut times = Vec::with_capacity(WARM_ITERS);
    for _ in 0..WARM_ITERS {
        let t0 = Instant::now();
        let rows = f().await?;
        times.push(t0.elapsed());
        assert_eq!(rows, expect, "{label}: row count stable across warm iters");
    }
    Ok(times)
}

fn person_put(id: i64) -> NodePut {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    doc.insert("name".to_string(), Value::Str(format!("p{id}")));
    NodePut {
        labels: vec!["Person".to_string()],
        doc,
    }
}

fn knows_put(src: i64, dst: i64) -> EdgePut {
    EdgePut {
        label: "KNOWS".to_string(),
        src: Value::Int(src),
        dst: Value::Int(dst),
        doc: Doc::new(),
    }
}

/// GQL loader (the historical path): one `db.execute` per program, counted
/// in programs. Returns the total program count.
async fn ingest_gql(
    db: &Db,
    g: &SocialGraph,
    node_batch: usize,
    edge_batch: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let node_stmts = g.node_statements(node_batch);
    let edge_programs = g.edge_programs(edge_batch);
    let total_programs = node_stmts.len() + edge_programs.len();
    let mut last_progress = Instant::now();
    let mut last_phase_completed = 0;
    let mut completed = 0;
    for (index, stmt) in node_stmts.iter().enumerate() {
        db.execute(stmt).await?;
        completed += 1;
        report_progress(
            "node",
            index + 1,
            node_stmts.len(),
            completed,
            total_programs,
            false,
            &mut last_phase_completed,
            &mut last_progress,
        );
    }

    last_progress = Instant::now();
    last_phase_completed = 0;
    for (index, program) in edge_programs.iter().enumerate() {
        db.execute(program).await?;
        completed += 1;
        report_progress(
            "edge",
            index + 1,
            edge_programs.len(),
            completed,
            total_programs,
            true,
            &mut last_phase_completed,
            &mut last_progress,
        );
    }
    Ok(total_programs)
}

/// Bulk loader: `db.ingest` data ops, one transaction per chunk. Progress is
/// counted in ENTITIES (nodes/edges), not chunks — chunks vary in size (the
/// last one is smaller), and the admission-gate lesson requires homogeneous
/// completed-work units for trustworthy ETAs. Returns total entity count.
async fn ingest_bulk(
    db: &Db,
    g: &SocialGraph,
    node_batch: usize,
    edge_batch: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let total_entities = g.people + g.edges.len();
    let mut last_progress = Instant::now();
    let mut last_phase_completed = 0;
    let mut completed = 0;

    let mut start = 0usize;
    while start < g.people {
        let end = (start + node_batch).min(g.people);
        let chunk: Vec<NodePut> = (start..end).map(|id| person_put(id as i64)).collect();
        completed += chunk.len();
        db.ingest(chunk, Vec::new()).await?;
        report_progress(
            "node",
            end,
            g.people,
            completed,
            total_entities,
            false,
            &mut last_phase_completed,
            &mut last_progress,
        );
        start = end;
    }

    last_progress = Instant::now();
    last_phase_completed = 0;
    let mut phase_completed = 0;
    for chunk in g.edges.chunks(edge_batch) {
        let puts: Vec<EdgePut> = chunk.iter().map(|&(s, d)| knows_put(s, d)).collect();
        completed += chunk.len();
        phase_completed += chunk.len();
        db.ingest(Vec::new(), puts).await?;
        report_progress(
            "edge",
            phase_completed,
            g.edges.len(),
            completed,
            total_entities,
            true,
            &mut last_phase_completed,
            &mut last_progress,
        );
    }
    Ok(total_entities)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempfile::tempdir()?;
    let (people, friendships, node_batch, edge_batch) = benchmark_shape()?;
    let mode = ingest_mode()?;
    println!(
        "fixture people={people} friendships={friendships} seed={SEED} node_batch={node_batch} edge_batch={edge_batch} ingest={}",
        mode.name()
    );
    let g = social_graph(people, friendships, SEED);

    // Phase 1: ingest the full fixture (timed), then drop — acked txs are
    // durable (log) or flushed (blocks).
    let ingest_started = Instant::now();
    let total_units;
    {
        let db = Db::open(config(dir.path())?).await?;
        total_units = match mode {
            IngestMode::Gql => ingest_gql(&db, &g, node_batch, edge_batch).await?,
            IngestMode::Bulk => ingest_bulk(&db, &g, node_batch, edge_batch).await?,
        };
    }
    let ingest = ingest_started.elapsed();
    let unit = mode.unit();
    let units_per_sec = total_units as f64 / ingest.as_secs_f64();
    println!(
        "ingest {total_units} {unit} ({} nodes, {} edges) in {ingest:.2?} ({units_per_sec:.0} {unit}/s)",
        g.people,
        g.edges.len()
    );

    // Phase 2: restart = latest manifest + log tail replay.
    let reopen_started = Instant::now();
    let db = Db::open(config(dir.path())?).await?;
    println!(
        "reopen (manifest + log tail): {:.2?}",
        reopen_started.elapsed()
    );

    // Phase 2b (opt-in): drain compaction to idle before serving queries.
    if compact_enabled()? {
        let compact_started = Instant::now();
        let mut jobs = 0usize;
        loop {
            let report = db.compact_full_once().await?;
            if report.jobs == 0 {
                break;
            }
            jobs += report.jobs;
        }
        println!(
            "compact jobs={jobs} in {:.2?} (drained to idle)",
            compact_started.elapsed()
        );
    }

    // Phase 3: 2-hop friend-of-friend — cold once, then warm.
    let cold_started = Instant::now();
    let two_hop_rows = two_hop(&db).await?;
    let two_hop_cold = cold_started.elapsed();
    let two_hop_warm = warm_timings(|| two_hop(&db), two_hop_rows, "2-hop").await?;
    let two_hop_warm_avg = avg(&two_hop_warm);
    let two_hop_warm_p50 = p50(two_hop_warm);

    // Phase 4: -[:KNOWS]->{1,3} — same shape.
    let cold_started = Instant::now();
    let q13_rows = quantified_1_3(&db).await?;
    let q13_cold = cold_started.elapsed();
    let q13_warm = warm_timings(|| quantified_1_3(&db), q13_rows, "{1,3}").await?;
    let q13_warm_avg = avg(&q13_warm);
    let q13_warm_p50 = p50(q13_warm);

    println!(
        "ingest {:.2}s · {units_per_sec:.0} {unit}/s · \
         2-hop ({two_hop_rows} rows) cold {two_hop_cold:.2?} warm avg {two_hop_warm_avg:.2?} p50 {two_hop_warm_p50:.2?} · \
         {{1,3}} ({q13_rows} rows) cold {q13_cold:.2?} warm avg {q13_warm_avg:.2?} p50 {q13_warm_p50:.2?}",
        ingest.as_secs_f64(),
    );
    println!(
        "exit criterion (warm 2-hop < 50ms): {}",
        if two_hop_warm_avg.as_millis() < 50 {
            "PASS"
        } else {
            "FAIL"
        }
    );
    Ok(())
}
