use std::fs;
use std::path::PathBuf;

fn workspace_file(path: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(path)
}

#[test]
fn trie_benchmark_consumes_bucket_results() {
    let source = fs::read_to_string(workspace_file("crates/varve-types/benches/trie.rs"))
        .expect("read trie benchmark source");
    let bucket_case = source
        .split("bucketer/bucket_level3")
        .nth(1)
        .and_then(|tail| tail.split("bucketer/path_4_levels").next())
        .expect("locate bucket_level3 benchmark body");

    assert!(
        bucket_case.contains("black_box"),
        "bucket_level3 must consume inputs/results through black_box"
    );
}

#[test]
fn traversal_benchmark_accepts_explicit_large_fixture_shape() {
    let source = fs::read_to_string(workspace_file("crates/varve/examples/traversal_bench.rs"))
        .expect("read traversal benchmark source");

    assert!(
        source.contains("VARVE_TRAVERSAL_PEOPLE") && source.contains("VARVE_TRAVERSAL_FRIENDSHIPS"),
        "traversal benchmark must accept explicit node and edge counts"
    );
}

#[test]
fn traversal_benchmark_accepts_explicit_loader_batches() {
    let source = fs::read_to_string(workspace_file("crates/varve/examples/traversal_bench.rs"))
        .expect("read traversal benchmark source");

    for required in [
        "VARVE_TRAVERSAL_NODE_BATCH",
        "VARVE_TRAVERSAL_EDGE_BATCH",
        "node_statements(node_batch)",
        "edge_programs(edge_batch)",
    ] {
        assert!(
            source.contains(required),
            "traversal benchmark missing {}",
            required
        );
    }
}

#[test]
fn traversal_benchmark_reports_machine_readable_program_progress() {
    let source = fs::read_to_string(workspace_file("crates/varve/examples/traversal_bench.rs"))
        .expect("read traversal benchmark source");

    for required in ["progress completed=", "total=", "rate=", "eta_seconds="] {
        assert!(
            source.contains(required),
            "progress output missing {}",
            required
        );
    }
    assert!(
        source.matches("report_progress(").count() >= 3,
        "progress must be reported from both ingest loops"
    );
}

#[test]
fn traversal_progress_uses_phase_local_completed_work_deltas() {
    let source = fs::read_to_string(workspace_file("crates/varve/examples/traversal_bench.rs"))
        .expect("read traversal benchmark source");

    for required in [
        "phase={phase}",
        "phase_completed=",
        "phase_total=",
        "phase_completed - *last_completed",
        "now.duration_since(*last_report)",
    ] {
        assert!(
            source.contains(required),
            "phase-local progress missing {}",
            required
        );
    }
}

#[test]
fn traversal_benchmark_supports_bulk_ingest_mode() {
    let source = fs::read_to_string(workspace_file("crates/varve/examples/traversal_bench.rs"))
        .expect("read traversal benchmark source");

    for required in [
        // opt-in switch: default stays GQL so results remain comparable to
        // the recorded baseline runs; calibrations select bulk explicitly.
        "VARVE_TRAVERSAL_INGEST",
        // the bulk path must use the data-op ingest API (no GQL, no MATCH)…
        "db.ingest(",
        "NodePut",
        "EdgePut",
        // …while the GQL path keeps exercising the statement surface.
        "db.execute(",
    ] {
        assert!(
            source.contains(required),
            "bulk ingest mode missing {}",
            required
        );
    }
}

#[test]
fn traversal_bulk_mode_reports_progress_in_entities_not_chunks() {
    let source = fs::read_to_string(workspace_file("crates/varve/examples/traversal_bench.rs"))
        .expect("read traversal benchmark source");

    // The admission-gate lesson: ETA must come from homogeneous completed
    // work units. Bulk chunks vary in size (the last chunk is smaller), so
    // progress must be counted in entities (nodes/edges), not chunk count.
    assert!(
        source.contains("chunk.len()"),
        "bulk progress must advance by chunk.len() entities, not by 1 per chunk"
    );
}

#[test]
fn traversal_benchmark_supports_post_ingest_compaction() {
    let source = fs::read_to_string(workspace_file("crates/varve/examples/traversal_bench.rs"))
        .expect("read traversal benchmark source");

    for required in [
        // opt-in switch: default stays uncompacted so results remain
        // comparable to the recorded baseline runs; the 1M evidence stage
        // selects it explicitly (bulk load → compact → serve).
        "VARVE_TRAVERSAL_COMPACT",
        // compaction must drain to idle (jobs == 0) with the FULL sweep —
        // standard compact_once leaves undersized L0 groups untouched…
        "compact_full_once(",
        "jobs == 0",
        // …and the stage must be timed and reported like every other stage.
        "compact jobs=",
    ] {
        assert!(
            source.contains(required),
            "post-ingest compaction stage missing {}",
            required
        );
    }
}

#[test]
fn object_store_transaction_benchmark_is_concurrent_and_s3_backed() {
    let source = fs::read_to_string(workspace_file(
        "crates/varve/examples/object_store_tx_bench.rs",
    ))
    .expect("read object-store transaction benchmark source");

    assert!(source.contains(r#"backend = \"object-store\""#));
    assert!(source.contains(r#"backend = \"s3\""#));
    assert!(source.contains("tokio::spawn"));
    assert!(source.contains("VARVE_S3_ENDPOINT"));
    assert!(source.contains("VARVE_S3_BUCKET"));
}

#[test]
fn object_store_transaction_benchmark_can_fill_group_commit_windows() {
    let source = fs::read_to_string(workspace_file(
        "crates/varve/examples/object_store_tx_bench.rs",
    ))
    .expect("read object-store transaction benchmark source");

    assert!(source.contains("const DEFAULT_TOTAL: u64 = 65_536;"));
    assert!(source.contains("const DEFAULT_WORKERS: u64 = 128;"));
}

#[test]
fn object_store_environment_parser_handles_absent_values_before_parsing() {
    let source = fs::read_to_string(workspace_file(
        "crates/varve/examples/object_store_tx_bench.rs",
    ))
    .expect("read object-store transaction benchmark source");

    assert!(
        !source.contains(".transpose()?"),
        "Result<Result<_, _>, VarError> cannot be transposed"
    );
    assert!(source.contains("std::env::VarError::NotPresent"));
}

#[test]
fn evidence_runner_preserves_raw_outputs_and_reproducibility_metadata() {
    let source = fs::read_to_string(workspace_file("scripts/varve_benchmark_run.sh"))
        .expect("read evidence benchmark runner");

    for required in [
        "/usr/bin/time -v",
        "exit_code.txt",
        "source-sha256.txt",
        "target/criterion",
        "object_store_tx_bench",
        "VARVE_TRAVERSAL_PEOPLE=1000000",
        "VARVE_TRAVERSAL_COMPACT=1",
    ] {
        assert!(source.contains(required), "runner missing {required}");
    }
}

#[test]
fn evidence_runner_times_compound_shell_commands() {
    let source = fs::read_to_string(workspace_file("scripts/varve_benchmark_run.sh"))
        .expect("read evidence benchmark runner");

    assert!(
        source.contains("exec /usr/bin/time -v bash -c"),
        "time must wrap a shell so assignments and pipelines remain valid"
    );
}
