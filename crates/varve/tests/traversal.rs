#![allow(clippy::unwrap_used)]
//! Multi-element MATCH traversal (slice 6, task 8): pattern lowering to
//! per-element mangled scans + left-deep hash joins. The e2e results here are
//! the contract for `varve_plan::pattern`.
use datafusion::arrow::array::Array;
use std::path::Path;
use varve::{Config, Db};

async fn seed_triangle(db: &Db) {
    // ada -KNOWS-> bob -KNOWS-> cy;  ada -KNOWS-> cy
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'}), (:Person {_id: 2, name: 'Bob'}), (:Person {_id: 3, name: 'Cy'})")
        .await.unwrap();
    db.execute(
        "MATCH (a:Person {_id: 1}), (b:Person {_id: 2}) INSERT (a)-[:KNOWS {since: 2020}]->(b)",
    )
    .await
    .unwrap();
    db.execute(
        "MATCH (a:Person {_id: 2}), (b:Person {_id: 3}) INSERT (a)-[:KNOWS {since: 2021}]->(b)",
    )
    .await
    .unwrap();
    db.execute(
        "MATCH (a:Person {_id: 1}), (b:Person {_id: 3}) INSERT (a)-[:KNOWS {since: 2022}]->(b)",
    )
    .await
    .unwrap();
}

fn names(batches: &[varve::RecordBatch], col: &str) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let idx = b.schema().column_with_name(col).unwrap().0;
        let arr = b
            .column(idx)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..arr.len() {
            out.push(arr.value(i).to_string());
        }
    }
    out.sort();
    out
}

/// Zips two string columns row-by-row (within and across batches), then
/// sorts the resulting pairs. Unlike sorting each column independently
/// (which loses the row-to-row correspondence), this keeps each row's two
/// values paired, so it can catch a join that mismatches column `a` against
/// the wrong row of column `b`.
fn string_pairs(batches: &[varve::RecordBatch], col_a: &str, col_b: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for b in batches {
        let ia = b.schema().column_with_name(col_a).unwrap().0;
        let ib = b.schema().column_with_name(col_b).unwrap().0;
        let arr_a = b
            .column(ia)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let arr_b = b
            .column(ib)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((arr_a.value(i).to_string(), arr_b.value(i).to_string()));
        }
    }
    out.sort();
    out
}

/// Same row-pairing contract as [`string_pairs`], for a string column
/// zipped against an int64 column (e.g. a node's name against an edge
/// property).
fn string_int_pairs(
    batches: &[varve::RecordBatch],
    str_col: &str,
    int_col: &str,
) -> Vec<(String, i64)> {
    let mut out = Vec::new();
    for b in batches {
        let is_ = b.schema().column_with_name(str_col).unwrap().0;
        let ii = b.schema().column_with_name(int_col).unwrap().0;
        let arr_s = b
            .column(is_)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::StringArray>()
            .unwrap();
        let arr_i = b
            .column(ii)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            out.push((arr_s.value(i).to_string(), arr_i.value(i)));
        }
    }
    out.sort();
    out
}

#[tokio::test]
async fn single_hop_join() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Ada' RETURN b.name")
        .await
        .unwrap();
    assert_eq!(
        names(&rows, "name"),
        vec!["Bob".to_string(), "Cy".to_string()]
    );
}

#[tokio::test]
async fn two_hop_friend_of_friend() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.name = 'Ada' RETURN c.name")
        .await
        .unwrap();
    assert_eq!(names(&rows, "name"), vec!["Cy".to_string()]);
}

#[tokio::test]
async fn reverse_direction_and_edge_props() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (b:Person)<-[:KNOWS {since: 2020}]-(a:Person) RETURN b.name")
        .await
        .unwrap();
    assert_eq!(names(&rows, "name"), vec!["Bob".to_string()]);
}

#[tokio::test]
async fn node_inline_props_filter_scans() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (a:Person {name: 'Ada'})-[:KNOWS]->(b:Person) RETURN b.name AS friend")
        .await
        .unwrap();
    assert_eq!(
        names(&rows, "friend"),
        vec!["Bob".to_string(), "Cy".to_string()]
    );
}

#[tokio::test]
async fn return_from_multiple_vars_and_edge_var() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let rows = db
        .query("MATCH (a:Person)-[k:KNOWS]->(b:Person) WHERE a.name = 'Ada' RETURN a.name AS a, b.name AS b, k.since AS since")
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
    assert!(names(&rows, "a").iter().all(|n| n == "Ada"));
    // Ada -KNOWS(2020)-> Bob and Ada -KNOWS(2022)-> Cy (per `seed_triangle`).
    // Checking `b` and `k.since` independently would pass even if the join
    // scrambled which `since` goes with which `b`; zipping them per-row
    // guards the actual edge-property <-> node pairing.
    assert_eq!(
        string_int_pairs(&rows, "b", "since"),
        vec![("Bob".to_string(), 2020), ("Cy".to_string(), 2022)]
    );
}

#[tokio::test]
async fn backward_join_chain_on_asymmetric_label_sizes() {
    let db = Db::memory();
    // 3 `:Person` nodes vs. 1 `:Company` node: the raw per-element snapshot
    // counts (pre-predicate, pre-join) are unequal here, unlike every other
    // e2e test in this file (all triangles use same-size label pairs). Only
    // Ada and Bob work at Acme; Cy does not.
    db.execute(
        "INSERT (:Person {_id: 1, name: 'Ada'}), (:Person {_id: 2, name: 'Bob'}), (:Person {_id: 3, name: 'Cy'})",
    )
    .await
    .unwrap();
    db.execute("INSERT (:Company {_id: 4, name: 'Acme'})")
        .await
        .unwrap();
    db.execute("MATCH (a:Person {_id: 1}), (b:Company {_id: 4}) INSERT (a)-[:WORKS_AT]->(b)")
        .await
        .unwrap();
    db.execute("MATCH (a:Person {_id: 2}), (b:Company {_id: 4}) INSERT (a)-[:WORKS_AT]->(b)")
        .await
        .unwrap();

    // `join_chain`'s size heuristic is `forward = has_expand ||
    // row_counts.first() <= row_counts.last()`. This query has no `Expand`
    // (no quantifier), and `row_counts` are the raw per-element snapshot
    // sizes taken before any predicate is applied: `first` is the
    // `:Person` snapshot (3 rows), `last` is the `:Company` snapshot
    // (1 row). Since 3 > 1, `forward` is false, so `join_chain` takes the
    // *backward* branch (accumulator seeded from the last node, walked
    // right-to-left) — otherwise unexercised by this suite.
    let rows = db
        .query("MATCH (a:Person)-[:WORKS_AT]->(b:Company) RETURN a.name AS a, b.name AS b")
        .await
        .unwrap();
    assert_eq!(
        string_pairs(&rows, "a", "b"),
        vec![
            ("Ada".to_string(), "Acme".to_string()),
            ("Bob".to_string(), "Acme".to_string()),
        ]
    );
}

#[tokio::test]
async fn traversal_respects_temporal_bounds() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'a'}), (:P {_id: 2, name: 'b'})")
        .await
        .unwrap();
    db.execute(
        "MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:K]->(b) VALID FROM TIMESTAMP '2030-01-01T00:00:00Z'",
    )
    .await
    .unwrap();
    // Edge not valid yet at current valid time:
    let now_rows = db
        .query("MATCH (a:P)-[:K]->(b:P) RETURN b.name")
        .await
        .unwrap();
    assert_eq!(now_rows.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    let then_rows = db
        .query("FOR VALID_TIME AS OF TIMESTAMP '2031-01-01T00:00:00Z' MATCH (a:P)-[:K]->(b:P) RETURN b.name")
        .await
        .unwrap();
    assert_eq!(then_rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn unknown_variable_in_return_errors() {
    let db = Db::memory();
    seed_triangle(&db).await;
    let err = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN z.name")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("z"));
}

// ---- flushed-and-restarted traversal ------------------------------------

fn toml_escaped(dir: &Path) -> String {
    format!("{:?}", dir.display().to_string())
}

/// log + storage both local under `dir`, tiny block threshold so seeding
/// actually flushes edges into persisted blocks, 1 ms group-commit window.
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
async fn two_hop_traversal_survives_flush_and_restart() {
    let dir = tempfile::tempdir().unwrap();
    {
        // Threshold 4 flushes the first node batch + one edge into block 0,
        // leaving the remaining edges live — the two-hop join must span both
        // the persisted blocks and the live tail after a restart.
        let db = Db::open(blocks_config(dir.path(), 4)).await.unwrap();
        seed_triangle(&db).await;
        wait_for_flush(dir.path()).await;
    }
    let db = Db::local(dir.path()).await.unwrap();
    let rows = db
        .query("MATCH (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(c:Person) WHERE a.name = 'Ada' RETURN c.name")
        .await
        .unwrap();
    assert_eq!(names(&rows, "name"), vec!["Cy".to_string()]);
}
