#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns
use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use varve::{Db, Doc, EdgePut, EngineError, Instant, NodePut, RecordBatch, Value};

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

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

fn ints(batches: &[RecordBatch], col: &str) -> Vec<i64> {
    let mut out: Vec<i64> = batches
        .iter()
        .flat_map(|b| {
            let a: &Int64Array = b
                .column_by_name(col)
                .unwrap()
                .as_any()
                .downcast_ref()
                .unwrap();
            (0..a.len()).map(|i| a.value(i)).collect::<Vec<_>>()
        })
        .collect();
    out.sort();
    out
}

// Scenario 1 — as-of past valid time: Ada moves city in 2024; a 2022 query
// still finds her in London.
#[tokio::test]
async fn valid_time_travel_sees_the_old_version() {
    let db = Db::memory();
    db.execute(
        "INSERT (:Person {_id: 1, name: 'Ada', city: 'London'}) VALID FROM DATE '2020-01-01'",
    )
    .await
    .unwrap();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada', city: 'Oslo'}) VALID FROM DATE '2024-01-01'")
        .await
        .unwrap();

    let current = db
        .query("MATCH (p:Person) RETURN p.city AS city")
        .await
        .unwrap();
    assert_eq!(rows(&current), 1);
    assert_eq!(strings(&current, "city"), vec!["Oslo"]);

    let past = db
        .query("FOR VALID_TIME AS OF DATE '2022-06-01' MATCH (p:Person) RETURN p.city AS city")
        .await
        .unwrap();
    assert_eq!(strings(&past, "city"), vec!["London"]);
}

// Scenarios 2 + 3 — a retroactive correction changes the past, but the old
// belief remains reachable at the old system time.
#[tokio::test]
async fn retroactive_correction_is_system_time_dependent() {
    let db = Db::memory();
    let before = db
        .execute("INSERT (:Employee {_id: 7, salary: 50000})")
        .await
        .unwrap();
    // Correction backdated to Jan 2026 — before the original insert's valid_from.
    db.execute("INSERT (:Employee {_id: 7, salary: 55000}) VALID FROM DATE '2026-01-01'")
        .await
        .unwrap();

    // New system time (default): the correction won.
    let now = db
        .query("MATCH (e:Employee) RETURN e.salary AS salary")
        .await
        .unwrap();
    assert_eq!(ints(&now, "salary"), vec![55000]);

    // Old system time: we still see what we believed then.
    let then = db
        .query(format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (e:Employee) RETURN e.salary AS salary",
            before.system_time
        ))
        .await
        .unwrap();
    assert_eq!(ints(&then, "salary"), vec![50000]);

    // And at the old system time, February 2026 had no known salary at all.
    let feb_then = db
        .query(format!(
            "FOR VALID_TIME AS OF DATE '2026-02-01' FOR SYSTEM_TIME AS OF TIMESTAMP '{}' \
             MATCH (e:Employee) RETURN e.salary AS salary",
            before.system_time
        ))
        .await
        .unwrap();
    assert_eq!(rows(&feb_then), 0);
}

// Scenario 4 — delete, then time travel to before the delete.
#[tokio::test]
async fn delete_then_as_of_before_the_delete() {
    let db = Db::memory();
    let ins = db
        .execute("INSERT (:Person {_id: 9, name: 'Zoe'})")
        .await
        .unwrap();
    db.execute("MATCH (p:Person) WHERE p.name = 'Zoe' DELETE p")
        .await
        .unwrap();

    assert_eq!(
        rows(&db.query("MATCH (p:Person) RETURN p.name").await.unwrap()),
        0
    );
    let back = db
        .query(format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Person) RETURN p.name AS name",
            ins.system_time
        ))
        .await
        .unwrap();
    assert_eq!(strings(&back, "name"), vec!["Zoe"]);
}

// Task 7 of slice 6 wires inline props on a DELETE-matched node into
// iids_from_snapshot (ANDed with WHERE, same as MATCH…INSERT's pattern
// props): they filter exactly like an equivalent WHERE clause would,
// superseding the earlier reject-outright guard.
#[tokio::test]
async fn delete_with_inline_props_filters_like_where() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'keep'})")
        .await
        .unwrap();
    db.execute("INSERT (:P {_id: 2, name: 'drop'})")
        .await
        .unwrap();

    db.execute("MATCH (p:P {name: 'drop'}) DELETE p")
        .await
        .unwrap();

    let batches = db.query("MATCH (p:P) RETURN p.name AS name").await.unwrap();
    assert_eq!(rows(&batches), 1, "only the matching node was deleted");
    assert_eq!(strings(&batches, "name"), vec!["keep"]);
}

#[tokio::test]
async fn same_tx_batch_on_one_entity_is_last_write_wins() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 5, v: 1}), (:P {_id: 5, v: 2})")
        .await
        .unwrap();
    let batches = db.query("MATCH (p:P) RETURN p.v AS v").await.unwrap();
    assert_eq!(rows(&batches), 1);
    assert_eq!(ints(&batches, "v"), vec![2]);
}

#[tokio::test]
async fn temporal_functions_expose_version_metadata() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 3, name: 'Eve'}) VALID FROM TIMESTAMP '2021-03-04T05:06:07Z'")
        .await
        .unwrap();
    let batches = db
        .query("MATCH (p:P) RETURN p.name AS name, valid_from(p) AS vf, valid_to(p) AS vt, system_from(p) AS sf")
        .await
        .unwrap();
    let batch = &batches[0];
    let vf: &TimestampMicrosecondArray = batch
        .column_by_name("vf")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let vt: &TimestampMicrosecondArray = batch
        .column_by_name("vt")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let sf: &TimestampMicrosecondArray = batch
        .column_by_name("sf")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(
        vf.value(0),
        Instant::parse_rfc3339("2021-03-04T05:06:07Z")
            .unwrap()
            .as_micros()
    );
    assert_eq!(vt.value(0), Instant::END_OF_TIME.as_micros());
    assert!(sf.value(0) > 0);
}

#[tokio::test]
async fn for_valid_time_all_returns_every_version() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, city: 'London'}) VALID FROM DATE '2020-01-01'")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 1, city: 'Oslo'}) VALID FROM DATE '2024-01-01'")
        .await
        .unwrap();
    let all = db
        .query("FOR VALID_TIME ALL MATCH (p:Person) RETURN p.city AS city")
        .await
        .unwrap();
    // At the current system time the valid axis holds London [2020, 2024) then Oslo [2024, ∞).
    assert_eq!(strings(&all, "city"), vec!["London", "Oslo"]);
}

// ---------------------------------------------------------------------------
// Range-form temporal clauses over a multi-element pattern
// (docs/plans/2026-07-29-interval-results.md, tasks 0, 1a, 1–3).
//
// A range window tests each matched element against the window independently;
// without a cross-element intersection, a version of `a` valid only in January
// could be reported as joined to an edge valid only in December — a path that
// never simultaneously existed. Each store below is built through `Db::ingest`
// because it is the only write path that can set a distinct valid interval per
// element (GQL `MATCH … INSERT` reads current state, so it cannot attach an
// edge to a node version that is not valid now).
//
// Task 0 ran these stores against the ranged query and recorded the wrong
// answer each one produced; the observation is quoted in every test below.
// Task 1a turned those wrong answers into a refusal. Tasks 1–2 made the joins
// coincident (a final `max(from) < min(to)` filter per ranged axis, and
// walk-interval pruning inside quantified expansion), so each test now asserts
// the coincident answer. The refusal survives only for OPTIONAL MATCH and
// EXISTS under a range window — the two shapes whose own join must carry the
// predicate.
// ---------------------------------------------------------------------------

fn svc(id: i64, name: &str, from: &str, to: &str) -> NodePut {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    doc.insert("name".to_string(), Value::Str(name.to_string()));
    NodePut {
        labels: vec!["Service".to_string()],
        doc,
        valid_from: Some(Instant::parse_date(from).unwrap()),
        valid_to: Some(Instant::parse_date(to).unwrap()),
    }
}

fn calls(src: i64, dst: i64, from: &str, to: &str) -> EdgePut {
    EdgePut {
        label: "CALLS".to_string(),
        src: Value::Int(src),
        dst: Value::Int(dst),
        doc: Doc::new(),
        valid_from: Some(Instant::parse_date(from).unwrap()),
        valid_to: Some(Instant::parse_date(to).unwrap()),
    }
}

const YEAR_2021: &str = "FOR VALID_TIME BETWEEN DATE '2021-01-01' AND DATE '2021-12-31'";

/// The surviving task-1a refusal (OPTIONAL MATCH / EXISTS under a range
/// window): refused at plan time, naming the shape and the ranged axis so a
/// mixed-axis query is not misdiagnosed.
fn expect_refusal(result: Result<Vec<RecordBatch>, EngineError>, shape: &str, axes: &str) {
    let message = match result {
        Ok(batches) => panic!(
            "expected a plan error, got {} rows — a range window over {shape} \
             must not answer silently",
            rows(&batches)
        ),
        Err(err) => err.to_string(),
    };
    assert!(
        message.contains(axes) && message.contains(shape),
        "refusal must name the shape ({shape}) and the ranged axis ({axes}), got: {message}"
    );
}

#[tokio::test]
async fn two_hop_range_does_not_join_non_coincident_versions() {
    let db = Db::memory();
    // a and b coexist through January; the edge between them exists only in
    // December. No instant holds all three.
    db.ingest(
        vec![
            svc(1, "a", "2021-01-01", "2021-02-01"),
            svc(2, "b", "2021-01-01", "2021-02-01"),
        ],
        vec![calls(1, 2, "2021-12-01", "2021-12-31")],
    )
    .await
    .unwrap();

    // Task 0 observed 1 row here. The coincident answer is 0 rows: the edge and
    // its endpoints were never simultaneously valid.
    let batches = db
        .query(format!(
            "{YEAR_2021} MATCH (a:Service)-[:CALLS]->(b:Service) \
             RETURN a.name AS a_name, b.name AS b_name"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows(&batches),
        0,
        "a January node must not join a December edge"
    );
}

#[tokio::test]
async fn three_element_coincidence_is_not_pairwise() {
    let db = Db::memory();
    // [1,3), [2,5), [4,6) in months: each *adjacent* pair overlaps, but the
    // outer pair is disjoint, so the triple shares no instant. A join chain
    // only compares adjacent frames, so a fix that filters just the two frames
    // at each join accepts this row; the running intersection must be carried
    // through the chain.
    db.ingest(
        vec![
            svc(1, "a", "2021-01-01", "2021-03-01"),
            svc(2, "b", "2021-04-01", "2021-06-01"),
        ],
        vec![calls(1, 2, "2021-02-01", "2021-05-01")],
    )
    .await
    .unwrap();

    // Task 0 observed 1 row here too. The coincident answer is 0 rows: adjacent
    // overlap is not coincidence, and `a` ends before `b` begins. This is the
    // case a per-adjacent-join filter wrongly accepts — the intersection must
    // be global (plan §2.1).
    let batches = db
        .query(format!(
            "{YEAR_2021} MATCH (a:Service)-[:CALLS]->(b:Service) \
             RETURN a.name AS a_name, b.name AS b_name"
        ))
        .await
        .unwrap();
    assert_eq!(rows(&batches), 0, "adjacent overlap is not coincidence");
}

// Not in the plan's task 0 list, and found while scoping task 1a: the defect is
// per-element, not per-hop. A comma pattern binds two elements with no edge
// between them and pairs their versions just as wrongly, which is why the guard
// counts elements rather than hops.
#[tokio::test]
async fn comma_pattern_range_does_not_pair_non_coincident_versions() {
    let db = Db::memory();
    db.ingest(
        vec![
            svc(1, "a", "2021-01-01", "2021-02-01"),
            svc(2, "b", "2021-11-01", "2021-12-01"),
        ],
        Vec::new(),
    )
    .await
    .unwrap();

    // Task 0 observed 1 row: a January `a` paired with a November `b`. The
    // coincident answer is 0 rows.
    let batches = db
        .query(format!(
            "{YEAR_2021} MATCH (a:Service {{_id: 1}}), (b:Service {{_id: 2}}) \
             RETURN a.name AS a_name, b.name AS b_name"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows(&batches),
        0,
        "a January version must not pair with a November one"
    );
}

#[tokio::test]
async fn quantified_hop_range_does_not_chain_non_coincident_edges() {
    let db = Db::memory();
    // All three nodes span 2021. 1→2 exists only in January, 2→3 only in
    // November, so the two-hop walk 1→2→3 never existed as a path even though
    // each of its edges falls inside the window.
    db.ingest(
        vec![
            svc(1, "a", "2021-01-01", "2022-01-01"),
            svc(2, "b", "2021-01-01", "2022-01-01"),
            svc(3, "c", "2021-01-01", "2022-01-01"),
        ],
        vec![
            calls(1, 2, "2021-01-01", "2021-02-01"),
            calls(2, 3, "2021-11-01", "2021-12-01"),
        ],
    )
    .await
    .unwrap();

    // Task 0 observed ["b", "c"]. The coincident answer is ["b"]: only the
    // one-hop path ever held, since 1→2→3 chains January to November — the
    // walk's interval empties at the frontier and the path is pruned (task 2).
    let batches = db
        .query(format!(
            "{YEAR_2021} MATCH (a:Service {{_id: 1}})-[:CALLS]->{{1,3}}(c:Service) \
             RETURN c.name AS c_name"
        ))
        .await
        .unwrap();
    assert_eq!(
        strings(&batches, "c_name"),
        vec!["b"],
        "a January edge must not chain to a November edge"
    );
}

/// The mixed case from §2.1a: a point on the valid axis and a range on the
/// system axis is coincident on the system axis only — "were these facts ever
/// simultaneously *known*". One ingest batch gives every element the same
/// system interval, so the hop holds; the valid axis stays the point default.
#[tokio::test]
async fn system_time_range_over_a_hop_is_coincident_on_the_system_axis() {
    let db = Db::memory();
    db.ingest(
        vec![
            svc(1, "a", "2021-01-01", "2100-01-01"),
            svc(2, "b", "2021-01-01", "2100-01-01"),
        ],
        vec![calls(1, 2, "2021-01-01", "2100-01-01")],
    )
    .await
    .unwrap();

    let batches = db
        .query(
            "FOR SYSTEM_TIME ALL MATCH (a:Service)-[:CALLS]->(b:Service) \
             RETURN a.name AS a_name",
        )
        .await
        .unwrap();
    assert_eq!(strings(&batches, "a_name"), vec!["a"]);
}

/// Task 3: the nullary `coincide_*` projections read the match's coincidence
/// interval — `max(valid_from)`/`min(valid_to)` over every bound element. The
/// store's intervals `[Jan,Jun) ∩ [Feb,May) ∩ [Mar,Dec)` intersect to
/// `[Mar,May)`, and the row survives the range window because that
/// intersection is non-empty.
#[tokio::test]
async fn coincide_projections_return_the_match_interval() {
    let db = Db::memory();
    db.ingest(
        vec![
            svc(1, "a", "2021-01-01", "2021-06-01"),
            svc(2, "b", "2021-03-01", "2021-12-01"),
        ],
        vec![calls(1, 2, "2021-02-01", "2021-05-01")],
    )
    .await
    .unwrap();

    let batches = db
        .query(format!(
            "{YEAR_2021} MATCH (a:Service)-[:CALLS]->(b:Service) \
             RETURN b.name AS name, coincide_valid_from() AS since, coincide_valid_to() AS until"
        ))
        .await
        .unwrap();
    assert_eq!(strings(&batches, "name"), vec!["b"]);
    let micros = |col: &str| -> i64 {
        let array: &TimestampMicrosecondArray = batches[0]
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        array.value(0)
    };
    assert_eq!(
        micros("since"),
        Instant::parse_date("2021-03-01").unwrap().as_micros(),
        "the interval opens when the last element becomes valid"
    );
    assert_eq!(
        micros("until"),
        Instant::parse_date("2021-05-01").unwrap().as_micros(),
        "the interval closes when the first element expires"
    );
}

/// §2.1's second property: one pair can be coincident over several disjoint
/// intervals — two versions of `a` each intersect the edge, so the match is
/// two rows, each carrying its own interval, none merged (coalescing is task
/// 4, deliberately unbuilt).
#[tokio::test]
async fn disjoint_version_coincidence_stays_two_rows() {
    let db = Db::memory();
    db.ingest(
        vec![
            svc(1, "a-v1", "2021-01-01", "2021-03-01"),
            svc(1, "a-v2", "2021-04-01", "2021-06-01"),
            svc(2, "b", "2021-01-01", "2021-12-01"),
        ],
        vec![calls(1, 2, "2021-02-01", "2021-05-01")],
    )
    .await
    .unwrap();

    let batches = db
        .query(format!(
            "{YEAR_2021} MATCH (a:Service)-[:CALLS]->(b:Service) \
             RETURN a.name AS name, coincide_valid_from() AS since, coincide_valid_to() AS until \
             ORDER BY since"
        ))
        .await
        .unwrap();
    assert_eq!(strings(&batches, "name"), vec!["a-v1", "a-v2"]);
    let micros = |col: &str, row: usize| -> i64 {
        let array: &TimestampMicrosecondArray = batches[0]
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        array.value(row)
    };
    let date = |d: &str| Instant::parse_date(d).unwrap().as_micros();
    assert_eq!(
        (micros("since", 0), micros("until", 0)),
        (date("2021-02-01"), date("2021-03-01")),
        "v1's interval is [Feb, Mar)"
    );
    assert_eq!(
        (micros("since", 1), micros("until", 1)),
        (date("2021-04-01"), date("2021-05-01")),
        "v2's interval is [Apr, May)"
    );
}

/// Chained `MATCH` clauses share `b`, and under a range window the shared var
/// joins on version identity while coincidence spans all five elements: the
/// January and November edges never coexisted, so no row — while the control
/// store with overlapping edges answers.
#[tokio::test]
async fn chained_match_range_is_coincident_across_clauses() {
    let disjoint = Db::memory();
    disjoint
        .ingest(
            vec![
                svc(1, "a", "2021-01-01", "2022-01-01"),
                svc(2, "b", "2021-01-01", "2022-01-01"),
                svc(3, "c", "2021-01-01", "2022-01-01"),
            ],
            vec![
                calls(1, 2, "2021-01-01", "2021-02-01"),
                calls(2, 3, "2021-11-01", "2021-12-01"),
            ],
        )
        .await
        .unwrap();
    let gql = format!(
        "{YEAR_2021} MATCH (a:Service {{_id: 1}})-[:CALLS]->(b:Service) \
         MATCH (b)-[:CALLS]->(c:Service) RETURN c.name AS name"
    );
    let batches = disjoint.query(&gql).await.unwrap();
    assert_eq!(
        rows(&batches),
        0,
        "January and November edges never chained"
    );

    let overlapping = Db::memory();
    overlapping
        .ingest(
            vec![
                svc(1, "a", "2021-01-01", "2022-01-01"),
                svc(2, "b", "2021-01-01", "2022-01-01"),
                svc(3, "c", "2021-01-01", "2022-01-01"),
            ],
            vec![
                calls(1, 2, "2021-01-01", "2021-06-01"),
                calls(2, 3, "2021-05-01", "2021-12-01"),
            ],
        )
        .await
        .unwrap();
    let batches = overlapping.query(&gql).await.unwrap();
    assert_eq!(strings(&batches, "name"), vec!["c"], "May overlap chains");
}

/// Task 3's companion: `system_to(v)` projects the derived `_system_to` — a
/// live, never-superseded row reads as end-of-time.
#[tokio::test]
async fn system_to_projects_the_derived_upper_bound() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();
    let batches = db
        .query("MATCH (p:Person) RETURN system_to(p) AS st")
        .await
        .unwrap();
    let st: &TimestampMicrosecondArray = batches[0]
        .column_by_name("st")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(st.value(0), Instant::END_OF_TIME.as_micros());
}

/// What remains of task 1a: OPTIONAL MATCH and EXISTS under a range window are
/// refused — their joins must carry the coincidence predicate themselves, and
/// until they do, a silent wrong answer stays a loud refusal.
#[tokio::test]
async fn optional_and_exists_under_a_range_window_are_still_refused() {
    let db = Db::memory();
    db.ingest(
        vec![
            svc(1, "a", "2021-01-01", "2022-01-01"),
            svc(2, "b", "2021-01-01", "2022-01-01"),
        ],
        vec![calls(1, 2, "2021-01-01", "2022-01-01")],
    )
    .await
    .unwrap();

    expect_refusal(
        db.query(format!(
            "{YEAR_2021} MATCH (a:Service) OPTIONAL MATCH (a)-[:CALLS]->(b:Service) \
             RETURN a.name AS a_name"
        ))
        .await,
        "OPTIONAL MATCH",
        "FOR VALID_TIME",
    );
    expect_refusal(
        db.query(
            "FOR SYSTEM_TIME ALL MATCH (a:Service) \
             WHERE EXISTS { (a)-[:CALLS]->(b:Service) } RETURN a.name AS a_name",
        )
        .await,
        "EXISTS",
        "FOR SYSTEM_TIME",
    );
}

/// The other half of the guard: a point window on both axes is exempt, so every
/// ordinary query — including multi-hop ones — is untouched. `AS OF` is the only
/// temporal form any shipped query uses (§2.1a), so this is the case that must
/// not regress.
#[tokio::test]
async fn as_of_over_a_hop_is_unaffected() {
    let db = Db::memory();
    db.ingest(
        vec![
            svc(1, "a", "2021-01-01", "2022-01-01"),
            svc(2, "b", "2021-01-01", "2022-01-01"),
        ],
        vec![calls(1, 2, "2021-06-01", "2021-07-01")],
    )
    .await
    .unwrap();

    let inside = db
        .query(
            "FOR VALID_TIME AS OF DATE '2021-06-15' \
             MATCH (a:Service)-[:CALLS]->(b:Service) RETURN b.name AS name",
        )
        .await
        .unwrap();
    assert_eq!(strings(&inside, "name"), vec!["b"]);

    let outside = db
        .query(
            "FOR VALID_TIME AS OF DATE '2021-09-15' \
             MATCH (a:Service)-[:CALLS]->(b:Service) RETURN b.name AS name",
        )
        .await
        .unwrap();
    assert_eq!(rows(&outside), 0, "the edge is not valid in September");
}

/// A range window over a single element intersects nothing, so it stays legal —
/// this is what `for_valid_time_all_returns_every_version` relies on, restated
/// here as the boundary of the guard rather than as version-travel coverage.
#[tokio::test]
async fn range_over_a_single_element_is_still_allowed() {
    let db = Db::memory();
    db.ingest(
        vec![
            svc(1, "a", "2021-01-01", "2021-02-01"),
            svc(2, "b", "2021-11-01", "2021-12-01"),
        ],
        Vec::new(),
    )
    .await
    .unwrap();

    let batches = db
        .query(format!(
            "{YEAR_2021} MATCH (s:Service) RETURN s.name AS name"
        ))
        .await
        .unwrap();
    assert_eq!(strings(&batches, "name"), vec!["a", "b"]);
}

/// `DELETE … VALID FROM <dt>` ends a fact at a chosen valid time instead of
/// at the tx's system time. The MATCH still reads current state; only the
/// tombstone is placed in the past. Three views must agree: valid time
/// before the cut still sees the edge, valid time after it does not, and
/// system time before the delete tx sees it at every valid time.
#[tokio::test]
async fn delete_valid_from_ends_fact_at_chosen_valid_time() {
    let db = Db::memory();
    // Endpoints must be valid wherever the edge is probed, so backdate them too.
    db.execute(
        "INSERT (:P {_id: 1, name: 'a'}), (:P {_id: 2, name: 'b'})          VALID FROM TIMESTAMP '2020-01-01T00:00:00Z'",
    )
    .await
    .unwrap();
    let inserted = db
        .execute(
            "MATCH (a:P {_id: 1}), (b:P {_id: 2}) INSERT (a)-[:K]->(b) \
             VALID FROM TIMESTAMP '2020-01-01T00:00:00Z'",
        )
        .await
        .unwrap();
    db.execute("MATCH (a:P)-[e:K]->(b:P) DELETE e VALID FROM TIMESTAMP '2024-06-01T00:00:00Z'")
        .await
        .unwrap();

    // Current state (system now, valid now): gone.
    let now = db
        .query("MATCH (a:P)-[:K]->(b:P) RETURN b.name AS name")
        .await
        .unwrap();
    assert_eq!(rows(&now), 0);
    // Valid time before the cut: still true.
    let before = db
        .query(
            "FOR VALID_TIME AS OF TIMESTAMP '2023-01-01T00:00:00Z' \
             MATCH (a:P)-[:K]->(b:P) RETURN b.name AS name",
        )
        .await
        .unwrap();
    assert_eq!(strings(&before, "name"), vec!["b".to_string()]);
    // Valid time after the cut: gone.
    let after = db
        .query(
            "FOR VALID_TIME AS OF TIMESTAMP '2024-06-01T00:00:00Z' \
             MATCH (a:P)-[:K]->(b:P) RETURN b.name AS name",
        )
        .await
        .unwrap();
    assert_eq!(rows(&after), 0);
    // System time before the delete tx: the edge is still known, at a valid
    // time the delete would otherwise cover.
    let known_then = db
        .query(format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' FOR VALID_TIME AS OF TIMESTAMP '2025-01-01T00:00:00Z' \
             MATCH (a:P)-[:K]->(b:P) RETURN b.name AS name",
            inserted.system_time
        ))
        .await
        .unwrap();
    assert_eq!(strings(&known_then, "name"), vec!["b".to_string()]);
    // The cut is visible through the temporal projection of the surviving version.
    let bounds = db
        .query(
            "FOR VALID_TIME AS OF TIMESTAMP '2023-01-01T00:00:00Z' \
             MATCH (a:P)-[e:K]->(b:P) RETURN valid_to(e) AS until",
        )
        .await
        .unwrap();
    let until: &TimestampMicrosecondArray = bounds[0]
        .column_by_name("until")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(
        until.value(0),
        Instant::parse_rfc3339("2024-06-01T00:00:00Z")
            .unwrap()
            .as_micros()
    );
}

/// `DELETE … VALID FROM x TO y` removes a fact for a bounded window only;
/// it holds again after `y`.
#[tokio::test]
async fn delete_valid_window_leaves_fact_true_outside_it() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'a'}) VALID FROM TIMESTAMP '2020-01-01T00:00:00Z'")
        .await
        .unwrap();
    db.execute(
        "MATCH (p:P) DELETE p VALID FROM TIMESTAMP '2021-01-01T00:00:00Z' \
         TO TIMESTAMP '2022-01-01T00:00:00Z'",
    )
    .await
    .unwrap();
    for (at, expect) in [
        ("2020-06-01T00:00:00Z", 1),
        ("2021-06-01T00:00:00Z", 0),
        ("2022-06-01T00:00:00Z", 1),
    ] {
        let got = db
            .query(format!(
                "FOR VALID_TIME AS OF TIMESTAMP '{at}' MATCH (p:P) RETURN p.name AS name"
            ))
            .await
            .unwrap();
        assert_eq!(rows(&got), expect, "valid time {at}");
    }
    // Current state: the window is over, so the node is back.
    let now = db.query("MATCH (p:P) RETURN p.name AS name").await.unwrap();
    assert_eq!(rows(&now), 1);
}

/// A `DELETE … VALID FROM` at or after the tx's system-time default with a
/// `TO` before it is rejected by the engine, mirroring `INSERT`.
#[tokio::test]
async fn delete_valid_to_before_default_from_is_rejected() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1})").await.unwrap();
    // valid_from defaults to tx time (2026+) which lands AFTER VALID TO.
    let err = db
        .execute("MATCH (p:P) DELETE p VALID TO TIMESTAMP '2000-01-01T00:00:00Z'")
        .await
        .unwrap_err();
    assert!(
        matches!(err, EngineError::InvalidValidRange { .. }),
        "{err:?}"
    );
}
