#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns
use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use varve::{Db, EngineError, Instant, RecordBatch};

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
        .query(&format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (e:Employee) RETURN e.salary AS salary",
            before.system_time
        ))
        .await
        .unwrap();
    assert_eq!(ints(&then, "salary"), vec![50000]);

    // And at the old system time, February 2026 had no known salary at all.
    let feb_then = db
        .query(&format!(
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
        .query(&format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Person) RETURN p.name AS name",
            ins.system_time
        ))
        .await
        .unwrap();
    assert_eq!(strings(&back, "name"), vec!["Zoe"]);
}

// Guard: inline props on a DELETE-matched node aren't filtered yet (task 7
// of slice 6 wires them into iids_from_snapshot). Until then the engine must
// refuse rather than silently deleting every node of the label — proving no
// data loss when the guard fires.
#[tokio::test]
async fn delete_with_inline_props_is_rejected_and_deletes_nothing() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'keep'})")
        .await
        .unwrap();
    db.execute("INSERT (:P {_id: 2, name: 'drop'})")
        .await
        .unwrap();

    let err = db
        .execute("MATCH (p:P {name: 'drop'}) DELETE p")
        .await
        .unwrap_err();
    assert!(
        matches!(err, EngineError::Unsupported(_)),
        "expected Unsupported, got {err:?}"
    );

    let batches = db.query("MATCH (p:P) RETURN p.name").await.unwrap();
    assert_eq!(rows(&batches), 2, "guard must not delete either node");
    assert_eq!(strings(&batches, "name"), vec!["drop", "keep"]);
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
