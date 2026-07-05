use arrow::array::StringArray;
use varve::Db;

#[tokio::test]
async fn insert_then_match_end_to_end() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 2, name: 'Bob'})")
        .await
        .unwrap();
    db.execute("INSERT (:City {_id: 3, name: 'Oslo'})")
        .await
        .unwrap();

    let batches = db
        .query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name AS name")
        .await
        .unwrap();

    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1);
    let names: &StringArray = batches[0]
        .column_by_name("name")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    assert_eq!(names.value(0), "Ada");
}

#[tokio::test]
async fn tx_ids_are_monotonic() {
    let db = Db::memory();
    let a = db.execute("INSERT (:X {_id: 1})").await.unwrap();
    let b = db.execute("INSERT (:X {_id: 2})").await.unwrap();
    assert!(b.tx_id > a.tx_id);
}

#[tokio::test]
async fn query_via_execute_is_error_and_vice_versa() {
    let db = Db::memory();
    assert!(db.execute("MATCH (p:P) RETURN p.x").await.is_err());
    assert!(db.query("INSERT (:P {_id: 1})").await.is_err());
}

#[tokio::test]
async fn multi_node_insert_is_atomic_on_invalid_id() {
    let db = Db::memory();
    assert!(db
        .execute("INSERT (:A {_id: 1}), (:B {_id: 2.5})")
        .await
        .is_err());

    let batches = db.query("MATCH (a:A) RETURN a._id").await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0);
}

#[tokio::test]
async fn probe_capabilities_reports_through_the_facade() {
    use varve::ProbeVerdict;
    let db = Db::memory();
    // Db::memory wraps an InMemory store in the cache — Supported proves
    // both the blanket conditional impl and the CachedStore delegation.
    let report = db.probe_capabilities().await.unwrap();
    assert_eq!(report.verdict, ProbeVerdict::Supported);
    assert!(
        report.probe_key.starts_with("v1/probe/"),
        "{}",
        report.probe_key
    );
}
