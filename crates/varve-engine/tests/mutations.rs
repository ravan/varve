use varve_engine::Db;
use varve_types::Instant;

fn rows(batches: Vec<varve_engine::RecordBatch>) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

#[tokio::test]
async fn tx_system_time_is_strictly_increasing() {
    let db = Db::memory();
    let a = db.execute("INSERT (:X {_id: 1})").await.unwrap();
    let b = db.execute("INSERT (:X {_id: 2})").await.unwrap();
    assert!(b.system_time > a.system_time);
    assert!(a.system_time > Instant::from_micros(0));
}

#[tokio::test]
async fn insert_valid_from_is_visible_only_in_its_valid_range() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'Eve'}) VALID FROM DATE '2020-06-01'")
        .await
        .unwrap();

    // Default query (valid AS OF now, 2026+): visible.
    assert_eq!(
        rows(db.query("MATCH (p:P) RETURN p.name").await.unwrap()),
        1
    );
    // Before the valid range: invisible.
    assert_eq!(
        rows(
            db.query("FOR VALID_TIME AS OF DATE '2019-01-01' MATCH (p:P) RETURN p.name")
                .await
                .unwrap()
        ),
        0
    );
    // Inside the valid range: visible.
    assert_eq!(
        rows(
            db.query("FOR VALID_TIME AS OF DATE '2021-01-01' MATCH (p:P) RETURN p.name")
                .await
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn insert_with_inverted_computed_range_errors() {
    let db = Db::memory();
    // valid_from defaults to tx time (2026+), which lands AFTER the given VALID TO.
    let err = db
        .execute("INSERT (:P {_id: 1}) VALID TO DATE '2020-01-01'")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("VALID FROM"), "{err}");
}

#[tokio::test]
async fn delete_hides_now_but_not_in_the_past() {
    let db = Db::memory();
    db.execute("INSERT (:Person {_id: 1, name: 'Zoe'})")
        .await
        .unwrap();
    let amy = db
        .execute("INSERT (:Person {_id: 2, name: 'Amy'})")
        .await
        .unwrap();
    db.execute("MATCH (p:Person) WHERE p.name = 'Zoe' DELETE p")
        .await
        .unwrap();

    // Only the delete's target disappears.
    assert_eq!(
        rows(db.query("MATCH (p:Person) RETURN p.name").await.unwrap()),
        1
    );
    // Time travel to just before the delete (Amy's tx): both are visible.
    let time_travel = format!(
        "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Person) RETURN p.name",
        amy.system_time
    );
    assert_eq!(rows(db.query(&time_travel).await.unwrap()), 2);
}

#[tokio::test]
async fn delete_with_no_matches_is_an_empty_tx() {
    let db = Db::memory();
    let receipt = db.execute("MATCH (p:Nobody) DELETE p").await.unwrap();
    assert!(receipt.tx_id > 0);
}

#[tokio::test]
async fn delete_with_filtered_to_zero_matches_is_an_empty_tx() {
    let db = Db::memory();
    db.execute("INSERT (:P {_id: 1, name: 'Zoe'})")
        .await
        .unwrap();
    let receipt = db
        .execute("MATCH (p:P) WHERE p.name = 'Nobody' DELETE p")
        .await
        .unwrap();
    assert!(receipt.tx_id > 0);
}
