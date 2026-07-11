#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns

use varve::{Db, EngineError, RecordBatch};

fn rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn seed_connected(db: &Db) -> varve::TxReceipt {
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})-[:KNOWS]->(:Person {_id: 2, name: 'Bob'})")
        .await
        .unwrap()
}

#[tokio::test]
async fn erase_hides_history_at_every_system_time() {
    let db = Db::memory();
    let inserted = db
        .execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();
    let updated = db
        .execute("MATCH (p:Person {_id: 1}) SET p.name = 'Adele'")
        .await
        .unwrap();

    db.execute("MATCH (p:Person {_id: 1}) ERASE p")
        .await
        .unwrap();

    for system_time in [inserted.system_time, updated.system_time] {
        let history = db
            .query(format!(
                "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Person {{_id: 1}}) RETURN p.name",
                system_time
            ))
            .await
            .unwrap();
        assert_eq!(rows(&history), 0);
    }

    let latest = db
        .query("MATCH (p:Person {_id: 1}) RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&latest), 0);
}

#[tokio::test]
async fn erase_connected_requires_detach() {
    let db = Db::memory();
    seed_connected(&db).await;

    let err = db
        .execute("MATCH (p:Person) WHERE p.name = 'Ada' ERASE p")
        .await
        .unwrap_err();
    assert!(
        matches!(err, EngineError::StillConnected(1)),
        "expected StillConnected, got {err:?}"
    );

    let ada = db
        .query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&ada), 1);
}

#[tokio::test]
async fn detach_erase_erases_incident_edges() {
    let db = Db::memory();
    let inserted = seed_connected(&db).await;

    db.execute("MATCH (p:Person) WHERE p.name = 'Ada' DETACH ERASE p")
        .await
        .unwrap();

    let old_edge = db
        .query(format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN b.name",
            inserted.system_time
        ))
        .await
        .unwrap();
    assert_eq!(rows(&old_edge), 0);

    let bob = db
        .query("MATCH (p:Person) WHERE p.name = 'Bob' RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&bob), 1);
}

#[tokio::test]
async fn erase_then_reinsert_same_id_is_fresh_entity() {
    let db = Db::memory();
    let first = db
        .execute("INSERT (:Person {_id: 1, name: 'Ada'})")
        .await
        .unwrap();

    let erased = db
        .execute("MATCH (p:Person {_id: 1}) ERASE p")
        .await
        .unwrap();
    db.execute("INSERT (:Person {_id: 1, name: 'Fresh'})")
        .await
        .unwrap();

    let old_history = db
        .query(format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Person {{_id: 1}}) RETURN p.name",
            first.system_time
        ))
        .await
        .unwrap();
    assert_eq!(rows(&old_history), 0);

    let after_erase = db
        .query(format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:Person {{_id: 1}}) RETURN p.name",
            erased.system_time
        ))
        .await
        .unwrap();
    assert_eq!(rows(&after_erase), 0);

    let fresh = db
        .query("MATCH (p:Person {_id: 1}) WHERE p.name = 'Fresh' RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows(&fresh), 1);
}
