#![allow(clippy::unwrap_used)] // tests may use unwrap; crate-level allow covers helper fns
use varve::Db;

async fn seed(db: &Db) {
    db.execute("INSERT (:Person {_id: 1, name: 'Ada'})-[:KNOWS]->(:Person {_id: 2, name: 'Bob'})")
        .await
        .unwrap();
}

#[tokio::test]
async fn plain_delete_on_connected_node_fails_atomically() {
    let db = Db::memory();
    seed(&db).await;
    let err = db
        .execute("MATCH (p:Person) WHERE p.name = 'Ada' DELETE p")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("DETACH"));
    // Nothing was applied: Ada still visible.
    let rows = db
        .query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name")
        .await
        .unwrap();
    assert_eq!(rows.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
}

#[tokio::test]
async fn detach_delete_removes_node_and_incident_edges_in_one_tx() {
    let db = Db::memory();
    seed(&db).await;
    db.execute("MATCH (p:Person) WHERE p.name = 'Ada' DETACH DELETE p")
        .await
        .unwrap();
    let ada = db
        .query("MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.name")
        .await
        .unwrap();
    assert_eq!(ada.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
    // Bob survives, and the edge is gone: deleting Bob plainly now succeeds.
    db.execute("MATCH (p:Person) WHERE p.name = 'Bob' DELETE p")
        .await
        .unwrap();
}

#[tokio::test]
async fn detach_delete_handles_self_loops_and_unconnected_nodes() {
    let db = Db::memory();
    db.execute("INSERT (a:Person {_id: 7, name: 'Solo'}), (a)-[:LIKES]->(a)")
        .await
        .unwrap();
    db.execute("MATCH (p:Person) WHERE p.name = 'Solo' DETACH DELETE p")
        .await
        .unwrap();
    // Plain DELETE on an unconnected node keeps working.
    db.execute("INSERT (:Person {_id: 8, name: 'Free'})")
        .await
        .unwrap();
    db.execute("MATCH (p:Person) WHERE p.name = 'Free' DELETE p")
        .await
        .unwrap();
}
