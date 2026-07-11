#![allow(clippy::unwrap_used)]

use arrow::array::{Array, StringArray};
use varve::{Db, RecordBatch, TxReceipt};

async fn seed_people(db: &Db) -> TxReceipt {
    db.execute(
        "INSERT (:Person {_id: 1, name: 'Ada'}),
                (:Person {_id: 2, name: 'Bob'}),
                (:Person {_id: 3, name: 'Cy'}),
                (:Person {_id: 4, name: 'Dee'})",
    )
    .await
    .unwrap();
    db.execute(
        "MATCH (a:Person {_id: 1}), (b:Person {_id: 2})
         INSERT (a)-[:KNOWS]->(b)",
    )
    .await
    .unwrap();
    db.execute(
        "MATCH (b:Person {_id: 2}), (c:Person {_id: 3})
         INSERT (b)-[:KNOWS {since: 2020}]->(c)",
    )
    .await
    .unwrap()
}

fn string_rows(batches: &[RecordBatch], col: &str) -> Vec<String> {
    let mut rows = Vec::new();
    for batch in batches {
        let idx = batch.schema().column_with_name(col).unwrap().0;
        let array = batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for row in 0..array.len() {
            rows.push(array.value(row).to_string());
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn exists_filters_to_connected_nodes() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)
             WHERE EXISTS { (a)-[:KNOWS]->(:Person) }
             RETURN a.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Ada", "Bob"]);
}

#[tokio::test]
async fn not_exists_is_anti_join() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)
             WHERE NOT EXISTS { (a)-[:KNOWS]->(:Person) }
             RETURN a.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Cy", "Dee"]);
}

#[tokio::test]
async fn not_exists_with_empty_inner_pattern_keeps_outer_rows() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)
             WHERE NOT EXISTS { (a)-[:MISSING]->(:Person) }
             RETURN a.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Ada", "Bob", "Cy", "Dee"]);
}

#[tokio::test]
async fn not_exists_with_empty_zero_hop_shared_node_keeps_outer_rows() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)
             WHERE NOT EXISTS { (a:Missing) }
             RETURN a.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Ada", "Bob", "Cy", "Dee"]);
}

#[tokio::test]
async fn exists_with_inner_where() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)
             WHERE EXISTS { (a)-[r:KNOWS]->(:Person) WHERE r.since = 2020 }
             RETURN a.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Bob"]);
}

#[tokio::test]
async fn exists_quantified_path_can_share_start_node() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)
             WHERE EXISTS { (a)-[:KNOWS]->{1,2}(:Person) }
             RETURN a.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Ada", "Bob"]);
}

#[tokio::test]
async fn exists_quantified_path_can_share_end_node() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (b:Person)
             WHERE EXISTS { (:Person)-[:KNOWS]->{1,2}(b) }
             RETURN b.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Bob", "Cy"]);
}

#[tokio::test]
async fn filter_exists_after_match_uses_semi_join() {
    let db = Db::memory();
    seed_people(&db).await;

    let rows = db
        .query(
            "MATCH (a:Person)
             FILTER EXISTS { (a)-[:KNOWS]->(:Person) }
             RETURN a.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Ada", "Bob"]);
}

#[tokio::test]
async fn exists_under_or_rejected() {
    let db = Db::memory();
    seed_people(&db).await;

    let err = db
        .query(
            "MATCH (a:Person)
             WHERE a.name = 'Ada' OR EXISTS { (a)-[:KNOWS]->(:Person) }
             RETURN a.name AS name",
        )
        .await
        .unwrap_err();

    assert!(err.to_string().contains("EXISTS outside top-level"));
}

#[tokio::test]
async fn exists_no_shared_var_rejected() {
    let db = Db::memory();
    seed_people(&db).await;

    let err = db
        .query(
            "MATCH (a:Person)
             WHERE EXISTS { (:Person)-[:KNOWS]->(:Person) }
             RETURN a.name AS name",
        )
        .await
        .unwrap_err();

    assert!(err
        .to_string()
        .contains("EXISTS must share variable enclosing pattern"));
}

#[tokio::test]
async fn exists_respects_temporal_bounds() {
    let db = Db::memory();
    let before_delete = seed_people(&db).await.system_time;
    db.execute("MATCH (a:Person {_id: 1}) DETACH DELETE a")
        .await
        .unwrap();

    let earlier = db
        .query(format!(
            "MATCH (a:Person)
             FOR SYSTEM_TIME AS OF TIMESTAMP '{}'
             WHERE EXISTS {{ (a)-[:KNOWS]->(:Person) }}
             RETURN a.name AS name",
            before_delete
        ))
        .await
        .unwrap();
    let latest = db
        .query(
            "MATCH (a:Person)
             WHERE EXISTS { (a)-[:KNOWS]->(:Person) }
             RETURN a.name AS name",
        )
        .await
        .unwrap();

    assert_eq!(string_rows(&earlier, "name"), vec!["Ada", "Bob"]);
    assert_eq!(string_rows(&latest, "name"), vec!["Bob"]);
}

#[tokio::test]
async fn filter_exists_respects_enclosing_match_temporal_bounds() {
    let db = Db::memory();
    let before_delete = seed_people(&db).await.system_time;
    db.execute("MATCH (a:Person {_id: 1}) DETACH DELETE a")
        .await
        .unwrap();

    let rows = db
        .query(format!(
            "MATCH (a:Person)
             FOR SYSTEM_TIME AS OF TIMESTAMP '{}'
             FILTER EXISTS {{ (a)-[:KNOWS]->(:Person) }}
             RETURN a.name AS name",
            before_delete
        ))
        .await
        .unwrap();

    assert_eq!(string_rows(&rows, "name"), vec!["Ada", "Bob"]);
}
