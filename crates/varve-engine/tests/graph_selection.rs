//! Silt-0a tasks 2 and 3: a caller can name the target graph beside the GQL
//! text (`Query::graph`, `Db::execute_as_in`, `Db::ingest_in_as`) instead
//! of a `USE` prefix. The two must not disagree.
#![allow(clippy::unwrap_used)]

use std::collections::BTreeMap;
use varve_engine::{Db, EngineError, NodePut, RecordBatch};
use varve_types::{Doc, Value};

fn rows(batches: Vec<RecordBatch>) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

fn params() -> BTreeMap<String, Value> {
    BTreeMap::new()
}

fn person(id: i64) -> NodePut {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Int(id));
    NodePut {
        labels: vec!["P".to_string()],
        doc,
        valid_from: None,
        valid_to: None,
    }
}

async fn two_graphs() -> Db {
    let db = Db::memory();
    db.execute("CREATE GRAPH a").await.unwrap();
    db.execute("CREATE GRAPH b").await.unwrap();
    db
}

#[tokio::test]
async fn query_graph_reads_only_that_graph() {
    let db = two_graphs().await;
    db.execute("USE a; INSERT (:P {_id: 1})").await.unwrap();
    db.execute("INSERT (:P {_id: 2}), (:P {_id: 3})")
        .await
        .unwrap();
    let in_a = db
        .query("MATCH (p:P) RETURN p._id")
        .graph("a")
        .await
        .unwrap();
    assert_eq!(rows(in_a), 1);
    let in_default = db.query("MATCH (p:P) RETURN p._id").await.unwrap();
    assert_eq!(rows(in_default), 2);
    let in_b = db
        .query("MATCH (p:P) RETURN p._id")
        .graph("b")
        .await
        .unwrap();
    assert_eq!(rows(in_b), 0);
}

#[tokio::test]
async fn query_graph_with_use_is_a_conflict() {
    let db = two_graphs().await;
    let err = db
        .query("USE b; MATCH (p:P) RETURN p._id")
        .graph("a")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::GraphConflict { field, use_graph } if field == "a" && use_graph == "b"),
        "{err}"
    );
    assert!(err.to_string().contains("graph given twice"), "{err}");
}

#[tokio::test]
async fn query_graph_unknown_and_reserved_names_error() {
    let db = two_graphs().await;
    let err = db
        .query("MATCH (p:P) RETURN p._id")
        .graph("nope")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::UnknownGraph(g) if g == "nope"),
        "{err}"
    );
    let err = db
        .query("MATCH (p:P) RETURN p._id")
        .graph("__meta")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Unsupported(_)), "{err}");
}

#[tokio::test]
async fn execute_as_in_inserts_into_the_named_graph() {
    let db = two_graphs().await;
    let receipt = db
        .execute_as_in(Some("a"), "INSERT (:P {_id: 1})", &params(), "ada")
        .await
        .unwrap();
    assert_eq!(receipt.user, "ada");
    assert_eq!(receipt.side_effects.nodes_created, 1);
    assert_eq!(
        rows(db.query("USE a; MATCH (p:P) RETURN p._id").await.unwrap()),
        1
    );
    assert_eq!(rows(db.query("MATCH (p:P) RETURN p._id").await.unwrap()), 0);

    // `None` keeps today's rule: program `USE`, else the default graph.
    db.execute_as_in(None, "INSERT (:P {_id: 2})", &params(), "ada")
        .await
        .unwrap();
    db.execute_as_in(None, "USE b; INSERT (:P {_id: 3})", &params(), "ada")
        .await
        .unwrap();
    assert_eq!(rows(db.query("MATCH (p:P) RETURN p._id").await.unwrap()), 1);
    assert_eq!(
        rows(db.query("USE b; MATCH (p:P) RETURN p._id").await.unwrap()),
        1
    );
}

#[tokio::test]
async fn execute_as_in_rejects_conflict_and_unknown_graph_before_the_writer() {
    let db = two_graphs().await;
    let err = db
        .execute_as_in(Some("a"), "USE b; INSERT (:P {_id: 1})", &params(), "ada")
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::GraphConflict { .. }), "{err}");
    let err = db
        .execute_as_in(Some("nope"), "INSERT (:P {_id: 1})", &params(), "ada")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::UnknownGraph(g) if g == "nope"),
        "{err}"
    );
    let err = db
        .try_execute_as_in(Some("nope"), "INSERT (:P {_id: 1})", &params(), "ada")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::UnknownGraph(g) if g == "nope"),
        "{err}"
    );
    // Nothing reached the writer: the next commit gets the next tx id after
    // the two CREATE GRAPH statements.
    let receipt = db
        .try_execute_as_in(Some("a"), "INSERT (:P {_id: 1})", &params(), "ada")
        .await
        .unwrap();
    assert_eq!(receipt.tx_id, 3);
    assert_eq!(receipt.user, "ada");
}

#[tokio::test]
async fn ingest_in_as_targets_the_named_graph() {
    let db = two_graphs().await;
    let receipt = db
        .ingest_in_as("b", "ada", vec![person(1), person(2)], Vec::new())
        .await
        .unwrap();
    assert_eq!(receipt.user, "ada");
    assert_eq!(receipt.side_effects.nodes_created, 2);
    assert_eq!(
        rows(
            db.query("MATCH (n:P) RETURN n._id")
                .graph("a")
                .await
                .unwrap()
        ),
        0
    );
    assert_eq!(
        rows(
            db.query("MATCH (n:P) RETURN n._id")
                .graph("b")
                .await
                .unwrap()
        ),
        2
    );
    assert_eq!(rows(db.query("MATCH (n:P) RETURN n._id").await.unwrap()), 0);
}

#[tokio::test]
async fn ingest_in_as_rejects_unknown_and_reserved_graphs() {
    let db = two_graphs().await;
    let err = db
        .ingest_in_as("nope", "ada", vec![person(1)], Vec::new())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, EngineError::UnknownGraph(g) if g == "nope"),
        "{err}"
    );
    let err = db
        .ingest_in_as("__meta", "ada", vec![person(1)], Vec::new())
        .await
        .unwrap_err();
    assert!(matches!(err, EngineError::Unsupported(_)), "{err}");
}
