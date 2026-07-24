//! Whole-graph snapshot primitive (`Db::snapshot_all_nodes` /
//! `snapshot_all_edges`) that BI-5's `varve export --format ndjson` builds on.
//! Unlike the GQL query surface (an unlabelled `MATCH (n)` returns nothing),
//! this walks EVERY entity in the data graph regardless of label, at current
//! system+valid time, so a faithful bulk NDJSON export can be produced.
#![allow(clippy::unwrap_used)]

use varve::{Db, Doc, EdgePut, NodePut, Value};

fn node(id: &str, label: &str) -> NodePut {
    let mut doc = Doc::new();
    doc.insert("_id".to_string(), Value::Str(id.to_string()));
    NodePut {
        labels: vec![label.to_string()],
        doc,
        valid_from: None,
        valid_to: None,
    }
}

fn edge(label: &str, src: &str, dst: &str) -> EdgePut {
    EdgePut {
        label: label.to_string(),
        src: Value::Str(src.to_string()),
        dst: Value::Str(dst.to_string()),
        doc: Doc::new(),
        valid_from: None,
        valid_to: None,
    }
}

/// Every live node is returned regardless of its label — the label-blind scan
/// the GQL surface deliberately refuses — and the edge table carries endpoint
/// iids for the exporter to map back to `_id`s.
#[tokio::test]
async fn snapshot_all_returns_every_node_and_edge_across_labels() {
    let db = Db::memory();
    db.ingest(
        vec![node("a", "Alpha"), node("b", "Beta")],
        vec![edge("LINKS", "a", "b")],
    )
    .await
    .unwrap();

    let nodes = db
        .snapshot_all_nodes()
        .await
        .unwrap()
        .expect("two nodes were ingested");
    assert_eq!(nodes.num_rows(), 2, "both labelled nodes must be returned");
    assert!(
        nodes.schema().column_with_name("_labels").is_some(),
        "node snapshot must carry the _labels column"
    );
    assert!(
        nodes.schema().column_with_name("_id").is_some(),
        "node snapshot must carry the _id property column"
    );

    let edges = db
        .snapshot_all_edges()
        .await
        .unwrap()
        .expect("one edge was ingested");
    assert_eq!(edges.num_rows(), 1);
    assert!(
        edges.schema().column_with_name("_src_iid").is_some()
            && edges.schema().column_with_name("_dst_iid").is_some(),
        "edge snapshot must carry endpoint iid columns"
    );
}

/// An empty data graph snapshots to `None`, not an empty batch — so the
/// exporter writes zero records without touching a schema.
#[tokio::test]
async fn snapshot_all_is_none_on_an_empty_graph() {
    let db = Db::memory();
    assert!(db.snapshot_all_nodes().await.unwrap().is_none());
    assert!(db.snapshot_all_edges().await.unwrap().is_none());
}
