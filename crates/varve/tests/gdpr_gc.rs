#![allow(clippy::unwrap_used)]

use std::path::Path;
use varve::{Db, Instant};
use varve_testkit::db_harness::{
    compact_until_idle, local_gc_blocks_config as gc_config,
    local_gc_small_segment_config as small_segment_config, row_count as rows,
    wait_for_manifest_count,
};

async fn graph_object_bytes(dir: &Path) -> Vec<u8> {
    let store = varve_storage::local_store(&dir.join("store")).unwrap();
    let mut bytes = Vec::new();
    for key in store.list("v1/graphs").await.unwrap() {
        if key.ends_with(".arrow") {
            bytes.extend_from_slice(&store.get(&key).await.unwrap());
        }
    }
    bytes
}

async fn assert_reinsert_visible_and_history_hidden(
    db: &Db,
    inserted_system_time: Instant,
    erased_system_time: Instant,
    old_token: &str,
    fresh_token: &str,
) {
    let current = db
        .query("MATCH (p:P {_id: 1}) RETURN p.token")
        .await
        .unwrap();
    assert_eq!(rows(&current), 1);

    let fresh = db
        .query(format!(
            "MATCH (p:P {{_id: 1}}) WHERE p.token = '{fresh_token}' RETURN p.token"
        ))
        .await
        .unwrap();
    assert_eq!(rows(&fresh), 1);

    let old = db
        .query(format!(
            "MATCH (p:P {{_id: 1}}) WHERE p.token = '{old_token}' RETURN p.token"
        ))
        .await
        .unwrap();
    assert_eq!(rows(&old), 0);

    for system_time in [inserted_system_time, erased_system_time] {
        let history = db
            .query(format!(
                "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:P {{_id: 1}}) RETURN p.token",
                system_time
            ))
            .await
            .unwrap();
        assert_eq!(rows(&history), 0);
    }
}

#[tokio::test]
async fn erased_property_bytes_absent_after_compaction_and_gc() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(gc_config(dir.path(), 1)).await.unwrap();
    let secret = "gdpr-secret-sentinel-8f2f1de0";

    db.execute(&format!("INSERT (:P {{_id: 1, token: '{secret}'}})"))
        .await
        .unwrap();
    db.execute("MATCH (p:P {_id: 1}) ERASE p").await.unwrap();
    db.execute("INSERT (:P {_id: 1, token: 'fresh-public'})")
        .await
        .unwrap();
    for id in 2..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}, token: 'filler-{id}'}})"))
            .await
            .unwrap();
    }
    wait_for_manifest_count(dir.path(), 64).await;

    let before = graph_object_bytes(dir.path()).await;
    assert!(String::from_utf8_lossy(&before).contains(secret));

    let compact = db.compact_once().await.unwrap();
    assert_eq!(compact.jobs, 1);
    db.gc_once().await.unwrap();

    let after = graph_object_bytes(dir.path()).await;
    assert!(!String::from_utf8_lossy(&after).contains(secret));
    let current = db
        .query("MATCH (p:P {_id: 1}) WHERE p.token = 'fresh-public' RETURN p.token")
        .await
        .unwrap();
    assert_eq!(rows(&current), 1);
}

#[tokio::test]
async fn post_erase_reinsert_survives_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(gc_config(dir.path(), 1)).await.unwrap();
    let old_token = "gdpr-reinsert-secret-sentinel-49181ba4";
    let fresh_token = "gdpr-reinsert-fresh-public";

    let inserted = db
        .execute(&format!("INSERT (:P {{_id: 1, token: '{old_token}'}})"))
        .await
        .unwrap();
    let erased = db.execute("MATCH (p:P {_id: 1}) ERASE p").await.unwrap();
    db.execute(&format!("INSERT (:P {{_id: 1, token: '{fresh_token}'}})"))
        .await
        .unwrap();
    for id in 2..=62 {
        db.execute(&format!("INSERT (:P {{_id: {id}, token: 'filler-{id}'}})"))
            .await
            .unwrap();
    }
    wait_for_manifest_count(dir.path(), 64).await;

    assert_reinsert_visible_and_history_hidden(
        &db,
        inserted.system_time,
        erased.system_time,
        old_token,
        fresh_token,
    )
    .await;

    let jobs = compact_until_idle(&db).await.unwrap();
    assert!(jobs >= 1);

    assert_reinsert_visible_and_history_hidden(
        &db,
        inserted.system_time,
        erased.system_time,
        old_token,
        fresh_token,
    )
    .await;

    drop(db);
    let restarted = Db::open(gc_config(dir.path(), 1)).await.unwrap();
    assert_reinsert_visible_and_history_hidden(
        &restarted,
        inserted.system_time,
        erased.system_time,
        old_token,
        fresh_token,
    )
    .await;
}

fn all_disk_bytes(root: &Path) -> Vec<u8> {
    fn visit(dir: &Path, out: &mut Vec<u8>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(&path, out);
            } else {
                out.extend_from_slice(&std::fs::read(&path).unwrap());
            }
        }
    }
    let mut out = Vec::new();
    visit(root, &mut out);
    out
}

fn contains(haystack: &[u8], needle: &str) -> bool {
    haystack
        .windows(needle.len())
        .any(|w| w == needle.as_bytes())
}

#[tokio::test]
async fn erased_bytes_absent_from_every_stored_object_and_log_segment() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(small_segment_config(dir.path(), 1)).await.unwrap();
    let secret = "gdpr-fullscan-sentinel-3c1d9a77";

    let inserted = db
        .execute(&format!("INSERT (:P {{_id: 1, token: '{secret}'}})"))
        .await
        .unwrap();
    let erased = db.execute("MATCH (p:P {_id: 1}) ERASE p").await.unwrap();
    // The nodes-table L0 compaction trigger (`LOG_LIMIT`) fires only once a
    // scope accumulates at least 64 L0 tries; with `max_block_rows == 1` each
    // statement below is its own trie, so we need 62 fillers to bring the
    // insert + erase + fillers total to 64.
    for id in 2..=63 {
        db.execute(&format!("INSERT (:P {{_id: {id}, token: 'filler-{id}'}})"))
            .await
            .unwrap();
    }
    wait_for_manifest_count(dir.path(), 64).await;

    // Non-vacuous: the sentinel IS on disk before compaction+GC (in a log
    // segment and/or an L0 block).
    assert!(contains(&all_disk_bytes(dir.path()), secret));

    compact_until_idle(&db).await.unwrap();
    db.gc_once().await.unwrap();

    // THE slice exit assertion: no stored byte anywhere still spells the secret.
    assert!(!contains(&all_disk_bytes(dir.path()), secret));

    // Invisibility on every time axis survives compaction…
    for gql in [
        "MATCH (p:P {_id: 1}) RETURN p.token".to_string(),
        format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:P {{_id: 1}}) RETURN p.token",
            inserted.system_time
        ),
        format!(
            "FOR SYSTEM_TIME AS OF TIMESTAMP '{}' MATCH (p:P {{_id: 1}}) RETURN p.token",
            erased.system_time
        ),
    ] {
        assert_eq!(
            rows(&db.query(&gql).await.unwrap()),
            0,
            "visible via: {gql}"
        );
    }

    // …and restart.
    drop(db);
    let db = Db::open(small_segment_config(dir.path(), 1)).await.unwrap();
    assert!(!contains(&all_disk_bytes(dir.path()), secret));
    assert_eq!(
        rows(
            &db.query("MATCH (p:P {_id: 1}) RETURN p.token")
                .await
                .unwrap()
        ),
        0
    );
    assert_eq!(
        rows(&db.query("MATCH (p:P) RETURN p.token").await.unwrap()),
        62 // the fillers survive untouched
    );
}

#[tokio::test]
async fn detach_erase_scrubs_edge_property_bytes_too() {
    let dir = tempfile::tempdir().unwrap();
    let db = Db::open(small_segment_config(dir.path(), 1)).await.unwrap();
    let node_secret = "gdpr-node-sentinel-91b2e6f0";
    let edge_secret = "gdpr-edge-sentinel-5a44c8d2";

    db.execute(&format!(
        "INSERT (:P {{_id: 1, token: '{node_secret}'}}), (:P {{_id: 2, token: 'keep'}})"
    ))
    .await
    .unwrap();
    db.execute(&format!(
        "MATCH (a:P {{_id: 1}}), (b:P {{_id: 2}}) \
         INSERT (a)-[:KNOWS {{note: '{edge_secret}'}}]->(b)"
    ))
    .await
    .unwrap();
    db.execute("MATCH (p:P {_id: 1}) DETACH ERASE p")
        .await
        .unwrap();
    // The edges table (and adjacency families) are separate compaction
    // scopes from the nodes table, each gated behind the same `LOG_LIMIT`
    // (64) L0-tries-per-scope trigger. Filler nodes alone only pad the
    // nodes scope, so pad the edges scope too with filler `:LINK` edges
    // (a distinct label so they never satisfy the final `:KNOWS` query).
    for id in 3..=74 {
        db.execute(&format!("INSERT (:P {{_id: {id}}})"))
            .await
            .unwrap();
    }
    for id in 3..=74 {
        db.execute(&format!(
            "MATCH (a:P {{_id: 2}}), (b:P {{_id: {id}}}) INSERT (a)-[:LINK]->(b)"
        ))
        .await
        .unwrap();
    }
    wait_for_manifest_count(dir.path(), 147).await;
    assert!(contains(&all_disk_bytes(dir.path()), edge_secret));

    compact_until_idle(&db).await.unwrap();
    db.gc_once().await.unwrap();

    let bytes = all_disk_bytes(dir.path());
    assert!(!contains(&bytes, node_secret));
    assert!(!contains(&bytes, edge_secret)); // adjacency families scrubbed too
    assert_eq!(
        rows(
            &db.query("MATCH (:P)-[k:KNOWS]->(:P) RETURN k.note")
                .await
                .unwrap()
        ),
        0
    );
}
