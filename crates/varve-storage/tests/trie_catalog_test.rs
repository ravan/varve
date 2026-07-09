use bytes::Bytes;
use varve_storage::keys::manifest_key;
use varve_storage::{
    manifest_history, memory_store, BlockManifest, TableTries, TrieCatalog, TrieEntry, TrieState,
};

fn entry(key: &str) -> TrieEntry {
    TrieEntry {
        trie_key: key.to_string(),
        row_count: 1,
        data_len: 10,
    }
}

fn manifest(block_id: u64, tries: Vec<TrieEntry>) -> BlockManifest {
    BlockManifest {
        block_id,
        watermark: block_id,
        max_tx_id: block_id,
        max_system_time_us: block_id as i64,
        tables: vec![TableTries {
            graph: "default".into(),
            table: "nodes".into(),
            family: String::new(),
            tries,
        }],
    }
}

fn keys(entries: Vec<TrieEntry>) -> Vec<String> {
    entries.into_iter().map(|entry| entry.trie_key).collect()
}

#[tokio::test]
async fn manifest_history_is_sorted_by_block_id_and_ignores_strays() {
    let store = memory_store();
    for block_id in [2, 0, 1] {
        let m = manifest(block_id, vec![entry(&format!("l00-rc-b0{block_id}"))]);
        store
            .put(&manifest_key(block_id), Bytes::from(m.to_wire()))
            .await
            .unwrap();
    }
    store
        .put(
            "v1/blocks/not-a-manifest.tmp",
            Bytes::from_static(b"ignore"),
        )
        .await
        .unwrap();

    let history = manifest_history(store.as_ref()).await.unwrap();
    assert_eq!(
        history
            .iter()
            .map(|manifest| manifest.block_id)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
}

#[test]
fn catalog_marks_latest_inventory_live_and_superseded_garbage() {
    let catalog = TrieCatalog::from_manifests(&[
        manifest(0, vec![entry("l00-rc-b00")]),
        manifest(1, vec![entry("l01-rc-b01")]),
    ])
    .unwrap();

    assert_eq!(
        keys(catalog.live_for("default", "nodes", "")),
        vec!["l01-rc-b01"]
    );
    assert_eq!(
        catalog.state_for("default", "nodes", "", "l00-rc-b00"),
        Some(TrieState::Garbage)
    );
    assert_eq!(
        keys(
            catalog
                .garbage_entries()
                .into_iter()
                .map(|(_, _, _, entry)| entry)
                .collect()
        ),
        vec!["l00-rc-b00"]
    );
}

#[test]
fn catalog_groups_by_graph_table_family_and_shard() {
    let catalog = TrieCatalog::from_manifests(&[BlockManifest {
        block_id: 0,
        watermark: 0,
        max_tx_id: 0,
        max_system_time_us: 0,
        tables: vec![
            TableTries {
                graph: "default".into(),
                table: "nodes".into(),
                family: String::new(),
                tries: vec![entry("l00-rc-b00")],
            },
            TableTries {
                graph: "default".into(),
                table: "edges".into(),
                family: "adj-out".into(),
                tries: vec![entry("l00-rc-b01")],
            },
        ],
    }])
    .unwrap();

    assert_eq!(
        keys(catalog.live_for("default", "nodes", "")),
        vec!["l00-rc-b00"]
    );
    assert_eq!(
        keys(catalog.live_for("default", "edges", "adj-out")),
        vec!["l00-rc-b01"]
    );
    assert!(catalog.live_for("default", "edges", "").is_empty());
}

#[test]
fn l1_historical_is_nascent_until_matching_l1_current() {
    let historical_only =
        TrieCatalog::from_manifests(&[manifest(9, vec![entry("l01-r20200106-b09")])]).unwrap();
    assert_eq!(
        historical_only.state_for("default", "nodes", "", "l01-r20200106-b09"),
        Some(TrieState::Nascent)
    );

    let with_current = TrieCatalog::from_manifests(&[manifest(
        9,
        vec![entry("l01-r20200106-b09"), entry("l01-rc-b09")],
    )])
    .unwrap();
    assert_eq!(
        with_current.state_for("default", "nodes", "", "l01-r20200106-b09"),
        Some(TrieState::Live)
    );
}

#[test]
fn l2_partition_siblings_become_live_as_a_group() {
    let partial =
        TrieCatalog::from_manifests(&[manifest(4, vec![entry("l02-rc-p0-b04")])]).unwrap();
    assert_eq!(
        partial.state_for("default", "nodes", "", "l02-rc-p0-b04"),
        Some(TrieState::Nascent)
    );

    let full = TrieCatalog::from_manifests(&[manifest(
        4,
        vec![
            entry("l02-rc-p0-b04"),
            entry("l02-rc-p1-b04"),
            entry("l02-rc-p2-b04"),
            entry("l02-rc-p3-b04"),
        ],
    )])
    .unwrap();
    for key in [
        "l02-rc-p0-b04",
        "l02-rc-p1-b04",
        "l02-rc-p2-b04",
        "l02-rc-p3-b04",
    ] {
        assert_eq!(
            full.state_for("default", "nodes", "", key),
            Some(TrieState::Live)
        );
    }
}
