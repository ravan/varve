use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use varve_storage::keys::{self, manifest_block_id, manifest_key};
use varve_storage::{
    manifest_history, BlockManifest, ObjectStore, ScopedTrieKey, StorageError, PROBE_PREFIX,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GcConfig {
    pub enabled: bool,
    pub blocks_to_keep: u64,
    pub garbage_lifetime_us: i64,
}

impl Default for GcConfig {
    fn default() -> GcConfig {
        GcConfig {
            enabled: false,
            blocks_to_keep: 10,
            garbage_lifetime_us: 24 * 60 * 60 * 1_000_000,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct GcPlan {
    pub delete_keys: Vec<String>,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    pub planned_objects: usize,
    pub deleted_objects: usize,
}

pub(crate) async fn execute_gc(
    store: &Arc<dyn ObjectStore>,
    config: &GcConfig,
) -> Result<GcReport, StorageError> {
    if !config.enabled {
        return Ok(GcReport::default());
    }

    let manifests = manifest_history(store.as_ref()).await?;
    let mut listed_keys = store.list("v1/graphs").await?;
    listed_keys.extend(store.list(keys::MANIFEST_PREFIX).await?);
    let plan = plan_gc(&manifests, &listed_keys, config);
    let planned_objects = plan.delete_keys.len();
    let mut deleted_objects = 0;
    for key in plan.delete_keys {
        store.delete(&key).await?;
        deleted_objects += 1;
    }
    Ok(GcReport {
        planned_objects,
        deleted_objects,
    })
}

pub(crate) fn plan_gc(
    manifests: &[BlockManifest],
    listed_keys: &[String],
    config: &GcConfig,
) -> GcPlan {
    if !config.enabled {
        return GcPlan::default();
    }

    let mut protected = BTreeSet::new();
    let mut sorted_manifests: Vec<_> = manifests.iter().collect();
    sorted_manifests.sort_by_key(|manifest| manifest.block_id);

    if let Some(latest) = sorted_manifests.last().copied() {
        let retain_from = latest.block_id.saturating_sub(config.blocks_to_keep);
        for manifest in &sorted_manifests {
            if manifest.block_id >= retain_from {
                protected.insert(manifest_key(manifest.block_id));
                protect_manifest_entries(manifest, &mut protected);
            }
        }
        protect_unexpired_garbage(
            &sorted_manifests,
            latest.max_system_time_us,
            config,
            &mut protected,
        );
    }

    let mut listed = listed_keys.to_vec();
    listed.sort();
    listed.dedup();

    let delete_keys = listed
        .into_iter()
        .filter(|key| should_delete_key(key, &protected, &sorted_manifests, config))
        .collect();
    GcPlan { delete_keys }
}

fn protect_unexpired_garbage(
    manifests: &[&BlockManifest],
    latest_time_us: i64,
    config: &GcConfig,
    protected: &mut BTreeSet<String>,
) {
    let Some(latest) = manifests.last().copied() else {
        return;
    };
    let latest_entries: BTreeSet<_> = latest
        .trie_entries()
        .map(|entry| entry.scoped_trie_key())
        .collect();
    let mut last_present = BTreeMap::new();
    for (idx, manifest) in manifests.iter().enumerate() {
        for entry in manifest.trie_entries().map(|entry| entry.scoped_trie_key()) {
            last_present.insert(entry, idx);
        }
    }

    for (entry, idx) in last_present {
        if latest_entries.contains(&entry) {
            continue;
        }
        let Some(garbage_manifest) = manifests.get(idx + 1) else {
            continue;
        };
        let expires_at = garbage_manifest
            .max_system_time_us
            .saturating_add(config.garbage_lifetime_us);
        if expires_at > latest_time_us {
            protect_entry(&entry, protected);
        }
    }
}

fn protect_manifest_entries(manifest: &BlockManifest, protected: &mut BTreeSet<String>) {
    for entry in manifest.trie_entries().map(|entry| entry.scoped_trie_key()) {
        protect_entry(&entry, protected);
    }
}

fn protect_entry(entry: &ScopedTrieKey, protected: &mut BTreeSet<String>) {
    protected.insert(entry.data_key());
    protected.insert(entry.meta_key());
}

fn should_delete_key(
    key: &str,
    protected: &BTreeSet<String>,
    manifests: &[&BlockManifest],
    config: &GcConfig,
) -> bool {
    if protected.contains(key)
        || has_path_prefix(key, keys::LOG_PREFIX)
        || has_path_prefix(key, PROBE_PREFIX)
    {
        return false;
    }
    if let Some(block_id) = manifest_block_id(key) {
        return manifests.last().is_some_and(|latest| {
            block_id < latest.block_id.saturating_sub(config.blocks_to_keep)
        });
    }
    is_graph_data_or_meta_key(key)
}

fn has_path_prefix(key: &str, prefix: &str) -> bool {
    key == prefix
        || key
            .strip_prefix(prefix)
            .is_some_and(|remaining| remaining.starts_with('/'))
}

fn is_graph_data_or_meta_key(key: &str) -> bool {
    let parts: Vec<_> = key.split('/').collect();
    match parts.as_slice() {
        ["v1", "graphs", graph, "tables", table, kind, object] => {
            !graph.is_empty()
                && !table.is_empty()
                && matches!(*kind, "data" | "meta")
                && arrow_object_name(object)
        }
        ["v1", "graphs", graph, "tables", table, family, kind, object] => {
            !graph.is_empty()
                && !table.is_empty()
                && !family.is_empty()
                && matches!(*kind, "data" | "meta")
                && arrow_object_name(object)
        }
        _ => false,
    }
}

fn arrow_object_name(object: &str) -> bool {
    object
        .strip_suffix(".arrow")
        .is_some_and(|stem| !stem.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_storage::keys::{
        adj_data_key, adj_meta_key, data_key, log_key, manifest_key, meta_key, ADJ_OUT,
    };
    use varve_storage::{BlockManifest, TableTries, TrieEntry, PROBE_PREFIX};
    use varve_types::LogPosition;

    fn entry(trie_key: &str) -> TrieEntry {
        TrieEntry {
            trie_key: trie_key.to_string(),
            row_count: 1,
            data_len: 8,
        }
    }

    fn manifest(block_id: u64, max_system_time_us: i64, tries: Vec<&str>) -> BlockManifest {
        BlockManifest {
            block_id,
            watermark: block_id,
            max_tx_id: block_id,
            max_system_time_us,
            tables: vec![TableTries {
                graph: "default".to_string(),
                table: "nodes".to_string(),
                family: String::new(),
                tries: tries.into_iter().map(entry).collect(),
            }],
        }
    }

    fn edge_manifest(block_id: u64, max_system_time_us: i64, tries: Vec<&str>) -> BlockManifest {
        BlockManifest {
            block_id,
            watermark: block_id,
            max_tx_id: block_id,
            max_system_time_us,
            tables: vec![TableTries {
                graph: "default".to_string(),
                table: "edges".to_string(),
                family: ADJ_OUT.to_string(),
                tries: tries.into_iter().map(entry).collect(),
            }],
        }
    }

    fn enabled_config() -> GcConfig {
        GcConfig {
            enabled: true,
            blocks_to_keep: 0,
            garbage_lifetime_us: 0,
        }
    }

    fn plan(manifests: &[BlockManifest], listed_keys: Vec<String>, config: GcConfig) -> GcPlan {
        plan_gc(manifests, &listed_keys, &config)
    }

    #[test]
    fn gc_plan_keeps_objects_referenced_by_retained_manifests() {
        let manifests = vec![
            manifest(8, 80, vec!["l00-rc-b08"]),
            manifest(9, 90, vec!["l00-rc-b09"]),
            manifest(10, 100, vec!["l00-rc-b10"]),
        ];
        let mut config = enabled_config();
        config.blocks_to_keep = 1;
        let listed = vec![
            data_key("default", "nodes", "l00-rc-b09"),
            meta_key("default", "nodes", "l00-rc-b09"),
            data_key("default", "nodes", "l00-rc-b10"),
            meta_key("default", "nodes", "l00-rc-b10"),
            data_key("default", "nodes", "l00-rc-b08"),
            meta_key("default", "nodes", "l00-rc-b08"),
        ];

        let plan = plan(&manifests, listed, config);

        assert!(!plan
            .delete_keys
            .contains(&data_key("default", "nodes", "l00-rc-b09")));
        assert!(!plan
            .delete_keys
            .contains(&meta_key("default", "nodes", "l00-rc-b09")));
        assert!(!plan
            .delete_keys
            .contains(&data_key("default", "nodes", "l00-rc-b10")));
        assert!(plan
            .delete_keys
            .contains(&data_key("default", "nodes", "l00-rc-b08")));
    }

    #[test]
    fn gc_plan_deletes_orphan_data_meta_pairs() {
        let manifests = vec![manifest(10, 100, vec!["l00-rc-b10"])];
        let listed = vec![
            data_key("default", "nodes", "l00-rc-b10"),
            meta_key("default", "nodes", "l00-rc-b10"),
            data_key("default", "nodes", "l00-rc-b09"),
            meta_key("default", "nodes", "l00-rc-b09"),
        ];

        let plan = plan(&manifests, listed, enabled_config());

        assert_eq!(
            plan.delete_keys,
            vec![
                data_key("default", "nodes", "l00-rc-b09"),
                meta_key("default", "nodes", "l00-rc-b09"),
            ]
        );
    }

    #[test]
    fn gc_plan_deletes_old_unretained_manifests() {
        let manifests = vec![
            manifest(7, 70, vec!["l00-rc-b07"]),
            manifest(8, 80, vec!["l00-rc-b08"]),
            manifest(9, 90, vec!["l00-rc-b09"]),
        ];
        let mut config = enabled_config();
        config.blocks_to_keep = 1;
        let listed = vec![manifest_key(7), manifest_key(8), manifest_key(9)];

        let plan = plan(&manifests, listed, config);

        assert_eq!(plan.delete_keys, vec![manifest_key(7)]);
    }

    #[test]
    fn gc_plan_respects_blocks_to_keep_and_garbage_lifetime() {
        let manifests = vec![
            manifest(7, 700, vec!["l00-rc-b07"]),
            manifest(8, 800, vec!["l00-rc-b08"]),
            manifest(9, 900, vec!["l00-rc-b09"]),
        ];
        let listed = vec![
            data_key("default", "nodes", "l00-rc-b07"),
            meta_key("default", "nodes", "l00-rc-b07"),
            data_key("default", "nodes", "l00-rc-b08"),
            meta_key("default", "nodes", "l00-rc-b08"),
        ];
        let protected_by_lifetime = GcConfig {
            enabled: true,
            blocks_to_keep: 0,
            garbage_lifetime_us: 150,
        };
        let expired = GcConfig {
            enabled: true,
            blocks_to_keep: 0,
            garbage_lifetime_us: 50,
        };

        let protected = plan(&manifests, listed.clone(), protected_by_lifetime);
        let expired = plan(&manifests, listed, expired);

        assert!(protected.delete_keys.is_empty());
        assert_eq!(
            expired.delete_keys,
            vec![
                data_key("default", "nodes", "l00-rc-b07"),
                meta_key("default", "nodes", "l00-rc-b07"),
            ]
        );
    }

    #[test]
    fn gc_plan_keeps_probe_and_log_objects_out_of_scope() {
        let manifests = vec![edge_manifest(10, 100, vec!["l00-rc-b10"])];
        let log_position = LogPosition::new(0, 9).unwrap();
        let listed = vec![
            adj_data_key("default", "edges", ADJ_OUT, "l00-rc-b10"),
            adj_meta_key("default", "edges", ADJ_OUT, "l00-rc-b10"),
            adj_data_key("default", "edges", ADJ_OUT, "l00-rc-b09"),
            adj_meta_key("default", "edges", ADJ_OUT, "l00-rc-b09"),
            log_key(log_position),
            format!("{PROBE_PREFIX}/stale"),
        ];

        let plan = plan(&manifests, listed, enabled_config());

        assert_eq!(
            plan.delete_keys,
            vec![
                adj_data_key("default", "edges", ADJ_OUT, "l00-rc-b09"),
                adj_meta_key("default", "edges", ADJ_OUT, "l00-rc-b09"),
            ]
        );
    }
}
