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
    listed_keys.extend(store.list(keys::LOG_PREFIX).await?);
    listed_keys.extend(store.list(PROBE_PREFIX).await?);
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

    let mut min_retained_watermark: Option<u64> = None;
    if let Some(latest) = sorted_manifests.last().copied() {
        let retain_from = latest.block_id.saturating_sub(config.blocks_to_keep);
        for manifest in &sorted_manifests {
            if manifest.block_id >= retain_from {
                protected.insert(manifest_key(manifest.block_id));
                protect_manifest_entries(manifest, &mut protected);
                min_retained_watermark = Some(match min_retained_watermark {
                    Some(current) => current.min(manifest.watermark),
                    None => manifest.watermark,
                });
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

    let (log_keys, other_keys): (Vec<String>, Vec<String>) = listed
        .into_iter()
        .partition(|key| has_path_prefix(key, keys::LOG_PREFIX));

    let mut delete_keys: Vec<String> = other_keys
        .into_iter()
        .filter(|key| should_delete_key(key, &protected, &sorted_manifests, config))
        .collect();
    delete_keys.extend(deletable_log_keys(&log_keys, min_retained_watermark));
    delete_keys.sort();
    GcPlan { delete_keys }
}

/// Log objects wholly below the minimum retained manifest watermark are
/// deletable garbage; the LAST object (by packed first-position) is never
/// deletable because its span is open-ended.
///
/// Every listed key under `LOG_PREFIX` is parsed with `parse_log_key`;
/// unparseable keys are foreign and never touched. The remaining objects are
/// sorted by packed first-position. A log object spans
/// `[first_i, first_{i+1})`; object *i* is deletable iff
/// `first_{i+1}.as_u64() <= min_retained_watermark`. `min_retained_watermark`
/// is the minimum `watermark` over the retained manifest set (the same
/// `block_id >= retain_from` set `plan_gc` already protects); `None` (no
/// manifests) means no log object is ever deleted. Positions are compared in
/// their packed form, so epoch bumps sort correctly and a fenced zombie's
/// stale-position object simply becomes sweepable garbage once superseded.
///
/// Safety: a query follower lagging below the min retained watermark loses
/// its tail and terminates with `LogGap` (restart recovers from the latest
/// manifest); `blocks_to_keep`/`garbage_lifetime_us` are the operator's guard
/// against sweeping objects a slow follower still needs.
fn deletable_log_keys(log_keys: &[String], min_retained_watermark: Option<u64>) -> Vec<String> {
    let Some(watermark) = min_retained_watermark else {
        return Vec::new();
    };
    let mut objects: Vec<(u64, &String)> = log_keys
        .iter()
        .filter_map(|key| keys::parse_log_key(key).map(|p| (p.as_u64(), key)))
        .collect();
    objects.sort_by_key(|(pos, _)| *pos);
    objects
        .windows(2)
        .filter(|pair| pair[1].0 <= watermark)
        .map(|pair| pair[0].1.clone())
        .collect()
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
    if has_path_prefix(key, PROBE_PREFIX) {
        return true;
    }
    if protected.contains(key) {
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
    use varve_storage::keys::{data_key, log_key, manifest_key, meta_key};
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

    fn manifest_with_watermark(block_id: u64, watermark: u64, tries: Vec<&str>) -> BlockManifest {
        let mut m = manifest(block_id, 100, tries);
        m.watermark = watermark;
        m
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
    fn gc_plan_sweeps_log_objects_wholly_below_the_min_retained_watermark() {
        // Retained manifest watermark = position 4 (epoch 0). Log objects start at 0, 2, 4, 6.
        // Object@0 spans [0,2) and object@2 spans [2,4): both wholly < 4 → swept.
        // Object@4 spans [4,6) and object@6 is last: kept.
        let w = LogPosition::new(0, 4).unwrap().as_u64();
        let manifests = vec![manifest_with_watermark(10, w, vec!["l00-rc-b10"])];
        let keys: Vec<String> = [0u64, 2, 4, 6]
            .into_iter()
            .map(|off| log_key(LogPosition::new(0, off).unwrap()))
            .collect();
        let plan = plan(&manifests, keys.clone(), enabled_config());
        assert!(plan.delete_keys.contains(&keys[0]));
        assert!(plan.delete_keys.contains(&keys[1]));
        assert!(!plan.delete_keys.contains(&keys[2]));
        assert!(!plan.delete_keys.contains(&keys[3]));
    }

    #[test]
    fn gc_plan_uses_the_minimum_watermark_across_retained_manifests() {
        // blocks_to_keep = 1 retains blocks 9 and 10; block 9's watermark (2) is the floor,
        // so only the object whose SUCCESSOR starts at <= 2 is swept.
        let w9 = LogPosition::new(0, 2).unwrap().as_u64();
        let w10 = LogPosition::new(0, 6).unwrap().as_u64();
        let manifests = vec![
            manifest_with_watermark(9, w9, vec!["l00-rc-b09"]),
            manifest_with_watermark(10, w10, vec!["l00-rc-b10"]),
        ];
        let mut config = enabled_config();
        config.blocks_to_keep = 1;
        let keys: Vec<String> = [0u64, 2, 4]
            .into_iter()
            .map(|off| log_key(LogPosition::new(0, off).unwrap()))
            .collect();
        let plan = plan(&manifests, keys.clone(), config);
        assert_eq!(
            plan.delete_keys
                .iter()
                .filter(|k| k.starts_with("v1/log"))
                .collect::<Vec<_>>(),
            vec![&keys[0]]
        );
    }

    #[test]
    fn gc_plan_boundary_spanning_and_last_log_objects_are_kept() {
        // Watermark 3 falls INSIDE object@2's span [2,4): object@2 kept. object@0 swept.
        let w = LogPosition::new(0, 3).unwrap().as_u64();
        let manifests = vec![manifest_with_watermark(10, w, vec!["l00-rc-b10"])];
        let keys: Vec<String> = [0u64, 2]
            .into_iter()
            .map(|off| log_key(LogPosition::new(0, off).unwrap()))
            .collect();
        let plan = plan(&manifests, keys.clone(), enabled_config());
        assert!(plan.delete_keys.contains(&keys[0]));
        assert!(!plan.delete_keys.contains(&keys[1]));
    }

    #[test]
    fn gc_plan_sweeps_across_epoch_bumps_in_packed_order() {
        // Fenced epoch 0 objects at 0 and 3; epoch 1 resumed at offset 3; retained
        // watermark = (1, 5). Epoch-0 object@0 (successor (0,3) <= w) and object@(0,3)
        // (successor (1,3) <= w) are swept; (1,3) is followed by (1,5) <= w → swept too;
        // (1,5) is last → kept.
        let w = LogPosition::new(1, 5).unwrap().as_u64();
        let manifests = vec![manifest_with_watermark(10, w, vec!["l00-rc-b10"])];
        let positions = [
            LogPosition::new(0, 0).unwrap(),
            LogPosition::new(0, 3).unwrap(),
            LogPosition::new(1, 3).unwrap(),
            LogPosition::new(1, 5).unwrap(),
        ];
        let keys: Vec<String> = positions.into_iter().map(log_key).collect();
        let plan = plan(&manifests, keys.clone(), enabled_config());
        assert!(plan.delete_keys.contains(&keys[0]));
        assert!(plan.delete_keys.contains(&keys[1]));
        assert!(plan.delete_keys.contains(&keys[2]));
        assert!(!plan.delete_keys.contains(&keys[3]));
    }

    #[test]
    fn gc_plan_keeps_foreign_keys_under_the_log_prefix() {
        let w = LogPosition::new(0, 9).unwrap().as_u64();
        let manifests = vec![manifest_with_watermark(10, w, vec!["l00-rc-b10"])];
        let keys = vec![
            "v1/log/0000/notavlog.txt".to_string(),
            "v1/logish".to_string(),
        ];
        let plan = plan(&manifests, keys, enabled_config());
        assert!(plan.delete_keys.iter().all(|k| !k.contains("log")));
    }

    #[test]
    fn gc_plan_without_manifests_never_touches_log_objects() {
        let keys = vec![log_key(LogPosition::new(0, 0).unwrap())];
        let plan = plan(&[], keys, enabled_config());
        assert!(plan.delete_keys.is_empty());
    }

    #[test]
    fn gc_plan_sweeps_probe_objects_and_keeps_fence_and_writer_keys() {
        let manifests = vec![manifest(10, 100, vec!["l00-rc-b10"])];
        let keys = vec![
            format!("{PROBE_PREFIX}/deadbeef"),
            "v1/epochs/0001.json".to_string(),
            "v1/writer.json".to_string(),
        ];
        let plan = plan(&manifests, keys, enabled_config());
        assert_eq!(plan.delete_keys, vec![format!("{PROBE_PREFIX}/deadbeef")]);
    }
}
