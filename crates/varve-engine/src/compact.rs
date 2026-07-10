use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet};

use crate::state::{EDGES_TABLE, NODES_TABLE};
use varve_index::{
    decode_events, encode_sorted_events_by, Ceiling, EncodedBlock, Event, IndexError, Op, PageMeta,
    Polygon, SortOrder,
};
use varve_storage::keys::{Recency, TrieKey};
use varve_storage::{
    BlockManifest, ScopedTrieKey, StorageError, TableScope, TableTries, TrieCatalog, TrieEntry,
    TrieShard,
};
use varve_types::{Bucketer, Iid, Instant, TRIE_BRANCH_FACTOR};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompactionConfig {
    pub log_limit: usize,
    pub file_size_target: u64,
}

impl Default for CompactionConfig {
    fn default() -> CompactionConfig {
        CompactionConfig {
            log_limit: varve_storage::keys::LOG_LIMIT,
            file_size_target: 104_857_600,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompactionKind {
    L0ToL1Split,
    SameShard,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CompactionOutputPlan {
    Exact(Vec<TrieKey>),
    L0RecencySplit {
        level: u64,
        part: Vec<u8>,
        block: u64,
    },
}

impl CompactionOutputPlan {
    fn output_key_for_event(
        &self,
        event: &Event,
        order: SortOrder,
        recency: Recency,
    ) -> Option<TrieKey> {
        match self {
            CompactionOutputPlan::Exact(output_keys) => {
                let key = sort_key(event, order);
                output_keys
                    .iter()
                    .find(|candidate| {
                        candidate.recency == recency && Bucketer::contains(&candidate.part, &key)
                    })
                    .cloned()
            }
            CompactionOutputPlan::L0RecencySplit { level, part, block } => Some(TrieKey {
                level: *level,
                recency,
                part: part.clone(),
                block: *block,
            }),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompactionJob {
    pub kind: CompactionKind,
    pub scope: TableScope,
    pub input_trie_keys: Vec<TrieKey>,
    pub output_plan: CompactionOutputPlan,
}

impl CompactionJob {
    pub(crate) fn scope(&self) -> &TableScope {
        &self.scope
    }

    pub(crate) fn input_key_strings(&self) -> Vec<String> {
        self.input_trie_keys
            .iter()
            .map(TrieKey::to_key_string)
            .collect()
    }

    pub(crate) fn scoped_key(&self, trie_key: &TrieKey) -> ScopedTrieKey {
        self.scope.scoped_trie_key(trie_key.to_key_string())
    }

    pub(crate) fn data_key(&self, trie_key: &TrieKey) -> String {
        self.scoped_key(trie_key).data_key()
    }

    pub(crate) fn meta_key(&self, trie_key: &TrieKey) -> String {
        self.scoped_key(trie_key).meta_key()
    }

    pub(crate) fn target_sort_order(&self) -> Option<SortOrder> {
        match (self.scope.table.as_str(), self.scope.family.as_str()) {
            (NODES_TABLE, "") | (EDGES_TABLE, "") => Some(SortOrder::ByIid),
            (EDGES_TABLE, varve_storage::ADJ_OUT) => Some(SortOrder::BySrc),
            (EDGES_TABLE, varve_storage::ADJ_IN) => Some(SortOrder::ByDst),
            _ => None,
        }
    }

    pub(crate) fn replace_inputs_with_outputs<T>(
        &self,
        tries: &mut Vec<T>,
        outputs: impl IntoIterator<Item = T>,
        trie_key: impl Fn(&T) -> &str,
    ) {
        let input_keys = self
            .input_key_strings()
            .into_iter()
            .collect::<BTreeSet<_>>();
        tries.retain(|entry| !input_keys.contains(trie_key(entry)));
        tries.extend(outputs);
        tries.sort_by(|a, b| trie_key(a).cmp(trie_key(b)));
    }
}

pub(crate) fn compacted_manifest(
    latest: &BlockManifest,
    block_id: u64,
    job: &CompactionJob,
    output_entries: Vec<TrieEntry>,
) -> BlockManifest {
    let mut tables = latest.tables.clone();
    let mut replaced = false;
    for table in &mut tables {
        if table.scope_ref().eq(job.scope()) {
            job.replace_inputs_with_outputs(&mut table.tries, output_entries.clone(), |entry| {
                entry.trie_key.as_str()
            });
            replaced = true;
            break;
        }
    }
    if !replaced && !output_entries.is_empty() {
        let mut tries = Vec::new();
        job.replace_inputs_with_outputs(&mut tries, output_entries, |entry| {
            entry.trie_key.as_str()
        });
        tables.push(TableTries::new(job.scope().clone(), tries));
    }
    BlockManifest {
        block_id,
        watermark: latest.watermark,
        max_tx_id: latest.max_tx_id,
        max_system_time_us: latest.max_system_time_us,
        tables,
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompactionReport {
    pub jobs: usize,
    pub input_tries: usize,
    pub output_tries: usize,
    pub input_rows: u64,
    pub output_rows: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct CompactionInputBlock {
    pub trie_key: TrieKey,
    pub data: Vec<u8>,
    pub pages: Vec<PageMeta>,
}

pub(crate) struct CompactedBlock {
    pub trie_key: TrieKey,
    pub encoded: EncodedBlock,
}

#[derive(Clone)]
struct LiveTrie {
    scoped_key: ScopedTrieKey,
    key: TrieKey,
    data_len: u64,
}

pub(crate) fn select_compaction_jobs(
    catalog: &TrieCatalog,
    config: &CompactionConfig,
) -> Result<Vec<CompactionJob>, StorageError> {
    let mut live = Vec::new();
    for live_entry in catalog.live_entries() {
        let key = live_entry.scoped_key.parse_trie_key()?;
        let data_len = live_entry.entry.data_len;
        live.push(LiveTrie {
            scoped_key: live_entry.scoped_key,
            key,
            data_len,
        });
    }
    live.sort_by(|a, b| {
        (
            &a.scoped_key.scope,
            &a.key.level,
            &a.key.recency,
            &a.key.part,
            &a.key.block,
        )
            .cmp(&(
                &b.scoped_key.scope,
                &b.key.level,
                &b.key.recency,
                &b.key.part,
                &b.key.block,
            ))
    });

    let mut jobs = Vec::new();
    select_l0_jobs(&live, config, &mut jobs);
    select_same_shard_jobs(&live, config, &mut jobs);
    jobs.sort_by(|a, b| {
        (&a.scope, &a.input_trie_keys, &a.output_plan).cmp(&(
            &b.scope,
            &b.input_trie_keys,
            &b.output_plan,
        ))
    });
    Ok(jobs)
}

fn select_l0_jobs(live: &[LiveTrie], config: &CompactionConfig, jobs: &mut Vec<CompactionJob>) {
    let mut groups: BTreeMap<TableScope, Vec<&LiveTrie>> = BTreeMap::new();
    for trie in live.iter().filter(|trie| trie.key.level == 0) {
        groups
            .entry(trie.scoped_key.scope.clone())
            .or_default()
            .push(trie);
    }

    for (scope, mut tries) in groups {
        if tries.len() < config.log_limit {
            continue;
        }
        tries.sort_by_key(|trie| trie.key.block);
        let inputs = tries.into_iter().take(config.log_limit).collect::<Vec<_>>();
        let block = inputs
            .iter()
            .map(|trie| trie.key.block)
            .max()
            .unwrap_or_default();
        jobs.push(CompactionJob {
            kind: CompactionKind::L0ToL1Split,
            scope,
            input_trie_keys: inputs.iter().map(|trie| trie.key.clone()).collect(),
            output_plan: CompactionOutputPlan::L0RecencySplit {
                level: 1,
                part: Vec::new(),
                block,
            },
        });
    }
}

fn select_same_shard_jobs(
    live: &[LiveTrie],
    config: &CompactionConfig,
    jobs: &mut Vec<CompactionJob>,
) {
    let mut groups: BTreeMap<TrieShard, Vec<&LiveTrie>> = BTreeMap::new();
    for trie in live
        .iter()
        .filter(|trie| trie.key.level >= 1 && trie.data_len >= config.file_size_target)
    {
        groups
            .entry(TrieShard::from_trie_key(
                trie.scoped_key.scope.clone(),
                &trie.key,
            ))
            .or_default()
            .push(trie);
    }

    for (shard, mut tries) in groups {
        if tries.len() < TRIE_BRANCH_FACTOR as usize {
            continue;
        }
        tries.sort_by_key(|trie| trie.key.block);
        let inputs = tries
            .into_iter()
            .take(TRIE_BRANCH_FACTOR as usize)
            .collect::<Vec<_>>();
        let block = inputs
            .iter()
            .map(|trie| trie.key.block)
            .max()
            .unwrap_or_default();
        let parent = shard.key_shard.to_trie_key(block);
        let output_keys = (0..TRIE_BRANCH_FACTOR)
            .map(|bucket| parent.child(bucket, block))
            .collect();
        jobs.push(CompactionJob {
            kind: CompactionKind::SameShard,
            scope: shard.scope,
            input_trie_keys: inputs.iter().map(|trie| trie.key.clone()).collect(),
            output_plan: CompactionOutputPlan::Exact(output_keys),
        });
    }
}

pub(crate) fn write_compacted_blocks(
    job: &CompactionJob,
    inputs: &[CompactionInputBlock],
    order: SortOrder,
    page_rows: usize,
) -> Result<Vec<CompactedBlock>, IndexError> {
    let mut sorted_inputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        sorted_inputs.push((input.trie_key.clone(), input));
    }
    sorted_inputs.sort_by(|a, b| a.0.cmp(&b.0));

    let mut by_iid: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
    for (_key, input) in sorted_inputs {
        let mut block_events: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
        let mut pages = input.pages.clone();
        pages.sort_by_key(|page| page.offset);
        for page in pages {
            let start = page.offset as usize;
            let end = start.saturating_add(page.len as usize);
            let bytes = input
                .data
                .get(start..end)
                .ok_or_else(|| IndexError::Codec("compaction page range outside data".into()))?;
            for event in decode_events(bytes)? {
                block_events.entry(event.iid).or_default().push(event);
            }
        }
        for (iid, mut events) in block_events {
            events.reverse();
            by_iid.entry(iid).or_default().extend(events);
        }
    }

    let mut routed: BTreeMap<TrieKey, Vec<Event>> = BTreeMap::new();
    for (_iid, events) in by_iid {
        for (event, recency) in retained_events_with_recency(events) {
            let key = job
                .output_plan
                .output_key_for_event(&event, order, recency)
                .ok_or_else(|| IndexError::Codec("compaction output trie key missing".into()))?;
            routed.entry(key).or_default().push(event);
        }
    }

    let mut outputs = Vec::new();
    for (trie_key, mut rows) in routed {
        rows.sort_by(|a, b| {
            (sort_key(a, order), a.iid, Reverse(a.system_from)).cmp(&(
                sort_key(b, order),
                b.iid,
                Reverse(b.system_from),
            ))
        });
        let encoded = encode_sorted_events_by(&rows, page_rows, order, trie_key.level as usize)?;
        if !encoded.pages.is_empty() {
            outputs.push(CompactedBlock { trie_key, encoded });
        }
    }
    Ok(outputs)
}

fn retained_events_with_recency(events: Vec<Event>) -> Vec<(Event, Recency)> {
    let start = events
        .iter()
        .rposition(|event| matches!(event.op, Op::Erase))
        .map_or(0, |idx| idx + 1);
    let mut retained = Vec::new();
    let mut ceiling = Ceiling::new();
    let mut polygon = Polygon::default();
    for event in events[start..].iter().rev() {
        if event.valid_from >= event.valid_to {
            continue;
        }
        polygon.calculate_for(&ceiling, event.valid_from, event.valid_to);
        ceiling.apply_log(event.system_from, event.valid_from, event.valid_to);
        if polygon.range_count() == 0 {
            continue;
        }
        retained.push((event.clone(), recency_for(polygon.recency())));
    }
    retained.reverse();
    retained
}

fn sort_key(event: &Event, order: SortOrder) -> Iid {
    match order {
        SortOrder::ByIid => event.iid,
        SortOrder::BySrc => event.src.unwrap_or(event.iid),
        SortOrder::ByDst => event.dst.unwrap_or(event.iid),
    }
}

fn recency_for(instant: Instant) -> Recency {
    if instant == Instant::END_OF_TIME {
        Recency::Current
    } else {
        Recency::Week {
            yyyymmdd: week_bucket_yyyymmdd(instant),
        }
    }
}

fn week_bucket_yyyymmdd(instant: Instant) -> u32 {
    const MICROS_PER_DAY: i64 = 86_400_000_000;
    let days = div_floor(instant.as_micros(), MICROS_PER_DAY);
    let weekday_monday_zero = (days + 3).rem_euclid(7);
    let monday = days - weekday_monday_zero;
    let (year, month, day) = civil_from_days(monday);
    (year as u32) * 10_000 + month * 100 + day
}

fn div_floor(n: i64, d: i64) -> i64 {
    let q = n / d;
    let r = n % d;
    if r != 0 && (r > 0) != (d > 0) {
        q - 1
    } else {
        q
    }
}

fn civil_from_days(days_since_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}

#[cfg(test)]
mod tests {
    use super::*;
    use varve_index::{decode_events, encode_block_by, Event, Op, SortOrder};
    use varve_storage::keys::{lex_hex, Recency, TrieKey};
    use varve_storage::{BlockManifest, TableTries, TrieCatalog, TrieEntry};
    use varve_types::{Doc, Iid, Instant, Value};

    fn entry(key: String) -> TrieEntry {
        entry_with_data_len(key, 10)
    }

    fn entry_with_data_len(key: String, data_len: u64) -> TrieEntry {
        TrieEntry {
            trie_key: key,
            row_count: 1,
            data_len,
        }
    }

    fn parse_trie_key(key: &str) -> TrieKey {
        TrieKey::parse(key).unwrap()
    }

    fn catalog(keys: Vec<String>) -> TrieCatalog {
        catalog_entries(keys.into_iter().map(entry).collect())
    }

    fn catalog_entries(tries: Vec<TrieEntry>) -> TrieCatalog {
        TrieCatalog::from_manifests(&[BlockManifest {
            block_id: 99,
            watermark: 99,
            max_tx_id: 99,
            max_system_time_us: 99,
            tables: vec![TableTries {
                graph: "default".into(),
                table: "nodes".into(),
                family: String::new(),
                tries,
            }],
        }])
        .unwrap()
    }

    fn full_catalog(keys: Vec<String>, file_size_target: u64) -> TrieCatalog {
        catalog_entries(
            keys.into_iter()
                .map(|key| entry_with_data_len(key, file_size_target))
                .collect(),
        )
    }

    fn empty_manifest(tables: Vec<TableTries>) -> BlockManifest {
        BlockManifest {
            block_id: 7,
            watermark: 8,
            max_tx_id: 9,
            max_system_time_us: 10,
            tables,
        }
    }

    fn job_for_scope(scope: TableScope) -> CompactionJob {
        CompactionJob {
            kind: CompactionKind::L0ToL1Split,
            scope,
            input_trie_keys: vec![parse_trie_key("l00-rc-b00")],
            output_plan: CompactionOutputPlan::L0RecencySplit {
                level: 1,
                part: Vec::new(),
                block: 0,
            },
        }
    }

    #[test]
    fn target_sort_order_matches_table_scope() {
        assert_eq!(
            job_for_scope(TableScope::new("default", "nodes", "")).target_sort_order(),
            Some(SortOrder::ByIid)
        );
        assert_eq!(
            job_for_scope(TableScope::new("default", "edges", "")).target_sort_order(),
            Some(SortOrder::ByIid)
        );
        assert_eq!(
            job_for_scope(TableScope::new("default", "edges", varve_storage::ADJ_OUT))
                .target_sort_order(),
            Some(SortOrder::BySrc)
        );
        assert_eq!(
            job_for_scope(TableScope::new("default", "edges", varve_storage::ADJ_IN))
                .target_sort_order(),
            Some(SortOrder::ByDst)
        );
    }

    #[test]
    fn compaction_config_defaults_match_public_limits() {
        let config = CompactionConfig::default();

        assert_eq!(varve_storage::keys::PAGE_LIMIT, 1024);
        assert_eq!(config.log_limit, 64);
        assert_eq!(config.file_size_target, 104_857_600);
    }

    #[test]
    fn target_sort_order_returns_none_for_unknown_scope() {
        assert_eq!(
            job_for_scope(TableScope::new("default", "widgets", "")).target_sort_order(),
            None
        );
        assert_eq!(
            job_for_scope(TableScope::new("default", "edges", "adj-sideways")).target_sort_order(),
            None
        );
    }

    #[test]
    fn compacted_manifest_replaces_inputs_adds_outputs_and_sorts_tries() {
        let scope = TableScope::new("default", "nodes", "");
        let latest = empty_manifest(vec![TableTries::new(
            scope.clone(),
            vec![
                entry("l00-rc-b01".into()),
                entry("l00-rc-b02".into()),
                entry("l00-rc-b03".into()),
                entry("l01-rc-b10".into()),
            ],
        )]);
        let job = CompactionJob {
            input_trie_keys: vec![parse_trie_key("l00-rc-b02"), parse_trie_key("l00-rc-b03")],
            ..job_for_scope(scope.clone())
        };

        let compacted = compacted_manifest(
            &latest,
            11,
            &job,
            vec![entry("l01-rc-b04".into()), entry("l01-rc-b00".into())],
        );

        assert_eq!(compacted.block_id, 11);
        assert_eq!(compacted.watermark, latest.watermark);
        assert_eq!(compacted.max_tx_id, latest.max_tx_id);
        assert_eq!(compacted.max_system_time_us, latest.max_system_time_us);
        assert_eq!(compacted.tables.len(), 1);
        assert_eq!(compacted.tables[0].scope_ref(), scope);
        assert_eq!(
            compacted.tables[0]
                .tries
                .iter()
                .map(|entry| entry.trie_key.as_str())
                .collect::<Vec<_>>(),
            vec!["l00-rc-b01", "l01-rc-b00", "l01-rc-b04", "l01-rc-b10"]
        );
    }

    #[test]
    fn compacted_manifest_adds_missing_scope_when_outputs_are_non_empty() {
        let scope = TableScope::new("default", "edges", varve_storage::ADJ_OUT);
        let latest = empty_manifest(Vec::new());
        let job = job_for_scope(scope.clone());

        let compacted = compacted_manifest(&latest, 11, &job, vec![entry("l01-rc-b00".into())]);

        assert_eq!(compacted.tables.len(), 1);
        assert_eq!(compacted.tables[0].scope_ref(), scope);
        assert_eq!(
            compacted.tables[0]
                .tries
                .iter()
                .map(|entry| entry.trie_key.as_str())
                .collect::<Vec<_>>(),
            vec!["l01-rc-b00"]
        );
    }

    #[test]
    fn selects_l0_job_when_log_limit_reached() {
        let keys = (0..64)
            .map(|block| format!("l00-rc-b{}", lex_hex(block)))
            .collect();
        let jobs = select_compaction_jobs(&catalog(keys), &CompactionConfig::default()).unwrap();

        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.kind, CompactionKind::L0ToL1Split);
        assert_eq!(job.scope, TableScope::new("default", "nodes", ""));
        assert_eq!(job.input_trie_keys.len(), 64);
        assert_eq!(job.input_trie_keys[0], TrieKey::l0(0));
        assert_eq!(
            job.output_plan,
            CompactionOutputPlan::L0RecencySplit {
                level: 1,
                part: Vec::new(),
                block: 63,
            }
        );
    }

    #[test]
    fn selects_l0_recency_split_outputs() {
        let keys = (0..varve_storage::keys::LOG_LIMIT)
            .map(|block| format!("l00-rc-b{}", lex_hex(block as u64)))
            .collect();
        let jobs = select_compaction_jobs(&catalog(keys), &CompactionConfig::default()).unwrap();

        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.kind, CompactionKind::L0ToL1Split);
        assert_eq!(
            job.output_plan,
            CompactionOutputPlan::L0RecencySplit {
                level: 1,
                part: Vec::new(),
                block: (varve_storage::keys::LOG_LIMIT - 1) as u64,
            }
        );
    }

    #[test]
    fn partial_same_shard_files_do_not_schedule_compaction() {
        let keys = (0..4)
            .map(|block| format!("l01-rc-b{}", lex_hex(block)))
            .collect();
        let jobs = select_compaction_jobs(&catalog(keys), &CompactionConfig::default()).unwrap();

        assert!(jobs.is_empty());
    }

    #[test]
    fn selects_four_full_same_shard_level_jobs() {
        let config = CompactionConfig::default();
        let keys = (0..4)
            .map(|block| format!("l01-rc-b{}", lex_hex(block)))
            .collect();
        let jobs =
            select_compaction_jobs(&full_catalog(keys, config.file_size_target), &config).unwrap();

        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.kind, CompactionKind::SameShard);
        assert_eq!(
            job.input_trie_keys,
            vec![
                parse_trie_key("l01-rc-b00"),
                parse_trie_key("l01-rc-b01"),
                parse_trie_key("l01-rc-b02"),
                parse_trie_key("l01-rc-b03"),
            ]
        );
        assert_eq!(
            job.output_plan,
            CompactionOutputPlan::Exact(vec![
                TrieKey {
                    level: 2,
                    recency: Recency::Current,
                    part: vec![0],
                    block: 3,
                },
                TrieKey {
                    level: 2,
                    recency: Recency::Current,
                    part: vec![1],
                    block: 3,
                },
                TrieKey {
                    level: 2,
                    recency: Recency::Current,
                    part: vec![2],
                    block: 3,
                },
                TrieKey {
                    level: 2,
                    recency: Recency::Current,
                    part: vec![3],
                    block: 3,
                },
            ])
        );
    }

    #[test]
    fn same_shard_selection_skips_partial_files_and_preserves_full_file_order() {
        let config = CompactionConfig {
            file_size_target: 100,
            ..CompactionConfig::default()
        };
        let catalog = catalog_entries(
            [(0, 99), (1, 100), (2, 1), (3, 101), (4, 100), (5, 200)]
                .into_iter()
                .map(|(block, data_len)| {
                    entry_with_data_len(format!("l01-rc-b{}", lex_hex(block)), data_len)
                })
                .collect(),
        );

        let jobs = select_compaction_jobs(&catalog, &config).unwrap();

        assert_eq!(jobs.len(), 1);
        assert_eq!(
            jobs[0].input_trie_keys,
            vec![
                parse_trie_key("l01-rc-b01"),
                parse_trie_key("l01-rc-b03"),
                parse_trie_key("l01-rc-b04"),
                parse_trie_key("l01-rc-b05"),
            ]
        );
    }

    #[test]
    fn job_selection_is_order_independent() {
        let forward = (0..64)
            .map(|block| format!("l00-rc-b{}", lex_hex(block)))
            .collect();
        let reverse = (0..64)
            .rev()
            .map(|block| format!("l00-rc-b{}", lex_hex(block)))
            .collect();

        assert_eq!(
            select_compaction_jobs(&catalog(forward), &CompactionConfig::default()).unwrap(),
            select_compaction_jobs(&catalog(reverse), &CompactionConfig::default()).unwrap()
        );
    }

    #[test]
    fn duplicate_output_key_for_same_catalog_state() {
        let config = CompactionConfig::default();
        let keys = (0..4)
            .map(|block| format!("l01-rc-b{}", lex_hex(block)))
            .collect::<Vec<_>>();
        let catalog = full_catalog(keys, config.file_size_target);

        let a = select_compaction_jobs(&catalog, &config).unwrap();
        let b = select_compaction_jobs(&catalog, &config).unwrap();

        assert_eq!(a, b);
    }

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn iid(n: u8) -> Iid {
        Iid::derive("g", "nodes", &[n])
    }

    fn put(
        entity: u8,
        system_from: i64,
        valid_from: Instant,
        valid_to: Instant,
        marker: &str,
    ) -> Event {
        let mut doc = Doc::new();
        doc.insert("marker".into(), Value::Str(marker.into()));
        Event {
            iid: iid(entity),
            system_from: us(system_from),
            valid_from,
            valid_to,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".into()],
                doc,
            },
        }
    }

    fn erase(entity: u8, system_from: i64) -> Event {
        Event {
            iid: iid(entity),
            system_from: us(system_from),
            valid_from: Instant::MIN,
            valid_to: Instant::END_OF_TIME,
            src: None,
            dst: None,
            op: Op::Erase,
        }
    }

    fn input_block(trie_key: &str, events: &[Event]) -> CompactionInputBlock {
        let mut live = varve_index::LiveTable::new();
        for event in events {
            live.append(event.clone()).unwrap();
        }
        let encoded = encode_block_by(&live, 4, SortOrder::ByIid).unwrap();
        CompactionInputBlock {
            trie_key: parse_trie_key(trie_key),
            data: encoded.data,
            pages: encoded.pages,
        }
    }

    fn l0_job() -> CompactionJob {
        CompactionJob {
            kind: CompactionKind::L0ToL1Split,
            scope: TableScope::new("default", "nodes", ""),
            input_trie_keys: vec![parse_trie_key("l00-rc-b00"), parse_trie_key("l00-rc-b01")],
            output_plan: CompactionOutputPlan::L0RecencySplit {
                level: 1,
                part: Vec::new(),
                block: 1,
            },
        }
    }

    fn only_events(outputs: &[CompactedBlock]) -> Vec<Event> {
        let mut events = Vec::new();
        for output in outputs {
            for page in &output.encoded.pages {
                let bytes =
                    &output.encoded.data[page.offset as usize..(page.offset + page.len) as usize];
                events.extend(decode_events(bytes).unwrap());
            }
        }
        events
    }

    #[test]
    fn compacted_output_is_byte_identical_for_permuted_inputs() {
        let a = input_block("l00-rc-b00", &[put(1, 1, us(1), Instant::END_OF_TIME, "a")]);
        let b = input_block("l00-rc-b01", &[put(2, 2, us(2), Instant::END_OF_TIME, "b")]);

        let forward =
            write_compacted_blocks(&l0_job(), &[a.clone(), b.clone()], SortOrder::ByIid, 4)
                .unwrap();
        let reverse = write_compacted_blocks(&l0_job(), &[b, a], SortOrder::ByIid, 4).unwrap();

        assert_eq!(forward.len(), reverse.len());
        for (left, right) in forward.iter().zip(reverse.iter()) {
            assert_eq!(left.trie_key, right.trie_key);
            assert_eq!(left.encoded.data, right.encoded.data);
            assert_eq!(left.encoded.meta, right.encoded.meta);
            assert_eq!(left.encoded.pages, right.encoded.pages);
        }
    }

    #[test]
    fn compacted_events_are_sorted_by_iid_and_system_desc() {
        let input = input_block(
            "l00-rc-b00",
            &[
                put(1, 1, us(1), Instant::END_OF_TIME, "old"),
                put(2, 2, us(2), Instant::END_OF_TIME, "other"),
                put(1, 3, us(3), Instant::END_OF_TIME, "new"),
            ],
        );

        let outputs = write_compacted_blocks(&l0_job(), &[input], SortOrder::ByIid, 4).unwrap();
        let events = only_events(&outputs);

        assert_eq!(events.len(), 3);
        for pair in events.windows(2) {
            assert!(
                pair[0].iid < pair[1].iid
                    || (pair[0].iid == pair[1].iid && pair[0].system_from >= pair[1].system_from),
                "events must be (_iid asc, _system_from desc)"
            );
        }
    }

    #[test]
    fn l0_compaction_routes_current_and_weekly_historical_outputs() {
        let historical_to = Instant::parse_rfc3339("2020-01-08T00:00:00Z").unwrap();
        let input = input_block(
            "l00-rc-b00",
            &[
                put(
                    1,
                    1,
                    Instant::parse_rfc3339("2020-01-01T00:00:00Z").unwrap(),
                    historical_to,
                    "hist",
                ),
                put(2, 2, us(2), Instant::END_OF_TIME, "current"),
            ],
        );

        let outputs = write_compacted_blocks(&l0_job(), &[input], SortOrder::ByIid, 4).unwrap();
        let keys = outputs
            .iter()
            .map(|output| output.trie_key.clone())
            .collect::<Vec<_>>();

        assert_eq!(
            keys,
            vec![
                TrieKey {
                    level: 1,
                    recency: Recency::Current,
                    part: Vec::new(),
                    block: 1,
                },
                TrieKey {
                    level: 1,
                    recency: Recency::Week { yyyymmdd: 20200106 },
                    part: Vec::new(),
                    block: 1,
                },
            ]
        );
    }

    #[test]
    fn erase_drops_prior_bytes_but_keeps_later_reinsert() {
        let input = input_block(
            "l00-rc-b00",
            &[
                put(1, 1, us(1), Instant::END_OF_TIME, "secret-before"),
                erase(1, 2),
                put(1, 3, us(3), Instant::END_OF_TIME, "after"),
            ],
        );

        let outputs = write_compacted_blocks(&l0_job(), &[input], SortOrder::ByIid, 4).unwrap();
        let events = only_events(&outputs);
        let bytes = outputs
            .iter()
            .flat_map(|output| output.encoded.data.iter().copied())
            .collect::<Vec<_>>();
        let haystack = String::from_utf8_lossy(&bytes);

        assert_eq!(events.len(), 1);
        assert!(haystack.contains("after"));
        assert!(!haystack.contains("secret-before"));
    }
}
