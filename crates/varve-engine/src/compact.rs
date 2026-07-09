use std::cmp::Reverse;
use std::collections::BTreeMap;
use varve_index::{
    decode_events, encode_sorted_events_by, Ceiling, EncodedBlock, Event, IndexError, Op, PageMeta,
    Polygon, SortOrder,
};
use varve_storage::keys::{Bucketer, Recency, TrieKey, TRIE_BRANCH_FACTOR};
use varve_storage::{StorageError, TableScope, TrieCatalog, TrieShard};
use varve_types::{Iid, Instant};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompactionConfig {
    pub log_limit: usize,
}

impl Default for CompactionConfig {
    fn default() -> CompactionConfig {
        CompactionConfig {
            log_limit: varve_storage::keys::LOG_LIMIT,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CompactionKind {
    L0ToL1Split,
    SameShard,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompactionJob {
    pub kind: CompactionKind,
    pub scope: TableScope,
    pub input_trie_keys: Vec<String>,
    pub output_trie_keys: Vec<TrieKey>,
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
    pub trie_key: String,
    pub data: Vec<u8>,
    pub pages: Vec<PageMeta>,
}

pub(crate) struct CompactedBlock {
    pub trie_key: TrieKey,
    pub encoded: EncodedBlock,
}

#[derive(Clone)]
struct LiveTrie {
    scope: TableScope,
    key: TrieKey,
}

pub(crate) fn select_compaction_jobs(
    catalog: &TrieCatalog,
    config: &CompactionConfig,
) -> Result<Vec<CompactionJob>, StorageError> {
    let mut live = Vec::new();
    for live_entry in catalog.live_entries() {
        live.push(LiveTrie {
            scope: live_entry.scope,
            key: TrieKey::parse(&live_entry.entry.trie_key)?,
        });
    }
    live.sort_by(|a, b| {
        (
            &a.scope,
            &a.key.level,
            &a.key.recency,
            &a.key.part,
            &a.key.block,
        )
            .cmp(&(
                &b.scope,
                &b.key.level,
                &b.key.recency,
                &b.key.part,
                &b.key.block,
            ))
    });

    let mut jobs = Vec::new();
    select_l0_jobs(&live, config, &mut jobs);
    select_same_shard_jobs(&live, &mut jobs);
    jobs.sort_by(|a, b| {
        (&a.scope, &a.input_trie_keys, &a.output_trie_keys).cmp(&(
            &b.scope,
            &b.input_trie_keys,
            &b.output_trie_keys,
        ))
    });
    Ok(jobs)
}

fn select_l0_jobs(live: &[LiveTrie], config: &CompactionConfig, jobs: &mut Vec<CompactionJob>) {
    let mut groups: BTreeMap<TableScope, Vec<&LiveTrie>> = BTreeMap::new();
    for trie in live.iter().filter(|trie| trie.key.level == 0) {
        groups.entry(trie.scope.clone()).or_default().push(trie);
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
            input_trie_keys: inputs.iter().map(|trie| trie.key.to_key_string()).collect(),
            output_trie_keys: vec![TrieKey {
                level: 1,
                recency: Recency::Current,
                part: Vec::new(),
                block,
            }],
        });
    }
}

fn select_same_shard_jobs(live: &[LiveTrie], jobs: &mut Vec<CompactionJob>) {
    let mut groups: BTreeMap<TrieShard, Vec<&LiveTrie>> = BTreeMap::new();
    for trie in live.iter().filter(|trie| trie.key.level >= 1) {
        groups
            .entry(TrieShard {
                scope: trie.scope.clone(),
                key_shard: trie.key.shard(),
            })
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
        let parent = TrieKey {
            level: shard.key_shard.level,
            recency: shard.key_shard.recency.clone(),
            part: shard.key_shard.part.clone(),
            block,
        };
        let output_trie_keys = (0..TRIE_BRANCH_FACTOR)
            .map(|bucket| parent.child(bucket, block))
            .collect();
        jobs.push(CompactionJob {
            kind: CompactionKind::SameShard,
            scope: shard.scope,
            input_trie_keys: inputs.iter().map(|trie| trie.key.to_key_string()).collect(),
            output_trie_keys,
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
        let key = TrieKey::parse(&input.trie_key).map_err(|err| {
            IndexError::Codec(format!("invalid compaction input trie key: {err}"))
        })?;
        sorted_inputs.push((key, input));
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
            let key = output_key_for_event(job, &event, order, recency)
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
        let encoded = encode_sorted_events_by(&rows, page_rows, order, &trie_key.part)?;
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

fn output_key_for_event(
    job: &CompactionJob,
    event: &Event,
    order: SortOrder,
    recency: Recency,
) -> Option<TrieKey> {
    match job.kind {
        CompactionKind::L0ToL1Split => {
            let base = job.output_trie_keys.first()?;
            Some(TrieKey {
                level: base.level,
                recency,
                part: base.part.clone(),
                block: base.block,
            })
        }
        CompactionKind::SameShard => {
            let key = sort_key(event, order);
            job.output_trie_keys
                .iter()
                .find(|candidate| {
                    candidate.recency == recency && Bucketer::contains(&candidate.part, &key)
                })
                .cloned()
        }
    }
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
        TrieEntry {
            trie_key: key,
            row_count: 1,
            data_len: 10,
        }
    }

    fn catalog(keys: Vec<String>) -> TrieCatalog {
        TrieCatalog::from_manifests(&[BlockManifest {
            block_id: 99,
            watermark: 99,
            max_tx_id: 99,
            max_system_time_us: 99,
            tables: vec![TableTries {
                graph: "default".into(),
                table: "nodes".into(),
                family: String::new(),
                tries: keys.into_iter().map(entry).collect(),
            }],
        }])
        .unwrap()
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
        assert_eq!(
            job.output_trie_keys,
            vec![TrieKey {
                level: 1,
                recency: Recency::Current,
                part: Vec::new(),
                block: 63,
            }]
        );
    }

    #[test]
    fn selects_four_same_shard_level_jobs() {
        let keys = (0..4)
            .map(|block| format!("l01-rc-b{}", lex_hex(block)))
            .collect();
        let jobs = select_compaction_jobs(&catalog(keys), &CompactionConfig::default()).unwrap();

        assert_eq!(jobs.len(), 1);
        let job = &jobs[0];
        assert_eq!(job.kind, CompactionKind::SameShard);
        assert_eq!(
            job.input_trie_keys,
            vec!["l01-rc-b00", "l01-rc-b01", "l01-rc-b02", "l01-rc-b03",]
        );
        assert_eq!(
            job.output_trie_keys,
            vec![
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
        let keys = (0..4)
            .map(|block| format!("l01-rc-b{}", lex_hex(block)))
            .collect::<Vec<_>>();
        let catalog = catalog(keys);

        let a = select_compaction_jobs(&catalog, &CompactionConfig::default()).unwrap();
        let b = select_compaction_jobs(&catalog, &CompactionConfig::default()).unwrap();

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
            trie_key: trie_key.to_string(),
            data: encoded.data,
            pages: encoded.pages,
        }
    }

    fn l0_job() -> CompactionJob {
        CompactionJob {
            kind: CompactionKind::L0ToL1Split,
            scope: TableScope::new("default", "nodes", ""),
            input_trie_keys: vec!["l00-rc-b00".into(), "l00-rc-b01".into()],
            output_trie_keys: vec![TrieKey {
                level: 1,
                recency: Recency::Current,
                part: Vec::new(),
                block: 1,
            }],
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
