//! Block format v1 (spec §9, roadmap slice 4). The data file is a
//! concatenation of self-contained per-page Arrow IPC streams (the slice-3
//! event codec, verbatim); the meta file is a single-level page index whose
//! per-page byte ranges make a page read one ranged GET. The full hash-trie
//! meta arrives with slice 8's compaction.

use crate::codec::{downcast, encode_events};
use crate::event::{Event, Op};
use crate::live::{IndexError, LiveTable};
use arrow::array::{
    ArrayRef, BinaryArray, BinaryBuilder, BooleanArray, BooleanBuilder, FixedSizeBinaryArray,
    FixedSizeBinaryBuilder, TimestampMicrosecondArray, TimestampMicrosecondBuilder, UInt64Array,
    UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use varve_types::{
    Bucketer, Iid, Instant, TemporalBounds, MAX_TRIE_LEVELS, PAGE_LIMIT, TRIE_BRANCH_FACTOR,
};

/// Rows per page (XTDB `pageLimit`). A parameter on `encode_block` so tests
/// can force page splits; the engine passes this constant.
pub const DEFAULT_PAGE_ROWS: usize = PAGE_LIMIT;
/// One page's entry in the meta file: byte range in the data file plus the
/// stats the scan prunes by.
#[derive(Debug, Clone, PartialEq)]
pub struct PageMeta {
    pub path: Vec<u8>,
    pub offset: u64,
    pub len: u64,
    pub rows: u64,
    pub min_iid: Iid,
    pub max_iid: Iid,
    pub min_system_from: Instant,
    pub max_system_from: Instant,
    pub min_valid_from: Instant,
    pub max_valid_from: Instant,
    pub min_valid_to: Instant,
    pub max_valid_to: Instant,
    pub has_erase: bool,
}

impl PageMeta {
    /// Should the scan read this page? Prune rules (slice-4 plan, decision 4):
    /// - IID point outside `[min_iid, max_iid]` → skip: resolution is
    ///   per-entity, other entities' pages are irrelevant.
    /// - Every event at/after `bounds.system.upper` → skip: `resolve()`
    ///   ignores such events BEFORE they touch the ceiling, so dropping the
    ///   page is exactly output-preserving — UNLESS the page holds an
    ///   `Erase`, which hides history at every system time (slice-2 GDPR
    ///   decision) and must always be scanned.
    /// - The valid axis deliberately does NOT prune: an event valid-disjoint
    ///   from the query window still clips the reported `_valid_from`/
    ///   `_valid_to` of visible rectangles inside it (`valid_to(x)`
    ///   introspection). Valid stats are recorded for slice 8.
    pub fn selected(&self, bounds: &TemporalBounds, iid_point: Option<&Iid>) -> bool {
        if let Some(iid) = iid_point {
            if !self.path.is_empty() && !Bucketer::contains(&self.path, iid) {
                return false;
            }
            if *iid < self.min_iid || *iid > self.max_iid {
                return false;
            }
        }
        if self.min_system_from >= bounds.system.upper && !self.has_erase {
            return false;
        }
        true
    }
}

pub struct EncodedBlock {
    pub data: Vec<u8>,
    pub meta: Vec<u8>,
    pub pages: Vec<PageMeta>,
}

/// Which key a block file is sorted (and page-pruned) by (slice-6 decision 4).
/// The primary table sorts by `_iid`; the adjacency families co-locate an
/// edge's full history under its `src`/`dst` endpoint so anchor lookups prune
/// pages via `PageMeta::selected` with zero new prune code.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SortOrder {
    ByIid,
    BySrc,
    ByDst,
}

/// Serializes the live table into one L0 block: rows in `(_iid asc,
/// _system_from desc)` file order (spec §5.2) chunked into pages of
/// `page_rows`. Pure function of the table (determinism constraint):
/// BTreeMap iteration + stable per-entity reversal, no clocks, no maps
/// with random order. Thin wrapper over [`encode_block_by`] with the primary
/// [`SortOrder::ByIid`] order.
pub fn encode_block(live: &LiveTable, page_rows: usize) -> Result<EncodedBlock, IndexError> {
    encode_block_by(live, page_rows, SortOrder::ByIid)
}

/// Like [`encode_block`] but sorted by `(sort_key asc, iid asc, system_from
/// desc)`, where `sort_key` is `_iid` ([`SortOrder::ByIid`]), `src`
/// ([`SortOrder::BySrc`]), or `dst` ([`SortOrder::ByDst`]). `PageMeta.min_iid`/
/// `max_iid` record the SORT KEY's range (not the row iids) so
/// `PageMeta::selected` prunes adjacency lookups by anchor unchanged.
/// `BySrc`/`ByDst` on an event that lacks that endpoint ⇒ `IndexError::Codec`.
///
/// For `ByIid` the sort key equals each row's iid, so both the row order and
/// the page min/max iid are byte-identical to the pre-refactor `encode_block`.
pub fn encode_block_by(
    live: &LiveTable,
    page_rows: usize,
    order: SortOrder,
) -> Result<EncodedBlock, IndexError> {
    // Per-entity event lists in (system_from desc) file order, keyed for sorting.
    let mut groups: Vec<(Iid, Iid, Vec<Event>)> = Vec::new(); // (sort_key, iid, desc events)
    for (iid, events) in live.entities() {
        // Stable reversal: arrival order → system_from desc, ties reversed
        // exactly; the scan's reversal restores arrival order (decision 9).
        let desc: Vec<Event> = events.iter().rev().cloned().collect();
        let sort_key = match order {
            SortOrder::ByIid => *iid,
            SortOrder::BySrc => desc.first().and_then(|e| e.src).ok_or_else(|| {
                IndexError::Codec("edge event missing src endpoint in adjacency encode".into())
            })?,
            SortOrder::ByDst => desc.first().and_then(|e| e.dst).ok_or_else(|| {
                IndexError::Codec("edge event missing dst endpoint in adjacency encode".into())
            })?,
        };
        groups.push((sort_key, *iid, desc));
    }
    // Total order over (sort_key, iid) ⇒ deterministic file layout.
    groups.sort_by_key(|g| (g.0, g.1));

    let mut rows: Vec<Event> = Vec::with_capacity(live.event_count());
    let mut keys: Vec<Iid> = Vec::with_capacity(live.event_count()); // sort key per row
    for (key, _iid, events) in groups {
        for e in events {
            rows.push(e);
            keys.push(key);
        }
    }

    encode_pages_by_keys(&rows, &keys, page_rows, 0)
}

/// Serializes rows already ordered for a compacted trie output.
///
/// `rows` must be sorted by `(sort_key asc, _iid asc, _system_from desc)`.
/// This helper exists so compaction can preserve deterministic merge order
/// without rebuilding a `LiveTable`, while still sharing the exact page/meta
/// codec used by ordinary flushes. Page paths are longest shared sort-key
/// bucket prefixes, capped at `page_path_levels`.
pub fn encode_sorted_events_by(
    rows: &[Event],
    page_rows: usize,
    order: SortOrder,
    page_path_levels: usize,
) -> Result<EncodedBlock, IndexError> {
    let mut keys = Vec::with_capacity(rows.len());
    for event in rows {
        keys.push(event_sort_key(event, order)?);
    }

    encode_pages_by_keys(rows, &keys, page_rows, page_path_levels)
}

fn event_sort_key(event: &Event, order: SortOrder) -> Result<Iid, IndexError> {
    match order {
        SortOrder::ByIid => Ok(event.iid),
        SortOrder::BySrc => event.src.ok_or_else(|| {
            IndexError::Codec("edge event missing src endpoint in adjacency encode".into())
        }),
        SortOrder::ByDst => event.dst.ok_or_else(|| {
            IndexError::Codec("edge event missing dst endpoint in adjacency encode".into())
        }),
    }
}

fn encode_pages_by_keys(
    rows: &[Event],
    keys: &[Iid],
    page_rows: usize,
    page_path_levels: usize,
) -> Result<EncodedBlock, IndexError> {
    if rows.len() != keys.len() {
        return Err(IndexError::Codec("page key count mismatch".into()));
    }
    if page_path_levels > MAX_TRIE_LEVELS {
        return Err(IndexError::Codec(format!(
            "page path level {page_path_levels} exceeds 128-bit IID trie depth"
        )));
    }

    let mut data = Vec::new();
    let mut pages = Vec::new();
    let chunk = page_rows.max(1);
    for (i, events) in rows.chunks(chunk).enumerate() {
        let offset = data.len() as u64;
        let bytes = encode_events(events)?;
        data.extend_from_slice(&bytes);
        let key_chunk = &keys[i * chunk..i * chunk + events.len()];
        let mut meta = page_meta(events, offset, bytes.len() as u64);
        meta.path = shared_page_path(key_chunk, page_path_levels)?;
        // Record the SORT KEY's range, not row iids; adjacency scans use this
        // field for the anchor entity while primary scans use the row iid.
        if let Some(min_iid) = key_chunk.iter().min().copied() {
            meta.min_iid = min_iid;
        }
        if let Some(max_iid) = key_chunk.iter().max().copied() {
            meta.max_iid = max_iid;
        }
        pages.push(meta);
    }

    let meta = encode_meta(&pages)?;
    Ok(EncodedBlock { data, meta, pages })
}

fn shared_page_path(keys: &[Iid], page_path_levels: usize) -> Result<Vec<u8>, IndexError> {
    let Some(first) = keys.first() else {
        return Ok(Vec::new());
    };

    let mut path = Vec::with_capacity(page_path_levels);
    for level in 0..page_path_levels {
        let bucket = Bucketer::bucket(first, level).ok_or_else(|| {
            IndexError::Codec(format!(
                "page path level {page_path_levels} exceeds 128-bit IID trie depth"
            ))
        })?;
        if keys
            .iter()
            .skip(1)
            .all(|key| Bucketer::bucket(key, level) == Some(bucket))
        {
            path.push(bucket);
        } else {
            break;
        }
    }
    Ok(path)
}

/// Stats over one page's events. `chunks()` never yields an empty slice.
fn page_meta(events: &[Event], offset: u64, len: u64) -> PageMeta {
    let first = &events[0];
    let mut meta = PageMeta {
        path: Vec::new(),
        offset,
        len,
        rows: events.len() as u64,
        min_iid: first.iid,
        max_iid: first.iid,
        min_system_from: first.system_from,
        max_system_from: first.system_from,
        min_valid_from: first.valid_from,
        max_valid_from: first.valid_from,
        min_valid_to: first.valid_to,
        max_valid_to: first.valid_to,
        has_erase: false,
    };
    for e in events {
        meta.min_iid = meta.min_iid.min(e.iid);
        meta.max_iid = meta.max_iid.max(e.iid);
        meta.min_system_from = meta.min_system_from.min(e.system_from);
        meta.max_system_from = meta.max_system_from.max(e.system_from);
        meta.min_valid_from = meta.min_valid_from.min(e.valid_from);
        meta.max_valid_from = meta.max_valid_from.max(e.valid_from);
        meta.min_valid_to = meta.min_valid_to.min(e.valid_to);
        meta.max_valid_to = meta.max_valid_to.max(e.valid_to);
        meta.has_erase |= matches!(e.op, Op::Erase);
    }
    meta
}

fn meta_schema() -> Arc<Schema> {
    let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("len", DataType::UInt64, false),
        Field::new("rows", DataType::UInt64, false),
        Field::new("path", DataType::Binary, false),
        Field::new("min_iid", DataType::FixedSizeBinary(16), false),
        Field::new("max_iid", DataType::FixedSizeBinary(16), false),
        Field::new("min_system_from", ts(), false),
        Field::new("max_system_from", ts(), false),
        Field::new("min_valid_from", ts(), false),
        Field::new("max_valid_from", ts(), false),
        Field::new("min_valid_to", ts(), false),
        Field::new("max_valid_to", ts(), false),
        Field::new("has_erase", DataType::Boolean, false),
    ]))
}

fn encode_meta(pages: &[PageMeta]) -> Result<Vec<u8>, IndexError> {
    let schema = meta_schema();
    let mut buf = Vec::new();
    let mut writer = StreamWriter::try_new(&mut buf, &schema)?;
    if !pages.is_empty() {
        let mut offset_b = UInt64Builder::new();
        let mut len_b = UInt64Builder::new();
        let mut rows_b = UInt64Builder::new();
        let mut path_b = BinaryBuilder::new();
        let mut min_iid_b = FixedSizeBinaryBuilder::new(16);
        let mut max_iid_b = FixedSizeBinaryBuilder::new(16);
        let ts_builder = || TimestampMicrosecondBuilder::new().with_timezone("UTC");
        let mut min_sf_b = ts_builder();
        let mut max_sf_b = ts_builder();
        let mut min_vf_b = ts_builder();
        let mut max_vf_b = ts_builder();
        let mut min_vt_b = ts_builder();
        let mut max_vt_b = ts_builder();
        let mut erase_b = BooleanBuilder::new();
        for p in pages {
            offset_b.append_value(p.offset);
            len_b.append_value(p.len);
            rows_b.append_value(p.rows);
            path_b.append_value(&p.path);
            min_iid_b.append_value(p.min_iid.as_bytes())?;
            max_iid_b.append_value(p.max_iid.as_bytes())?;
            min_sf_b.append_value(p.min_system_from.as_micros());
            max_sf_b.append_value(p.max_system_from.as_micros());
            min_vf_b.append_value(p.min_valid_from.as_micros());
            max_vf_b.append_value(p.max_valid_from.as_micros());
            min_vt_b.append_value(p.min_valid_to.as_micros());
            max_vt_b.append_value(p.max_valid_to.as_micros());
            erase_b.append_value(p.has_erase);
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(offset_b.finish()),
            Arc::new(len_b.finish()),
            Arc::new(rows_b.finish()),
            Arc::new(path_b.finish()),
            Arc::new(min_iid_b.finish()),
            Arc::new(max_iid_b.finish()),
            Arc::new(min_sf_b.finish()),
            Arc::new(max_sf_b.finish()),
            Arc::new(min_vf_b.finish()),
            Arc::new(max_vf_b.finish()),
            Arc::new(min_vt_b.finish()),
            Arc::new(max_vt_b.finish()),
            Arc::new(erase_b.finish()),
        ];
        writer.write(&RecordBatch::try_new(schema.clone(), columns)?)?;
    }
    writer.finish()?;
    drop(writer);
    Ok(buf)
}

/// Deserializes a meta file written by `encode_block`.
///
/// Task-6 fuzzing found that arrow-rs 58.3.0's IPC `StreamReader` PANICS
/// (rather than returning an `Err`) on several classes of adversarial
/// flatbuffer/record-batch bytes — see `fuzz/regressions/block_meta/*.bin`.
/// No public arrow API avoids these panics (confirmed by reading
/// arrow-ipc's `convert`/`reader` source directly: `StreamReader::try_new`
/// calls the panicking `fb_to_schema` unconditionally, with no
/// `Result`-returning alternative). `decode_meta` is a pure function of
/// `bytes` with no shared mutable state, so this is the textbook legitimate
/// use of `catch_unwind`: it converts a third-party-library panic into a
/// clean `IndexError` at a pure-function trust boundary, rather than papering
/// over a bug of our own (owner-approved, narrow override of the plan's
/// general no-`catch_unwind` rule for this specific case).
///
/// Panic-hook note: the `catch_unwind` and its fuzz-only panic-hook handling
/// live in [`crate::codec::catch_arrow_panic`]. `libfuzzer-sys` installs a
/// process-global abort-before-unwind hook that would defeat `catch_unwind`
/// inside a `cargo fuzz` target, so — and ONLY under `--cfg fuzzing` — that
/// helper swaps a no-op hook around the call. Production and `cargo test`
/// builds do NOT touch the process-global hook: swapping it around a decode
/// that runs concurrently with other decodes raced (two interleaved swaps
/// could leave a no-op hook installed for good), so those builds use a bare
/// `catch_unwind` and simply let arrow's panic message reach stderr (harmless).
///
/// The `catch_unwind` guard handles arrow's UNWINDING panic classes only.
/// Fuzzing surfaced a fourth, distinct class it cannot reach: arrow's
/// `MessageReader::maybe_next` allocates a buffer sized from an unvalidated,
/// attacker-controlled `bodyLength`, so a malformed huge value ABORTS the
/// allocator with no unwind. [`crate::codec::validate_ipc_framing`] runs first
/// (outside the guard) to bound that allocation against the actual input size
/// before arrow sees the bytes.
pub fn decode_meta(bytes: &[u8]) -> Result<Vec<PageMeta>, IndexError> {
    crate::codec::validate_ipc_framing(bytes)?;
    match crate::codec::catch_arrow_panic(|| decode_meta_uncaught(bytes)) {
        Ok(result) => result,
        Err(_) => Err(IndexError::Codec(
            "arrow IPC decode panicked (corrupt input)".into(),
        )),
    }
}

fn decode_meta_uncaught(bytes: &[u8]) -> Result<Vec<PageMeta>, IndexError> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)?;
    if reader.schema() != meta_schema() {
        return Err(IndexError::Codec("meta file schema mismatch".into()));
    }
    let mut pages = Vec::new();
    for batch in reader {
        let batch = batch?;
        let offset = downcast::<UInt64Array>(&batch, 0)?;
        let len = downcast::<UInt64Array>(&batch, 1)?;
        let rows = downcast::<UInt64Array>(&batch, 2)?;
        let path = downcast::<BinaryArray>(&batch, 3)?;
        let min_iid = downcast::<FixedSizeBinaryArray>(&batch, 4)?;
        let max_iid = downcast::<FixedSizeBinaryArray>(&batch, 5)?;
        let min_sf = downcast::<TimestampMicrosecondArray>(&batch, 6)?;
        let max_sf = downcast::<TimestampMicrosecondArray>(&batch, 7)?;
        let min_vf = downcast::<TimestampMicrosecondArray>(&batch, 8)?;
        let max_vf = downcast::<TimestampMicrosecondArray>(&batch, 9)?;
        let min_vt = downcast::<TimestampMicrosecondArray>(&batch, 10)?;
        let max_vt = downcast::<TimestampMicrosecondArray>(&batch, 11)?;
        let has_erase = downcast::<BooleanArray>(&batch, 12)?;
        for row in 0..batch.num_rows() {
            let trie_path = path.value(row);
            if trie_path.len() > MAX_TRIE_LEVELS {
                return Err(IndexError::Codec(format!(
                    "meta trie path has {} levels, maximum is {MAX_TRIE_LEVELS}",
                    trie_path.len()
                )));
            }
            if let Some(bucket) = trie_path
                .iter()
                .find(|bucket| **bucket >= TRIE_BRANCH_FACTOR)
            {
                return Err(IndexError::Codec(format!(
                    "meta trie path bucket {bucket} outside branch factor {TRIE_BRANCH_FACTOR}"
                )));
            }
            let iid_at = |arr: &FixedSizeBinaryArray, i: usize| -> Result<Iid, IndexError> {
                let bytes: [u8; 16] = arr
                    .value(i)
                    .try_into()
                    .map_err(|_| IndexError::Codec("meta iid is not 16 bytes".into()))?;
                Ok(Iid::from_bytes(bytes))
            };
            pages.push(PageMeta {
                path: trie_path.to_vec(),
                offset: offset.value(row),
                len: len.value(row),
                rows: rows.value(row),
                min_iid: iid_at(min_iid, row)?,
                max_iid: iid_at(max_iid, row)?,
                min_system_from: Instant::from_micros(min_sf.value(row)),
                max_system_from: Instant::from_micros(max_sf.value(row)),
                min_valid_from: Instant::from_micros(min_vf.value(row)),
                max_valid_from: Instant::from_micros(max_vf.value(row)),
                min_valid_to: Instant::from_micros(min_vt.value(row)),
                max_valid_to: Instant::from_micros(max_vt.value(row)),
                has_erase: has_erase.value(row),
            });
        }
    }
    Ok(pages)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::decode_events;
    use crate::event::{Event, Op};
    use crate::live::LiveTable;
    use crate::scan::{snapshot_entities, LabelFilter};
    use std::collections::BTreeMap;
    use varve_types::{Doc, Iid, Instant, TemporalBounds, TemporalDimension, Value};

    const EOT: Instant = Instant::END_OF_TIME;

    fn iid(n: u8) -> Iid {
        Iid::derive("g", "nodes", &[n])
    }

    fn us(n: i64) -> Instant {
        Instant::from_micros(n)
    }

    fn put(entity: u8, sf: i64, vf: i64, vt: Instant, seq: i64) -> Event {
        let mut doc = Doc::new();
        doc.insert("seq".into(), Value::Int(seq));
        Event {
            iid: iid(entity),
            system_from: us(sf),
            valid_from: us(vf),
            valid_to: vt,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".into()],
                doc,
            },
        }
    }

    fn erase(entity: u8, sf: i64) -> Event {
        Event {
            iid: iid(entity),
            system_from: us(sf),
            valid_from: Instant::MIN,
            valid_to: EOT,
            src: None,
            dst: None,
            op: Op::Erase,
        }
    }

    fn table(events: &[Event]) -> LiveTable {
        let mut t = LiveTable::new();
        for e in events {
            t.append(e.clone()).unwrap();
        }
        t
    }

    fn at(n: i64) -> TemporalBounds {
        TemporalBounds {
            valid: TemporalDimension::at(us(n)),
            system: TemporalDimension::at(us(n)),
        }
    }

    /// Edge event with endpoints (Task 2 helper shape): edge iid `n`, `src`/
    /// `dst` node iids, arrival `at`. Used by the adjacency-family encode tests.
    fn edge(n: u8, src: u8, dst: u8, at: i64) -> Event {
        Event {
            iid: Iid::derive("g", "edges", &[n]),
            system_from: us(at),
            valid_from: us(at),
            valid_to: EOT,
            src: Some(Iid::derive("g", "nodes", &[src])),
            dst: Some(Iid::derive("g", "nodes", &[dst])),
            op: Op::Put {
                labels: vec!["KNOWS".into()],
                doc: BTreeMap::new(),
            },
        }
    }

    fn raw_iid(first_byte: u8, tag: u8) -> Iid {
        let mut bytes = [0; 16];
        bytes[0] = first_byte;
        bytes[15] = tag;
        Iid::from_bytes(bytes)
    }

    fn put_raw(iid: Iid, at: i64) -> Event {
        Event {
            iid,
            system_from: us(at),
            valid_from: us(at),
            valid_to: EOT,
            src: None,
            dst: None,
            op: Op::Put {
                labels: vec!["P".into()],
                doc: Doc::new(),
            },
        }
    }

    fn edge_raw(iid: Iid, src: Iid, dst: Iid, at: i64) -> Event {
        Event {
            iid,
            system_from: us(at),
            valid_from: us(at),
            valid_to: EOT,
            src: Some(src),
            dst: Some(dst),
            op: Op::Put {
                labels: vec!["KNOWS".into()],
                doc: BTreeMap::new(),
            },
        }
    }

    #[test]
    fn encoded_meta_records_leaf_paths() {
        let rows = vec![
            put_raw(raw_iid(0b0000_0000, 1), 1),
            put_raw(raw_iid(0b0100_0000, 2), 2),
            put_raw(raw_iid(0b1000_0000, 3), 3),
        ];

        let block = encode_sorted_events_by(&rows, 1, SortOrder::ByIid, 1).unwrap();

        assert_eq!(block.pages.len(), rows.len());
        assert_eq!(
            block
                .pages
                .iter()
                .map(|page| page.path.clone())
                .collect::<Vec<_>>(),
            rows.iter()
                .map(|row| Bucketer::path(&row.iid, 1).unwrap())
                .collect::<Vec<_>>()
        );
        assert_eq!(decode_meta(&block.meta).unwrap(), block.pages);

        let shared_prefix_rows = vec![
            put_raw(raw_iid(0b0000_0000, 4), 4),
            put_raw(raw_iid(0b0010_0000, 5), 5),
        ];
        let shared_prefix =
            encode_sorted_events_by(&shared_prefix_rows, 2, SortOrder::ByIid, 2).unwrap();

        assert_eq!(shared_prefix.pages.len(), 1);
        assert_eq!(
            shared_prefix.pages[0].path,
            vec![Bucketer::bucket(&shared_prefix_rows[0].iid, 0).unwrap()]
        );
        assert_ne!(
            shared_prefix.pages[0].path,
            Bucketer::path(&shared_prefix_rows[0].iid, 2).unwrap()
        );
    }

    #[test]
    fn by_src_adjacency_paths_use_sort_key_not_edge_iid() {
        let src_a = raw_iid(0b0000_0000, 10);
        let src_b = raw_iid(0b0100_0000, 11);
        let dst = raw_iid(0b0000_0000, 12);
        let rows = vec![
            edge_raw(raw_iid(0b1100_0000, 20), src_a, dst, 1),
            edge_raw(raw_iid(0b1000_0000, 21), src_b, dst, 2),
        ];

        let block = encode_sorted_events_by(&rows, 1, SortOrder::BySrc, 1).unwrap();

        assert_eq!(block.pages.len(), 2);
        assert_eq!(block.pages[0].path, Bucketer::path(&src_a, 1).unwrap());
        assert_eq!(block.pages[1].path, Bucketer::path(&src_b, 1).unwrap());
        assert_ne!(
            block.pages[0].path,
            Bucketer::path(&rows[0].iid, 1).unwrap()
        );
        assert_ne!(
            block.pages[1].path,
            Bucketer::path(&rows[1].iid, 1).unwrap()
        );
        assert_eq!(
            (block.pages[0].min_iid, block.pages[0].max_iid),
            (src_a, src_a)
        );
        assert_eq!(
            (block.pages[1].min_iid, block.pages[1].max_iid),
            (src_b, src_b)
        );
    }

    #[test]
    fn encode_by_src_sorts_and_stats_by_src() {
        let mut live = LiveTable::new();
        // src 30 first by arrival, src 10 second — BySrc must reorder.
        live.append(edge(1, 30, 40, 1)).unwrap();
        live.append(edge(2, 10, 20, 2)).unwrap();
        live.append(Event {
            op: Op::Delete,
            ..edge(2, 10, 20, 3)
        })
        .unwrap();
        let block = encode_block_by(&live, 1024, SortOrder::BySrc).unwrap();
        assert_eq!(block.pages.len(), 1);
        let page = &block.pages[0];
        assert_eq!(page.min_iid, Iid::derive("g", "nodes", &[10]));
        assert_eq!(page.max_iid, Iid::derive("g", "nodes", &[30]));
        let events = crate::codec::decode_events(
            &block.data[page.offset as usize..(page.offset + page.len) as usize],
        )
        .unwrap();
        // (src asc, iid asc, system_from desc): edge 2's two events (delete first) then edge 1.
        assert_eq!(events[0].iid, Iid::derive("g", "edges", &[2]));
        assert!(matches!(events[0].op, Op::Delete));
        assert_eq!(events[2].iid, Iid::derive("g", "edges", &[1]));
    }

    /// Mirrors `encode_by_src_sorts_and_stats_by_src` for the `In` family's
    /// `SortOrder::ByDst`: same shape (arrival order reversed by the sort
    /// key, a delete landing before its put via `system_from desc`), but
    /// keyed on `dst` instead of `src` — this direction was never exercised
    /// by the original BySrc-only coverage.
    #[test]
    fn encode_by_dst_sorts_and_stats_by_dst() {
        let mut live = LiveTable::new();
        // dst 30 first by arrival, dst 10 second — ByDst must reorder.
        live.append(edge(1, 100, 30, 1)).unwrap();
        live.append(edge(2, 200, 10, 2)).unwrap();
        live.append(Event {
            op: Op::Delete,
            ..edge(2, 200, 10, 3)
        })
        .unwrap();
        let block = encode_block_by(&live, 1024, SortOrder::ByDst).unwrap();
        assert_eq!(block.pages.len(), 1);
        let page = &block.pages[0];
        assert_eq!(page.min_iid, Iid::derive("g", "nodes", &[10]));
        assert_eq!(page.max_iid, Iid::derive("g", "nodes", &[30]));
        let events = crate::codec::decode_events(
            &block.data[page.offset as usize..(page.offset + page.len) as usize],
        )
        .unwrap();
        // (dst asc, iid asc, system_from desc): edge 2's two events (delete first) then edge 1.
        assert_eq!(events[0].iid, Iid::derive("g", "edges", &[2]));
        assert!(matches!(events[0].op, Op::Delete));
        assert_eq!(events[2].iid, Iid::derive("g", "edges", &[1]));
    }

    #[test]
    fn encode_by_src_without_endpoints_errors() {
        let mut live = LiveTable::new();
        live.append(Event {
            src: None,
            dst: None,
            ..edge(1, 0, 0, 1)
        })
        .unwrap();
        assert!(encode_block_by(&live, 1024, SortOrder::BySrc).is_err());
    }

    #[test]
    fn primary_encode_is_unchanged_by_the_refactor() {
        let mut live = LiveTable::new();
        live.append(edge(1, 30, 40, 1)).unwrap();
        live.append(edge(2, 10, 20, 2)).unwrap();
        let a = encode_block(&live, 1024).unwrap();
        let b = encode_block_by(&live, 1024, SortOrder::ByIid).unwrap();
        assert_eq!(a.data, b.data);
        assert_eq!(a.meta, b.meta);
    }

    #[test]
    fn encode_decode_round_trips_pages_and_meta() {
        // 3 entities × 3 events each, page_rows = 2 → 5 pages over 9 rows.
        // Loop nested sf-major/entity-minor so the push sequence is globally
        // non-decreasing in system_from (LiveTable::append's log-order
        // invariant) while each entity's own events still arrive sf-ascending
        // — identical per-entity arrival order to an entity-major loop, just
        // interleaved with other entities' same-sf events.
        let mut events = Vec::new();
        for sf in [1i64, 2, 3] {
            for entity in [1u8, 2, 3] {
                events.push(put(entity, sf, sf, EOT, sf));
            }
        }
        let live = table(&events);
        let block = encode_block(&live, 2).unwrap();

        assert_eq!(block.pages.len(), 5);
        assert_eq!(block.pages.iter().map(|p| p.rows).sum::<u64>(), 9);
        // The meta file round-trips the page index exactly.
        assert_eq!(decode_meta(&block.meta).unwrap(), block.pages);

        // Every page is a self-contained IPC stream at its recorded range;
        // file order is (_iid asc, _system_from desc per entity).
        let mut all = Vec::new();
        for page in &block.pages {
            let bytes = &block.data[page.offset as usize..(page.offset + page.len) as usize];
            let page_events = decode_events(bytes).unwrap();
            assert_eq!(page_events.len() as u64, page.rows);
            all.extend(page_events);
        }
        for pair in all.windows(2) {
            assert!(
                pair[0].iid < pair[1].iid
                    || (pair[0].iid == pair[1].iid && pair[0].system_from >= pair[1].system_from),
                "file order violated"
            );
        }

        // Reassembling per entity and reversing restores arrival order.
        let mut per_entity: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
        for e in all {
            per_entity.entry(e.iid).or_default().push(e);
        }
        for (iid, desc) in per_entity {
            let asc: Vec<Event> = desc.into_iter().rev().collect();
            assert_eq!(asc.as_slice(), live.events_for(&iid).unwrap());
        }
    }

    #[test]
    fn page_meta_stats_are_per_page() {
        let events = [put(1, 5, 3, us(30), 0), put(1, 7, 4, EOT, 1)];
        let block = encode_block(&table(&events), 16).unwrap();
        assert_eq!(block.pages.len(), 1);
        let p = &block.pages[0];
        assert_eq!((p.min_iid, p.max_iid), (iid(1), iid(1)));
        assert_eq!((p.min_system_from, p.max_system_from), (us(5), us(7)));
        assert_eq!((p.min_valid_from, p.max_valid_from), (us(3), us(4)));
        assert_eq!((p.min_valid_to, p.max_valid_to), (us(30), EOT));
        assert!(!p.has_erase);

        let with_erase = encode_block(&table(&[put(1, 5, 3, EOT, 0), erase(1, 6)]), 16).unwrap();
        assert!(with_erase.pages[0].has_erase);
    }

    /// Regression for Task 6's fuzzing: `fb.fields().unwrap()` panic in
    /// `arrow_ipc::convert::fb_to_schema` on a Schema flatbuffer that omits
    /// its `fields` vector. Pre-fix, this input made `decode_meta` unwind
    /// the process; post-fix, the `catch_unwind` guard converts it to a
    /// clean `Err`.
    #[test]
    fn decode_meta_rejects_fb_to_schema_panic_bytes() {
        let bytes: &[u8] =
            include_bytes!("../../../fuzz/regressions/block_meta/fb-to-schema-unwrap.bin");
        assert!(decode_meta(bytes).is_err());
    }

    /// Regression for Task 6's fuzzing: `arrow_buffer::Buffer::slice_with_length`
    /// offset-exceeds-length panic inside `RecordBatchDecoder`, triggered by
    /// malformed buffer offset/length metadata in a record batch message.
    #[test]
    fn decode_meta_rejects_buffer_slice_panic_bytes() {
        let bytes: &[u8] =
            include_bytes!("../../../fuzz/regressions/block_meta/buffer-slice-panic.bin");
        assert!(decode_meta(bytes).is_err());
    }

    /// Round-trip over a meta stream carrying TWO record-batch messages, so the
    /// framing walk steps over two consecutive NON-zero `bodyLength` messages
    /// and still reaches EOS. `encode_meta` only ever emits one record batch,
    /// so this reuses a real batch written twice to build the multi-body
    /// stream. Guards against a body-advance bug that a single-batch stream
    /// (schema + one batch + EOS) would not surface.
    #[test]
    fn decode_meta_walks_multi_batch_stream() {
        // page_rows = 1 over 3 entities → 3 pages → a meta batch with 3 rows.
        let block = encode_block(
            &table(&[
                put(1, 1, 1, EOT, 0),
                put(2, 2, 2, EOT, 0),
                put(3, 3, 3, EOT, 0),
            ]),
            1,
        )
        .unwrap();
        assert_eq!(block.pages.len(), 3);

        let mut reader = StreamReader::try_new(std::io::Cursor::new(&block.meta), None).unwrap();
        let batch = reader.next().unwrap().unwrap();

        let schema = meta_schema();
        let mut buf = Vec::new();
        {
            let mut writer = StreamWriter::try_new(&mut buf, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let pages = decode_meta(&buf).unwrap();
        assert_eq!(pages.len(), block.pages.len() * 2);
        assert_eq!(&pages[..block.pages.len()], &block.pages[..]);
        assert_eq!(&pages[block.pages.len()..], &block.pages[..]);
    }

    /// Allocation-bound regression: a meta stream whose record-batch message
    /// declares a `bodyLength` of ~1.15 EB. Pre-fix, arrow's
    /// `MessageReader::maybe_next` would `MutableBuffer::from_len_zeroed(..)`
    /// that many bytes; the allocation fails and `handle_alloc_error` ABORTS the
    /// process — no unwind, so `catch_unwind` could not intercept it (this test
    /// would kill the runner). Post-fix, `validate_ipc_framing` bounds the body
    /// length against the input and returns a clean `Err`.
    #[test]
    fn decode_meta_rejects_unbounded_body_length() {
        let block =
            encode_block(&table(&[put(1, 1, 1, EOT, 0), put(2, 2, 2, EOT, 0)]), 16).unwrap();
        let corrupt = crate::codec::corrupt_body_length_to_huge(&block.meta);
        assert!(matches!(decode_meta(&corrupt), Err(IndexError::Codec(_))));
    }

    #[test]
    #[ignore = "regenerates the committed fuzz seed corpus"]
    fn write_block_meta_fuzz_seed() {
        let events = [put(1, 5, 3, us(30), 0), put(1, 7, 4, EOT, 1)];
        let block = encode_block(&table(&events), 16).unwrap();
        let dir =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fuzz/corpus/block_meta");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("valid.bin"), &block.meta).unwrap();
    }

    #[test]
    fn empty_table_encodes_no_pages() {
        let block = encode_block(&LiveTable::new(), 4).unwrap();
        assert!(block.pages.is_empty());
        assert!(block.data.is_empty());
        assert_eq!(decode_meta(&block.meta).unwrap(), vec![]);
    }

    #[test]
    fn malformed_page_paths_are_rejected_without_panicking_selection() {
        let block = encode_block(&table(&[put(1, 1, 1, EOT, 0)]), 16).unwrap();
        let mut page = block.pages[0].clone();

        page.path = vec![0; MAX_TRIE_LEVELS + 1];
        assert!(!page.selected(&at(10), Some(&iid(1))));
        let err = decode_meta(&encode_meta(&[page.clone()]).unwrap()).unwrap_err();
        assert!(err.to_string().contains("maximum is 64"));

        page.path = vec![TRIE_BRANCH_FACTOR];
        assert!(!page.selected(&at(10), Some(&iid(1))));
        let err = decode_meta(&encode_meta(&[page]).unwrap()).unwrap_err();
        assert!(err.to_string().contains("outside branch factor 4"));
    }

    #[test]
    fn iid_point_prunes_only_foreign_pages() {
        let block =
            encode_block(&table(&[put(1, 1, 1, EOT, 0), put(3, 2, 2, EOT, 0)]), 16).unwrap();
        let page = &block.pages[0];
        assert!(page.selected(&at(10), Some(&iid(1))));
        assert!(page.selected(&at(10), Some(&iid(3))));
        assert!(page.selected(&at(10), None));
        // iid(2) may sort inside or outside [min,max] — pick a definitely-outside probe.
        let all_iids = [iid(1), iid(2), iid(3)];
        let outside = *all_iids.iter().max().unwrap();
        if outside != iid(1) && outside != iid(3) {
            assert!(!page.selected(&at(10), Some(&outside)));
        }
        // Deterministic outside probe: anything beyond max_iid.
        let mut beyond = *page.max_iid.as_bytes();
        if beyond != [0xff; 16] {
            for b in beyond.iter_mut().rev() {
                if *b < 0xff {
                    *b += 1;
                    break;
                }
                *b = 0;
            }
            assert!(!page.selected(&at(10), Some(&Iid::from_bytes(beyond))));
        }
    }

    /// The system-axis prune is EXACTLY output-preserving: resolve() ignores
    /// events at/after the system upper bound even when present, so dropping
    /// a page whose every event is at/after the bound changes nothing.
    #[test]
    fn system_upper_prune_is_output_identical() {
        let old = put(1, 1, 0, EOT, 0);
        let newer = put(1, 20, 0, EOT, 1); // supersedes, but only from system 20
        let bounds = TemporalBounds {
            valid: TemporalDimension::at(us(5)),
            system: TemporalDimension::at(us(10)), // upper = 11 < 20
        };

        // Page containing only `newer` is prunable...
        let newer_block = encode_block(&table(std::slice::from_ref(&newer)), 16).unwrap();
        assert!(!newer_block.pages[0].selected(&bounds, None));
        // ...and the page containing `old` is not.
        let old_block = encode_block(&table(std::slice::from_ref(&old)), 16).unwrap();
        assert!(old_block.pages[0].selected(&bounds, None));

        // Output equivalence: [old] alone == [old, newer] under these bounds.
        let pruned_events = [old.clone()];
        let full_events = [old.clone(), newer.clone()];
        let pruned = snapshot_entities(
            vec![(iid(1), &pruned_events[..])],
            LabelFilter::Single("P"),
            &bounds,
        )
        .unwrap();
        let full = snapshot_entities(
            vec![(iid(1), &full_events[..])],
            LabelFilter::Single("P"),
            &bounds,
        )
        .unwrap();
        assert_eq!(pruned, full);
        assert!(pruned.is_some());
    }

    /// An erase at system 20 hides history even when querying AS OF system 10
    /// (slice-2 GDPR decision) — its page must survive the system-axis prune.
    #[test]
    fn erase_pages_are_never_pruned_on_the_system_axis() {
        let block = encode_block(&table(&[erase(1, 20)]), 16).unwrap();
        let bounds = TemporalBounds {
            valid: TemporalDimension::at(us(5)),
            system: TemporalDimension::at(us(10)),
        };
        assert!(block.pages[0].min_system_from >= bounds.system.upper);
        assert!(
            block.pages[0].selected(&bounds, None),
            "erase page must be scanned"
        );
    }

    /// Regression guard for a subtle bug this plan almost shipped: an event
    /// whose valid range is DISJOINT from the query window still clips the
    /// reported _valid_to of visible rows, so the valid axis must not prune.
    #[test]
    fn valid_axis_is_deliberately_not_pruned() {
        let base = put(1, 1, 0, EOT, 0); // valid [0, ∞) from system 1
        let correction = put(1, 2, 20, us(30), 1); // valid [20, 30) from system 2

        // The correction's page has valid range [20, 30) — disjoint from a
        // query at valid 5 — yet selected() must keep it:
        let block = encode_block(&table(std::slice::from_ref(&correction)), 16).unwrap();
        let bounds = TemporalBounds {
            valid: TemporalDimension::at(us(5)),
            system: TemporalDimension::at(us(10)),
        };
        assert!(block.pages[0].selected(&bounds, None));

        // ...because with it, the visible row's _valid_to is clipped to 20:
        use arrow::array::TimestampMicrosecondArray;
        let events = [base, correction];
        let batch = snapshot_entities(
            vec![(iid(1), &events[..])],
            LabelFilter::Single("P"),
            &bounds,
        )
        .unwrap()
        .unwrap();
        let vt: &TimestampMicrosecondArray = batch
            .column_by_name("_valid_to")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(
            vt.value(0),
            20,
            "correction outside the window still clips valid_to"
        );
    }
}
