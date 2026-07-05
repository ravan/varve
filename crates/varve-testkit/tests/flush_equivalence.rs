//! Roadmap slice 4: same op history, randomized flush points (and page
//! sizes) must yield IDENTICAL query results to the never-flushed table —
//! across random bounds, including erase/delete histories. The merge loop
//! below mirrors `varve-engine::scan::merged_snapshot` exactly (block order,
//! page pruning, per-entity reversal), minus the object store.
#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use std::collections::BTreeMap;
use varve_index::block::{encode_block, EncodedBlock};
use varve_index::{decode_events, snapshot_entities, Event, LiveTable};
use varve_testkit::strategy::{arb_bounds, arb_history};
use varve_types::{Iid, TemporalBounds};

fn cases() -> u32 {
    // 10k in CI; the nightly job raises this via PROPTEST_CASES (slice 2).
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
}

fn table(events: &[Event]) -> LiveTable {
    let mut t = LiveTable::new();
    for e in events {
        t.append(e.clone()).unwrap();
    }
    t
}

/// Sorted, deduped cut positions in `0..=len`.
fn split_points(idxs: &[prop::sample::Index], len: usize) -> Vec<usize> {
    let mut pts: Vec<usize> = idxs.iter().map(|i| i.index(len + 1)).collect();
    pts.sort_unstable();
    pts.dedup();
    pts
}

/// The engine's merge (scan.rs), minus the object store: blocks in ascending
/// (time) order, pages pruned by `selected(bounds, None)`, per-entity
/// reversal to arrival order, live events last.
fn merged(
    blocks: &[EncodedBlock],
    live: &LiveTable,
    bounds: &TemporalBounds,
) -> BTreeMap<Iid, Vec<Event>> {
    let mut merged: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
    for block in blocks {
        let mut per_block: BTreeMap<Iid, Vec<Event>> = BTreeMap::new();
        for page in block.pages.iter().filter(|p| p.selected(bounds, None)) {
            let bytes = &block.data[page.offset as usize..(page.offset + page.len) as usize];
            for event in decode_events(bytes).unwrap() {
                per_block.entry(event.iid).or_default().push(event);
            }
        }
        for (iid, desc) in per_block {
            merged
                .entry(iid)
                .or_default()
                .extend(desc.into_iter().rev());
        }
    }
    for (iid, events) in live.entities() {
        merged
            .entry(*iid)
            .or_default()
            .extend(events.iter().cloned());
    }
    merged
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), ..ProptestConfig::default() })]

    #[test]
    fn randomized_flush_points_do_not_change_query_results(
        history in arb_history(24),
        cut_idxs in proptest::collection::vec(any::<prop::sample::Index>(), 0..3),
        page_rows in 1..4usize,
        bounds in arb_bounds(),
    ) {
        // Reference: the whole history in one live table (slice-2 machinery,
        // itself equivalence-tested against the naive ReferenceStore).
        let expected = table(&history).snapshot_for_label("P", &bounds).unwrap();

        // Flushed variant: every segment before the last cut becomes a block.
        let pts = split_points(&cut_idxs, history.len());
        let mut blocks = Vec::new();
        let mut start = 0usize;
        for p in pts {
            let segment = &history[start..p];
            if !segment.is_empty() {
                blocks.push(encode_block(&table(segment), page_rows).unwrap());
            }
            start = p;
        }
        let live = table(&history[start..]);

        let sources = merged(&blocks, &live, &bounds);
        let got = snapshot_entities(
            sources.iter().map(|(iid, events)| (*iid, events.as_slice())),
            "P",
            &bounds,
        )
        .unwrap();
        prop_assert_eq!(got, expected);
    }
}
