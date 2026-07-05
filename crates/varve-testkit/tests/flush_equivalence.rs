//! Roadmap slice 4: same op history, randomized flush points (and page
//! sizes) must yield IDENTICAL query results to the never-flushed table —
//! across random bounds, including erase/delete histories. This test
//! decodes persisted blocks the same way `varve-engine::scan::merged_snapshot`
//! does (page pruning, per-block decode), minus the object store, then calls
//! the SHIPPED `varve_index::merge_sources` to merge them with the live
//! tail — so the property exercises the real merge core, not a copy of it.
#![allow(clippy::unwrap_used)]

use proptest::prelude::*;
use varve_index::block::{encode_block, EncodedBlock};
use varve_index::{decode_events, snapshot_entities, Event, LiveTable};
use varve_testkit::strategy::{arb_bounds, arb_history};
use varve_types::Iid;

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
        let mut encoded_blocks: Vec<EncodedBlock> = Vec::new();
        let mut start = 0usize;
        for p in pts {
            let segment = &history[start..p];
            if !segment.is_empty() {
                encoded_blocks.push(encode_block(&table(segment), page_rows).unwrap());
            }
            start = p;
        }
        let live = table(&history[start..]);

        // Decode each block's selected pages in file order (mirrors the
        // engine's page pruning + ranged decode, minus the object store),
        // then merge via the shipped `varve_index::merge_sources` core.
        let blocks: Vec<Vec<Event>> = encoded_blocks
            .iter()
            .map(|block: &EncodedBlock| {
                block
                    .pages
                    .iter()
                    .filter(|p| p.selected(&bounds, None))
                    .flat_map(|page| {
                        let bytes = &block.data
                            [page.offset as usize..(page.offset + page.len) as usize];
                        decode_events(bytes).unwrap()
                    })
                    .collect::<Vec<Event>>()
            })
            .collect();
        let live_events: Vec<(Iid, Vec<Event>)> = live
            .entities()
            .map(|(iid, events)| (*iid, events.to_vec()))
            .collect();

        let sources = varve_index::merge_sources(blocks, live_events);
        let got = snapshot_entities(
            sources.iter().map(|(iid, events)| (*iid, events.as_slice())),
            "P",
            &bounds,
        )
        .unwrap();
        prop_assert_eq!(got, expected);
    }
}
