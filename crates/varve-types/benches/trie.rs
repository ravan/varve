//! Spec §13.7 micro-bench: `trie::Bucketer` (bucket/path/contains) and
//! `Iid::derive` — the hot path walked on every trie-block lookup and on
//! every write's entity-id derivation.
//!
//! Benches are non-library targets (not under `#[cfg(test)]`), so
//! `clippy.toml`'s `allow-unwrap-in-tests` doesn't cover them even though the
//! workspace denies `clippy::unwrap_used`; allow it here explicitly.
#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, Criterion};
use varve_types::trie::Bucketer;
use varve_types::{Iid, Value};

fn bench_trie(c: &mut Criterion) {
    // Real `_id` -> `Iid` derivation path (see `varve-engine/src/writer.rs`):
    // `Value::id_bytes()` is the canonical, type-tagged encoding actually fed
    // to `Iid::derive`, not a raw `to_le_bytes()` slice.
    let iids: Vec<Iid> = (0..1024i64)
        .map(|i| Iid::derive("bench", "nodes", &Value::Int(i).id_bytes().unwrap()))
        .collect();
    let path = Bucketer::path(&iids[0], 4).unwrap();

    c.bench_function("bucketer/bucket_level3", |b| {
        b.iter(|| {
            iids.iter()
                .filter_map(|iid| Bucketer::bucket(iid, 3))
                .count()
        })
    });
    c.bench_function("bucketer/path_4_levels", |b| {
        b.iter(|| iids.iter().filter_map(|iid| Bucketer::path(iid, 4)).count())
    });
    c.bench_function("bucketer/contains", |b| {
        b.iter(|| {
            iids.iter()
                .filter(|iid| Bucketer::contains(&path, iid))
                .count()
        })
    });
    c.bench_function("iid/derive", |b| {
        b.iter(|| Iid::derive("bench", "nodes", b"benchmark-id"))
    });
}

criterion_group!(benches, bench_trie);
criterion_main!(benches);
