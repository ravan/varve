//! Spec §13.7 micro-bench: `parse_program` — the hot path walked once per
//! incoming statement/program before planning.
//!
//! Benches are non-library targets (not under `#[cfg(test)]`), so
//! `clippy.toml`'s `allow-unwrap-in-tests` doesn't cover them even though the
//! workspace denies `clippy::unwrap_used`; allow it here explicitly.
#![allow(clippy::unwrap_used)]

use criterion::{criterion_group, criterion_main, Criterion};
use varve_gql::parse_program;

const POINT: &str = "MATCH (p:Person {_id: 42}) RETURN p.name";
const TRAVERSAL: &str = "FOR SYSTEM_TIME AS OF TIMESTAMP '2020-01-01T00:00:00Z' \
    MATCH (a:Person {_id: 0})-[:KNOWS]->{1,3}(b:Person) \
    WHERE b.age > 21 AND b.name <> 'x' \
    RETURN DISTINCT b.name AS name ORDER BY name LIMIT 100";
const PROGRAM: &str = "INSERT (:Person {_id: 1, name: 'Ada'}); \
    MATCH (p:Person {_id: 1}) SET p.name = 'Lovelace'; \
    MATCH (p:Person {_id: 1}) RETURN p.name";

fn bench_parse(c: &mut Criterion) {
    for (name, src) in [
        ("point", POINT),
        ("traversal", TRAVERSAL),
        ("program", PROGRAM),
    ] {
        c.bench_function(&format!("parse/{name}"), |b| {
            b.iter(|| parse_program(src).unwrap())
        });
    }
}

criterion_group!(benches, bench_parse);
criterion_main!(benches);
