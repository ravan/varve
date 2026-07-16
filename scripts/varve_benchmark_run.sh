#!/usr/bin/env bash
# Evidence-complete Varve benchmark runner for the SLES/Podman benchmark VM.
set -u

source_root=${VARVE_BENCH_SOURCE_ROOT:-$(pwd)}
artifacts=${VARVE_BENCH_ARTIFACTS:?set VARVE_BENCH_ARTIFACTS}
image=${VARVE_BENCH_IMAGE:-localhost/varve-bench:rust-1.97}
overall=0

mkdir -p "${artifacts}/commands" "${artifacts}/environment" "${artifacts}/coverage"

snapshot() {
    name=$1
    shift
    "$@" >"${artifacts}/environment/${name}.stdout" 2>"${artifacts}/environment/${name}.stderr"
    printf '%s\n' "$?" >"${artifacts}/environment/${name}.exit"
}

snapshot uname uname -a
snapshot os-release cat /etc/os-release
snapshot uptime uptime
snapshot cpu lscpu
snapshot memory free -h
snapshot block lsblk -o NAME,SIZE,TYPE,FSTYPE,MOUNTPOINTS
snapshot disk df -h
snapshot podman-version podman version
snapshot image-inspect podman image inspect "${image}"

printf '%s\n' "${VARVE_SOURCE_COMMIT:-unknown}" >"${artifacts}/environment/source-commit.txt"
printf '%s\n' "${VARVE_SOURCE_TREE:-unknown}" >"${artifacts}/environment/source-tree.txt"
(
    cd "${source_root}" || exit 1
    sha256sum Cargo.lock Justfile scripts/varve_benchmark_run.sh \
        crates/varve-types/benches/trie.rs \
        crates/varve-index/benches/resolution.rs \
        crates/varve-gql/benches/parse.rs \
        crates/varve/examples/write_bench.rs \
        crates/varve/examples/block_bench.rs \
        crates/varve/examples/cache_bench.rs \
        crates/varve/examples/traversal_bench.rs \
        crates/varve/examples/social_bench.rs \
        crates/varve/examples/object_store_tx_bench.rs \
        crates/varve-server/tests/scale_out_bench.rs
) >"${artifacts}/environment/source-sha256.txt" 2>"${artifacts}/environment/source-sha256.stderr"

run_case() {
    name=$1
    command=$2
    env_file=${3:-}
    case_dir="${artifacts}/commands/${name}"
    mkdir -p "${case_dir}"
    printf '%s\n' "${command}" >"${case_dir}/command.txt"
    date -u +%Y-%m-%dT%H:%M:%SZ >"${case_dir}/started_at.txt"

    podman_args=(run --rm --network host -v "${source_root}:/work:Z" -w /work)
    if [ -n "${env_file}" ]; then
        podman_args+=(--env-file "${env_file}")
    fi
    podman_args+=("${image}" bash -c 'exec /usr/bin/time -v bash -c "$1"' _ "${command}")
    podman "${podman_args[@]}" >"${case_dir}/stdout.log" 2>"${case_dir}/stderr.log"
    code=$?
    printf '%s\n' "${code}" >"${case_dir}/exit_code.txt"
    date -u +%Y-%m-%dT%H:%M:%SZ >"${case_dir}/finished_at.txt"
    uptime >"${case_dir}/host_load_after.txt"
    if [ "${code}" -ne 0 ]; then
        overall=1
    fi
}

run_case build-examples 'cargo build --locked --release --examples'
run_case build-criterion 'cargo bench --locked -p varve-index -p varve-types -p varve-gql --no-run'
run_case build-scale-out 'cargo test --locked -p varve-server --release --test scale_out_bench --no-run'
run_case benchmark-contract 'cargo test --locked -p varve --test benchmark_contract'
run_case criterion 'cargo bench --locked -p varve-index -p varve-types -p varve-gql'
run_case write-bench 'target/release/examples/write_bench'
run_case block-bench 'target/release/examples/block_bench'
run_case cache-bench 'target/release/examples/cache_bench'
run_case traversal-bench 'target/release/examples/traversal_bench'
run_case social-bench 'target/release/examples/social_bench'
run_case scale-out-bench 'bin=$(find target/release/deps -maxdepth 1 -type f -name "scale_out_bench-*" -perm -111 | head -n 1); test -n "$bin"; VARVE_SCALE_BENCH=1 "$bin" --nocapture --test-threads=1'

if [ -n "${VARVE_S3_ENV_FILE:-}" ]; then
    run_case object-store-tx 'target/release/examples/object_store_tx_bench' "${VARVE_S3_ENV_FILE}"
else
    printf '%s\n' 'not_measured: VARVE_S3_ENV_FILE not supplied' >"${artifacts}/coverage/object-store-tx.txt"
fi

# Bulk ingest (xtdb-style data ops via Db::ingest): the fast loader. Runs at
# full 1M/6M scale unconditionally — it completes in well under a minute on
# reference hardware (26s / ~267k entities/s on an 8-core dev machine), so it
# needs no admission gate. VARVE_TRAVERSAL_COMPACT=1 drains compaction to
# idle before the query phases (bulk load → compact → serve): the warm 2-hop
# exit criterion is measured against the compacted steady state, not the
# all-L0 shape a bulk load leaves behind.
run_case traversal-1m-bulk 'VARVE_TRAVERSAL_INGEST=bulk VARVE_TRAVERSAL_COMPACT=1 VARVE_TRAVERSAL_PEOPLE=1000000 VARVE_TRAVERSAL_FRIENDSHIPS=6000000 VARVE_TRAVERSAL_NODE_BATCH=10000 VARVE_TRAVERSAL_EDGE_BATCH=10000 target/release/examples/traversal_bench'

if [ "${VARVE_RUN_TRAVERSAL_1M:-0}" = 1 ]; then
    run_case traversal-1m 'VARVE_TRAVERSAL_PEOPLE=1000000 VARVE_TRAVERSAL_FRIENDSHIPS=6000000 target/release/examples/traversal_bench'
else
    printf '%s\n' 'not_measured: VARVE_RUN_TRAVERSAL_1M is not 1 (GQL loader needs the ETA admission gate)' >"${artifacts}/coverage/traversal-1m.txt"
fi

if [ -d "${source_root}/target/criterion" ]; then
    cp -a "${source_root}/target/criterion" "${artifacts}/criterion"
fi
date -u +%Y-%m-%dT%H:%M:%SZ >"${artifacts}/finished_at.txt"
exit "${overall}"
