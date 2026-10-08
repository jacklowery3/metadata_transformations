#!/usr/bin/env bash
# Reruns every experiment, sequentially (never concurrently, so timings don't
# contend). Raw outputs land in each experiment's results/ (gitignored).
# Expects: .venv (requirements.txt), .venv-dbt (dbt-duckdb), scripts/setup_duckdb.py
# already run, and `cargo` for the Rust port. Takes roughly 40-60 minutes.
# Fails fast if the DuckDB extensions are missing (run scripts/setup_duckdb.py).
set -uo pipefail
cd "$(dirname "$0")"
PY="$(cd .. && pwd)/.venv/bin/python"
export PYTHON="$PY" DBT="$(cd .. && pwd)/.venv-dbt/bin/dbt"
CARGO="${CARGO:-cargo}"
section() { echo; echo "=== $* ($(date -u +%H:%M:%S)Z)"; }

[ -d ../.duckdb_ext ] || { echo "missing .duckdb_ext: run scripts/setup_duckdb.py first" >&2; exit 1; }

section "source_tests: dbt baseline"
cd source_tests
for i in 1 2; do ./run_dbt.sh test_$i test | grep --line-buffered -E 'Done\.|WALL'; ./run_dbt.sh freshness_$i source freshness | grep --line-buffered -E 'Done\.|WALL'; done
section "source_tests: metadata-first"
for i in 1 2; do "$PY" run_metadata.py --only tests --label metadata_tests_$i | grep --line-buffered -E 'WALL|^fail'; "$PY" run_metadata.py --only freshness --label metadata_freshness_$i | grep --line-buffered -E 'WALL|^(warn|error)'; done
cd ..

section "expression_is_true: iceberg-rust"
(cd expression_is_true/rust && mkdir -p ../results && "$CARGO" run --release -q -- 3 > ../results/rust.jsonl)
section "expression_is_true: pyiceberg + duckdb"
(cd expression_is_true && "$PY" run.py 3 --rust results/rust.jsonl)

section "dbt_fivetran_log: dbt baseline"
cd dbt_fivetran_log
rm -f bench.duckdb mirror.duckdb bench_mirror.duckdb
for i in 1 2; do ./run_dbt.sh build_full_$i build | grep --line-buffered -E 'Done\.|WALL'; done
section "dbt_fivetran_log: mirror"
"$PY" mirror.py sync --as-of-hours-ago 24 --label cold_24h_ago
"$PY" mirror.py sync --label delta_24h
"$PY" mirror.py sync --label noop
./run_dbt.sh build_full_same_snapshot build | grep --line-buffered -E 'Done\.|WALL'
for i in 1 2; do ./run_dbt.sh build_mirror_$i build --target mirror --vars '{fivetran_platform_database: mirror}' | grep --line-buffered -E 'Done\.|WALL'; done
"$PY" compare_outputs.py
cd ..

section "dbt_utils"
(cd dbt_utils && "$PY" get_relations_by_prefix.py 5 && "$PY" deduplicate.py 3 && "$PY" generate_series.py 3)
section "done"
