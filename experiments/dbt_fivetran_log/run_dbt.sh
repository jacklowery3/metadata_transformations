#!/usr/bin/env bash
# Usage: ./run_dbt.sh <label> <dbt args...>      e.g. ./run_dbt.sh test_1 test
# Times the dbt command; saves run_results.json (and sources.json) under results/.
# DBT: dbt executable (default: dbt on PATH); PYTHON: repo venv python.
set -euo pipefail
cd "$(dirname "$0")"
REPO="$(cd ../.. && pwd)"
eval "$("${PYTHON:-$REPO/.venv/bin/python}" "$REPO/scripts/polaris_env.py")"
label="$1"; shift
mkdir -p results
start=$(python3 -c 'import time; print(time.time())')
"${DBT:-dbt}" "$@" --profiles-dir . || status=$?
end=$(python3 -c 'import time; print(time.time())')
cp target/run_results.json "results/$label.json" 2>/dev/null || true
[ -f target/sources.json ] && cp target/sources.json "results/${label}_sources.json" || true
python3 -c "print(f'WALL $label: {$end-$start:.1f}s')"
exit ${status:-0}
