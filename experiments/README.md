# Experiments

Each experiment compares an Iceberg-metadata approach against the SQL a dbt
user would run today (DuckDB + duckdb-iceberg, or dbt-duckdb), on **time to
the same answer**, and checks that the answers match.

| experiment | question | write-up |
|---|---|---|
| [`source_tests/`](source_tests) | A realistic dbt source test suite (41 tests + 12 freshness checks): how much can metadata decide, and how fast? | [RESULTS.md](source_tests/RESULTS.md) |
| [`dbt_fivetran_log/`](dbt_fivetran_log) | Running `fivetran/dbt_fivetran_log` locally on dbt-duckdb: how long does it take, and what does a snapshot-diff mirror save? | [RESULTS.md](dbt_fivetran_log/RESULTS.md) |
| [`expression_is_true/`](expression_is_true) | Deciding `dbt_utils.expression_is_true` from manifest stats: pyiceberg vs an iceberg-rust port vs DuckDB | [RESULTS.md](expression_is_true/RESULTS.md) |
| [`dbt_utils/`](dbt_utils) | `get_relations_by_prefix`, `deduplicate`, `generate_series` | [RESULTS.md](dbt_utils/RESULTS.md) |

## Setup (once)

From the repo root:

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
python3 -m venv .venv-dbt && .venv-dbt/bin/pip install dbt-core==1.12.5 dbt-duckdb==1.11.0
cp .env.example .env                       # Polaris catalog-integration credentials
.venv/bin/python scripts/setup_duckdb.py   # Iceberg extensions (core_nightly) + DuckDB secret
(cd experiments/source_tests && ../../.venv-dbt/bin/dbt deps --profiles-dir .)
(cd experiments/dbt_fivetran_log && ../../.venv-dbt/bin/dbt deps --profiles-dir .)
```

The Rust port additionally needs `cargo` (Rust ≥ 1.95) and network access to
fetch iceberg-rust (see `expression_is_true/rust/setup_vendor.sh`).

Then either run each experiment as its own write-up describes, or everything
sequentially (roughly 40-60 minutes, mostly the SQL baselines):

```bash
experiments/run_all.sh
```

## Cases, private data and raw results

- Cases live in `*.json` next to each script. Cases that touch private
  datasets go in `*.local.json` (and dbt tests in `models/private/`), which
  are **gitignored**. A case's optional `"alias"` is printed and saved instead
  of its real table and expression, so outputs from private cases are safe
  to quote.
- Raw outputs go to each experiment's `results/` (gitignored). The
  `RESULTS.md` files summarise the run they cite.

## How to read the numbers

- Single machine (a laptop) reading S3 over the internet; absolute times will
  differ elsewhere. Every comparison is between runs in the same session,
  run **sequentially** so they don't compete for network.
- "Cold" means a fresh table load or a fresh DuckDB connection per rep, so
  neither side reuses another rep's caches. Medians are reported.
- The datasets are small (≤ ~1.4M rows per table) but **fragmented** (tens
  to hundreds of rows per file), so time is dominated by per-file requests,
  not bytes.
