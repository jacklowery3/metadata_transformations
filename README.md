# metadata_transformations

What can be done **in Apache Iceberg metadata** (snapshots, manifests,
per-file column stats, declared keys) to get the same answer as costly SQL,
faster? This repo is the experiments and the small library behind them,
measured on a Fivetran Managed Data Lake Service (MDLS) destination with a
Fivetran-hosted Polaris REST catalog.

It builds on
[unified_lake_tables](https://github.com/fivetran-jacklowery/unified_lake_tables),
which replaced `UNION ALL` across tenants with zero-copy manifest splicing.
That project is one instance of the idea; this one looks for the rest.

**North star: faster development, same answer.** Every result here is checked
against the SQL it replaces. When metadata can't decide, the answer is "run
the SQL", not a guess.

## Results so far

All measured on 2026-10-08 with [`experiments/run_all.sh`](experiments/run_all.sh),
against DuckDB 1.5.6 + duckdb-iceberg (nightly) and dbt-duckdb 1.11.0. Every
row reached **the same answer** as the baseline.

| what | baseline | metadata-first | write-up |
|---|---|---|---|
| a dbt **source test suite**: 41 tests (`unique`, `not_null`, `accepted_values`, `relationships`, …) | `dbt test`: 118-120 s | **31 s** (3.8×); 32 of 41 decided with no data read | [source_tests](experiments/source_tests/RESULTS.md) |
| **source freshness**, 12 sources | `dbt source freshness`: 95-126 s | **6.3 s** (15-20×) | [source_tests](experiments/source_tests/RESULTS.md) |
| **`fivetran/dbt_fivetran_log`**, daily rerun | `dbt build`: ~91 s every run | **8.4 s** (≈11×): snapshot-diff sync + local build, identical outputs | [dbt_fivetran_log](experiments/dbt_fivetran_log/RESULTS.md) |
| **`dbt_utils.expression_is_true`**, 10 cases | DuckDB SQL: 0.8-9 s; 67 s on a 2,341-file table | iceberg-rust: **0.4-0.7 s**; 3.4-5.0 s on that table | [expression_is_true](experiments/expression_is_true/RESULTS.md) |
| **spine bounds** (compile-time `min()`/`max()`) | 1.2-75 s | **1.0-2.0 s** (up to 38×), exact to the µs | [dbt_utils](experiments/dbt_utils/RESULTS.md#generate_series) |
| `get_relations_by_prefix` | 0.57-0.77 s (and **errors** without a shim) | 0.19-0.23 s | [dbt_utils](experiments/dbt_utils/RESULTS.md#get_relations_by_prefix) |
| `deduplicate` on the declared key | the dbt-utils default **drops rows with NULLs** on DuckDB | proven no-op (`select *`) | [dbt_utils](experiments/dbt_utils/RESULTS.md#deduplicate) |

Bugs and quirks found along the way (pyiceberg can't load Fivetran tables
as-is, iceberg-rust's NaN handling, DuckDB pruning gaps, two dbt-utils
problems on DuckDB) are in [docs/FINDINGS.md](docs/FINDINGS.md).

## How it works

[`mdt/decide.py`](mdt/decide.py) answers "does any row violate X?" per file,
from manifest stats:

- **prune** files that can't violate, and **prove** files where every row
  violates (Iceberg's inclusive and strict metrics evaluators);
- **witnesses:** an exact bound is a real value in the file, and a positive
  null count is a real NULL, so one violating value proves FAIL;
- **single-valued files** (min = max) are decided by that value;
- **declared keys:** uniqueness on Iceberg `identifier-field-ids` holds;
- anything left is read once per table, only the undecided files, and
  counted with the original test's own logic.

The snapshot-diff mirror ([`experiments/dbt_fivetran_log/mirror.py`](experiments/dbt_fivetran_log/mirror.py))
uses the same idea for builds: compare snapshot file lists, read only added
files, drop rows of removed ones, and read nothing when nothing changed.

Where it's going: [docs/ROADMAP.md](docs/ROADMAP.md), ending in a dbt
adapter that sits on the catalog and calls a query engine only when it must.

## Repository layout

```
mdt/              library: Polaris catalog (incl. a Fivetran blob workaround), manifest stats,
                  decide engine, DuckDB baseline sessions, experiment case loading
scripts/          survey_catalog.py, inspect_bounds.py (read-only), setup_duckdb.py, polaris_env.py
experiments/      one folder per experiment, each with RESULTS.md; run_all.sh reruns everything
docs/             FINDINGS.md, ROADMAP.md, dbt_utils_triage.md
```

## Quick start

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
cp .env.example .env    # Polaris catalog-integration credentials (OAuth client credentials only)
.venv/bin/python scripts/survey_catalog.py              # tables, files, snapshots: catalog JSON only
.venv/bin/python scripts/inspect_bounds.py ns.table     # per-column manifest stats: no Parquet reads
```

Credentials are in the Fivetran UI under Destinations → *your MDLS
destination* → Catalog Integration. Polaris vends short-lived S3 credentials
per table, so no AWS keys are used. To rerun the experiments, see
[experiments/README.md](experiments/README.md).

## Scope and caveats

- **One catalog, small, fragmented data** (≤ ~1.4M rows per table, tens to
  hundreds of rows per file), one laptop reading S3 over the internet.
  Speedups depend on file layout and network; the "same answer" checks don't.
- **Fivetran-specific assumptions**, each stated where it's used:
  copy-on-write with no delete files, primary keys declared as identifier
  fields (trusted, not enforced by Iceberg), 32-character string bounds.
- **Read-only so far.** Nothing here writes to the catalog; the
  zero-rewrite table ideas in the roadmap are untested.
- Prototypes: pyiceberg 0.11.1 is pinned because private internals are used;
  the Rust port patches a vendored iceberg-rust.
- Metadata-proven failures report a lower bound (`>=N`), not dbt's exact
  count, and `where:` configs aren't supported yet.

## License

Apache License 2.0. See [LICENSE](LICENSE).
