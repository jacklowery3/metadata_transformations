# A dbt source test suite: `dbt test` vs metadata-first

**Question:** a typical source test suite on Fivetran-landed Iceberg tables,
run with dbt-duckdb, versus the same suite decided from Iceberg metadata. How
long does each take to reach the same verdicts?

**Answer (from the run cited below):** the tests take **about 31 s instead of
about 119 s (3.8×)** and freshness **about 6.3 s instead of 95-126 s
(15-20×)**, with **identical verdicts and failure counts on all 41 tests and
identical freshness on all 12 sources**. 32 of the 41 tests were decided
without reading any Parquet data.

## Setup

- **Suite:** 41 tests plus 12 freshness checks on 12 source tables.
  [`models/sources.yml`](models/sources.yml) holds 34 of the tests and 10
  sources (Fivetran Platform Connector `fivetran_log.*` and synthetic
  `ticket_data.*`). The other 7 tests and 2 sources are on a private dataset
  in `models/private/` (gitignored), shown below only as `<private>`.
- **Test types:** `unique`, `not_null`, `accepted_values`, `relationships`,
  `dbt_utils.accepted_range`, `dbt_utils.unique_combination_of_columns`, and
  freshness on `_fivetran_synced` (warn 12-24 h, error 48-72 h).
- **Expected failures:** two, which make sure the FAIL path gets exercised.
  `log.connection_id → connection` (log keeps events of deleted
  connections), and a private `accepted_values` list missing one newer value.
- **Baseline:** dbt-core 1.12.5 + dbt-duckdb 1.11.0, DuckDB 1.5.6 +
  duckdb-iceberg nightly `5dcf5070c`, 4 threads, catalog attached directly.
- **Metadata-first:** [`run_metadata.py`](run_metadata.py) on
  [`mdt/decide.py`](../../mdt/decide.py) (pyiceberg 0.11.1), 8 workers.
  - **Phase 1** decides every test from metadata. Each table is loaded and
    planned once.
  - **Phase 2** reads each table **once**, for every test still undecided,
    then counts failures with the dbt test's own logic in local DuckDB.
  - It reads the same YAML, applies the same semantics and reports the same
    failure counts. Agreement is checked by [`compare.py`](compare.py).

Run: `experiments/run_all.sh`, 2026-10-08 04:03-04:12 UTC, two reps each,
sequential.

## Results

| | rep 1 | rep 2 |
|---|---|---|
| `dbt test` (41 tests) | 117.5 s | 119.7 s |
| **metadata-first tests** | **31.7 s** (metadata phase 13.3 s) | **30.6 s** (metadata phase 12.3 s) |
| `dbt source freshness` (12) | 95.5 s | 126.1 s |
| **metadata freshness** | **6.4 s** | **6.3 s** |

`compare.py`, both reps:

```
tests: 41 compared (7 private), disagreements: 0
  decided by: {'metadata': 20, 'narrowed scan': 9, 'metadata (declared key)': 11, 'metadata (+parent keys)': 1}
  failing: [('<private>', 1), ('relationships|fivetran_log.log|connection_id', 7)]
freshness: 12 sources, same status 12/12, identical max_loaded_at 12/12
```

## How each test was decided

| decided by | tests | how |
|---|---|---|
| metadata (stats) | 20 | null counts; exact bounds vs range; single-valued files |
| metadata (declared key) | 11 | `unique` / `unique_combination_of_columns` on columns covering the table's Iceberg `identifier-field-ids` |
| metadata + parent keys | 1 | `log.connection_id → connection` **FAIL proven without reading `log`**: some files' exact min/max are connection ids missing from the parent (dbt counted 7 orphan rows) |
| narrowed scan | 9 | 6 `accepted_values` and 3 `relationships`, where every file mixes values so stats can't settle it |

The 9 scans were batched into **4 table reads**, each once for all its tests
(rep 2):

| table | files read | columns | read time |
|---|---|---|---|
| `fivetran_log.log` | 382 of 2,341 (only multi-valued files) | 1 | 12.4 s |
| `ticket_data.tickets` | 257 | 5 (5 tests) | 5.0 s |
| `ticket_data.ticket_messages` | 257 | 2 | 18.3 s |
| `<private>` | 47 | 1 | 13.6 s |

## Where the remaining ~31 s goes

- **Metadata phase, about 12-13 s:** loading and planning 12 tables (1-5 s
  each, in parallel) and reading parent key sets.
- **Batched reads, about 18 s on the critical path:** these tables hold
  tens of rows per file, so time goes on per-file requests, not bytes.

## What would trim it further (not built)

1. Run phase 2 against the snapshot-diff mirror
   ([../dbt_fivetran_log](../dbt_fivetran_log/RESULTS.md)), which syncs in
   about 4 s, so the residual reads become local queries.
2. Port the engine to Rust. The iceberg-rust port was 2-5× faster than pyiceberg at
   the same work ([../expression_is_true](../expression_is_true/RESULTS.md)).
3. Better clustering or compaction on the writer side would let more files
   be settled from stats.

## Caveats

- `unique` on the declared key trusts the writer to upsert by that key.
  Iceberg doesn't enforce identifier fields. It held everywhere it was
  checked, including here: dbt passed all 11.
- Metadata-proven FAILs report a lower bound (`>=N`). That's fine for dbt's
  default `error_if: != 0`; `warn_if`/`error_if` thresholds and
  `store_failures` would need a count from a narrowed scan.
- `where:` configs and custom tests aren't supported yet.
- Freshness compares against "now" at run time; dbt and metadata agree on
  `max_loaded_at` exactly, and on status here.
