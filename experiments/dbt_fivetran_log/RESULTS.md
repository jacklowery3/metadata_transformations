# `fivetran/dbt_fivetran_log` on dbt-duckdb: baseline vs snapshot-diff mirror

**Question:** running [fivetran/dbt_fivetran_log](https://github.com/fivetran/dbt_fivetran_log)
(v2.5.x: 19 models, 8 tests) locally with dbt-duckdb against Fivetran's
Platform Connector tables in Polaris, how long does a build take, and how much
can Iceberg metadata trim?

**Answer (from the run cited below):** a full build takes about **90 s,
every time**. With a mirror kept current by snapshot diffs, a daily rerun
takes **3.8 s (sync) + 4.6 s (build) = 8.4 s, about 11× faster**, with
**identical outputs** in all 7 models.

## Setup

- dbt-core 1.12.5, dbt-duckdb 1.11.0, DuckDB 1.5.6 + duckdb-iceberg nightly
  `5dcf5070c`, 4 threads. Sources are read from the attached catalog; models
  are built into a local `.duckdb` file. Nothing is written to the catalog.
- Variables disable sources this catalog doesn't have (`destination_membership`,
  `transformation_runs`, `credits_used`, `usage_cost`); see
  [`dbt_project.yml`](dbt_project.yml).
- [`mirror.py`](mirror.py): for each source table, it compares the live file
  list at a snapshot (manifests only) with the files already mirrored (a
  `_src_file` column). It reads only **added** files and deletes rows of
  **removed** files, because Fivetran rewrites whole files to apply updates.
  When nothing has changed, it reads no data at all.
- [`compare_outputs.py`](compare_outputs.py): row counts plus an
  order-independent hash over every row, model by model.

Run: `experiments/run_all.sh`, 2026-10-08 04:23-04:28 UTC, sequential.

## Results

| scenario | time |
|---|---|
| baseline `dbt build`, reading Iceberg directly | **90.3 s**, **91.6 s** (a third attempt failed; see below) |
| mirror, cold (state as of 24 h ago) | 55.5 s |
| **mirror, 24 h delta to now** | **3.8 s** (log: +24 files; incremental_mar: +5 / −2; the 4 small tables each rewritten once) |
| **mirror, no-op** | **3.2 s** (no data read) |
| `dbt build` on the mirror | **4.6 s**, **3.3 s** |

| dev-loop scenario | baseline | with metadata | speedup |
|---|---|---|---|
| daily rerun: delta sync + build | ~91 s | 8.4 s | ≈11× |
| rerun with nothing new: no-op sync + build | ~91 s | 6.5 s | ≈14× |
| first-ever run: cold mirror + build | ~91 s | 60.1 s | ≈1.5× |

`compare_outputs.py`: all 7 models identical (audit_table 56,808 rows,
audit_user_activity 120, connection_daily_events 2,080, connection_status 7,
mar_table_history 402, schema_changelog 62, usage_history 16).

## Where the baseline's time goes

On DuckDB the package can't run incrementally, and its staging models are
views, so every build re-reads every source:

- `connection_status` and `audit_user_activity`: about **76 s each**, in
  parallel, both cold-reading all ~2,340 `fivetran_log.log` files (about 90
  rows per file). Later log models are fast only because DuckDB's
  in-process file cache is warm by then.
- `mar_table_history`: about 18 s on ~557 tiny `incremental_mar` files.
- Views and tests: 1-4 s each, mostly catalog round trips.

The models filter on a `CASE`-computed `event_subtype`, so neither DuckDB nor
per-file stats can prune `log`. The fix isn't pruning; it's not re-reading
what hasn't changed.

## A failed baseline attempt (reported, not counted)

The first baseline build in this run failed after 13.8 s with HTTP errors
reading three tables' `metadata.json`. It started at 04:23:35, right as
Fivetran's hourly sync committed. The writer keeps only 3 previous metadata
files, so the version DuckDB had resolved was pruned mid-read. Any engine
reading a live catalog during a sync can hit this; a retry succeeded. The
mirror reads at a fixed snapshot and then works locally, so it's exposed for
seconds, not minutes.

## What would trim more (not built)

- Skip unchanged tables by comparing snapshot ids before reading manifests.
  A no-op sync should drop from about 3 s to about 0.5 s.
- Do the sync in Rust: 2-5× faster than pyiceberg for the same planning and reads in
  [../expression_is_true](../expression_is_true/RESULTS.md).
- The cold mirror (55 s) is bounded by fragmentation (one request per small
  file). Metadata can't merge files; after the first sync, the mirror acts as
  a local compaction.

## Caveats

- Part of the 8.4 s is reading new data. Metadata decides what to read and
  skips the rest.
- The mirror assumes copy-on-write with **no delete files** (true for this
  writer). If a snapshot ever adds delete files, it should fall back to a
  full refresh of that table (not implemented).
- `connection_daily_events` uses `current_timestamp` for its date spine, so
  identical outputs need both builds on the same day and snapshot.
