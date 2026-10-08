# Findings

What we learned about a Fivetran Managed Data Lake Service (MDLS)
destination, its Polaris catalog, and the engines and dbt packages around
it. These findings come from one catalog (5 namespaces, 66 tables, mostly
small: the largest are about 0.2-1.4M rows) surveyed **2026-10-06 to
2026-10-08** from metadata and manifests. Writer behaviour can change;
re-check with `scripts/survey_catalog.py` and `scripts/inspect_bounds.py`.

Tables named below are either from Fivetran's standard Platform Connector
schema (`fivetran_log.*`) or a synthetic support-ticket dataset
(`ticket_data.*`). A third, private dataset is referred to only as
`sensor_data` with generic table and column names.

## How Fivetran MDLS writes Iceberg

1. **Nothing was partitioned or sorted.** Every table had an empty partition
   spec and sort order 0, so engines rely on per-file column bounds to prune.
2. **Copy-on-write only, with no delete files.** `total-delete-files = 0`
   everywhere, and table properties set `write.{delete,update,merge}.mode =
   copy-on-write`. Snapshot `total-records` is therefore an exact row count
   (including soft-deleted rows, which keep `_fivetran_deleted = true`).
   Updates retire whole files and add new ones.
3. **Snapshots are labelled with their sync.** Summaries carry
   `fivetran_sync_id`, `fivetran_sync_count`, `fivetran_flush_id` and
   `fivetran_flush_count`, alongside the standard `added-records` and
   `added-data-files`.
4. **Per-file bounds exist for every column** (Fivetran documents this for
   tables of ≤200 columns), plus value and null counts on recent files.
5. **Files are naturally clustered.** Every data file checked covered exactly
   one UTC day of `_fivetran_synced`, and in `fivetran_log.log` 84% of files
   held a single `connection_id`. That makes `day()` / identity partitioning
   applicable without rewriting (roadmap B1), and lets many checks be
   settled per file.
6. **Every table declares its primary key** as Iceberg
   `identifier-field-ids` (for example `log` = `(id, time_stamp)`, `tickets` =
   `ticket_id`). Iceberg doesn't enforce them, but uniqueness held on every
   table checked, as expected from an upsert-by-key writer.
7. **String bounds are truncated at 32 characters**, not Iceberg's default 16
   (no `write.metadata.metrics.*` property is set; 32-character bounds were
   observed on long values). Bounds shorter than the truncation length are
   real values.
8. **No NaN counts** are written for any column.
9. **Statistics completeness changed over time.** In one long-lived table,
   files written before mid-May 2026 had bounds but **no value counts, null
   counts or column sizes**; later files had all three. Checks involving
   NULLs can't be decided from those older files.
10. **The only puffin statistics blob is Fivetran's own
    `fivetran-synced-distribution`** (on `_fivetran_synced`); there are no
    theta sketches, so no NDV from metadata.
11. **Old metadata JSON files are pruned** (`write.metadata.previous-versions-max
    = 3`), but snapshots are retained (40-80 per table here), so time travel
    reaches back that far.

## Polaris

12. **Advertised REST endpoints** (Polaris 1.7.0): table CRUD, `register`,
    `tables/rename`, `transactions/commit` (atomic multi-table), table
    `metrics`, **Iceberg views** (CRUD, rename, `register-view`), plus Polaris
    generic tables and policies. The catalog-integration principal's grants
    suggest write access, but **no write has been exercised by this
    project**.
13. Credentials are vended per table load (`X-Iceberg-Access-Delegation:
    vended-credentials`); no static AWS keys are needed.

14. **Reading during a sync can fail.** Fivetran's hourly sync commits a new
    `metadata.json` and the writer keeps only 3 previous versions, so a long
    read that resolved an older version can hit HTTP errors when that file is
    pruned mid-read. Observed once (a dbt build started as the sync committed;
    a retry succeeded). Short reads at a fixed snapshot are far less exposed.

## Engines and libraries

15. **pyiceberg 0.11.1 can't load these tables as-is.** It rejects the
    `fivetran-synced-distribution` blob type with a `ValidationError`.
    `mdt.catalog.get_catalog()` strips unknown blob types from the REST
    response.
16. **iceberg-rust (`ab047b5`)** loads them fine (the blob type is a plain
    string), but:
    - its strict metrics evaluator treats a **missing NaN count as "may contain
      NaN" for every type**, so with Fivetran's files (no NaN counts) its
      "every row matches" proofs never fire; the Rust port works around this;
    - the metrics evaluators and manifest cache are crate-private (the port
      patches a vendored copy);
    - the application must enable TLS for its `reqwest` (`rustls-tls`).
17. **DuckDB 1.5.6 + duckdb-iceberg (nightly `5dcf5070c`)** uses bounds to
    skip files for simple one-sided predicates, but reads **every** file for
    `NOT BETWEEN`, `!=`, `NOT IN`, `IS NULL` on files without null counts, and
    for plain `MIN`/`MAX` aggregates. Its `information_schema` lists the whole
    catalog on every query (about 0.55 s here), uncached.

## dbt and dbt-utils on DuckDB with an attached Iceberg catalog

18. **`dbt_utils.get_relations_by_prefix` / `_by_pattern` error out.** The
    default SQL queries `<database>.information_schema.tables`, but DuckDB's
    `information_schema` is global, not per attached database. A ten-line
    `duckdb__get_tables_by_pattern_sql` shim fixes it
    (`experiments/dbt_fivetran_log/macros/`).
19. **`dbt_utils.deduplicate` silently drops every row containing a NULL.**
    dbt-utils has no `duckdb__deduplicate`, and the default re-joins with a
    `NATURAL JOIN`, where `NULL = NULL` is never true. A table with an
    all-NULL column dedups to **zero rows**. Known upstream
    ([#621](https://github.com/dbt-labs/dbt-utils/issues/621),
    [#824](https://github.com/dbt-labs/dbt-utils/issues/824), closed as not
    planned).
20. **`fivetran/dbt_fivetran_log` can't run incrementally on DuckDB**
    (`is_incremental_compatible()` is false for it), and its staging models
    are views, so every build re-reads the sources.
