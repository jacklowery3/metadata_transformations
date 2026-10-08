# dbt-utils macros: `get_relations_by_prefix`, `deduplicate`, `generate_series`

Three macros from the [dbt-utils triage](../../docs/dbt_utils_triage.md),
each compared with what DuckDB + duckdb-iceberg (1.5.6, nightly `5dcf5070c`)
does today. Cases are in `*.cases.json`; private cases appear only by alias.

Run: `experiments/run_all.sh`, 2026-10-08 04:28-04:40 UTC, sequential,
medians.

## get_relations_by_prefix

[`get_relations_by_prefix.py`](get_relations_by_prefix.py), 5 reps. This
also covers `get_relations_by_pattern`, which uses the same
`get_tables_by_pattern_sql`.

**Finding 1, it doesn't work out of the box.** On dbt-duckdb with an attached
Iceberg catalog, the macro errors. dbt-utils queries
`<database>.information_schema.tables`, but DuckDB's `information_schema` is
global, not per attached database. The ten-line shim in
[`../dbt_fivetran_log/macros/`](../dbt_fivetran_log/macros) fixes it.

**Finding 2, timing.** Polaris REST listing vs the shimmed SQL:

| case | tables | metadata (REST) | DuckDB query | DuckDB attach (one-off) |
|---|---|---|---|---|
| `fivetran_log`, `destination*` | 7 | **224 ms** (2 calls) | 653 ms | 325 ms |
| `%` (all namespaces), `source_*` | 7 | **223 ms** (6 calls, parallel) | 649 ms | 299 ms |
| `fivetran_log`, `source_*` excluding `%change%` | 4 | **209 ms** (2 calls) | 566 ms | 306 ms |
| `fivetran%`, `%_change_event` (pattern) | 6 | **191 ms** (2 calls) | 640 ms | 318 ms |
| all namespaces, prefix P (private) | 7 | **229 ms** (6 calls) | 768 ms | 407 ms |

All reps returned identical table sets. **About 3× faster, but only
~0.4 s per call.** DuckDB lists the entire catalog on every query (uncached);
REST lists only matching namespaces (`_` is an ILIKE wildcard, so
`fivetran_log` costs a namespace listing too). It matters when called many
times at compile time, and it avoids engine-specific SQL.

## deduplicate

[`deduplicate.py`](deduplicate.py), 3 reps per variant, each built as a table
on a fresh connection.

**Rule:** if `partition_by` covers the table's declared Iceberg
`identifier-field-ids`, each partition holds one row, so dedup is
`select *`. Deciding this takes one `loadTable` call (0.2-0.5 s). Iceberg
doesn't enforce identifier fields, so the premise is re-checked with SQL: it
held in every case (rows = distinct partitions).

| relation, `partition_by` | metadata | correct rows | `select *` | QUALIFY | **dbt-utils default** |
|---|---|---|---|---|---|
| tickets, `ticket_id` (declared key) | no-op proven | 6,900 | 9.7 s | 9.4 s | 8.7 s, **4,855 rows ✗** |
| tickets, `ticket_id, status` | no-op proven | 6,900 | 9.4 s | 9.4 s | 10.4 s, **4,855 rows ✗** |
| tickets, `customer_id` (not a key) | can't prove, so run it | 200 | n/a | 9.9 s, 200 | 9.2 s, **130 rows ✗** |
| log on the local mirror, `id, time_stamp` | no-op proven | 211,027 | **56 ms** | 125 ms | 181 ms, **208,291 rows ✗** |
| `sensor_data.readings`, declared key | no-op proven | 735,933 | 5.7 s | 5.4 s | 4.8 s, **0 rows ✗** |

**Finding 1, correctness: dbt-utils' default `deduplicate` drops every row
containing a NULL.** dbt-duckdb gets `default__deduplicate`, which re-joins
with `NATURAL JOIN` across all columns, and `NULL = NULL` is never true. On
tickets the 2,045 rows with any NULL vanish; a table with an all-NULL column
dedups to **zero rows**. This is known upstream
([#621](https://github.com/dbt-labs/dbt-utils/issues/621),
[#824](https://github.com/dbt-labs/dbt-utils/issues/824), closed as not
planned). The metadata rule avoids it for the common case (dedup on the
primary key); otherwise use `QUALIFY`.

**Finding 2, time: little to trim at this scale.** Reading from Iceberg
dominates, so skipping the window function changes nothing visible (about
9 s either way on tickets). On local data the window is the whole cost:
56 ms vs 125 ms vs 181 ms. The saving grows with row count.

## generate_series

[`generate_series.py`](generate_series.py), 3 reps.

**The macro itself: not applicable.** It cross-joins powers of two and
touches no table: 1,000 rows in about 23 ms, 100,000 in about 15 ms.

**Its inputs: yes.** Spines take their start/end from
`run_query("select min(ts) …")` at compile time (dbt_fivetran_log does
`min(set_up_at)`). For timestamp columns, MIN/MAX are exact from manifest
bounds:

| column | files | metadata (manifests) | SQL `min()/max()` | Parquet GETs | speedup |
|---|---|---|---|---|---|
| `fivetran_log.connection.signed_up` (the package's spine) | 2 | 1.1 s | 1.2 s | 2 | 1.1× |
| `sensor_data.readings.synced_at` | 101 | 1.0 s | 4.5 s | 102 | 4.8× |
| `ticket_data.tickets.created_at` | 257 | 1.9 s | 10.1 s | 257 | 5.3× |
| `fivetran_log.log.time_stamp` | 2,341 | **2.0 s** | **75.0 s** | 2,344 | **38×** |

All agree with SQL **to the microsecond**. DuckDB doesn't answer `min`/`max`
from Iceberg stats; it reads every file. Compile-time bound queries are paid
on every `dbt compile`/`run`, so this is directly on the dev loop.
