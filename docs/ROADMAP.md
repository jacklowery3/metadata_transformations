# Roadmap: Iceberg metadata in place of SQL

**North star: faster development. Get the same answer, or the same outcome,
faster.** Every idea is judged first by time-to-answer in the dev loop (a test
result, a freshness check, a rebuild that never has to run) and second by
bytes or compute saved. "The same" is non-negotiable: the same answer as the
SQL, or an explicit "can't decide from metadata, run the SQL".

**Thesis.** An Iceberg table is mostly metadata: snapshots, manifests,
per-file column stats, partition specs and declared identifier fields. Many
expensive SQL patterns are really questions *about* files, or reshuffles *of*
files, and can be answered or performed without reading or rewriting
Parquet.

[unified_lake_tables](https://github.com/fivetran-jacklowery/unified_lake_tables)
proved one case: `UNION ALL` across N tenants becomes manifest splicing. Its
lesson carries over: a good optimizer already prunes a lot, so claims need
benchmarks against a real engine (here DuckDB with duckdb-iceberg), not
intuition.

Status: `idea` · `next` · `tried` (prototype measured; see linked results) · `done` · `dropped`

---

## A. Answer queries from metadata (read-only, no data scan)

| # | SQL it replaces | Metadata source | Exact when | Status |
|---|---|---|---|---|
| A1 | `COUNT(*)` | snapshot `total-records` | no delete files (Fivetran MDLS writes copy-on-write) | idea |
| A2 | `COUNT(*) WHERE NOT _fivetran_deleted` | per-file bounds and counts of `_fivetran_deleted` | every file is single-valued on the flag | idea |
| A3 | `MIN`/`MAX(col)` | manifest lower/upper bounds | numeric/date/timestamp; strings only if untruncated | **tried**: [spine bounds](../experiments/dbt_utils/RESULTS.md#generate_series) |
| A4 | `COUNT(col)`, null counts | `value_counts − null_value_counts` | files carry these counts | tried (inside the test engine) |
| A5 | `MAX(_fivetran_synced)` freshness | upper bounds | timestamp bounds present | **tried**: [source freshness](../experiments/source_tests/RESULTS.md) |
| A6 | `GROUP BY DATE(_fivetran_synced)` row growth | snapshot history `added-records` + timestamps | append snapshots | idea |
| A7 | `WHERE _fivetran_synced > :watermark` (incremental) | diff two snapshots' file lists, read only added files | handles copy-on-write removals too | **tried**: [snapshot-diff mirror](../experiments/dbt_fivetran_log/RESULTS.md) |
| A8 | rows per sync / sync audit | snapshot summary `fivetran_sync_id` | always | idea |

## B. Metadata-only transformations (new tables, no data rewrite)

Build a new table, in a separate namespace, whose manifests point at existing
files. Never mutate Fivetran-managed source tables. Because Fivetran rewrites
whole files to apply updates and deletes (copy-on-write), derived tables need
unified_lake_tables-style orphan retirement to stay correct.

| # | Idea | Replaces | Status |
|---|---|---|---|
| B1 | **Retroactive partitioning.** Add a spec (e.g. `day(_fivetran_synced)`, `identity(connection_id)`) and re-register files whose bounds prove they fit; the rest stay on the old spec (Iceberg allows mixed specs). | full scans on time/tenant filters; CTAS into a partitioned copy | idea (high applicability: see FINDINGS #5) |
| B2 | **Zero-copy filtered table**: only files whose bounds prove every row matches. | `CREATE TABLE AS SELECT … WHERE`, per-tenant views | idea |
| B3 | **Projection / rename table** over the same field ids. | views that only rename/drop columns | idea |
| B4 | **Point-in-time clone** (register a snapshot's manifests, or Iceberg tags). | `CREATE TABLE AS` backups, "as of" queries | idea |
| B5 | **Manifest compaction** (rewrite manifests, not data). | faster planning for every query | idea |
| B6 | **Union** across tenants. | `UNION ALL` views | done in unified_lake_tables |
| B7 | **Metadata join pruning**: key bounds pick the fact files that can match. | full fact scans in joins | idea |

Caveat for all of B: per-file pruning already exists in engines (DuckDB uses
bounds), so the win may be mostly in planning, not reads. Measure first.

## C. Add metadata Fivetran doesn't write

| # | Idea | Replaces | Status |
|---|---|---|---|
| C1 | Theta sketches (puffin `apache-datasketches-theta-v1`), mergeable, so they can be maintained incrementally | `COUNT(DISTINCT col)`, `cardinality_equality` | idea |
| C2 | Per-file top-k or histograms | `GROUP BY low_card_col` | idea |
| C3 | `sort-order` metadata where files are already sorted | sorts, merge joins | idea |

## D. dbt-utils, utility by utility

The full triage and what was tested is in [dbt_utils_triage.md](dbt_utils_triage.md).
The main finding: **data tests are the richest vein.** A test is a yes/no
question that usually scans a whole table, while per-file stats often prove
PASS or FAIL outright, and the rest can be scanned narrowly. That led to the
[source test suite benchmark](../experiments/source_tests/RESULTS.md).

## E. A dbt adapter that sits on the catalog, not a compute engine

Every dbt adapter today sits on a compute engine. This one would sit on the
Iceberg REST catalog (Polaris): do as much of dbt as possible through
metadata, and call a query engine **only when it has to, with the engine
configurable**. Sections A-D are its features.

| dbt feature | Metadata implementation | Engine needed? | Status |
|---|---|---|---|
| `dbt test` (generic + dbt-utils tests) | `mdt.decide`: PASS/FAIL from stats and declared keys, else one batched narrowed read per table | only for undecided files | **tried**: [source tests](../experiments/source_tests/RESULTS.md) |
| `dbt source freshness` | exact `MAX(loaded_at)` from upper bounds | no | **tried**: same |
| source sync / local cache | snapshot-diff mirror | reads only added files | **tried**: [dbt_fivetran_log](../experiments/dbt_fivetran_log/RESULTS.md) |
| relation listing, `get_columns_in_relation` | catalog listing and schema | no | tried (`get_relations_by_prefix`) |
| `dbt docs generate` (`catalog.json`) | types plus free stats (rows, bytes, files, min/max/null%) | no | idea |
| `dbt clone` | Polaris `register` / zero-copy table | no | idea |
| `view` | Iceberg views via Polaris `/views` | to read | idea |
| custom `union` / `projection` / `filtered` | B6 / B3 / B2 | no | idea |
| `incremental` | snapshot diff (A7) then the engine transforms only the delta | delta only | partly tried (mirror) |
| `seed` | pyiceberg append | no | idea |
| `table` with arbitrary SQL | n/a | **yes: the fallback engine** | n/a |
| whole-run atomic publish | Polaris `transactions/commit` (multi-table) | no | idea |

Sketch of the configuration surface:

```yaml
my_lake:
  target: dev
  outputs:
    dev:
      type: polaris
      catalog_uri: ...
      warehouse: ...
      fallback_engine: duckdb      # duckdb | snowflake | spark | trino | none
      metadata_strict: false       # true: error instead of falling back
```

Build options:

- **Fastest prototype: extend dbt-duckdb (dbt v1).** DuckDB already attaches
  Polaris, so the fallback is built in and local. dbt-utils tests dispatch
  (`adapter.dispatch('test_…', 'dbt_utils')`), so an adapter can supply
  `<adapter>__test_…` macros that call a Python method that decides from
  metadata and return a trivial or narrowed query.
- **dbt v2 / Fusion (Rust).** dbt-core `main` is the Rust rewrite.
  iceberg-rust has the same building blocks (REST catalog, inclusive/strict
  metrics evaluators), and the [Rust port](../experiments/expression_is_true/rust)
  shows the logic carries over. Its Python binding (`pyiceberg-core`)
  currently exposes manifests and stats but not the evaluators.
- Check for prior art (Iceberg- or Polaris-aware dbt adapters) before
  building.

Open questions: dbt expects every node to compile to SQL; Iceberg views store
engine-specific SQL; write access to a Fivetran-managed catalog; exact failure
counts for `warn_if`/`error_if`/`store_failures`; keeping derived tables in
sync with copy-on-write sources.

## Other open questions

- **Decode `fivetran-synced-distribution`**, Fivetran's puffin blob on
  `_fivetran_synced`; it may already be a sync-time histogram.
- **Exactness contract.** Every metadata answer says why it's exact, or that
  it isn't.
- **Fragmentation is separate.** Small files (tens of rows per file) dominate
  read cost and limit how often stats can decide. Metadata can't merge files;
  a rewrite or the local mirror can.
- **Statistics completeness.** Older files in a long-lived table lacked null
  and value counts (FINDINGS #9); those can't be decided from metadata.
