# `dbt_utils.expression_is_true` from Iceberg metadata

**Question:** the macro compiles to `select 1 from <model> where
not(<expression>)` and passes when no rows come back. Can manifest stats
decide it without reading Parquet, and how fast is that versus running the
SQL?

**Answer (from the run cited below):** yes, for single-column predicates, and
**the verdict matches the SQL in every case and rep**. 7 of 10 cases are
decided with **no data read**, 2 by reading only the files stats couldn't
settle, and 1 (column vs column) correctly falls back to SQL. The iceberg-rust
port reaches the verdict in **0.4-0.7 s** where DuckDB takes up to 9 s, and in
**3.4-5.0 s vs 67 s** on the fragmented log table.

## How it decides

[`mdt/decide.py`](../../mdt/decide.py) `decide_expr`, and the same rules in
[`rust/src/main.rs`](rust/src/main.rs). For each live file:

1. **Prune:** pyiceberg's inclusive metrics evaluator on `NOT(expr)`. False
   means the file can't hold a violation.
2. **Proof:** the strict evaluator. True means every row in the file
   violates, so the test FAILs.
3. **Witness:** an *exact* bound is a value that really occurs in the file,
   and `null_count > 0` is a real NULL. If one of those violates, the test
   FAILs. String bounds count as exact only when shorter than the table's
   inferred truncation length.
4. **Single-valued file** (exact min = max): decided by that one value.
5. **Otherwise:** read only the undecided files.

SQL NULL semantics are kept: `NOT(expr)` is ANDed with `col IS NOT NULL`
unless the expression tests for NULL, because SQL never counts a NULL
comparison as a violation. Expressions that aren't a single-column Iceberg
predicate (column vs column, functions, arithmetic) are UNKNOWN, so run the
SQL.

## Setup

- **pyiceberg** 0.11.1 ([`run.py`](run.py)): a cold table load and plan each
  rep.
- **iceberg-rust** `ab047b5` ([`rust/`](rust)): a release build with the same
  rules. It needs a vendored copy with the evaluators made public
  (`setup_vendor.sh`) and parses the case expressions with a small grammar.
- **DuckDB** 1.5.6 + duckdb-iceberg nightly `5dcf5070c`: a fresh connection
  each rep, `select count(*) … where not (<expr>)`, with Parquet GETs counted
  from its HTTP log in the same execution.
- **Cases:** [`cases.json`](cases.json), plus 4 private cases shown by alias
  (`sensor_data.readings`, a 101-file table; `L1`, `L2`, `H` and `c1` stand
  in for real thresholds and values).

Run: `experiments/run_all.sh`, 2026-10-08 04:12-04:23 UTC, 3 reps per engine,
sequential. Medians:

| case | iceberg-rust | pyiceberg | DuckDB | DuckDB Parquet GETs | verdict (all agree) | decided by |
|---|---|---|---|---|---|---|
| `tickets: satisfaction_score between 1 and 5` | **719 ms** | 2,295 ms | 9,342 ms | 258 | PASS | metadata |
| `tickets: _fivetran_deleted = false` | **545 ms** | 2,373 ms | 9,193 ms | 257 | PASS | metadata (single-valued files) |
| `tickets: created_at >= '2024-01-01…'` | **464 ms** | 2,396 ms | 2,106 ms | 0 | PASS | metadata |
| `tickets: resolved_at >= created_at` | n/a | n/a | 9,224 ms | 257 | PASS (SQL) | UNKNOWN, so SQL |
| `log: connection_id is not null` | **3,388 ms** | 7,581 ms | 67,453 ms | 2,340 | FAIL | narrowed scan (257 of 2,341 files) |
| `log: event in ('INFO','WARNING','SEVERE')` | **4,956 ms** | 10,396 ms | 66,704 ms | 2,343 | PASS | narrowed scan (382 of 2,341 files) |
| `sensor_data.readings: value >= L1` | **534 ms** | 1,882 ms | 791 ms | 0 | PASS | metadata |
| `sensor_data.readings: value >= L2` | **514 ms** | 1,379 ms | 1,519 ms | 2 | FAIL | metadata (witness: an exact bound below L2) |
| `sensor_data.readings: value between L1 and H` | **425 ms** | 1,639 ms | 3,919 ms | 102 | PASS | metadata |
| `sensor_data.readings: category = 'c1'` | **389 ms** | 1,455 ms | 3,920 ms | 102 | FAIL | metadata (strict proof) |

Rust phase breakdown (medians): `load_table` 0.19-0.47 s, manifests
0.17-0.58 s, evaluating every file's stats **≤ 7 ms even for 2,341 files**,
and the narrowed reads 2.4 s (257 files) and 4.1 s (382 files). The time is in
network round trips; the logic is free.

## Takeaways

- **DuckDB already prunes simple one-sided predicates** (0 GETs for
  `created_at >= …`, `value >= L1`) and is competitive there. It reads
  **every** file for `NOT BETWEEN`, `!=`, `NOT IN` and `IS NULL`, which is
  where metadata is 5-20× faster.
- **Witnesses matter.** pyiceberg's and iceberg-rust's evaluators only prove
  "might match" and "all rows match"; treating exact bounds and NULL counts
  as real values decides cases like `value >= L2` without a read.
- **Rust is 2-5× faster than pyiceberg** on the same logic (I/O and
  planning). pyiceberg's times here include inferring the string truncation
  length from all bounds on every cold load.
- **What's left is statistics completeness and fragmentation.** The 257
  files scanned for `connection_id is not null` are exactly the older files
  without null counts (FINDINGS #9). The 382 for `event in (…)` are files
  holding several event values.

## Caveats

- FAIL from metadata gives a lower bound on violations, not dbt's exact
  count; `warn_if`/`error_if` thresholds would need a narrowed scan.
- Relies on private pyiceberg classes (`_InclusiveMetricsEvaluator`,
  `_StrictMetricsEvaluator`) in 0.11.1, and on a patched iceberg-rust.
- `where:` configs aren't handled.
