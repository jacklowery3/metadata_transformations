# dbt-utils, utility by utility

[dbt-labs/dbt-utils](https://github.com/dbt-labs/dbt-utils) (triaged at
`2a8f07b`, 2026-10-01) is a ready-made list of SQL people write over and
over. For each macro: can Iceberg metadata **replace** it, **prove or narrow**
it, or is it **not applicable**?

Fit: **strong** = exact from metadata · **partial** = proves one side, or
narrows the scan · **no** = row-level compute · **already** = already a
catalog operation. "Tried" links go to measured results; everything else is
a first-pass assessment, not tested.

## Generic tests (`macros/generic_tests/`)

| utility | fit | metadata approach | status |
|---|---|---|---|
| `expression_is_true` | partial (single-column predicates) | prune / strict proof / witnesses / single-valued files, else narrowed scan | **tried**: [results](../experiments/expression_is_true/RESULTS.md) |
| `accepted_range` | strong | exact bounds vs range | **tried** in the [source test suite](../experiments/source_tests/RESULTS.md) |
| `unique_combination_of_columns` | strong on the declared key; else partial | identifier fields ⊆ columns ⇒ PASS | **tried** in the source test suite |
| `recency` | strong | timestamp upper bounds | covered by source freshness (tried) |
| `equal_rowcount`, `fewer_rows_than` | strong | snapshot `total-records` | not tested |
| `at_least_one`, `not_null_proportion` | strong | value and null counts | not tested (same stats as `not_null`, tried) |
| `not_constant` | strong | any file with min ≠ max, or files disagreeing | not tested |
| `not_empty_string` | strong-ish | lower bounds vs `''` | not tested |
| `not_accepted_values` | partial | values outside every file's range ⇒ PASS | not tested |
| `relationships_where` | partial | a child's exact bound missing from the parent ⇒ FAIL | dbt-core `relationships` **tried** in the source test suite |
| `equality` | partial | identical file sets ⇒ equal; count mismatch ⇒ unequal | not tested |
| `cardinality_equality` | partial | needs NDV sketches (roadmap C1) | not tested |
| `mutually_exclusive_ranges`, `sequential_values` | no | row-level ordering | n/a |

## SQL macros (`macros/sql/`)

| utility | fit | metadata approach | status |
|---|---|---|---|
| `union_relations` | strong | zero-copy manifest splicing | done in [unified_lake_tables](https://github.com/fivetran-jacklowery/unified_lake_tables) |
| `get_relations_by_prefix` / `_by_pattern`, `get_tables_by_*_sql` | already | Polaris REST listing | **tried**: [results](../experiments/dbt_utils/RESULTS.md#get_relations_by_prefix): also **broken on dbt-duckdb with an attached catalog** |
| `deduplicate` | partial | `partition_by` ⊇ declared key ⇒ no-op (`select *`) | **tried**: [results](../experiments/dbt_utils/RESULTS.md#deduplicate): also **the default implementation drops rows with NULLs on DuckDB** |
| `date_spine`, `generate_series` | no for the macro; strong for its bounds | compile-time `MIN`/`MAX` from bounds | **tried**: [results](../experiments/dbt_utils/RESULTS.md#generate_series) |
| `star`, `get_filtered_columns_in_relation` | already | schema from catalog | not tested |
| `get_column_values` (feeds `pivot`) | partial | single-valued files; free with identity partitioning (roadmap B1) | not tested |
| `insert_by_period` (now in dbt-experimental-features) | strong | snapshot diff / `day()` partition | snapshot diff tried via the mirror |
| `get_single_value`, `get_query_results_as_dict` | depends | as good as the wrapped query (e.g. `MAX` ⇒ A3) | n/a |
| `nullcheck`, `nullcheck_table`, `generate_surrogate_key`, `safe_*`, `haversine_distance`, `width_bucket`, `group_by`, `web/*`, `jinja_helpers/*` | no | row-level expressions or Jinja | n/a |

dbt-core's built-in tests (`unique`, `not_null`, `accepted_values`,
`relationships`) map onto the same machinery and were all tried in the
[source test suite](../experiments/source_tests/RESULTS.md).
