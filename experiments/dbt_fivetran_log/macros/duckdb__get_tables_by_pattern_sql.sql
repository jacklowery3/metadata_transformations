{# dbt-utils' default queries <database>.information_schema.tables, which DuckDB
   doesn't have for attached catalogs (information_schema is global). Minimal fix. #}
{% macro duckdb__get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) %}
        select distinct
            table_schema as {{ adapter.quote('table_schema') }},
            table_name as {{ adapter.quote('table_name') }},
            {{ dbt_utils.get_table_types_sql() }}
        from information_schema.tables
        where table_catalog = '{{ database }}'
        and table_schema ilike '{{ schema_pattern }}'
        and table_name ilike '{{ table_pattern }}'
        and table_name not ilike '{{ exclude }}'
{% endmacro %}
