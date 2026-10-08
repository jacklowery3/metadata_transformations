#!/usr/bin/env python3
"""dbt_utils.generate_series, and the MIN/MAX bounds spines feed it.

generate_series(n) is pure generation (cross-joined powers of two): no table,
nothing for metadata to replace. Spines built on it (date_spine,
fivetran_utils.fivetran_date_spine) take start/end from data at compile time,
typically `run_query("select min(ts) ...")`. For date/timestamp columns, MIN
and MAX are exact from manifest lower/upper bounds (which, like SQL, ignore
NULLs). A file missing bounds makes the answer "not exact": run the SQL.

  series    the macro's rendered SQL for n = 1,000 / 100,000, local DuckDB
  bounds    SQL min/max via DuckDB + duckdb-iceberg (Parquet GETs counted)
            vs manifest bounds via pyiceberg (manifests only)

Cases: generate_series.cases.json (+ gitignored .local.json).

    python generate_series.py [reps]
"""
from __future__ import annotations

import json
import math
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))

import duckdb  # noqa: E402
from pyiceberg.conversions import from_bytes  # noqa: E402

from mdt.cases import load_cases  # noqa: E402
from mdt.catalog import get_catalog  # noqa: E402
from mdt.duck import attach  # noqa: E402


def series_sql(n: int) -> str:  # dbt_utils default__generate_series, rendered
    k = max(1, math.ceil(math.log2(n)))
    terms = " + ".join(f"p{i}.generated_number * power(2, {i})" for i in range(k))
    joins = " cross join ".join(f"p as p{i}" for i in range(k))
    return (f"with p as (select 0 as generated_number union all select 1), "
            f"unioned as (select {terms} + 1 as generated_number from {joins}) "
            f"select * from unioned where generated_number <= {n} order by generated_number")


def metadata_minmax(cat, table: str, col: str):
    """(min_us, max_us, files, exact) from manifest bounds of a timestamp column."""
    t = cat.load_table(table)
    field = t.schema().find_field(col)
    lo = hi = None
    files = 0
    for task in t.scan().plan_files():
        f = task.file
        files += 1
        values = (f.value_counts or {}).get(field.field_id)
        nulls = (f.null_value_counts or {}).get(field.field_id)
        lb, ub = (f.lower_bounds or {}).get(field.field_id), (f.upper_bounds or {}).get(field.field_id)
        if lb is None or ub is None:
            if values is not None and nulls is not None and values == nulls:
                continue  # all-NULL file contributes nothing, as in SQL
            return None, None, files, False
        lb, ub = from_bytes(field.field_type, lb), from_bytes(field.field_type, ub)
        lo = lb if lo is None else min(lo, lb)
        hi = ub if hi is None else max(hi, ub)
    return lo, hi, files, True


def main():
    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    out = {"series": [], "bounds": []}
    con = duckdb.connect()
    for n in (1_000, 100_000):
        times = []
        for _ in range(reps):
            t0 = time.perf_counter()
            rows = con.execute(f"select count(*) from ({series_sql(n)})").fetchone()[0]
            times.append((time.perf_counter() - t0) * 1e3)
        out["series"].append({"n": n, "rows": rows, "median_ms": statistics.median(times)})
        print(f"generate_series({n:,}): {rows:,} rows in {statistics.median(times):.1f} ms")

    cat = get_catalog()
    for case in load_cases(HERE, "generate_series.cases"):
        table, col = case["table"], case["column"]
        label = case.get("alias") or f"{table}.{col}"
        md_t, dd_t, gets, agree = [], [], [], True
        for _ in range(reps):
            t0 = time.perf_counter()
            lo, hi, files, exact = metadata_minmax(cat, table, col)
            md_t.append((time.perf_counter() - t0) * 1e3)
            dcon, _ = attach("fl")
            dcon.execute("CALL enable_logging('HTTP', storage='memory')")
            t0 = time.perf_counter()
            dlo, dhi = dcon.execute(f"select epoch_us(min({col})), epoch_us(max({col})) from fl.{table}").fetchone()
            dd_t.append((time.perf_counter() - t0) * 1e3)
            gets.append(dcon.execute("select count(*) from duckdb_logs where type='HTTP' and message like '%.parquet%'").fetchone()[0])
            dcon.close()
            agree &= exact and (lo, hi) == (dlo, dhi)
        r = {"case": label, "files": files, "exact": exact, "agree": agree, "metadata_ms": statistics.median(md_t),
             "duckdb_ms": statistics.median(dd_t), "duckdb_parquet_gets": statistics.median(gets)}
        out["bounds"].append(r)
        print(f"{label} ({files} files): metadata {r['metadata_ms']:.0f} ms | SQL {r['duckdb_ms']:.0f} ms "
              f"({r['duckdb_parquet_gets']:.0f} Parquet GETs) | agree to the microsecond={agree}")
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "generate_series.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
