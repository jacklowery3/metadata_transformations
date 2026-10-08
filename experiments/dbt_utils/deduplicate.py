#!/usr/bin/env python3
"""dbt_utils.deduplicate, skipped when Iceberg metadata proves it's a no-op.

Rule: if `partition_by` covers the table's declared Iceberg identifier fields,
every partition has one row (the writer upserts by that key), so dedup is
`select *`. Iceberg doesn't enforce identifier fields, so the premise is
re-checked here with SQL (rows vs distinct partitions).

Each variant is built as a temp table on a fresh DuckDB connection:
  skip      select *  (the metadata rewrite)
  qualify   row_number() ... QUALIFY = 1   (what a duckdb__ override would do)
  default   dbt-utils default__deduplicate (row_number + NATURAL JOIN), which is
            what dbt-duckdb gets, since dbt-utils has no duckdb__ version

Cases: deduplicate.cases.json (+ gitignored .local.json). Relations starting
with `mirror.` read ../dbt_fivetran_log/mirror.duckdb (run its mirror.py first).

    python deduplicate.py [reps]
"""
from __future__ import annotations

import json
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))

from mdt.cases import load_cases  # noqa: E402
from mdt.catalog import get_catalog  # noqa: E402
from mdt.duck import attach  # noqa: E402

MIRROR = HERE.parent / "dbt_fivetran_log" / "mirror.duckdb"


def sql_default(rel, part, order):  # dbt_utils default__deduplicate, same shape
    return f"""with row_numbered as (
        select _inner.*, row_number() over (partition by {part} order by {order}) as rn
        from {rel} as _inner)
    select distinct data.* from {rel} as data natural join row_numbered where row_numbered.rn = 1"""


def sql_qualify(rel, part, order):
    return f"select * from {rel} qualify row_number() over (partition by {part} order by {order}) = 1"


def connect():
    con, _ = attach("fl")
    if MIRROR.exists():
        con.execute(f"ATTACH '{MIRROR}' AS mirror (READ_ONLY)")
    return con


def main():
    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    cat = get_catalog()
    out = []
    for case in load_cases(HERE, "deduplicate.cases"):
        rel, part, order = case["relation"], case["partition_by"], case["order_by"]
        label = case.get("alias") or f"{case['table']}, partition_by=({part})"
        t0 = time.perf_counter()
        schema = cat.load_table(case["table"]).schema()  # one REST call, no manifests
        key = {schema.find_field(i).name for i in schema.identifier_field_ids}
        proven = bool(key) and key <= {c.strip() for c in part.split(",")}
        decide_ms = (time.perf_counter() - t0) * 1e3

        runs = {}
        for variant, sql in (("skip", f"select * from {rel}"), ("qualify", sql_qualify(rel, part, order)),
                             ("default", sql_default(rel, part, order))):
            times, counts = [], set()
            for _ in range(reps):
                con = connect()
                t = time.perf_counter()
                con.execute(f"create temp table _out as {sql}")
                times.append((time.perf_counter() - t) * 1e3)
                counts.add(con.execute("select count(*) from _out").fetchone()[0])
                con.close()
            runs[variant] = {"median_ms": statistics.median(times), "rows": sorted(counts)}

        con = connect()
        total, distinct = con.execute(f"select count(*), count(distinct ({part})) from {rel}").fetchone()
        cols = [r[0] for r in con.execute(f"describe {rel}").fetchall()]
        with_null = con.execute(f"select count(*) from {rel} where {' or '.join(f'{chr(34)}{c}{chr(34)} is null' for c in cols)}").fetchone()[0]
        con.close()
        r = {"case": label, "proven_noop": proven, "decide_ms": decide_ms, "rows": total,
             "distinct_partitions": distinct, "rows_with_any_null": with_null, "runs": runs}
        out.append(r)
        print(f"\n{label}: proven no-op={proven} ({decide_ms:.0f} ms); rows={total} distinct partitions={distinct}; "
              f"rows with a NULL={with_null}")
        for v in ("skip", "qualify", "default"):
            print(f"  {v:8} {runs[v]['median_ms']:8.0f} ms  rows={runs[v]['rows']}")
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "deduplicate.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
