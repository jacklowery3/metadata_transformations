#!/usr/bin/env python3
"""dbt_utils.get_relations_by_prefix / _by_pattern: Polaris REST listing vs DuckDB SQL.

The macros run (with the duckdb__ shim in ../dbt_fivetran_log/macros, needed
because DuckDB has no per-database information_schema):
    select distinct table_schema, table_name from information_schema.tables
    where table_catalog = '<db>' and table_schema ilike '<schema>'
      and table_name ilike '<prefix>%' and table_name not ilike '<exclude>'

  metadata  Polaris REST: list namespaces (only if the schema pattern has
            wildcards; note `_` is one), then list tables per matching
            namespace in parallel, ILIKE in Python
  duckdb    that SQL on DuckDB + duckdb-iceberg with the catalog ATTACHed

Cases: get_relations_by_prefix.cases.json (+ gitignored .local.json).

    python get_relations_by_prefix.py [reps]
"""
from __future__ import annotations

import json
import re
import statistics
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))

from mdt.cases import load_cases  # noqa: E402
from mdt.catalog import get_catalog  # noqa: E402
from mdt.duck import attach  # noqa: E402


def ilike(pattern: str):
    """SQL ILIKE -> regex: % = any run, _ = one char, case-insensitive, whole string."""
    rx = "".join(".*" if c == "%" else "." if c == "_" else re.escape(c) for c in pattern)
    return re.compile(rf"^{rx}$", re.IGNORECASE | re.DOTALL)


def by_metadata(cat, schema: str, prefix: str, exclude: str) -> tuple[set, int]:
    calls = 0
    if any(c in schema for c in "%_"):
        namespaces = [ns for ns in cat.list_namespaces() if ilike(schema).match(".".join(ns))]
        calls += 1
    else:
        namespaces = [(schema,)]
    t_rx, x_rx = ilike(prefix + "%"), ilike(exclude)
    with ThreadPoolExecutor(max_workers=16) as pool:
        listed = list(pool.map(cat.list_tables, namespaces))
    calls += len(namespaces)
    return ({".".join(i) for idents in listed for i in idents
             if t_rx.match(i[-1]) and not x_rx.match(i[-1])}, calls)


def by_duckdb(con, schema, prefix, exclude) -> set:
    rows = con.execute(f"""select distinct table_schema, table_name from information_schema.tables
        where table_catalog = 'fl' and table_schema ilike '{schema}'
          and table_name ilike '{prefix}%' and table_name not ilike '{exclude}'""").fetchall()
    return {f"{s}.{t}" for s, t in rows}


def main():
    reps = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    cat = get_catalog()
    out = []
    for case in load_cases(HERE, "get_relations_by_prefix.cases"):
        s, p, x = case["schema"], case["prefix"], case["exclude"]
        label = case.get("alias") or f"schema={s!r} prefix={p!r} exclude={x!r}"
        md_t, dd_t, attach_t, agree = [], [], [], True
        for _ in range(reps):
            t = time.perf_counter()
            md, calls = by_metadata(cat, s, p, x)
            md_t.append((time.perf_counter() - t) * 1e3)
            con, a_ms = attach("fl")
            t = time.perf_counter()
            dd = by_duckdb(con, s, p, x)
            dd_t.append((time.perf_counter() - t) * 1e3)
            attach_t.append(a_ms)
            con.close()
            agree &= md == dd
        r = {"case": label, "tables": len(md), "rest_calls": calls, "agree": agree,
             "metadata_ms": statistics.median(md_t), "duckdb_query_ms": statistics.median(dd_t),
             "duckdb_attach_ms": statistics.median(attach_t)}
        out.append(r)
        print(f"{label}: {r['tables']} tables, agree={agree} | metadata {r['metadata_ms']:.0f} ms "
              f"({calls} REST calls) | duckdb query {r['duckdb_query_ms']:.0f} ms (+ attach {r['duckdb_attach_ms']:.0f} ms)")
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "get_relations_by_prefix.json").write_text(json.dumps(out, indent=1))


if __name__ == "__main__":
    main()
