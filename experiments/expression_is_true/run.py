#!/usr/bin/env python3
"""dbt_utils.expression_is_true decided from Iceberg metadata, vs DuckDB running the SQL.

The macro compiles to `select 1 from <model> where not(<expression>)`; the test
passes iff that returns no rows. Per case and rep:

  pyiceberg  mdt.decide.decide_expr on a freshly loaded table (cold manifests):
             PASS/FAIL from metadata, else count failures over only the undecided files
  duckdb     DuckDB + duckdb-iceberg (fresh connection, catalog ATTACHed) running
             `select count(*) ... where not (<expression>)`, with its HTTP log
             captured in the same execution (Parquet GETs)

Expressions that pyiceberg's parser can't express as a single-column Iceberg
predicate (e.g. column vs column) are UNKNOWN: the SQL must run.

Cases: cases.json (+ gitignored cases.local.json). A case's optional "alias"
is printed and saved instead of its table and expression.

    python run.py [reps] [--rust rust/results.jsonl]
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))

import duckdb  # noqa: E402
from pyiceberg.expressions.parser import parse  # noqa: E402

from mdt.cases import load_cases  # noqa: E402
from mdt.decide import Residual, TableCache, cols_in, decide_expr, run_batched  # noqa: E402
from mdt.duck import attach, iceberg_extension_version  # noqa: E402


def label(case):
    return case.get("alias") or f"{case['table']}: {case['expression']}"


def run_metadata(case) -> dict:
    t0 = time.perf_counter()
    try:
        expr = parse(case["expression"])
        if len(cols_in(expr)) != 1:
            raise ValueError("multi-column")
    except Exception:
        return {"verdict": "UNKNOWN", "how": "not a single-column Iceberg predicate",
                "total_ms": (time.perf_counter() - t0) * 1e3}
    ctx = TableCache().get(case["table"])  # cold: load + plan every rep
    sql = case.get("sql") or case["expression"]
    d = decide_expr(ctx, expr, f"select count(*) from rows where not ({sql})")
    if isinstance(d, Residual):
        d = run_batched([d])[0]
    return {"verdict": d["status"].upper(), "how": d["how"], "failures": d.get("failures"),
            "scanned_files": d.get("scanned_files", 0), "files": len(ctx.tasks),
            "total_ms": (time.perf_counter() - t0) * 1e3}


def run_duckdb(case) -> dict:
    con, attach_ms = attach("fl")
    con.execute("CALL enable_logging('HTTP', storage='memory')")
    t0 = time.perf_counter()
    n = con.execute(f"select count(*) from fl.{case['table']} where not ({case.get('sql') or case['expression']})").fetchone()[0]
    ms = (time.perf_counter() - t0) * 1e3
    gets = con.execute("select count(*) from duckdb_logs where type='HTTP' and message like '%.parquet%'").fetchone()[0]
    con.close()
    return {"verdict": "FAIL" if n else "PASS", "violations": n, "parquet_gets": gets,
            "total_ms": ms, "attach_ms": attach_ms}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("reps", nargs="?", type=int, default=3)
    ap.add_argument("--rust", type=Path, help="JSON lines from rust/ (cargo run --release)")
    args = ap.parse_args()
    cases = load_cases(HERE)
    print(f"duckdb {duckdb.__version__}, iceberg extension {iceberg_extension_version()}", file=sys.stderr)

    rows = []
    for case in cases:
        for rep in range(args.reps):
            rows.append({"engine": "pyiceberg", "case": label(case), "rep": rep, **run_metadata(case)})
            rows.append({"engine": "duckdb", "case": label(case), "rep": rep, **run_duckdb(case)})
    if args.rust:
        for line in args.rust.read_text().splitlines():
            if line.startswith("{"):
                rows.append(json.loads(line))

    engines = [e for e in ("iceberg-rust", "pyiceberg", "duckdb") if any(r["engine"] == e for r in rows)]
    print("| case | " + " | ".join(f"{e} (median ms)" for e in engines) + " | DuckDB Parquet GETs | verdicts |")
    print("|---|" + "---|" * (len(engines) + 2))
    for case in cases:
        lab, cells, verdicts = label(case), [], set()
        for e in engines:
            rs = [r for r in rows if r["engine"] == e and r["case"] == lab]
            cells.append(f"{statistics.median(r['total_ms'] for r in rs):,.0f}" if rs else "-")
            verdicts |= {r["verdict"] for r in rs if r["verdict"] != "UNKNOWN"}
        gets = [r["parquet_gets"] for r in rows if r["engine"] == "duckdb" and r["case"] == lab]
        md = {r["how"] for r in rows if r["engine"] == "pyiceberg" and r["case"] == lab}
        print(f"| `{lab}` | " + " | ".join(cells) + f" | {statistics.median(gets):,.0f} | "
              f"{'agree' if len(verdicts) <= 1 else 'DISAGREE'} {sorted(verdicts)} via {sorted(md)} |")
    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / "run.json").write_text(json.dumps(rows, indent=1, default=str))


if __name__ == "__main__":
    main()
