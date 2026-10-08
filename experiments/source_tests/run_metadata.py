#!/usr/bin/env python3
"""Run the dbt source test suite (models/**/*.yml) metadata-first, via mdt.decide.

Same tests, pass/fail semantics and failure counts as `dbt test` and
`dbt source freshness`:

  phase 1  decide every test from metadata (tables planned once, in parallel)
  phase 2  for the rest, read each table once (union of needed columns and
           undecided files) and count failures with the dbt test's own logic
           in local DuckDB

Supported: not_null, unique, accepted_values, relationships,
dbt_utils.accepted_range, dbt_utils.unique_combination_of_columns, freshness.
Not supported: `where:` configs, custom tests.

    python run_metadata.py [--only tests|freshness] [--label NAME] [--workers 8]
"""
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))

from pyiceberg.expressions import And, GreaterThanOrEqual, In, LessThanOrEqual, NotNull  # noqa: E402

from mdt.decide import (  # noqa: E402
    Residual, TableCache, decide_expr, decide_freshness, decide_relationships, decide_unique, lit, q, run_batched,
)

SRC_RE = re.compile(r"source\('([^']+)',\s*'([^']+)'\)")


@dataclass
class Test:
    kind: str
    schema: str
    table: str
    column: str | None
    args: dict = field(default_factory=dict)

    @property
    def fq(self):
        return f"{self.schema}.{self.table}"

    def key(self):
        cols = self.column or ",".join(self.args.get("combination_of_columns", []))
        return f"{self.kind}|{self.fq}|{cols}"


def parse_suite(models: Path):
    tests, fresh = [], []
    for path in sorted(models.rglob("*.yml")):
        for src in (yaml.safe_load(path.read_text()) or {}).get("sources", []):
            for tbl in src["tables"]:
                fresh.append((src["schema"], tbl["name"], tbl.get("loaded_at_field", src.get("loaded_at_field")),
                              tbl.get("freshness", src.get("freshness")) or {}))
                specs = [(None, t) for t in tbl.get("data_tests", [])]
                specs += [(c["name"], t) for c in tbl.get("columns", []) for t in c.get("data_tests", [])]
                for col, t in specs:
                    kind, args = (t, {}) if isinstance(t, str) else next(iter(t.items()))
                    args = (args or {}).get("arguments", args or {})
                    tests.append(Test(kind.split(".")[-1], src["schema"], tbl["name"], col, args))
    return tests, fresh


def decide(cache: TableCache, t: Test):
    """dbt's failure-count semantics: not_null/accepted_range/relationships count rows,
    accepted_values counts distinct invalid values, unique counts duplicated groups."""
    ctx, c = cache.get(t.fq), t.column
    qc = c and q(c)
    if t.kind == "not_null":
        return decide_expr(ctx, NotNull(c), f"select count(*) from rows where {qc} is null")
    if t.kind == "accepted_values":
        vals = ", ".join(lit(v) for v in t.args["values"])
        return decide_expr(ctx, In(c, set(t.args["values"])),
                           f"select count(distinct {qc}) from rows where {qc} is not null and {qc} not in ({vals})")
    if t.kind == "accepted_range":
        parts, conds = [], []
        if "min_value" in t.args:
            parts.append(GreaterThanOrEqual(c, t.args["min_value"]))
            conds.append(f"{qc} >= {lit(t.args['min_value'])}")
        if "max_value" in t.args:
            parts.append(LessThanOrEqual(c, t.args["max_value"]))
            conds.append(f"{qc} <= {lit(t.args['max_value'])}")
        expr = parts[0] if len(parts) == 1 else And(*parts)
        return decide_expr(ctx, expr, f"select count(*) from rows where {qc} is not null and not ({' and '.join(conds)})")
    if t.kind in ("unique", "unique_combination_of_columns"):
        cols = [c] if t.kind == "unique" else list(t.args["combination_of_columns"])
        gc = ", ".join(q(x) for x in cols)
        where = f"{qc} is not null" if t.kind == "unique" else "true"
        return decide_unique(ctx, cols, f"select count(*) from (select {gc} from rows where {where} "
                                        f"group by {gc} having count(*) > 1)")
    if t.kind == "relationships":
        ps, pt = SRC_RE.search(t.args["to"]).groups()
        return decide_relationships(ctx, c, cache.get(f"{ps}.{pt}"), t.args["field"],
                                    f"select count(*) from rows where {qc} is not null and {qc} not in (select k from parents)")
    return {"status": "error", "how": f"unsupported test {t.kind}"}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", default="metadata")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--only", choices=["tests", "freshness"])
    args = ap.parse_args()
    tests, fresh = parse_suite(HERE / "models")
    out = {"n_tests": len(tests), "n_freshness": len(fresh)}

    if args.only in (None, "tests"):
        t0 = time.perf_counter()
        cache = TableCache()
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            decided = list(pool.map(lambda t: decide(cache, t), tests))
        out["metadata_phase_s"] = time.perf_counter() - t0
        residual_idx = [i for i, d in enumerate(decided) if isinstance(d, Residual)]
        for i, r in zip(residual_idx, run_batched([decided[i] for i in residual_idx])):
            decided[i] = r
        out["tests_wall_s"] = time.perf_counter() - t0
        out["tests"] = [{"key": t.key(), **d} for t, d in zip(tests, decided)]
        for r in sorted(out["tests"], key=lambda r: r["key"]):
            print(f"{r['status']:5} {r['how']:24} {str(r.get('failures', '')):>6} scanned={r.get('scanned_files', 0):>4}  {r['key']}")
        print(f"WALL tests ({len(tests)}): {out['tests_wall_s']:.1f}s (metadata phase {out['metadata_phase_s']:.1f}s)")

    if args.only in (None, "freshness"):
        t0 = time.perf_counter()
        cache = TableCache()  # cold, like a standalone `dbt source freshness`
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            out["freshness"] = list(pool.map(
                lambda f: {"key": f"{f[0]}.{f[1]}", **decide_freshness(
                    cache.get(f"{f[0]}.{f[1]}"), f[2], f[3].get("warn_after"), f[3].get("error_after"))}, fresh))
        out["freshness_wall_s"] = time.perf_counter() - t0
        for r in out["freshness"]:
            print(f"{r['status']:5} freshness max_loaded_at={r['max_loaded_at']}  {r['key']}")
        print(f"WALL freshness ({len(fresh)}): {out['freshness_wall_s']:.1f}s")

    (HERE / "results").mkdir(exist_ok=True)
    (HERE / "results" / f"{args.label}.json").write_text(json.dumps(out, indent=1, default=str))


if __name__ == "__main__":
    main()
