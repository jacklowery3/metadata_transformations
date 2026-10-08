#!/usr/bin/env python3
"""Compare dbt's results with run_metadata.py's, test by test and source by source.

    python compare.py <dbt test label> <metadata tests label> <dbt freshness label> <metadata freshness label>

Prints disagreements and a summary of how each test was decided. Metadata-proven
FAILs report a lower bound (">=N"), which agrees with dbt when dbt's count is >= N.
Tests defined under models/private/ are counted but not named.
"""
import json
import re
import sys
from collections import Counter
from pathlib import Path

HERE = Path(__file__).resolve().parent
R = HERE / "results"
dbt_t, md_t, dbt_f, md_f = sys.argv[1:5]
manifest = json.loads((HERE / "target" / "manifest.json").read_text())


def key(node):
    meta = node["test_metadata"]
    kind, kw = meta["name"], meta["kwargs"]
    schema, table = re.search(r"source\('([^']+)',\s*'([^']+)'\)", kw["model"]).groups()  # the tested (child) table
    col = kw.get("column_name") or ",".join(kw.get("combination_of_columns", []))
    return f"{kind}|{schema}.{table}|{col}", "models/private/" in node["original_file_path"]


dbt = {}
for r in json.loads((R / f"{dbt_t}.json").read_text())["results"]:
    k, private = key(manifest["nodes"][r["unique_id"]])
    dbt[k] = (r["status"], r["failures"], private)
md = {t["key"]: t for t in json.loads((R / f"{md_t}.json").read_text())["tests"]}

bad, hows, priv = [], Counter(), 0
for k, (status, failures, private) in dbt.items():
    m = md[k]
    priv += private
    hows[m["how"]] += 1
    same_status = (status == "pass") == (m["status"] == "pass")
    f = m.get("failures")
    same_count = (str(f).startswith(">=") and failures >= int(str(f)[2:])) or f == failures
    if not (same_status and same_count):
        bad.append((("<private>" if private else k), status, failures, m["status"], f))
print(f"tests: {len(dbt)} compared ({priv} private), disagreements: {len(bad)}")
for b in bad:
    print("  DISAGREE", b)
print("  decided by:", dict(hows))
print("  failing:", sorted(("<private>" if v[2] else k, v[1]) for k, v in dbt.items() if v[0] != "pass"))

srcs = json.loads((R / f"{dbt_f}_sources.json").read_text())["results"]
mdf = {r["key"]: r for r in json.loads((R / f"{md_f}.json").read_text())["freshness"]}
same_ts = same_status = 0
for r in srcs:
    k = ".".join(r["unique_id"].split(".")[-2:])
    same_status += r["status"] == mdf[k]["status"]
    same_ts += r["max_loaded_at"].replace(" ", "T") == mdf[k]["max_loaded_at"]
print(f"freshness: {len(srcs)} sources, same status {same_status}/{len(srcs)}, "
      f"identical max_loaded_at {same_ts}/{len(srcs)} (differences = a sync landed between the runs)")
