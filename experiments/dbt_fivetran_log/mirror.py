#!/usr/bin/env python3
"""Metadata-driven incremental mirror of Fivetran Iceberg sources into local DuckDB.

Which files to read is decided purely from Iceberg metadata. Each table's
live file list at the target snapshot (manifests only) is diffed against the
files already mirrored (tracked in a `_src_file` column):

  added   = live - mirrored   -> read just these Parquet files (pyiceberg ArrowScan)
  removed = mirrored - live   -> delete their rows (Fivetran copy-on-write updates
                                 retire whole files; same idea as unified_lake_tables step 5)

Nothing is read when nothing changed: a no-op sync is pure metadata.

    python mirror.py sync [--as-of-hours-ago N] [--tables a,b]   # N: mirror state as of N hours ago
    python mirror.py status

Run with the repo venv (requirements.txt).
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import pyarrow as pa

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))
from mdt.catalog import get_catalog  # noqa: E402
from pyiceberg.expressions import AlwaysTrue  # noqa: E402
from pyiceberg.io.pyarrow import ArrowScan  # noqa: E402

NAMESPACE = "fivetran_log"
# Sources dbt_fivetran_log reads that exist in this catalog
TABLES = ["account", "connection", "destination", "incremental_mar", "log", "user"]
DB = HERE / "mirror.duckdb"


def snapshot_as_of(table, hours_ago: float | None):
    if not hours_ago:
        return table.current_snapshot()
    cutoff_ms = (time.time() - hours_ago * 3600) * 1000
    older = [s for s in table.metadata.snapshots if s.timestamp_ms <= cutoff_ms]
    return max(older, key=lambda s: s.timestamp_ms) if older else None


def sync_table(cat, con, name: str, hours_ago: float | None) -> dict:
    t0 = time.perf_counter()
    table = cat.load_table(f"{NAMESPACE}.{name}")
    snap = snapshot_as_of(table, hours_ago)
    out = {"table": name, "snapshot_id": snap.snapshot_id if snap else None}
    if snap is None:
        out.update(skipped="no snapshot at that time", total_ms=(time.perf_counter() - t0) * 1e3)
        return out
    scan = table.scan(snapshot_id=snap.snapshot_id)
    tasks = list(scan.plan_files())  # manifests only
    live = {t.file.file_path: t for t in tasks}
    plan_ms = (time.perf_counter() - t0) * 1e3

    exists = con.execute("select count(*) from information_schema.tables where table_schema=? and table_name=?",
                         [NAMESPACE, name]).fetchone()[0]
    mirrored = set()
    if exists:
        mirrored = {r[0] for r in con.execute(f'select distinct _src_file from {NAMESPACE}."{name}"').fetchall()}
    added = [live[p] for p in live.keys() - mirrored]
    removed = sorted(mirrored - live.keys())

    t1 = time.perf_counter()
    projected = scan.projection()

    def read(task):
        tbl = ArrowScan(table.metadata, table.io, projected, AlwaysTrue(), True).to_table([task])
        return tbl.append_column("_src_file", pa.array([task.file.file_path] * tbl.num_rows, pa.string()))

    with ThreadPoolExecutor(max_workers=32) as pool:
        parts = list(pool.map(read, added))
    read_ms = (time.perf_counter() - t1) * 1e3

    t2 = time.perf_counter()
    con.execute("begin")
    if removed:
        con.execute(f'delete from {NAMESPACE}."{name}" where _src_file in (select unnest(?))', [removed])
    if parts:
        new = pa.concat_tables(parts, promote_options="permissive")
        if not exists:
            con.execute(f'create table {NAMESPACE}."{name}" as select * from new')
        else:
            have = {r[0] for r in con.execute(f"select column_name from information_schema.columns where table_schema=? and table_name=?",
                                              [NAMESPACE, name]).fetchall()}
            for f in new.schema:  # additive schema evolution
                if f.name not in have:
                    col_type = con.sql(f'select "{f.name}" from new limit 0').types[0]
                    con.execute(f'alter table {NAMESPACE}."{name}" add column "{f.name}" {col_type}')
            con.execute(f'insert into {NAMESPACE}."{name}" by name select * from new')
    con.execute("commit")
    apply_ms = (time.perf_counter() - t2) * 1e3

    out.update(live_files=len(live), added_files=len(added), removed_files=len(removed),
               added_rows=sum(p.num_rows for p in parts), added_bytes=sum(t.file.file_size_in_bytes for t in added),
               plan_ms=plan_ms, read_ms=read_ms, apply_ms=apply_ms, total_ms=(time.perf_counter() - t0) * 1e3)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["sync", "status"])
    ap.add_argument("--as-of-hours-ago", type=float, default=None)
    ap.add_argument("--tables", default=",".join(TABLES))
    ap.add_argument("--label", default=None)
    args = ap.parse_args()

    con = duckdb.connect(str(DB))
    con.execute(f"create schema if not exists {NAMESPACE}")
    if args.cmd == "status":
        for (n,) in con.execute("select table_name from information_schema.tables where table_schema=?", [NAMESPACE]).fetchall():
            rows, files = con.execute(f'select count(*), count(distinct _src_file) from {NAMESPACE}."{n}"').fetchone()
            print(f"{n:20} rows={rows:>9} files={files:>5}")
        return

    t0 = time.perf_counter()
    cat = get_catalog()
    connect_ms = (time.perf_counter() - t0) * 1e3
    results = []
    with ThreadPoolExecutor(max_workers=len(TABLES)) as pool:
        # one DuckDB cursor per thread; Iceberg reads run in parallel across tables
        futs = [pool.submit(sync_table, cat, con.cursor(), n, args.as_of_hours_ago) for n in args.tables.split(",")]
        results = [f.result() for f in futs]
    wall_ms = (time.perf_counter() - t0) * 1e3
    for r in results:
        print(f"{r['table']:16} live={r.get('live_files', '-'):>5} +{r.get('added_files', 0):>4} files "
              f"-{r.get('removed_files', 0):>3}  rows+{r.get('added_rows', 0):>7}  "
              f"plan {r.get('plan_ms', 0):6.0f} ms  read {r.get('read_ms', 0):7.0f} ms  apply {r.get('apply_ms', 0):5.0f} ms")
    print(f"WALL sync ({args.label or 'unlabeled'}): {wall_ms / 1e3:.1f}s  (catalog connect {connect_ms:.0f} ms)")
    if args.label:
        (HERE / "results").mkdir(exist_ok=True)
        (HERE / "results" / f"mirror_{args.label}.json").write_text(
            json.dumps({"wall_ms": wall_ms, "connect_ms": connect_ms, "tables": results}, indent=1, default=str))


if __name__ == "__main__":
    main()
