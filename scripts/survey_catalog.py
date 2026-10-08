#!/usr/bin/env python3
"""Tier-1 survey of every table in the catalog: rows, files, partitioning,
snapshot ops, schema versions. Reads only catalog metadata JSON (zero S3 reads).

    python scripts/survey_catalog.py [namespace ...]
"""
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from mdt.catalog import get_catalog  # noqa: E402

cat = get_catalog()
namespaces = [tuple(a.split(".")) for a in sys.argv[1:]] or cat.list_namespaces()
print(f"{'table':45} {'rows':>9} {'files':>6} {'rows/file':>9} {'part':>5} {'snaps':>5} {'schemas':>7}  ops")
for ns in namespaces:
    for ident in cat.list_tables(ns):
        t = cat.load_table(ident)
        m = t.metadata
        snap = t.current_snapshot()
        s = snap.summary.additional_properties if snap and snap.summary else {}
        rows, files = int(s.get("total-records", 0)), int(s.get("total-data-files", 0))
        ops = Counter(str(x.summary.operation.value) for x in m.snapshots if x.summary)
        print(f"{'.'.join(ident):45} {rows:>9} {files:>6} {rows // files if files else 0:>9} "
              f"{len(t.spec().fields):>5} {len(m.snapshots):>5} {len(m.schemas):>7}  {dict(ops)}")
