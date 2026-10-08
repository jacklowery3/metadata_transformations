#!/usr/bin/env python3
"""Per-column summary of manifest stats across a table's live files -- the raw
material for every metadata-only idea in docs/ROADMAP.md. Reads manifests only.

    python scripts/inspect_bounds.py fivetran_log.log ticket_data.tickets

Key columns:
  single_value_files  files where min == max  -> identity-partitionable for free
  single_day_files    timestamp files within one UTC day -> day()-partitionable for free
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from mdt.catalog import get_catalog  # noqa: E402
from mdt.stats import file_stats  # noqa: E402

cat = get_catalog()
for name in sys.argv[1:]:
    t = cat.load_table(name)
    files = file_stats(t)
    print(f"\n== {name}: {len(files)} files, {sum(f['record_count'] for f in files)} rows")
    print(f"  {'column':28} {'min':>32} {'max':>32} {'w/bounds':>8} {'single_val':>10} {'single_day':>10}")
    for col in t.schema().column_names:
        vals = [f["cols"][col] for f in files if f["cols"][col]["min"] is not None]
        if not vals:
            print(f"  {col:28} (no bounds)")
            continue
        single = sum(v["min"] == v["max"] for v in vals)
        day = sum(v["min"].date() == v["max"].date() for v in vals) if hasattr(vals[0]["min"], "date") else "-"
        print(f"  {col:28} {str(min(v['min'] for v in vals))[:32]:>32} {str(max(v['max'] for v in vals))[:32]:>32} "
              f"{len(vals):>8} {single:>10} {day:>10}")
