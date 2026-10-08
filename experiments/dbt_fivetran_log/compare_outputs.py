#!/usr/bin/env python3
"""Check the mirror build matches the direct-from-Iceberg build, model by model:
row count plus an order-independent hash over every row (bit_xor(hash(row)))."""
import sys
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
a = duckdb.connect(str(HERE / "bench.duckdb"), read_only=True)
b = duckdb.connect(str(HERE / "bench_mirror.duckdb"), read_only=True)
rels = a.execute("select table_schema, table_name from information_schema.tables "
                 "where table_schema like 'main_%' and table_type = 'BASE TABLE' order by 1, 2").fetchall()
ok = True
for s, t in rels:
    sql = f'select count(*), bit_xor(hash(x)) from "{s}"."{t}" x'
    ra, rb = a.execute(sql).fetchone(), b.execute(sql).fetchone()
    ok &= ra == rb
    print(f"{t:45} rows {ra[0]:>7} vs {rb[0]:>7}  {'IDENTICAL' if ra == rb else 'DIFFERENT'}")
sys.exit(0 if ok else 1)
