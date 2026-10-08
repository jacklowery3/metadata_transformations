"""Decode Iceberg per-file column bounds (manifest lower_bounds/upper_bounds).

Bounds are Iceberg single-value binary serialization:
https://iceberg.apache.org/spec/#binary-single-value-serialization
"""
from __future__ import annotations

import struct
from datetime import date, datetime, timedelta, timezone

from pyiceberg.table import Table

EPOCH = date(1970, 1, 1)


def decode_bound(type_str: str, b: bytes | None):
    if b is None:
        return None
    if type_str in ("timestamptz", "timestamp"):
        return datetime.fromtimestamp(struct.unpack("<q", b)[0] / 1e6, tz=timezone.utc)
    if type_str == "date":
        return EPOCH + timedelta(days=struct.unpack("<i", b)[0])
    if type_str == "boolean":
        return b == b"\x01"
    if type_str == "int":
        return struct.unpack("<i", b)[0]
    if type_str == "long":
        return struct.unpack("<q", b)[0]
    if type_str == "float":
        return struct.unpack("<f", b)[0]
    if type_str == "double":
        return struct.unpack("<d", b)[0]
    if type_str == "string":
        # NOTE: string bounds are truncated (16 chars by default) -- not exact MIN/MAX.
        return b.decode("utf-8", errors="replace")
    return b


def file_stats(table: Table) -> list[dict]:
    """One dict per live data file: path, record_count, and decoded per-column stats.

    Reads only manifests (Avro metadata), never Parquet data.
    """
    schema = table.schema()
    fields = {f.field_id: (f.name, str(f.field_type)) for f in schema.fields}
    out = []
    for task in table.scan().plan_files():
        df = task.file
        lo, hi = df.lower_bounds or {}, df.upper_bounds or {}
        nulls, values = df.null_value_counts or {}, df.value_counts or {}
        cols = {}
        for fid, (name, typ) in fields.items():
            cols[name] = {
                "min": decode_bound(typ, lo.get(fid)),
                "max": decode_bound(typ, hi.get(fid)),
                "nulls": nulls.get(fid),
                "values": values.get(fid),
            }
        out.append({"path": df.file_path, "record_count": df.record_count,
                    "size": df.file_size_in_bytes, "cols": cols})
    return out
