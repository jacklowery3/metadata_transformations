"""DuckDB sessions attached to the Polaris catalog, used as the SQL baseline.

Requires the persistent DuckDB secret `polaris_secret` and the Iceberg
extensions in MDT_DUCKDB_EXT_DIR. Both are created by `scripts/setup_duckdb.py`.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

import duckdb

from mdt.catalog import REPO_ROOT, polaris_config

EXT_DIR = Path(os.getenv("MDT_DUCKDB_EXT_DIR", REPO_ROOT / ".duckdb_ext"))
EXTENSIONS = ("httpfs", "avro", "iceberg")


def attach(alias: str = "fl"):
    """New connection with the catalog ATTACHed as `alias`. Returns (con, attach_ms)."""
    cfg = polaris_config()
    t0 = time.perf_counter()
    con = duckdb.connect()
    con.execute(f"SET extension_directory='{EXT_DIR}'")
    for ext in EXTENSIONS:
        con.execute(f"LOAD {ext}")
    con.execute(f"""ATTACH '{cfg["warehouse"]}' AS {alias} (TYPE ICEBERG, ENDPOINT '{cfg["uri"]}',
                   SECRET polaris_secret, DEFAULT_REGION '{cfg["region"]}')""")
    return con, (time.perf_counter() - t0) * 1e3


def iceberg_extension_version() -> str:
    con = duckdb.connect()
    con.execute(f"SET extension_directory='{EXT_DIR}'")
    con.execute("LOAD iceberg")
    return con.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='iceberg'").fetchone()[0]
