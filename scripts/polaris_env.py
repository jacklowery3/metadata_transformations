#!/usr/bin/env python3
"""Print shell exports (no secrets) for the dbt profiles in experiments/:
POLARIS_WAREHOUSE, POLARIS_ENDPOINT, BUCKET_REGION, MDT_DUCKDB_EXT_DIR.

    eval "$(python scripts/polaris_env.py)"
"""
import shlex
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from mdt.catalog import polaris_config  # noqa: E402
from mdt.duck import EXT_DIR  # noqa: E402

c = polaris_config()
for k, v in {"POLARIS_WAREHOUSE": c["warehouse"], "POLARIS_ENDPOINT": c["uri"],
             "BUCKET_REGION": c["region"], "MDT_DUCKDB_EXT_DIR": str(EXT_DIR)}.items():
    print(f"export {k}={shlex.quote(v)}")
