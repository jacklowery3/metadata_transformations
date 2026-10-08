#!/usr/bin/env python3
"""One-time DuckDB setup for the SQL baselines.

- installs httpfs/avro/iceberg into MDT_DUCKDB_EXT_DIR (default .duckdb_ext/),
  from `core_nightly` by default (newest duckdb-iceberg for released DuckDB);
- creates the persistent secret `polaris_secret` (OAuth client credentials,
  stored by DuckDB under ~/.duckdb/stored_secrets).

    python scripts/setup_duckdb.py [--repo core|core_nightly]
"""
import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import duckdb  # noqa: E402

from mdt.catalog import polaris_config  # noqa: E402
from mdt.duck import EXT_DIR, EXTENSIONS, iceberg_extension_version  # noqa: E402

ap = argparse.ArgumentParser()
ap.add_argument("--repo", default="core_nightly")
args = ap.parse_args()

cfg = polaris_config()
con = duckdb.connect()
con.execute(f"SET extension_directory='{EXT_DIR}'")
for ext in EXTENSIONS:
    con.execute(f"FORCE INSTALL {ext} FROM {args.repo}")
    con.execute(f"LOAD {ext}")
q = lambda v: "'" + str(v).replace("'", "''") + "'"  # noqa: E731
con.execute(f"""CREATE OR REPLACE PERSISTENT SECRET polaris_secret (
    TYPE iceberg, CLIENT_ID {q(cfg["client_id"])}, CLIENT_SECRET {q(cfg["client_secret"])},
    OAUTH2_SCOPE {q(cfg["scope"])}, OAUTH2_SERVER_URI {q(cfg["token_uri"])})""")
print(f"duckdb {duckdb.__version__}; iceberg extension {iceberg_extension_version()} ({args.repo}) in {EXT_DIR}")
print("persistent secret polaris_secret: created")
