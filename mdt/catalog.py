"""Polaris catalog connection for a Fivetran Managed Data Lake (MDLS) destination.

Credentials come from (first match wins):
  1. POLARIS_* environment variables / a .env file in the repo root
  2. the lakehouse-explorer skill config (~/.claude/skills/lakehouse-explorer/.lakehouse_config.json)

Only OAuth client-credentials are used; Polaris vends scoped S3 credentials per
table load (same model as unified_lake_tables -- see its HOW_IT_WORKS.md).
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from dotenv import load_dotenv
from pyiceberg.catalog import load_catalog
from pyiceberg.catalog.rest import RestCatalog

REPO_ROOT = Path(__file__).resolve().parent.parent
SKILL_CONFIG = Path.home() / ".claude/skills/lakehouse-explorer/.lakehouse_config.json"

# Puffin blob types pyiceberg 0.11.1's pydantic model accepts. Fivetran also
# writes "fivetran-synced-distribution", which makes load_table() raise a
# ValidationError -- so we drop unknown blob types from the REST response.
KNOWN_BLOB_TYPES = {"apache-datasketches-theta-v1", "deletion-vector-v1"}


def polaris_config() -> dict:
    load_dotenv(REPO_ROOT / ".env")
    if os.getenv("POLARIS_CATALOG_URI"):
        uri = os.environ["POLARIS_CATALOG_URI"]
        return {
            "uri": uri,
            "warehouse": os.environ["POLARIS_WAREHOUSE"],
            "client_id": os.environ["POLARIS_CLIENT_ID"],
            "client_secret": os.environ["POLARIS_CLIENT_SECRET"],
            "token_uri": os.getenv("POLARIS_TOKEN_URI") or f"{uri}/v1/oauth/tokens",
            "scope": os.getenv("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL"),
            "region": os.getenv("BUCKET_REGION", ""),
        }
    if SKILL_CONFIG.exists():
        p = json.loads(SKILL_CONFIG.read_text())["polaris"]
        return {
            "uri": p["polaris_endpoint"],
            "warehouse": p["polaris_warehouse"],
            "client_id": p["polaris_client_id"],
            "client_secret": p["polaris_client_secret"],
            "token_uri": f"{p['polaris_endpoint']}/v1/oauth/tokens",
            "scope": "PRINCIPAL_ROLE:ALL",
            "region": p.get("bucket_region", ""),
        }
    raise RuntimeError("No Polaris credentials: fill in .env (see .env.example).")


def _strip_unknown_blobs(resp, *args, **kwargs):
    if resp.status_code != 200 or "/tables/" not in resp.url:
        return resp
    try:
        body = resp.json()
    except ValueError:
        return resp
    stats = body.get("metadata", {}).get("statistics") if isinstance(body, dict) else None
    if stats:
        for s in stats:
            s["blob-metadata"] = [b for b in s.get("blob-metadata", []) if b.get("type") in KNOWN_BLOB_TYPES]
        resp._content = json.dumps(body).encode()
    return resp


def get_catalog() -> RestCatalog:
    cfg = polaris_config()
    props = {
        "type": "rest",
        "uri": cfg["uri"],
        "warehouse": cfg["warehouse"],
        "credential": f"{cfg['client_id']}:{cfg['client_secret']}",
        "scope": cfg["scope"],
        "oauth2-server-uri": cfg["token_uri"],
    }
    if cfg["region"]:
        props["s3.region"] = cfg["region"]
    cat = load_catalog("polaris", **props)
    # Private attribute: pyiceberg's RestCatalog keeps its requests.Session here.
    cat._session.hooks["response"].append(_strip_unknown_blobs)
    return cat
