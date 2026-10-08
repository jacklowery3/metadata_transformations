"""Experiment cases: public `cases.json` plus an optional, gitignored `cases.local.json`.

Keep cases that reference private datasets in `cases.local.json`; it is never
committed. Both files hold a JSON list; local cases are appended.
"""
from __future__ import annotations

import json
from pathlib import Path


def load_cases(directory: Path, name: str = "cases") -> list:
    cases = []
    for suffix in ("", ".local"):
        p = Path(directory) / f"{name}{suffix}.json"
        if p.exists():
            cases += json.loads(p.read_text())
    return cases
