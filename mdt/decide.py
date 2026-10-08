"""Decide data checks from Iceberg metadata, scanning only what metadata can't settle.

A check here is "does any row violate X?". Each decider looks at per-file
manifest stats (bounds, null and value counts) and declared identifier fields,
and returns either:

  dict      a verdict reached from metadata: {"status": "pass"|"fail", "how", "failures"}
            (a metadata-proven FAIL reports a lower bound, e.g. ">=3")
  Residual  the files and columns still needed, plus the SQL that counts
            failures over them. `run_batched` reads each table once for all
            residuals on it and runs that SQL in local DuckDB.

Rules (see docs/FINDINGS.md for what they rely on):

  * inclusive / strict metrics evaluation of NOT(expr), as Iceberg scan
    planning does; the SQL NULL semantics are kept by adding `col IS NOT NULL`
    unless the check itself tests for NULL
  * witnesses: an exact bound is a value that really occurs in the file, and
    null_count > 0 is a real NULL; if one violates, a violation exists
  * single-valued files (exact min == max) are fully decided by that value
  * exact string bounds: the truncation length is inferred per table as the
    longest string bound seen (always <= the writer's limit), so a shorter
    bound is guaranteed untruncated (Fivetran writes 32, not Iceberg's 16)
  * uniqueness on a superset of the declared identifier fields passes (this
    trusts the writer to upsert by that key; Iceberg doesn't enforce it)
"""
from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import duckdb
import pyarrow as pa
from pyiceberg.conversions import from_bytes
from pyiceberg.expressions import AlwaysTrue, And, IsNull, Not, NotNull, Or
from pyiceberg.expressions.visitors import (
    _InclusiveMetricsEvaluator,
    _StrictMetricsEvaluator,
    expression_evaluator,
)
from pyiceberg.io.pyarrow import ArrowScan
from pyiceberg.typedef import Record
from pyiceberg.types import BinaryType, DoubleType, FixedType, FloatType, StringType

from mdt.catalog import get_catalog

ICEBERG_DEFAULT_TRUNCATE = 16

# ---------------------------------------------------------------- tables


@dataclass
class TableCtx:
    """A table loaded and planned once (manifests only), shared by all checks on it."""

    name: str
    table: object
    tasks: list
    schema: object
    identifier: set
    trunc: int
    plan_ms: float

    def field(self, col):
        return self.schema.find_field(col)

    def exact(self, col, b):
        """Decoded bound if it is guaranteed to be a real value in the file, else None."""
        if b is None:
            return None
        t = self.field(col).field_type
        if isinstance(t, (BinaryType, FixedType)):
            return None
        v = from_bytes(t, b)
        if isinstance(t, (FloatType, DoubleType)) and v != v:
            return None
        if isinstance(t, StringType) and len(v) >= self.trunc:
            return None
        return v

    def scan(self, cols, tasks=None, row_filter=AlwaysTrue()):
        proj = self.table.scan(selected_fields=tuple(cols)).projection()
        return ArrowScan(self.table.metadata, self.table.io, proj, row_filter, True).to_table(
            self.tasks if tasks is None else tasks)


class TableCache:
    """Thread-safe: each table is loaded and planned at most once."""

    def __init__(self, catalog=None):
        self.cat = catalog or get_catalog()
        self.tables: dict[str, TableCtx] = {}
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def get(self, name: str) -> TableCtx:
        with self._lock:
            lock = self._locks.setdefault(name, threading.Lock())
        with lock:
            if name not in self.tables:
                t0 = time.perf_counter()
                t = self.cat.load_table(name)
                tasks = list(t.scan().plan_files())
                schema = t.schema()
                ident = {schema.find_field(i).name for i in schema.identifier_field_ids}
                strs = {f.field_id for f in schema.fields if isinstance(f.field_type, StringType)}
                longest = ICEBERG_DEFAULT_TRUNCATE
                for task in tasks:
                    for fid in strs:
                        for b in ((task.file.lower_bounds or {}).get(fid), (task.file.upper_bounds or {}).get(fid)):
                            if b is not None:
                                longest = max(longest, len(b.decode("utf-8", "replace")))
                self.tables[name] = TableCtx(name, t, tasks, schema, ident, longest,
                                             (time.perf_counter() - t0) * 1e3)
            return self.tables[name]


# ---------------------------------------------------------------- residuals


class Residual:
    """Undecided check: `failure_sql` counts failures over view `rows` holding `cols` of `tasks`."""

    def __init__(self, ctx: TableCtx, tasks, cols, failure_sql: str, how="narrowed scan", extra=None):
        self.ctx, self.tasks, self.cols, self.failure_sql, self.how = ctx, tasks, cols, failure_sql, how
        self.extra = extra or {}  # name -> pyarrow.Table registered alongside `rows`


def run_batched(residuals: list[Residual]) -> list[dict]:
    """Read each table once (union of columns over union of undecided files), then count
    each residual's failures. Rows from files metadata settled can't fail, so counts are exact."""
    by_table: dict[str, list[int]] = {}
    for i, r in enumerate(residuals):
        by_table.setdefault(r.ctx.name, []).append(i)
    out: list[dict] = [{}] * len(residuals)

    def one_table(idxs):
        ctx = residuals[idxs[0]].ctx
        seen, tasks, cols = set(), [], []
        for i in idxs:
            for task in residuals[i].tasks:
                if task.file.file_path not in seen:
                    seen.add(task.file.file_path)
                    tasks.append(task)
            cols += [c for c in residuals[i].cols if c not in cols]
        t0 = time.perf_counter()
        rows = ctx.scan(cols, tasks)
        scan_ms = (time.perf_counter() - t0) * 1e3
        con = duckdb.connect()
        con.register("rows", rows)
        for i in idxs:
            r = residuals[i]
            for name, tbl in r.extra.items():
                con.register(name, tbl)
            n = con.execute(r.failure_sql).fetchone()[0]
            out[i] = {"status": "fail" if n else "pass", "how": r.how, "failures": n,
                      "scanned_files": len(r.tasks), "batch_files": len(tasks), "batch_cols": cols,
                      "batch_scan_ms": scan_ms}

    with ThreadPoolExecutor(max_workers=len(by_table) or 1) as pool:
        list(pool.map(one_table, by_table.values()))
    return out


# ---------------------------------------------------------------- deciders


def cols_in(expr) -> set:
    if isinstance(expr, (And, Or)):
        return cols_in(expr.left) | cols_in(expr.right)
    if isinstance(expr, Not):
        return cols_in(expr.child)
    return {expr.term.name}


def has_null_test(expr) -> bool:
    if isinstance(expr, (And, Or)):
        return has_null_test(expr.left) or has_null_test(expr.right)
    if isinstance(expr, Not):
        return has_null_test(expr.child)
    return isinstance(expr, (IsNull, NotNull))


def decide_expr(ctx: TableCtx, expr, failure_sql: str):
    """Pass iff no row has NOT(expr) TRUE (SQL semantics). Single-column `expr`."""
    (col,) = cols_in(expr)
    null_test = has_null_test(expr)
    neg = Not(expr) if null_test else And(Not(expr), NotNull(col))
    might = _InclusiveMetricsEvaluator(ctx.schema, neg)
    must = _StrictMetricsEvaluator(ctx.schema, neg)
    violates = expression_evaluator(ctx.schema, neg, True)
    fid = ctx.field(col).field_id
    pos = [f.field_id for f in ctx.schema.fields].index(fid)

    def row(v):
        vals = [None] * len(ctx.schema.fields)
        vals[pos] = v
        return Record(*vals)

    proven, undecided, settled = 0, [], 0
    for task in ctx.tasks:
        f = task.file
        if not might.eval(f):
            continue
        if f.record_count and must.eval(f):
            proven += f.record_count
            continue
        lo = ctx.exact(col, (f.lower_bounds or {}).get(fid))
        hi = ctx.exact(col, (f.upper_bounds or {}).get(fid))
        nulls = (f.null_value_counts or {}).get(fid) if f.null_value_counts else None
        cands = [v for v in (lo, hi) if v is not None] + ([None] if nulls else [])
        if any(violates(row(v)) for v in cands):
            proven += 1
        elif lo is not None and lo == hi and (nulls == 0 or not null_test):
            settled += 1  # single-valued and OK; NULLs can't violate a null-rejecting check
        else:
            undecided.append(task)
    if proven:
        return {"status": "fail", "how": "metadata", "failures": f">={proven}", "scanned_files": 0}
    if not undecided:
        return {"status": "pass", "how": "metadata", "failures": 0, "scanned_files": 0}
    return Residual(ctx, undecided, [col], failure_sql)


def decide_unique(ctx: TableCtx, cols: list, failure_sql: str):
    if ctx.identifier and ctx.identifier <= set(cols):
        return {"status": "pass", "how": "metadata (declared key)", "failures": 0, "scanned_files": 0}
    return Residual(ctx, list(ctx.tasks), list(cols), failure_sql, how="full scan")


def decide_relationships(child: TableCtx, col: str, parent: TableCtx, pfield: str, failure_sql: str):
    """Child values must exist in parent.pfield. `failure_sql` may use table `parents(k)`."""
    parent_vals = {v for v in parent.scan([pfield]).column(pfield).to_pylist() if v is not None}
    fid = child.field(col).field_id
    proven, undecided = 0, []
    for task in child.tasks:
        f = task.file
        values, nulls = (f.value_counts or {}).get(fid), (f.null_value_counts or {}).get(fid)
        if values is not None and nulls is not None and values == nulls:
            continue  # all NULL: NULL children are ignored
        lo = child.exact(col, (f.lower_bounds or {}).get(fid))
        hi = child.exact(col, (f.upper_bounds or {}).get(fid))
        if (lo is not None and lo not in parent_vals) or (hi is not None and hi not in parent_vals):
            proven += 1  # a real child value with no parent
        elif lo is not None and lo == hi:
            pass  # every non-NULL value in the file is lo, which has a parent
        else:
            undecided.append(task)
    if proven:
        return {"status": "fail", "how": "metadata (+parent keys)", "failures": f">={proven}", "scanned_files": 0}
    if not undecided:
        return {"status": "pass", "how": "metadata (+parent keys)", "failures": 0, "scanned_files": 0}
    return Residual(child, undecided, [col], failure_sql,
                    extra={"parents": pa.table({"k": pa.array(sorted(parent_vals))})})


_PERIOD = {"minute": "minutes", "hour": "hours", "day": "days"}


def max_timestamp(ctx: TableCtx, col: str) -> tuple[datetime | None, int]:
    """Exact MAX(col) for a timestamp column from upper bounds; files without bounds are scanned.
    Returns (max, scanned_files)."""
    fid = ctx.field(col).field_id
    best, scan_tasks = None, []
    for task in ctx.tasks:
        f = task.file
        values, nulls = (f.value_counts or {}).get(fid), (f.null_value_counts or {}).get(fid)
        ub = (f.upper_bounds or {}).get(fid)
        if ub is None:
            if not (values is not None and nulls is not None and values == nulls):
                scan_tasks.append(task)
            continue
        v = from_bytes(ctx.field(col).field_type, ub)
        best = v if best is None else max(best, v)
    if scan_tasks:
        m = max((x for x in ctx.scan([col], scan_tasks).column(col).to_pylist() if x is not None), default=None)
        if m is not None:
            m = int(m.timestamp() * 1e6)
            best = m if best is None else max(best, m)
    return (None if best is None else datetime.fromtimestamp(best / 1e6, tz=timezone.utc)), len(scan_tasks)


def decide_freshness(ctx: TableCtx, col: str, warn_after=None, error_after=None) -> dict:
    """dbt source freshness: max(loaded_at) vs now, against {count, period} thresholds."""
    max_loaded, scanned = max_timestamp(ctx, col)
    age = datetime.now(timezone.utc) - max_loaded
    td = lambda c: timedelta(**{_PERIOD[c["period"]]: c["count"]})  # noqa: E731
    status = ("error" if error_after and age > td(error_after)
              else "warn" if warn_after and age > td(warn_after) else "pass")
    return {"status": status, "how": "metadata" if not scanned else "metadata + narrowed scan",
            "max_loaded_at": max_loaded.isoformat(), "scanned_files": scanned}


# ---------------------------------------------------------------- SQL helpers


def q(c: str) -> str:
    return '"' + c.replace('"', '""') + '"'


def lit(v) -> str:
    return "'" + str(v).replace("'", "''") + "'" if isinstance(v, str) else str(v)
