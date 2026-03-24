import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import duckdb

from .config import settings

logger = logging.getLogger(__name__)

_WINDOW_RE = re.compile(r"^(\d+)([hmd])$")


def _parse_window(window: str) -> timedelta:
    m = _WINDOW_RE.match(window)
    if not m:
        raise ValueError(f"Invalid window format '{window}'. Expected e.g. '1h', '30m', '7d'.")
    value, unit = int(m.group(1)), m.group(2)
    if unit == "h":
        return timedelta(hours=value)
    if unit == "m":
        return timedelta(minutes=value)
    return timedelta(days=value)


def _glob_for_pipeline(pipeline_id: str) -> str:
    base = settings.local_path
    return f"{base}/pipeline_id={pipeline_id}/date=*/part-*.parquet"


def _violation_glob(pipeline_id: str) -> str:
    base = settings.local_path
    return f"{base}/violations/pipeline_id={pipeline_id}/part-*.parquet"


def _any_parquet_exists(glob_pattern: str) -> bool:
    base = glob_pattern.split("/pipeline_id=")[0]
    pipeline_part = glob_pattern.split("/pipeline_id=")[1].split("/")[0]
    path = Path(base) / f"pipeline_id={pipeline_part}"
    return path.exists() and any(path.rglob("part-*.parquet"))


def query_feature_health(
    pipeline_id: str,
    feature_name: str,
    window: str,
    compare_to: str | None,
) -> dict[str, Any]:
    glob_pattern = _glob_for_pipeline(pipeline_id)

    if not _any_parquet_exists(glob_pattern):
        return _empty_feature_health(pipeline_id, feature_name, window)

    delta = _parse_window(window)
    cutoff = (datetime.now(tz=timezone.utc) - delta).isoformat()

    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT
                processing_time,
                input_cardinality,
                output_cardinality,
                watermark,
                capture_drop_since_last,
                emit_interval_ms
            FROM read_parquet('{glob_pattern}')
            WHERE feature_name = ?
              AND processing_time >= ?
            ORDER BY processing_time DESC
            """,
            [feature_name, cutoff],
        ).fetchall()
    except duckdb.IOException:
        logger.warning("No Parquet data found at %s", glob_pattern)
        return _empty_feature_health(pipeline_id, feature_name, window)
    finally:
        con.close()

    cardinality_trend = [
        {
            "processing_time": r[0],
            "input_cardinality": r[1],
            "output_cardinality": r[2],
            "watermark": r[3],
            "capture_drop_since_last": r[4],
        }
        for r in rows
    ]

    capture_drops = any(r[4] for r in rows)
    emit_interval_ms: int | None = rows[0][5] if rows and rows[0][5] else None

    watermark_lag_ms: int | None = None
    if rows and rows[0][3]:
        try:
            wm_dt = datetime.fromisoformat(rows[0][3])
            now = datetime.now(tz=timezone.utc)
            if wm_dt.tzinfo is None:
                wm_dt = wm_dt.replace(tzinfo=timezone.utc)
            watermark_lag_ms = max(0, int((now - wm_dt).total_seconds() * 1000))
        except (ValueError, TypeError):
            pass

    comparison: dict[str, Any] | None = None
    if compare_to:
        comparison = _build_comparison(pipeline_id, feature_name, window, compare_to, glob_pattern)

    return {
        "feature_name": feature_name,
        "pipeline_id": pipeline_id,
        "window": window,
        "cardinality_trend": cardinality_trend,
        "watermark_lag_ms": watermark_lag_ms,
        "capture_drops": capture_drops,
        "emit_interval_ms": emit_interval_ms,
        "freshness_violation": False,
        "comparison": comparison,
    }


def _build_comparison(
    pipeline_id: str,
    feature_name: str,
    window: str,
    compare_to: str,
    glob_pattern: str,
) -> dict[str, Any]:
    delta = _parse_window(window)
    compare_delta = _parse_window(compare_to.replace("_ago", ""))
    now = datetime.now(tz=timezone.utc)
    compare_end = (now - compare_delta).isoformat()
    compare_start = (now - compare_delta - delta).isoformat()

    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT
                avg(input_cardinality) as avg_input,
                avg(output_cardinality) as avg_output,
                bool_or(capture_drop_since_last) as any_drops
            FROM read_parquet('{glob_pattern}')
            WHERE feature_name = ?
              AND processing_time >= ?
              AND processing_time < ?
            """,
            [feature_name, compare_start, compare_end],
        ).fetchone()
    except duckdb.IOException:
        return {}
    finally:
        con.close()

    if not rows or rows[0] is None:
        return {}
    return {
        "period": compare_to,
        "avg_input_cardinality": rows[0],
        "avg_output_cardinality": rows[1],
        "any_drops": rows[2],
    }


def _empty_feature_health(pipeline_id: str, feature_name: str, window: str) -> dict[str, Any]:
    return {
        "feature_name": feature_name,
        "pipeline_id": pipeline_id,
        "window": window,
        "cardinality_trend": [],
        "watermark_lag_ms": None,
        "capture_drops": False,
        "emit_interval_ms": None,
        "freshness_violation": False,
        "comparison": None,
    }


def query_pipeline_health(pipeline_id: str) -> list[dict[str, Any]]:
    glob_pattern = _glob_for_pipeline(pipeline_id)

    if not _any_parquet_exists(glob_pattern):
        return []

    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT
                operator_id,
                operator_type,
                max(processing_time) as last_seen,
                sum(input_cardinality) as total_input,
                bool_or(capture_drop_since_last) as any_drops
            FROM read_parquet('{glob_pattern}')
            GROUP BY operator_id, operator_type
            ORDER BY operator_id
            """,
        ).fetchall()
    except duckdb.IOException:
        logger.warning("No Parquet data found at %s", glob_pattern)
        return []
    finally:
        con.close()

    return [
        {
            "operator_id": r[0],
            "operator_type": r[1],
            "last_seen": r[2],
            "total_input": int(r[3]) if r[3] is not None else 0,
            "any_drops": bool(r[4]),
        }
        for r in rows
    ]


def query_violations(
    pipeline_id: str | None,
    feature_name: str | None,
    since: str | None,
    severity_gte: str | None,
) -> list[dict[str, Any]]:
    _severity_rank = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
    min_rank = _severity_rank.get((severity_gte or "LOW").upper(), 0)

    base = settings.local_path
    if pipeline_id:
        glob_pattern = _violation_glob(pipeline_id)
        if not _any_violation_exists(pipeline_id):
            return []
    else:
        glob_pattern = f"{base}/violations/pipeline_id=*/part-*.parquet"
        viol_root = Path(base) / "violations"
        if not viol_root.exists() or not any(viol_root.rglob("part-*.parquet")):
            return []

    conditions = ["1=1"]
    params: list[Any] = []

    if feature_name:
        conditions.append("feature_name = ?")
        params.append(feature_name)
    if since:
        conditions.append("detected_at >= ?")
        params.append(since)

    where_clause = " AND ".join(conditions)

    con = duckdb.connect()
    try:
        rows = con.execute(
            f"""
            SELECT pipeline_id, feature_name, violation_type, detected_at, severity, detail
            FROM read_parquet('{glob_pattern}')
            WHERE {where_clause}
            ORDER BY detected_at DESC
            """,
            params,
        ).fetchall()
    except duckdb.IOException:
        return []
    finally:
        con.close()

    result = []
    for r in rows:
        row_rank = _severity_rank.get((r[4] or "LOW").upper(), 0)
        if row_rank >= min_rank:
            result.append({
                "pipeline_id": r[0],
                "feature_name": r[1],
                "violation_type": r[2],
                "detected_at": r[3],
                "severity": r[4],
                "detail": r[5],
            })
    return result


def _any_violation_exists(pipeline_id: str) -> bool:
    base = settings.local_path
    path = Path(base) / "violations" / f"pipeline_id={pipeline_id}"
    return path.exists() and any(path.rglob("part-*.parquet"))


def check_freshness_violation(
    pipeline_id: str,
    feature_name: str,
    emit_interval_ms: int | None,
) -> bool:
    if not emit_interval_ms:
        return False

    window_ms = emit_interval_ms * 3
    cutoff = (
        datetime.now(tz=timezone.utc) - timedelta(milliseconds=window_ms)
    ).isoformat()

    glob_pattern = _glob_for_pipeline(pipeline_id)
    if not _any_parquet_exists(glob_pattern):
        return True

    con = duckdb.connect()
    try:
        row = con.execute(
            f"""
            SELECT count(*) FROM read_parquet('{glob_pattern}')
            WHERE feature_name = ?
              AND processing_time >= ?
            """,
            [feature_name, cutoff],
        ).fetchone()
    except duckdb.IOException:
        return True
    finally:
        con.close()

    return row is None or row[0] == 0
