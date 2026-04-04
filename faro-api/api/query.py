import logging
import re
import struct
from datetime import datetime, timedelta, timezone
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
    if settings.storage_backend == "s3":
        return f"s3://{settings.s3_bucket}/{settings.s3_prefix}pipeline_id={pipeline_id}/date=*/part-*.parquet"
    return f"{settings.local_path}/pipeline_id={pipeline_id}/date=*/part-*.parquet"


def _violation_glob(pipeline_id: str) -> str:
    if settings.storage_backend == "s3":
        return f"s3://{settings.s3_bucket}/{settings.s3_prefix}violations/pipeline_id={pipeline_id}/part-*.parquet"
    return f"{settings.local_path}/violations/pipeline_id={pipeline_id}/part-*.parquet"


def _all_violations_glob() -> str:
    if settings.storage_backend == "s3":
        return f"s3://{settings.s3_bucket}/{settings.s3_prefix}violations/pipeline_id=*/part-*.parquet"
    return f"{settings.local_path}/violations/pipeline_id=*/part-*.parquet"


def _configure_s3(con: duckdb.DuckDBPyConnection) -> None:
    if settings.storage_backend != "s3":
        return
    con.execute(f"SET s3_region='{settings.s3_region}'")
    if settings.s3_access_key_id:
        con.execute(f"SET s3_access_key_id='{settings.s3_access_key_id}'")
    if settings.s3_secret_access_key:
        con.execute(f"SET s3_secret_access_key='{settings.s3_secret_access_key}'")
    if settings.s3_endpoint_url:
        con.execute(f"SET s3_endpoint='{settings.s3_endpoint_url}'")


def query_feature_health(
    pipeline_id: str,
    feature_name: str,
    window: str,
    compare_to: str | None,
    operator_id: str | None = None,
    end_time: str | None = None,
) -> dict[str, Any]:
    glob_pattern = _glob_for_pipeline(pipeline_id)
    delta = _parse_window(window)
    cutoff = (datetime.now(tz=timezone.utc) - delta).isoformat()

    conditions = ["capture_mode = 'AGGREGATE'", "feature_name = ?", "processing_time >= ?"]
    params: list[Any] = [feature_name, cutoff]
    if end_time:
        conditions.append("processing_time < ?")
        params.append(end_time)
    if operator_id:
        conditions.append("operator_id = ?")
        params.append(operator_id)
    where_clause = " AND ".join(conditions)

    con = duckdb.connect()
    _configure_s3(con)
    try:
        rows = con.execute(
            f"""
            SELECT
                processing_time,
                input_cardinality,
                output_cardinality,
                watermark,
                capture_drop_since_last,
                emit_interval_ms,
                output_cardinality * 1.0 / NULLIF(input_cardinality, 0) as filter_ratio
            FROM read_parquet('{glob_pattern}')
            WHERE {where_clause}
            ORDER BY processing_time DESC
            """,
            params,
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
            "filter_ratio": float(r[6]) if r[6] is not None else None,
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
    _configure_s3(con)
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
        "comparison": None,
    }


def query_pipeline_health(
    pipeline_id: str,
    window: str = "24h",
    operator_id: str | None = None,
) -> list[dict[str, Any]]:
    glob_pattern = _glob_for_pipeline(pipeline_id)
    delta = _parse_window(window)
    cutoff = (datetime.now(tz=timezone.utc) - delta).isoformat()

    conditions = ["capture_mode = 'AGGREGATE'", "processing_time >= ?"]
    params: list[Any] = [cutoff]
    if operator_id:
        conditions.append("operator_id = ?")
        params.append(operator_id)
    where_clause = " AND ".join(conditions)

    con = duckdb.connect()
    _configure_s3(con)
    try:
        rows = con.execute(
            f"""
            SELECT
                operator_id,
                operator_type,
                max(processing_time) as last_seen,
                sum(input_cardinality) as total_input,
                bool_or(capture_drop_since_last) as any_drops,
                avg(output_cardinality * 1.0 / NULLIF(input_cardinality, 0)) as filter_ratio
            FROM read_parquet('{glob_pattern}')
            WHERE {where_clause}
            GROUP BY operator_id, operator_type
            ORDER BY operator_id
            """,
            params,
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
            "filter_ratio": float(r[5]) if r[5] is not None else None,
        }
        for r in rows
    ]


_SEVERITY_RANK = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
_SEVERITY_BY_RANK = {v: k for k, v in _SEVERITY_RANK.items()}


def query_violations(
    pipeline_id: str | None,
    feature_name: str | None,
    since: str | None,
    severity_gte: str | None,
    violation_type: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> tuple[list[dict[str, Any]], int]:
    if pipeline_id:
        glob_pattern = _violation_glob(pipeline_id)
    else:
        glob_pattern = _all_violations_glob()

    min_rank = _SEVERITY_RANK.get((severity_gte or "LOW").upper(), 0)
    eligible_severities = [k for k, v in _SEVERITY_RANK.items() if v >= min_rank]

    conditions = [f"severity IN ({', '.join(repr(s) for s in eligible_severities)})"]
    params: list[Any] = []

    if feature_name:
        conditions.append("feature_name = ?")
        params.append(feature_name)
    if since:
        conditions.append("detected_at >= ?")
        params.append(since)
    if violation_type:
        conditions.append("violation_type = ?")
        params.append(violation_type)

    where_clause = " AND ".join(conditions)

    con = duckdb.connect()
    _configure_s3(con)
    try:
        total_row = con.execute(
            f"SELECT count(*) FROM read_parquet('{glob_pattern}') WHERE {where_clause}",
            params,
        ).fetchone()
        total = int(total_row[0]) if total_row else 0

        rows = con.execute(
            f"""
            SELECT pipeline_id, feature_name, violation_type, detected_at, severity, detail
            FROM read_parquet('{glob_pattern}')
            WHERE {where_clause}
            ORDER BY detected_at DESC
            LIMIT {limit} OFFSET {offset}
            """,
            params,
        ).fetchall()
    except duckdb.IOException:
        return [], 0
    finally:
        con.close()

    return [
        {
            "pipeline_id": r[0],
            "feature_name": r[1],
            "violation_type": r[2],
            "detected_at": r[3],
            "severity": r[4],
            "detail": r[5],
        }
        for r in rows
    ], total


def _decode_feature_value(raw: bytes | None, value_type: str | None) -> float | int | str | None:
    if raw is None or value_type is None:
        return None
    try:
        if value_type == "SCALAR_DOUBLE":
            return struct.unpack(">d", raw)[0]
        if value_type == "SCALAR_LONG":
            return struct.unpack(">q", raw)[0]
        if value_type == "SCALAR_STRING":
            return raw.decode("utf-8")
    except Exception:
        logger.warning("Failed to decode feature_value for type %s", value_type)
    return None


def query_entity_values(
    pipeline_id: str,
    feature_name: str,
    window: str,
    entity_id: str | None,
    limit: int,
    capture_mode: str | None = "ENTITY",
    operator_id: str | None = None,
) -> list[dict[str, Any]]:
    glob_pattern = _glob_for_pipeline(pipeline_id)
    delta = _parse_window(window)
    cutoff = (datetime.now(tz=timezone.utc) - delta).isoformat()

    conditions = ["feature_name = ?", "processing_time >= ?"]
    params: list[Any] = [feature_name, cutoff]

    if capture_mode and capture_mode.upper() != "ALL":
        conditions.append("capture_mode = ?")
        params.append(capture_mode.upper())

    if entity_id:
        conditions.append("entity_id = ?")
        params.append(entity_id)

    if operator_id:
        conditions.append("operator_id = ?")
        params.append(operator_id)

    where_clause = " AND ".join(conditions)

    con = duckdb.connect()
    _configure_s3(con)
    try:
        rows = con.execute(
            f"""
            SELECT entity_id, feature_value, feature_value_type, processing_time, event_time
            FROM read_parquet('{glob_pattern}')
            WHERE {where_clause}
            ORDER BY processing_time DESC
            LIMIT {limit}
            """,
            params,
        ).fetchall()
    except (duckdb.IOException, duckdb.BinderException):
        logger.warning("Could not query entity values at %s", glob_pattern)
        return []
    finally:
        con.close()

    return [
        {
            "entity_id": r[0],
            "feature_value_decoded": _decode_feature_value(r[1], r[2]),
            "feature_value_type": r[2],
            "processing_time": r[3],
            "event_time": r[4],
        }
        for r in rows
    ]


def query_entity_value_summary(
    pipeline_id: str,
    feature_name: str,
    window: str,
) -> dict[str, Any] | None:
    glob_pattern = _glob_for_pipeline(pipeline_id)
    delta = _parse_window(window)
    cutoff = (datetime.now(tz=timezone.utc) - delta).isoformat()

    con = duckdb.connect()
    _configure_s3(con)
    try:
        rows = con.execute(
            f"""
            SELECT feature_value, feature_value_type
            FROM read_parquet('{glob_pattern}')
            WHERE capture_mode = 'ENTITY'
              AND feature_name = ?
              AND processing_time >= ?
              AND feature_value IS NOT NULL
            """,
            [feature_name, cutoff],
        ).fetchall()
    except (duckdb.IOException, duckdb.BinderException):
        logger.warning("Could not query entity values at %s", glob_pattern)
        return None
    finally:
        con.close()

    numeric: list[float] = []
    null_count = 0
    entity_count = 0

    for raw, vtype in rows:
        entity_count += 1
        decoded = _decode_feature_value(raw, vtype)
        if decoded is None or not isinstance(decoded, (int, float)):
            null_count += 1
        else:
            numeric.append(float(decoded))

    if not numeric:
        return {
            "feature_name": feature_name,
            "pipeline_id": pipeline_id,
            "window": window,
            "entity_count": entity_count,
            "value_min": None,
            "value_max": None,
            "value_mean": None,
            "value_p50": None,
            "value_p95": None,
            "null_count": null_count,
        }

    values_sql = ", ".join(f"({v})" for v in numeric)
    stats_con = duckdb.connect()
    try:
        stats = stats_con.execute(
            f"""
            SELECT
                min(v), max(v), avg(v),
                percentile_cont(0.50) WITHIN GROUP (ORDER BY v),
                percentile_cont(0.95) WITHIN GROUP (ORDER BY v)
            FROM (VALUES {values_sql}) t(v)
            """
        ).fetchone()
    finally:
        stats_con.close()

    return {
        "feature_name": feature_name,
        "pipeline_id": pipeline_id,
        "window": window,
        "entity_count": entity_count,
        "value_min": stats[0],
        "value_max": stats[1],
        "value_mean": stats[2],
        "value_p50": stats[3],
        "value_p95": stats[4],
        "null_count": null_count,
    }


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

    con = duckdb.connect()
    _configure_s3(con)
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


def _compute_mean_for_window(
    glob_pattern: str,
    feature_name: str,
    start: str,
    end: str | None,
) -> float | None:
    con = duckdb.connect()
    _configure_s3(con)
    conditions = ["feature_name = ?", "capture_mode = 'ENTITY'", "feature_value IS NOT NULL", "processing_time >= ?"]
    params: list[Any] = [feature_name, start]
    if end:
        conditions.append("processing_time < ?")
        params.append(end)
    where_clause = " AND ".join(conditions)
    try:
        rows = con.execute(
            f"""
            SELECT feature_value, feature_value_type
            FROM read_parquet('{glob_pattern}')
            WHERE {where_clause}
            """,
            params,
        ).fetchall()
    except (duckdb.IOException, duckdb.BinderException):
        return None
    finally:
        con.close()

    numeric = [_decode_feature_value(r[0], r[1]) for r in rows]
    numeric = [v for v in numeric if isinstance(v, (int, float))]
    if not numeric:
        return None
    return sum(numeric) / len(numeric)


def check_mean_drift(
    pipeline_id: str,
    feature_name: str,
    window: str,
) -> bool:
    glob_pattern = _glob_for_pipeline(pipeline_id)
    delta = _parse_window(window)
    now = datetime.now(tz=timezone.utc)
    current_start = (now - delta).isoformat()
    prev_start = (now - 2 * delta).isoformat()
    prev_end = current_start

    current_mean = _compute_mean_for_window(glob_pattern, feature_name, current_start, None)
    prev_mean = _compute_mean_for_window(glob_pattern, feature_name, prev_start, prev_end)

    if current_mean is None or prev_mean is None or prev_mean == 0:
        return False

    return abs(current_mean - prev_mean) / abs(prev_mean) > settings.drift_threshold


def check_null_rate(
    pipeline_id: str,
    feature_name: str,
    window: str,
) -> bool:
    glob_pattern = _glob_for_pipeline(pipeline_id)
    delta = _parse_window(window)
    cutoff = (datetime.now(tz=timezone.utc) - delta).isoformat()

    con = duckdb.connect()
    _configure_s3(con)
    try:
        row = con.execute(
            f"""
            SELECT
                count(*) FILTER (WHERE feature_value IS NULL),
                count(*)
            FROM read_parquet('{glob_pattern}')
            WHERE feature_name = ?
              AND capture_mode = 'ENTITY'
              AND processing_time >= ?
            """,
            [feature_name, cutoff],
        ).fetchone()
    except (duckdb.IOException, duckdb.BinderException):
        return False
    finally:
        con.close()

    if row is None or row[1] == 0:
        return False

    return (row[0] / row[1]) > settings.null_rate_threshold


def check_cardinality_anomaly(
    pipeline_id: str,
    feature_name: str,
    window: str,
) -> bool:
    glob_pattern = _glob_for_pipeline(pipeline_id)
    delta = _parse_window(window)
    now = datetime.now(tz=timezone.utc)
    current_start = (now - delta).isoformat()
    prev_start = (now - 2 * delta).isoformat()
    prev_end = current_start

    con = duckdb.connect()
    _configure_s3(con)
    try:
        row = con.execute(
            f"""
            SELECT
                avg(CASE WHEN processing_time >= ? THEN output_cardinality * 1.0 / NULLIF(input_cardinality, 0) END),
                avg(CASE WHEN processing_time >= ? AND processing_time < ? THEN output_cardinality * 1.0 / NULLIF(input_cardinality, 0) END)
            FROM read_parquet('{glob_pattern}')
            WHERE feature_name = ?
              AND capture_mode = 'AGGREGATE'
              AND processing_time >= ?
            """,
            [current_start, prev_start, prev_end, feature_name, prev_start],
        ).fetchone()
    except (duckdb.IOException, duckdb.BinderException):
        return False
    finally:
        con.close()

    if row is None or row[0] is None or row[1] is None or row[1] == 0:
        return False

    return (row[1] - row[0]) / row[1] > settings.cardinality_drop_threshold


def query_list_pipelines() -> list[str]:
    if settings.storage_backend == "s3":
        glob_pattern = f"s3://{settings.s3_bucket}/{settings.s3_prefix}pipeline_id=*/date=*/part-*.parquet"
    else:
        glob_pattern = f"{settings.local_path}/pipeline_id=*/date=*/part-*.parquet"

    con = duckdb.connect()
    _configure_s3(con)
    try:
        rows = con.execute(
            f"SELECT DISTINCT pipeline_id FROM read_parquet('{glob_pattern}') ORDER BY pipeline_id"
        ).fetchall()
    except duckdb.IOException:
        return []
    finally:
        con.close()

    return [r[0] for r in rows]


def query_list_features(pipeline_id: str) -> list[str]:
    glob_pattern = _glob_for_pipeline(pipeline_id)

    con = duckdb.connect()
    _configure_s3(con)
    try:
        rows = con.execute(
            f"SELECT DISTINCT feature_name FROM read_parquet('{glob_pattern}') WHERE feature_name IS NOT NULL ORDER BY feature_name"
        ).fetchall()
    except duckdb.IOException:
        return []
    finally:
        con.close()

    return [r[0] for r in rows]


def _all_pipelines_glob() -> str:
    if settings.storage_backend == "s3":
        return f"s3://{settings.s3_bucket}/{settings.s3_prefix}pipeline_id=*/date=*/part-*.parquet"
    return f"{settings.local_path}/pipeline_id=*/date=*/part-*.parquet"


def query_trace_events(trace_id: str) -> list[dict[str, Any]]:
    glob_pattern = _all_pipelines_glob()

    con = duckdb.connect()
    _configure_s3(con)
    try:
        rows = con.execute(
            f"""
            SELECT pipeline_id, operator_id, operator_type, feature_name, capture_mode,
                   processing_time, trace_id, span_id, parent_span_id,
                   input_cardinality, output_cardinality
            FROM read_parquet('{glob_pattern}')
            WHERE trace_id = ?
            ORDER BY processing_time ASC
            """,
            [trace_id],
        ).fetchall()
    except duckdb.IOException:
        return []
    finally:
        con.close()

    return [
        {
            "pipeline_id": r[0],
            "operator_id": r[1],
            "operator_type": r[2],
            "feature_name": r[3],
            "capture_mode": r[4],
            "processing_time": r[5],
            "trace_id": r[6],
            "span_id": r[7],
            "parent_span_id": r[8],
            "input_cardinality": r[9],
            "output_cardinality": r[10],
        }
        for r in rows
    ]


def query_entity_feature_points(entity_id: str) -> list[dict[str, Any]]:
    glob_pattern = _all_pipelines_glob()

    con = duckdb.connect()
    _configure_s3(con)
    try:
        rows = con.execute(
            f"""
            SELECT DISTINCT ON (pipeline_id, feature_name)
                pipeline_id, feature_name, feature_value, feature_value_type, processing_time
            FROM read_parquet('{glob_pattern}')
            WHERE entity_id = ?
              AND capture_mode = 'ENTITY'
              AND feature_name IS NOT NULL
            ORDER BY pipeline_id, feature_name, processing_time DESC
            """,
            [entity_id],
        ).fetchall()
    except (duckdb.IOException, duckdb.BinderException):
        return []
    finally:
        con.close()

    return [
        {
            "pipeline_id": r[0],
            "feature_name": r[1],
            "feature_value_decoded": _decode_feature_value(r[2], r[3]),
            "processing_time": r[4],
        }
        for r in rows
    ]
