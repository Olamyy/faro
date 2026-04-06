"""Tests that health-check writes violations at most once per cooldown window."""
from datetime import datetime, timezone
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import api.config as cfg
from api.main import app
from api.store import ParquetStore

_EVENT_SCHEMA = pa.schema([
    ("schema_version", pa.string()),
    ("pipeline_id", pa.string()),
    ("operator_id", pa.string()),
    ("operator_type", pa.string()),
    ("feature_name", pa.string()),
    ("capture_mode", pa.string()),
    ("event_time", pa.string()),
    ("event_time_min", pa.string()),
    ("processing_time", pa.string()),
    ("watermark", pa.string()),
    ("window_start", pa.string()),
    ("window_end", pa.string()),
    ("late_event_count", pa.int64()),
    ("late_tracking_mode", pa.string()),
    ("input_cardinality", pa.int64()),
    ("output_cardinality", pa.int64()),
    ("emit_interval_ms", pa.int64()),
    ("timer_fired_count", pa.int64()),
    ("async_pending_count", pa.int64()),
    ("pattern_match_count", pa.int64()),
    ("join_input_side", pa.string()),
    ("join_lower_bound_ms", pa.int64()),
    ("join_upper_bound_ms", pa.int64()),
    ("join_match_rate", pa.float64()),
    ("value_count", pa.int64()),
    ("value_min", pa.float64()),
    ("value_max", pa.float64()),
    ("value_mean", pa.float64()),
    ("value_p50", pa.float64()),
    ("value_p95", pa.float64()),
    ("null_count", pa.int64()),
    ("entity_id", pa.string()),
    ("feature_value", pa.binary()),
    ("feature_value_type", pa.string()),
    ("upstream_source", pa.string()),
    ("upstream_system", pa.string()),
    ("trace_id", pa.string()),
    ("span_id", pa.string()),
    ("parent_span_id", pa.string()),
    ("capture_drop_since_last", pa.bool_()),
])


def _make_row(pipeline_id, processing_time, emit_interval_ms):
    return pa.table({
        "schema_version": ["1.0.0"],
        "pipeline_id": [pipeline_id],
        "operator_id": ["op-1"],
        "operator_type": ["WINDOW"],
        "feature_name": ["temp"],
        "capture_mode": ["AGGREGATE"],
        "event_time": [None],
        "event_time_min": [None],
        "processing_time": [processing_time],
        "watermark": [None],
        "window_start": [None],
        "window_end": [None],
        "late_event_count": pa.array([None], type=pa.int64()),
        "late_tracking_mode": [None],
        "input_cardinality": pa.array([100], type=pa.int64()),
        "output_cardinality": pa.array([50], type=pa.int64()),
        "emit_interval_ms": pa.array([emit_interval_ms], type=pa.int64()),
        "timer_fired_count": pa.array([None], type=pa.int64()),
        "async_pending_count": pa.array([None], type=pa.int64()),
        "pattern_match_count": pa.array([None], type=pa.int64()),
        "join_input_side": [None],
        "join_lower_bound_ms": pa.array([None], type=pa.int64()),
        "join_upper_bound_ms": pa.array([None], type=pa.int64()),
        "join_match_rate": pa.array([None], type=pa.float64()),
        "value_count": pa.array([None], type=pa.int64()),
        "value_min": pa.array([None], type=pa.float64()),
        "value_max": pa.array([None], type=pa.float64()),
        "value_mean": pa.array([None], type=pa.float64()),
        "value_p50": pa.array([None], type=pa.float64()),
        "value_p95": pa.array([None], type=pa.float64()),
        "null_count": pa.array([None], type=pa.int64()),
        "entity_id": [None],
        "feature_value": pa.array([None], type=pa.binary()),
        "feature_value_type": [None],
        "upstream_source": [None],
        "upstream_system": [None],
        "trace_id": ["t"],
        "span_id": ["s"],
        "parent_span_id": [None],
        "capture_drop_since_last": [False],
    }, schema=_EVENT_SCHEMA)


@pytest.fixture()
def stale_feature_store(tmp_path):
    """Seed a store so freshness fires.

    We write one recent event with emit_interval_ms=1 (1 ms). The freshness
    check window is 3 × emit_interval_ms = 3 ms, so any event older than ~3 ms
    makes the feature stale. query_feature_health (window=1h) finds the recent
    event and returns emit_interval_ms=1.  check_freshness_violation then looks
    for an event within the last 3 ms and finds nothing → freshness=True.
    """
    original = cfg.settings.local_path
    cfg.settings.local_path = str(tmp_path)

    pipeline_id = "pipe-dedup"
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    part_dir = tmp_path / f"pipeline_id={pipeline_id}" / f"date={date_str}"
    part_dir.mkdir(parents=True)

    # 5 minutes ago — inside the 1h health window but far outside the 3ms freshness window
    five_min_ago = (datetime.now(tz=timezone.utc).replace(second=0, microsecond=0)
                    .isoformat()).replace("+00:00", "Z")
    from datetime import timedelta as _td
    five_min_ago = (datetime.now(tz=timezone.utc) - _td(minutes=5)).isoformat()

    table = _make_row(pipeline_id, five_min_ago, emit_interval_ms=1)
    pq.write_table(table, str(part_dir / "part-0001.parquet"))

    yield pipeline_id, tmp_path

    cfg.settings.local_path = original


def _count_violations(tmp_path, pipeline_id: str, violation_type: str) -> int:
    import glob as glob_mod
    pattern = str(tmp_path / "violations" / f"pipeline_id={pipeline_id}" / "*.parquet")
    files = glob_mod.glob(pattern)
    if not files:
        return 0
    import duckdb
    con = duckdb.connect()
    rows = con.execute(
        f"SELECT count(*) FROM read_parquet({files!r}) WHERE violation_type = ?",
        [violation_type],
    ).fetchone()
    con.close()
    return rows[0]


def test_repeated_health_check_writes_violation_once(stale_feature_store):
    pipeline_id, tmp_path = stale_feature_store
    client = TestClient(app)

    for _ in range(5):
        resp = client.get(f"/features/temp/health?pipeline_id={pipeline_id}")
        assert resp.status_code == 200

    count = _count_violations(tmp_path, pipeline_id, "FRESHNESS")
    assert count == 1, f"Expected 1 FRESHNESS violation, got {count}"


def test_has_recent_violation_returns_false_when_no_violations(stale_feature_store):
    pipeline_id, tmp_path = stale_feature_store
    result = ParquetStore.has_recent_violation(pipeline_id, "FRESHNESS", "temp")
    assert result is False


def test_has_recent_violation_returns_true_after_write(stale_feature_store):
    pipeline_id, tmp_path = stale_feature_store
    from datetime import datetime, timezone
    ParquetStore.write_violation(
        pipeline_id=pipeline_id,
        feature_name="temp",
        violation_type="FRESHNESS",
        detected_at=datetime.now(tz=timezone.utc).isoformat(),
        severity="HIGH",
        detail="test",
    )
    result = ParquetStore.has_recent_violation(pipeline_id, "FRESHNESS", "temp")
    assert result is True
