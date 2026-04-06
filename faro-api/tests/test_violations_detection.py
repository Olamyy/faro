import struct
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import api.config as cfg
from api.main import app
from api.models import CaptureEvent
from api.store import ParquetStore, _SCHEMA


@pytest.fixture(autouse=True)
def set_local_path(tmp_path):
    original = cfg.settings.local_path
    cfg.settings.local_path = str(tmp_path)
    yield tmp_path
    cfg.settings.local_path = original


def _ts(delta: timedelta) -> str:
    return (datetime.now(tz=timezone.utc) + delta).isoformat()


def _write_entity_rows(tmp_path, pipeline_id: str, rows: list[dict]):
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    out_dir = tmp_path / f"pipeline_id={pipeline_id}" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    n = len(rows)
    table = pa.table({
        "schema_version": [None] * n,
        "pipeline_id": [pipeline_id] * n,
        "operator_id": ["op-1"] * n,
        "operator_type": ["WINDOW"] * n,
        "feature_name": ["temp"] * n,
        "capture_mode": ["ENTITY"] * n,
        "event_time": [r["processing_time"] for r in rows],
        "event_time_min": [None] * n,
        "processing_time": [r["processing_time"] for r in rows],
        "watermark": [None] * n,
        "window_start": [None] * n,
        "window_end": [None] * n,
        "late_event_count": pa.array([None] * n, type=pa.int64()),
        "late_tracking_mode": [None] * n,
        "input_cardinality": pa.array([1] * n, type=pa.int64()),
        "output_cardinality": pa.array([1] * n, type=pa.int64()),
        "emit_interval_ms": pa.array([0] * n, type=pa.int64()),
        "timer_fired_count": pa.array([None] * n, type=pa.int64()),
        "async_pending_count": pa.array([None] * n, type=pa.int64()),
        "pattern_match_count": pa.array([None] * n, type=pa.int64()),
        "join_input_side": [None] * n,
        "join_lower_bound_ms": pa.array([None] * n, type=pa.int64()),
        "join_upper_bound_ms": pa.array([None] * n, type=pa.int64()),
        "join_match_rate": pa.array([None] * n, type=pa.float64()),
        "value_count": pa.array([None] * n, type=pa.int64()),
        "value_min": pa.array([None] * n, type=pa.float64()),
        "value_max": pa.array([None] * n, type=pa.float64()),
        "value_mean": pa.array([None] * n, type=pa.float64()),
        "value_p50": pa.array([None] * n, type=pa.float64()),
        "value_p95": pa.array([None] * n, type=pa.float64()),
        "null_count": pa.array([None] * n, type=pa.int64()),
        "entity_id": ["e1"] * n,
        "feature_value": pa.array([r["feature_value"] for r in rows], type=pa.binary()),
        "feature_value_type": [r["feature_value_type"] for r in rows],
        "upstream_source": [None] * n,
        "upstream_system": [None] * n,
        "trace_id": ["t"] * n,
        "span_id": ["s"] * n,
        "parent_span_id": [None] * n,
        "capture_drop_since_last": [False] * n,
    }, schema=_SCHEMA)
    pq.write_table(table, str(out_dir / "part-001.parquet"))


def _write_agg_rows(tmp_path, pipeline_id: str, rows: list[dict]):
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    out_dir = tmp_path / f"pipeline_id={pipeline_id}" / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    n = len(rows)
    table = pa.table({
        "schema_version": [None] * n,
        "pipeline_id": [pipeline_id] * n,
        "operator_id": ["op-1"] * n,
        "operator_type": ["FILTER"] * n,
        "feature_name": ["temp"] * n,
        "capture_mode": ["AGGREGATE"] * n,
        "event_time": [None] * n,
        "event_time_min": [None] * n,
        "processing_time": [r["processing_time"] for r in rows],
        "watermark": [None] * n,
        "window_start": [None] * n,
        "window_end": [None] * n,
        "late_event_count": pa.array([None] * n, type=pa.int64()),
        "late_tracking_mode": [None] * n,
        "input_cardinality": pa.array([r["input_c"] for r in rows], type=pa.int64()),
        "output_cardinality": pa.array([r["output_c"] for r in rows], type=pa.int64()),
        "emit_interval_ms": pa.array([5000] * n, type=pa.int64()),
        "timer_fired_count": pa.array([None] * n, type=pa.int64()),
        "async_pending_count": pa.array([None] * n, type=pa.int64()),
        "pattern_match_count": pa.array([None] * n, type=pa.int64()),
        "join_input_side": [None] * n,
        "join_lower_bound_ms": pa.array([None] * n, type=pa.int64()),
        "join_upper_bound_ms": pa.array([None] * n, type=pa.int64()),
        "join_match_rate": pa.array([None] * n, type=pa.float64()),
        "value_count": pa.array([None] * n, type=pa.int64()),
        "value_min": pa.array([None] * n, type=pa.float64()),
        "value_max": pa.array([None] * n, type=pa.float64()),
        "value_mean": pa.array([None] * n, type=pa.float64()),
        "value_p50": pa.array([None] * n, type=pa.float64()),
        "value_p95": pa.array([None] * n, type=pa.float64()),
        "null_count": pa.array([None] * n, type=pa.int64()),
        "entity_id": [None] * n,
        "feature_value": pa.array([None] * n, type=pa.binary()),
        "feature_value_type": [None] * n,
        "upstream_source": [None] * n,
        "upstream_system": [None] * n,
        "trace_id": ["t"] * n,
        "span_id": ["s"] * n,
        "parent_span_id": [None] * n,
        "capture_drop_since_last": [False] * n,
    }, schema=_SCHEMA)
    pq.write_table(table, str(out_dir / "part-agg.parquet"))


def _pack(v: float) -> bytes:
    return struct.pack(">d", v)


def test_mean_drift_violation_written_when_drift_exceeds_threshold(tmp_path):
    prev_rows = [
        {"processing_time": _ts(timedelta(minutes=-90)), "feature_value": _pack(10.0), "feature_value_type": "SCALAR_DOUBLE"},
        {"processing_time": _ts(timedelta(minutes=-80)), "feature_value": _pack(10.0), "feature_value_type": "SCALAR_DOUBLE"},
    ]
    curr_rows = [
        {"processing_time": _ts(timedelta(minutes=-10)), "feature_value": _pack(100.0), "feature_value_type": "SCALAR_DOUBLE"},
        {"processing_time": _ts(timedelta(minutes=-5)), "feature_value": _pack(100.0), "feature_value_type": "SCALAR_DOUBLE"},
    ]
    _write_entity_rows(tmp_path, "pipe-1", prev_rows + curr_rows)

    client = TestClient(app)
    client.get("/features/temp/health?pipeline_id=pipe-1&window=1h")

    resp = client.get("/violations?pipeline_id=pipe-1&violation_type=MEAN_DRIFT")
    assert resp.status_code == 200
    assert len(resp.json()["violations"]) >= 1


def test_null_rate_violation_written_when_null_rate_exceeds_threshold(tmp_path):
    rows = [
        {"processing_time": _ts(timedelta(minutes=-i)), "feature_value": None, "feature_value_type": None}
        for i in range(1, 10)
    ] + [
        {"processing_time": _ts(timedelta(minutes=-30)), "feature_value": _pack(1.0), "feature_value_type": "SCALAR_DOUBLE"},
    ]
    _write_entity_rows(tmp_path, "pipe-1", rows)

    client = TestClient(app)
    client.get("/features/temp/health?pipeline_id=pipe-1&window=1h")

    resp = client.get("/violations?pipeline_id=pipe-1&violation_type=NULL_RATE")
    assert resp.status_code == 200
    assert len(resp.json()["violations"]) >= 1


def test_cardinality_anomaly_violation_written_when_ratio_drops(tmp_path):
    prev_rows = [
        {"processing_time": _ts(timedelta(minutes=-90)), "input_c": 100, "output_c": 80},
    ]
    curr_rows = [
        {"processing_time": _ts(timedelta(minutes=-10)), "input_c": 100, "output_c": 10},
    ]
    _write_agg_rows(tmp_path, "pipe-1", prev_rows + curr_rows)

    client = TestClient(app)
    client.get("/features/temp/health?pipeline_id=pipe-1&window=1h")

    resp = client.get("/violations?pipeline_id=pipe-1&violation_type=CARDINALITY_ANOMALY")
    assert resp.status_code == 200
    assert len(resp.json()["violations"]) >= 1


def test_no_false_positive_drift_when_stable(tmp_path):
    rows = [
        {"processing_time": _ts(timedelta(minutes=-90)), "feature_value": _pack(10.0), "feature_value_type": "SCALAR_DOUBLE"},
        {"processing_time": _ts(timedelta(minutes=-10)), "feature_value": _pack(10.0), "feature_value_type": "SCALAR_DOUBLE"},
    ]
    _write_entity_rows(tmp_path, "pipe-1", rows)

    client = TestClient(app)
    client.get("/features/temp/health?pipeline_id=pipe-1&window=1h")

    resp = client.get("/violations?pipeline_id=pipe-1&violation_type=MEAN_DRIFT")
    assert resp.status_code == 200
    assert resp.json()["violations"] == []


def test_no_false_positive_null_rate_when_rate_is_low(tmp_path):
    rows = [
        {"processing_time": _ts(timedelta(minutes=-i)), "feature_value": _pack(float(i)), "feature_value_type": "SCALAR_DOUBLE"}
        for i in range(1, 11)
    ]
    _write_entity_rows(tmp_path, "pipe-1", rows)

    client = TestClient(app)
    client.get("/features/temp/health?pipeline_id=pipe-1&window=1h")

    resp = client.get("/violations?pipeline_id=pipe-1&violation_type=NULL_RATE")
    assert resp.status_code == 200
    assert resp.json()["violations"] == []


def test_no_false_positive_cardinality_anomaly_when_ratio_stable(tmp_path):
    rows = [
        {"processing_time": _ts(timedelta(minutes=-90)), "input_c": 100, "output_c": 80},
        {"processing_time": _ts(timedelta(minutes=-10)), "input_c": 100, "output_c": 80},
    ]
    _write_agg_rows(tmp_path, "pipe-1", rows)

    client = TestClient(app)
    client.get("/features/temp/health?pipeline_id=pipe-1&window=1h")

    resp = client.get("/violations?pipeline_id=pipe-1&violation_type=CARDINALITY_ANOMALY")
    assert resp.status_code == 200
    assert resp.json()["violations"] == []


def test_freshness_violation_written_when_aggregate_events_stale(tmp_path):
    stale_rows = [
        {"processing_time": _ts(timedelta(hours=-2)), "input_c": 100, "output_c": 80},
    ]
    _write_agg_rows(tmp_path, "pipe-1", stale_rows)

    agg_event = CaptureEvent(
        pipeline_id="pipe-1",
        operator_id="op-1",
        operator_type="FILTER",
        capture_mode="AGGREGATE",
        processing_time=_ts(timedelta(hours=-2)),
        trace_id="t",
        span_id="s",
        input_cardinality=100,
        output_cardinality=80,
        emit_interval_ms=5000,
        capture_drop_since_last=False,
        feature_name="temp",
    )
    ParquetStore.write_events([agg_event])

    client = TestClient(app)
    client.get("/features/temp/health?pipeline_id=pipe-1&window=3h")

    resp = client.get("/violations?pipeline_id=pipe-1&violation_type=FRESHNESS")
    assert resp.status_code == 200
    assert len(resp.json()["violations"]) >= 1


def test_freshness_no_violation_when_aggregate_events_recent(tmp_path):
    agg_event = CaptureEvent(
        pipeline_id="pipe-1",
        operator_id="op-1",
        operator_type="FILTER",
        capture_mode="AGGREGATE",
        processing_time=_ts(timedelta(minutes=-1)),
        trace_id="t",
        span_id="s",
        input_cardinality=100,
        output_cardinality=80,
        emit_interval_ms=30000,
        capture_drop_since_last=False,
        feature_name="temp",
    )
    ParquetStore.write_events([agg_event])

    client = TestClient(app)
    client.get("/features/temp/health?pipeline_id=pipe-1&window=1h")

    resp = client.get("/violations?pipeline_id=pipe-1&violation_type=FRESHNESS")
    assert resp.status_code == 200
    assert resp.json()["violations"] == []
