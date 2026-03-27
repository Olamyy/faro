import struct
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

import api.config as cfg
from api.main import app
from api.store import ParquetStore
from api.models import CaptureEvent


@pytest.fixture(autouse=True)
def set_local_path(tmp_path):
    original = cfg.settings.local_path
    cfg.settings.local_path = str(tmp_path)
    yield tmp_path
    cfg.settings.local_path = original


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _entity_event(entity_id: str, value: float, feature_name: str = "temperature") -> CaptureEvent:
    return CaptureEvent(
        pipeline_id="pipe-1",
        operator_id="op-1",
        operator_type="WINDOW",
        capture_mode="ENTITY",
        processing_time=_now(),
        event_time=_now(),
        trace_id="trace-abc",
        span_id="span-abc",
        input_cardinality=1,
        output_cardinality=1,
        emit_interval_ms=0,
        capture_drop_since_last=False,
        feature_name=feature_name,
        entity_id=entity_id,
        feature_value=struct.pack(">d", value),
        feature_value_type="SCALAR_DOUBLE",
    )


def test_summary_returns_stats():
    ParquetStore.write_events([
        _entity_event("a", 10.0),
        _entity_event("b", 20.0),
        _entity_event("c", 30.0),
        _entity_event("d", 40.0),
        _entity_event("e", 50.0),
    ])

    client = TestClient(app)
    resp = client.get("/features/temperature/values/summary?pipeline_id=pipe-1&window=1d")
    assert resp.status_code == 200

    body = resp.json()
    assert body["feature_name"] == "temperature"
    assert body["pipeline_id"] == "pipe-1"
    assert body["entity_count"] == 5
    assert body["null_count"] == 0
    assert pytest.approx(body["value_min"]) == 10.0
    assert pytest.approx(body["value_max"]) == 50.0
    assert pytest.approx(body["value_mean"]) == 30.0
    assert body["value_p50"] is not None
    assert body["value_p95"] is not None


def test_summary_empty_when_no_data():
    client = TestClient(app)
    resp = client.get("/features/temperature/values/summary?pipeline_id=pipe-missing&window=1h")
    assert resp.status_code == 200

    body = resp.json()
    assert body["entity_count"] == 0
    assert body["value_min"] is None
    assert body["value_max"] is None
    assert body["null_count"] == 0


def test_summary_excludes_aggregate_events():
    from api.models import CaptureEvent as CE
    agg = CE(
        pipeline_id="pipe-1",
        operator_id="op-1",
        operator_type="WINDOW",
        capture_mode="AGGREGATE",
        processing_time=_now(),
        trace_id="t", span_id="s",
        input_cardinality=100, output_cardinality=100,
        emit_interval_ms=30000, capture_drop_since_last=False,
        feature_name="temperature",
    )
    ParquetStore.write_events([agg, _entity_event("x", 99.0)])

    client = TestClient(app)
    resp = client.get("/features/temperature/values/summary?pipeline_id=pipe-1&window=1d")
    assert resp.status_code == 200

    body = resp.json()
    assert body["entity_count"] == 1
    assert pytest.approx(body["value_min"]) == 99.0


def test_summary_null_count_for_none_values():
    no_value = CaptureEvent(
        pipeline_id="pipe-1",
        operator_id="op-1",
        operator_type="WINDOW",
        capture_mode="ENTITY",
        processing_time=_now(),
        trace_id="t", span_id="s",
        input_cardinality=1, output_cardinality=1,
        emit_interval_ms=0, capture_drop_since_last=False,
        feature_name="temperature",
        entity_id="unknown",
        feature_value_type="SCALAR_DOUBLE",
    )
    ParquetStore.write_events([no_value, _entity_event("x", 5.0)])

    client = TestClient(app)
    resp = client.get("/features/temperature/values/summary?pipeline_id=pipe-1&window=1d")
    assert resp.status_code == 200

    body = resp.json()
    assert body["entity_count"] == 1
    assert pytest.approx(body["value_min"]) == 5.0
