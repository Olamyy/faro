import struct
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

import api.config as cfg
from api.main import app
from api.models import CaptureEvent
from api.store import ParquetStore


@pytest.fixture(autouse=True)
def set_local_path(tmp_path):
    original = cfg.settings.local_path
    cfg.settings.local_path = str(tmp_path)
    yield tmp_path
    cfg.settings.local_path = original


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _agg(pipeline_id="pipe-1", feature="temp", trace_id="trace-abc", span_id="span-1") -> CaptureEvent:
    return CaptureEvent(
        pipeline_id=pipeline_id,
        operator_id="op-1",
        operator_type="FILTER",
        capture_mode="AGGREGATE",
        processing_time=_now(),
        trace_id=trace_id,
        span_id=span_id,
        input_cardinality=10,
        output_cardinality=8,
        emit_interval_ms=5000,
        capture_drop_since_last=False,
        feature_name=feature,
    )


def _entity(entity_id: str, value: float, pipeline_id="pipe-1", feature="temp", trace_id="trace-abc") -> CaptureEvent:
    return CaptureEvent(
        pipeline_id=pipeline_id,
        operator_id="op-2",
        operator_type="WINDOW",
        capture_mode="ENTITY",
        processing_time=_now(),
        event_time=_now(),
        trace_id=trace_id,
        span_id="span-entity",
        input_cardinality=1,
        output_cardinality=1,
        emit_interval_ms=0,
        capture_drop_since_last=False,
        feature_name=feature,
        entity_id=entity_id,
        feature_value=struct.pack(">d", value),
        feature_value_type="SCALAR_DOUBLE",
    )


def test_trace_lookup_returns_events_for_trace():
    ParquetStore.write_events([
        _agg(trace_id="trace-abc", span_id="span-1"),
        _entity("device-1", 42.0, trace_id="trace-abc"),
        _agg(trace_id="trace-other", span_id="span-x"),
    ])
    client = TestClient(app)
    resp = client.get("/traces/trace-abc")
    assert resp.status_code == 200
    body = resp.json()
    assert body["trace_id"] == "trace-abc"
    events = body["events"]
    assert len(events) == 2
    assert all(e["trace_id"] == "trace-abc" for e in events)


def test_trace_lookup_empty_for_unknown_trace():
    client = TestClient(app)
    resp = client.get("/traces/no-such-trace")
    assert resp.status_code == 200
    assert resp.json()["events"] == []


def test_entity_features_returns_latest_per_pipeline_feature():
    ParquetStore.write_events([
        _entity("device-1", 10.0, pipeline_id="pipe-a", feature="temp"),
        _entity("device-1", 20.0, pipeline_id="pipe-a", feature="temp"),
        _entity("device-1", 5.0, pipeline_id="pipe-b", feature="humidity"),
        _entity("device-2", 99.0, pipeline_id="pipe-a", feature="temp"),
    ])
    client = TestClient(app)
    resp = client.get("/entities/device-1/features")
    assert resp.status_code == 200
    body = resp.json()
    assert body["entity_id"] == "device-1"
    features = body["features"]
    assert len(features) == 2
    by_key = {(f["pipeline_id"], f["feature_name"]): f for f in features}
    assert ("pipe-a", "temp") in by_key
    assert ("pipe-b", "humidity") in by_key
    assert by_key[("pipe-a", "temp")]["feature_value_decoded"] == 20.0


def test_entity_features_empty_for_unknown_entity():
    client = TestClient(app)
    resp = client.get("/entities/nobody/features")
    assert resp.status_code == 200
    assert resp.json()["features"] == []
