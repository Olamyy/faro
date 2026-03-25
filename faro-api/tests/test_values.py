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


def test_values_returns_decoded_entity_values():
    ParquetStore.write_events([
        _entity_event("device-A", 42.5),
        _entity_event("device-B", 17.0),
    ])

    client = TestClient(app)
    resp = client.get("/features/temperature/values?pipeline_id=pipe-1&window=1d")
    assert resp.status_code == 200

    body = resp.json()
    assert body["feature_name"] == "temperature"
    assert body["pipeline_id"] == "pipe-1"
    assert len(body["values"]) == 2

    by_entity = {v["entity_id"]: v for v in body["values"]}
    assert pytest.approx(by_entity["device-A"]["feature_value_decoded"]) == 42.5
    assert pytest.approx(by_entity["device-B"]["feature_value_decoded"]) == 17.0
    assert by_entity["device-A"]["feature_value_type"] == "SCALAR_DOUBLE"


def test_values_filters_by_entity_id():
    ParquetStore.write_events([
        _entity_event("device-A", 42.5),
        _entity_event("device-B", 17.0),
    ])

    client = TestClient(app)
    resp = client.get("/features/temperature/values?pipeline_id=pipe-1&window=1d&entity_id=device-A")
    assert resp.status_code == 200

    body = resp.json()
    assert len(body["values"]) == 1
    assert body["values"][0]["entity_id"] == "device-A"


def test_values_empty_when_no_data():
    client = TestClient(app)
    resp = client.get("/features/temperature/values?pipeline_id=pipe-missing&window=1h")
    assert resp.status_code == 200
    assert resp.json()["values"] == []


def test_values_excludes_aggregate_events():
    from api.models import CaptureEvent as CE
    agg_event = CE(
        pipeline_id="pipe-1",
        operator_id="op-1",
        operator_type="WINDOW",
        capture_mode="AGGREGATE",
        processing_time=_now(),
        trace_id="trace-abc",
        span_id="span-abc",
        input_cardinality=100,
        output_cardinality=100,
        emit_interval_ms=30000,
        capture_drop_since_last=False,
        feature_name="temperature",
    )
    ParquetStore.write_events([agg_event, _entity_event("device-A", 5.0)])

    client = TestClient(app)
    resp = client.get("/features/temperature/values?pipeline_id=pipe-1&window=1d")
    assert resp.status_code == 200

    body = resp.json()
    assert all(v["entity_id"] is not None for v in body["values"])
    assert len(body["values"]) == 1


def test_values_respects_limit():
    ParquetStore.write_events([_entity_event(f"device-{i}", float(i)) for i in range(20)])

    client = TestClient(app)
    resp = client.get("/features/temperature/values?pipeline_id=pipe-1&window=1d&limit=5")
    assert resp.status_code == 200
    assert len(resp.json()["values"]) == 5
