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


def _agg(pipeline_id="pipe-1", operator_id="op-filter", input_c=100, output_c=60, feature="temp"):
    return CaptureEvent(
        pipeline_id=pipeline_id,
        operator_id=operator_id,
        operator_type="FILTER",
        capture_mode="AGGREGATE",
        processing_time=_now(),
        trace_id="trace-1",
        span_id="span-1",
        input_cardinality=input_c,
        output_cardinality=output_c,
        emit_interval_ms=5000,
        capture_drop_since_last=False,
        feature_name=feature,
    )


def _entity(pipeline_id="pipe-1", operator_id="op-filter", feature="temp"):
    return CaptureEvent(
        pipeline_id=pipeline_id,
        operator_id=operator_id,
        operator_type="FILTER",
        capture_mode="ENTITY",
        processing_time=_now(),
        trace_id="trace-1",
        span_id="span-2",
        input_cardinality=1,
        output_cardinality=1,
        emit_interval_ms=0,
        capture_drop_since_last=False,
        feature_name=feature,
        entity_id="device-1",
        feature_value=struct.pack(">d", 42.0),
        feature_value_type="SCALAR_DOUBLE",
    )


def test_pipeline_health_excludes_entity_rows_from_cardinality():
    """ENTITY rows must not inflate total_input in pipeline health."""
    ParquetStore.write_events([
        _agg(input_c=100, output_c=60),
        _entity(),
        _entity(),
        _entity(),
    ])
    client = TestClient(app)
    resp = client.get("/pipelines/pipe-1/health")
    assert resp.status_code == 200
    ops = resp.json()["operators"]
    assert len(ops) == 1
    assert ops[0]["total_input"] == 100


def test_pipeline_health_exposes_filter_ratio():
    ParquetStore.write_events([_agg(input_c=100, output_c=60)])
    client = TestClient(app)
    resp = client.get("/pipelines/pipe-1/health")
    assert resp.status_code == 200
    ops = resp.json()["operators"]
    assert pytest.approx(ops[0]["filter_ratio"], abs=0.01) == 0.60


def test_pipeline_health_window_param():
    ParquetStore.write_events([_agg()])
    client = TestClient(app)
    resp = client.get("/pipelines/pipe-1/health?window=1h")
    assert resp.status_code == 200
    assert len(resp.json()["operators"]) == 1


def test_pipeline_health_operator_id_filter():
    ParquetStore.write_events([
        _agg(operator_id="op-a"),
        _agg(operator_id="op-b"),
    ])
    client = TestClient(app)
    resp = client.get("/pipelines/pipe-1/health?operator_id=op-a")
    assert resp.status_code == 200
    ops = resp.json()["operators"]
    assert len(ops) == 1
    assert ops[0]["operator_id"] == "op-a"


def test_feature_health_cardinality_trend_excludes_entity_rows():
    ParquetStore.write_events([
        _agg(input_c=200, output_c=100),
        _entity(),
    ])
    client = TestClient(app)
    resp = client.get("/features/temp/health?pipeline_id=pipe-1")
    assert resp.status_code == 200
    trend = resp.json()["cardinality_trend"]
    assert len(trend) == 1
    assert trend[0]["input_cardinality"] == 200


def test_feature_health_filter_ratio_in_trend():
    ParquetStore.write_events([_agg(input_c=100, output_c=40)])
    client = TestClient(app)
    resp = client.get("/features/temp/health?pipeline_id=pipe-1")
    assert resp.status_code == 200
    trend = resp.json()["cardinality_trend"]
    assert pytest.approx(trend[0]["filter_ratio"], abs=0.01) == 0.40


def test_feature_health_operator_id_filter():
    ParquetStore.write_events([
        _agg(operator_id="op-a", input_c=10),
        _agg(operator_id="op-b", input_c=20),
    ])
    client = TestClient(app)
    resp = client.get("/features/temp/health?pipeline_id=pipe-1&operator_id=op-b")
    assert resp.status_code == 200
    trend = resp.json()["cardinality_trend"]
    assert len(trend) == 1
    assert trend[0]["input_cardinality"] == 20


def test_values_capture_mode_aggregate():
    """GET /features/{name}/values?capture_mode=AGGREGATE returns aggregate rows."""
    ParquetStore.write_events([
        _agg(),
        _entity(),
    ])
    client = TestClient(app)
    resp = client.get("/features/temp/values?pipeline_id=pipe-1&window=1d&capture_mode=AGGREGATE")
    assert resp.status_code == 200
    values = resp.json()["values"]
    assert len(values) == 1
    assert values[0]["entity_id"] is None


def test_values_capture_mode_none_returns_all():
    ParquetStore.write_events([_agg(), _entity()])
    client = TestClient(app)
    resp = client.get("/features/temp/values?pipeline_id=pipe-1&window=1d&capture_mode=ALL")
    assert resp.status_code == 200
    assert len(resp.json()["values"]) == 2
