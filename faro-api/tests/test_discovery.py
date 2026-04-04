import pytest
from fastapi.testclient import TestClient

import api.config as cfg
from api.main import app
from api.models import CaptureEvent
from api.store import ParquetStore
from datetime import datetime, timezone


@pytest.fixture(autouse=True)
def set_local_path(tmp_path):
    original = cfg.settings.local_path
    cfg.settings.local_path = str(tmp_path)
    yield tmp_path
    cfg.settings.local_path = original


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _event(pipeline_id: str, feature_name: str) -> CaptureEvent:
    return CaptureEvent(
        pipeline_id=pipeline_id,
        operator_id="op-1",
        operator_type="FILTER",
        capture_mode="AGGREGATE",
        processing_time=_now(),
        trace_id="t",
        span_id="s",
        input_cardinality=10,
        output_cardinality=8,
        emit_interval_ms=5000,
        capture_drop_since_last=False,
        feature_name=feature_name,
    )


def test_list_pipelines_returns_known_pipeline():
    ParquetStore.write_events([_event("pipe-disco", "temp")])
    client = TestClient(app)
    resp = client.get("/pipelines")
    assert resp.status_code == 200
    assert "pipe-disco" in resp.json()["pipelines"]


def test_list_features_for_pipeline():
    ParquetStore.write_events([
        _event("pipe-feat", "temperature"),
        _event("pipe-feat", "humidity"),
    ])
    client = TestClient(app)
    resp = client.get("/pipelines/pipe-feat/features")
    assert resp.status_code == 200
    features = resp.json()["features"]
    assert set(features) == {"temperature", "humidity"}
    assert resp.json()["pipeline_id"] == "pipe-feat"


def test_list_features_empty_for_unknown_pipeline():
    client = TestClient(app)
    resp = client.get("/pipelines/no-such-pipeline/features")
    assert resp.status_code == 200
    assert resp.json()["features"] == []
