import pytest
from fastapi.testclient import TestClient

import api.config as cfg
from api.main import app


@pytest.fixture(autouse=True)
def set_local_path(tmp_path):
    original = cfg.settings.local_path
    cfg.settings.local_path = str(tmp_path)
    yield tmp_path
    cfg.settings.local_path = original


def _make_event(**overrides):
    base = {
        "pipeline_id": "pipe-1",
        "operator_id": "op-1",
        "operator_type": "WINDOW",
        "capture_mode": "AGGREGATE",
        "processing_time": "2026-03-24T10:00:00+00:00",
        "trace_id": "trace-abc",
        "span_id": "span-abc",
        "input_cardinality": 10,
        "output_cardinality": 5,
        "emit_interval_ms": 10000,
        "capture_drop_since_last": False,
        "feature_name": "temperature",
    }
    base.update(overrides)
    return base


def test_ingest_flushes_to_parquet(set_local_path):
    original = cfg.settings.flush_buffer_size
    cfg.settings.flush_buffer_size = 1
    try:
        client = TestClient(app)
        client.post("/ingest", json=_make_event())
        parquet_files = list(set_local_path.rglob("*.parquet"))
        assert len(parquet_files) >= 1
    finally:
        cfg.settings.flush_buffer_size = original


def test_ingest_partitioned_by_pipeline_and_date(set_local_path):
    original = cfg.settings.flush_buffer_size
    cfg.settings.flush_buffer_size = 1
    try:
        client = TestClient(app)
        client.post("/ingest", json=_make_event(pipeline_id="pipe-X"))
        dirs = [p for p in set_local_path.iterdir() if p.is_dir()]
        assert any("pipeline_id=pipe-X" in str(d) for d in dirs)
    finally:
        cfg.settings.flush_buffer_size = original
