from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import api.config as cfg
from api.main import app


@pytest.fixture()
def seeded_store(tmp_path):
    cfg.settings.local_path = str(tmp_path)

    pipeline_id = "sensor-pipeline-test"
    date_str = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
    part_dir = tmp_path / f"pipeline_id={pipeline_id}" / f"date={date_str}"
    part_dir.mkdir(parents=True)

    table = pa.table({
        "schema_version": ["1.0.0"],
        "pipeline_id": [pipeline_id],
        "operator_id": ["window.temperature-sum"],
        "operator_type": ["WINDOW"],
        "feature_name": ["temperature"],
        "capture_mode": ["AGGREGATE"],
        "event_time": [None],
        "event_time_min": [None],
        "processing_time": [datetime.now(tz=timezone.utc).isoformat()],
        "watermark": [datetime.now(tz=timezone.utc).isoformat()],
        "window_start": [None],
        "window_end": [None],
        "late_event_count": pa.array([None], type=pa.int64()),
        "late_tracking_mode": [None],
        "input_cardinality": pa.array([100], type=pa.int64()),
        "output_cardinality": pa.array([50], type=pa.int64()),
        "emit_interval_ms": pa.array([10000], type=pa.int64()),
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
        "feature_value_type": [None],
        "upstream_source": [None],
        "upstream_system": [None],
        "trace_id": ["trace-1"],
        "span_id": ["span-1"],
        "parent_span_id": [None],
        "capture_drop_since_last": [False],
    })
    pq.write_table(table, str(part_dir / "part-0001.parquet"))

    yield pipeline_id

    cfg.settings.local_path = "/var/faro/parquet"


def test_feature_health_returns_data(seeded_store):
    client = TestClient(app)
    resp = client.get(f"/features/temperature/health?pipeline_id={seeded_store}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["feature_name"] == "temperature"
    assert body["pipeline_id"] == seeded_store
    assert len(body["cardinality_trend"]) >= 1


def test_feature_health_empty_when_no_data(tmp_path):
    cfg.settings.local_path = str(tmp_path)
    client = TestClient(app)
    resp = client.get("/features/temperature/health?pipeline_id=does-not-exist")
    assert resp.status_code == 200
    assert resp.json()["cardinality_trend"] == []
