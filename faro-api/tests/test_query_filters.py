import struct
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import api.config as cfg
from api.main import app
from api.models import CaptureEvent
from api.store import ParquetStore

_VIOLATION_SCHEMA = pa.schema([
    ("pipeline_id", pa.string()),
    ("feature_name", pa.string()),
    ("violation_type", pa.string()),
    ("detected_at", pa.string()),
    ("severity", pa.string()),
    ("detail", pa.string()),
])


@pytest.fixture(autouse=True)
def set_local_path(tmp_path):
    original = cfg.settings.local_path
    cfg.settings.local_path = str(tmp_path)
    yield tmp_path
    cfg.settings.local_path = original


def _now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _agg(feature="temp", operator_id="op-1", input_c=100, output_c=60):
    return CaptureEvent(
        pipeline_id="pipe-1",
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


def _seed_violations(tmp_path, rows: list[dict]):
    pipeline_id = "pipe-viol"
    viol_dir = tmp_path / "violations" / f"pipeline_id={pipeline_id}"
    viol_dir.mkdir(parents=True)
    table = pa.table({
        "pipeline_id": [r["pipeline_id"] for r in rows],
        "feature_name": [r["feature_name"] for r in rows],
        "violation_type": [r["violation_type"] for r in rows],
        "detected_at": [r["detected_at"] for r in rows],
        "severity": [r["severity"] for r in rows],
        "detail": [r["detail"] for r in rows],
    }, schema=_VIOLATION_SCHEMA)
    pq.write_table(table, str(viol_dir / "part-001.parquet"))
    return pipeline_id


# --- emit_interval_ms on feature health ---

def test_feature_health_exposes_emit_interval_ms():
    ParquetStore.write_events([_agg()])
    client = TestClient(app)
    resp = client.get("/features/temp/health?pipeline_id=pipe-1")
    assert resp.status_code == 200
    assert resp.json()["emit_interval_ms"] == 5000


# --- end_time upper bound ---

def test_feature_health_end_time_excludes_newer_events():
    past_time = (datetime.now(tz=timezone.utc) - timedelta(hours=2)).isoformat()
    old_event = CaptureEvent(
        pipeline_id="pipe-1", operator_id="op-1", operator_type="FILTER",
        capture_mode="AGGREGATE", processing_time=past_time,
        trace_id="t", span_id="s",
        input_cardinality=50, output_cardinality=25,
        emit_interval_ms=1000, capture_drop_since_last=False, feature_name="temp",
    )
    ParquetStore.write_events([old_event, _agg(input_c=999)])

    cutoff = (datetime.now(tz=timezone.utc) - timedelta(hours=1)).isoformat()
    client = TestClient(app)
    resp = client.get(f"/features/temp/health?pipeline_id=pipe-1&window=3h&end_time={cutoff}")
    assert resp.status_code == 200
    trend = resp.json()["cardinality_trend"]
    assert len(trend) == 1
    assert trend[0]["input_cardinality"] == 50


# --- violations: severity_gte in SQL ---

def test_violations_severity_gte_filters_in_sql(tmp_path):
    pipeline_id = _seed_violations(tmp_path, [
        {"pipeline_id": "pipe-viol", "feature_name": "f", "violation_type": "FRESHNESS",
         "detected_at": "2026-01-01T00:00:00Z", "severity": "LOW", "detail": "d"},
        {"pipeline_id": "pipe-viol", "feature_name": "f", "violation_type": "FRESHNESS",
         "detected_at": "2026-01-01T01:00:00Z", "severity": "HIGH", "detail": "d"},
        {"pipeline_id": "pipe-viol", "feature_name": "f", "violation_type": "FRESHNESS",
         "detected_at": "2026-01-01T02:00:00Z", "severity": "CRITICAL", "detail": "d"},
    ])
    client = TestClient(app)
    resp = client.get(f"/violations?pipeline_id={pipeline_id}&severity_gte=HIGH")
    assert resp.status_code == 200
    violations = resp.json()["violations"]
    assert len(violations) == 2
    assert all(v["severity"] in ("HIGH", "CRITICAL") for v in violations)


# --- violations: violation_type filter ---

def test_violations_filter_by_violation_type(tmp_path):
    pipeline_id = _seed_violations(tmp_path, [
        {"pipeline_id": "pipe-viol", "feature_name": "f", "violation_type": "FRESHNESS",
         "detected_at": "2026-01-01T00:00:00Z", "severity": "HIGH", "detail": "d"},
        {"pipeline_id": "pipe-viol", "feature_name": "f", "violation_type": "MEAN_DRIFT",
         "detected_at": "2026-01-01T01:00:00Z", "severity": "MEDIUM", "detail": "d"},
    ])
    client = TestClient(app)
    resp = client.get(f"/violations?pipeline_id={pipeline_id}&violation_type=MEAN_DRIFT")
    assert resp.status_code == 200
    violations = resp.json()["violations"]
    assert len(violations) == 1
    assert violations[0]["violation_type"] == "MEAN_DRIFT"
