import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

import api.config as cfg
from api.main import app

_VIOLATION_SCHEMA = pa.schema([
    ("pipeline_id", pa.string()),
    ("feature_name", pa.string()),
    ("violation_type", pa.string()),
    ("detected_at", pa.string()),
    ("severity", pa.string()),
    ("detail", pa.string()),
])


@pytest.fixture()
def violation_store(tmp_path):
    cfg.settings.local_path = str(tmp_path)

    pipeline_id = "pipe-viol"
    viol_dir = tmp_path / "violations" / f"pipeline_id={pipeline_id}"
    viol_dir.mkdir(parents=True)

    table = pa.table({
        "pipeline_id": [pipeline_id, pipeline_id],
        "feature_name": ["temperature", "pressure"],
        "violation_type": ["FRESHNESS", "FRESHNESS"],
        "detected_at": ["2026-03-24T10:00:00+00:00", "2026-03-24T11:00:00+00:00"],
        "severity": ["HIGH", "MEDIUM"],
        "detail": ["No event in window", "No event in window"],
    }, schema=_VIOLATION_SCHEMA)
    pq.write_table(table, str(viol_dir / "part-0001.parquet"))

    yield pipeline_id

    cfg.settings.local_path = "/var/faro/parquet"


def test_violations_returns_all(violation_store):
    client = TestClient(app)
    resp = client.get(f"/violations?pipeline_id={violation_store}")
    assert resp.status_code == 200
    assert len(resp.json()["violations"]) == 2


def test_violations_filter_by_feature(violation_store):
    client = TestClient(app)
    resp = client.get(f"/violations?pipeline_id={violation_store}&feature_name=temperature")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["violations"]) == 1
    assert body["violations"][0]["feature_name"] == "temperature"


def test_violations_filter_by_severity(violation_store):
    client = TestClient(app)
    resp = client.get(f"/violations?pipeline_id={violation_store}&severity_gte=HIGH")
    assert resp.status_code == 200
    assert all(v["severity"] in ("HIGH", "CRITICAL") for v in resp.json()["violations"])


def test_violations_empty_when_no_data(tmp_path):
    cfg.settings.local_path = str(tmp_path)
    client = TestClient(app)
    resp = client.get("/violations?pipeline_id=no-such-pipeline")
    assert resp.status_code == 200
    assert resp.json()["violations"] == []
