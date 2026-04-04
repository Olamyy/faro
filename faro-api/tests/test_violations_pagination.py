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
def many_violations(tmp_path):
    original = cfg.settings.local_path
    cfg.settings.local_path = str(tmp_path)

    pipeline_id = "pipe-page"
    viol_dir = tmp_path / "violations" / f"pipeline_id={pipeline_id}"
    viol_dir.mkdir(parents=True)

    n = 25
    table = pa.table({
        "pipeline_id": [pipeline_id] * n,
        "feature_name": ["f"] * n,
        "violation_type": ["FRESHNESS"] * n,
        "detected_at": [f"2026-01-{i+1:02d}T00:00:00Z" for i in range(n)],
        "severity": ["HIGH"] * n,
        "detail": ["d"] * n,
    }, schema=_VIOLATION_SCHEMA)
    pq.write_table(table, str(viol_dir / "part-001.parquet"))

    yield pipeline_id

    cfg.settings.local_path = original


def test_violations_pagination_limit(many_violations):
    client = TestClient(app)
    resp = client.get(f"/violations?pipeline_id={many_violations}&limit=10")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["violations"]) == 10
    assert body["total"] == 25


def test_violations_pagination_offset(many_violations):
    client = TestClient(app)
    first = client.get(f"/violations?pipeline_id={many_violations}&limit=10&offset=0").json()
    second = client.get(f"/violations?pipeline_id={many_violations}&limit=10&offset=10").json()
    first_ids = {v["detected_at"] for v in first["violations"]}
    second_ids = {v["detected_at"] for v in second["violations"]}
    assert first_ids.isdisjoint(second_ids)


