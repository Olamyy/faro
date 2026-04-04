import api.config as cfg
from api.query import _glob_for_pipeline, _violation_glob


def test_glob_for_pipeline_local():
    original = cfg.settings.local_path
    cfg.settings.local_path = "/var/faro/parquet"
    try:
        result = _glob_for_pipeline("my-pipeline")
        assert result == "/var/faro/parquet/pipeline_id=my-pipeline/date=*/part-*.parquet"
    finally:
        cfg.settings.local_path = original


def test_glob_for_pipeline_s3():
    original_backend = cfg.settings.storage_backend
    original_bucket = cfg.settings.s3_bucket
    original_prefix = cfg.settings.s3_prefix
    cfg.settings.storage_backend = "s3"
    cfg.settings.s3_bucket = "my-bucket"
    cfg.settings.s3_prefix = "faro/"
    try:
        result = _glob_for_pipeline("my-pipeline")
        assert result == "s3://my-bucket/faro/pipeline_id=my-pipeline/date=*/part-*.parquet"
    finally:
        cfg.settings.storage_backend = original_backend
        cfg.settings.s3_bucket = original_bucket
        cfg.settings.s3_prefix = original_prefix


def test_violation_glob_local():
    original = cfg.settings.local_path
    cfg.settings.local_path = "/var/faro/parquet"
    try:
        result = _violation_glob("my-pipeline")
        assert result == "/var/faro/parquet/violations/pipeline_id=my-pipeline/part-*.parquet"
    finally:
        cfg.settings.local_path = original


def test_violation_glob_s3():
    original_backend = cfg.settings.storage_backend
    original_bucket = cfg.settings.s3_bucket
    original_prefix = cfg.settings.s3_prefix
    cfg.settings.storage_backend = "s3"
    cfg.settings.s3_bucket = "my-bucket"
    cfg.settings.s3_prefix = "faro/"
    try:
        result = _violation_glob("my-pipeline")
        assert result == "s3://my-bucket/faro/violations/pipeline_id=my-pipeline/part-*.parquet"
    finally:
        cfg.settings.storage_backend = original_backend
        cfg.settings.s3_bucket = original_bucket
        cfg.settings.s3_prefix = original_prefix


def test_query_returns_empty_when_no_data_no_filesystem_walk(tmp_path):
    """_any_parquet_exists must no longer be called — queries return empty on IOException."""
    from fastapi.testclient import TestClient
    from api.main import app

    original = cfg.settings.local_path
    cfg.settings.local_path = str(tmp_path)
    try:
        client = TestClient(app)
        resp = client.get("/features/temperature/health?pipeline_id=nonexistent")
        assert resp.status_code == 200
        assert resp.json()["cardinality_trend"] == []
    finally:
        cfg.settings.local_path = original
