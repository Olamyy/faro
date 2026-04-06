"""Tests for _get_filesystem S3 endpoint_override construction."""
from unittest.mock import patch, MagicMock

import api.config as cfg
import api.store as store


def _call_get_filesystem(url: str):
    """Helper: patch settings and capture kwargs passed to S3FileSystem."""
    captured = {}

    def fake_s3(**kwargs):
        captured.update(kwargs)
        return MagicMock()

    original_backend = cfg.settings.storage_backend
    original_url = cfg.settings.s3_endpoint_url
    try:
        cfg.settings.storage_backend = "s3"
        cfg.settings.s3_endpoint_url = url
        with patch("api.store.pafs.S3FileSystem", side_effect=fake_s3):
            store._get_filesystem()
    finally:
        cfg.settings.storage_backend = original_backend
        cfg.settings.s3_endpoint_url = original_url
    return captured


def test_s3_endpoint_with_port():
    kwargs = _call_get_filesystem("https://minio.example.com:9000")
    assert kwargs["endpoint_override"] == "minio.example.com:9000"


def test_s3_endpoint_without_port():
    kwargs = _call_get_filesystem("https://s3.custom.host")
    # Must not contain the literal string "None"
    assert "None" not in kwargs["endpoint_override"]
    assert kwargs["endpoint_override"] == "s3.custom.host"


def test_s3_endpoint_scheme_preserved():
    kwargs = _call_get_filesystem("http://minio.local:9000")
    assert kwargs["scheme"] == "http"
