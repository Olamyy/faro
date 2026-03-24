import os
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class Settings:
    storage_backend: Literal["local", "s3"] = field(
        default_factory=lambda: os.environ.get("FARO_STORAGE_BACKEND", "local")  # type: ignore[return-value]
    )
    local_path: str = field(
        default_factory=lambda: os.environ.get("FARO_LOCAL_PATH", "/var/faro/parquet")
    )
    s3_bucket: str | None = field(
        default_factory=lambda: os.environ.get("FARO_S3_BUCKET")
    )
    s3_prefix: str = field(
        default_factory=lambda: os.environ.get("FARO_S3_PREFIX", "faro/")
    )
    s3_endpoint_url: str | None = field(
        default_factory=lambda: os.environ.get("FARO_S3_ENDPOINT_URL")
    )
    flush_interval_seconds: int = field(
        default_factory=lambda: int(os.environ.get("FARO_FLUSH_INTERVAL_SECONDS", "60"))
    )
    flush_buffer_size: int = field(
        default_factory=lambda: int(os.environ.get("FARO_FLUSH_BUFFER_SIZE", "1000"))
    )


settings = Settings()
