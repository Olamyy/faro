import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from .config import settings
from .models import CaptureEvent

logger = logging.getLogger(__name__)

_SCHEMA = pa.schema([
    ("schema_version", pa.string()),
    ("pipeline_id", pa.string()),
    ("operator_id", pa.string()),
    ("operator_type", pa.string()),
    ("feature_name", pa.string()),
    ("capture_mode", pa.string()),
    ("event_time", pa.string()),
    ("event_time_min", pa.string()),
    ("processing_time", pa.string()),
    ("watermark", pa.string()),
    ("window_start", pa.string()),
    ("window_end", pa.string()),
    ("late_event_count", pa.int64()),
    ("late_tracking_mode", pa.string()),
    ("input_cardinality", pa.int64()),
    ("output_cardinality", pa.int64()),
    ("emit_interval_ms", pa.int64()),
    ("timer_fired_count", pa.int64()),
    ("async_pending_count", pa.int64()),
    ("pattern_match_count", pa.int64()),
    ("join_input_side", pa.string()),
    ("join_lower_bound_ms", pa.int64()),
    ("join_upper_bound_ms", pa.int64()),
    ("join_match_rate", pa.float64()),
    ("value_count", pa.int64()),
    ("value_min", pa.float64()),
    ("value_max", pa.float64()),
    ("value_mean", pa.float64()),
    ("value_p50", pa.float64()),
    ("value_p95", pa.float64()),
    ("null_count", pa.int64()),
    ("entity_id", pa.string()),
    ("feature_value", pa.binary()),
    ("feature_value_type", pa.string()),
    ("upstream_source", pa.string()),
    ("upstream_system", pa.string()),
    ("trace_id", pa.string()),
    ("span_id", pa.string()),
    ("parent_span_id", pa.string()),
    ("capture_drop_since_last", pa.bool_()),
])

_VIOLATION_SCHEMA = pa.schema([
    ("pipeline_id", pa.string()),
    ("feature_name", pa.string()),
    ("violation_type", pa.string()),
    ("detected_at", pa.string()),
    ("severity", pa.string()),
    ("detail", pa.string()),
])


class ParquetStore:

    @staticmethod
    def _base_path() -> str:
        if settings.storage_backend == "s3":
            return f"s3://{settings.s3_bucket}/{settings.s3_prefix}"
        return settings.local_path

    @staticmethod
    def _resolve_path(pipeline_id: str, date_str: str) -> str:
        base = ParquetStore._base_path()
        return f"{base}/pipeline_id={pipeline_id}/date={date_str}"

    @staticmethod
    def write_events(events: list[CaptureEvent]) -> None:
        if not events:
            return

        by_partition: dict[tuple[str, str], list[CaptureEvent]] = {}
        for event in events:
            try:
                dt = datetime.fromisoformat(event.processing_time)
            except (ValueError, TypeError):
                dt = datetime.now(tz=timezone.utc)
            date_str = dt.strftime("%Y-%m-%d")
            key = (event.pipeline_id, date_str)
            by_partition.setdefault(key, []).append(event)

        for (pipeline_id, date_str), partition_events in by_partition.items():
            ParquetStore._write_partition(pipeline_id, date_str, partition_events)

    @staticmethod
    def _write_partition(pipeline_id: str, date_str: str, events: list[CaptureEvent]) -> None:
        partition_path = ParquetStore._resolve_path(pipeline_id, date_str)
        file_name = f"part-{uuid.uuid4()}.parquet"

        if settings.storage_backend == "local":
            Path(partition_path).mkdir(parents=True, exist_ok=True)

        full_path = f"{partition_path}/{file_name}"
        table = _events_to_table(events)
        fs = _get_filesystem()
        if fs is not None:
            pq.write_table(table, full_path, filesystem=fs)
        else:
            pq.write_table(table, full_path)

    @staticmethod
    def write_violation(pipeline_id: str, feature_name: str | None,
                        violation_type: str, detected_at: str,
                        severity: str, detail: str) -> None:
        base = ParquetStore._base_path()
        violation_dir = f"{base}/violations/pipeline_id={pipeline_id}"

        if settings.storage_backend == "local":
            Path(violation_dir).mkdir(parents=True, exist_ok=True)

        file_name = f"part-{uuid.uuid4()}.parquet"
        full_path = f"{violation_dir}/{file_name}"

        table = pa.table({
            "pipeline_id": [pipeline_id],
            "feature_name": [feature_name],
            "violation_type": [violation_type],
            "detected_at": [detected_at],
            "severity": [severity],
            "detail": [detail],
        }, schema=_VIOLATION_SCHEMA)

        fs = _get_filesystem()
        if fs is not None:
            pq.write_table(table, full_path, filesystem=fs)
        else:
            pq.write_table(table, full_path)


def _events_to_table(events: list[CaptureEvent]) -> pa.Table:
    rows: dict[str, list] = {field.name: [] for field in _SCHEMA}

    for e in events:
        rows["schema_version"].append(e.schema_version)
        rows["pipeline_id"].append(e.pipeline_id)
        rows["operator_id"].append(e.operator_id)
        rows["operator_type"].append(e.operator_type)
        rows["feature_name"].append(e.feature_name)
        rows["capture_mode"].append(e.capture_mode)
        rows["event_time"].append(e.event_time)
        rows["event_time_min"].append(e.event_time_min)
        rows["processing_time"].append(e.processing_time)
        rows["watermark"].append(e.watermark)
        rows["window_start"].append(e.window_start)
        rows["window_end"].append(e.window_end)
        rows["late_event_count"].append(e.late_event_count)
        rows["late_tracking_mode"].append(e.late_tracking_mode)
        rows["input_cardinality"].append(e.input_cardinality)
        rows["output_cardinality"].append(e.output_cardinality)
        rows["emit_interval_ms"].append(e.emit_interval_ms)
        rows["timer_fired_count"].append(e.timer_fired_count)
        rows["async_pending_count"].append(e.async_pending_count)
        rows["pattern_match_count"].append(e.pattern_match_count)
        rows["join_input_side"].append(e.join_input_side)
        rows["join_lower_bound_ms"].append(e.join_lower_bound_ms)
        rows["join_upper_bound_ms"].append(e.join_upper_bound_ms)
        rows["join_match_rate"].append(e.join_match_rate)
        rows["value_count"].append(e.value_count)
        rows["value_min"].append(e.value_min)
        rows["value_max"].append(e.value_max)
        rows["value_mean"].append(e.value_mean)
        rows["value_p50"].append(e.value_p50)
        rows["value_p95"].append(e.value_p95)
        rows["null_count"].append(e.null_count)
        rows["entity_id"].append(e.entity_id)
        rows["feature_value"].append(e.feature_value)
        rows["feature_value_type"].append(e.feature_value_type)
        rows["upstream_source"].append(e.upstream_source)
        rows["upstream_system"].append(e.upstream_system)
        rows["trace_id"].append(e.trace_id)
        rows["span_id"].append(e.span_id)
        rows["parent_span_id"].append(e.parent_span_id)
        rows["capture_drop_since_last"].append(e.capture_drop_since_last)

    return pa.table(rows, schema=_SCHEMA)


def _get_filesystem():
    if settings.storage_backend != "s3":
        return None
    try:
        kwargs: dict = {}
        if settings.s3_endpoint_url:
            parsed = urlparse(settings.s3_endpoint_url)
            kwargs["endpoint_override"] = f"{parsed.hostname}:{parsed.port}"
            kwargs["scheme"] = parsed.scheme
        return pafs.S3FileSystem(**kwargs)
    except Exception:
        logger.exception("Failed to initialise S3FileSystem; writes will fail")
        return None

