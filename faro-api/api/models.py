import base64
from typing import Any
from pydantic import BaseModel, field_validator


class CaptureEvent(BaseModel):
    schema_version: str | None = None
    pipeline_id: str
    operator_id: str
    operator_type: str
    feature_name: str | None = None
    capture_mode: str
    event_time: str | None = None
    event_time_min: str | None = None
    processing_time: str
    watermark: str | None = None
    window_start: str | None = None
    window_end: str | None = None
    late_event_count: int | None = None
    late_tracking_mode: str | None = None
    input_cardinality: int = 0
    output_cardinality: int = 0
    emit_interval_ms: int = 0
    timer_fired_count: int | None = None
    async_pending_count: int | None = None
    pattern_match_count: int | None = None
    join_input_side: str | None = None
    join_lower_bound_ms: int | None = None
    join_upper_bound_ms: int | None = None
    join_match_rate: float | None = None
    value_count: int | None = None
    value_min: float | None = None
    value_max: float | None = None
    value_mean: float | None = None
    value_p50: float | None = None
    value_p95: float | None = None
    null_count: int | None = None
    entity_id: str | None = None
    feature_value: bytes | None = None
    feature_value_type: str | None = None

    @field_validator("feature_value", mode="before")
    @classmethod
    def _decode_feature_value(cls, v: object) -> bytes | None:
        if v is None:
            return None
        if isinstance(v, bytes):
            return v
        if isinstance(v, str):
            return base64.b64decode(v)
        return v
    upstream_source: str | None = None
    upstream_system: str | None = None
    trace_id: str
    span_id: str
    parent_span_id: str | None = None
    capture_drop_since_last: bool = False


class CardinalityPoint(BaseModel):
    processing_time: str
    input_cardinality: int
    output_cardinality: int
    watermark: str | None
    capture_drop_since_last: bool


class FeatureHealthResponse(BaseModel):
    feature_name: str
    pipeline_id: str
    window: str
    cardinality_trend: list[CardinalityPoint]
    watermark_lag_ms: int | None
    capture_drops: bool
    freshness_violation: bool
    comparison: dict[str, Any] | None = None


class OperatorSummary(BaseModel):
    operator_id: str
    operator_type: str
    last_seen: str | None
    total_input: int
    any_drops: bool


class PipelineHealthResponse(BaseModel):
    pipeline_id: str
    operators: list[OperatorSummary]


class Violation(BaseModel):
    pipeline_id: str
    feature_name: str | None
    violation_type: str
    detected_at: str
    severity: str
    detail: str


class ViolationsResponse(BaseModel):
    violations: list[Violation]


class EntityValuePoint(BaseModel):
    entity_id: str | None
    feature_value_decoded: float | int | str | None
    feature_value_type: str | None
    processing_time: str
    event_time: str | None


class EntityValuesResponse(BaseModel):
    feature_name: str
    pipeline_id: str
    window: str
    values: list[EntityValuePoint]
