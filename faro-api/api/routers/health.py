import re
from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from ..models import FeatureHealthResponse, PipelineHealthResponse, OperatorSummary, CardinalityPoint
from ..query import (
    check_freshness_violation,
    query_feature_health,
    query_pipeline_health,
    query_violation_signals,
)
from ..store import ParquetStore

_WINDOW_RE = re.compile(r"^\d+[hmd]$")

router = APIRouter()


@router.get("/features/{feature_name}/health", response_model=FeatureHealthResponse)
def get_feature_health(
    feature_name: str,
    pipeline_id: Annotated[str, Query(description="Pipeline ID (required)")],
    window: Annotated[str, Query(description="Time window, e.g. 1h, 30m, 7d")] = "1h",
    compare_to: Annotated[str | None, Query(description="Comparison period, e.g. 24h_ago")] = None,
    operator_id: Annotated[str | None, Query(description="Scope to a single operator")] = None,
    end_time: Annotated[str | None, Query(description="ISO-8601 upper bound for processing_time")] = None,
):
    if compare_to is not None:
        base = compare_to.removesuffix("_ago")
        if not _WINDOW_RE.match(base):
            raise HTTPException(status_code=422, detail=f"Invalid compare_to value: '{compare_to}'")
    result = query_feature_health(pipeline_id, feature_name, window, compare_to, operator_id, end_time)

    freshness = check_freshness_violation(pipeline_id, feature_name, result["emit_interval_ms"])
    result["freshness_violation"] = freshness
    signals = query_violation_signals(pipeline_id, feature_name, window)

    now_iso = datetime.now(tz=timezone.utc).isoformat()

    if freshness and not ParquetStore.has_recent_violation(pipeline_id, "FRESHNESS", feature_name):
        ParquetStore.write_violation(
            pipeline_id=pipeline_id,
            feature_name=feature_name,
            violation_type="FRESHNESS",
            detected_at=now_iso,
            severity="HIGH",
            detail=f"No event received for feature '{feature_name}' in expected window",
        )

    if signals["mean_drift"] and not ParquetStore.has_recent_violation(pipeline_id, "MEAN_DRIFT", feature_name):
        ParquetStore.write_violation(
            pipeline_id=pipeline_id,
            feature_name=feature_name,
            violation_type="MEAN_DRIFT",
            detected_at=now_iso,
            severity="MEDIUM",
            detail=f"Feature '{feature_name}' mean drifted beyond threshold",
        )

    if signals["null_rate"] and not ParquetStore.has_recent_violation(pipeline_id, "NULL_RATE", feature_name):
        ParquetStore.write_violation(
            pipeline_id=pipeline_id,
            feature_name=feature_name,
            violation_type="NULL_RATE",
            detected_at=now_iso,
            severity="HIGH",
            detail=f"Feature '{feature_name}' null rate exceeds threshold",
        )

    if signals["cardinality_anomaly"] and not ParquetStore.has_recent_violation(pipeline_id, "CARDINALITY_ANOMALY", feature_name):
        ParquetStore.write_violation(
            pipeline_id=pipeline_id,
            feature_name=feature_name,
            violation_type="CARDINALITY_ANOMALY",
            detected_at=now_iso,
            severity="HIGH",
            detail=f"Feature '{feature_name}' output/input ratio dropped beyond threshold",
        )

    return FeatureHealthResponse(
        feature_name=result["feature_name"],
        pipeline_id=result["pipeline_id"],
        window=result["window"],
        cardinality_trend=[CardinalityPoint(**p) for p in result["cardinality_trend"]],
        watermark_lag_ms=result["watermark_lag_ms"],
        capture_drops=result["capture_drops"],
        emit_interval_ms=result["emit_interval_ms"],
        freshness_violation=result["freshness_violation"],
        comparison=result["comparison"],
    )


@router.get("/pipelines/{pipeline_id}/health", response_model=PipelineHealthResponse)
def get_pipeline_health(
    pipeline_id: str,
    window: Annotated[str, Query(description="Time window, e.g. 1h, 30m, 7d")] = "24h",
    operator_id: Annotated[str | None, Query(description="Scope to a single operator")] = None,
):
    operators_data = query_pipeline_health(pipeline_id, window, operator_id)
    operators = [OperatorSummary(**op) for op in operators_data]
    return PipelineHealthResponse(pipeline_id=pipeline_id, operators=operators)
