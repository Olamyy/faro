from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Query

from ..models import FeatureHealthResponse, PipelineHealthResponse, OperatorSummary, CardinalityPoint
from ..query import (
    check_cardinality_anomaly,
    check_freshness_violation,
    check_mean_drift,
    check_null_rate,
    query_feature_health,
    query_pipeline_health,
)
from ..store import ParquetStore

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
    result = query_feature_health(pipeline_id, feature_name, window, compare_to, operator_id, end_time)

    freshness = check_freshness_violation(pipeline_id, feature_name, result["emit_interval_ms"])
    result["freshness_violation"] = freshness

    now_iso = datetime.now(tz=timezone.utc).isoformat()

    if freshness:
        ParquetStore.write_violation(
            pipeline_id=pipeline_id,
            feature_name=feature_name,
            violation_type="FRESHNESS",
            detected_at=now_iso,
            severity="HIGH",
            detail=f"No event received for feature '{feature_name}' in expected window",
        )

    if check_mean_drift(pipeline_id, feature_name, window):
        ParquetStore.write_violation(
            pipeline_id=pipeline_id,
            feature_name=feature_name,
            violation_type="MEAN_DRIFT",
            detected_at=now_iso,
            severity="MEDIUM",
            detail=f"Feature '{feature_name}' mean drifted beyond threshold",
        )

    if check_null_rate(pipeline_id, feature_name, window):
        ParquetStore.write_violation(
            pipeline_id=pipeline_id,
            feature_name=feature_name,
            violation_type="NULL_RATE",
            detected_at=now_iso,
            severity="HIGH",
            detail=f"Feature '{feature_name}' null rate exceeds threshold",
        )

    if check_cardinality_anomaly(pipeline_id, feature_name, window):
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
