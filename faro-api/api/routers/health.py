from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Query

from ..models import FeatureHealthResponse, PipelineHealthResponse, OperatorSummary, CardinalityPoint
from ..query import (
    query_feature_health,
    query_pipeline_health,
    check_freshness_violation,
)
from ..store import ParquetStore

router = APIRouter()


@router.get("/features/{feature_name}/health", response_model=FeatureHealthResponse)
def get_feature_health(
    feature_name: str,
    pipeline_id: Annotated[str, Query(description="Pipeline ID (required)")],
    window: Annotated[str, Query(description="Time window, e.g. 1h, 30m, 7d")] = "1h",
    compare_to: Annotated[str | None, Query(description="Comparison period, e.g. 24h_ago")] = None,
):
    result = query_feature_health(pipeline_id, feature_name, window, compare_to)

    freshness = check_freshness_violation(pipeline_id, feature_name, result["emit_interval_ms"])
    result["freshness_violation"] = freshness

    if freshness:
        ParquetStore.write_violation(
            pipeline_id=pipeline_id,
            feature_name=feature_name,
            violation_type="FRESHNESS",
            detected_at=datetime.now(tz=timezone.utc).isoformat(),
            severity="HIGH",
            detail=f"No event received for feature '{feature_name}' in expected window",
        )

    return FeatureHealthResponse(
        feature_name=result["feature_name"],
        pipeline_id=result["pipeline_id"],
        window=result["window"],
        cardinality_trend=[CardinalityPoint(**p) for p in result["cardinality_trend"]],
        watermark_lag_ms=result["watermark_lag_ms"],
        capture_drops=result["capture_drops"],
        freshness_violation=result["freshness_violation"],
        comparison=result["comparison"],
    )


@router.get("/pipelines/{pipeline_id}/health", response_model=PipelineHealthResponse)
def get_pipeline_health(pipeline_id: str):
    operators_data = query_pipeline_health(pipeline_id)
    operators = [OperatorSummary(**op) for op in operators_data]
    return PipelineHealthResponse(pipeline_id=pipeline_id, operators=operators)
