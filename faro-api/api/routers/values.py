from typing import Annotated

from fastapi import APIRouter, Query

from ..models import EntityValuePoint, EntityValuesResponse, EntityValueSummary
from ..query import query_entity_values, query_entity_value_summary

router = APIRouter()


@router.get("/features/{feature_name}/values", response_model=EntityValuesResponse)
def get_entity_values(
    feature_name: str,
    pipeline_id: Annotated[str, Query(description="Pipeline ID (required)")],
    window: Annotated[str, Query(description="Time window, e.g. 1h, 30m, 7d")] = "1h",
    entity_id: Annotated[str | None, Query(description="Filter to a specific entity")] = None,
    limit: Annotated[int, Query(description="Maximum number of results", ge=1, le=10_000)] = 10,
):
    rows = query_entity_values(pipeline_id, feature_name, window, entity_id, limit)
    return EntityValuesResponse(
        feature_name=feature_name,
        pipeline_id=pipeline_id,
        window=window,
        values=[EntityValuePoint(**r) for r in rows],
    )


@router.get("/features/{feature_name}/values/summary", response_model=EntityValueSummary)
def get_entity_value_summary(
    feature_name: str,
    pipeline_id: Annotated[str, Query(description="Pipeline ID (required)")],
    window: Annotated[str, Query(description="Time window, e.g. 1h, 30m, 7d")] = "1h",
):
    result = query_entity_value_summary(pipeline_id, feature_name, window)
    if result is None:
        result = {
            "feature_name": feature_name,
            "pipeline_id": pipeline_id,
            "window": window,
            "entity_count": 0,
            "value_min": None,
            "value_max": None,
            "value_mean": None,
            "value_p50": None,
            "value_p95": None,
            "null_count": 0,
        }
    return EntityValueSummary(**result)
