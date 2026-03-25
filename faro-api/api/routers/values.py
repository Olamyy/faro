from typing import Annotated

from fastapi import APIRouter, Query

from ..models import EntityValuePoint, EntityValuesResponse
from ..query import query_entity_values

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
