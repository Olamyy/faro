from typing import Annotated

from fastapi import APIRouter, Query

from ..models import Violation, ViolationsResponse
from ..query import query_violations

router = APIRouter()


@router.get("/violations", response_model=ViolationsResponse)
def get_violations(
    pipeline_id: Annotated[str | None, Query()] = None,
    feature_name: Annotated[str | None, Query()] = None,
    since: Annotated[str | None, Query(description="ISO-8601 timestamp lower bound")] = None,
    severity_gte: Annotated[str | None, Query(description="Minimum severity: LOW, MEDIUM, HIGH, CRITICAL")] = None,
    violation_type: Annotated[str | None, Query(description="Filter by violation type, e.g. FRESHNESS, MEAN_DRIFT")] = None,
):
    rows = query_violations(pipeline_id, feature_name, since, severity_gte, violation_type)
    return ViolationsResponse(violations=[Violation(**r) for r in rows])
