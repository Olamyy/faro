from fastapi import APIRouter

from ..models import EntityFeaturesResponse, EntityFeaturePoint, TraceEvent, TraceResponse
from ..query import query_entity_feature_points, query_trace_events

router = APIRouter()


@router.get("/traces/{trace_id}", response_model=TraceResponse)
def get_trace(trace_id: str):
    events = query_trace_events(trace_id)
    return TraceResponse(trace_id=trace_id, events=[TraceEvent(**e) for e in events])


@router.get("/entities/{entity_id}/features", response_model=EntityFeaturesResponse)
def get_entity_features(entity_id: str):
    points = query_entity_feature_points(entity_id)
    return EntityFeaturesResponse(entity_id=entity_id, features=[EntityFeaturePoint(**p) for p in points])
