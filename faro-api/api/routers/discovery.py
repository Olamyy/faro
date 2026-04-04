from fastapi import APIRouter

from ..models import FeaturesResponse, PipelinesResponse
from ..query import query_list_features, query_list_pipelines

router = APIRouter()


@router.get("/pipelines", response_model=PipelinesResponse)
def list_pipelines():
    return PipelinesResponse(pipelines=query_list_pipelines())


@router.get("/pipelines/{pipeline_id}/features", response_model=FeaturesResponse)
def list_features(pipeline_id: str):
    return FeaturesResponse(pipeline_id=pipeline_id, features=query_list_features(pipeline_id))
