"""Customer lifecycle and operational segment HTTP contracts."""
from typing import Any
import uuid
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel,ConfigDict,Field
from .analytics import Analytics
from .contracts import MAX_MONEY
from .lifecycle import LifecycleService,LifecycleError
from .lifecycle_contracts import SegmentRule
from .segment_store import SegmentStore
from .store import AppStore


class PreviewInput(BaseModel):
    model_config=ConfigDict(extra='forbid')
    rule:dict[str,Any]


class SegmentInput(PreviewInput):
    name:str=Field(min_length=1,max_length=80)
    expected_version:int=Field(strict=True,ge=0,le=MAX_MONEY)


def register_lifecycle_routes(app:FastAPI,analytics:Analytics,store:AppStore) -> None:
    service=LifecycleService(analytics);segments=SegmentStore(store)

    @app.exception_handler(LifecycleError)
    async def lifecycle_error(request,error):
        code=error.code
        return JSONResponse({'code':code,'message':code.replace('_',' ')},status_code=404 if code in {'profile_not_found','segment_not_found'} else 409)

    @app.get('/api/v1/lifecycle/cohorts')
    def cohorts():return service.cohorts()

    @app.get('/api/v1/lifecycle/email-engagement')
    def email_engagement():return service.email_engagement()

    @app.get('/api/v1/lifecycle/customer-mix')
    def customer_mix():return service.customer_mix()

    @app.get('/api/v1/lifecycle/profiles')
    def profiles(cohort_month:str|None=None,activity_month:str|None=None,profile_type:str|None=None,purpose:str='analytics',opportunity:str|None=None,limit:int=20,offset:int=0):
        return service.profiles(cohort_month=cohort_month,activity_month=activity_month,profile_type=profile_type,purpose=purpose,opportunity=opportunity,limit=limit,offset=offset)

    @app.get('/api/v1/lifecycle/profiles/{profile_ref}/timeline')
    def timeline(profile_ref:str,limit:int=50,offset:int=0):return service.timeline(profile_ref,limit=limit,offset=offset)

    @app.get('/api/v1/lifecycle/segments')
    def list_segments():return segments.list()

    @app.post('/api/v1/lifecycle/segments/preview')
    def preview(body:PreviewInput):return service.preview(SegmentRule.from_payload(body.rule))

    @app.post('/api/v1/lifecycle/segments')
    def create_segment(body:SegmentInput):
        if body.expected_version!=0:raise ValueError('new segment requires expected_version0')
        return segments.save(str(uuid.uuid4()),body.name,SegmentRule.from_payload(body.rule),0)

    @app.put('/api/v1/lifecycle/segments/{segment_id}')
    def update_segment(segment_id:str,body:SegmentInput):
        if body.expected_version==0:raise ValueError('update requires positive expected version')
        return segments.save(segment_id,body.name,SegmentRule.from_payload(body.rule),body.expected_version)

    @app.get('/api/v1/lifecycle/segments/{segment_id}/versions/{version}')
    def revision(segment_id:str,version:int):
        result=segments.revision(segment_id,version)
        if result is None:raise LifecycleError('segment_not_found')
        return result
