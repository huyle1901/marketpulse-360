"""Read-only MP07 endpoints; errors stay separate from MP06 write contracts."""
from datetime import date,timedelta
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from .analysis import AnalysisError,AnalysisService
from .contracts import DashboardFilter
from .journeys import JourneyService,validate_sampling


def register_analysis_routes(app:FastAPI,analytics:Any,store:Any) -> None:
    @app.exception_handler(AnalysisError)
    async def analysis_error(request,error):
        return JSONResponse({'code':error.code,'message':error.code.replace('_',' ')},status_code=409)

    def selection(session,from_date,to_date,platform,campaign_id,objective,market,currency):
        if from_date is None or to_date is None:
            rows=session.all_daily()
            end=max((row['event_date'] for row in rows),default=date.today())+timedelta(days=1)
            start=max(min((row['event_date'] for row in rows),default=end-timedelta(days=1)),end-timedelta(days=366))
            from_date,to_date=from_date or start,to_date or end
        return DashboardFilter(from_date,to_date,platform,campaign_id,objective,market,currency)

    @app.get('/api/v1/analysis/funnel')
    def funnel(from_date:date|None=None,to_date:date|None=None,platform:str|None=None,campaign_id:str|None=None,
               objective:str|None=None,market:str|None=None,currency:str|None=None):
        session=analytics.pin()
        filters=selection(session,from_date,to_date,platform,campaign_id,objective,market,currency)
        result=AnalysisService(session).funnel(filters,store.settings())
        return result|{'filters':{'from_date':filters.from_date.isoformat(),'to_date':filters.to_date.isoformat()},'campaign_options':session.campaigns()}

    @app.get('/api/v1/analysis/attribution')
    def attribution(from_date:date|None=None,to_date:date|None=None,platform:str|None=None,campaign_id:str|None=None,
                    objective:str|None=None,market:str|None=None,currency:str|None=None):
        session=analytics.pin()
        return AnalysisService(session).attribution(selection(session,from_date,to_date,platform,campaign_id,objective,market,currency),store.settings())

    @app.get('/api/v1/analysis/journeys')
    def journeys(from_date:date|None=None,to_date:date|None=None,platform:str|None=None,campaign_id:str|None=None,
                 objective:str|None=None,market:str|None=None,currency:str|None=None,model:str='last_touch',limit:int=5):
        validate_sampling(model,limit)
        session=analytics.pin()
        return JourneyService(session).sample(selection(session,from_date,to_date,platform,campaign_id,objective,market,currency),store.settings(),model,limit)
