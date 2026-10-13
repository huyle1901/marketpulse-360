"""Bounded representative explanations from a release-bound governed sidecar."""
from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import timedelta
from typing import Any

from marketpulse_lakehouse.gold import build_attribution_daily
from .analysis import AnalysisError, DEFINITIONS, MODELS, campaign_dimension, provenance, single_currency
from .analytics import Analytics, wire
from .contracts import CampaignSetting, DashboardFilter
from .journey_projection import JourneyJournal, reference


def validate_sampling(model: str, limit: int) -> None:
    if model not in MODELS or type(limit) is not int or not 1<=limit<=10:
        raise ValueError('invalid representative model/limit')


def explain_order(financial:Sequence[Mapping[str,object]],touches:Sequence[Mapping[str,object]],
                  identity:str,filters:DashboardFilter) -> dict[str,object]:
    if len(touches)>500 or len(financial)>100:
        return {'status':'context_too_large','allocations':[]}
    purchases=[row for row in financial if row['event_name']=='order_created']
    if len(purchases)!=1:raise AnalysisError('projection_mismatch')
    purchase=purchases[0]
    def event(row):
        return dict(row)|{'profile_key':row['profile_ref']}
    # Existing MP04 policy, including original-purchase window and refund recognition date.
    allocations=[row for row in build_attribution_daily([event(row) for row in [*touches,*financial]])
                 if filters.from_date<=row['event_date']<filters.to_date]
    selected_financial=[row for row in financial if filters.from_date<=row['event_date']<filters.to_date]
    return wire({'status':'available','journey_id':reference(identity,purchase['order_id']),
        'purchase_time':purchase['event_time'],'context_from':purchase['event_time']-timedelta(days=30),
        'context_to':purchase['event_time'],'allocations':allocations,
        'touches':[{'touch_ref':reference(identity,row['click_id']),'event_time':row['event_time'],
                   'campaign_id':row['campaign_id'],'outside_report_interval':not filters.from_date<=row['event_date']<filters.to_date}
                  for row in touches],
        'financial_events':[{'event_ref':reference(identity,row['event_id']),'event_name':row['event_name'],'event_time':row['event_time'],
            'gross_revenue_minor':row['gross_amount_minor'] or 0 if row['event_name']=='order_created' else 0,
            'refund_amount_minor':row['refund_amount_minor'] or 0 if row['event_name']=='order_refunded' else 0} for row in selected_financial]})


def credited_campaign_matches(campaign_id:str,filters:DashboardFilter,dimensions:Mapping[str,Mapping[str,object]],settings:Sequence[CampaignSetting]) -> bool:
    dimension=dimensions.get(campaign_id)
    if filters.campaign_id and campaign_id!=filters.campaign_id:return False
    if any(getattr(filters,name) and (not dimension or dimension[name]!=getattr(filters,name)) for name in ('platform','objective')):return False
    if filters.market and not any(row.campaign_id==campaign_id and row.market==filters.market for row in settings):return False
    return True


class JourneyService:
    def __init__(self,analytics:Analytics):self.analytics=analytics

    def sample(self,filters:DashboardFilter,settings:Sequence[CampaignSetting],model:str='last_touch',limit:int=5) -> dict[str,object]:
        validate_sampling(model,limit)
        session=self.analytics.pin()
        currency=single_currency(session)
        try:publication=JourneyJournal(session.client).resolve(session.release)
        except ValueError as error:raise AnalysisError('projection_mismatch') from error
        common={'model':model,'definitions':DEFINITIONS,'representative':True,'population_claim':False,'currency':currency,
                'provenance':provenance(session,'attribution_daily',[]),'rows':[]}
        if publication is None:return common|{'status':'unavailable','message':'Publish pinned journey detail for this certified release first.'}
        common['provenance'].update(silver_snapshot_id=str(publication.silver_snapshot_id),projection_version=publication.version,
            projection_checksum=publication.checksum,projection_row_count=str(publication.row_count))
        if filters.currency and filters.currency!=currency:return common|{'status':'empty','candidates_scanned':'0','context_unavailable':'0'}
        candidates=session._query(f'SELECT order_id,min(event_time) AS recognized_at FROM {publication.database}.events '
            "WHERE event_name IN ('order_created','order_refunded') AND event_date>={from_date:Date32} AND event_date<{to_date:Date32} "
            'AND currency={currency:String} GROUP BY order_id ORDER BY recognized_at,order_id LIMIT 101',
            {'from_date':filters.from_date,'to_date':filters.to_date,'currency':currency})
        dimensions={row['campaign_id']:row for row in session.campaigns()}
        results=[];scanned=unavailable=0;dates=[]
        for candidate in candidates[:100]:
            scanned+=1
            financial=session._query(f'SELECT * FROM {publication.database}.events WHERE order_id={{order_id:String}} '
                "AND event_name IN ('order_created','order_refunded') ORDER BY event_time,event_id LIMIT 101",{'order_id':candidate['order_id']})
            purchases=[row for row in financial if row['event_name']=='order_created']
            if len(financial)>100:unavailable+=1;continue
            if len(purchases)!=1:raise AnalysisError('projection_mismatch')
            purchase=purchases[0]
            touches=session._query(f'SELECT * FROM {publication.database}.paid_touches WHERE profile_ref={{profile_ref:String}} '
                'AND event_time>={window_start:DateTime64(6)} AND event_time<={order_time:DateTime64(6)} '
                'ORDER BY event_time,campaign_id,click_id,event_id LIMIT 501',{'profile_ref':purchase['profile_ref'],
                    'window_start':purchase['event_time']-timedelta(days=30),'order_time':purchase['event_time']})
            result=explain_order(financial,touches,session.release.identity,filters)
            if result['status']!='available':unavailable+=1;continue
            if not any(row['model']==model and credited_campaign_matches(row['campaign_id'],filters,dimensions,settings) for row in result['allocations']):continue
            for touch in result['touches']:
                touch.update(campaign_dimension(touch['campaign_id'],dimensions))
            for allocation in result['allocations']:
                allocation.update(campaign_dimension(allocation['campaign_id'],dimensions))
            results.append(result)
            dates.extend(row for row in financial if filters.from_date<=row['event_date']<filters.to_date)
            if len(results)==limit:break
        common['provenance']=provenance(session,'attribution_daily',dates)|{key:value for key,value in common['provenance'].items() if key.startswith(('silver_','projection_'))}
        return common|{'status':'available' if results else 'empty','rows':results,'candidates_scanned':str(scanned),
            'context_unavailable':str(unavailable),'candidate_scan_capped':len(candidates)>100,
            'sample_limit':str(limit),'context_limit':'500 canonical touches / 100 financial events'}
