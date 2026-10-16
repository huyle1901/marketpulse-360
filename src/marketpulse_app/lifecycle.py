"""Pinned lifecycle reads over certified Gold/CRM and the existing event sidecar."""
from __future__ import annotations
from collections.abc import Mapping
from datetime import datetime,time,timedelta,timezone
import hashlib
import json
import re
from typing import Any
from .analytics import Analytics,wire
from .analysis import single_currency
from .journey_projection import JourneyJournal,reference
from .lifecycle_contracts import SegmentRule,month,validate_selection
from .lifecycle_math import activation_eligible,identity_evidence,lifecycle_metrics

POLICY='Identified CRM customer with historical analytics and latest certified analytics + marketing consent true.'
GOLD_FIELDS=('profile_type','first_activity_at','last_activity_at','event_count','session_count','order_count','refund_count',
             'gross_revenue_minor','refund_amount_minor','net_revenue_minor','cohort_month','consent_analytics')


class LifecycleError(ValueError):
    def __init__(self,code:str):
        self.code=code
        super().__init__(code)


def _cte(session:Analytics,publication:Any|None) -> str:
    database=session.release.database
    source_fields=','.join(f'p.{name} AS {name}' for name in ('profile_key','customer_id','anonymous_id',*GOLD_FIELDS))
    hashed="lower(hex(SHA256(concat({identity:String},'|',p.profile_key))))"
    crm=f"(SELECT customer_id,consent_analytics,consent_marketing,toUInt8(1) AS matched FROM {database}.customers) c"
    if publication:
        detail=f'''LEFT JOIN (SELECT profile_ref,count() AS detail_event_count,uniqExact(session_id) AS detail_session_count,
            countIf(event_name='order_created') AS detail_order_count,
            sumIf(toInt128(gross_amount_minor),event_name='order_created') AS detail_gross,
            sumIf(toInt128(refund_amount_minor),event_name='order_refunded') AS detail_refund,
            if(countIf(event_name='order_created')=0,CAST(NULL AS Nullable(DateTime64(6,'UTC'))),toNullable(maxIf(event_time,event_name='order_created'))) AS last_purchase_at,
            countIf(event_name='add_to_cart') AS add_to_cart_count,countIf(event_name='email_clicked') AS email_click_count
            FROM {publication.database}.events GROUP BY profile_ref) d ON d.profile_ref={hashed}'''
        metrics="d.detail_event_count,d.detail_session_count,d.detail_order_count,d.detail_gross,d.detail_refund,d.last_purchase_at,d.add_to_cart_count,d.email_click_count"
    else:
        detail=''
        metrics="CAST(NULL AS Nullable(DateTime64(6,'UTC'))) AS last_purchase_at,CAST(NULL AS Nullable(UInt64)) AS add_to_cart_count,CAST(NULL AS Nullable(UInt64)) AS email_click_count"
    return f'''WITH base AS (SELECT {source_fields},{hashed} AS profile_ref,
        CAST(c.matched=1 AS Bool) AS crm_matched,
        if(c.matched=1,toNullable(c.consent_analytics),NULL) AS crm_consent_analytics,
        if(c.matched=1,toNullable(c.consent_marketing),NULL) AS crm_consent_marketing,
        CAST(p.profile_type='customer' AND c.matched=1 AND p.consent_analytics AND c.consent_analytics AND c.consent_marketing AS Bool) AS activation_eligible,
        {metrics} FROM {database}.customer_360 p LEFT JOIN {crm} ON p.customer_id=c.customer_id {detail}),
        profiles AS (SELECT *,dateDiff('day',toDate(last_purchase_at),toDate({{as_of:DateTime64(6)}})) AS recency_days FROM base)'''


def _where(rule:SegmentRule,publication:Any,opportunity:str|None=None) -> tuple[str,dict[str,object]]:
    clauses=['1'];bound={}
    if rule.purpose=='activation':clauses.append('activation_eligible=1')
    for name in ('cohort_month','profile_type'):
        value=getattr(rule,name)
        if value is not None:
            bound[name]=month(value) if name=='cohort_month' else value
            clauses.append(f'{name}={{{name}:Date32}}' if name=='cohort_month' else f'{name}={{{name}:String}}')
    if rule.activity_month:
        bound['activity_month']=month(rule.activity_month)
        clauses.append(f'profile_ref IN (SELECT profile_ref FROM {publication.database}.events WHERE toStartOfMonth(event_date)={{activity_month:Date32}})')
    for n,condition in enumerate(rule.conditions):
        key=f'condition_{n}';bound[key]=int(condition.value)
        operator={'gte':'>=','lte':'<=','eq':'='}[condition.op]
        clauses.append(f'{condition.field} {operator} {{{key}:Int64}}')
    if opportunity:
        clauses.append({'high_value':'order_count>0 AND net_revenue_minor>=5000000',
            'churn_risk':'order_count>0 AND recency_days>=30',
            'cart_opportunity':'order_count=0 AND add_to_cart_count>0'}[opportunity])
    return ' AND '.join('('+part+')' for part in clauses),bound


def _public_profile(row:Mapping[str,object],as_of:datetime,detail:bool=True) -> dict[str,object]:
    result={key:row[key] for key in GOLD_FIELDS}
    result.update({key:row[key] for key in ('profile_ref','crm_matched','crm_consent_analytics','crm_consent_marketing')})
    result.update(identity_evidence(row['profile_type']))
    result['activation_eligible']=activation_eligible(row)
    reasons=[]
    if row['profile_type']!='customer':reasons.append('unidentified_profile')
    if row['crm_matched'] is not True:reasons.append('no_certified_crm_match')
    for key in ('analytics','marketing'):
        value=row['crm_consent_'+key]
        if value is not True:reasons.append(key+('_denied' if value is False else '_unknown'))
    if row['consent_analytics'] is not True:reasons.append('historical_analytics_denied')
    result['activation_exclusion_reasons']=reasons
    result['detail_available']=detail
    if detail:result.update(lifecycle_metrics(row,row,as_of))
    else:result.update(last_purchase_at=None,recency_days=None,add_to_cart_count=None,email_click_count=None,opportunities=None)
    return wire(result)


class LifecycleService:
    def __init__(self,analytics:Analytics):self.analytics=analytics

    def _context(self,*,detail:bool=True) -> tuple[Analytics,Any|None,datetime,dict[str,object]]:
        session=self.analytics.pin();currency=single_currency(session)
        info=session._query(f'SELECT max(last_activity_at) AS last_activity_at,count() AS population FROM {session.release.database}.customer_360')[0]
        latest=info['last_activity_at'] if info['population'] else None
        day=latest.astimezone(timezone.utc).date() if latest else session.release.logical_day
        as_of=datetime.combine(day+timedelta(days=1),time.min,timezone.utc)
        publication=None
        if detail:
            try:publication=JourneyJournal(session.client).resolve(session.release)
            except ValueError as error:raise LifecycleError('projection_mismatch') from error
        provenance=session.provenance()|{'model':'gold.customer_360','serving_relation':'customer_360',
            'as_of':as_of.isoformat(),'data_through':as_of.isoformat() if latest else None,'max_event_date':day.isoformat() if latest else None,
            'timestamp_resolution':'day','observed_at':datetime.now(timezone.utc).isoformat(),'historical':day<datetime.now(timezone.utc).date()-timedelta(days=1),'currency':currency}
        if publication:
            provenance.update(silver_snapshot_id=str(publication.silver_snapshot_id),projection_checksum=publication.checksum,projection_version=publication.version,projection_row_count=str(publication.row_count))
            check=session._query(_cte(session,publication)+''' SELECT countIf(detail_event_count!=event_count OR detail_session_count!=session_count OR
                detail_order_count!=order_count OR detail_gross!=gross_revenue_minor OR detail_refund!=refund_amount_minor) AS mismatches,
                sum(toUInt128(event_count)) AS gold_events FROM profiles''',{'identity':session.release.identity,'as_of':as_of})[0]
            if check['mismatches'] or check['gold_events']!=publication.row_count:raise LifecycleError('projection_mismatch')
        return session,publication,as_of,provenance

    def cohorts(self) -> dict[str,object]:
        session,_,as_of,provenance=self._context(detail=False)
        rows=session._query(f'SELECT * FROM {session.release.database}.cohort_retention ORDER BY cohort_month,profile_type,activity_month')
        totals=session._query(_cte(session,None)+''' SELECT count() AS profiles_total,countIf(activation_eligible) AS activation_eligible_total FROM profiles''',
            {'identity':session.release.identity,'as_of':as_of})[0]
        return wire({'rows':rows,'totals':totals,'population':'Historical governed analytics, not an activation population',
            'consent_policy':POLICY,'provenance':provenance|{'model':'gold.cohort_retention','serving_relation':'cohort_retention'}})

    def _members(self,session:Analytics,publication:Any,as_of:datetime,rule:SegmentRule,limit:int,offset:int,opportunity:str|None=None) -> tuple[list[dict],int]:
        where,bound=_where(rule,publication,opportunity)
        params={'identity':session.release.identity,'as_of':as_of,'limit':limit,'offset':offset}|bound
        cte=_cte(session,publication)
        total=session._query(cte+f' SELECT count() AS total FROM profiles WHERE {where}',params)[0]['total']
        rows=session._query(cte+f' SELECT * FROM profiles WHERE {where} ORDER BY last_activity_at DESC,profile_ref ASC LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}',params)
        return [_public_profile(row,as_of) for row in rows],total

    def profiles(self,*,cohort_month:str|None=None,activity_month:str|None=None,profile_type:str|None=None,purpose:str='analytics',opportunity:str|None=None,limit:int=20,offset:int=0) -> dict[str,object]:
        validate_selection(cohort_month,activity_month,profile_type,purpose,opportunity,limit,offset)
        session,publication,as_of,provenance=self._context()
        common={'provenance':provenance,'purpose':purpose,'consent_policy':POLICY,'limit':str(limit),'offset':str(offset),
            'filters':{'cohort_month':cohort_month,'activity_month':activity_month,'profile_type':profile_type,'purpose':purpose,'opportunity':opportunity}}
        if not publication:return common|{'status':'unavailable','message':'Publish the pinned MP-07 journey detail before lifecycle profile analysis.','rows':[],'total':None,'has_more':False}
        rule=SegmentRule(purpose,(),cohort_month,activity_month,profile_type)
        rows,total=self._members(session,publication,as_of,rule,limit,offset,opportunity)
        return common|{'status':'available' if rows else 'empty','rows':rows,'total':str(total),'has_more':offset+len(rows)<total}

    def timeline(self,profile_ref:str,*,limit:int=50,offset:int=0) -> dict[str,object]:
        if not isinstance(profile_ref,str) or re.fullmatch(r'[0-9a-f]{64}',profile_ref) is None:raise ValueError('invalid profile reference')
        validate_selection(None,None,None,'analytics',limit=limit,offset=offset)
        session,publication,as_of,provenance=self._context()
        params={'identity':session.release.identity,'as_of':as_of,'profile_ref':profile_ref,'limit':limit,'offset':offset}
        matches=session._query(_cte(session,publication)+' SELECT * FROM profiles WHERE profile_ref={profile_ref:String}',params)
        if not matches:raise LifecycleError('profile_not_found')
        row=matches[0]
        common={'profile':_public_profile(row,as_of,publication is not None),'provenance':provenance,'events':[],'limit':str(limit),'offset':str(offset),'total':None,'has_more':False}
        if row['crm_consent_analytics'] is False:return common|{'status':'consent_restricted','message':'Latest certified CRM analytics consent is denied; timeline detail is suppressed.'}
        if not publication:return common|{'status':'unavailable','message':'Publish the pinned MP-07 journey detail first.'}
        total=session._query(f'SELECT count() AS total FROM {publication.database}.events WHERE profile_ref={{profile_ref:String}}',params)[0]['total']
        events=session._query(f'SELECT * FROM {publication.database}.events WHERE profile_ref={{profile_ref:String}} ORDER BY event_time,event_id LIMIT {{limit:UInt32}} OFFSET {{offset:UInt32}}',params)
        dimensions={r['campaign_id']:r for r in session.campaigns()}
        output=[]
        for event in events:
            output.append({'event_ref':reference(session.release.identity,event['event_id']),'event_name':event['event_name'],'event_time':event['event_time'],
                'source':event['source'],'campaign_id':event['campaign_id'],'campaign_name':dimensions.get(event['campaign_id'],{}).get('name','Unattributed'),
                'gross_revenue_minor':(event['gross_amount_minor'] or 0) if event['event_name']=='order_created' else 0,
                'refund_amount_minor':(event['refund_amount_minor'] or 0) if event['event_name']=='order_refunded' else 0})
        return wire(common|{'status':'available','events':output,'total':total,'has_more':offset+len(events)<total})

    def preview(self,rule:SegmentRule) -> dict[str,object]:
        session,publication,as_of,provenance=self._context()
        if not publication:raise LifecycleError('lifecycle_detail_unavailable')
        rows,total=self._members(session,publication,as_of,rule,20,0)
        excluded=session._query(_cte(session,publication)+' SELECT count() AS total FROM profiles WHERE NOT activation_eligible',{'identity':session.release.identity,'as_of':as_of})[0]['total']
        key=hashlib.sha256(json.dumps([session.release.identity,session.release.proof_digest,as_of.isoformat(),rule.digest],separators=(',',':')).encode()).hexdigest()
        return wire({'purpose':rule.purpose,'rule_digest':rule.digest,'evaluation_key':key,'matched_total':total,'excluded_from_activation_total':excluded,
            'rows':rows,'sample_limit':20,'sample_truncated':total>20,'consent_policy':POLICY,'provenance':provenance})
