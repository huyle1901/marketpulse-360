"""Read-only decision readiness; never mislabel automation as human usability."""
from __future__ import annotations
import json
import time
import urllib.request
from pathlib import Path
from typing import Any
from marketpulse_app.proof_http import ProofHTTP
from .contracts import digest, percentiles

TASKS = [
    {'id':'live_kpi','role':'Performance manager','route':'/','decision':'Find the campaign to investigate from live KPI and certified baseline.'},
    {'id':'alerts','role':'Performance manager','route':'/','decision':'Explain one alert using its threshold and immutable evidence.'},
    {'id':'pacing','role':'CMO','route':'/','decision':'Assess spend against configured campaign budget and period.'},
    {'id':'simulation','role':'CMO','route':'/','decision':'Compare a VND 1000 allocation against historical source ROAS.'},
    {'id':'funnel','role':'Product manager','route':'/analysis','decision':'Locate a stage-reach drop without treating it as a sequential funnel.'},
    {'id':'attribution','role':'Marketing analyst','route':'/analysis','decision':'Compare three models while preserving reconciled source revenue.'},
    {'id':'customer_360','role':'CRM marketer','route':'/customers','decision':'Inspect a pseudonymous customer timeline and identity confidence.'},
    {'id':'segmentation','role':'CRM marketer','route':'/customers','decision':'Preview a reproducible consent-eligible audience and inspect saved revisions.'},
    {'id':'cohorts','role':'CRM marketer','route':'/customers','decision':'Compare cohort activity within the reported governed population.'},
    {'id':'opportunity','role':'CRM marketer','route':'/customers','decision':'Find a high-value audience and distinguish it from activation eligibility.'},
]


def check_decision(task: str, value: Any) -> dict[str, Any]:
    try:
        if task == 'live_kpi':
            totals = value['certified']['totals']; live = value['streaming']
            valid = (len(totals)==1 and totals[0]['net_revenue_minor']=='22193000'
                     and live['status']=='available' and len(live['rows'])==14)
            summary = {'certified_net_minor':totals[0]['net_revenue_minor'],'live_keys':len(live['rows'])}
        elif task == 'alerts':
            valid = bool(value) and any(row['evidence']['kind']=='budget_overrun' for row in value)
            summary = {'explained_occurrences':len(value)}
        elif task == 'pacing':
            valid = value['spend_minor']=='91461' and value['forecast_minor']=='91461'
            summary = {'spend_minor':value['spend_minor'],'status':value['status']}
        elif task == 'simulation':
            valid = value['projected_net_minor']=='35282'
            summary = {'projected_net_minor':value['projected_net_minor'],'assumption':'historical source ROAS; not causal uplift'}
        elif task == 'funnel':
            totals=value['totals'];valid=bool(value['campaigns']) and int(totals['sessions'])>0 and all(0<=int(v)<=int(totals['sessions']) for v in totals.values())
            summary={'stage_reach_totals':totals,'unit':value['unit']}
        elif task == 'attribution':
            valid=(value['global_reconciliation']['balanced'] and len(value['models'])==3
                   and all(r['net_revenue_minor']=='22193000' for r in value['models']))
            summary={'models':len(value['models']),'net_minor':'22193000'}
        elif task == 'customer_360':
            valid=bool(value['events']) and bool(value['profile']['confidence'])
            summary={'timeline_events':len(value['events']),'confidence':value['profile']['confidence']}
        elif task == 'segmentation':
            preview=value['preview'];valid=(preview['evaluation_key']==value['repeated_key'] and int(preview['matched_total'])>0
                and bool(value['saved_revisions']) and all(r['activation_eligible'] is True for r in preview['rows']))
            summary={'eligible_preview_total':preview['matched_total'],'saved_revision_verified':True}
        elif task == 'cohorts':
            valid=int(value['totals']['profiles_total'])==60 and bool(value['rows'])
            summary={'population':value['population'],'profiles_total':value['totals']['profiles_total']}
        elif task == 'opportunity':
            valid=int(value['total'])>0 and all(r['opportunities']['high_value'] for r in value['rows'])
            summary={'high_value_total':value['total'],'purpose':value['purpose']}
        else: raise ValueError('unknown decision task')
        if not valid: raise ValueError('decision result did not meet fixture expectation')
        return summary
    except (KeyError, IndexError, TypeError) as error:
        raise ValueError('incomplete decision evidence: '+task) from error


def evaluate_app(api: str, origin: str, username: str, password_file: Path) -> dict[str, Any]:
    http=ProofHTTP(api,origin,username,password_file)
    def call(path: str, body: dict | None = None) -> Any:
        request=urllib.request.Request(api+'/api/v1/'+path,data=json.dumps(body).encode() if body is not None else None,
            headers={'Content-Type':'application/json'})
        with http.open(request) as response: return json.load(response)
    role=call('auth/session')['role']
    if role not in ('operator','admin'): raise ValueError('decision proof requires operator or admin')
    stable={p:call(p) for p in ('campaign-settings','alert-rules','lifecycle/segments')}
    period='from_date=2026-01-01&to_date=2026-01-03'
    dashboard=call('dashboard?'+period);identity=dashboard['certified']['provenance']['identity']
    campaign='b00db876-5a1b-55c9-8412-9441821fe3ef'
    rule={'schema_version':'mp08-segment-v1','purpose':'activation','cohort_month':None,'activity_month':None,'profile_type':None,'conditions':[]}
    results=[]
    for task in TASKS:
        start=time.perf_counter();name=task['id']
        if name=='live_kpi': value=call('dashboard?'+period)
        elif name=='alerts': value=call('alerts')
        elif name=='pacing': value=call('budget-pacing/'+campaign)
        elif name=='simulation': value=call('budget-simulation',{'from_date':'2026-01-01','to_date':'2026-01-03','currency':'VND','total_budget_minor':'1000','allocations':[{'campaign_id':campaign,'budget_minor':'1000'}]})
        elif name=='funnel': value=call('analysis/funnel?'+period)
        elif name=='attribution': value=call('analysis/attribution?'+period)
        elif name=='customer_360':
            profiles=call('lifecycle/profiles?limit=1'); value=call('lifecycle/profiles/'+profiles['rows'][0]['profile_ref']+'/timeline')
        elif name=='segmentation':
            preview=call('lifecycle/segments/preview',{'rule':rule});repeated=call('lifecycle/segments/preview',{'rule':rule})
            saved=[]
            for segment in stable['lifecycle/segments']:
                saved.append(call('lifecycle/segments/'+segment['segment_id']+'/versions/1'))
            value={'preview':preview,'repeated_key':repeated['evaluation_key'],'saved_revisions':saved}
        elif name=='cohorts': value=call('lifecycle/cohorts')
        else: value=call('lifecycle/profiles?opportunity=high_value')
        summary=check_decision(name,value)
        provenance=value.get('provenance') if isinstance(value,dict) else None
        if provenance and provenance['identity']!=identity: raise ValueError('release changed during decision proof')
        results.append({**task,'passed':True,'mode':'automated_api_decision_readiness','seconds':time.perf_counter()-start,
                        'summary':summary,'response_hash':digest([value] if isinstance(value,dict) else value)})
    if stable!={p:call(p) for p in stable}: raise ValueError('decision proof changed operational settings')
    timings=[]
    for i in range(120):
        start=time.perf_counter();value=call('dashboard?'+period)
        if value['certified']['provenance']['identity']!=identity: raise ValueError('dashboard benchmark changed release')
        if i>=20: timings.append((time.perf_counter()-start)*1000)
    return {'version':1,'kind':'app','role':role,'identity':identity,'proof_digest':dashboard['certified']['provenance']['proof_digest'],
            'decisions':results,'dashboard_http':{**percentiles(timings),'warmups':20},
            'operational_configuration_preserved':True,'human_study':'pending',
            'limitations':'automated API checks and prior UI regression; no human task time or live pipeline-to-dashboard SLA'}
