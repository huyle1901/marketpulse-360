#!/usr/bin/env python3
"""MP08 proof on an existing test-owned project; no source reseed/replay/promotion.

Creates one operator-authored demonstration segment with two revisions, retains
it, and optionally restarts API/web. Source oracles are independent of lifecycle
rollups/predicates. Evidence is restricted to ignored .lakehouse storage.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
from datetime import datetime,timedelta,timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.error
import urllib.request
from urllib.parse import urlencode
import uuid

PROFILE_FIELDS=('profile_type','first_activity_at','last_activity_at','cohort_month','event_count','session_count','order_count','refund_count',
 'gross_revenue_minor','refund_amount_minor','net_revenue_minor','consent_analytics','crm_matched','crm_consent_analytics','crm_consent_marketing',
 'activation_eligible','confidence','last_purchase_at','recency_days','add_to_cart_count','email_click_count','opportunities')
NUMBERS=('event_count','session_count','order_count','refund_count','gross_revenue_minor','refund_amount_minor','net_revenue_minor')


def assert_profiles(expected:list[dict],actual:list[dict],*,fields:tuple[str,...]=PROFILE_FIELDS) -> None:
    def keyed(rows):
        result={row['profile_ref']:{k:row[k] for k in fields} for row in rows}
        assert len(result)==len(rows),'duplicate profile keys'
        return result
    assert keyed(expected)==keyed(actual),'exact profile/consent/RFM equality failed'


def assert_activation(rows:list[dict]) -> None:
    assert all(r['profile_type']=='customer' and all(r.get(k) is True for k in ('consent_analytics','crm_matched','crm_consent_analytics','crm_consent_marketing','activation_eligible')) for r in rows),'unknown/denied identity or consent in activation'


def assert_public_payload(value:object) -> None:
    forbidden={'profile_key','customer_id','anonymous_id','event_id','order_id','session_id','click_id','email','phone','payload_json','url'}
    if isinstance(value,dict):
        assert not forbidden.intersection(value),'raw identity/contact key in HTTP'
        for child in value.values():assert_public_payload(child)
    elif isinstance(value,list):
        for child in value:assert_public_payload(child)


def stamp(value:str) -> datetime:return datetime.fromisoformat(value.replace('Z','+00:00')).astimezone(timezone.utc)
def ref(identity:str,value:str) -> str:return hashlib.sha256((identity+'|'+value).encode()).hexdigest()
def digest(rule:dict) -> str:return hashlib.sha256(json.dumps(rule,sort_keys=True,separators=(',',':')).encode()).hexdigest()


def independent_profile(gold:dict,events:list[dict],crm:dict|None,identity:str,as_of:str) -> dict:
    result={k:str(gold[k]) for k in NUMBERS}
    result.update({k:gold[k] for k in ('profile_type','cohort_month','consent_analytics')})
    result.update({k:stamp(gold[k]).isoformat() for k in ('first_activity_at','last_activity_at')})
    purchases=[stamp(e['event_time']) for e in events if e['event_name']=='order_created']
    last=max(purchases,default=None);recency=(stamp(as_of).date()-last.date()).days if last else None
    eligible=gold['profile_type']=='customer' and gold['consent_analytics'] is True and crm is not None and crm['consent_analytics'] is True and crm['consent_marketing'] is True
    carts=sum(e['event_name']=='add_to_cart' for e in events);emails=sum(e['event_name']=='email_clicked' for e in events)
    result.update(profile_ref=ref(identity,gold['profile_key']),crm_matched=crm is not None,
        crm_consent_analytics=crm['consent_analytics'] if crm else None,crm_consent_marketing=crm['consent_marketing'] if crm else None,
        confidence={'customer':'deterministic_customer_reference','anonymous':'anonymous_device_only','session':'session_only'}[gold['profile_type']],
        activation_eligible=eligible,last_purchase_at=last.isoformat() if last else None,recency_days=str(recency) if recency is not None else None,
        add_to_cart_count=str(carts),email_click_count=str(emails),opportunities={'high_value':int(gold['order_count'])>0 and int(gold['net_revenue_minor'])>=5000000,
        'churn_risk':int(gold['order_count'])>0 and recency is not None and recency>=30,'cart_opportunity':int(gold['order_count'])==0 and carts>0})
    return result


def ordered(rows:list[dict]) -> list[dict]:
    result=sorted(rows,key=lambda r:r['profile_ref']);result.sort(key=lambda r:stamp(r['last_activity_at']),reverse=True);return result


def selected(rows:list[dict],events:dict,selection:dict) -> list[dict]:
    result=[]
    for row in rows:
        if selection.get('purpose')=='activation' and not row['activation_eligible']:continue
        if any(selection.get(k) and row[k]!=selection[k] for k in ('profile_type','cohort_month')):continue
        if selection.get('activity_month') and not any(e['event_time'][:7]==selection['activity_month'][:7] for e in events[row['profile_ref']]):continue
        if selection.get('opportunity') and not row['opportunities'][selection['opportunity']]:continue
        good=True
        for c in selection.get('conditions',[]):
            value=row[c['field']]
            if value is None:good=False;break
            value,threshold=int(value),int(c['value'])
            if not {'gte':value>=threshold,'lte':value<=threshold,'eq':value==threshold}[c['op']]:good=False;break
        if good:result.append(row)
    return ordered(result)


GOLD_ORACLE=r'''
import os,json
from marketpulse_batch.settings import load_settings
from marketpulse_batch.resources import clickhouse_client
from marketpulse_batch.ledger import BatchLedger
from marketpulse_batch.publication import resolve_active
from marketpulse_batch.certification import read_certification,verify_certification_current
from marketpulse_batch.export import ExportManifest
from marketpulse_batch.jobs import local_receipt_path
from marketpulse_batch.proof import export_rows,validate_release_binding
from marketpulse_batch.canonical import rowset_checksum
from marketpulse_lakehouse.source_files import create_s3_client
s=load_settings(os.environ);c=clickhouse_client(s);release=resolve_active(c);ledger=BatchLedger(s.ledger_dsn)
receipt,_=ledger.get_receipt(release.identity);bundle=ledger.get_certified_bundle(release.identity)
cert=read_certification(local_receipt_path(bundle.certification_uri));verify_certification_current(c,receipt,cert)
manifest=ExportManifest.from_payload(json.loads(local_receipt_path(bundle.export_manifest_uri).read_bytes()));validate_release_binding(release,bundle,cert,manifest)
s3=create_s3_client();rows={}
for name in ('gold.customer_360','gold.cohort_retention','silver.customers','silver.campaigns','gold.campaign_performance_daily'):
 relation=next(r for r in manifest.relations if r.name==name);values=list(export_rows(manifest,name,s3))
 assert rowset_checksum(relation.columns,values)==(relation.row_count,relation.business_checksum),'typed export differs';rows[name]=values
print(json.dumps({'identity':release.identity,'proof_digest':release.proof_digest,'silver_snapshot_id':str(receipt.output_snapshots['silver.customer_events']),
 'export_digest':manifest.digest,'source_row_counts':{r.name:r.row_count for r in manifest.relations},'rows':rows},default=str));c.close()
'''

EVENT_ORACLE=r'''
import os,json
from marketpulse_app.settings import load_settings,clients
from marketpulse_app.journey_projection import JourneyJournal,verify_projection
from marketpulse_app.analytics import wire
analytics,_=clients(load_settings(os.environ));session=analytics.pin();pub=JourneyJournal(session.client).resolve(session.release);assert pub is not None
verify_projection(session.client,pub)
rows=session._query(f'SELECT * FROM {pub.database}.events ORDER BY event_time,event_id')
print(json.dumps(wire({'identity':session.release.identity,'silver_snapshot_id':pub.silver_snapshot_id,'checksum':pub.checksum,'rows':rows,'campaigns':session.campaigns()})))
session.client.close()
'''


def oracle(project:str,service:str,code:str) -> dict:
    result=subprocess.run(['docker','exec','-i',f'{project}-{service}-1','python','-'],input=code,capture_output=True,text=True,check=True)
    return json.loads(result.stdout)


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--project',required=True);parser.add_argument('--api',required=True);parser.add_argument('--web',required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--restart',action='store_true');args=parser.parse_args()
    if re.fullmatch(r'marketpulse-[a-z0-9-]+-proof',args.project) is None:parser.error('explicit test-owned *-proof project required')
    if not str(args.output.resolve()).startswith(str(Path('.lakehouse').resolve())+'/'):parser.error('ignored .lakehouse evidence path required')
    def request(path,selection=None,body=None,method='GET',expected=200):
        url=args.api.rstrip('/')+'/api/v1/'+path+('?' + urlencode(selection) if selection else '')
        req=urllib.request.Request(url,data=json.dumps(body).encode() if body is not None else None,method=method,headers={'Content-Type':'application/json','Cache-Control':'no-cache'})
        try:response=urllib.request.urlopen(req,timeout=30)
        except urllib.error.HTTPError as error:response=error
        with response:
            assert response.status==expected,f'HTTP status differs for {path}'
            assert response.headers['cache-control']=='no-store';return json.load(response)
    gold=oracle(args.project,'batch-code',GOLD_ORACLE);event_source=oracle(args.project,'api',EVENT_ORACLE)
    assert gold['identity']==event_source['identity'] and gold['silver_snapshot_id']==event_source['silver_snapshot_id']
    cohorts=request('lifecycle/cohorts');as_of=cohorts['provenance']['as_of'];identity=gold['identity']
    assert cohorts['provenance']['identity']==identity
    expected_cohorts=gold['rows']['gold.cohort_retention']
    keys=('cohort_month','activity_month','profile_type')
    def cohort_map(rows):return {tuple(r[k] for k in keys):{k:str(v) for k,v in r.items()} for r in rows}
    assert len(cohort_map(cohorts['rows']))==len(cohorts['rows']) and cohort_map(expected_cohorts)==cohort_map(cohorts['rows']),'cohort keyed rows differ'
    events=defaultdict(list)
    for e in event_source['rows']:events[e['profile_ref']].append(e)
    crm={r['customer_id']:r for r in gold['rows']['silver.customers']}
    expected=ordered([independent_profile(r,events[ref(identity,r['profile_key'])],crm.get(r['customer_id']),identity,as_of) for r in gold['rows']['gold.customer_360']])
    assert cohorts['totals']=={'profiles_total':str(len(expected)),'activation_eligible_total':str(sum(r['activation_eligible'] for r in expected))}
    assert as_of[:10]==(max(stamp(r['last_activity_at']).date() for r in expected)+timedelta(days=1)).isoformat()
    checks=[{}, {'purpose':'activation'}, {'profile_type':'anonymous'}, {'profile_type':'session'}, {'cohort_month':'1900-01-01'},
            {'opportunity':'high_value'},{'opportunity':'churn_risk'},{'opportunity':'cart_opportunity'}]
    checks.extend({k:r[k] for k in keys} for r in cohorts['rows'])
    checks=[{('profile_type' if k=='profile_type' else k):v for k,v in s.items()} for s in checks]
    # profile discovery uses cohort/type/activity, all certified cells must drill exactly.
    for selection in checks:
        selection.pop('month_number',None)
        want=selected(expected,events,selection);actual=[];offset=0
        while True:
            page=request('lifecycle/profiles',selection|{'limit':100,'offset':offset})
            assert page['provenance']['identity']==identity and page['total']==str(len(want));actual+=page['rows']
            if not page['has_more']:break
            offset+=100;assert offset<=100000
        assert_profiles(want,actual)
        if selection.get('purpose')=='activation':assert_activation(actual)
    timelines={}
    for row in expected:
        timeline=request('lifecycle/profiles/'+row['profile_ref']+'/timeline',{'limit':100})
        assert timeline['provenance']['identity']==identity
        if row['crm_consent_analytics'] is False:
            assert timeline['status']=='consent_restricted' and timeline['events']==[];continue
        assert timeline['status']=='available';want=events[row['profile_ref']]
        assert timeline['total']==str(len(want));assert len(timeline['events'])==min(100,len(want))
        for source,public in zip(want,timeline['events']):
            assert public['event_ref']==ref(identity,source['event_id']) and public['event_name']==source['event_name']
            assert public['event_time']==source['event_time']
            assert public['gross_revenue_minor']==str(int(source['gross_amount_minor'] or 0) if source['event_name']=='order_created' else 0)
            assert public['refund_amount_minor']==str(int(source['refund_amount_minor'] or 0) if source['event_name']=='order_refunded' else 0)
        timelines[row['profile_ref']]=timeline
        assert_public_payload(timeline)
    rule={'schema_version':'mp08-segment-v1','purpose':'activation','cohort_month':None,'activity_month':None,'profile_type':None,'conditions':[]}
    def preview(definition):
        result=request('lifecycle/segments/preview',body={'rule':definition},method='POST');want=selected(expected,events,definition)
        assert result['matched_total']==str(len(want));assert_profiles(want[:20],result['rows']);assert result['rule_digest']==digest(definition)
        assert result['sample_truncated']==(len(want)>20)
        if definition['purpose']=='activation':assert_activation(result['rows'])
        evaluation=hashlib.sha256(json.dumps([identity,gold['proof_digest'],as_of,digest(definition)],separators=(',',':')).encode()).hexdigest()
        assert result['evaluation_key']==evaluation;return result
    first_preview=preview(rule);assert preview(rule)['evaluation_key']==first_preview['evaluation_key']
    for field,op,value in (('recency_days','gte','1'),('net_revenue_minor','gte','5000000'),('order_count','eq','0'),('email_click_count','gte','1')):
        preview(rule|{'conditions':[{'field':field,'op':op,'value':value}]})
    name='MP08 proof '+uuid.uuid4().hex[:8]
    saved=request('lifecycle/segments',body={'name':name,'rule':rule,'expected_version':0},method='POST')
    old=request(f'lifecycle/segments/{saved["segment_id"]}/versions/1');assert saved==old
    updated_rule=rule|{'conditions':[{'field':'net_revenue_minor','op':'gte','value':'5000000'}]}
    second=request('lifecycle/segments/'+saved['segment_id'],body={'name':name+' high value','rule':updated_rule,'expected_version':1},method='PUT')
    assert second['version']=='2';assert request(f'lifecycle/segments/{saved["segment_id"]}/versions/1')==old
    request('lifecycle/segments/'+saved['segment_id'],body={'name':name,'rule':rule,'expected_version':1},method='PUT',expected=409)
    snapshots={key:request(key) for key in ('dashboard','campaign-settings','alert-rules','alerts','analysis/funnel','analysis/attribution','analysis/journeys')}
    timeline_ref=next(iter(timelines));benchmarks={}
    for workload,path,body in (('cohorts','lifecycle/cohorts',None),('profiles','lifecycle/profiles',None),('timeline','lifecycle/profiles/'+timeline_ref+'/timeline',None),('preview','lifecycle/segments/preview',{'rule':rule})):
        times=[]
        for n in range(120):
            began=time.perf_counter();request(path,body=body,method='POST' if body else 'GET');elapsed=(time.perf_counter()-began)*1000
            if n>=20:times.append(elapsed)
        values=sorted(times);benchmarks[workload]={'warmups':20,'samples':100,'p50_ms':values[49],'p95_ms':values[94],'max_ms':max(times),'failures':0,'query_cache':False}
        assert values[94]<=500,'fixture p95 exceeds budget'
    restarted=False
    if args.restart:
        subprocess.run(['docker','restart',f'{args.project}-api-1',f'{args.project}-web-1'],capture_output=True,check=True)
        deadline=time.monotonic()+60
        while True:
            try:request('health/ready');urllib.request.urlopen(args.web+'/customers',timeout=5).close();break
            except Exception:
                if time.monotonic()>deadline:raise
                time.sleep(.5)
        assert request(f'lifecycle/segments/{saved["segment_id"]}/versions/1')==old
        assert request(f'lifecycle/segments/{saved["segment_id"]}/versions/2')==second
        assert preview(old['rule'])['evaluation_key']==first_preview['evaluation_key']
        assert_profiles(expected,request('lifecycle/profiles',{'limit':100})['rows'])
        for key,before in snapshots.items():
            after=request(key)
            if key=='dashboard':assert before['certified']['totals']==after['certified']['totals'] and before['certified']['provenance']['identity']==after['certified']['provenance']['identity']
            elif key=='analysis/funnel':assert before['totals']==after['totals']
            elif key=='analysis/attribution':assert before['models']==after['models'] and before['global_reconciliation']==after['global_reconciliation']
            elif key=='analysis/journeys':assert before['rows']==after['rows']
            else:assert before==after
        restarted=True
    resources=subprocess.run(['docker','info','--format','{{.NCPU}} CPUs; {{.MemTotal}} memory bytes'],capture_output=True,text=True,check=True).stdout.strip()
    report={'accepted':True,'identity':identity,'proof_digest':gold['proof_digest'],'export_digest':gold['export_digest'],'silver_snapshot_id':gold['silver_snapshot_id'],'projection_checksum':event_source['checksum'],
        'source_row_counts':gold['source_row_counts'],'cohort_rows':len(cohorts['rows']),'historical_profiles':len(expected),'activation_eligible':sum(r['activation_eligible'] for r in expected),'governed_events':len(event_source['rows']),
        'discovery_cases':len(checks),'timeline_cases':len(expected),'as_of':as_of,'saved_demo_segment_id':saved['segment_id'],'saved_revisions':['1','2'],'immutable_revision_and_stale_write':True,'restart_preserved_mp06_mp07_mp08':restarted,
        'benchmarks':benchmarks,'docker_resources':resources,'versions':{'Python':'3.12.13','Next':'16.4.0','ClickHouse':'26.8.25.14','PostgreSQL':'17.6'},
        'limits':'fixture only, profiles/timeline 1..100 rows/page, offset<=100000, preview first20 plus exact count; no activation delivery'}
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'accepted':True,'output':str(args.output),'benchmarks':benchmarks},indent=2));return 0

if __name__=='__main__':raise SystemExit(main())
