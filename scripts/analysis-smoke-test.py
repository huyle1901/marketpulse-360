#!/usr/bin/env python3
"""Read-only MP07 proof on an explicit existing test-owned Compose project.

Uses validated MP05 exports and full governed sidecar oracles, never reseeds or
promotes a release. Restarts API/web only. Evidence belongs in ignored storage.
"""
from __future__ import annotations
import argparse
from collections import defaultdict
from decimal import Decimal
import json
from pathlib import Path
import re
import subprocess
import time
import urllib.request
from urllib.parse import urlencode

MODELS=('first_touch','last_touch','linear')
STAGES=('sessions','page_view_sessions','view_item_sessions','add_to_cart_sessions','checkout_sessions','purchase_sessions')
MONEY=('gross_revenue_minor','refund_amount_minor','net_revenue_minor')
FINANCE=(*MONEY,'allocated_order_count')


def assert_keyed(expected:list[dict],observed:list[dict],keys:tuple[str,...],fields:tuple[str,...]) -> None:
    def keyed(rows):
        result={tuple(row[k] for k in keys):tuple(row[f] for f in fields) for row in rows}
        assert len(result)==len(rows),'duplicate analytical keys'
        return result
    assert keyed(expected)==keyed(observed),'exact keyed analytical rows differ'


def assert_global(models:list[dict],source:dict) -> None:
    assert len(models)==3 and {r['model'] for r in models}==set(MODELS),'missing attribution model'
    assert int(source[MONEY[0]])-int(source[MONEY[1]])==int(source[MONEY[2]]),'source balance differs'
    assert all(all(row[k]==source[k] for k in MONEY) for row in models),'global financial totals differ'


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
settings=load_settings(os.environ);c=clickhouse_client(settings);release=resolve_active(c)
ledger=BatchLedger(settings.ledger_dsn);receipt,_=ledger.get_receipt(release.identity);bundle=ledger.get_certified_bundle(release.identity)
cert=read_certification(local_receipt_path(bundle.certification_uri));verify_certification_current(c,receipt,cert)
manifest=ExportManifest.from_payload(json.loads(local_receipt_path(bundle.export_manifest_uri).read_bytes()))
validate_release_binding(release,bundle,cert,manifest);s3=create_s3_client();result={}
for name in ('gold.funnel_daily','gold.attribution_daily','gold.campaign_performance_daily','silver.campaigns'):
 rows=list(export_rows(manifest,name,s3));relation=next(r for r in manifest.relations if r.name==name)
 assert rowset_checksum(relation.columns,rows)==(relation.row_count,relation.business_checksum),'export rowset mismatch'
 result[name]=rows
print(json.dumps({'rows':result,'identity':release.identity,'proof_digest':release.proof_digest,'export_digest':manifest.digest,
 'silver_snapshot_id':str(receipt.output_snapshots['silver.customer_events']),'source_row_counts':{r.name:r.row_count for r in manifest.relations}},default=str))
c.close()
'''

SILVER_ORACLE=r'''
import os,json,hashlib
from collections import defaultdict
from marketpulse_app.settings import load_settings,clients
from marketpulse_app.journey_projection import JourneyJournal,verify_projection
from marketpulse_app.analytics import wire
from marketpulse_batch.canonical import rowset_checksum
from marketpulse_batch.export import business_schema
from marketpulse_lakehouse.gold import build_funnel_daily,build_attribution_daily
analytics,store=clients(load_settings(os.environ));session=analytics.pin();publication=JourneyJournal(session.client).resolve(session.release)
assert publication is not None,'missing journey projection';verify_projection(session.client,publication)
events=session._query(f'SELECT * FROM {publication.database}.events');events=[r|{'profile_key':r['profile_ref']} for r in events]
for name,oracle in (('funnel_daily',build_funnel_daily(events)),('attribution_daily',build_attribution_daily(events))):
 actual=session._query(f'SELECT * FROM {session.release.database}.{name}')
 assert rowset_checksum(business_schema('gold.'+name),oracle)==rowset_checksum(business_schema('gold.'+name),actual),'Silver/Gold rowset differs'
orders=defaultdict(list);touches=[r for r in events if r['source'] in ('web','mobile')]
for r in events:
 if r['event_name'] in ('order_created','order_refunded'):orders[r['order_id']].append(r)
journeys={}
for order,finance in orders.items():
 key=hashlib.sha256((session.release.identity+'|'+order).encode()).hexdigest()
 journeys[key]={'allocations':build_attribution_daily(touches+finance),'purchase_time':next(r['event_time'] for r in finance if r['event_name']=='order_created'),
 'financial_events':[{'event_time':r['event_time'],'event_name':r['event_name'],'gross_revenue_minor':r['gross_amount_minor'] or 0 if r['event_name']=='order_created' else 0,
 'refund_amount_minor':r['refund_amount_minor'] or 0 if r['event_name']=='order_refunded' else 0} for r in finance]}
print(json.dumps(wire({'identity':session.release.identity,'silver_snapshot_id':publication.silver_snapshot_id,'projection_checksum':publication.checksum,
 'row_count':len(events),'silver_gold_full_row_equality':True,'journeys':journeys})))
session.client.close()
'''


def execute_oracle(project:str,service:str,code:str) -> dict:
    result=subprocess.run(['docker','exec','-i',f'{project}-{service}-1','python','-'],input=code,text=True,capture_output=True,check=True)
    return json.loads(result.stdout)


def report_oracle(gold:dict,start:str,end:str,dimensions:dict,selection:dict) -> dict:
    def eligible(campaign):
        dim=dimensions.get(campaign,{})
        return all(not value or (campaign if key=='campaign_id' else dim.get(key))==value for key,value in selection.items())
    funnel=[r for r in gold['gold.funnel_daily'] if start<=r['event_date']<end and eligible(r['campaign_id'])]
    fcampaigns={};fchannels={};totals=dict.fromkeys(STAGES,0)
    for row in funnel:
        campaign=row['campaign_id'];channel=dimensions.get(campaign,{}).get('platform','unattributed' if campaign=='__unattributed__' else 'unmapped')
        for group in (fcampaigns.setdefault(campaign,dict.fromkeys(STAGES,0)),fchannels.setdefault(channel,dict.fromkeys(STAGES,0)),totals):
            for k in STAGES:group[k]+=int(row[k])
    def frows(groups,key):return [{key:value,**{k:str(n) for k,n in data.items()}} for value,data in groups.items()]
    selected=[r for r in gold['gold.attribution_daily'] if start<=r['event_date']<end and eligible(r['campaign_id'])]
    global_rows=[r for r in gold['gold.attribution_daily'] if start<=r['event_date']<end]
    def credit(rows,group):
        groups={}
        if group=='model':groups={(m,):dict.fromkeys(MONEY,0)|{'allocated_order_count':Decimal(0)} for m in MODELS}
        for row in rows:
            campaign=row['campaign_id'];channel=dimensions.get(campaign,{}).get('platform','unattributed' if campaign=='__unattributed__' else 'unmapped')
            key=(row['model'],) if group=='model' else (row['model'],channel if group=='channel' else campaign)
            current=groups.setdefault(key,dict.fromkeys(MONEY,0)|{'allocated_order_count':Decimal(0)})
            for target,source in zip(MONEY,('allocated_gross_minor','allocated_refund_minor','allocated_net_minor')):current[target]+=int(row[source])
            current['allocated_order_count']+=Decimal(row['allocated_order_count'])
        return [{'model':key[0],**({group if group=='channel' else 'campaign_id':key[1]} if group!='model' else {}),**{k:format(v,'.6f') if k=='allocated_order_count' else str(v) for k,v in value.items()}} for key,value in groups.items()]
    source={k:str(sum(int(r[k]) for r in gold['gold.campaign_performance_daily'] if start<=r['event_date']<end)) for k in MONEY}
    return {'funnel':{'totals':{k:str(v) for k,v in totals.items()},'campaigns':frows(fcampaigns,'campaign_id'),'channels':frows(fchannels,'channel')},
            'attribution':{'models':credit(selected,'model'),'campaigns':credit(selected,'campaign'),'channels':credit(selected,'channel')},
            'global_models':credit(global_rows,'model'),'source':source}


def verify_reports(reports:dict,oracle:dict,silver:dict,start:str,end:str) -> None:
    assert len({r['provenance']['identity'] for r in reports.values()})==1,'mixed HTTP release'
    f=reports['funnel'];a=reports['attribution'];j=reports['journeys']
    assert f['totals']==oracle['funnel']['totals'],'funnel totals differ'
    assert f['unit']=='campaign-day session incidences'
    for group,key in (('campaigns','campaign_id'),('channels','channel')):assert_keyed(oracle['funnel'][group],f[group],(key,),STAGES)
    for group,keys in (('models',('model',)),('campaigns',('model','campaign_id')),('channels',('model','channel'))):
        assert_keyed(oracle['attribution'][group],a[group],keys,FINANCE)
    rec=a['global_reconciliation'];assert rec['balanced'] and rec['source_totals']==oracle['source']
    assert_global(rec['model_totals'],oracle['source'])
    assert_keyed(oracle['global_models'],rec['model_totals'],('model',),MONEY)
    assert j['provenance']['silver_snapshot_id']==silver['silver_snapshot_id']
    assert j['provenance']['projection_checksum']==silver['projection_checksum']
    for row in j['rows']:
        expected=silver['journeys'][row['journey_id']]
        assert row['purchase_time']==expected['purchase_time']
        allocations=[r for r in expected['allocations'] if start<=r['event_date']<end]
        assert_keyed(allocations,row['allocations'],('model','event_date','campaign_id','is_attributed'),('allocated_gross_minor','allocated_refund_minor','allocated_net_minor','allocated_order_count'))
        financial=[r for r in expected['financial_events'] if start<=r['event_time'][:10]<end]
        assert_keyed(financial,row['financial_events'],('event_time','event_name'),('gross_revenue_minor','refund_amount_minor'))
    payload=json.dumps(j)
    for key in ('profile_key','profile_ref','customer_id','anonymous_id','email','phone','order_id','event_id','session_id','click_id'):
        assert '"'+key+'"' not in payload,'raw identity in public journey'


def main() -> int:
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--project',required=True);parser.add_argument('--api',required=True);parser.add_argument('--web',required=True)
    parser.add_argument('--output',type=Path,required=True);parser.add_argument('--restart',action='store_true')
    args=parser.parse_args()
    if re.fullmatch(r'marketpulse-[a-z0-9-]+-proof',args.project) is None:parser.error('explicit test-owned *-proof project required')
    if not str(args.output.resolve()).startswith(str(Path('.lakehouse').resolve())+'/'):parser.error('evidence must stay under ignored .lakehouse')
    def get(path,selection=None):
        url=args.api.rstrip('/')+'/api/v1/'+path+('?' + urlencode(selection) if selection else '')
        with urllib.request.urlopen(urllib.request.Request(url,headers={'Cache-Control':'no-cache'}),timeout=30) as response:
            assert response.headers['cache-control']=='no-store';return json.load(response)
    def fetch(selection):return {name:get('analysis/'+name,selection) for name in ('funnel','attribution','journeys')}
    gold=execute_oracle(args.project,'batch-code',GOLD_ORACLE)
    silver=execute_oracle(args.project,'api',SILVER_ORACLE)
    assert gold['identity']==silver['identity'] and gold['silver_snapshot_id']==silver['silver_snapshot_id']
    before=fetch({});start,end=before['funnel']['filters'].values()
    dimensions={r['campaign_id']:r for r in gold['rows']['silver.campaigns']}
    baseline={'from_date':start,'to_date':end}
    checks=[({},start,end),({'platform':'google'},start,end),({'objective':'traffic'},start,end),({'campaign_id':next(iter(dimensions))},start,end),({'campaign_id':'__unattributed__'},start,end),({'campaign_id':'unknown'},start,end)]
    for day in sorted({r['event_date'] for r in gold['rows']['gold.campaign_performance_daily']}):
        from datetime import date,timedelta
        checks.append(({},day,(date.fromisoformat(day)+timedelta(days=1)).isoformat()))
    for selection,lo,hi in checks:
        actual=fetch(baseline|selection|{'from_date':lo,'to_date':hi})
        verify_reports(actual,report_oracle(gold['rows'],lo,hi,dimensions,selection),silver,lo,hi)
    for model in MODELS:
        assert get('analysis/journeys',baseline|{'model':model,'limit':'10'})['model']==model
    snapshots={key:get(key) for key in ('dashboard','campaign-settings','alert-rules','alerts')}
    benchmarks={}
    for name in ('funnel','attribution','journeys'):
        durations=[]
        for n in range(120):
            began=time.perf_counter();get('analysis/'+name,baseline);elapsed=(time.perf_counter()-began)*1000
            if n>=20:durations.append(elapsed)
        ordered=sorted(durations)
        benchmarks[name]={'samples':100,'warmups':20,'p50_ms':ordered[49],'p95_ms':ordered[94],'max_ms':max(durations),'failures':0,'query_cache':False}
        assert ordered[94]<=500,'fixture HTTP latency budget exceeded'
    restart=False
    if args.restart:
        subprocess.run(['docker','restart',f'{args.project}-api-1',f'{args.project}-web-1'],capture_output=True,check=True)
        deadline=time.monotonic()+60
        while True:
            try:
                get('health/ready')
                urllib.request.urlopen(args.web+'/analysis',timeout=5).close();break
            except Exception:
                if time.monotonic()>deadline:raise
                time.sleep(.5)
        after=fetch(baseline);verify_reports(after,report_oracle(gold['rows'],start,end,dimensions,{}),silver,start,end)
        assert [r['journey_id'] for r in before['journeys']['rows']]==[r['journey_id'] for r in after['journeys']['rows']],'sample changed after restart'
        for key,old in snapshots.items():
            new=get(key)
            if key=='dashboard':assert old['certified']['totals']==new['certified']['totals'] and old['certified']['provenance']['identity']==new['certified']['provenance']['identity']
            else:assert old==new,'MP06 operational state changed'
        after_silver=execute_oracle(args.project,'api',SILVER_ORACLE)
        assert silver==after_silver,'projection or independent oracle changed'
        restart=True
    resources=subprocess.run(['docker','info','--format','{{.NCPU}} CPUs; {{.MemTotal}} memory bytes'],capture_output=True,text=True,check=True).stdout.strip()
    evidence={'accepted':True,'identity':gold['identity'],'proof_digest':gold['proof_digest'],'export_digest':gold['export_digest'],
        'source_row_counts':gold['source_row_counts'],'silver_snapshot_id':silver['silver_snapshot_id'],'projection_checksum':silver['projection_checksum'],
        'governed_event_rows':silver['row_count'],'silver_gold_full_row_equality':True,'independent_export_http_cases':len(checks),'restart_preserved_mp06_and_mp07':restart,
        'benchmarks':benchmarks,'docker_resources':resources,'interval':baseline,'funnel_totals':before['funnel']['totals'],
        'financial_totals':before['attribution']['global_reconciliation']['source_totals'],'sample_count':len(before['journeys']['rows']),
        'versions':{'python':'3.12.13','Next':'16.4.0','ClickHouse':'26.8.25.14','Spark':'4.0.4','Iceberg':'1.12.0'},
        'limits':'fixture only; 100 candidate orders, 500 canonical touches / 100 financial events per context; default 5 samples'}
    args.output.parent.mkdir(parents=True,exist_ok=True);args.output.write_text(json.dumps(evidence,indent=2)+'\n')
    print(json.dumps({'accepted':True,'output':str(args.output),'benchmarks':benchmarks},indent=2));return 0

if __name__=='__main__':raise SystemExit(main())
