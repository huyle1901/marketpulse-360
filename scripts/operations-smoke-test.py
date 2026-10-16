#!/usr/bin/env python3
"""MP09 real retained-stack acceptance. Never reseeds, publishes or replays sources."""
from __future__ import annotations
import argparse,json,math,time,subprocess,urllib.request,urllib.error
from pathlib import Path
from marketpulse_app.proof_http import ProofHTTP,add_auth_arguments

def verify_access(api:str,web:str,origin:str,secret_root:Path) -> dict:
    rows=[]
    for base in (api,web):
        for role in ('viewer','operator','admin'):
            http=ProofHTTP(base,origin,'mp09-'+role,secret_root/(role+'_password'))
            for path,expected in [('dashboard',200),('analysis/journeys',403 if role=='viewer' else 200),('lifecycle/profiles',403 if role=='viewer' else 200),('ops/users',200 if role=='admin' else 403)]:
                try:
                    with http.open(base+'/api/v1/'+path) as r:code=r.status;r.read()
                except urllib.error.HTTPError as e:code=e.code
                assert code==expected,(role,path,code,expected)
                rows.append({'surface':'api' if base==api else 'proxy','role':role,'path':path,'status':code})
            req=urllib.request.Request(base+'/api/v1/auth/logout',data=b'{}',headers={'Origin':'http://foreign.test'})
            try:http.opener.open(req);raise AssertionError('foreign Origin allowed')
            except urllib.error.HTTPError as e:assert e.code==403
        try:urllib.request.urlopen(base+'/api/v1/dashboard');raise AssertionError('anonymous allowed')
        except urllib.error.HTTPError as e:assert e.code==401
    for supplied in (None,'null','http://foreign.test'):
        req=urllib.request.Request(api+'/api/v1/auth/login',data=b'{}',headers={'Origin':supplied} if supplied else {})
        try:urllib.request.urlopen(req);raise AssertionError('invalid Origin allowed')
        except urllib.error.HTTPError as e:assert e.code==403
    token=(secret_root/'metrics_token').read_text().strip()
    try:urllib.request.urlopen(api+'/metrics');raise AssertionError('anonymous metrics allowed')
    except urllib.error.HTTPError as e:assert e.code==401
    with urllib.request.urlopen(urllib.request.Request(api+'/metrics',headers={'Authorization':'Bearer '+token})) as r:metrics=r.read().decode()
    for name in ('admin_password','operator_password','viewer_password','metrics_token'):
        assert (secret_root/name).read_text().strip() not in metrics
    assert 'mp_api_request_seconds_bucket' in metrics
    return {'checks':rows,'anonymous_denied':True,'foreign_origin_rejected':True,'metrics_sanitized':True}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--api',default='http://localhost:48000');p.add_argument('--web',default='http://localhost:43100');p.add_argument('--project',required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--restart',action='store_true');p.add_argument('--roles-directory',type=Path,help='Explicit mp09-admin/operator/viewer demo credential directory');add_auth_arguments(p);a=p.parse_args()
    if not a.project.startswith('marketpulse-') or 'proof' not in a.project:p.error('explicit retained proof project required')
    access=verify_access(a.api,a.web,a.origin,a.roles_directory) if a.roles_directory else None
    http=ProofHTTP(a.api,a.origin,a.username,a.password_file,a.web)
    def call(path,body=None,base=None,method=None):
        req=urllib.request.Request((base or a.api)+'/api/v1/'+path,data=json.dumps(body).encode() if body is not None else None,method=method or ('POST' if body is not None else 'GET'),headers={'Content-Type':'application/json'})
        with http.open(req,timeout=120) as r:return json.load(r)
    stable={k:call(k) for k in ('campaign-settings','alert-rules','alerts','lifecycle/segments')}
    before=call('dashboard');lineage=call('ops/lineage');start=time.perf_counter();quality=call('ops/quality/check',{});quality_ms=(time.perf_counter()-start)*1000
    assert quality['status']=='passed',quality['reason']
    probe=call('ops/quality/probe',{});assert probe['reason']=='controlled_same_count_finance_corruption_detected',probe['reason']
    exact=probe['corruption_evidence'];assert exact['expected_row_count']==exact['observed_row_count'];assert exact['expected_checksum']!=exact['observed_checksum'];assert int(exact['observed_finance']['net_revenue_minor'])-int(exact['expected_finance']['net_revenue_minor'])==1
    assert probe['source_finance_mismatch'] and probe['rowset_mismatch'] and probe['row_count_unchanged']
    assert call('ops/lineage')==lineage
    after=call('dashboard');assert before['certified']==after['certified'] or before['certified']['totals']==after['certified']['totals']
    with urllib.request.urlopen('http://localhost:49090/api/v1/targets',timeout=10) as r:targets=json.load(r)['data']['activeTargets']
    assert len(targets)==2 and all(t['health']=='up' for t in targets),'Prometheus targets not healthy'
    def alert_active():
        with urllib.request.urlopen('http://localhost:49090/api/v1/alerts',timeout=10) as r:return any(x['labels']['alertname']=='ControlledQualityProbeFailed' and x['state']=='firing' for x in json.load(r)['data']['alerts'])
    deadline=time.monotonic()+40
    while not alert_active() and time.monotonic()<deadline:time.sleep(1)
    assert alert_active(),'controlled probe alert did not fire'
    call('ops/quality/probe/clear',{})
    deadline=time.monotonic()+40
    while alert_active() and time.monotonic()<deadline:time.sleep(1)
    assert not alert_active(),'controlled probe alert did not clear'
    assert any(r.get('reason')==probe['reason'] for r in call('ops/quality')),'probe history removed'
    workloads={}
    for path in ('dashboard','lifecycle/cohorts','ops/status'):
        for _ in range(20):call(path)
        values=[]
        for _ in range(100):
            start=time.perf_counter();call(path);values.append((time.perf_counter()-start)*1000)
        p95=sorted(values)[math.ceil(.95*len(values))-1];assert p95<=500,(path,p95)
        workloads[path]={'warmups':20,'samples':100,'p95_ms':round(p95,3)}
    retention=call('ops/retention');assert retention['analytical_deletion_enabled'] is False
    deleted=call('ops/retention/sessions',{});assert int(deleted['deleted_expired_sessions'])<=1000
    session=call('auth/session');assert call('auth/session',base=a.web)==session
    if a.restart:
        users_before=call('ops/users');audit_marker=call('ops/audit')[0]['event_id']
        subprocess.run(['docker','compose','-p',a.project,'--profile','*','restart','api','web','collector','prometheus','grafana'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        deadline=time.monotonic()+90
        while True:
            try:assert call('auth/session')==session;assert call('ops/status')['telemetry']['status']=='available';break
            except Exception:
                if time.monotonic()>deadline:raise
                time.sleep(1)
        assert users_before==call('ops/users')
        assert any(r['event_id']==audit_marker for r in call('ops/audit')),'audit lost on restart'
        assert lineage==call('ops/lineage');assert quality['finance']==call('ops/quality/check',{})['finance']
        assert stable=={k:call(k) for k in stable}
    report={'access':access,'accepted':True,'identity':lineage['identity'],'proof_digest':lineage['proof_digest'],'source_snapshots':lineage['output_snapshots'],'projection':lineage['projection'],'quality':quality,'probe':probe,'probe_alert_fired_and_cleared':True,'targets':[{'job':t['labels']['job'],'health':t['health']} for t in targets],'workloads':workloads,'quality_check_ms':round(quality_ms,3),'retention':retention,'restart_preserved':a.restart,'certified_finance':after['certified']['totals']}
    a.output.parent.mkdir(parents=True,exist_ok=True);a.output.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps({'accepted':True,'workloads':workloads,'quality_check_ms':round(quality_ms,3),'probe':'detected and acknowledged','restart':a.restart}))
if __name__=='__main__':main()
