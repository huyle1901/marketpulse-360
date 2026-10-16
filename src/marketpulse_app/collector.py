"""Independent bounded background polls; HTTP scrapes only immutable cached samples."""
from __future__ import annotations
import hmac
import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from .metrics import Sample,render,process_samples

class Collector:
    def __init__(self,sources):self.sources=sources;self._cache={};self._lock=threading.Lock()
    def poll(self,name:str) -> None:
        now=time.time()
        try:
            samples=list(self.sources[name]());render(samples);success=True
        except Exception:samples=[];success=False
        with self._lock:self._cache[name]=(now,success,samples)
    def samples(self) -> list[Sample]:
        result=process_samples()
        with self._lock:
            for name in self.sources:
                observed,success,samples=self._cache.get(name,(0,False,[]));age=time.time()-observed
                healthy=success and age<(180 if name=='quality' else 60)
                result.extend([Sample('mp_source_up',int(healthy),(('source',name),)),Sample('mp_source_observed_timestamp_seconds',observed,(('source',name),))])
                if healthy:result.extend(samples)
        return result
    def render(self):return render(self.samples())
    def status(self):
        return [{'name':r.name,'value':str(r.value),'labels':dict(r.labels)} for r in self.samples()]
    def start(self):
        def loop(name):
            while True:self.poll(name);time.sleep(60 if name=='quality' else 5)
        for name in self.sources:threading.Thread(target=loop,args=(name,),daemon=True).start()

def main():
    from .settings import clients,load_settings
    from .auth_store import AuthStore
    from .operations import runtime_operations
    from .platform_metrics import kafka_samples,poll_flink,clickhouse_samples,tcp_sample
    analytics,store=clients(load_settings(os.environ));auth=AuthStore(store)
    ops=runtime_operations(analytics,store,auth)
    def quality():
        report=ops.store.record(ops.quality.check())
        return [Sample('mp_quality_status',int(report['status']==state),(('state',state),)) for state in ('passed','failed','unavailable')]+[Sample('mp_quality_checked_timestamp_seconds',time.time())]
    def probe():return [Sample('mp_quality_probe_active',int(ops.store.probe_active()))]
    collector=Collector({'kafka':kafka_samples,'flink':poll_flink,'clickhouse':lambda:clickhouse_samples(analytics),'quality':quality,'probe':probe,
        'app_postgres':lambda:tcp_sample('app_postgres','app-postgres',5432),'batch_postgres':lambda:tcp_sample('batch_postgres','batch-postgres',5432),'minio':lambda:tcp_sample('minio','minio',9000),'dagster':lambda:tcp_sample('dagster','dagster-webserver',3000)})
    token=Path(os.environ.get('METRICS_TOKEN_FILE','/run/secrets/metrics_token')).read_text().strip();collector.start()
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path=='/health':data=b'ok';status=200;kind='text/plain'
            elif self.path not in ('/metrics','/status'):data=b'not found';status=404;kind='text/plain'
            elif not token or not hmac.compare_digest(self.headers.get('Authorization',''),'Bearer '+token):data=b'unauthorized';status=401;kind='text/plain'
            else:
                data=(collector.render() if self.path=='/metrics' else json.dumps(collector.status())).encode();status=200;kind='text/plain; version=0.0.4' if self.path=='/metrics' else 'application/json'
            self.send_response(status);self.send_header('Content-Type',kind);self.end_headers();self.wfile.write(data)
        def log_message(self,*args):pass
    ThreadingHTTPServer(('0.0.0.0',9100),Handler).serve_forever()
if __name__=='__main__':main()
