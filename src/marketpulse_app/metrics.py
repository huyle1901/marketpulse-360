"""Small fixed-cardinality Prometheus exposition; never certified money transport."""
from __future__ import annotations
import json
import math
import os
import resource
import re
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass
from .security import ROUTES

@dataclass(frozen=True)
class Sample:
    name:str
    value:float
    labels:tuple[tuple[str,str],...]=()

def render(samples:list[Sample]) -> str:
    lines=[];identities=set()
    for sample in samples:
        if not isinstance(sample,Sample) or re.fullmatch(r'[a-zA-Z_:][a-zA-Z0-9_:]*',sample.name) is None:
            raise ValueError('invalid metric sample')
        if type(sample.value) not in (int,float) or not math.isfinite(sample.value):raise ValueError('nonfinite metric')
        keys=set()
        for key,value in sample.labels:
            if re.fullmatch(r'[a-zA-Z_][a-zA-Z0-9_]*',key) is None or key in keys or not isinstance(value,str):raise ValueError('invalid metric label')
            keys.add(key)
        identity=(sample.name,tuple(sorted(sample.labels)))
        if identity in identities:raise ValueError('duplicate metric identity')
        identities.add(identity)
        labels='{'+','.join(k+'='+json.dumps(v,ensure_ascii=True) for k,v in sample.labels)+'}' if sample.labels else ''
        lines.append(f'{sample.name}{labels} {sample.value}')
    return '\n'.join(lines)+'\n'

def process_samples() -> list[Sample]:
    usage=resource.getrusage(resource.RUSAGE_SELF)
    # Linux proc gives current RSS; fallback explicitly exposes peak RSS separately.
    result=[Sample('mp_process_cpu_seconds_total',usage.ru_utime+usage.ru_stime)]
    try:
        with open('/proc/self/statm') as stream:pages=int(stream.read().split()[1])
        result.append(Sample('mp_process_resident_bytes',pages*os.sysconf('SC_PAGE_SIZE')))
    except OSError:result.append(Sample('mp_process_peak_resident_bytes',usage.ru_maxrss*(1 if sys.platform=='darwin' else 1024)))
    return result

class Metrics:
    BUCKETS=(.01,.025,.05,.1,.25,.5,1,2.5,5,10,float('inf'))
    def __init__(self):self._lock=threading.Lock();self.rows=defaultdict(lambda:[0,0.,[0]*11])
    def observe(self,route:str,method:str,status:int,seconds:float) -> None:
        known={'/api/v1/'+r[3] for r in ROUTES}|{'/api/v1/auth/login','/metrics','docs'}
        route=route if route in known else 'unknown';method=method if method in ('GET','POST','PUT','DELETE','PATCH','HEAD','OPTIONS') else 'OTHER'
        key=(route,method,str(status//100)+'xx')
        with self._lock:
            r=self.rows[key];r[0]+=1;r[1]+=seconds
            for i,b in enumerate(self.BUCKETS):r[2][i]+=int(seconds<=b)
    def render(self) -> str:
        samples=process_samples()
        with self._lock:
            for (route,method,status),(count,total,buckets) in self.rows.items():
                labels=(('route',route),('method',method),('status',status))
                samples.extend([Sample('mp_api_requests_total',count,labels),Sample('mp_api_request_seconds_count',count,labels),Sample('mp_api_request_seconds_sum',total,labels)])
                for boundary,value in zip(self.BUCKETS,buckets):samples.append(Sample('mp_api_request_seconds_bucket',value,labels+(('le','+Inf' if math.isinf(boundary) else str(boundary)),)))
        return render(samples)
