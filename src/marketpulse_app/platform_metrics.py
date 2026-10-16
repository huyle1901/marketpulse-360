"""Read-only platform probes with bounded labels and explicit offset semantics."""
from __future__ import annotations
import json
import os
import socket
import urllib.request
from .metrics import Sample
TOPICS=('marketing.raw.web-events.v1','marketing.raw.order-events.v1','marketing.raw.email-events.v1','marketing.gold.campaign-metrics.v1','marketing.gold.funnel-metrics.v1','marketing.gold.revenue-metrics.v1','marketing.dlq.validation.v1','marketing.dlq.flink-validation.v1')
GROUPS=('marketpulse-campaign-serving-v1','marketpulse-flink-v1')

def committed_lag(end:int,offset:int) -> int|None:
    return max(0,end-offset) if offset>=0 else None

def kafka_samples() -> list[Sample]:
    from confluent_kafka import Consumer,TopicPartition
    result=[]
    for group in GROUPS:
        c=Consumer({'bootstrap.servers':os.environ.get('KAFKA_BOOTSTRAP_SERVERS','kafka:29092'),'group.id':group,'enable.auto.commit':False,'isolation.level':'read_committed','socket.timeout.ms':2000})
        try:
            metadata=c.list_topics(timeout=2);parts=[TopicPartition(t,p) for t in TOPICS if t in metadata.topics and not metadata.topics[t].error for p in metadata.topics[t].partitions]
            committed=c.committed(parts,timeout=2) if parts else []
            for part in committed:
                _,end=c.get_watermark_offsets(part,timeout=2,cached=False)
                labels=(('topic',part.topic),('partition',str(part.partition)))
                if group==GROUPS[0]:result.append(Sample('mp_kafka_log_end_offset',end,labels))
                lag=committed_lag(end,part.offset)
                # Only actual group/topic assignments have committed offsets; unknown is omitted.
                if lag is not None:result.append(Sample('mp_kafka_committed_lag',lag,labels+(('group',group),)))
        finally:c.close()
    if not result:raise OSError('no configured topic offsets')
    return result

def get_json(url:str):
    with urllib.request.urlopen(url,timeout=2) as r:return json.loads(r.read(2*1024*1024))

def flink_samples(overview:dict) -> list[Sample]:
    jobs=[j for j in overview.get('jobs',[]) if j.get('name')=='marketpulse-stream-v1']
    return [Sample('mp_flink_job_running',int(any(j.get('state')=='RUNNING' for j in jobs)))]

def poll_flink() -> list[Sample]:
    base=os.environ.get('FLINK_REST_URL','http://flink-jobmanager:8081')
    overview=get_json(base+'/jobs/overview');result=flink_samples(overview)
    job=next((j for j in overview.get('jobs',[]) if j.get('name')=='marketpulse-stream-v1' and j.get('state')=='RUNNING'),None)
    if job is None:return result
    job_id=job['jid'];checkpoints=get_json(base+'/jobs/'+job_id+'/checkpoints');counts=checkpoints.get('counts',{})
    for key in ('completed','failed','in_progress'):
        if key in counts:result.append(Sample('mp_flink_checkpoints',counts[key],(('kind',key),)))
    latest=checkpoints.get('latest',{}).get('completed')
    if latest:result.append(Sample('mp_flink_checkpoint_timestamp_seconds',latest['latest_ack_timestamp']/1000))
    counters=('accepted','duplicates','invalid','source_marked_late','watermark_late','aggregate_ineligible')
    details=get_json(base+'/jobs/'+job_id)
    for vertex in details.get('vertices',[])[:30]:
        url=base+'/jobs/'+job_id+'/vertices/'+vertex['id']+'/subtasks/metrics'
        available=get_json(url)
        for kind in counters:
            keys=[row['id'] for row in available if row['id'].split('.')[-1]==kind]
            for key in keys[:1]:
                rows=get_json(url+'?get='+urllib.parse.quote(key)+'&agg=sum')
                if rows and 'sum' in rows[0]:result.append(Sample('mp_flink_operator_events_total',float(rows[0]['sum']),(('kind',kind),)))
    taskmanagers=get_json(base+'/taskmanagers')
    for manager in taskmanagers.get('taskmanagers',[])[:4]:
        url=base+'/taskmanagers/'+manager['id']+'/metrics?get=Status.JVM.Memory.Heap.Used,Status.JVM.CPU.Load'
        for r in get_json(url):
            kind={'Status.JVM.Memory.Heap.Used':'heap_bytes','Status.JVM.CPU.Load':'cpu_load'}.get(r['id'])
            if kind:result.append(Sample('mp_flink_jvm',float(r['value']),(('kind',kind),)))
    return result

def clickhouse_samples(analytics) -> list[Sample]:
    session=analytics.pin();client=session.client
    result=[]
    memory=client.query("SELECT value FROM system.metrics WHERE metric='MemoryTracking'").first_row
    result.append(Sample('mp_clickhouse_tracked_memory_bytes',memory[0]))
    maximum=client.query(f'SELECT max(last_activity_at) FROM {session.release.database}.customer_360').first_row[0]
    if maximum:result.append(Sample('mp_business_event_timestamp_seconds',maximum.timestamp()))
    return result

def tcp_sample(name:str,host:str,port:int) -> list[Sample]:
    with socket.create_connection((host,port),timeout=2):pass
    return [Sample('mp_dependency_reachable',1,(('service',name),('semantics','tcp'),))]
