"""Identical SQL workload on isolated Gold-shaped facts; no application database."""
from __future__ import annotations
import time
import uuid
from pathlib import Path
from typing import Any
from .analytics import ANALYTICS_SCALES, WORKLOADS, analytical_oracle
from .contracts import percentiles
from .resources import ResourceSampler
from .runner import ROOT, safe_metadata


def query_sql(table: str, workload: str) -> str:
    if workload == 'platform':
        return f'SELECT platform,COUNT(*),SUM(net_minor) FROM {table} GROUP BY platform ORDER BY platform'
    where = ' WHERE campaign=7 AND event_day>=5 AND event_day<20' if workload == 'filtered' else ''
    return f'SELECT campaign,event_day,COUNT(*),SUM(net_minor) FROM {table}{where} GROUP BY campaign,event_day ORDER BY campaign,event_day'


def run_analytics(engine: str, scale: str, *, warmups: int = 20, samples: int = 100) -> dict[str, Any]:
    if engine not in ('clickhouse', 'postgres') or scale not in ANALYTICS_SCALES or warmups < 0 or samples < 1:
        raise ValueError('invalid analytical benchmark configuration')
    secret = (ROOT / '.secrets/evaluation_password').read_text().strip()
    count = ANALYTICS_SCALES[scale]; name = 'eval_' + uuid.uuid4().hex[:16]
    client = None; created = False
    started = time.perf_counter()
    if engine == 'clickhouse':
        import clickhouse_connect
        client = clickhouse_connect.get_client(host='localhost',port=49323,username='evaluation',password=secret,
            settings={'use_query_cache':0,'max_threads':2},send_receive_timeout=300)
        client.command('CREATE DATABASE IF NOT EXISTS evaluation')
        table = 'evaluation.' + name
        execute = client.command
        def query(sql: str) -> list[tuple]: return [tuple(int(v) for v in row) for row in client.query(sql).result_rows]
        version = client.query('SELECT version()').first_row[0]
        ddl = f'CREATE TABLE {table} (row_id Int64,campaign Int32,event_day Int32,platform Int32,net_minor Int64) ENGINE=MergeTree ORDER BY (campaign,event_day,row_id)'
        load = f'INSERT INTO {table} SELECT number,number%64,number%30,number%3,((number%97)+1)*100 FROM numbers({count})'
    else:
        import psycopg
        client = psycopg.connect(host='localhost',port=49332,user='evaluation',password=secret,dbname='evaluation',autocommit=True)
        client.execute('SET max_parallel_workers_per_gather=1')
        table = name
        execute = client.execute
        def query(sql: str) -> list[tuple]: return [tuple(int(v) for v in row) for row in client.execute(sql).fetchall()]
        version = client.execute('SELECT version()').fetchone()[0]
        ddl = f'CREATE TABLE {table} (row_id bigint,campaign integer,event_day integer,platform integer,net_minor bigint)'
        load = f'INSERT INTO {table} SELECT i,i%64,i%30,i%3,((i%97)+1)*100 FROM generate_series(0,{count-1}) i'
    try:
        execute(ddl); created = True; execute(load)
        if engine == 'postgres':
            execute(f'CREATE INDEX ON {table} (campaign,event_day)'); execute(f'ANALYZE {table}')
        load_seconds = time.perf_counter() - started
        if query(f'SELECT COUNT(*) FROM {table}')[0][0] != count: raise ValueError('wrong loaded row count')
        workloads = {}
        with ResourceSampler(['marketpulse-evaluation-' + engine + '-1']) as resources:
            for workload in WORKLOADS:
                expected = analytical_oracle(count,workload); sql = query_sql(table,workload)
                if query(sql) != expected: raise ValueError('independent analytical answer mismatch')
                for _ in range(warmups):
                    if query(sql) != expected: raise ValueError('warmup answer mismatch')
                timings = []
                for _ in range(samples):
                    start = time.perf_counter(); answer = query(sql); elapsed = (time.perf_counter() - start) * 1000
                    if answer != expected: raise ValueError('measured analytical answer mismatch')
                    timings.append(elapsed)
                workloads[workload] = {**percentiles(timings),'warmups':warmups,'failures':0,'result_rows':len(expected),'sql':sql}
        return {'version':1,'kind':'analytics','engine':engine,'scale':scale,'row_count':count,
                'correctness':{'passed':True,'oracle':'independent periodic integer aggregation'},'versions':{'engine':version},
                'ddl':ddl,'postgres_index':'campaign,event_day' if engine=='postgres' else None,
                'load_seconds':load_seconds,'workloads':workloads,'resources':resources.result(),
                'cache':'warm OS/buffer; ClickHouse result cache disabled','timing_scope':'serial client request and decode',
                'percentile_method':'nearest_rank','metadata':safe_metadata()}
    finally:
        if created: execute(f'DROP TABLE {table}')
        if client is not None: client.close()
