"""Immutable, typed journey sidecars bound to an already certified release."""
from __future__ import annotations

import hashlib
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from marketpulse_batch.canonical import Columns, canonical_row, clickhouse_type, rowset_checksum
from marketpulse_batch.contracts import validate_digest
from marketpulse_batch.ledger import PublicationReservation
from .contracts import code, identifier

VERSION = "mp07-journeys-v1"
JOURNEY_COLUMNS: Columns = (
    ("event_id","string",False),("profile_ref","string",False),("session_id","string",False),
    ("click_id","string",True),("order_id","string",True),("campaign_id","string",True),
    ("event_name","string",False),("event_time","timestamp",False),("event_date","date",False),
    ("source","string",False),("currency","string",True),("gross_amount_minor","long",True),
    ("refund_amount_minor","long",True),("is_bot_session","boolean",False),
)
PUBLICATION_COLUMNS = ("identity","proof_digest","database_name","silver_snapshot_id","version","row_count","checksum")


def reference(identity: str, value: str) -> str:
    validate_digest(identity,"release identity")
    identifier(value)
    return hashlib.sha256((identity+"|"+value).encode()).hexdigest()


def project_event(row: Mapping[str, object], identity: str) -> dict[str, object]:
    if row.get("consent_analytics") is not True or (row.get("source") in {'web','mobile'} and row.get("is_bot_session") is not False):
        raise ValueError("journey projection requires governed consent and nonbot web/mobile rows")
    result={name:row[name] for name,_,_ in JOURNEY_COLUMNS if name!='profile_ref'}
    result['profile_ref']=reference(identity,row['profile_key'])
    for name in ('event_id','session_id','event_name','source'):
        identifier(result[name])
    for name in ('click_id','order_id','campaign_id'):
        if result[name] is not None:identifier(result[name])
    code(result['currency'],3)
    if result['event_time'].tzinfo is None or result['event_time'].astimezone(timezone.utc).date()!=result['event_date']:
        raise ValueError("journey event UTC timestamp/date mismatch")
    canonical_row(JOURNEY_COLUMNS,result)
    return result


@dataclass(frozen=True, slots=True)
class JourneyPublication:
    identity: str
    proof_digest: str
    database: str
    silver_snapshot_id: int
    version: str
    row_count: int
    checksum: str

    def __post_init__(self):
        for value in (self.identity,self.proof_digest,self.checksum):validate_digest(value,'journey digest')
        if re.fullmatch(rf'mp_journey_{self.identity}_[0-9a-f]{{32}}',self.database) is None:
            raise ValueError('unowned journey database')
        if self.version!=VERSION or type(self.silver_snapshot_id) is not int or self.silver_snapshot_id<1 or type(self.row_count) is not int or self.row_count<0:
            raise ValueError('invalid journey publication')

    @property
    def semantic_identity(self):
        return (self.identity,self.proof_digest,self.silver_snapshot_id,self.version,self.row_count,self.checksum)


class JourneyJournal:
    def __init__(self,client:Any,database:str='marketpulse_serving'):
        if database not in {'marketpulse_serving','mp07_journal_test'}:raise ValueError('invalid journal database')
        self.client,self.database=client,database

    def initialize(self):
        self.client.command(f'CREATE DATABASE IF NOT EXISTS {self.database}')
        self.client.command(f'CREATE TABLE IF NOT EXISTS {self.database}.journey_publications ('
            'identity String,proof_digest String,database_name String,silver_snapshot_id UInt64,version String,row_count UInt64,checksum String) '
            'ENGINE=MergeTree ORDER BY (identity,version)')

    def resolve(self,release:PublicationReservation) -> JourneyPublication | None:
        exists=self.client.query('SELECT count() FROM system.tables WHERE database={database:String} AND name=\'journey_publications\'',
            parameters={'database':self.database}).first_row[0]
        if not exists:return None
        rows=self.client.query(f'SELECT DISTINCT {",".join(PUBLICATION_COLUMNS)} FROM {self.database}.journey_publications '
            'WHERE identity={identity:String} AND version={version:String}',parameters={'identity':release.identity,'version':VERSION}).result_rows
        if not rows:return None
        publications=[JourneyPublication(*row) for row in rows]
        if len({row.semantic_identity for row in publications})!=1 or publications[0].proof_digest!=release.proof_digest:
            raise ValueError('journey publication content/release conflict')
        return min(publications,key=lambda row:row.database)

    def publish(self,publication:JourneyPublication) -> None:
        self.initialize()
        self.client.insert(f'{self.database}.journey_publications',[(publication.identity,publication.proof_digest,publication.database,
            publication.silver_snapshot_id,publication.version,publication.row_count,publication.checksum)],column_names=PUBLICATION_COLUMNS,
            settings={'async_insert':0})


def verify_projection(client:Any,publication:JourneyPublication) -> None:
    schema=client.query(f'DESCRIBE TABLE {publication.database}.events').result_rows
    observed=[(row[0],row[1].replace(' ','')) for row in schema]
    expected=[(name,clickhouse_type(kind,nullable).replace(' ','')) for name,kind,nullable in JOURNEY_COLUMNS]
    if observed!=expected:raise ValueError('journey serving schema mismatch')
    counts=client.query(f'SELECT count(),uniqExact(event_id) FROM {publication.database}.events').first_row
    if counts[0]!=counts[1]:raise ValueError('duplicate journey event identity')
    names=[name for name,_,_ in JOURNEY_COLUMNS]
    with client.query_rows_stream(f'SELECT * FROM {publication.database}.events',query_tz='UTC',tz_mode='aware') as rows:
        count,checksum=rowset_checksum(JOURNEY_COLUMNS,(dict(zip(names,row)) for row in rows))
    if (count,checksum)!=(publication.row_count,publication.checksum):raise ValueError('journey rowset checksum mismatch')


def copy_journeys(client:Any,release:PublicationReservation,snapshot_id:int,
                  rows:Iterable[Mapping[str,object]]) -> JourneyPublication:
    database=f'mp_journey_{release.identity}_{uuid.uuid4().hex}'
    client.command(f'CREATE DATABASE {database} ENGINE=Atomic')
    schema=','.join(f'{name} {clickhouse_type(kind,nullable)}' for name,kind,nullable in JOURNEY_COLUMNS)
    client.command(f'CREATE TABLE {database}.events ({schema}) ENGINE=MergeTree ORDER BY (profile_ref,event_time,event_id)')
    names=[name for name,_,_ in JOURNEY_COLUMNS]
    def copy_rows():
        batch=[]
        for row in rows:
            canonical_row(JOURNEY_COLUMNS,row)
            batch.append([row[name] for name in names])
            yield row
            if len(batch)>=2000:
                client.insert(f'{database}.events',batch,column_names=names,settings={'async_insert':0});batch=[]
        if batch:client.insert(f'{database}.events',batch,column_names=names,settings={'async_insert':0})
    count,checksum=rowset_checksum(JOURNEY_COLUMNS,copy_rows())
    publication=JourneyPublication(release.identity,release.proof_digest,database,snapshot_id,VERSION,count,checksum)
    verify_projection(client,publication)
    # Canonicalize globally BEFORE any thirty-day window. Whole tuples preserve nullable fields.
    expressions=','.join(f'tupleElement(touch,{index}) AS {name}' for index,(name,_,_) in enumerate(JOURNEY_COLUMNS,1))
    client.command(f'CREATE VIEW {database}.paid_touches AS SELECT {expressions} FROM ('
        f'SELECT argMin(tuple({",".join(names)}),tuple(event_time,campaign_id,event_id)) AS touch FROM {database}.events '
        "WHERE source IN ('web','mobile') AND NOT is_bot_session AND event_name='page_view' AND click_id IS NOT NULL AND campaign_id IS NOT NULL "
        'GROUP BY profile_ref,click_id)')
    return publication
