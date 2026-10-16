"""Transactional operator-authored segments; revisions contain no member data."""
from __future__ import annotations
import json
from importlib.resources import files
import uuid
from .contracts import integer
from .store import AppStore,VersionConflict
from .lifecycle_contracts import SegmentRule
from .lifecycle import LifecycleError

COLUMNS='segment_id,version,name,rule,rule_digest,created_at'


def segment_uuid(value:str) -> str:
    try:result=str(uuid.UUID(value))
    except (ValueError,AttributeError,TypeError) as error:raise ValueError('invalid segment ID') from error
    if result!=value:raise ValueError('canonical UUID required')
    return result


class SegmentStore:
    def __init__(self,store:AppStore):self.store=store

    def initialize(self) -> None:
        with self.store.connect() as connection:
            connection.execute('SELECT pg_advisory_xact_lock(36008)')
            connection.execute(files('marketpulse_app').joinpath('sql/segments.sql').read_text())

    @staticmethod
    def _record(row:tuple) -> dict[str,object]:
        rule=SegmentRule.from_payload(row[3])
        if rule.digest!=row[4]:raise LifecycleError('segment_revision_mismatch')
        return {'segment_id':str(row[0]),'version':str(row[1]),'name':row[2],'rule':rule.payload(),'rule_digest':row[4],'created_at':row[5].isoformat()}

    def list(self) -> list[dict[str,object]]:
        with self.store.connect() as connection:
            columns=','.join('r.'+column for column in COLUMNS.split(','))
            rows=connection.execute(f'SELECT {columns} FROM marketpulse_app.segments s '
                'JOIN marketpulse_app.segment_revisions r ON s.segment_id=r.segment_id AND s.version=r.version ORDER BY r.name,s.segment_id').fetchall()
            return [self._record(row) for row in rows]

    def revision(self,segment_id:str,version:int) -> dict[str,object]|None:
        segment_uuid(segment_id);integer(version,1)
        with self.store.connect() as connection:
            row=connection.execute(f'SELECT {COLUMNS} FROM marketpulse_app.segment_revisions WHERE segment_id=%s AND version=%s',(segment_id,version)).fetchone()
            return self._record(row) if row else None

    def save(self,segment_id:str,name:str,rule:SegmentRule,expected_version:int) -> dict[str,object]:
        segment_uuid(segment_id);integer(expected_version)
        if not isinstance(name,str) or not 1<=len(name.strip())<=80:raise ValueError('requires segment name1..80')
        if not isinstance(rule,SegmentRule):raise ValueError('requires validated segment rule')
        with self.store.connect() as connection:
            if expected_version==0:
                row=connection.execute('INSERT INTO marketpulse_app.segments(segment_id,version) VALUES(%s,1) '
                    'ON CONFLICT DO NOTHING RETURNING version',(segment_id,)).fetchone()
            else:
                row=connection.execute('UPDATE marketpulse_app.segments SET version=version+1,updated_at=now() '
                    'WHERE segment_id=%s AND version=%s RETURNING version',(segment_id,expected_version)).fetchone()
            if row is None:raise VersionConflict('stale segment revision')
            revision=connection.execute(f'INSERT INTO marketpulse_app.segment_revisions(segment_id,version,name,rule,rule_digest) '
                f'VALUES(%s,%s,%s,%s::jsonb,%s) RETURNING {COLUMNS}',
                (segment_id,row[0],name.strip(),json.dumps(rule.payload(),sort_keys=True),rule.digest)).fetchone()
            return self._record(revision)
