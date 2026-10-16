"""Read-only certification checks and a bounded disposable corruption experiment."""
from __future__ import annotations
import json
import uuid
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlparse,unquote
from marketpulse_batch.canonical import rowset_checksum
from marketpulse_batch.certification import read_certification,verify_certification_current
from marketpulse_batch.export import ExportManifest,business_schema
from marketpulse_lakehouse.contracts import TABLE_CONTRACTS
from .journey_projection import JourneyJournal,verify_projection
from .analytics import wire
from .analysis import MONEY,MODELS

class QualityMismatch(ValueError):pass

def reconcile_finance(source:dict,performance:dict,models:list[dict]) -> None:
    if source['gross_revenue_minor']-source['refund_amount_minor']!=source['net_revenue_minor']:raise QualityMismatch('finance_equation')
    if any(performance[k]!=source[k] for k in MONEY):raise QualityMismatch('source_finance_mismatch')
    if len(models)!=3 or {r['model'] for r in models}!=set(MODELS):raise QualityMismatch('attribution_models')
    for r in models:
        if any(r[k]!=source[n] for n,k in zip(MONEY,('allocated_gross_minor','allocated_refund_minor','allocated_net_minor'))):raise QualityMismatch('attribution_finance_mismatch')

class QualityService:
    def __init__(self,analytics,ledger,state_root:Path):self.analytics=analytics;self.ledger=ledger;self.state_root=state_root
    def _path(self,uri:str) -> Path:
        parsed=urlparse(uri);path=Path(unquote(parsed.path)).resolve()
        if parsed.scheme!='file' or parsed.netloc or parsed.query or parsed.fragment or not path.is_relative_to(self.state_root.resolve()):raise ValueError('proof_path')
        return path
    def _context(self):
        session=self.analytics.pin();release=session.release
        receipt,_=self.ledger.get_receipt(release.identity);bundle=self.ledger.get_certified_bundle(release.identity)
        if bundle is None:raise OSError('no evidence')
        cert=read_certification(self._path(bundle.certification_uri))
        manifest=ExportManifest.from_payload(json.loads(self._path(bundle.export_manifest_uri).read_bytes()))
        if any(getattr(release,k)!=getattr(bundle,k) or getattr(release,k)!=getattr(cert,k) for k in ('identity','database','proof_digest')) or manifest.identity!=release.identity or manifest.digest!=cert.manifest_digest:raise QualityMismatch('release_binding')
        verify_certification_current(session.client,receipt,cert)
        projection=JourneyJournal(session.client).resolve(release)
        if projection is None:raise OSError('no projection')
        if projection.silver_snapshot_id!=receipt.output_snapshots.get('silver.events'):raise QualityMismatch('projection_snapshot')
        verify_projection(session.client,projection)
        return session,receipt,cert,manifest,projection
    @staticmethod
    def _rowsets(client,database:str,proof:dict) -> list[dict]:
        result=[]
        for relation in proof['relations']:
            name=relation['relation'];model=name.split('.')[1];columns=business_schema(name);names=[r[0] for r in columns]
            with client.query_rows_stream(f'SELECT * FROM {database}.{model}',query_tz='UTC',tz_mode='aware') as rows:
                count,digest=rowset_checksum(columns,(dict(zip(names,row)) for row in rows))
            keys=TABLE_CONTRACTS[name].key_fields
            actual=client.query(f'SELECT count(),uniqExact(tuple({",".join(keys)})) FROM {database}.{model}').first_row
            if actual[0]!=actual[1] or (count,digest)!=(relation['row_count'],relation['checksum']):raise QualityMismatch('certified_rowset_mismatch')
            result.append({'relation':name,'row_count':str(count),'checksum':digest})
        return result
    @staticmethod
    def _finance(client,database:str,projection) -> dict:
        source=client.query(f"SELECT sumIf(toInt128(ifNull(gross_amount_minor,0)),event_name='purchase'),sumIf(toInt128(ifNull(refund_amount_minor,0)),event_name='refund') FROM {projection.database}.events").first_row
        baseline=dict(zip(MONEY,(source[0],source[1],source[0]-source[1])))
        performance=client.query(f'SELECT sum(toInt128(gross_revenue_minor)),sum(toInt128(refund_amount_minor)),sum(toInt128(net_revenue_minor)) FROM {database}.campaign_performance_daily').first_row
        models=client.query(f'SELECT model,sum(toInt128(allocated_gross_minor)),sum(toInt128(allocated_refund_minor)),sum(toInt128(allocated_net_minor)) FROM {database}.attribution_daily GROUP BY model').result_rows
        reconcile_finance(baseline,dict(zip(MONEY,performance)),[dict(zip(('model','allocated_gross_minor','allocated_refund_minor','allocated_net_minor'),row)) for row in models])
        return wire(baseline)
    def lineage(self) -> dict:
        session,receipt,cert,manifest,p=self._context()
        return wire({'identity':receipt.identity,'proof_digest':cert.proof_digest,'model_digest':receipt.model_digest,'export_digest':manifest.digest,'dbt_invocation_id':cert.dbt_invocation_id,'source_snapshots':dict(receipt.bronze_snapshots),'output_snapshots':dict(receipt.output_snapshots),'projection':{'version':p.version,'snapshot_id':p.silver_snapshot_id,'row_count':p.row_count,'checksum':p.checksum}})
    def check(self,scope:str='live') -> dict:
        report={'scope':scope,'observed_at':datetime.now(timezone.utc).isoformat(),'status':'unavailable','reason':'dependency_or_evidence_unavailable'}
        try:
            session,receipt,cert,manifest,projection=self._context();report.update(identity=receipt.identity,proof_digest=cert.proof_digest)
            proof=json.loads(cert.proof_path.read_bytes())
            relations=self._rowsets(session.client,cert.database,proof)
            finance=self._finance(session.client,cert.database,projection)
            report.update(status='passed',reason='certified_rowsets_and_source_finance',relations=relations,finance=finance)
        except ValueError:report.update(status='failed',reason='certification_or_reconciliation_failed')
        except Exception:pass
        return report
    def probe(self) -> dict:
        report=self.check('probe')
        if report['status']!='passed':return report
        owned=None;client=None
        try:
            session,receipt,cert,manifest,projection=self._context();client=session.client;proof=json.loads(cert.proof_path.read_bytes())
            if sum(r['row_count'] for r in proof['relations'])>5000:raise ValueError('probe_limit')
            candidate='mp09_probe_'+uuid.uuid4().hex
            client.command(f'CREATE DATABASE {candidate} ENGINE=Atomic');owned=candidate
            for r in proof['relations']:
                model=r['relation'].split('.')[1]
                client.command(f'CREATE TABLE {owned}.{model} AS {cert.database}.{model} ENGINE=MergeTree ORDER BY tuple()')
                client.command(f'INSERT INTO {owned}.{model} SELECT * FROM {cert.database}.{model}')
            self._rowsets(client,owned,proof);self._finance(client,owned,projection)
            key=client.query(f'SELECT event_date,campaign_id,currency FROM {owned}.campaign_performance_daily LIMIT 1').first_row
            client.command(f'ALTER TABLE {owned}.campaign_performance_daily UPDATE gross_revenue_minor=gross_revenue_minor+1,net_revenue_minor=net_revenue_minor+1 WHERE event_date={{day:Date32}} AND campaign_id={{campaign:String}} AND currency={{currency:String}}',parameters={'day':key[0],'campaign':key[1],'currency':key[2]},settings={'mutations_sync':2})
            checks=[]
            for name,check in [('rowset',lambda:self._rowsets(client,owned,proof)),('finance',lambda:self._finance(client,owned,projection))]:
                try:check();checks.append((name,False))
                except QualityMismatch:checks.append((name,True))
            if all(value for _,value in checks):report.update(status='failed',reason='controlled_same_count_finance_corruption_detected',rowset_mismatch=True,source_finance_mismatch=True,delta_minor='1',row_count_unchanged=True)
            else:report.update(status='failed',reason='probe_detection_failed')
        except Exception:report.update(status='unavailable',reason='probe_dependency_or_limit')
        finally:
            if owned is not None:
                try:client.command(f'DROP DATABASE {owned} SYNC')
                except Exception:report.update(status='unavailable',reason='probe_cleanup_failed')
        return report
