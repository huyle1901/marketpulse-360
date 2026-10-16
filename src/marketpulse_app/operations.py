"""Admin operations, immutable evidence and conservative session-only retention."""
class Operations:
    def __init__(self,quality,store,auth):self.quality=quality;self.store=store;self.auth=auth
    def status(self):
        reports=self.store.reports()
        return {'status':'available','quality':next((r for r in reports if r['scope']=='live'),None),'probe_active':self.store.probe_active(),'analytical_deletion_enabled':False}
    def retention(self):
        release=self.quality.analytics.pin().release
        client=self.quality.analytics.client
        releases=client.query("SELECT count() FROM system.databases WHERE name LIKE 'mp_release_%'").first_row[0]
        sidecars=client.query("SELECT count() FROM system.databases WHERE name LIKE 'mp_journey_%'").first_row[0]
        return {'session_ttl_hours':'8','cleanup_limit':'1000','expired_sessions':str(self.auth.expired_sessions()),'prometheus_retention_days':'7','analytical_deletion_enabled':False,'protected_releases':str(releases),'protected_sidecars':str(sidecars),'active_identity':release.identity,'protected':['users','audit','quality_reports','revisions','source_objects','snapshots','certified_releases','journey_projections']}

def register_operations_routes(app,ops):
    app.get('/api/v1/ops/status')(ops.status)
    app.get('/api/v1/ops/quality')(ops.store.reports)
    app.get('/api/v1/ops/lineage')(ops.quality.lineage)
    app.get('/api/v1/ops/audit')(ops.auth.audit_events)
    app.get('/api/v1/ops/retention')(ops.retention)
    @app.post('/api/v1/ops/quality/check')
    def check():return ops.store.record(ops.quality.check())
    @app.post('/api/v1/ops/quality/probe')
    def probe():return ops.store.record(ops.quality.probe())
    app.post('/api/v1/ops/quality/probe/clear')(ops.store.clear)
    @app.post('/api/v1/ops/retention/sessions')
    def cleanup():return {'deleted_expired_sessions':str(ops.auth.cleanup_sessions())}

def runtime_operations(analytics,store,auth):
    import os
    from pathlib import Path
    from urllib.parse import quote
    from marketpulse_batch.ledger import BatchLedger
    from .quality import QualityService
    from .ops_store import OpsStore
    password=os.environ.get('DAGSTER_POSTGRES_PASSWORD','')
    if not password:raise ValueError('batch operational credentials required')
    dsn='postgresql://'+quote(os.environ.get('DAGSTER_POSTGRES_USER','dagster'),safe='')+':'+quote(password,safe='')+'@batch-postgres:5432/'+quote(os.environ.get('DAGSTER_POSTGRES_DB','dagster'),safe='')
    auth.initialize()
    evidence=OpsStore(store);evidence.initialize()
    return Operations(QualityService(analytics,BatchLedger(dsn),Path('/opt/marketpulse/batch-state')),evidence,auth)
