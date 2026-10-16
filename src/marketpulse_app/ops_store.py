"""Append-only quality evidence; independent mutable probe acknowledgement."""
import json
from importlib.resources import files
class OpsStore:
    def __init__(self,store):self.store=store
    def initialize(self):
        with self.store.connect() as c:
            c.execute('SELECT pg_advisory_xact_lock(360092)')
            c.execute(files('marketpulse_app').joinpath('sql/009_operations.sql').read_text())
    def record(self,report):
        with self.store.connect() as c:
            c.execute('INSERT INTO marketpulse_app.quality_reports(report) VALUES(%s::jsonb)',(json.dumps(report),))
            if report.get('reason')=='controlled_same_count_finance_corruption_detected':c.execute('UPDATE marketpulse_app.ops_state SET probe_active=true')
        return report
    def reports(self):
        with self.store.connect() as c:return [r[0]|{'report_id':str(r[1])} for r in c.execute('SELECT report,report_id FROM marketpulse_app.quality_reports ORDER BY report_id DESC LIMIT 50').fetchall()]
    def probe_active(self):
        with self.store.connect() as c:return c.execute('SELECT probe_active FROM marketpulse_app.ops_state').fetchone()[0]
    def clear(self):
        with self.store.connect() as c:c.execute('UPDATE marketpulse_app.ops_state SET probe_active=false')
        return {'status':'acknowledged'}
