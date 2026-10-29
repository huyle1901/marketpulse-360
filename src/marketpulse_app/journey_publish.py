"""Explicit one-shot publisher: pinned governed Silver -> validated journey sidecar."""
from __future__ import annotations

import json
import os
from datetime import date,timezone
from dataclasses import asdict

from marketpulse_batch.ledger import BatchLedger
from marketpulse_batch.publication import resolve_active
from marketpulse_batch.resources import clickhouse_client
from marketpulse_batch.settings import load_settings
from marketpulse_lakehouse.session import create_spark_session
from marketpulse_lakehouse.verify import validate_physical_schema
from marketpulse_app.journey_projection import JourneyJournal, copy_journeys, project_event, verify_projection



def release_events_query(namespace:str,snapshot:int,through:date)->str:
    """Governed Silver events of the certified release: its pinned snapshot, on or before the release day."""
    from marketpulse_lakehouse.silver import validate_namespace
    namespace=validate_namespace(namespace)
    if type(snapshot) is not int or snapshot<1 or type(through) is not date:raise ValueError('invalid release event selection')
    return f"SELECT * FROM lakehouse.`{namespace}`.customer_events VERSION AS OF {snapshot} WHERE event_date <= DATE '{through.isoformat()}'"

def main() -> int:
    import psycopg
    settings=load_settings(os.environ)
    client=clickhouse_client(settings)
    spark=None
    try:
        release=resolve_active(client)
        ledger=BatchLedger(settings.ledger_dsn)
        receipt,_=ledger.get_receipt(release.identity)
        bundle=ledger.get_certified_bundle(release.identity)
        if bundle is None or (bundle.database,bundle.proof_digest)!=(release.database,release.proof_digest):
            raise ValueError('journey publisher requires certified release/receipt binding')
        snapshot=receipt.output_snapshots['silver.customer_events']
        with psycopg.connect(settings.ledger_dsn,autocommit=True) as connection:
            connection.execute('SELECT pg_advisory_lock(36007)')
            journal=JourneyJournal(client)
            existing=journal.resolve(release)
            if existing:
                if existing.silver_snapshot_id!=snapshot:raise ValueError('journey Silver snapshot mismatch')
                verify_projection(client,existing)
                print(json.dumps({'reused':True,**asdict(existing)},sort_keys=True));return 0
            spark=create_spark_session('marketpulse-mp07-journeys')
            frame=spark.sql(release_events_query(receipt.namespace,snapshot,receipt.logical_day))
            validate_physical_schema(frame.schema,'silver.customer_events')
            def rows():
                for item in frame.toLocalIterator():
                    row=item.asDict()
                    # Spark's configured UTC session returns naive Python datetimes.
                    row['event_time']=row['event_time'].replace(tzinfo=timezone.utc)
                    yield project_event(row,release.identity)
            publication=copy_journeys(client,release,snapshot,rows())
            journal.publish(publication)
            print(json.dumps({'reused':False,**asdict(publication)},sort_keys=True))
        return 0
    finally:
        if spark is not None:spark.stop()
        client.close()


if __name__=='__main__':
    raise SystemExit(main())
