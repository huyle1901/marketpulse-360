"""Observable Dagster graph with isolated backfills and certified publication."""

import json
import os
import subprocess
import uuid
from datetime import date, timedelta
from pathlib import Path

from dagster import (
    AssetExecutionContext, AssetIn, AssetKey, AssetOut, AssetSelection, BackfillPolicy, DailyPartitionsDefinition,
    DefaultScheduleStatus, Definitions, Field, Output, RunRequest, ScheduleDefinition,
    asset, multi_asset, resource, define_asset_job, in_process_executor,
)
from dagster_dbt import DbtCliResource, DagsterDbtTranslator, dbt_assets

from .settings import load_settings
from .resources import run_spark_operation, clickhouse_client
from .contracts import BatchSelection, PreparationReceipt
from .ledger import BatchLedger, UnknownPreparationReceipt
from .jobs import model_digest, rebuild_receipt, resolve_rebuild_receipts, local_receipt_path, ordered_asset_outputs, resolve_run_artifact
from .receipts import preparation_identity, read_receipt, write_receipt
from .export import ExportManifest, EXPORTED_RELATIONS
from .copying import load_candidate
from .certification import certify_candidate, read_certification, verify_certification_current
from .publication import publish_certified, resolve_active
from marketpulse_lakehouse.source_files import build_source_archive_plan, create_s3_client

DAILY = DailyPartitionsDefinition(start_date="2026-01-01", timezone="UTC")
DAILY_BACKFILL = BackfillPolicy.multi_run(max_partitions_per_run=1)
PROJECT = Path("/opt/marketpulse/dbt/marketpulse_serving")


@resource
def batch_settings(_):
    return load_settings(os.environ)


def _settings(context):
    return context.resources.batch_settings


def _ledger(context):
    ledger = BatchLedger(_settings(context).ledger_dsn)
    ledger.initialize()
    return ledger


def _directory(context):
    path = _settings(context).state_path / "dagster-runs" / context.run_id
    path.mkdir(parents=True, exist_ok=True)
    return path


def _run_artifact(context, name):
    def parent(run_id):
        run = context.run if run_id == context.run_id else context.instance.get_run_by_id(run_id)
        return run.parent_run_id if run else None
    return resolve_run_artifact(_settings(context).state_path, context.run_id, name, parent)


def _save(context, name, value):
    path = _directory(context) / f"{name}.json"
    path.write_text(json.dumps(value, sort_keys=True, default=str))
    return path.as_uri()


def _read(uri):
    return json.loads(local_receipt_path(uri).read_bytes())


def _rebuild(context):
    if context.job_name == "source_history_job":
        return False
    return context.job_name == "historical_rebuild_job" or any(
        key in context.run.tags for key in ("dagster/backfill", "dagster/backfill_id")
    )


def _selection(context):
    return BatchSelection(date.fromisoformat(context.partition_key),
                          tuple(os.environ.get("BATCH_REQUIRED_PLATFORMS", "google_ads,meta_ads,tiktok_ads").split(",")))


def _source(context, datasets, daily=True):
    if _rebuild(context):
        context.add_output_metadata({"mode": "recorded_snapshot_rebuild", "source_ingestion": "skipped"})
        return _save(context, context.asset_key.path[-1], {"skipped": True})
    settings = _settings(context)
    payload = {"run_id": context.run_id, "source_root": str(settings.source_root), "datasets": datasets}
    if daily:
        selection = _selection(context)
        payload["selection"] = {"logical_day": selection.logical_day.isoformat(), "required_platforms": selection.required_platforms}
    result = run_spark_operation(settings, "ingest_files", payload)
    context.add_output_metadata({key: value for key, value in result.items() if isinstance(value, (str, int))})
    return _save(context, context.asset_key.path[-1], result)


@asset(required_resource_keys={"batch_settings"})
def historical_file_manifest(context):
    if _rebuild(context):
        context.add_output_metadata({"mode": "receipt_only", "manifest_validation": "not_reloaded"})
        return str(_settings(context).source_root)
    plan = build_source_archive_plan(_settings(context).source_root)
    context.add_output_metadata({"manifest_sha256": plan.manifest_sha256, "eligible_files": len(plan.source_files),
                                 "input_root": str(_settings(context).source_root)})
    return str(_settings(context).source_root)


@asset(required_resource_keys={"batch_settings"})
def master_data(context, historical_file_manifest):
    return _source(context, ["master/ads", "master/campaigns", "master/customers", "master/products"], daily=False)


@asset(partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, required_resource_keys={"batch_settings"})
def ad_reports(context, historical_file_manifest):
    return _source(context, list(_selection(context).required_platforms))


@asset(partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, required_resource_keys={"batch_settings"})
def crm_snapshots(context, historical_file_manifest):
    return _source(context, ["crm"])


@asset(partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, required_resource_keys={"batch_settings"})
def bounded_kafka(context):
    if _rebuild(context):
        context.add_output_metadata({"mode": "recorded_snapshot_rebuild", "offset_capture": "skipped"})
        return _save(context, "bounded_kafka", {"skipped": True})
    result = run_spark_operation(_settings(context), "bounded_kafka",
                                 {"run_id": context.run_id, "bootstrap_servers": _settings(context).kafka_bootstrap_servers})
    context.add_output_metadata({key: value for key, value in result.items() if isinstance(value, (str, int))})
    return _save(context, "bounded_kafka", result)


@asset(partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, deps=[master_data, ad_reports, crm_snapshots, bounded_kafka],
       required_resource_keys={"batch_settings"},
       config_schema={"receipt_mapping": Field(dict, default_value={})})
def pinned_bronze(context):
    if _rebuild(context):
        receipt = resolve_rebuild_receipts((date.fromisoformat(context.partition_key),), context.op_config["receipt_mapping"])[0]
        uri = context.op_config["receipt_mapping"][context.partition_key]
        return _save(context, "pinned_bronze", {"mode": "rebuild", "receipt_uri": uri})
    selection = _selection(context)
    snapshots = run_spark_operation(_settings(context), "pin_bronze", {})["bronze_snapshots"]
    digest = model_digest()
    identity = preparation_identity(selection, snapshots, digest)
    context.add_output_metadata({"identity": identity, "snapshots": json.dumps(snapshots, sort_keys=True)})
    return _save(context, "pinned_bronze", {"mode": "refresh", "identity": identity, "model_digest": digest,
                                            "bronze_snapshots": snapshots, "logical_day": selection.logical_day.isoformat(),
                                            "required_platforms": selection.required_platforms})


@asset(partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, required_resource_keys={"batch_settings"})
def governed_silver(context, pinned_bronze):
    recipe = _read(pinned_bronze)
    if recipe["mode"] == "rebuild":
        rebuilt = rebuild_receipt(read_receipt(local_receipt_path(recipe["receipt_uri"])), settings=_settings(context))
        uri = (_settings(context).state_path / "preparations" / rebuilt.identity / rebuilt.namespace.rsplit("_", 1)[1] / "receipt.json").as_uri()
        return _save(context, "silver", {"receipt_uri": uri})
    ledger = _ledger(context)
    try:
        receipt, uri = ledger.get_receipt(recipe["identity"])
    except UnknownPreparationReceipt:
        pass
    else:
        if receipt != read_receipt(local_receipt_path(uri)):
            raise ValueError("persisted receipt differs from operational ledger")
        context.add_output_metadata({"preparation": "reused", "identity": receipt.identity})
        return _save(context, "silver", {"receipt_uri": uri})
    attempt = ledger.allocate_attempt(recipe["identity"], "preparation")
    namespace = f"mp_batch_{recipe['identity']}_{attempt}"
    silver = run_spark_operation(_settings(context), "silver", {"run_id": context.run_id, "namespace": namespace,
                                                               "bronze_snapshots": recipe["bronze_snapshots"]})
    context.add_output_metadata({"namespace": namespace, "counts": json.dumps(silver["counts"], sort_keys=True)})
    return _save(context, "silver", recipe | {"namespace": namespace, "attempt": attempt,
                                              "silver_snapshots": {f"silver.{name}": value for name, value in silver["snapshots"].items()}})


@asset(partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, required_resource_keys={"batch_settings"})
def marketing_gold(context, governed_silver):
    stage = _read(governed_silver)
    if "receipt_uri" in stage:
        return stage["receipt_uri"]
    gold = run_spark_operation(_settings(context), "gold", stage | {"run_id": context.run_id})
    receipt = PreparationReceipt(stage["identity"], date.fromisoformat(stage["logical_day"]), stage["model_digest"],
                                 stage["bronze_snapshots"], gold["output_snapshots"], stage["namespace"],
                                 gold["proof_digest"], tuple(stage["required_platforms"]))
    directory = _settings(context).state_path / "preparations" / receipt.identity / str(stage["attempt"])
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "receipt.json"
    write_receipt(receipt, path)
    (directory / "proof.json").write_text(json.dumps(gold["proof"], sort_keys=True, separators=(",", ":")))
    _ledger(context).record_receipt(receipt, path.as_uri())
    context.add_output_metadata({"identity": receipt.identity, "proof_digest": receipt.proof_digest,
                                 "output_snapshots": json.dumps(dict(receipt.output_snapshots), sort_keys=True)})
    return path.as_uri()


@asset(partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, required_resource_keys={"batch_settings"})
def snapshot_export(context, marketing_gold):
    receipt = read_receipt(local_receipt_path(marketing_gold))
    bundle = _ledger(context).get_certified_bundle(receipt.identity) if not _rebuild(context) else None
    if bundle:
        context.add_output_metadata({"export": "reused_certified", "database": bundle.database})
        return _save(context, "export", {"receipt_uri": marketing_gold, "manifest_uri": bundle.export_manifest_uri})
    manifest_path = _directory(context) / "export_manifest.json"
    if not manifest_path.exists():
        payload = run_spark_operation(_settings(context), "export", {"receipt_path": str(local_receipt_path(marketing_gold)),
                                      "output_prefix": f"s3://marketpulse-lakehouse/batch-exports/{receipt.identity}/{uuid.uuid4().hex}"})
        manifest_path.write_text(json.dumps(payload, sort_keys=True))
    manifest = ExportManifest.from_payload(json.loads(manifest_path.read_bytes()))
    if manifest.identity != receipt.identity:
        raise ValueError("cached export belongs to a different preparation")
    context.add_output_metadata({"export_digest": manifest.digest, "relations": len(manifest.relations)})
    return _save(context, "export", {"receipt_uri": marketing_gold, "manifest_uri": manifest_path.as_uri()})


RAW_OUTS = {f"raw_{name.split('.')[1]}": AssetOut(key=AssetKey(["pinned_gold", f"raw_{name.split('.')[1]}"])) for name in EXPORTED_RELATIONS}


@multi_asset(outs=RAW_OUTS, partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, required_resource_keys={"batch_settings"})
def clickhouse_raw_candidates(context, snapshot_export):
    path = _directory(context) / "candidate.json"
    if not path.exists():
        exported = _read(snapshot_export)
        receipt = read_receipt(local_receipt_path(exported["receipt_uri"]))
        manifest = ExportManifest.from_payload(_read(exported["manifest_uri"]))
        bundle = _ledger(context).get_certified_bundle(receipt.identity) if not _rebuild(context) else None
        if bundle:
            _save(context, "candidate", exported | {"database": bundle.database, "certification_uri": bundle.certification_uri})
        else:
            client = clickhouse_client(_settings(context))
            try:
                database = load_candidate(client, manifest, attempt=_ledger(context).allocate_attempt(receipt.identity, "copy"), s3=create_s3_client())
            finally:
                client.close()
            _save(context, "candidate", exported | {"database": database})
    for name in RAW_OUTS:
        yield Output(path.as_uri(), output_name=name, metadata={"database": json.loads(path.read_bytes())["database"]})


class StableTranslator(DagsterDbtTranslator):
    def get_asset_key(self, props):
        return AssetKey(["pinned_gold", props["name"]]) if props["resource_type"] == "source" else AssetKey([props["name"]])


def _manifest():
    settings = load_settings(os.environ)
    os.environ["DBT_ENV_SECRET_CLICKHOUSE_PASSWORD"] = settings.clickhouse_password
    os.environ.setdefault("BATCH_CANDIDATE_DATABASE", "mp_release_" + "0" * 64 + "_1")
    os.environ["DBT_SEND_ANONYMOUS_USAGE_STATS"] = "false"
    directory = settings.state_path / "definition-manifest" / model_digest()
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "manifest.json"
    if not path.exists():
        result = subprocess.run(["dbt", "parse", "--project-dir", str(PROJECT), "--profiles-dir", str(PROJECT),
                                 "--target-path", str(directory), "--log-path", str(directory / "logs")],
                                capture_output=True, text=True, timeout=120)
        if result.returncode:
            raise ValueError("dbt runtime manifest generation failed")
    return path


@dbt_assets(manifest=_manifest(), partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, dagster_dbt_translator=StableTranslator(),
            required_resource_keys={"batch_settings", "dbt"})
def marketing_dbt(context: AssetExecutionContext):
    candidate_path = _run_artifact(context, "candidate")
    candidate = json.loads(candidate_path.read_bytes())
    if "certification_uri" not in candidate:
        receipt = read_receipt(local_receipt_path(candidate["receipt_uri"]))
        bundle = _ledger(context).get_certified_bundle(receipt.identity)
        if bundle and bundle.database == candidate["database"]:
            candidate["certification_uri"] = bundle.certification_uri
    result_path = candidate_path.parent / "dbt_result.json"
    if "certification_uri" in candidate:
        cert = read_certification(local_receipt_path(candidate["certification_uri"]))
        keys = {name: key.to_user_string() for name, key in marketing_dbt.keys_by_output_name.items()}
        dependencies = {key.to_user_string(): {dep.to_user_string() for dep in deps} for key, deps in marketing_dbt.asset_deps.items()}
        for output_name in ordered_asset_outputs(keys, dependencies):
            yield Output(None, output_name=output_name, metadata={"dbt_execution": "reused_certified_invocation", "dbt_invocation_id": cert.dbt_invocation_id})
        result_path.write_text(json.dumps({"target_path": str(cert.proof_path.parent)}))
        return
    os.environ["BATCH_CANDIDATE_DATABASE"] = candidate["database"]
    target = _directory(context) / f"dbt-{uuid.uuid4().hex}"
    yield from context.resources.dbt.cli(["build"], context=context, target_path=target).stream()
    result_path.write_text(json.dumps({"target_path": str(target)}))


@asset(partitions_def=DAILY, backfill_policy=DAILY_BACKFILL,
       ins={"candidate_uri": AssetIn(key=AssetKey(["pinned_gold", "raw_campaign_performance_daily"]))},
       deps=[AssetKey(name) for name in
       ("funnel_daily", "attribution_daily", "customer_360", "cohort_retention", "campaign_performance_daily", "campaigns", "customers", "campaign_dashboard")],
       required_resource_keys={"batch_settings"})
def certified_release(context, candidate_uri):
    # Partition IO can point to a later materialization. Retry ancestry pins this execution.
    candidate_path = _run_artifact(context, "candidate")
    candidate = json.loads(candidate_path.read_bytes())
    target = Path(json.loads((candidate_path.parent / "dbt_result.json").read_bytes())["target_path"])
    receipt = read_receipt(local_receipt_path(candidate["receipt_uri"]))
    manifest = ExportManifest.from_payload(_read(candidate["manifest_uri"]))
    client = clickhouse_client(_settings(context))
    try:
        if "certification_uri" in candidate:
            cert = read_certification(local_receipt_path(candidate["certification_uri"]))
            verify_certification_current(client, receipt, cert)
        else:
            cert = certify_candidate(client, receipt, manifest, candidate["database"], target)
            if not _rebuild(context):
                _ledger(context).record_certified_bundle(receipt.identity, cert.database, cert.proof_digest,
                                                         candidate["manifest_uri"], cert.proof_path.as_uri())
    finally:
        client.close()
    candidate_path.write_text(json.dumps(candidate | {"certification_uri": cert.proof_path.as_uri()}, sort_keys=True))
    context.add_output_metadata({"proof_digest": cert.proof_digest, "database": cert.database, "checked_relations": len(cert.checked_relations)})
    return cert.proof_path.as_uri()


@asset(partitions_def=DAILY, backfill_policy=DAILY_BACKFILL, required_resource_keys={"batch_settings"})
def active_release(context, certified_release):
    if _rebuild(context):
        context.add_output_metadata({"publication": "skipped_historical_rebuild"})
        return certified_release
    ledger = _ledger(context)
    candidate = json.loads(_run_artifact(context, "candidate").read_bytes())
    receipt = read_receipt(local_receipt_path(candidate["receipt_uri"]))
    cert_uri = candidate.get("certification_uri")
    if cert_uri is None:
        bundle = ledger.get_certified_bundle(receipt.identity)
        if bundle is None or bundle.database != candidate["database"]:
            raise ValueError("retry candidate has no matching persisted certification")
        cert_uri = bundle.certification_uri
    cert = read_certification(local_receipt_path(cert_uri))
    if cert.identity != receipt.identity or cert.database != candidate["database"]:
        raise ValueError("retry certification differs from pinned candidate")
    receipt, _ = ledger.get_receipt(cert.identity)
    client = clickhouse_client(_settings(context))
    try:
        published = publish_certified(client, ledger, receipt, cert)
        current = resolve_active(client)
    finally:
        client.close()
    context.add_output_metadata({"published_generation": published.generation, "active_generation": current.generation,
                                 "database": current.database})
    return _save(context, "publication", {"generation": current.generation, "identity": current.identity,
                                         "database": current.database, "proof_digest": current.proof_digest})


ALL_ASSETS = [historical_file_manifest, master_data, ad_reports, crm_snapshots, bounded_kafka, pinned_bronze,
              governed_silver, marketing_gold, snapshot_export, clickhouse_raw_candidates, marketing_dbt,
              certified_release, active_release]
refresh_job = define_asset_job("marketing_refresh_job", selection=AssetSelection.all(), executor_def=in_process_executor)
historical_job = define_asset_job("historical_rebuild_job", selection=AssetSelection.all() - AssetSelection.assets(active_release), executor_def=in_process_executor)
source_job = define_asset_job("source_history_job", selection=AssetSelection.assets(marketing_gold).upstream(), executor_def=in_process_executor)


def _daily_request(context):
    day = (context.scheduled_execution_time.date() - timedelta(days=1)).isoformat()
    return RunRequest(partition_key=day, run_key=f"marketing-{day}")


daily_schedule = ScheduleDefinition(name="daily_marketing_refresh", job=refresh_job, cron_schedule="0 2 * * *",
                                    execution_timezone="UTC", default_status=DefaultScheduleStatus.STOPPED,
                                    execution_fn=_daily_request)
defs = Definitions(assets=ALL_ASSETS, jobs=[refresh_job, historical_job, source_job], schedules=[daily_schedule],
                   resources={"batch_settings": batch_settings, "dbt": DbtCliResource(project_dir=PROJECT, profiles_dir=PROJECT)})
