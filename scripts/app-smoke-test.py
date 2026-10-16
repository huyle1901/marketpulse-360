#!/usr/bin/env python3
"""MP-06 proof against an explicitly selected test-owned existing Compose project.

Never seeds/rebuilds MP-01–05. Creates demo app settings/rules by HTTP and restarts
only the four new app services. Evidence contains no credentials/source PII.
"""
from __future__ import annotations

import argparse
import json
import math
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path


def percentile(values: list[float], fraction: float) -> float:
    return sorted(values)[math.ceil(len(values)*fraction)-1]


def assert_projection(expected: dict, observed: list[dict]) -> None:
    assert len(observed) == len(expected), "live keyed row count differs from independent Kafka oracle"
    assert {row["metric_key"] for row in observed} == set(expected), "live metric keys differ or repeat"
    for row in observed:
        oracle = expected[row["metric_key"]]
        assert row["partition"] == oracle["partition"] and row["offset"] == oracle["offset"], "wrong latest source position"
        for field, value in oracle["payload"].items():
            assert row[field] == value, f"live payload differs at {field}"


KAFKA_ORACLE = r'''
import json,time,uuid,os
from datetime import datetime,timezone,timedelta
from confluent_kafka import Consumer,TopicPartition,KafkaError,TopicCollection
from confluent_kafka.admin import AdminClient
from marketpulse_app.settings import clients,load_settings
from marketpulse_app.live_store import LiveStore
from marketpulse_app.streaming import deliver
topic="marketing.gold.campaign-metrics.v1"
c=Consumer({"bootstrap.servers":"kafka:29092","group.id":"mp06-proof-"+uuid.uuid4().hex,
    "isolation.level":"read_committed","enable.auto.commit":False,"enable.partition.eof":True})
metadata=c.list_topics(topic,timeout=10)
admin=AdminClient({"bootstrap.servers":"kafka:29092"})
source_identity=str(admin.describe_cluster().result(10).cluster_id)+"/"+str(admin.describe_topics(TopicCollection([topic]))[topic].result(10).topic_id)
partitions=sorted(metadata.topics[topic].partitions)
c.assign([TopicPartition(topic,p,c.get_watermark_offsets(TopicPartition(topic,p),timeout=10)[0]) for p in partitions])
eof=set(); latest={}; count=0; sample=None; deadline=time.monotonic()+60
while len(eof)<len(partitions) and time.monotonic()<deadline:
    m=c.poll(1)
    if m is None:continue
    if m.error():
        assert m.error().code()==KafkaError._PARTITION_EOF,"Kafka oracle consumption error"
        eof.add(m.partition());continue
    count+=1; value=json.loads(m.value()); key=m.key().decode()
    prior=latest.get(key)
    if prior is None or m.offset()>int(prior["offset"]):
        latest[key]={"partition":str(m.partition()),"offset":str(m.offset()),
                    "payload":{k:str(v) if type(v) is int else v for k,v in value.items()}}
    sample=sample or m
assert len(eof)==len(partitions) and sample is not None,"Kafka oracle did not finish/nonempty output required"
analytics,_=clients(load_settings(os.environ)); client=analytics.client
# Isolated serving fault proof: only the literal test-owned relation is reset.
client.command("DROP DATABASE IF EXISTS mp06_live_test")
store=LiveStore(client,database="mp06_live_test");store.initialize("mp06-fault-proof")
class LostAck:
    def commit(self,**kwargs):raise RuntimeError("injected lost offset acknowledgement")
now=datetime.now(timezone.utc); transport_start=time.perf_counter()
try:deliver(sample,store,LostAck(),now)
except RuntimeError:pass
before=store.latest()
from marketpulse_app.contracts import DashboardFilter
from datetime import date
analytics.live_store=store
response=analytics.dashboard(DashboardFilter(date(2026,1,1),date(2026,1,3)),[])
assert any(row["metric_key"]==sample.key().decode() for row in response["streaming"]["rows"])
adapter_to_analytics_ms=(time.perf_counter()-transport_start)*1000
deliver(sample,store,c,now+timedelta(seconds=10))
after=store.latest()
assert before==after,"retry changed latest snapshot or first receipt time"
client.command("DROP DATABASE mp06_live_test")
client.close();c.close()
print(json.dumps({"latest":latest,"records_read":count,"partitions":len(partitions),"lost_ack_retry":True,
                  "adapter_to_analytics_response_ms":adapter_to_analytics_ms,"source_identity":source_identity}))
'''


from marketpulse_app.proof_http import ProofHTTP,add_auth_arguments


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--api", default="http://127.0.0.1:48000")
    parser.add_argument("--output", type=Path, default=Path(".lakehouse/mp06-proof/acceptance.json"))
    add_auth_arguments(parser)
    args = parser.parse_args()
    http=ProofHTTP(args.api,args.origin,args.username,args.password_file)
    if not args.project.startswith("marketpulse-") or "proof" not in args.project:
        parser.error("select an explicitly test-owned marketpulse-...proof project")
    container = f"{args.project}-api-1"

    def request(path: str, body=None, method="GET"):
        req = urllib.request.Request(args.api+"/api/v1/"+path,
            data=json.dumps(body).encode() if body is not None else None,
            headers={"Content-Type": "application/json"}, method=method)
        with http.open(req, timeout=15) as response:
            assert response.headers.get("Cache-Control") == "no-store"
            return json.load(response)

    oracle_result = subprocess.run(["docker", "exec", "-i", container, "python", "-"],
                                   input=KAFKA_ORACLE, text=True, capture_output=True, check=True)
    oracle = json.loads(oracle_result.stdout)
    before = request("dashboard?from_date=2026-01-01&to_date=2026-01-03")
    assert before["streaming"]["source_identity"] == oracle["source_identity"]
    expected = {key: row for key, row in oracle["latest"].items() if "2026-01-01" <= row["payload"]["event_date"] < "2026-01-03"}
    assert_projection(expected, before["streaming"]["rows"])
    totals = before["certified"]["totals"]
    assert len(totals) == 1 and totals[0]["currency"] == "VND"
    assert {field: totals[0][field] for field in ("spend_minor", "gross_revenue_minor", "refund_amount_minor", "net_revenue_minor")} == {
        "spend_minor":"551737", "gross_revenue_minor":"27160000", "refund_amount_minor":"4967000", "net_revenue_minor":"22193000"}
    campaign = "b00db876-5a1b-55c9-8412-9441821fe3ef"
    old = next((row for row in request("campaign-settings") if row["campaign_id"] == campaign), None)
    setting = {"market":"VN", "currency":"VND", "period_start":"2026-01-01", "period_end":"2026-01-03",
               "budget_minor":"1", "expected_version":int(old["version"]) if old else 0}
    saved = request("campaign-settings/"+campaign, setting, "PUT")
    try:
        request("campaign-settings/"+campaign, setting, "PUT")
        raise AssertionError("stale edit was accepted")
    except urllib.error.HTTPError as error:
        assert error.code == 409
    rules = request("alert-rules")
    for kind, threshold in (("refund_share", "0"), ("budget_overrun", "1")):
        if not any(row["campaign_id"] == campaign and row["kind"] == kind and row["threshold"] == threshold for row in rules):
            request("alert-rules", {"campaign_id":campaign,"kind":kind,"threshold":threshold}, "POST")
    request("alerts/evaluate", {}, "POST")
    second = request("alerts/evaluate", {}, "POST")
    assert second["new_evaluations"] == "0"
    alerts = request("alerts")
    assert {row["evidence"]["kind"] for row in alerts if row["evidence"]["campaign_id"] == campaign} >= {"budget_overrun", "refund_share"}
    filtered = request("dashboard?from_date=2026-01-01&to_date=2026-01-03&market=VN&campaign_id="+campaign)
    assert {row["campaign_id"] for row in filtered["certified"]["rows"]} == {campaign}
    pace = request("budget-pacing/"+campaign)
    assert pace["status"] == "complete" and pace["spend_minor"] == "91461" and pace["forecast_minor"] == "91461"
    scenario_body = {"from_date":"2026-01-01","to_date":"2026-01-03","currency":"VND","total_budget_minor":"1000",
                     "allocations":[{"campaign_id":campaign,"budget_minor":"1000"}]}
    scenario = request("budget-simulation", scenario_body, "POST")
    assert scenario["projected_net_minor"] == "35282"  # floor(1000*3227000/91461), independent literal.
    benchmarks = {}
    for name, path, body, method in (("dashboard", "dashboard?from_date=2026-01-01&to_date=2026-01-03", None, "GET"),
                                   ("pacing", "budget-pacing/"+campaign, None, "GET"),
                                   ("simulation", "budget-simulation", scenario_body, "POST")):
        for _ in range(20): request(path, body, method)
        durations = []
        for _ in range(100):
            start=time.perf_counter();request(path, body, method);durations.append((time.perf_counter()-start)*1000)
        benchmarks[name] = {"samples":100,"warmups":20,"p50_ms":percentile(durations,.5),"p95_ms":percentile(durations,.95),"max_ms":max(durations),"failures":0}
        assert benchmarks[name]["p95_ms"] <= 500, "fixture HTTP latency exceeded budget"
    stable = {"settings":request("campaign-settings"), "rules":request("alert-rules"), "alerts":request("alerts")}
    subprocess.run(["docker", "restart", f"{args.project}-app-postgres-1"], check=True, capture_output=True)
    deadline=time.monotonic()+60
    while time.monotonic()<deadline:
        healthy=subprocess.run(["docker","inspect","--format","{{.State.Health.Status}}",f"{args.project}-app-postgres-1"],capture_output=True,text=True,check=True).stdout.strip()
        if healthy=="healthy":break
        time.sleep(.25)
    assert healthy=="healthy"
    subprocess.run(["docker","restart",container,f"{args.project}-campaign-worker-1",f"{args.project}-web-1"],check=True,capture_output=True)
    deadline=time.monotonic()+60
    while True:
        try:
            request("health/ready");break
        except (OSError, urllib.error.HTTPError):
            if time.monotonic()>deadline:raise
            time.sleep(.25)
    after = request("dashboard?from_date=2026-01-01&to_date=2026-01-03")
    assert after["certified"]["totals"] == before["certified"]["totals"]
    assert after["certified"]["provenance"]["identity"] == before["certified"]["provenance"]["identity"]
    assert after["streaming"]["rows"] == before["streaming"]["rows"] or all(
        {key:value for key,value in row.items() if key not in {"event_age_seconds","transport_age_seconds"}} ==
        {key:value for key,value in prior.items() if key not in {"event_age_seconds","transport_age_seconds"}}
        for row,prior in zip(after["streaming"]["rows"],before["streaming"]["rows"],strict=True))
    assert_projection(expected, after["streaming"]["rows"])
    assert stable == {"settings":request("campaign-settings"),"rules":request("alert-rules"),"alerts":request("alerts")}
    resources=subprocess.run(["docker","info","--format","{{.NCPU}} CPUs; {{.MemTotal}} memory bytes"],capture_output=True,text=True,check=True).stdout.strip()
    report={"accepted":True,"project":args.project,"fixture":{"source_files":22,"source_rows":615,"live_keys":len(expected),"kafka_snapshots_read":oracle["records_read"]},
            "independent_kafka_projection":True,"lost_offset_ack_retry":oracle["lost_ack_retry"],"restart_preserved_app_state":True,
            "adapter_to_analytics_response_ms":oracle["adapter_to_analytics_response_ms"],
            "configured_demo_budget":True,"benchmarks":benchmarks,"docker_resources":resources,"certified_totals":totals,
            "pacing":pace,"simulation":scenario,"settings":stable["settings"],"alert_occurrences":stable["alerts"],
            "business_event_freshness":"Historical source event timestamps; no event-to-dashboard SLA claim"}
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2,sort_keys=True)+"\n")
    print(json.dumps({"accepted":True,"output":str(args.output),"benchmarks":benchmarks},indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
