# MP-05 - Scheduled batch and certified analytical serving

**Status:** Accepted for implementation by user continuation on 2026-10-13.
Implementation progress is tracked in the plan/backlog; this document is not a completion claim.
**Scope authority:** `specs/05-batch-orchestration-and-serving.md` and MP-05 in
`specs/README.md`. MP-04's table contracts and business semantics remain unchanged.

## Why and ownership

Marketing analysts need fast, consistent campaign, funnel, attribution, customer
and cohort queries. Data engineers need observable ingestion, retries and
reproducible backfills. A maintainer needs restartable services and inspectable
evidence. The later FastAPI application will consume the serving contract; users
and app configuration still belong in PostgreSQL, not generated source datasets.

Iceberg/MinIO remains the durable analytical source of truth. ClickHouse contains
rebuildable serving copies. Dagster coordinates execution; Spark remains the
bounded Iceberg read/write engine. dbt-clickhouse builds/tests serving models; it
does not pretend to query the native Nessie catalog directly or implement a second
attribution algorithm.

## Builder choices and technology baseline

These implementation choices are proposed by the builder, not previously chosen
by the user. Candidate pins verified against official registries on 2026-10-13:

| Component | Proposed pin |
|---|---|
| Dagster / webserver | 1.13.26 / 1.13.26 |
| dagster-dbt / dagster-postgres | 0.29.26 / 0.29.26 |
| dbt Core / dbt-clickhouse | 1.12.5 / 1.10.3 |
| clickhouse-connect | 1.10.0 |
| ClickHouse server LTS | 26.8.25.14 |
| ClickHouse image digest | sha256:dd81097e2c38827a438287d8ae5d4dc8afafbdd0e61e83376fdd7ed4580673b9 |
| Existing lakehouse runtime | Python 3.12.12, Spark 4.0.4, Iceberg 1.12.0, Nessie 0.109.0 |

The published ClickHouse image provides amd64 and arm64. Dependency metadata
admits the proposed Python/dbt/Dagster versions, but an actual resolver and image
build must verify compatibility before they are considered accepted runtime pins.
Lock transitive dependencies. If a patch-level compatibility adjustment is needed,
record its evidence and update this design and architecture in that commit.

Sources: [Dagster packages](https://pypi.org/project/dagster/),
[Dagster dbt integration](https://docs.dagster.io/integrations/libraries/dbt),
[dbt adapter](https://github.com/ClickHouse/dbt-clickhouse),
[ClickHouse release](https://github.com/ClickHouse/ClickHouse/releases/tag/v26.8.25.14-lts),
[official image tag](https://hub.docker.com/layers/clickhouse/clickhouse-server/26.8.25.14/images/sha256-dd81097e2c38827a438287d8ae5d4dc8afafbdd0e61e83376fdd7ed4580673b9).

## Local deployment

- `serving` profile: authenticated ClickHouse, bootstrap tool, persistent
  `clickhouse_data`; API 8123 and native 9000 inside the Compose network. Publish
  HTTP on localhost:8123 and do not publish native 9000, which would collide with MinIO.
- `batch` profile: dedicated PostgreSQL database/service for Dagster and publication
  operational metadata, Dagster webserver (localhost:3000), daemon and gRPC code
  location. Persist instance artifacts/receipts in `batch_state`.
- The code-location image extends the proved Spark runtime and installs the pinned
  batch extra and dbt project. Dagster's default gRPC/subprocess launcher executes
  jobs there; no host Docker socket or Docker-in-Docker is required.
- Webserver/daemon use a queued coordinator with one running marketing refresh.
  All publishers additionally use a PostgreSQL advisory lock; a CLI bypassing
  Dagster must obey the same publication protocol.
- Run Spark steps sequentially as bounded child processes (2 GiB heap/local[2]);
  the worker container has a 4 GiB limit. Do not start a second Flink job.
- Require separate `CLICKHOUSE_PASSWORD` and `DAGSTER_POSTGRES_PASSWORD` at service
  startup, not global interpolation of disabled Compose profiles, in ignored
  `.env`, with empty committed template fields. Do not bake credentials into images,
  SQL, manifests, Dagster logs or dbt artifacts. Existing MP-04 services/volumes are
  not reset or silently migrated by MP-05.
- Build the local Spark base then the batch image using
  `scripts/build-batch-runtime.sh`; the Spark base is not a public registry image.

## Sources, dates and observable assets

`historical_file_manifest` reads a configured immutable source manifest and validates
hashes. `master_data` is unpartitioned; `ad_reports` and `crm_snapshots` select
the declared UTC logical day from the existing `date=YYYY-MM-DD` file partition.
For selected daily files, Google `segments_date`, Meta `date_start`/`date_stop`,
TikTok `stat_time_day`, or CRM `snapshot_date` must equal
that partition. Reject missing partitions, mixed-day files and conflicting row
dates; do not silently split files or renumber their original rows. This extra
validation applies to daily batch selection, not MP-04's default all-file CLI.
The validated batch configuration declares `required_platforms`, defaulting to
`google_ads`, `meta_ads`, `tiktok_ads`, and requires a nonempty CRM snapshot.
Reject unknown/duplicate configured platforms. Source selection must
report what actually exists, not fabricate today's fixture records. Missing
expected report platforms or a CRM snapshot fail before Gold publication.

Extend the existing file-ingest interface with optional dataset/date selection,
preserving the MP-04 default of all eligible files. Each selected file is archived
before parsing. Shared master/manifest objects and repeated file rows remain
content-addressed no-ops; never rewrite their first ingestion run or hash.

`bounded_kafka` captures and archives the existing raw/clean/DLQ topic bounds using
MP-04's read_committed ingestion, without a second consumer producing new clean
events. If Flink is still behind and raw/clean cannot reconcile, fail closed;
a fresh refresh may capture new bounds, but a retry of a pinned receipt cannot.

The daily schedule is 02:00 UTC for the preceding UTC day, initially stopped.
Manual demo/backfill runs select actual fixture dates. Daily partitions select
batch inputs: they do NOT mean Gold contains only one day. A refresh recomputes a
complete governed dataset from the captured cumulative Bronze snapshots, so old
analytical days are not dropped. A historical receipt means historical snapshot
state, not an invented business cutoff inferred from its logical-day label.

Assets after ingestion: `pinned_bronze`, `governed_silver`, `marketing_gold`,
`snapshot_export`, dbt serving models/checks, `certified_release`, `active_release`.
Record source files, hashes, actual insert/no-op counts, immutable snapshot maps,
model identity, row counts/checksums, timings and failure stages in Dagster.

## Preparation receipts and replay

Persist an immutable preparation identity derived from canonical Bronze snapshot
maps, logical input selection and a hash of relevant transformation/dbt/contract
code. Documentation-only edits are not model identity changes.

PostgreSQL's `marketpulse_ops` schema in the batch database maintains the unique
identity, attempt counter, monotonically increasing publication generation, stage,
snapshot receipt URI and certified candidate. This is operational metadata only.

Build Silver/Gold in an owned `mp_batch_<identity>_<attempt>` Iceberg namespace;
never overwrite the MP-04 original or a completed preparation namespace. The
first successful preparation freezes its output snapshot IDs and proof. Subsequent
retries of that identity reuse that receipt; they do not read current heads or
create new Gold snapshots merely to retry a failed serving copy.

An incomplete preparation can be rebuilt in a new owned attempt namespace from
the same pinned Bronze maps. Failed attempts are retained for inspection; cleanup
is explicit, never an automatic broad DROP/PURGE. Namespace identity collisions
with differing receipts fail closed. Existing MP-04 input provenance is preserved,
not falsified to make its single-run evidence command fit a mixed-ingestion history.

`historical_rebuild_job` consumes a recorded preparation receipt and rebuilds in
isolation. It compares full business checksums and financial totals. It does not
promote an active release. Default Dagster backfill runs use this mode. Each
partition requires an explicit day-to-receipt mapping in run configuration;
validate the receipt's logical day. Missing or ambiguous mappings fail before
execution: never infer "latest" or substitute current table heads. First-time
historical ingestion is a separate `source_history_job`, which prepares selected
days without publication. Deliberate promotion is a separate operator action with
the same concurrency checks.

## Snapshot export and serving types

Export all five MP-04 Gold models, plus `silver.campaigns` and `silver.customers`
reference dimensions, from the receipt's exact snapshots. Only contract business
columns are exported. No raw CRM email/phone/payload enters ClickHouse.

Spark writes partitioned JSONEachRow-compatible export objects to an owned MinIO
prefix. An export manifest records object URIs/hashes/bytes/rows, source snapshot
IDs, ordered physical schemas and canonical business checksums. Enumerate only the
successful job's output, not arbitrary warehouse files. Stream objects into
ClickHouse in bounded chunks; never collect the full export dataset in Python RAM.

Mapping: string -> String, int -> Int32, long -> Int64, boolean -> Bool, date ->
Date32, timestamp -> DateTime64(6,'UTC'), decimal(p,s) -> Decimal(p,s); wrap nullable
columns in Nullable. Preserve nulls, negative refunds/net values, leading-zero IDs,
exact integer amounts beyond 2**53 and declared decimal scale. Reject unknown types,
missing/extra fields, malformed dates, overflows and non-finite values. Do not use
floating point for money; dashboard financial sums widen to Int128.

## Candidate datasets, dbt and certification

Each copy attempt gets an isolated `mp_release_<identity>_<attempt>` Atomic database
with typed `raw_<model>` sources. Normal MergeTree tables order by the documented
model keys; exact uniqueness is a validation gate, not an eventual background
ReplacingMergeTree merge. A copy failure cannot leak partial rows to active queries.

Retry a partial copy in a fresh attempt database, not by appending the same rows to
the old attempt. A fully certified attempt can be reused. Earlier failed attempts
are never included in reader queries and are not automatically deleted.

dbt builds the five exact served Gold relations and reference dimensions from these
sources, plus `campaign_dashboard` joining campaign names/status to performance
and deriving null-safe CTR/CPC metrics without changing source monetary totals.
Use `DbtCliResource`/dbt artifacts to expose actual model/test outcomes in Dagster;
asset keys stay stable even when physical candidate database names change. Generate
the manifest after runtime credential injection, never with baked-in build secrets.

Tests: not-null required columns; composite-key uniqueness; all three attribution
models; profile/platform accepted values from MP-04; campaign and identified-customer
relationships with explicit handling of `__unattributed__` and anonymous profiles;
refund/net equations; per-model gross/refund/net conservation; cohort/funnel
invariants; consent eligibility; exact source/served schema, counts and rowset hashes.

The five Gold business rowsets must match MP-04 exactly. Derived read models have
their own independently computed fixture oracles. Certification records actual
source snapshots, export hashes, dbt invocation/test results and complete equality
checks. Missing/failed proof blocks publication even if row counts happen to match.

## Consistent publication and failure recovery

Keep certified release databases immutable. An append-only publication journal
in `marketpulse_serving` records generation, release identity, database, logical
day and certification digest in one synchronous INSERT row. Readers select the
greatest published generation, not a mutable pointer exchanged by competing
publishers. An empty journal returns a clear not-published error.

Under the PostgreSQL publisher lock, validate certification and candidate tables.
In one PostgreSQL transaction, reserve a monotonically increasing generation and
freeze its exact identity/database/digest; the reservation is unique per identity.
Reject a logical day older than the reserved day high-water mark, and reject any
attempt to replace a reservation's payload. Commit before submitting ClickHouse
publication. A retry reuses the reservation, never allocates a newer generation.
Append that frozen row with synchronous insertion, then acknowledge the ledger.

If acknowledgement is lost, an exact journal match repairs the ledger and returns
already-published. Otherwise retry the identical row. Physical duplicate journal
rows are allowed only when their entire payload is identical; the reader collapses
them and fails on conflicting payloads for a generation or identity. This is
logical exactly-once publication, not a claim of physical exactly-once delivery.
Business model rows are never appended during publication.

If PostgreSQL loses the publisher lock while an old ClickHouse request remains
in flight, a successor may reserve and append a higher generation. A delayed old
append cannot lower the reader's maximum generation. Test this interleaving,
including delayed duplicates after lost acknowledgements; do not rely on a
background MergeTree merge or a finite deduplication window for correctness.
Reserved-but-unpublished generations may leave gaps and must not hide the latest
actually published release. Certification digests must match persisted proof.

Readers resolve the journal once per query session/request, validate the owned
database identifier and pin it for every query. Old certified databases remain
available while reads complete. Multi-query sessions never mix release A's campaign
table with release B's attribution/customer table.

This generation protocol is an application contract, not a cross-database
transaction supplied by dbt or ClickHouse.

## Query contract and acceptance evidence

Provide parameterized workloads for campaign overview, daily funnel, attribution
comparison, cohort retention, customer drill-down and top campaigns. Date intervals
are UTC [from,to); monetary totals always group/filter by currency. Values use
query parameters; owned database/model names are allowlisted identifiers. Do not
provide an arbitrary SQL endpoint in MP-05.

Benchmark 20 warmups plus 100 serial measured requests per workload, query cache
disabled, against one certified release. Report correctness, p50/p95/max, failures,
source and served row counts, model identity, database/server versions and machine
resources. Fixture acceptance is p95 <=500ms, not a million-row capacity claim.
The same commands accept ignored demo/benchmark datasets; MP-10 owns scale claims.

The Docker proof bootstraps an isolated MP-04 input dataset, runs an actual queued
Dagster refresh, captures dbt model/check results, verifies serving equality and
query answers, injects partial-copy and bad-model failures, retries without inflated
active totals, simulates a lost acknowledgement, rejects stale publication, runs a
historical rebuild, restarts batch/serving services and re-verifies evidence.
No fault-injection controls are public API defaults. Faults belong in test harnesses.

## Non-goals and implementation latitude

No mandatory cloud services, production HA/TLS, Kafka-native serving consumer,
application account/UI behavior, new attribution policy, or automatic retention
cleanup. Preserve the MP-04 default CLI/schema behavior. Builder may choose small
module boundaries, low-level client APIs, artifact filenames and Docker health
checks if the contracts above hold; deviations require explicit documentation.
