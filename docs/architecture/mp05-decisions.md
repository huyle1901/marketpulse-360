# MP-05 implementation decisions and verification

This records decisions made while executing
[the implementation plan](../superpowers/plans/2026-10-13-batch-serving.md).
The [living architecture](high-level-design.md) defines the current system and
the [runbook](../runbooks/batch-serving.md) contains executable operator commands.

## Decisions and costs

| Decision | Reason | Cost or condition to revisit |
|---|---|---|
| Keep batch selection types in a dependency-free module; import selection only when requested by lakehouse ingestion. | Preserve MP-04 defaults and avoid circular imports. | Refactor this boundary if packages are split. |
| Reserve publication with the certification digest separately from the preparation digest. | Bind publication to the actual served candidate. | Ledger interface migration if the proof protocol changes. |
| Validate provider-specific date fields instead of a generic date. | Match Google/Meta/TikTok/CRM source contracts. | Update boundary validation when vendor schemas change. |
| Persist required platforms and verify identity on receipt read/write. | Reproduce nondefault source selections. | Receipt-format migration for contract changes. |
| Introduce an empty Dagster definition during runtime bootstrap, then replace it with the complete graph. | Test deployment independently before implementing business assets. | Temporary staged implementation only; final graph is complete. |
| Initialize only missing/blank batch secrets with a local tool. | Preserve existing MP-04 credentials. | Rotation is an explicit separate operation. |
| Build the Spark base before the batch image using a script. | Compose rejected an additional-context reference to a disabled-profile service. | Fresh builds must use the documented two-stage script. |
| Require new secrets at service startup rather than global Compose interpolation. | Keep disabled batch profiles from breaking MP-04 commands. | Some errors surface on startup rather than `compose config`. |
| Write bounded Spark export partitions to worker scratch, then conditionally upload to MinIO. | Existing Iceberg S3FileIO does not provide Hadoop s3a for JSON output. | Local scratch must fit the export; distributed workers need another storage arrangement. |
| Request timezone-aware ClickHouse result timestamps explicitly. | The client otherwise returns naive UTC datetimes. | Recheck adapter behavior on upgrades. |
| Governed platform values are `google/meta/tiktok`. | Source dataset names (`google_ads`, etc.) differ from governed values. | Update domain tests with any governed schema migration. |
| Combine real Spark export, dbt, exact equality and invalid-row checks in one integration fixture. | Avoid repeated JVM/bootstrap cost while exercising all assertions. | A shared fixture can make failure diagnosis broader. |
| Dashboard ratios use Decimal(38,6), truncate toward zero, and return null for zero denominators. | Preserve exact base money and define a reproducible display policy. | Revisit display rounding independently from financial totals. |
| Native backfill tags select receipt-only rebuild and skip promotion; historical job omits publication. | Prevent historical materialization from reading current sources or replacing active data. | Recheck Dagster tag/policy behavior on upgrades. |
| Share a preparation attempt counter across original/rebuild namespaces; separate copy attempts. | Avoid namespace collisions while giving partial copies fresh databases. | Operational bookkeeping migration if attempt ownership changes. |
| Persist certification paths and reverify artifacts/served rows at publication. | Detect post-certification mutations. | Full serving scans add publication time. |
| Dagster uses psycopg2 explicitly; the ledger uses psycopg3. | SQLAlchemy 2.1's psycopg3 default broke Dagster's parameterized NOTIFY path in the first live launch. | Revisit the notification driver on dependency upgrades. |
| Bind-mount tracked `dagster.yaml` read-only over persistent instance state. | Image COPY does not update configuration hidden by an existing volume. | Runtime configuration requires the repository checkout. |
| Store immutable certified bundle URIs/database/digest and reuse them on identical input refresh. | A new candidate conflicts with the identity's frozen publication reservation. | Persisted artifact paths are part of retry/recovery availability. |
| Emit reused dbt outputs in dependency order. | Dagster rejected the dashboard before its performance dependency. | Validate graph ordering on upgrades. |
| Run recovery against an explicitly selected existing test project; document bootstrap separately. | Test restart without implicitly reseeding sources. | A fresh clone must first execute the bootstrap commands. |
| Reject mixed-currency attribution/customer queries; label customer money as lifetime and filter cohorts by activity month. | Existing Gold schemas do not support arbitrary currency/period reinterpretation. | Future currency-aware Gold changes are required for broader financial queries. |
| Report fixture scale, resources and export-based independent query oracles. | Small-data latency is meaningful only with its dataset and machine context. | MP-10 must measure representative capacity. |
| Align all daily stages to one partition per native backfill run. | Dagster-dbt's default single-run policy split the graph across incompatible run boundaries. | Policy is a beta API; test behavior on dependency upgrades. |
| Resolve retry handoffs through the exact current/parent-run chain, including certification/publication. | Run-local paths fail under partial re-execution; partition IO may point to a newer unrelated candidate. | Retain original run metadata and artifacts; standalone downstream-only materialization must include upstream assets. |
| First-time source-history backfills ingest sources and exclude publication. | They have no historical receipt to replay. | Operators must choose the source-history job for first-time ingestion. |
| Leave acknowledgement pending across a real server restart and recover in a new publisher process. | Repair-before-restart evidence does not prove uncertain acknowledgement recovery. | Persist test state/volumes across the two proof phases. |
| Document cumulative seven-relation export scratch usage; defer per-relation cleanup. | Current temporary files remain until the complete export finishes. | Provide enough scratch for the whole export until optimized. |

## Observed local proof

- Queued refresh, explicit receipt-only historical rebuild and identical-input
  refresh completed successfully in the isolated `marketpulse-mp05-proof` project.
- Two-day native source-history and asset backfills completed successfully.
  GraphQL re-execution from the real dbt failure, certification and publication
  succeeded even after the later backfill materializations.
- Eight dbt models/28 data tests passed. Same-count monetary corruption and an
  invalid campaign reference were rejected before publication.
- The seven exported/copied relations contain 239 rows in total; the input
  fixture is 22 compressed files/615 rows. Financial totals are VND 27,160,000
  gross, 4,967,000 refunds, 22,193,000 net and 551,737 spend.
- All six queries were compared with independent Python aggregations over the
  pinned export objects. Each ran 20 warmups/100 uncached measurements, with no
  errors and fixture p95 below 10 ms on the observed machine.
- An injected lost acknowledgement in a dedicated test ledger recovered one
  logical publication. Restarting ClickHouse, PostgreSQL, code location,
  webserver and daemon retained the active release, snapshots, row counts and
  exact financial totals. Evidence is retained under ignored
  `.lakehouse/mp05-proof/` and can be regenerated using the runbook.
- Host Python suite: 306 tests, 24 opt-in integrations skipped there and exercised
  separately against real services. All 30 Flink Java tests passed. Fixture
  validation, Compose validation and whitespace checks passed. Current local
  credential values were absent from tracked files; remote secret scanning is
  a separate check and is not implied by this local scan.
- GitGuardian Security Checks on GitHub PR #5 returned `SUCCESS`. No separate
  GitHub Actions CI workflow is configured; the regression/integration results
  above come from the actual local executions.

Earlier failed development runs remain visible in Dagster as diagnostic history;
they do not change the active release. They exposed and led to fixes for driver
selection, immutable release retries and dbt output dependency order.

The independent whole-branch review found three Important issues: native
backfill/partial retry handoffs, first-time source-history routing, and an
acknowledgement proof that repaired before restart. The implementation fixes
these with regression tests and actual Dagster/backfill/restart checks. A
whitespace issue from an earlier commit was corrected. The only deferred Minor
is per-relation scratch cleanup, with its actual cumulative requirement now
documented. Production HA/TLS, distributed deployment, capacity and automatic
retention remain assigned outside this local milestone.
