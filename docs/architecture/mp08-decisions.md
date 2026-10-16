# MP-08 decisions and observed proof

MP-08 extends complete local MP-07 `0512e3d`, in branch
`feat/mp08-customer-lifecycle-intelligence` and the existing source tree. The
user's standing backlog delegation settled scope; spec/plan were committed in
`5d85711` before production code. Fresh builder check found no important gap.
The pre-existing untracked `work/` presentations/HTML assets remain untouched.

## Storage, identity and consent

FastAPI/Next.js add `/customers`, using ClickHouse's existing certified Customer
360/cohort/CRM tables and MP-07 event projection. PostgreSQL stores only
operator-authored segment definitions and immutable revisions, never analytical
members or generated customers. No new publisher, dependency, source replay or
earlier pipeline redesign was needed. This avoids a duplicate data path; if the
existing detail is unavailable, lifecycle detail fails visibly instead of zeroing
metrics. Missing detail leaves certified cohorts available.

Public profile/event hashes use the release-scoped MP-07 hash convention. Identity
confidence describes existing deterministic reference evidence: customer reference
(explicit ID or session stitch, indistinguishable in Gold), anonymous device only,
or session only. It is not a probability or new identity merge. HTTP excludes raw
profile/customer/session/order/click/event identifiers and contact/payload fields.

Historical governed cohorts remain distinct from activation populations. Activation
requires an identified matching CRM customer, historical analytics consent and both
latest-certified CRM consent flags true; unknown/denied values fail closed even for
an empty rule. A current-in-release analytics denial suppresses timeline details.
Consent describes the pinned release; newer withdrawals require a new certified
release. Production RBAC/privacy hardening stays MP-09 scope.

RFM uses all governed observed history: frequency counts original purchases,
monetary is exact net after refunds, recency uses the last original purchase.
As_of is the UTC day boundary after the latest Gold profile activity, not today's
clock or a selectable backdated view over future-inclusive Gold. Never-purchased
recency stays null. Heuristics are explicit thresholds, not predictions. Mixed
currency fails because Gold has no currency dimension. Money/integers are exact
JSON strings/BigInt; large expected_version writes preserve an exact JSON integer
token rather than converting a PostgreSQL version through JS Number.

## Rules, revisions and UI ordering

Canonical `mp08-segment-v1` rules use at most eight conjunctive allowlisted numeric
conditions plus purpose/cohort/activity/type. Unknown keys/operators and noncanonical
values fail validation. Parameters bind all filter/threshold values. Evaluation
identity includes release/proof/as_of/rule digest. Segment previews return exact
population counts plus the first twenty stable sanitized members; no member
persistence or external activation occurs.

An additive PostgreSQL migration under lock36008 creates current versions and
append-only revisions. Optimistic compare-and-swap allows one concurrent update;
a trigger rejects revision UPDATE/DELETE. Startup seeds no definitions. Historical
revisions remain read-only in the editor and preview reproducibly on the same
release. A save locks its draft and preserves input on failure.

Combined cohort/profile reports require matching release identities. Old timeline
and draft preview requests are cancelled/ignored; preview digest/release must match
the current context. Previous selection/error states remain labeled. Native inline
execution follows the user's one-tree instruction; the tradeoff is no separate
working-tree isolation. Scope choices follow the delegated backlog; future changes
can be handled in a later milestone.

## Observed evidence

Existing project `marketpulse-mp05-proof`, API48000/web43100, retained original
volumes. Docker daemon and required existing stores were started, then only changed
API/web images were rebuilt/recreated; no source replay or prior pipeline rebuild.
Existing app/web image tags remain MP-06 with MP-08 code built into them.

Release `31ffb7c713c694b9082b0f1daad7a32d50ca76e9b842fcb988029d3da4fdef06`,
Silver snapshot `505365576434850409`, projection checksum
`c218a8023e749c22f328fbe4d70020020ec8a73c951d0fc3a14c23caacc88654`.
Validated export/checksum oracles plus 191 governed events establish exact profile,
RFM/behavior and cohort results: 60 historical profiles, 37 activation eligible,
21 retained cohort rows, as_of `2026-01-03T00:00:00+00:00`. Twenty-nine discovery
cases and all sixty timelines were independently compared; keys, counts, exact
money and consent flags match. Historical denominators are not summed over cells.

Owned ClickHouse HTTP fixtures prove seven profiles yield only two activation
members, including exclusion of denied/unknown consent and anonymous/session
references; activity-month drill selects the single actual active member. A
refund-heavy/large-integer case preserves original-purchase recency33 and exact
net9007199254740990. Never-purchased cart opportunities retain null recency;
negative-net analytics and mixed-currency rejection are checked. Only owned
isolated candidates were removed. Dedicated `mp08_test` PostgreSQL proves empty/
idempotent migrations, immutable history, concurrent CAS and restored reads.

The HTTP proof created one operator-authored demonstration segment with revisions
1/2 and retained it. Stale writes409; revision1 and its evaluation identity remain
unchanged after edits and API/web restart. MP-06 finances/settings/rules/alerts and
MP-07 funnel/attribution/journeys remained unchanged. Proof privacy checks inspect
JSON keys recursively, allowing `source=email` while rejecting actual email keys.

All369 Python tests succeed (35 opt-in skips); all five new real integration
cases (four ClickHouse, one PostgreSQL) ran separately. All22 browser tests,
TypeScript/production builds, fixture22files/615rows, Compose/diff checks pass.
Actual Docker desktop/mobile, timeline and preview UI were inspected: no browser
errors or page overflow. Evidence/screenshots stay in ignored `.lakehouse/mp08-proof/`.

Fixture benchmark:20warmups/100serial uncached HTTP samples each, zero failures,
15 Docker CPUs and16,747,331,584memory bytes. Python3.12.13, Next16.4.0,
ClickHouse26.8.25.14, PostgreSQL17.6.

| Workload | p50ms | p95ms | Maxms |
| --- | ---: | ---: | ---: |
| Cohorts | 9.53 | 13.64 | 18.76 |
| Profiles | 20.80 | 28.53 | 34.63 |
| Timeline | 19.84 | 22.00 | 23.11 |
| Segment preview | 24.80 | 32.39 | 33.28 |

This proves the local fixture, not production capacity. Profiles/timelines are
paged1..100 with offset<=100000; previews sample20 plus exact total. No production
throughput claim, activation delivery or earlier-pipeline changes are implied.
Fresh whole-branch review/fixes remain the final completion gate.
