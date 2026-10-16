# MP-08 — Customer 360 and lifecycle intelligence

**Status:** Specified; implementation pending.
**Base:** `0512e3d`, complete locally verified MP-07; stacked branch
`feat/mp08-customer-lifecycle-intelligence` in the same source tree. Existing
untracked `work/` artifacts are user material and are not part of this feature.
Scope decisions follow the user's standing delegation to the ordered backlog.

## Outcome and boundaries

A CRM analyst can inspect historical cohort retention, drill to anonymized
profiles and governed timelines, identify rule-based high-value/churn/cart
opportunities, and save reproducible versioned segment rules. A distinct
activation preview must exclude unknown/denied consent and unidentified profiles.
Extend existing FastAPI + Next.js; ClickHouse holds analytical rows and PostgreSQL
holds only operator-created segment definitions/revisions. No external audience
export, messaging, ad-platform action, source regeneration or earlier pipeline
redesign. Production authentication/RBAC and retention remain MP-09 scope.

Reuse certified MP-05 `customer_360`, `cohort_retention`, `customers`, campaigns
and campaign performance, plus MP-07's exact snapshot-bound governed event
projection. No new Spark publisher, analytical database or dependency is needed.
Missing/mismatched projection has an explicit unavailable/error state; it never
becomes zero purchase/behavior evidence or a fabricated segment.

## Identity, consent and privacy

Release-scoped `profile_ref = sha256(identity + '|' + profile_key)` is the only
public profile key, matching MP-07 projection identity. HTTP excludes raw
profile/customer/anonymous/session/order/click/event IDs, contact data, URLs and
payload JSON. Public timeline event references are hashed the same way. Hashes
are synthetic-data pseudonyms, not an assertion of production anonymization.

Expose identity confidence as categorical evidence, never an invented probability:
customer -> `deterministic_customer_reference` (explicit customer ID or the
existing deterministic session stitch; Gold cannot distinguish them), anonymous
-> `anonymous_device_only`, session -> `session_only`. Do not claim a new identity
merge or person-level certainty. Explain the source rule in the UI.

Gold's historical `consent_analytics=True` describes its governed event population.
For a matched CRM customer, also expose latest certified `customers` analytics/
marketing flags. Historical analytical summaries/cohorts remain the unchanged
certified population, labeled **historical governed analytics, not an activation
population**. For latest CRM analytics denial, suppress timeline event detail
with `consent_restricted`; do not join those events into an activation result.
Unknown CRM/anonymous/session consent remains unknown, never assumed granted.

Activation eligibility requires profile_type=customer, a matched CRM row, historical
Gold analytics consent true, and both latest CRM flags true. All activation lists
and segment previews apply this gate before rule conditions and counts. A denied
or unknown profile cannot re-enter via a permissive/empty rule. Preview returns
excluded-consent/identity count separately, without leaking excluded members.
Consent is **as of the certified release**, not a real-time CRM promise. Publish a
new certified release to reflect a later withdrawal. MP-07 event governance stays
unchanged, including retained commerce/email bot-session flags.

## Snapshot and analytical semantics

Pin one release per request and resolve its MP-07 journal/proof binding. Customer
finance is single-currency, guarded by declared currencies across campaigns and
campaign performance, because Customer 360 Gold has no currency column. Reject
mixed currency with 409 `unsupported_currency`; there is no conversion.

Use all governed history in the pinned snapshot, not a misleading selectable
historical as-of. `as_of` is UTC midnight following the maximum Gold profile
last_activity_at date; empty population falls back to release logical_day+1.
RFM recency = as_of.date - latest original order_created UTC date; with no order,
recency is null. Frequency = Gold order_count (original purchases, not refunds).
Monetary = Gold net_revenue_minor (refunds may make it negative). These are
observed lifetime-to-release metrics, not predictions or CRM-declared lifetime value.

Event projection adds last_purchase_at, add_to_cart_count and email_click_count
(event_name=email_clicked); it does not count refund gross context as another
purchase. Existing Gold event/session/order/refund/finance fields remain the
canonical profile totals. Last activity/as_of/projection snapshot/checksum,
release generation/identity/proof, actual read time and historical flag accompany
analytical responses. JSON integral values/money are strings, UI uses BigInt;
ratios retain certified six-place strings. No-store responses throughout.

Default opportunity rules are transparent heuristics: high_value means net >=
5,000,000 declared currency minor units and at least one order; churn_risk means
at least one order and recency >=30 days; cart_opportunity means at least one
add_to_cart event and zero orders. They are not ML scores. UI shows operands and
lets saved segment conditions choose different thresholds. Null recency never
passes a numeric comparison. If event detail is absent, do not infer recency,
cart/email counts or opportunity membership.

## Cohorts, profile discovery and timelines

GET `/api/v1/lifecycle/cohorts` returns certified cohort rows, declared currency,
source/provenance and global profile/activation-eligible counts. Preserve Gold's
cohort_month, activity_month, profile_type, month_number, cohort_size,
active_profiles and retained_rate. Do not sum repeated cohort_size over activity
months or invent zero/complete future cells. Negative month numbers are shown
honestly if earlier event history precedes the CRM-created cohort date.

GET `/lifecycle/profiles` accepts optional cohort_month/activity_month (ISO first
of month), profile_type customer/anonymous/session, purpose analytics/activation,
opportunity high_value/churn_risk/cart_opportunity, limit1..100(default20),
offset0..100000. Cohort filters select membership; activity_month additionally
requires governed event activity in that month, matching Gold retention semantics.
Stable order last_activity_at descending then profile_ref ascending; return exact
filtered total and pagination metadata. Unknown valid cohort returns empty;
invalid values return422. Default purpose=analytics, clearly labeled.

GET `/lifecycle/profiles/{profile_ref}/timeline` returns sanitized summary and
chronological governed events, time/campaign/source/event_name/financial recognition
only, limit1..100(default50), offset0..100000, stable event_time/event_id ordering.
Event gross is nonzero only for order_created; refund amount only order_refunded.
History is bounded and page counts are explicit; no partial page masquerades as
complete. Unknown hash404, malformed hash422, denied analytics returns explicit
consent_restricted without events. Projection absence returns unavailable with no
invented rows. Profiles list similarly exposes unavailable rather than zero facts;
cohort aggregates remain usable.

## Segment rule contract and PostgreSQL revisions

A canonical JSON object has exactly schema_version=`mp08-segment-v1`, purpose
analytics/activation, nullable cohort_month/activity_month/profile_type and an
array `conditions` of 0..8 conjunctive predicates. Condition shape is exactly
`{field,op,value}`; value is a canonical whole decimal string (no exponent, plus,
leading zeros or -0). op is gte/lte/eq. Fields: recency_days, order_count,
net_revenue_minor, session_count, event_count, add_to_cart_count, email_click_count.
Only net may be negative; all absolute values <=2**63-1. Reject unknown fields,
extra keys, malformed values and duplicate field/op pairs. Condition order is
canonicalized by field/op/value; semantics/rule digest do not depend on input order.
All SQL predicates use bound values and fixed allowlisted operators/fields.

GET `/lifecycle/segments` lists saved definitions (no analytical member rows).
POST `/lifecycle/segments` creates UUID ID and version1 from name1..80 trimmed
characters, rule, expected_version=0. PUT `/lifecycle/segments/{id}` requires
expected_version>=1 and increments transactionally; stale concurrent edit409.
GET `/lifecycle/segments/{id}/versions/{version}` retrieves immutable past name/rule/
digest/version. Segment revision schema is additive/idempotent; no source data or
segment membership is stored in PostgreSQL, and startup seeds no definitions.

POST `/lifecycle/segments/preview` evaluates the supplied rule against a request-
pinned release without saving it. Return exact matched_total, first20 stable
sanitized members, excluded_from_activation_total, rule_digest and evaluation_key
hashing release identity/proof, as_of and canonical rule digest. Empty activation
rules still enforce all consent gates. Missing detail409 lifecycle_detail_unavailable,
conflicting proof409 projection_mismatch. A saved historical revision plus the
same pinned release/as_of is reproducible; a later release can legitimately change
membership and receives a different evaluation identity. No external activation
side effect is supported. Preview explicitly states its purpose and consent policy.

## Application behavior

Add `/customers` navigation alongside operations/analysis. Include historical/as-of
context, cohort retention table with a drill action, purpose/profile/opportunity
filters, paginated RFM profile table and selected profile timeline. Show identity
confidence, effective CRM consent, eligibility reasons, raw heuristic operands,
empty/unavailable/restricted states and complete provenance in expandable details.
A segment editor builds the allowlisted conjunctive rule, previews it, saves/edits
with optimistic versions and reloads immutable older revisions.

Cancel or ignore obsolete cohort/filter/profile/segment requests. Accept combined
cohort/profile reports only for matching release identities. Reject an old release
or old rule preview beside new workspace data; retain prior reports with explicit
previous-selection/error markers. Disable save drafts while saving so late writes
cannot overwrite a newer editable draft. HTTP write failures keep user input.
Responsive/keyboard/labeled controls; mobile page must not overflow (tables scroll).

## Acceptance and evidence

Spec/plan committed before production code. RED/GREEN focused math/query/consent/
rule/store/API/browser tests and full Python suite/MP06–07 browser regressions.
Real PostgreSQL revision/concurrent-write/restart tests in a dedicated test DB;
actual CH profile/cohort/timeline/activation results against validated pinned MP05
exports and MP07 projection, with denied/unknown consent fault cases in owned
isolated data only. Full keyed equality, no raw identities in HTTP, zero fabricated
unknowns, rule replay under fixed release and stable older revisions. Test currency,
refund-only/never-purchased, activity-month membership and stale UI requests.

Build/recreate only changed app API/web images on the existing proof project;
start existing required services/volumes without reseeding/replaying earlier
pipelines. Docker daemon may need starting. Preserve untracked work/ and secrets.
Actual desktop/mobile visual inspection, 20 warmups/100 uncached serial samples
per cohorts/profiles/timeline/preview workload, fixture p95<=500ms, versions and
resources disclosed. Restart app API/web verifies saved revisions and MP06–07
results unchanged. Source fixture validation, Compose/diff/Git checks; ignored
`.lakehouse/mp08-proof/` evidence and fresh whole-branch reviewer/fix pass.
