# Final evidence kit

Generate with the [evaluation runbook](../runbooks/evaluation.md). Local outputs:
`.lakehouse/mp10-package/report.pdf`, `slides.pdf`, `report.md`, copied measurement
JSON and content-hashed `index.json`. Large facts and scratch remain ignored.

[Report source](report.md), [ten stakeholder tasks](decision-tasks.md),
[25-minute video storyboard](video-storyboard.md),
[design decisions](../architecture/mp10-decisions.md).

| Required content / acceptance | Evidence | Status |
|---|---|---|
| Context, stakeholders, requirements | Report section1; task sheet | Prepared; group size unconfirmed |
| Sources and data characteristics | Section2; fixture manifest/validation | Implemented/verified |
| Data engineering solution | Sections2–3; living architecture/runbooks | Implemented/verified |
| Management and processing technologies | Five concrete examples in section2 | Prepared |
| Alternative processing solution | Same-input finance Flink/Streams results | Verified; evidence integrity rechecked 2026-10-20 |
| Alternative analytical solution | CH/PG1M/10M/20M exact-answer query timings | Verified; evidence integrity rechecked 2026-10-20 |
| Application development | Ten automated decision checks + browser regressions | Ten API decisions rechecked 2026-10-20; browser proof from 2026-10-16 |
| Correctness | Exact per-output/SQL oracles; prior certified releases | Evidence validated; fixture and Python suite rechecked 2026-10-20 |
| Performance and recovery/resources | Raw JSON with p50/p95/p99 and cgroup coverage | Measured 2026-10-16; original evidence preserved and verified |
| Human effectiveness | Participant task sheet | Pending real observations |
| Report4–7sections, >=4technology examples | Six-section source + PDF | Generated and visually checked; content hashes verified |
| Presentation PDF | Twelve-slide generated deck | Generated and visually checked; content hashes verified |
| Project video20–30minutes | Timed storyboard and recording/verification instructions | Pending narrated recording |
| Group naming and >=2*nrequirements | Real group ID/member count | Unconfirmed |

This checklist prevents an unsupported “top rubric band” or “<10% missing” claim.
It covers every requested report topic, but final submission acceptance includes
actual human/video/group evidence that automation cannot manufacture.

## Technical closeout — 2026-10-20

[PR #11](https://github.com/huyle1901/marketpulse-360/pull/11) contains the MP-10
implementation and final review fixes. All three implementation-plan tasks are
complete. The remaining submission items are the pending rows above.

Fresh checks: 423 Python tests (383 passed, 40 opt-in skipped), valid 615-row /
22-file source fixture, both Compose configurations, ten authenticated read-only
decision checks and unchanged release/proof binding. All 28 original package-file
hashes and the twelve benchmark/recovery cases were verified. Benchmark timings
remain the original 2026-10-16 observations; they were not remeasured during this
closeout. Java/browser/build verification logs from that implementation remain
included in the original package.

The local handoff archive is `.lakehouse/mp10-submission-2026-10-20.zip`, with
an adjacent SHA-256 file. It contains the original report/slides/evidence kit and
the fresh verification records under `closeout/`. It contains no credentials.
Recheck `package/index.json` when extracting the original kit. The closeout
manifest hashes the package and verification records and lists pending acceptance
items; an archive is not evidence of human testing or a completed video.
