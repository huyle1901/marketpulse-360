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
| Alternative processing solution | Same-input finance Flink/Streams results | Requires passed package validation |
| Alternative analytical solution | CH/PG1M/10M/20M exact-answer query timings | Requires passed package validation |
| Application development | Ten automated decision checks + browser regressions | Requires passed package validation |
| Correctness | Exact per-output/SQL oracles; prior certified releases | Requires passed package validation |
| Performance and recovery/resources | Raw JSON with p50/p95/p99 and cgroup coverage | Requires passed package validation |
| Human effectiveness | Participant task sheet | Pending real observations |
| Report4–7sections, >=4technology examples | Six-section source + PDF | Prepared; visually inspect generated PDF |
| Presentation PDF | Twelve-slide generated deck | Prepared; visually inspect generated PDF |
| Project video20–30minutes | Timed storyboard and recording/verification instructions | Pending narrated recording |
| Group naming and >=2*nrequirements | Real group ID/member count | Unconfirmed |

This checklist prevents an unsupported “top rubric band” or “<10% missing” claim.
It covers every requested report topic, but final submission acceptance includes
actual human/video/group evidence that automation cannot manufacture.
