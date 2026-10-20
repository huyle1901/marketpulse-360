# Project video / executable demonstration — 25-minute storyboard

Status: script prepared; narrated recording pending. The project video requirement
is 20–30 minutes (distinct from the course's earlier presentation video <=20min).
Use the generated twelve-slide PDF plus the retained app and evidence files.
Save final MP4/WebM with the real group ID. Do not expose `.env`, `.secrets`,
cookies, passwords, browser autofill or terminal-expanded Compose configuration.

| Time | Show | Explain / interaction |
|---|---|---|
| 00:00–02:00 | Slides1–2 | Retail context, stakeholders, ten requirements, synthetic sources/seed |
| 02:00–05:00 | Slide3 + architecture | Kafka vs Flink, Iceberg history, CH analytics vs PG operations, Dagster/dbt |
| 05:00–08:00 | Slide4 + evidence JSON | Exact finance; differing governed/streaming populations; duplicate/late/invalid tests |
| 08:00–11:00 | Slides5–7 + benchmark JSON | Bounded Flink/Streams parity, query scales, p50/p95/p99, resource/recovery scope |
| 11:00–15:00 | App `/` | KPI filters; historical explained alert; pacing; VND1,000 simulation |
| 15:00–18:00 | App `/analysis` | Stage-reach interpretation, attribution definitions/conservation, journey drill |
| 18:00–21:00 | App `/customers` | Cohort-to-profile timeline, identity confidence, segment preview/revisions, opportunity/consent |
| 21:00–23:00 | Operations + Grafana | Certified lineage/quality, resource coverage, failure vs unavailable, recovery evidence |
| 23:00–25:00 | Slides10–12 + package index | Automated vs actual human results; alternatives, limits, reproducibility and conclusions |

Before recording: run the evaluation runbook's app check and package verifier.
Open report/slides and app in separate windows, authenticate privately, select
2026-01-01 through2026-01-03, and rehearse each decision once. Use macOS
Screenshot (`Shift-Command-5`) to record a selected area with microphone enabled,
or an already installed screen recorder. Show real interactions and narrate the
limitations. Do not stretch a silent automated loop into a purported presentation.

After recording, inspect video metadata using ffprobe when available:

```bash
ffprobe -v error -show_entries format=duration:stream=codec_type \
  -of json '<GROUP_ID>-marketpulse360.webm'
```

Verify duration1200–1800seconds, video and audible narration, correct group name,
legible tables/UI and no secret exposure. Watch the complete recording and
check each task against decision-tasks.md before marking video accepted.
