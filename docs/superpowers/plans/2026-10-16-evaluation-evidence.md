# MP-10 Evaluation and Evidence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver reproducible technology comparisons and an honest final evidence kit.

**Architecture:** Isolated optional Compose evaluation stack; production services
and datasets are read-only inputs to application evidence. Python owns generation,
oracles, measurements and packaging; Java adapters execute the same bounded
finance reducer in Flink and Kafka Streams.

**Tech Stack:** Python 3.12+, Java 17, Flink 2.2.1, Kafka Streams/Kafka 4.3.1,
ClickHouse, PostgreSQL 17.6, ReportLab PDF.

**Spec:** `specs/10-evaluation-and-evidence.md`

## Global Constraints

- Use one source tree. Never modify production topics, publications or app state.
- PostgreSQL analytics is an isolated benchmark baseline only.
- No credentials, sessions, large data or local instructions in Git.
- No invented human observations, video completion, group size or timings.
- Every success claim requires fresh command evidence and exact oracle equality.

## Review Focus

- A missing/duplicated committed output must fail even when final totals match.
- Cross-engine inputs must match after excluding only `sent_ms`.
- Empty/nonfinite timing samples and negative durations must fail, not yield success.
- A restart without checkpoint/progress evidence must not count as recovery.
- Human/video/group coverage must stay pending without real supplied evidence.

### Task 1: Deterministic workload and evidence contracts

**Files:** create `src/marketpulse_eval/{__init__,contracts,workload,analytics}.py`,
`tests/test_evaluation.py`; update architecture with evaluation boundaries.

**Interfaces:** produces `events(count: int, seed: int=360) -> list[dict]`,
`oracle(rows: list[dict]) -> list[dict]`, `percentiles(values: list[float]) -> dict`,
`verify_outputs(inputs: list[dict], outputs: list[dict]) -> dict`,
`analytical_oracle(count: int, workload: str) -> list[tuple]`.

- [ ] Write tests for reproducible hashes, exact anomaly classes/money,
  watermark/horizon edges, malformed/bool values, changed duplicates, count/seq
  mismatches, empty/nonfinite/negative statistics and periodic analytical answers.
- [ ] Run `.venv/bin/python -m unittest discover -s tests -p test_evaluation.py -v`.
  Expected: RED missing module, then GREEN after implementation.
- [ ] Implement typed pure contracts and independent oracles; no engine calls.
- [ ] Run focused tests and `git diff --check`; commit `feat: add evaluation workload and oracle contracts`.

### Task 2: Engine adapters and Docker measurement runner

**Files:** create `evaluation-job/pom.xml`, Java reducer/Flink/Streams adapters and
JUnit tests under `evaluation-job/src`, `docker/evaluation/Dockerfile`,
`compose.evaluation.yaml`, `src/marketpulse_eval/{runner,queries,resources}.py`,
`scripts/evaluate.py`; update ignores/Docker context and architecture decisions.

**Interfaces:** consumes Task 1 public functions; produces CLI
`PYTHONPATH=src .venv/bin/python scripts/evaluate.py --output <ignored-directory>`
with `stream`, `analytics` subcommands and explicit scale/recovery flags. Result
JSON is version 1, contains inputs/oracle/observations/resources/versions/revision.

- [ ] Write Java reducer and topology tests for every class plus exact snapshot
  state across repeated inputs; Python tests reject insufficient recovery proof
  and differing input/output hashes. Run them RED before adapters exist.
- [ ] Implement persistent engine stores, EOS sinks, bounded producer/consumer,
  checkpoint-aware recovery, Docker counter sampling and independent SQL checks.
- [ ] Run Java tests with Maven container; Python focused tests GREEN.
- [ ] Build/start isolated Compose profile; run both stream scales, restart tests,
  all three analytical scales and exact-answer timing. Expected: every committed
  record matches oracle, complete resource/recovery evidence and no app mutation.
- [ ] Run full Python suite, original Java suite and `git diff --check`; commit
  `feat: benchmark isolated processing and analytical alternatives`.

### Task 3: Application decisions and final submission kit

**Files:** create `src/marketpulse_eval/{effectiveness,package}.py`,
`docs/evaluation/{README,report,decision-tasks,video-storyboard}.md`,
`docs/runbooks/evaluation.md`, `docs/architecture/mp10-decisions.md`;
update `README.md`, backlog and living architecture with actual results.

**Interfaces:** consumes version-1 runner JSON; produces `package` CLI report PDF,
slide PDF and evidence index, plus `app` CLI read-only authenticated task results
using `ProofHTTP`. Human acceptance stays pending unless real observations exist.

- [ ] Write tests rejecting missing benchmark runs, false human/video completion,
  missing required requirements and artifact hash mismatch; observe RED.
- [ ] Implement read-only task evidence and reproducible six-section kit builder.
- [ ] Run focused tests GREEN; run retained Docker app checks and render/review
  both PDFs. Expected: ten traceable decisions; clear pending external items.
- [ ] Run full Python/Java/browser suites, typecheck/build, deterministic fixture
  validation, Compose validation and `git diff --check`; commit final evidence.
- [ ] Final fresh whole-branch review, one TDD fix pass for Important findings;
  verify Git status, then report exact completion limits and artifact locations.
