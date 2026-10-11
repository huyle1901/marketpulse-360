# Mock Data Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic, streaming-write omnichannel marketing data generator with validation, fixtures, and a documented large demo profile.

**Architecture:** A dependency-free Python package emits canonical master entities and correlated journeys into source-specific gzip files. Writers rotate by row limit and update an in-memory manifest summary, while a validator streams outputs to verify checksums, schemas, relationships, ordering, anomaly counts, and reconciled truth.

**Tech Stack:** Python 3.12+, standard library, `unittest`, gzip CSV/JSONL, Docker-compatible CLI.

**Spec:** `specs/01-mock-data-foundation.md`

## Global Constraints

- A seed and profile must reproduce identical relative files and checksums.
- Generated large data belongs only under ignored `data/generated/`.
- All identifiers and PII are synthetic; emails use `.test`.
- Time is UTC, profiles default to `2026-01-01`, and all monetary values use VND.
- Vendor attribution is last paid click within seven 24-hour periods; refunds affect
  net revenue but do not erase conversions or gross vendor conversion value.
- Writers must operate with bounded event memory and rotate at a configured row limit.
- MP-01 does not publish to Kafka; its event-time contract must support MP-02 replay.
- Any changed component, data flow, schema, storage responsibility, or scope must be reflected in `docs/architecture/high-level-design.md` in the same commit.

## Review Focus

- Existing non-empty output directory: generation refuses unless exact-directory overwrite is explicit; tested in Task 6.
- Timestamp edge at UTC midnight: records stay in the `event_time` date partition; tested in Task 4.
- Zero-click campaign/day: vendor report still emits impressions and zero conversions; tested in Task 4.
- Rotated files: headers, ordering, and checksums remain correct across parts; tested in Task 5.
- Tampered gzip file or manifest: validation fails with the exact relative file; tested in Task 7.

---

### Task 1: Package skeleton and profile contract

**Files:**
- Create: `pyproject.toml`
- Create: `src/marketpulse_mock/__init__.py`
- Create: `src/marketpulse_mock/config.py`
- Create: `tests/test_config.py`

**Interfaces:**
- Produces: `ScaleProfile`, `AnomalyRates`, `GenerationConfig`, `PROFILES`, and `resolve_config(profile_name: str, seed: int, output_dir: Path, rows_per_file: int | None = None, start_date: date | None = None) -> GenerationConfig`.

- [ ] Write tests asserting exact `fixture`, `smoke`, `demo`, and `benchmark` values, fixed UTC start date, immutable configs, overrides, exact anomaly rates, and rejection of unknown profiles or non-positive values.
- [ ] Run `PYTHONPATH=src python3 -m unittest tests.test_config -v`; expect import failure because the package does not exist.
- [ ] Implement the dataclasses and validation with no third-party runtime dependencies.
- [ ] Re-run the focused test; expect all tests to pass.
- [ ] Run `python3 -m unittest discover -s tests -v`; expect the suite to pass.
- [ ] Commit with `test:` and `feat:` commits or one focused `feat: add mock data profile contracts` commit.

### Task 2: Deterministic identifiers and master entities

**Files:**
- Create: `src/marketpulse_mock/ids.py`
- Create: `src/marketpulse_mock/models.py`
- Create: `src/marketpulse_mock/masters.py`
- Create: `tests/test_masters.py`

**Interfaces:**
- Consumes: `GenerationConfig` from Task 1.
- Produces: `stable_id(seed: int, entity: str, ordinal: int) -> str`, `MasterData`, and `generate_master_data(config: GenerationConfig) -> MasterData`.

- [ ] Write tests proving byte-stable IDs, different-seed variation, platform coverage, referential integrity, `.test` emails, valid consent flags, and active campaign date ranges.
- [ ] Run the focused tests; expect missing-module failure.
- [ ] Implement immutable entity dataclasses and seeded master generation.
- [ ] Re-run focused and complete suites; expect all tests to pass.
- [ ] Commit `feat: generate deterministic marketing master data`.

### Task 3: Correlated journey and anomaly generation

**Files:**
- Create: `src/marketpulse_mock/journeys.py`
- Create: `tests/test_journeys.py`

**Interfaces:**
- Consumes: `GenerationConfig` and `MasterData`.
- Produces: `SourceRecord(dataset: str, event_time: datetime, payload: dict[str, object])`, `DailyCampaignTruth`, and `iter_day_records(config: GenerationConfig, masters: MasterData, day_index: int) -> Iterator[SourceRecord]`.

- [ ] Write tests for a click-to-purchase journey, cross-source IDs, web/order amount equality, seven-day last-paid-click attribution, gross/refund/net rules, report/truth reconciliation, zero-click campaign reports, deterministic anomaly counts, and event-time date partitioning at UTC midnight.
- [ ] Run focused tests; expect missing-module failure.
- [ ] Implement seeded funnel transitions and derived daily aggregates without accumulating all event records.
- [ ] Re-run focused and complete suites; expect all tests to pass.
- [ ] Commit `feat: generate correlated omnichannel journeys`.

### Task 4: Source-specific rotating writers

**Files:**
- Create: `src/marketpulse_mock/writers.py`
- Create: `tests/test_writers.py`

**Interfaces:**
- Consumes: `SourceRecord` from Task 3.
- Produces: `DatasetWriter(root: Path, rows_per_file: int)`, `write(record: SourceRecord) -> None`, and `close() -> tuple[FileStat, ...]`.

- [ ] Write tests proving CSV/JSONL gzip formats, stable headers/JSON key ordering, correct date partitions, row rotation, per-part nondecreasing event time, and SHA-256 calculated from final compressed bytes.
- [ ] Run focused tests; expect missing-module failure.
- [ ] Implement streaming writers with deterministic gzip metadata (`mtime=0`) and explicit close semantics.
- [ ] Re-run focused and complete suites; expect all tests to pass.
- [ ] Commit `feat: write partitioned source-specific mock files`.

### Task 5: Manifest and end-to-end dataset generation

**Files:**
- Create: `src/marketpulse_mock/manifest.py`
- Create: `src/marketpulse_mock/generator.py`
- Create: `tests/test_generator.py`

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: `generate_dataset(config: GenerationConfig, overwrite: bool = False) -> Manifest` and stable `manifest.json`.

- [ ] Write tests for deterministic two-run manifests, seed variation, expected dataset presence, row/anomaly totals, non-empty-directory refusal, safe exact-directory overwrite, and bounded generation using rotated files.
- [ ] Run focused tests; expect missing-module failure.
- [ ] Implement orchestration, manifest serialization, checksums, totals, and safe output handling.
- [ ] Re-run focused and complete suites; expect all tests to pass.
- [ ] Commit `feat: assemble reproducible mock datasets`.

### Task 6: Streaming validator and CLI

**Files:**
- Create: `src/marketpulse_mock/validator.py`
- Create: `src/marketpulse_mock/cli.py`
- Create: `src/marketpulse_mock/__main__.py`
- Create: `tests/test_validator.py`
- Create: `tests/test_cli.py`

**Interfaces:**
- Consumes: generated directory and manifest from Task 5.
- Produces: `validate_dataset(root: Path) -> ValidationReport`, CLI commands `profiles`, `generate`, and `validate`, with exit code `0` on success and `2` on validation/configuration failure.

- [ ] Write tests for a valid fixture plus checksum tampering, missing foreign key, broken purchase amount, aggregate mismatch, and CLI error/success outputs.
- [ ] Run focused tests; expect missing-module failure.
- [ ] Implement streaming validation and `argparse` CLI.
- [ ] Re-run focused and complete suites; expect all tests to pass.
- [ ] Commit `feat: validate and operate mock datasets from cli`.

### Task 7: Fixtures, documentation, and architecture sync

**Files:**
- Create: `data/mock/fixtures/seed-360/**`
- Create: `data/mock/README.md`
- Create: `README.md`
- Modify: `docs/architecture/high-level-design.md`
- Modify: `specs/README.md`
- Test: complete suite plus CLI generation/validation commands.

**Interfaces:**
- Consumes: CLI from Task 6.
- Produces: committed fixture, operator instructions, architecture traceability, and local demo dataset.

- [ ] Generate the fixture profile with seed `360`, validate it, and confirm its total committed size is reasonable.
- [ ] Generate `demo` with seed `360` under ignored `data/generated/demo-seed-360` and validate it.
- [ ] Document commands, source layouts, scale/capacity expectations, and later Kafka replay boundary.
- [ ] Update the architecture with the implemented mock-source flow and change MP-01 status to Implemented with verification evidence.
- [ ] Run `python3 -m unittest discover -s tests -v`, fixture validation, demo validation, `git diff --check`, and verify no ignored large data is staged.
- [ ] Commit `docs: add reproducible mock data fixtures and guidance`.
