# MP-10 evaluation and final evidence

Use Python3.12+, Java17/Maven (or the pinned Maven Docker build stage), Docker
Compose with BuildKit and enough disk for the20M PostgreSQL load plus WAL.
Run from the existing source root. The retained app is not reset or republished.

```bash
uv pip install --python .venv/bin/python -e '.[evaluation]'
export PYTHONPATH=src
.venv/bin/python scripts/evaluate.py --output .lakehouse/mp10-proof/setup.json setup
docker compose -f compose.evaluation.yaml --profile evaluation build jobmanager
docker compose -f compose.evaluation.yaml --profile evaluation up -d --wait \
  kafka clickhouse postgres
```

Setup preserves existing evaluation credentials, generates only a missing file
and never prints its contents. Benchmark endpoints are loopback49392(Kafka),
49381(Flink),49323(ClickHouse),49332(PostgreSQL). Do not expose these on a shared
network. The separate Compose project is `marketpulse-evaluation`; the runner
hardcodes that ownership and cannot target the primary deployment.

Run a separate warmup for each adapter, then measured runs serially:

```bash
for engine in flink streams; do
  .venv/bin/python scripts/evaluate.py --output ".lakehouse/mp10-proof/warmup-${engine}.json" \
    stream --engine "$engine" --scale eval-small --warmup
done
for scale in eval-small eval-medium; do
  for engine in flink streams; do
    .venv/bin/python scripts/evaluate.py --output ".lakehouse/mp10-proof/stream-${engine}-${scale}.json" \
      stream --engine "$engine" --scale "$scale"
  done
done
for engine in flink streams; do
  .venv/bin/python scripts/evaluate.py --output ".lakehouse/mp10-proof/recovery-${engine}.json" \
    stream --engine "$engine" --scale eval-small --recovery --eps 300
done
```

Every run creates unique topics, application ID and transactional prefix.
`read_committed` outputs must match every independent oracle result. Recovery
requires committed progress, a live producer, advancing input, newly committed
post-interruption records and (Flink) completed/restored checkpoint evidence.
Warmups are excluded from tables. Each measured process/job is fresh; it still
includes JIT effects. Do not mistake warmup labels for a long-lived warmed JVM.
Normal completion cancels only that Flink job or removes that named Streams
container. Kafka/checkpoint/state evidence remains for inspection. On an
interrupted/failed run inspect `docker compose ... ps` and Flink jobs; cancel only
the run-owned job if its CLI cleanup did not finish. Do not submit concurrent jobs.

Both adapters use a common benchmark reducer but separate shaded Kafka clients:
Flink connector5.0.0-2.2 requires client4.2.0; Streams4.3.1 uses client4.3.1.
The Docker build tests both profiles; never put the Streams JAR in Flink usrlib.

```bash
for scale in analytics-1m analytics-10m analytics-20m; do
  for engine in clickhouse postgres; do
    .venv/bin/python scripts/evaluate.py --output ".lakehouse/mp10-proof/analytics-${engine}-${scale}.json" \
      analytics --engine "$engine" --scale "$scale"
  done
done
```

Three ordered queries each use20warmups/100samples, exact independent answers,
warm buffer caches and no ClickHouse result cache. Each run creates/drops only
its unique `eval_<random>` table in the isolated database. Failed timings do not
produce passed evidence. Resource sampling uses host Docker CLI and cgroups;
no container receives the Docker socket. CPU seconds may undercount an abrupt
restart and working set is a sampled maximum, not an exact instantaneous peak.

Read-only application decision checks use existing operator credentials:

```bash
.venv/bin/python scripts/evaluate.py --output .lakehouse/mp10-proof/app.json app \
  --api http://localhost:48000 --origin http://localhost:43100 \
  --username mp09-operator --password-file .secrets/operator_password
.venv/bin/python scripts/evaluate.py --output .lakehouse/mp10-package package \
  --evidence .lakehouse/mp10-proof
```

Add `--group-size N` only with the real group count. This check expects the
retained seed-360 release and configured MP06 campaign/MP08 saved segment from
prior verified demos. It never creates missing budgets/rules/segments. Normal
login/audit metadata is written by the authenticated app. It fails on changed
finance, missing live keys/segment revisions or mixed release identity. API times
are automation durations, not human task times.

Packaging requires both processing scales, both recovery proofs, all three SQL
scales/engines and all ten decisions. It copies raw JSON and produces report.md,
report.pdf, slides.pdf, task sheet, storyboard and index.json with content hashes.
The package remains a draft until real group ID, representative human observations
and narrated20–30minute video exist. See `docs/evaluation/README.md`.

Fresh verification:

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover -s tests -v
JAVA_HOME=/opt/homebrew/opt/openjdk@17 mvn -q -f evaluation-job/pom.xml test
JAVA_HOME=/opt/homebrew/opt/openjdk@17 mvn -q -f evaluation-job/pom.xml test \
  -Dkafka.clients.version=4.3.1 -Devaluation.engine=streams
JAVA_HOME=/opt/homebrew/opt/openjdk@17 mvn -q -f flink-job/pom.xml test
PYTHONPATH=src .venv/bin/python -m marketpulse_mock validate --input data/mock/fixtures/seed-360
(cd web && npm test && npm run typecheck && npm run build)
docker compose -f compose.evaluation.yaml --profile evaluation config --quiet
git diff --check
```

On other Java17 installations set the appropriate JAVA_HOME or rely on the
Docker build. Stop only the isolated benchmark services when finished:

```bash
docker compose -f compose.evaluation.yaml --profile evaluation stop
```

Do not run `down -v` when preserving evidence. Do not apply benchmark cleanup to
the retained `marketpulse-mp05-proof` application project.
