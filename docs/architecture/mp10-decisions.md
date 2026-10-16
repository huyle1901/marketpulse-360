# MP-10 evaluation decisions

The comparison is an optional, isolated Compose deployment. It owns fresh Kafka
topics per run and separate engine/database volumes. Production lakehouse,
serving publications, application metadata and existing user `work/` are untouched.
Credentials are file-backed under ignored `.secrets/`; host clients read files.

Flink and Streams execute the same bounded finance reducer but keep state in
their own native stores. An independent Python oracle checks each committed
result, including classifications and intermediate exact money. This measures
the engine adapters, Kafka transaction policy and state backend on a controlled
workload. It does not establish equivalence of production sessionization,
watermarks, governance, TTL or all six MP-03 outputs. Production Flink remains.

One partition and one processing lane avoid different interleavings. Both engines
commit at one-second intervals and write exactly-once Kafka output. Flink consumes
two JVM services (JobManager and TaskManager); Streams one process. Report both
Flink processes' resource observations and Kafka separately. Resource measurements
are cgroup CPU counter deltas and sampled working set, with coverage and reset
flags. CPU accumulated between a last sample and an abrupt process/container
restart can be missed; recovery resource values explicitly carry that limitation.

Producer start is gated on all Flink operators RUNNING or the Streams state
listener's RUNNING marker. A fixed sleep is not an equivalent readiness gate.
Fresh jobs/processes still include JIT effects; separate warmups are not claimed
to warm the new JVM. Source/output protocol, transaction interval and scale are
held fixed while reporting adapter and process-topology differences.

The first real Docker warmup exposed a connector ABI constraint: Flink Kafka
connector5.0.0-2.2 calls `TransactionalRequestResult.await()` without arguments,
removed from kafka-clients4.3. Therefore two shaded artifacts isolate client4.2.0
for Flink and client4.3.1 for Streams. A reflection regression checks the Flink
transaction ABI; both builds run topology/reducer tests. The Streams JAR sits
outside Flink's `usrlib` to prevent accidental classpath mixing. These client
versions are reported as part of the measured adapters, not hidden as equivalent.

Pipeline latency is host send to committed observer, including queueing and
transaction visibility. Authenticated app response timing is a different metric.
Neither substitutes for production event-to-dashboard propagation. Historical
fixture event age is not a performance sample. Recovery requires a live producer,
input advancement, new post-interruption output and checkpoint restore for Flink.

Analytical scale fixtures repeat an explicitly specified integer distribution;
they are not 20 million independent real customers. Both engines receive equal
rows and integer queries checked against periodic independent Python answers.
PostgreSQL gets a composite campaign/day index, ClickHouse a corresponding sort
key. Both are capped at 1.5 GiB and run serial queries with warm caches. The test
machine also runs the retained demo services; measurements describe this local
environment and do not establish production capacity or a universal winner.

Group size, representative human tests and a narrated 20–30 minute final video
cannot be inferred from automated checks. Keep these acceptance items pending
until real group information and recordings exist. A PDF report and slide deck
are reviewable generated artifacts; a storyboard is not a finished video.

The final independent review found two evidence-gate issues, both fixed with
RED→GREEN regressions: all reported durations/counters/throughput must have valid
types and finite values, and each analytical HTTP response must match the initial
release identity plus proof digest before dashboard/preview values are wrapped.
The latter also rejects release changes followed by rollback, missing provenance
and proof-only changes during timed reads. Real retained-app checks were rerun;
the full423-test Python suite succeeds with40opt-in integrations skipped. There
were no Critical or deferred Minor review findings.

Primary implementation references:

- [Kafka Streams Processor API](https://kafka.apache.org/43/streams/developer-guide/processor-api/)
- [Streams application/dependency guide](https://kafka.apache.org/43/streams/developer-guide/write-streams-app/)
- [Flink 2.2 checkpoints](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Flink savepoints and state identity](https://nightlies.apache.org/flink/flink-docs-release-2.2/docs/ops/state/savepoints/)
