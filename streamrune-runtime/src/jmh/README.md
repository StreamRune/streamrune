# StreamRune JMH benchmarks

Micro-benchmarks for the framework's hot paths. Three modules have them, each in its own `src/jmh`:

| Module | Benchmarks | Needs |
| --- | --- | --- |
| `streamrune-runtime` (this directory) | `StreamRuneBenchmark`, `TracingBenchmark`, `OutboxEventMapperBenchmark`, `ProjectionRunnerBenchmark`, `EventSerializationBenchmark`, `PerfCandidatesBenchmark` | nothing |
| `streamrune-eventstore/streamrune-postgres` | `PostgresEventStoreBenchmark`: append, load, load through an upcaster chain | PostgreSQL: Docker, or a server named in `STREAMRUNE_BENCH_JDBC_URL` |
| `streamrune-crypto/streamrune-filesystem-crypto` | `CryptoShreddingBenchmark`: serialization of `@Encrypted` fields on the AES-GCM engine | nothing |

The README in each `src/jmh` describes its benchmarks.

## Running

```bash
./gradlew :streamrune-runtime:jmh                                       # every benchmark of a module
./gradlew :streamrune-runtime:jmh -PjmhIncludes=ProjectionRunnerBenchmark  # those whose name matches a regex
./gradlew :streamrune-eventstore:streamrune-postgres:jmh                 # starts a Testcontainers PostgreSQL
./gradlew :streamrune-crypto:streamrune-filesystem-crypto:jmh
```

Each run writes its results as JSON to `build/results/jmh/results.json` of the module and fails
on a benchmark that throws. Without further properties every benchmark runs with the mode, unit,
forks and iterations of its own annotations.

`-PbenchCi` gives every benchmark the settings of the nightly run: one mode and unit, average time
per operation in microseconds, two forks of three warmup and five measurement iterations of one
second each, and leaves out `PerfCandidatesBenchmark`, whose A/B pairs are evidence for decisions
already taken, not a regression baseline. The root `build.gradle.kts` holds this setup for every
module in `jmhModules`; when one build runs the `jmh` tasks of several modules, they run one after
another in that order.

```bash
./gradlew :streamrune-runtime:jmh -PbenchCi
```

## The nightly run and its history

`.github/workflows/benchmark.yml` runs the `jmh` tasks of the three modules with `-PbenchCi` every
night at 01:30 UTC (and by hand, from the Actions tab), against a PostgreSQL 18 service for the
PostgreSQL benchmarks. It merges the three result files into one, keeps it 90 days as the run's
`jmh-results` artifact, and hands it to github-action-benchmark, which appends it to the history in
`dev/bench/data.js` on the `gh-pages` branch. Only a run on `main` adds to the history. Once GitHub
Pages serves that branch, the history is a chart per benchmark at
<https://streamrune.github.io/streamrune/dev/bench/>; every value is a time per operation, so lower
is better.

When a benchmark takes more than 130% of its previous recorded time, the action comments on the
commit; the run does not fail. Shared GitHub runners vary by roughly 10–20% between runs, so the
nightly numbers catch large regressions; a smaller change shows as a trend over several points, and
a run of the benchmark on both commits on one machine confirms it.

## In `check`

Benchmarks are **not** run under the JMH harness by `check` or `build` — they assert nothing and are
slow. What `check` does enforce is that they still **compile** (`compileJmhJava`, wired in by
the root `build.gradle.kts`) and that every benchmark method still **runs**: the `jmhSmokeTest` task
of each module executes the JUnit smoke tests beside its benchmarks (`StreamRuneBenchmarkSmokeTest`,
`TracingBenchmarkSmokeTest`, `ProjectionRunnerBenchmarkSmokeTest` and
`EventSerializationBenchmarkSmokeTest` here), which invoke
each method twice against a real `@Setup` state, outside the harness, to catch a benchmark that
compiles but throws or measures a degenerate state. The compile gate makes an API change that breaks a benchmark fail `check`: nothing
else in the default task graph compiles this source set. The PostgreSQL smoke test starts a
Testcontainers container, so `streamrune-postgres`'s `check` needs Docker, as its tests do.

`jmhJar` is incompatible with Gradle's configuration cache (the JMH plugin holds a `Project`
reference). It is declared as such in the root `build.gradle.kts`, so a `jmh` invocation degrades to
running without the configuration cache instead of failing. `check` is unaffected — it depends on
`compileJmhJava`, not on `jmhJar`.

## What each class measures

### `PerfCandidatesBenchmark`

Pairs of `@Benchmark` methods — the CURRENT mechanism vs. the PROPOSED one — under identical
JIT/machine conditions, so each delta is apples-to-apples. This is the evidence base for the
framework's landed performance work.

| Candidate | Measured | Outcome |
| --- | --- | --- |
| A. Upcast payload re-bind: `writeValueAsString` + `readValue` vs `convertValue` | 7497 → 2092 ns/op (~3.6x) | **Landed** in `PostgresEventStore.toEnvelope` |
| B. Cache-key hash: `MessageDigest.getInstance("SHA-256")` per call vs a reused digest | no measurable win | **Hypothesis killed** — `getInstance` is cheap; the digest itself dominates |
| C. AES/GCM `Cipher.getInstance` per encrypt vs a thread-local reused `Cipher` | 1330 → 341 ns/op (~3.9x) | **Not used** — the crypto engines take a fresh `Cipher` for every encryption, check and decryption attempt: a reused cipher carries its state from one operation to the next, and a fresh one confines a fault to the one operation the encryption check or the decryption retry catches. The engines pay about one microsecond per operation for that |
| D. Id RNG under 8 threads: shared `SecureRandom` vs thread-local `SecureRandom` vs `ThreadLocalRandom` | thread-local `SecureRandom` did not help | **Hypothesis refined** — `SecureRandom` is slow from entropy work, not lock contention; only a non-cryptographic RNG helps |

Candidate D's result is why `IdGenerator` moved to `ThreadLocalRandom`: ids are
correlation/causation/audit handles, never security capabilities, so unguessability is not
required. The `idGen_generateEventId` benchmark measures the **real** production method rather than
the isolated RNG, and that honesty matters — it reported 1.32 → 16.49 ops/µs (~12.5x) at 8 threads,
far below the ~274x isolated-RNG delta, because base64 and timestamp encoding dominate the per-id
cost.

Keeping B and D in the source set is deliberate. A benchmark that *disproved* a change is worth as
much as one that justified one: it stops the same optimization being re-proposed from intuition.

### `OutboxEventMapperBenchmark`

Cost of `OutboxEventMapper#toOutbox` on the outbox emit hot path, for a SKIPPED event (type-check
only, returns an empty list) vs a PUBLISHED event (one Jackson `writeValueAsString`). Added with the
transactional-outbox feature to establish the per-event overhead the outbox imposes on
every append, including for events that produce no outbox entry at all.

### `StreamRuneBenchmark`

Broad throughput/latency baselines rather than A/B comparisons: `VirtualThreadCommandBus.execute`
throughput, `EventStore` append/load over batch sizes 1/10/100, and `Decider.decide` over state
sizes 0/10/100. No landed change traces to these; they exist as a regression baseline.

### `TracingBenchmark`

What tracing adds to one command: `VirtualThreadCommandBus.execute` on the serializing
`InMemoryEventStore`, with the bus, its default locker and the store traced, for three values of the
`tracing` parameter. `NOOP` traces nothing (the default of every builder). `SAMPLER_OFF` is
`OpenTelemetryTracing` over an SDK whose sampler drops every trace, so the spans start but are not
recorded. `SAMPLER_ON` records the six spans of the command and hands them to a batch span processor
whose exporter discards them. Compare `SAMPLER_OFF` and `SAMPLER_ON` with `NOOP`, not with
`StreamRuneBenchmark.commandBusThroughput`, whose store keeps its events by reference.

### `ProjectionRunnerBenchmark`

The cost of one projection batch: `PollingProjectionRunner` catching a `TRANSACTIONAL_LOCAL`
`BaseProjection` up over 10 or 100 events of an `InMemoryEventStore` as one batch — the read of the
global stream, the batch through the `AtomicBatchProcessor`, a read and a write of a read model per
event, and the checkpoint. The number is one catch-up call of `PollingProjectionRunner.run`,
including what the method does once per call besides the batch — the delivery-policy check and the
INFO line naming the delivery mode — so the logging configuration on the classpath affects it (the
module's `logback-test.xml` logs at WARN, which filters the line). The processor and offset store are one `InMemoryProjectionRepository`, so
the cost is the runner's and the projection's own, without a database. Each invocation rewinds the
checkpoint and processes the same events again; the projection writes each read model from its event
alone, so every invocation meets the same state. The polling runner is the one that returns once the
projection has caught up; the continuous runner of `MultiProjectionRunner` hands its batches to the
same processor and projection call from a loop that runs until it is stopped.

### `EventSerializationBenchmark`

Event deserialization and upcasting without a database: a load of 100 events from an
`InMemoryEventStore` in its serializing mode (`InMemoryEventStore.serializing(...)` with
`withUpcasters(...)`), stored at schema version 3, 2 or 1 and read by a reader that expects version 3
— 0, 1 or 2 upcast steps per event (`upcastSteps`). The serializing mode reads events the way
`PostgresEventStore` does: `readValue` for an event at the current version; for an older one
`readValue` to a map, `UpcasterChain.upcast`, then `convertValue`.

Two benchmarks cover upcasting, on purpose. This one is the low-noise CPU signal of the Jackson and
upcaster-chain mechanism. `PostgresEventStoreBenchmark.loadWithUpcasting` in `streamrune-postgres` is
the production path — the same mechanism inside `PostgresEventStore`, behind a database round trip
whose cost and variance hide a single upcast step. Read a change in upcasting cost here, and confirm
on the PostgreSQL benchmark that the production path moves with it.

## A note on drift

Benchmarks compile against the framework's real API, so an API change breaks them exactly as it
breaks a test. If an API change breaks these files, `check` says so.
