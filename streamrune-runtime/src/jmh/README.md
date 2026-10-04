# StreamRune JMH benchmarks

Micro-benchmarks for the framework's hot paths. They exercise the underlying JDK/Jackson
mechanism rather than the full production object graph, so they need no database and are
reproducible on a laptop.

## Running

```bash
./gradlew :streamrune-runtime:jmh    # all benchmarks
```

Benchmarks are **not** run under the JMH harness by `check` or `build` — they assert nothing and are
slow. What `check` does enforce is that they still **compile** (`compileJmhJava`, wired in by
the root `build.gradle.kts`) and that every `StreamRuneBenchmark` method still
**runs**: the `jmhSmokeTest` task executes `StreamRuneBenchmarkSmokeTest`, which invokes each of
those methods twice against a real `@Setup` state, outside the harness, to catch a benchmark that
compiles but throws or measures a degenerate state. The compile gate makes an API change that breaks
a benchmark fail `check`: nothing else in the default task graph compiles this source set.

`jmhJar` is incompatible with Gradle's configuration cache (the JMH plugin holds a `Project`
reference). It is declared as such in `streamrune-runtime/build.gradle.kts`, so a `jmh` invocation
degrades to running without the configuration cache instead of failing. `check` is unaffected — it
depends on `compileJmhJava`, not on `jmhJar`.

## What each class measures

### `PerfCandidatesBenchmark`

Pairs of `@Benchmark` methods — the CURRENT mechanism vs. the PROPOSED one — under identical
JIT/machine conditions, so each delta is apples-to-apples. This is the evidence base for the
framework's landed performance work.

| Candidate | Measured | Outcome |
| --- | --- | --- |
| A. Upcast payload re-bind: `writeValueAsString` + `readValue` vs `convertValue` | 7497 → 2092 ns/op (~3.6x) | **Landed** in `PostgresEventStore.toEnvelope` |
| B. Cache-key hash: `MessageDigest.getInstance("SHA-256")` per call vs a reused digest | no measurable win | **Hypothesis killed** — `getInstance` is cheap; the digest itself dominates |
| C. AES/GCM `Cipher.getInstance` per encrypt vs a thread-local reused `Cipher` | 11781 → 3148 ns/op (~3.7x) | **Landed** in `FileSystemCryptoEngine` + `PostgresCryptoEngine` |
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

## A note on drift

Benchmarks compile against the framework's real API, so an API change breaks them exactly as it
breaks a test. If an API change breaks these files, `check` says so.
