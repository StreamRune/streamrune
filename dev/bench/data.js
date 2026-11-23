window.BENCHMARK_DATA = {
  "lastUpdate": 1795426853672,
  "repoUrl": "https://github.com/StreamRune/streamrune",
  "entries": {
    "StreamRune JMH": [
      {
        "commit": {
          "author": {
            "name": "Martin Bednář",
            "username": "xxBedy",
            "email": "martin.bednar@streamrune.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a36e28a2d634f08cd6f10401ee21da2160021684",
          "message": "ci(bench): nightly benchmarks with a history on gh-pages, and benchmarks for the Postgres store, crypto, serialization and projections (#99)\n\n## What changed\n\n**Benchmarks.** Four new JMH families, so the regression baseline covers the production hot paths and not only the in-memory ones:\n- `streamrune-postgres` (new `src/jmh`): `PostgresEventStore` append of 1 and 10 events, to a fresh and to an existing stream, and load of 10 and 100 events, against a real PostgreSQL built from the framework's migrations. `loadWithUpcasting` runs 1 and 2 upcast steps on that read path.\n  - Each trial recreates the schema, and each append iteration truncates the event table, so the database does not grow across iterations.\n  - Locally the benchmark starts a Testcontainers PostgreSQL. In CI it takes the `STREAMRUNE_BENCH_JDBC_URL` / `_USER` / `_PASSWORD` service container.\n- `streamrune-filesystem-crypto` (new `src/jmh`): serialization (encrypt) and deserialization (decrypt) of events with one and several `@Encrypted` fields. It goes through the production Jackson crypto-shredding path on the real AES-GCM filesystem engine, with keys in a temporary directory.\n- `streamrune-runtime`:\n  - `EventSerializationBenchmark`: loads 100 events through `InMemoryEventStore.serializing(...)` with 0, 1 and 2 upcast steps. It runs the same Jackson and `UpcasterChain` mechanism as `PostgresEventStore` without a database, so it is the low-noise CPU signal. The Postgres variant stays the production path.\n  - `ProjectionRunnerBenchmark`: one catch-up call of `PollingProjectionRunner.run` over a batch of events.\n\nEach module has a smoke test that runs every benchmark method twice outside the harness, wired into `check` (`jmhSmokeTest`). The Postgres smoke test follows `PostgresTestImage`.\n\n**JMH setup.** The JMH configuration is shared by the three modules, and results are written as JSON.\n- `-PbenchCi` forces one mode and unit for every benchmark (average time, µs/op) with fixed fork, warmup and measurement settings, and leaves out `PerfCandidatesBenchmark`.\n- `-PjmhIncludes=<regex>` selects benchmarks. It replaces a `-Pjmh.include` that two javadocs named but the plugin never supported.\n\n**`.github/workflows/benchmark.yml`.** Nightly at 01:30 UTC and on demand. It has two jobs.\n- `benchmark`:\n  - runs the three modules' JMH suites with `-PbenchCi` against a `postgres:18-alpine` service;\n  - merges the JSON results and uploads them as an artifact kept for 90 days;\n  - has a read-only token, and its checkout does not keep credentials.\n- `record` (`contents: write`, runs no build code):\n  - feeds the results to `benchmark-action/github-action-benchmark` v1.22.2, pinned by commit SHA;\n  - attributes each data point to the benchmarked commit (`ref: github.sha`);\n  - pushes the history to `gh-pages` (`dev/bench`) only for runs on `main`;\n  - comments on the commit when a benchmark is more than 30% slower than the previous point. It does not fail the run and mentions nobody.\n  - It records each commit once: when `main` has not moved, it adds no point and no repeated comment.\n  - The first run on `main` creates `gh-pages` as one empty commit. Once GitHub Pages serves that branch, the chart is at https://streamrune.github.io/streamrune/dev/bench/.\n  - Shared runners vary by about 10–20%, which is why the threshold is 130%.\n- Every action is pinned to a commit SHA.\n\n**Docs.**\n- The runtime JMH README covers every benchmark, local runs, `-PbenchCi` and the nightly history. Each new `src/jmh` has its own README.\n- CONTRIBUTING.md mentions the nightly benchmarks.\n- `production.md` no longer calls the JMH suite in-memory only.\n\n## Why\n\nThe JMH benchmarks ran only by hand and their numbers were kept nowhere, so a gradual slowdown would go unnoticed. Now every commit on `main` gets a nightly data point, and a large regression produces a comment on the commit.\n\n## How it was tested\n\n- `./gradlew build`: BUILD SUCCESSFUL, 9076 tests, 0 failed. This includes the three `jmhSmokeTest` tasks and the coverage gates.\n- The workflow's commands were run locally with `-PbenchCi` against a `postgres:18-alpine` container through the `STREAMRUNE_BENCH_*` variables:\n  - the JMH runs exited 0;\n  - the workflow's `jq` merge gives 32 results, all avgt in µs/op;\n  - the benchmark time is about 9 minutes, an estimated 14–18 minutes on `ubuntu-latest`.\n- actionlint 1.7.12 (with shellcheck) is clean.\n- The record step's history check was run against a valid, an empty, a malformed and a missing `data.js`. The workflow itself first runs after merge.\n- A review covered:\n  - workflow permissions;\n  - every pinned SHA, peeled through the API;\n  - the action's smaller-is-better semantics for JMH, checked in its source;\n  - the gh-pages bootstrap, replayed against a local bare remote;\n  - benchmark validity and the build integration.\n  - Its findings are fixed and it approves.\n\n\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-11-10T19:02:11Z",
          "url": "https://github.com/StreamRune/streamrune/commit/a36e28a2d634f08cd6f10401ee21da2160021684"
        },
        "date": 1794338048013,
        "tool": "jmh",
        "benches": [
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"0\"} )",
            "value": 190.0896966605098,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 272.47453344857644,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 283.16467435965404,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapPublishedEvent",
            "value": 0.31364210178476104,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapSkippedEvent",
            "value": 0.0012291222526578985,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"10\"} )",
            "value": 3.6440088257976675,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"100\"} )",
            "value": 13.133434087759118,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.commandBusThroughput",
            "value": 15.413971747813116,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.deciderDecide ( {\"stateSize\":\"0\"} )",
            "value": 0.048305348381744946,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.deciderDecide ( {\"stateSize\":\"10\"} )",
            "value": 0.04819076840778043,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.deciderDecide ( {\"stateSize\":\"100\"} )",
            "value": 0.04834281313151162,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"1\"} )",
            "value": 13.649521408471756,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"10\"} )",
            "value": 137.93009941424268,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"100\"} )",
            "value": 1395.201110001343,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"1\"} )",
            "value": 0.12927910759441574,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"10\"} )",
            "value": 0.2990665167943255,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"100\"} )",
            "value": 2.1286330599408876,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"1\"} )",
            "value": 0.21266435656051405,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"10\"} )",
            "value": 0.2183187026588072,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"100\"} )",
            "value": 0.35329359407981176,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"1\"} )",
            "value": 1293.7936157492834,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"10\"} )",
            "value": 1618.9453213194834,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"1\"} )",
            "value": 1294.951055113025,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"10\"} )",
            "value": 1593.9748968040808,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"10\"} )",
            "value": 624.4792515131237,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"100\"} )",
            "value": 1199.8051117140294,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 1296.6326781319517,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 1311.0866132377262,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 13.929475638719802,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 65.26532769133338,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 22.253546323448948,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 109.27696725788,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Martin Bednář",
            "username": "xxBedy",
            "email": "martin.bednar@streamrune.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "a32ee27a8e22ede941fccbfcce02488c96772f86",
          "message": "feat(tracing)!: events carry their command's span, outbox/publish links to it, and the tracing seam takes no nulls (#102)\n\n## What changed\n\nTrace links that work without application help, and a tracing seam without nulls.\n\n**Events carry the span of their command**\n- `EventMetadata` gains `spanContext` (a `SpanLink`, `SpanLink.NONE` by default, never null). The command bus writes the current command span into it. It reads that span from the first `OpenTelemetryCommandInterceptor` in the chain whose command area is on, whether the builder added the interceptor or the application registered it.\n- `TraceScope.link()` returns the ids of the span a scope started, or `NONE`. A span the sampler dropped still yields its link when its context is valid, so links hold under parent-based sampling.\n- Every link reads only `spanContext`, through `SpanLink.of(EventMetadata)`. This covers `subscription/{name}/deliver`, `saga/{type}/step` (live and replay), and the new `outbox/publish` link.\n- `traceId` (the caller's `X-Trace-Id`) is correlation data and never builds a link. Before this change the bus always wrote a null `spanId`, so no link ever formed with the shipped command bus.\n- What a client can influence is documented: the span id is always the server's. The trace id follows the request's OpenTelemetry context, so a `traceparent` header that the server's instrumentation accepts chooses it. No StreamRune header, baggage entry or request-context value is copied into it.\n\n**`outbox/publish` links to the command that wrote the entry**\n- `OutboxEntry` gains `traceLink()` (`NONE` by default). `outbox_events` gains nullable `trace_id` / `span_id` columns, added in the V001 baseline (pre-1.0).\n- In the append path the store always sets the entry's link from the source event's `spanContext`, so a mapper or caller cannot plant a link.\n- The relay passes the link to each entry's `outbox/publish` span.\n\n**The seam takes no nulls**\n- `SpanLink.NONE` (with `isPresent()`) replaces a null link everywhere.\n- The new `SpanBaggage` (`KEEP` / `replace(Map)`) replaces the nullable baggage parameter.\n- With `SpanAttributes.empty()`, those are the seam's empty values. `start(...)` rejects a null link, baggage or start time.\n- `NONE` and `KEEP` never reach OpenTelemetry. The output of the existing emitters is unchanged apart from the new links.\n\n**Robustness**\n- A malformed stored `spanContext` (a string, number, array, empty object, wrong types or invalid ids) reads back as `NONE`. Loading and replay never fail on it.\n- Only W3C ids are written, and `NONE` is left out of JSON entirely. The Spring and Quarkus admin outbox listings therefore show no `traceLink` for an unlinked entry.\n- A tracing whose `link()` throws yields `NONE` and never fails a command.\n\n**Docs:** `production.md` (the \"What a client controls\" table, links, the outbox link, the seam's empty values), `api.md`, `outbox.md`, the Javadoc of `EventMetadata` / `SpanLink` / `StreamRuneTracing` / `OutboxEntry`, and the CHANGELOG lines for every change above. The CHANGELOG also corrects the earlier claim that \"trace and span ids propagate into `EventMetadata`\".\n\n## Why\n\nThe subscription, saga and outbox links never formed with the framework's own command bus. A link built from the caller-supplied `traceId` would let a client attach its events to any trace. The null objects make the seam safe to implement and call before the 1.0 freeze.\n\n## How it was tested\n\n- `./gradlew build`: BUILD SUCCESSFUL, 9305 tests, 0 failed. `brokerTest`: 4/4, and every `outbox/publish` span carries its entry's link.\n- New tests, none of them with hand-built metadata:\n  - a command's events carry its span: through `.tracing(...)` and through `.interceptors(...)`; for a command dispatched from an interceptor's `after()` while the outer span is open; and for `executeAsync`;\n  - the subscription, saga step and outbox publish spans link to the command span;\n  - a client `X-Trace-Id` never reaches a link;\n  - NOOP or an area switched off gives `NONE`;\n  - a real bus against PostgreSQL stores the same link in `spanContext` and in the outbox columns;\n  - every malformed JSON shape reads back as `NONE`;\n  - a mapper- or caller-set outbox link is replaced.\n- Mutant checks (restored):\n  - a `peekLast()` in the hand-off fails the nested-command test;\n  - the old map-only JSON reader fails 8 malformed-value cases;\n  - a bus writing `NONE` fails 5 tests.\n- A review covered the security of the span context, the hand-off under nesting, retries, sagas, async and virtual threads, the null objects, persistence and JSON. Its findings are fixed.\n- The e-commerce demo builds and passes its tests against this branch, with no change needed.\n\n\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-11-12T06:01:48Z",
          "url": "https://github.com/StreamRune/streamrune/commit/a32ee27a8e22ede941fccbfcce02488c96772f86"
        },
        "date": 1794473501098,
        "tool": "jmh",
        "benches": [
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"0\"} )",
            "value": 109.1118015927248,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 170.29923938976842,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 171.7004615168467,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapPublishedEvent",
            "value": 0.17666903717202215,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapSkippedEvent",
            "value": 0.0009553135351049738,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"10\"} )",
            "value": 1.9217162363797933,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"100\"} )",
            "value": 7.418297826233294,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.commandBusThroughput",
            "value": 10.975261645539645,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.deciderDecide ( {\"stateSize\":\"0\"} )",
            "value": 0.03476003566039865,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.deciderDecide ( {\"stateSize\":\"10\"} )",
            "value": 0.034197067649755615,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.deciderDecide ( {\"stateSize\":\"100\"} )",
            "value": 0.03401627319040573,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"1\"} )",
            "value": 9.420400169468072,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"10\"} )",
            "value": 93.50110364002296,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"100\"} )",
            "value": 970.0653930166831,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"1\"} )",
            "value": 0.06398396848366064,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"10\"} )",
            "value": 0.1700732902656616,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"100\"} )",
            "value": 1.404824258910847,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"1\"} )",
            "value": 0.11805474389743101,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"10\"} )",
            "value": 0.12533837625909677,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"100\"} )",
            "value": 0.18006766384350664,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"NOOP\"} )",
            "value": 11.297839927101304,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_OFF\"} )",
            "value": 13.267126575464909,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_ON\"} )",
            "value": 13.552179844033711,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"1\"} )",
            "value": 677.380825492323,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"10\"} )",
            "value": 854.5946162115475,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"1\"} )",
            "value": 673.081705150651,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"10\"} )",
            "value": 862.900159689971,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"10\"} )",
            "value": 293.33194262717575,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"100\"} )",
            "value": 626.3421346550406,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 684.9414524045743,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 684.5770642314229,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 10.567850966095792,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 48.140160369656755,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 14.677704694584014,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 72.4339194825985,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Martin Bednář",
            "username": "xxBedy",
            "email": "martin.bednar@streamrune.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "5c373d1a3423c9ca5359bc101c4b2fb738897bc2",
          "message": "ci(snapshot): repeat a rate-limited upload after a pause, up to three attempts (#106)\n\n## What changed\n\nThe Publish Snapshot workflow repeats a failed upload after a 180-second pause, up to three attempts in all. Each attempt publishes a complete new timestamped snapshot.\n\n## Why\n\nUploading one module at a time (StreamRune/streamrune#105) cut the `429 Too Many Requests` answers from the Central Portal snapshot repository down to one file per run. That single file still fails the run, and rerunning it at once hit the limit again. A pause lets the repository's rate window pass.\n\n## How it was tested\n\n- The workflow only runs on `main` after a green Build. The run after this merge is the test.\n- The change touches only the publish step.\n\n\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-11-12T12:20:15Z",
          "url": "https://github.com/StreamRune/streamrune/commit/5c373d1a3423c9ca5359bc101c4b2fb738897bc2"
        },
        "date": 1794504543408,
        "tool": "jmh",
        "benches": [
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"0\"} )",
            "value": 141.3982331622555,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 216.024321624449,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 217.5944554448587,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapPublishedEvent",
            "value": 0.23754522973807987,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapSkippedEvent",
            "value": 0.0009270123438042651,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"10\"} )",
            "value": 2.3003834869448485,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"100\"} )",
            "value": 10.33650804297225,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.commandBusThroughput",
            "value": 13.062324396807966,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.deciderDecide ( {\"stateSize\":\"0\"} )",
            "value": 0.040621752318527785,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.deciderDecide ( {\"stateSize\":\"10\"} )",
            "value": 0.04066619164184432,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.deciderDecide ( {\"stateSize\":\"100\"} )",
            "value": 0.04064654023728135,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"1\"} )",
            "value": 11.64156071316265,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"10\"} )",
            "value": 113.54396757877718,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"100\"} )",
            "value": 1136.3027256356725,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"1\"} )",
            "value": 0.08966825018210213,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"10\"} )",
            "value": 0.23773904760974385,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"100\"} )",
            "value": 1.749416480584036,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"1\"} )",
            "value": 0.1667728529787455,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"10\"} )",
            "value": 0.16689825519882623,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"100\"} )",
            "value": 0.26136393999765306,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"NOOP\"} )",
            "value": 13.43727285902404,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_OFF\"} )",
            "value": 15.770953700415351,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_ON\"} )",
            "value": 16.40961852513692,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"1\"} )",
            "value": 977.4648773410379,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"10\"} )",
            "value": 1338.0851015094238,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"1\"} )",
            "value": 1046.4185762959976,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"10\"} )",
            "value": 1158.3416033263907,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"10\"} )",
            "value": 392.52485997418876,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"100\"} )",
            "value": 790.0473400555068,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 862.6993346683103,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 878.7674285421224,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 11.705833914443959,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 54.413400485928705,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 17.0460263079719,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.crypto.filesystem.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 83.38056325113118,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Martin Bednář",
            "username": "xxBedy",
            "email": "martin.bednar@streamrune.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "f3d720219452e2fcae828c220eec5696c8d07b80",
          "message": "fix(tracing): what a span records, one trace per poll, async and switched-off SDK, vendor-neutral names, and texts that say what is true (#110)\n\nTracing data, semantics, names and texts: what a span records, which spans a deployment gets, and what the documentation says about both.\n\n## What changes\n\n**Names**\n- `OpenTelemetryCommandInterceptor` is `TracingCommandInterceptor`, and `CommandInterceptorOrdering.ORDER_OPEN_TELEMETRY` is `ORDER_TRACING`: the interceptor takes a `StreamRuneTracing` and names no OpenTelemetry type, like `TracingEventListener` and `TracingProjectionDecorator`. Breaking rename, no alias (pre-1.0); `api/streamrune-runtime.api` records it.\n\n**What a span records**\n- An exception's own message passes the free-text sanitizer and the 256-character trace cap wherever a span records it: the `exception.message` attribute, the first line of `exception.stacktrace` and the status description.\n- A command that a `DomainException` rejects (validation and authorization included) keeps the unset status, records no exception event and no message, and carries `streamrune.command.rejection.type`. A saga forward command that the forward fence refuses also keeps the unset status, with `streamrune.command.outcome` `FENCED`, which is the outcome its append span already records. Every other failure records the exception and the `ERROR` status. An operator who alerts on failed spans is not paged for a command's expected answer, as with a `4xx` on an HTTP server span.\n- A saga's refused compensation stays `ERROR` on `saga/{SagaType}/compensate`, because it is a failure of the saga. The span now records the rejection by type alone (a message-less `CompensationRejectedException` around it, plus `streamrune.command.rejection.type`), so neither a command span nor a compensation span exports a rejection message (a listener or projection that lets a `DomainException` escape still records it, as any escaping failure).\n- A subscription's delivery span clears the OpenTelemetry baggage while the listener runs. Before, it used the first event's metadata baggage, including the caller's `role`, for the whole batch, and that batch can hold events from many callers.\n- `SpanLink.toJson()` / `fromJson(Object)` are public, so a mapper with `CAN_OVERRIDE_ACCESS_MODIFIERS` disabled can read and write event metadata. They are Jackson's entry point, not an application's, so they are `@Internal` and kept out of the recorded API.\n\n**Which spans a deployment gets**\n- `eventstore/read` starts only as a child of a current span, through the new seam method `StreamRuneTracing.startChild` (the default starts the span as before; `OpenTelemetryTracing` needs a valid current span). A read made by a request or a listener still nests under its span. A subscription's or projection runner's poll is now one trace, where before it also produced an unattributable root `eventstore/read` trace.\n- An `executeAsync` command continues the caller's trace on every bus, including the ones an integration builds. The new seam method `StreamRuneTracing.propagating(Runnable)` is the identity by default and `Context.current().wrap(task)` in `OpenTelemetryTracing`. The bus captures it on the submitting thread and applies it around any `asyncTaskWrapper`, so a span the wrapper opens nests under the caller's span. A wrapper that makes another context current (a root one) decides the command's trace.\n- An OpenTelemetry SDK that its own settings disable counts as no OpenTelemetry. Those settings are `otel.sdk.disabled=true`, Spring Boot 4's `management.opentelemetry.enabled=false` (Spring Boot maps `OTEL_SDK_DISABLED` onto it), and `quarkus.otel.enabled` / `quarkus.otel.sdk.disabled` / `quarkus.otel.traces.enabled` on Quarkus. Each value is read the way its owner reads it: Spring Boot treats any value but `true` as off, and SmallRye Config's boolean spellings apply to the Quarkus keys. The integration's tracing bean is then `StreamRuneTracing.NOOP`, logged once at `INFO`, so no span starts and no event or outbox row gets a span context. The switch list is the internal `OpenTelemetrySwitches` in `streamrune-integration-api`. Settings that only remove the exporter are not switches, because span processors still receive the spans. That includes `management.tracing.export.enabled` and Spring Boot 3's `management.tracing.enabled`, which Spring Boot 4 no longer reads.\n\n**Texts**\n- The production guide's Tracing section now says what a span carries and what it does not. Ids are exported as they are, so an id that names a person appears in every trace of its aggregate. A failure exports its own message. A delivery carries no caller's baggage. A new subsection covers telling replicas apart (`service.instance.id`).\n- `TraceAttributes` now states that same rule instead of an absolute one. The span context's source is now stated exactly in the CHANGELOG, the release notes, `EventMetadata` and the bus: the command span, never a StreamRune header, the request context or baggage; its trace id is the request's, which a `traceparent` header chooses. The release notes' wiring line and the `SpanId`/`TraceId` javadoc now describe what the code does.\n\n## Tests\n\nEach change has a test that fails without it. Each test was checked by removing the change and watching the test fail:\n- runtime: `TracingErrorRecordingTest` (message sanitized and bounded), `TracingEventListenerTest` (a batch from several callers runs with no baggage), `AsyncCommandTracingTest` (async command a child of the caller's span: built by the builder, registered by the application, a span the wrapper opens in between, a root-context wrapper), `TracingCommandInterceptorTest` (rejections, fenced command), `VirtualThreadCommandBusForwardFenceTest` (fenced command through the real bus), `SagaTracingTest` (rejected compensation by type), `EventStoreTracingTest` and `OpenTelemetryTracingTest` (`startChild`, one trace per poll);\n- core: `EventMetadataTest` (an access-checking mapper), `StreamRuneTracingTest` (seam defaults);\n- postgres: `PostgresEventStoreTracingTest` (read span only under a current span);\n- integrations: two new wiring-contract tests run on Spring, Quarkus and Micronaut, `anAsyncCommandContinuesTheCallersTrace` and `anOpenTelemetrySwitchedOffBySdkSettingsStartsNoSpan`. The TCK's reference application gained the two rules, so the contract proves both tests fail on an application that breaks them. Each integration also has its own switch tests. On Spring, these run Spring Boot's own `OpenTelemetrySdkAutoConfiguration`: switched off it gives NOOP, while the default and the old key keep tracing. `OpenTelemetrySwitchesTest` covers the switch list and every spelling.\n\n`./gradlew build` passes locally (every suite, Testcontainers, coverage gates, Javadoc, `verifyApiSurface`, doc-lint).\n\n\n\n\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-11-13T19:44:57Z",
          "url": "https://github.com/StreamRune/streamrune/commit/f3d720219452e2fcae828c220eec5696c8d07b80"
        },
        "date": 1794643292339,
        "tool": "jmh",
        "benches": [
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"0\"} )",
            "value": 115.33374203672797,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 161.34142956793977,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 172.6338303193566,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapPublishedEvent",
            "value": 0.2152878557578231,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"10\"} )",
            "value": 2.241016073042029,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"100\"} )",
            "value": 8.193762312488731,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.commandBusThroughput",
            "value": 15.832278449731621,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"1\"} )",
            "value": 14.746004253768714,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"10\"} )",
            "value": 148.1836204206638,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"100\"} )",
            "value": 1498.1166965902444,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"1\"} )",
            "value": 0.10347910426012838,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"10\"} )",
            "value": 0.18223553176687993,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"100\"} )",
            "value": 1.5085575403200537,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"1\"} )",
            "value": 0.136823506103447,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"10\"} )",
            "value": 0.1387478927107822,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"100\"} )",
            "value": 0.2143963616632175,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"NOOP\"} )",
            "value": 16.293282360031753,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_OFF\"} )",
            "value": 18.131547156147995,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_ON\"} )",
            "value": 18.68546133238958,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"1\"} )",
            "value": 604.3415266391354,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"10\"} )",
            "value": 789.7659222808787,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"1\"} )",
            "value": 591.1588132909578,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"10\"} )",
            "value": 793.7990112296251,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"10\"} )",
            "value": 249.83373712914295,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"100\"} )",
            "value": 548.3315290417238,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 603.2795337940973,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 608.1367609666228,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 6.187634883255663,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 27.58949684712734,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 9.89189873390731,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 48.68386162518526,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Martin Bednář",
            "username": "xxBedy",
            "email": "martin.bednar@streamrune.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "f76eccf811fdb9574c1319577a9fa0786339e246",
          "message": "fix(test): InMemoryEventSubscription takes an append from another thread during a delivery (#117)\n\n## Why\n\nCode that runs several listener steps at once appends events from worker threads while the test kit's `InMemoryEventSubscription` is still delivering a page on another thread. Until now such an append waited for the delivery in progress — and the delivery waits for the workers, so a test of parallel processing deadlocked.\n\n## Change\n\n- `InMemoryEventSubscription` serialises deliveries with a `ReentrantLock` and a \"delivery requested\" flag instead of a monitor:\n  - an append notification from another thread no longer waits — it marks a delivery as requested and returns;\n  - the thread that holds the lock keeps draining until an empty page;\n  - a re-check after unlocking catches an append that landed after the last read, so no wake-up is lost.\n- `poll()` and `start()` still wait for a delivery in progress and then deliver.\n- A failed delivery returns without retrying; its events stay pending for the next notification or poll.\n- The lock and the delivering-thread state are released on every path, including an `Error` from the listener.\n- The class Javadoc, `docs/guide/testing.md` and the 1.0.0 reference state the delivery rule exactly, including:\n  - when events appended during a delivery are delivered;\n  - that work handed to another thread must neither call `poll()` nor need a lock the delivering thread holds;\n  - which thread the listener runs on.\n\n## Tests\n\nNew cases in `InMemoryEventSubscriptionTest`:\n- an append from another thread during a delivery returns at once and is still delivered;\n- a failed delivery is not retried at once;\n- `poll()` waits for a delivery in progress;\n- an append that lands after a delivery's last read is still delivered (fails without the re-check);\n- an `Error` from the listener leaves no lock held (a poll from another thread completes).\n\n`:streamrune-test:check` (incl. doc lint and coverage) is green.\n\n\n\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-11-14T15:00:11Z",
          "url": "https://github.com/StreamRune/streamrune/commit/f76eccf811fdb9574c1319577a9fa0786339e246"
        },
        "date": 1794730984739,
        "tool": "jmh",
        "benches": [
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"0\"} )",
            "value": 162.70916396148698,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 229.07196558553514,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 236.85731569426721,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapPublishedEvent",
            "value": 0.2660047955181625,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"10\"} )",
            "value": 18.46199260052486,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"100\"} )",
            "value": 164.2355964403313,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.commandBusThroughput",
            "value": 17.620195465874012,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"1\"} )",
            "value": 16.041029253923377,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"10\"} )",
            "value": 57.40442396831905,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"100\"} )",
            "value": 401.3590151101164,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"1\"} )",
            "value": 1.4363949237749227,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"10\"} )",
            "value": 13.183604082390497,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"100\"} )",
            "value": 127.82006721898472,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"1\"} )",
            "value": 7.282776916503359,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"10\"} )",
            "value": 7.305151462872416,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"100\"} )",
            "value": 7.309364961393213,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"NOOP\"} )",
            "value": 17.70311018990575,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_OFF\"} )",
            "value": 20.743371552878983,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_ON\"} )",
            "value": 21.71559139931488,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"1\"} )",
            "value": 1040.832481602463,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"10\"} )",
            "value": 1320.819807130897,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"1\"} )",
            "value": 1046.3798251499002,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"10\"} )",
            "value": 1317.537083199854,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"10\"} )",
            "value": 396.4521307189967,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"100\"} )",
            "value": 889.9126289569728,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 941.4139597729452,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 962.6834807301027,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 8.386937405874729,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 37.90313734556802,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 13.775316379160452,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 66.11093099836407,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Martin Bednář",
            "username": "xxBedy",
            "email": "martin.bednar@streamrune.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "264a65c96bf9dd95b62820592b4a67a21ce7d101",
          "message": "fix(saga): a hold keeps the poison's dead-letter cause; a crash or a stale row cannot misplace a fault (#128)\n\n## What this fixes\n\nThree defects in how a saga records events in the saga dead-letter store.\n\n**A redelivery replaced the poison's cause.** When a correlated event's `evolve` (or another orchestrator method) throws, the runner records the event with the exception's class name and message and marks the saga `FAULTED`. The subscription delivers a page again whenever it retries it (a retry-later failure later in the page, a crash before the checkpoint, a handover). The poison event then met the `FAULTED` row it had faulted and was held, and the hold re-published over the poison's own entry: the store's upsert replaced the error type and message with `SagaSkippedWhileFaultedException` and a message saying the event was delivered while the saga was faulted. A held event delivered again refreshed its entry the same way.\n\n**A crash inside the poison handler did not converge.** The handler wrote the row's shield on its own, then the entry with the shield, then the fault write. A process that stopped after the first shield write or after the entry left the row shielded but `RUNNING`; the redelivery held the poison event behind the shield, so the saga stayed `RUNNING` until someone drained it, where a run without the crash ends `FAULTED`.\n\n**A hold decided on a stale row recorded an applied event.** A hold is decided on the row the delivery read. A drain running in another process could feed and discard the event, and clear the shield, between that read and the hold's write; the hold then recorded an event the row had applied, and the next drain evolved it a second time.\n\n## The change\n\n- `SagaDeadLetterStore.publishShieldedIfAbsent(entry)` returns a `HoldResult`. In one write against the owner row as the store sees it:\n  - when the row's `last_applied_offset` is at or above the event's offset, nothing is written and the result is `AlreadyApplied`;\n  - when an entry of the same saga type is stored at `(sagaId, eventOffset)`, it is kept exactly as stored, and the result is `Kept` with that entry;\n  - otherwise the entry and the shield are written and the result is `Written`.\n\n  PostgreSQL does it in one transaction: the upsert's conflict arm (`DO UPDATE … WHERE saga_type <> EXCLUDED.saga_type`), then the shield `UPDATE … RETURNING last_applied_offset`, rolled back when the event is applied. The in-memory store does it under its entries monitor. `SagaDeadLetterStoreContract` pins the rules for every store.\n- `SagaStore.loadRowFacts(sagaId, sagaType)` reads a row's status, shield and `last_applied_offset` without its state. The test kit's `InMemorySagaDeadLetterStore` reads only these facts, so it works over any saga store, `PostgresSagaStore` included: its hold, shield clear, retention and backlog count never bind the state to a class.\n- `SagaRunner`'s holds use it. They write no shield on their own; the entry and the shield are one write. A redelivery of an event already recorded, or of one the saga has applied, changes nothing, counts in no metric and logs at `DEBUG`. The poison path keeps `publishShielded` and its refresh, so a replay that still fails records the newest failure.\n- The poison handler writes no shield before its entry, so a crash before the entry write leaves nothing and the redelivery runs the event again.\n- A redelivery over a shielded live row whose kept entry was written by the poison handler (its error type is not one of the two hold types, which are one constant set) makes the fault write the crashed run did not make. It does so with one compare-and-swap at the version it read; if the CAS is lost, the event is routed again on the row that won. That way a drain in another process that applied the event first is never followed by a fault.\n\nCrash points of a correlated poison, each ending where the uninterrupted run ends (row `FAULTED`, entry with the poison's own class name and message):\n\n| The process stops | What is left | What the redelivery does |\n|---|---|---|\n| before the entry write | nothing | runs the event again, meets the poison, records it and faults the row |\n| after the entry write, before the fault write | entry, row shielded and `RUNNING` | holds the event, keeps the entry, makes the fault write |\n| after the fault write | entry, row `FAULTED` | keeps the entry, changes nothing |\n| after the next event's hold | entry, row `FAULTED`, the next event's entry | keeps both, changes nothing |\n\n## Tests\n\n- `SagaCorrelatedPoisonCrashPointTest` runs each crash point and compares the full end state with a run that does not crash. The page is delivered again until it completes, then twice more. It also checks that no shield is written on its own.\n- `SagaRunnerHoldTest` (both routes):\n  - a redelivered poison keeps its cause;\n  - a start-event poison is unchanged;\n  - a held event redelivered counts nothing;\n  - a drain's clear before the hold's write cannot strand the entry, with or without a crash after it;\n  - a kept entry of another saga type is not taken for this saga's poison;\n  - a redelivery after two lost fault writes faults the saga;\n  - the hold error types are exactly the two documented class names, and any other hold cause is refused.\n- `SagaDeadLetterReplayerTest`, two processes (two runners over one set of stores):\n  - an interrupted fault completed beside another process's drain never faults an applied event;\n  - a hold decided on a stale row beside another process's drain records nothing, and each event evolves once.\n- `SagaStoreContract` (in-memory and PostgreSQL) covers `loadRowFacts`. `SagaDeadLetterStoreContract` also runs against the in-memory dead-letter store over a `PostgresSagaStore`.\n- A runner test: a fault completion that loses its compare-and-swap to a writer that leaves the row live is routed again and faults the saga.\n- `SagaDeadLetterStoreContract` (in-memory and PostgreSQL) covers writing, keeping, refreshing by the poison path, replacing another type's entry, an absent owner row, and an applied event.\n- PostgreSQL-specific tests:\n  - the hold's entry and shield are one transaction;\n  - the hold fails at the saga-row write bound behind a held row;\n  - the hold's shield write is among the bounded statements;\n  - two-connection interleavings: a hold queued behind an uncommitted poison keeps the poison, and a hold queued behind an uncommitted apply records nothing.\n\nRemoving each change turns its tests red: the hold refreshing, the stand-alone shield, the fault completion off or reloading, the applied check off, the entry without its shield, the saga-type filter off, and the hold and shield split into two transactions on PostgreSQL.\n\n## API\n\n`SagaDeadLetterStore` gains the abstract method `publishShieldedIfAbsent` and the sealed `HoldResult`; `SagaStore` gains the abstract method `loadRowFacts` and the record `RowFacts`. They are implemented by `PostgresSagaDeadLetterStore`, `InMemorySagaDeadLetterStore` and the test kit's `ForwardingSagaDeadLetterStore`, and the `.api` baselines are updated. The saga guide, the metric javadoc and table, and the release notes describe the behaviour.\n\n\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-11-15T20:32:52Z",
          "url": "https://github.com/StreamRune/streamrune/commit/264a65c96bf9dd95b62820592b4a67a21ce7d101"
        },
        "date": 1794817802380,
        "tool": "jmh",
        "benches": [
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"0\"} )",
            "value": 128.761238261364,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 192.60616142763573,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 201.29921154423263,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapPublishedEvent",
            "value": 0.22239835498705704,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"10\"} )",
            "value": 21.514863744525574,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"100\"} )",
            "value": 206.286939242788,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.commandBusThroughput",
            "value": 17.8491200376947,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"1\"} )",
            "value": 16.49193315868712,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"10\"} )",
            "value": 58.069881472452195,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"100\"} )",
            "value": 376.68008427821394,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"1\"} )",
            "value": 1.2255604688940807,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"10\"} )",
            "value": 10.423335387098975,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"100\"} )",
            "value": 102.88360672071906,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"1\"} )",
            "value": 6.001582091396301,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"10\"} )",
            "value": 6.37999809369295,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"100\"} )",
            "value": 6.102215135394614,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"NOOP\"} )",
            "value": 17.75197139926926,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_OFF\"} )",
            "value": 20.364860039561187,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_ON\"} )",
            "value": 21.00535299501546,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"1\"} )",
            "value": 1027.460066696763,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"10\"} )",
            "value": 1276.1728483642105,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"1\"} )",
            "value": 1033.8812826905726,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"10\"} )",
            "value": 1278.088342213206,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"10\"} )",
            "value": 411.091308405236,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"100\"} )",
            "value": 779.4870381204212,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 840.1474382705621,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 839.981895976556,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 7.20640997956149,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 31.884079228260724,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 11.057433170635807,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 54.28300693339405,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Martin Bednář",
            "username": "xxBedy",
            "email": "martin.bednar@streamrune.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "b9fbf11d9b06918d977fb82de56705dcaa2c01b8",
          "message": "feat: read-model stores — checkpoint with the read model, per-registration routing, several stores per application (#130)\n\n* feat(core): ReadModelStore, CheckpointTarget, split supportsFencing into transactional/fencesCheckpoint\n\nAtomicBatchProcessor.supportsFencing() is removed. transactional() (shared\ntransaction, non-null handed repository) gates TRANSACTIONAL_LOCAL and\nEXTERNAL_EFFECT in ProjectionDeliveryPolicy; fencesCheckpoint() (durable\nstamp, epoch rejected before the updater, epoch-conditional checkpoint)\ngates real leadership in ProjectionFencingPolicy, whose message now points\nat routing to a fencing store instead of defining a bean.\n\nNew core types ReadModelStore (name, repository, checkpoints, processor,\ncheckpointTarget(), of(...)) and CheckpointTarget, plus\n@ProjectionConfig.readModelStore. JdbcProjectionRepository and\nInMemoryProjectionRepository declare both capabilities;\nAtomicBatchProcessorContract asserts both.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(postgres): subscription_checkpoint rename and read-model migration series\n\nCo-Authored-By: Claude Sonnet 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* refactor(postgres): extract the checkpoint-row fence into PostgresCheckpointFence\n\nThe epoch fence, the first-offset overlap guard and the monotonic offset\nguard move out of JdbcProjectionRepository into the package-private\nPostgresCheckpointFence. The fence is parameterised by the CheckpointTable\nenum (subscription_checkpoint | readmodel_checkpoint), the only source of\nthe identifier it splices into SQL. The statements are byte-identical to\ntoday's, with only the table substituted.\n\nJdbcProjectionRepository gets a package-private CheckpointTable constructor\nparameter. Its public constructors keep SUBSCRIPTION, so behaviour is\nunchanged (ruling R2: the default flips in Task 2.8). The fence also\ncarries lastOffset/saveUnlocked for PostgresOffsetStore (Task 2.4).\nSagaStateWriteRatchetTest lists the fence's composed-target writes.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* refactor(postgres): lift the checkpoint transaction shell out of JdbcProjectionRepository\n\nCheckpointTransaction (BATCH, REPLAY) now owns the one shell a checkpoint\ntransaction runs in: autocommit off, the body, commit, a single\nrollback-or-abort in finally, autocommit restore, and the same\nEventStoreException messages and wrap points. JdbcProjectionRepository\ncalls it unchanged in behaviour; the subscription-checkpoint processor\nwill share it.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* test(postgres): pin the fence's row seeding, regression message and guard order\n\nWhere a case claims no row or a seeded row it now asserts the row itself\n(a (0,0) row reads like no row by value); the epoch-regression case\nasserts the stored-epoch phrase instead of a bare \"6\"; and a batch that\nis both stale and overlapping is pinned to EPOCH_FENCE over both tables.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(postgres): fencing at-least-once processor over subscription_checkpoint\n\nPostgresSubscriptionCheckpointProcessor fences its checkpoint without\nbeing transactional: in the shared CheckpointTransaction shell it locks\nthe subscription_checkpoint row, rejects a stale epoch or an overlapping\nbatch before the updater runs, hands the updater null, then advances the\ncheckpoint under the monotonic guard and commits. A replay takes the same\nlock and moves neither offset nor epoch. A save or commit failure after\nthe updater returned is a ProjectionCheckpointSaveException (applied,\ncheckpoint not moved). PostgresOffsetStore now delegates to the same fence.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(postgres): PostgresReadModelStore with its own checkpoint series\n\nPostgresReadModelStore is the Postgres ReadModelStore and the factory of\nthe read-model schema: server-version check, the read-model series on\nflyway_schema_history_readmodel, validation (each schema step\nswitchable). Its JdbcProjectionRepository (repository and processor)\ncommits to readmodel_checkpoint, read by PostgresReadModelCheckpoints.\n\nThe one-series Flyway runner (baseline 0, migrateConverging) moves from\nPostgresEventStoreFactory into package-private SeriesMigration, used by\nboth factories; the event-store factory's behaviour is unchanged and it\nnever applies the read-model series.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* test(postgres): sharpen subscription checkpoint processor tests\n\n- the batch-waits-for-replay test commits the checkpoint row first, so\n  the batch waits on FOR UPDATE rather than the seed INSERT (M13)\n- an updater's EventStoreException propagates unwrapped (M14)\n- a close failure after the commit is a ProjectionCheckpointSaveException\n  and the offset did advance (M15)\n- executeAtomically's @throws names the close-after-commit leg (M17)\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(integration): per-registration store routing\n\nProjectionStoreRouting routes one registration to a CheckpointTarget by\nspec §5.6 rules 1-4 in order (explicit readModelStore, writeTarget()\nidentity match, the single transactional store, the subscription\ncheckpoint), then checks transactional() for transactional modes and\nfencesCheckpoint() under real leadership. validateStores refuses\nduplicate names and duplicate write-target identities. Every refusal\nnames the projection, its mode, the stores with their capabilities and\nthe fix.\n\nINLINE_INFO and requireInlineMode now live here; ProjectionProcessorResolution\ndelegates to them until the integrations move over (Task 2.8).\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(runtime): per-registration checkpoint targets\n\nEach MultiProjectionRunner / ScheduledProjectionRunner registration reads\nits checkpoint from and commits through one pair: its own CheckpointTarget,\nor the runner-wide offsetStore + atomicProcessor (default pair). Delivery\nand fencing policies run per registration against that registration's\nprocessor and name it (and its target) on refusal. Both runners gain a\nregister(..., CheckpointTarget) overload and reset(ProjectionName), which\nrewinds the checkpoint in the registration's own store.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(runtime): atomic reset claim, processor-half routing tests, half-pair message\n\nreset() and start()'s claim of the started flag are now mutually exclusive\nthrough a dedicated lock held for the whole rewind (stop() clears the flag\nunconditionally, so the flag itself cannot be reset's claim). Routing tests\nnow use a distinct runner-wide processor and a (store, nonAtomic) target so\na commit through the wrong half fails; Multi gains a per-target epoch-stamp\ntest. Both half-pair messages offer the route-every-registration way out.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(spring): wire default postgres read-model store and subscription checkpoint\n\nReplace the JdbcProjectionRepository bean with a lazy PostgresReadModelStore\nbean \"postgres\" (migrate per streamrune.read-model.schema.auto-initialize,\ndefaulting to the event-store switch; validate always) and a\nProjectionRepository bean that is its repository. Add the\nstreamRuneSubscriptionCheckpoint CheckpointTarget (PostgresOffsetStore +\nPostgresSubscriptionCheckpointProcessor with a DataSource, otherwise the\nOffsetStore bean + nonAtomicAtLeastOnce). ProjectionAutoConfig routes each\ncontinuous and scheduled registration through ProjectionStoreRouting and\nsets no runner-wide offset store or processor.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(spring): build the subscription checkpoint processor from its offset store\n\nPostgresOffsetStore#subscriptionCheckpoint() pairs the offset store with a\nPostgresSubscriptionCheckpointProcessor on its own DataSource, so the\nstreamRuneSubscriptionCheckpoint bean no longer looks up a DataSource and a\nPostgresOffsetStore on a second database is committed and read in the same\ndatabase.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(quarkus): wire default postgres read-model store and subscription checkpoint\n\nReplace the JdbcProjectionRepository producer with a @DefaultBean\nReadModelStore named \"postgres\" (PostgresReadModelStore.create with the\ncrypto-aware mapper; migrate per streamrune.read-model.schema.auto-initialize,\ndefaulting to the event-store switch; validate always) and a ProjectionRepository\nproducer that is its repository. Add the named streamRuneSubscriptionCheckpoint\nCheckpointTarget built from the OffsetStore bean (PostgresOffsetStore ->\nsubscriptionCheckpoint(), otherwise nonAtomicAtLeastOnce). ProjectionProducer\nroutes each continuous and scheduled registration through ProjectionStoreRouting\nand sets no runner-wide offset store or processor. Register CheckpointTarget for\nnative reflection.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* refactor(integration): share default-repository selection and subscription checkpoint pairing\n\nOffsetStore gets a default subscriptionCheckpoint() (this store + the nonatomic\nprocessor, named subscription_checkpoint); PostgresOffsetStore overrides it with\nits fencing processor. ProjectionStoreRouting.defaultRepository holds the by-name\nselection of the default store's repository and its error message. Spring and\nQuarkus call both instead of carrying their own copies.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(micronaut): wire default postgres read-model store and subscription checkpoint\n\nThe module defines the default read-model store (PostgresReadModelStore\nnamed postgres, on the DataSource, crypto-aware mapper, migrated under\nstreamrune.read-model.schema.auto-initialize which follows the event-store\nkey when unset), the injectable ProjectionRepository as that store's\nrepository, and the event store's subscription checkpoint\n(offsetStore.subscriptionCheckpoint()). ProjectionFactory routes every\nregistration on its own through ProjectionStoreRouting and registers it\nwith its CheckpointTarget; the runners get no runner-wide offset store or\nprocessor. Stores are collected with BeanContext.getBeansOfType so a\nruntime-singleton store counts. CheckpointTarget is registered for\nreflection.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(postgres): JdbcProjectionRepository checkpoints in readmodel_checkpoint\n\nThe public constructors now fence and advance readmodel_checkpoint (read\nthrough PostgresReadModelCheckpoints on the same DataSource); the\npackage-private CheckpointTable constructor is gone. Postgres suites are\nre-pointed: transactional pairs read readmodel_checkpoint, unrouted\nat-least-once runs on PostgresOffsetStore#subscriptionCheckpoint(). Stale\nfence method names in JdbcProjectionRepositoryAtomicityTest renamed (M11);\nits save-failure test now really fails the save after the write; new test\npins the takeover stamp waiting for an open batch.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* refactor(integration): delete ProjectionProcessorResolution\n\nNo integration uses it since per-registration routing; every case its test\ncovered is in ProjectionStoreRoutingTest (rules 3/4, leadership capability,\nduplicate stores, arguments, INLINE).\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs(guide): read-model schema property and checkpoint tables\n\nDocuments streamrune.read-model.schema.auto-initialize in the configuration\nguide and names readmodel_checkpoint as the JDBC processor's lock row in the\nreplay and fencing sections.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* test(e2e): re-point the framework-free suite to the routed checkpoint pairs\n\nTransactional ITs read readmodel_checkpoint through PostgresReadModelCheckpoints\n(E2ETestBase applies the read-model series); HaDualInstanceAtLeastOnceIT runs\non PostgresOffsetStore#subscriptionCheckpoint(). New\nReadModelStoreOnAnotherDatabaseLeadershipIT: leases in the event-store\ndatabase, the epoch stamped into and fencing in a read-model store's own row\non another database.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(test): forward subscriptionCheckpoint and list the read-model series\n\nForwardingOffsetStore now overrides OffsetStore#subscriptionCheckpoint()\n(forwarded, under the schedule), as it does every other method; the\nShippedMigrationScripts fixtures include db/readmodel-migration. Both were\nleft red by earlier PR-2 commits and surfaced in the full build.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs(guide): state no default for the read-model schema key\n\nThe key binds no value of its own (it follows the event-store key when\nunset); the doc lints and wiring contracts compare the default column with\nthe bound default.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(runtime): halt message names the matching checkpoint pairs\n\nProjectionCheckpointDivergedException recommended a PostgresOffsetStore for a\nJdbcProjectionRepository, the pairing that now causes the halt. It names one\ncheckpoint table and the two matching pairs (PostgresReadModelStore#\ncheckpointTarget(), PostgresOffsetStore#subscriptionCheckpoint()); a test pins\nthe remedy text.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(test): keep the fault schedule on ForwardingOffsetStore's subscription checkpoint\n\nsubscriptionCheckpoint() returned the delegate's pair, whose checkpoints are the\nraw delegate, so an application OffsetStore bean built as a ForwardingOffsetStore\ninjected nothing into the integrations' subscription checkpoint. It now rebuilds\nthe pair around the decorator (the delegate's name and processor). The decorator\nharness gains a documented rebuilt-answer exception for that one method; a test\nproves a saveOffset fault fires when committing through the pair.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(postgres)!: PostgresEventStoreFactory#createReadModelStore replaces createProjectionRepository\n\ncreateProjectionRepository() returned a JDBC repository whose checkpoints need\nthe read-model series, which the factory never applied (ruling R11). The\nfactory now builds the default store: PostgresReadModelStore \"postgres\" on its\nDataSource, with its crypto-aware mapper, migrating under its\nautoInitializeSchema switch and always validating. Pre-1.0, no deprecation.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: pair JdbcProjectionRepository with its read-model checkpoints everywhere\n\nThe flip made every documented JdbcProjectionRepository + PostgresOffsetStore\npairing read the wrong table. Recipes and samples (concepts hand-built and\nintegration recipes, runtime README, QUICKSTART) now use PostgresReadModelStore\nand its checkpointTarget()/checkpoints()+processor(); QUICKSTART says how the\nread-model series is applied; the divergence section, the checkpoint-row\nrunbook, the backup pairs, the Flyway series table and the fencing/wiring\nparagraphs of the production guide name the right table and the routed pairs;\nrunner javadoc says which offset store a JdbcProjectionRepository needs.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat: several read-model stores per application (#131)\n\n* feat(integration): select read-model stores by name and route registrations once\n\nProjectionStoreRouting.selectStores keeps the default 'postgres' store\nunless an application store takes its name, without creating a replaced\ndefault, and validates the stores once. ReadModelStores holds that\nselection per application; ProjectionRoutes routes every runner\nregistration once and tells the index declaration which store each\nprojection is routed to.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(spring): discover every ReadModelStore; default store replaced only by name\n\nThe default postgres store is always defined (lazily) as\nstreamRuneDefaultReadModelStore; the streamRuneReadModelStores bean\nleaves it out by bean name and creates it only when no application\nstore is named postgres. The runners and the index declaration share\none streamRuneProjectionRoutes bean: each ProjectionIndex is declared\non the store its projection is routed to.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(quarkus): discover every ReadModelStore; default store replaced only by name\n\nThe default postgres store is produced as\n@Named(\"streamRuneDefaultReadModelStore\"); the ReadModelStores bean\nreads the application's stores through Instance handles, leaving the\ndefault out by bean name and looking it up only when no application\nstore is named postgres. The runners and ProjectionIndexDeclarer share\none ProjectionRoutes bean. Without a usable database the default store\nsays that such an application produces its own store named postgres.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(micronaut): discover every ReadModelStore; default store replaced only by name\n\nThe default postgres store is defined as\n@Named(\"streamRuneDefaultReadModelStore\") with no missingBeans back-off;\nthe ReadModelStores bean collects the application's stores with\ngetBeansOfType under a qualifier that leaves the default out by bean\nname, and resolves the default only when no application store is named\npostgres. The runners and ProjectionIndexDeclarer share one\nProjectionRoutes bean. README: a store registered before the context\nstarts counts too.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* chore(integration): keep the store-routing plumbing out of the public API\n\nBean methods and constructors whose signatures name the @Internal\nReadModelStores / ProjectionRoutes are marked @Internal (@hidden), and\nthe three integrations' API baselines are rewritten.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(integration): validate the read-model stores once per collection\n\nRouting a registration from ProjectionRoutes no longer re-validates the\nstore set, and selectStores compares each pair of stores once (the\ndefault store against the application's, not the whole set again). The\npublic route keeps its own validation.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(spring): the default read-model store is a fallback bean\n\nAn unqualified ReadModelStore injection point resolves to the\napplication's store again when the default store is defined beside it,\nas on Quarkus (@DefaultBean) and Micronaut (@Secondary).\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(quarkus): give only an unreadable server version the no-database hint\n\nA readable but unsupported server version is a real, old server: its\nUnsupportedServerVersionException is rethrown as is. Adds the parity test\nthat an unqualified ReadModelStore injection resolves to the\napplication's store.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(micronaut): never create a replaced default store, even with eager singletons\n\nMicronaut has no lazy singleton: eagerInitSingletons(true) created the\ndefault store and migrated onto the application's DataSource although a\nstore named postgres replaced it. The default bean is now a\nDeferredReadModelStore that creates the PostgresReadModelStore on first\nuse of a part. Adds the parity test that an unqualified ReadModelStore\ninjection resolves to the application's store.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs(integration): index placement routes declarations of runners you own\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(runtime): health lag reads each registration's own checkpoints\n\nA projection routed to a read-model store keeps its checkpoint in that\nstore (readmodel_checkpoint), but the health contributor read every\nentry's checkpoint from its one OffsetStore (subscription_checkpoint),\nso a caught-up TRANSACTIONAL_LOCAL projection showed lag = head and\nwent DEGRADED.\n\nThe runners now register each projection with the offset store of the\npair they read and commit through, and the target's name:\nSubscriptionHealthContributor.register(name, lifecycle, checkpoints,\nstore). Lag and the lag gauge read that store; the source survives\nreplaceLifecycle and unregister (the gauge outlives the entry).\nSubscriptionHealth gains a nullable store component naming it.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(runtime): dead-letter replay on each projection's own target\n\nProjectionDeadLetterReplayer(eventStore, deadLetterStore, targets)\ntakes each projection's CheckpointTarget - the pair its runner\nregistration reads and commits through - and replays a projection on\nits own target's processor, so one replayer serves projections of\nseveral read-model stores and never replays one on another store's\nprocessor. Refusals name the target. A projection with no target is\nrefused before any entry is read. The single-processor constructor\nstays for a runner with one default pair.\n\nNo integration constructs a replayer; the docs show the per-projection\nform with ReadModelStore#checkpointTarget() and\nOffsetStore#subscriptionCheckpoint().\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(integration): health details name each projection's store\n\nSpring, Quarkus and Micronaut render SubscriptionHealth#store as the\n`store` detail of a projection's entry (subscription.<name>.store on\nQuarkus). A real-Postgres wiring test per framework proves a caught-up\nTRANSACTIONAL_LOCAL projection (checkpoint in readmodel_checkpoint, no\nrow in subscription_checkpoint) reports lag 0 and UP under a lag\nthreshold of 1, naming store `postgres`; without the runtime fix each\nfails with lag 1. The production guide's health section says where lag\nis measured and documents the detail; it also shows the per-projection\nreplay targets.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* feat(runtime): register a projection on an explicit read-model store\n\nProjectionRunner gains run(name, projection, mode, CheckpointTarget); its\ndefault refuses with UnsupportedOperationException instead of running the\nprojection on the runner's own pair. ContinuousProjectionRunner and\nPollingProjectionRunner route it: the run reads and commits through the\ntarget, checks name, mode and fencing against the target's processor, and\nreset() rewinds where the projection last ran. ContinuousProjectionRunner's\noffsetStore + atomicProcessor become an optional both-or-neither pair.\n\nStreamRune.Builder.registerProjection takes a ReadModelStore (the explicit\ndeclaration; a disagreeing writeTarget() is refused at the call) or a\nCheckpointTarget (the subscription checkpoint form); startProjections()\nhands it to the runner. Javadoc, QUICKSTART, getting-started, concepts and\nthe API reference show the new forms; api dumps regenerated.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(runtime): reset a routed projection on its target; fresh rejection state per run\n\nProjectionRunner gains reset(name, CheckpointTarget), refusing by default.\nContinuousProjectionRunner and PollingProjectionRunner rewind the target's\ncheckpoints with the read-back guard, independent of what ran on the\ninstance: reset(name) on a fresh runner rewound the runner's own offset\nstore, and a rebuild then resumed from the target's old checkpoint.\nreset(name) is documented as knowing only what ran on this instance.\n\nContinuousProjectionRunner clears its commit-rejection tracker at run\nstart, so a run on a new target never takes its first rejection for a\nrepeat of the previous run's and halts.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* test(tck): two read-model stores on two databases scenario\n\nMultiReadModelStoreScenario: event store and the default postgres store\non database A, a user-defined PostgresReadModelStore 'secondary' on\ndatabase B; p1 (TRANSACTIONAL_LOCAL on the injected repository), p2\n(TRANSACTIONAL_LOCAL declaring 'secondary') and p3 (at-least-once, no\ntarget). Four tests: checkpoints land in each store's own table with\nhealth naming the store at lag 0, dropping B resets only p2, a takeover\nstamps B's row and fences a stale commit there, and an ambiguous\ntransactional projection fails startup naming both stores.\n\nContractBeans.injected marks a constructor-injected contract bean. The\nTCK's own docker-free tests cover the refusal rule against stub\napplications; the database-bound tests run in the three integrations.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* test(spring): run the two-store scenario\n\nSpringMultiReadModelStoreScenarioTest starts each application exactly as\nthe wiring contract does. The contract adapter declares a\nconstructor-injected contract bean with its constructor autowired. Each\ncontract adapter's PostgreSQL container can now be stopped by the\nscenario's test class as well. The scenario declares its event types:\nthe projections read the events back.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* test(quarkus): run the two-store scenario\n\nQuarkusMultiReadModelStoreScenarioTest starts each application exactly\nas the wiring contract does, in a real Arc container: the scenario's\nconstructor-injected projection is a Singleton class bean whose single\nconstructor Arc injects, its suppliers synthetic beans.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* test(micronaut): run the two-store scenario\n\nMicronautMultiReadModelStoreScenarioTest starts each application exactly\nas the wiring contract does. The scenario's constructor-injected\nprojection is declared in the adapter's compile-time ApplicationBeans,\ngated by a property: a runtime singleton is built before the context\nexists. Any other constructor-injected contract bean fails loudly.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* test(tck): name the scenario's projections in words\n\nThe tracker-id ratchet reads a bare P2 as a priority label: the\nconstants are now ON_DEFAULT, ON_SECONDARY and UNROUTED.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs(javadoc): non-transactional processors produce ProjectionCheckpointSaveException\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: replace supportsFencing with transactional/fencesCheckpoint; list readmodel-migration in native resources\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: read-model stores guide\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs(changelog): read-model stores\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: reword read-model stores guide for the history-narration ratchet\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs(javadoc): readModelStore does not rescue an unowned writeTarget\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: fix read-model stores guide (Spring primary DataSource, unowned writeTarget, replay wording)\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: state exactly when an unowned writeTarget is refused; link per-store replay\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(postgres): name the history table when a series migration runs again (M18)\n\nThree series (event store, crypto, read model) converge through\nSeriesMigration.migrateConverging; the INFO line now says which.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix: fencing refusal no longer calls a transactional checkpoint inert; tidy Quarkus comment (M3, M5)\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(runtime)!: hand SubscriptionFactory the run's checkpoints; plain health register keeps a routed source (F2, M70)\n\nA factory-built live subscription read and saved through whatever\nOffsetStore the factory captured, so run(..., target) mixed the pair:\nlive started from a store catch-up never committed to, and the overlap\nguard halted the run as diverged (or a stale row ahead skipped events).\ncreate(...) now takes OffsetStore checkpoints (pair.checkpoints()).\n\nSubscriptionHealthContributor#register(name, subscription) no longer\ndrops a checkpoint source set by the 4-arg register: a subscription\nself-registering on start() kept repointing a routed projection's lag\nat subscription_checkpoint (DEGRADED forever).\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(integration): routing refusals offer a declaration only where it can work (M23)\n\nA projection that exposes writeTarget() moves by what it writes: rule 1\nrefuses a readModelStore declaration naming another store. The rule-3\nforeign-writeTarget, non-transactional-target and non-fencing-target\nrefusals now tell such a projection to return the store's repository\nfrom writeTarget() instead of offering the declaration.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs(javadoc): health lag is against the run's shared checkpoint; reset note out of @throws (M71, M34)\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(postgres): read-model checkpoints refuse to be the subscription checkpoint\n\nPostgresReadModelCheckpoints inherited OffsetStore#subscriptionCheckpoint(),\nwhich paired readmodel_checkpoint with the non-fencing at-least-once\nprocessor under the name subscription_checkpoint. It now refuses, naming\nthe store's checkpointTarget() and PostgresOffsetStore instead; the\nOffsetStore javadoc says the method is meaningful only on the event\nstore's offset store.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* fix(micronaut): create the default store when the stores are selected (M68, M69)\n\nMicronaut selected the DeferredReadModelStore without creating it, so an\napplication whose projections never used a part of the store skipped the\nread-model migration and validation that Spring and Quarkus run at\nselection. The readModelStores default supplier now creates it and selects\nthe Postgres store itself, so messages also name PostgresReadModelStore.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: Spring two-DataSource snippet binds spring.datasource.hikari.*; QUICKSTART on createReadModelStore validation (F3/M87, M58)\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: reset, rebuild, mode-switch and restore point at the projection's own checkpoint target (F1)\n\nThe pages told DI users to call offsetStore.reset(name), whose bean is\nPostgresOffsetStore over subscription_checkpoint; a projection routed to\na read-model store checkpoints in that store's readmodel_checkpoint, so\nthe reset rewound nothing and the mode-switch truncate emptied the read\nmodel for good. Reset now goes through the runner that holds the\nregistration, runner.reset(name, store.checkpointTarget()) or\nstore.checkpoints().reset(name), and each page says how to find a\nprojection's target (startup INFO line, health store detail). The\nrestore runbook adds the event_stream <-> every store's\nreadmodel_checkpoint pair, whatever database the store is in. OffsetStore\nreset caveat and ProjectionRunner's refusal updated the same way.\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: replay target map must follow the routes; at-least-once BaseProjection over the injected repository is on the postgres store (M73)\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* chore(api): regenerate postgres api dump (PostgresReadModelCheckpoints#subscriptionCheckpoint)\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n* docs: state the replay-map and health-register behaviour in the present tense\n\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\n\n---------\n\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\nCo-authored-by: Claude Opus 5.5 <noreply@anthropic.com>\n\n---------\n\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\nCo-authored-by: Claude Opus 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-11-17T13:38:51Z",
          "url": "https://github.com/StreamRune/streamrune/commit/b9fbf11d9b06918d977fb82de56705dcaa2c01b8"
        },
        "date": 1794992645216,
        "tool": "jmh",
        "benches": [
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"0\"} )",
            "value": 143.88716373611447,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 209.03441080969583,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 218.41520074302161,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapPublishedEvent",
            "value": 0.2384155760917141,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"10\"} )",
            "value": 23.76869014569051,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"100\"} )",
            "value": 216.04608669097473,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.commandBusThroughput",
            "value": 13.219648877961768,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"1\"} )",
            "value": 11.856762748402906,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"10\"} )",
            "value": 43.465659320828195,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"100\"} )",
            "value": 375.55905985348375,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"1\"} )",
            "value": 1.3462934235418693,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"10\"} )",
            "value": 12.056606801315093,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"100\"} )",
            "value": 113.46341866296083,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"1\"} )",
            "value": 6.346422319075155,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"10\"} )",
            "value": 6.643001672418263,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"100\"} )",
            "value": 6.809535004711047,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"NOOP\"} )",
            "value": 13.28058819882583,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_OFF\"} )",
            "value": 15.845408959004669,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_ON\"} )",
            "value": 16.277586126716706,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"1\"} )",
            "value": 901.2148762282874,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"10\"} )",
            "value": 1180.7514373624515,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"1\"} )",
            "value": 937.3431575548011,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"10\"} )",
            "value": 1535.0172622600953,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"10\"} )",
            "value": 396.38840019763563,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"100\"} )",
            "value": 788.1591615418303,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 864.6284800287831,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 879.6486502260207,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 12.022695605750002,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 56.878672691973634,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 17.678723123851583,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 86.1592262613693,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "name": "Martin Bednář",
            "username": "xxBedy",
            "email": "martin.bednar@streamrune.com"
          },
          "committer": {
            "name": "GitHub",
            "username": "web-flow",
            "email": "noreply@github.com"
          },
          "id": "cf1db4f330a4aec8980b1b6c302f5f711b45a86e",
          "message": "feat(saga): the runner executes the sagas of a page in parallel (#134)\n\nA `SagaRunner` now executes the sagas of one delivered page in parallel: steps of different sagas run at once, at most `parallelism` of them (default **4**, range 1–64), while each saga's own events stay in offset order, one step at a time. A saga type is no longer capped at one step at a time.\n\n## How a page runs\n\n- The runner routes the page on the delivering thread, groups the items by saga and hands them to a package-private `KeyedPageExecutor`: the items of one saga run in order on one virtual thread, at most `parallelism` sagas at once, and the page is acknowledged only when every item ended. Checkpoint, lease, stores and schema are unchanged; nothing durable is added.\n- A failed item ends its own saga's group. The page stops starting items on a connection-class failure, on its third failed item, or on an `Error`; the other sagas' steps that already started finish. The page then throws, is not acknowledged, and its redelivery is answered from the saga rows. At `parallelism(1)` the runner behaves as before, apart from routing the whole page before its first step.\n- Connection-class is portable (SQLState class `08`, `SQLTransientConnectionException`, `SQLNonTransientConnectionException`); a store adds its own codes through the new `SagaStore.connectionFailureClassifier()` — PostgreSQL contributes `57P01`, `57P02`, `57P03`, `53300`.\n- A page whose workers end without running their items, or a worker that dies while recording a failure, fails the page; nothing is acknowledged that did not run.\n- Orchestrators are now called from several threads at once; the `SagaDecider` contract says so. `SagaFixture` keeps parallelism 1 unless its customizer sets it.\n\n## Configuration\n\nParallelism comes from a per-subscription setting, the builder (`SagaRunner.Builder.parallelism`), a global setting, or the default 4, in that order; a subscription binds the runner's value when it is built, an unknown subscription name is refused, and a new value applies from the next page. A runner bound to several subscriptions can have one page per subscription in flight. A startup line states the value, where it came from and the connection budget.\n\n## Metrics\n\n`streamrune.saga.steps.in_flight` and `streamrune.saga.page.pending` (gauges, across the runner's pages), `streamrune.saga.page.effective_parallelism` (distribution summary, median published) and `streamrune.saga.page.failed` (counter, tag `stop` = `connection`, `third_failure` or `none`), all tagged by `saga.type` and recorded at parallelism above 1. The page WARN is rate-limited.\n\n## Tests\n\n- The executor, the stop rule, the classifier, the accounting and the parallelism sources each have their own tests.\n- A seeded equivalence test: parallel runs end in exactly the state of serial runs (rows, commands in order, inbox keys, dead letters with their error types).\n- One crash-point test per point where a parallel page can be cut (in memory and on PostgreSQL): the rerun converges to the uninterrupted run.\n- PostgreSQL and two-node tests: 200 sagas at parallelism 8, 50 sagas on one aggregate, a deadline claim while workers step (the forward fence holds), a pool smaller than the budget, a lease lost mid-page, an `Error` on a worker; the leadership ITs run at parallelism 1 and 8.\n\n## Docs\n\n`docs/guide/advanced/saga.md` (parallel steps, failure rule, connection budget per locker, pool threshold), `production.md` (pool sizing), `subscription-lifecycle.md`, `testing.md`, the 1.0.0 release notes and the CHANGELOG.\n\n\nSigned-off-by: Martin Bednář <martin.bednar@streamrune.com>\nCo-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>",
          "timestamp": "2026-11-23T07:59:05Z",
          "url": "https://github.com/StreamRune/streamrune/commit/cf1db4f330a4aec8980b1b6c302f5f711b45a86e"
        },
        "date": 1795426852714,
        "tool": "jmh",
        "benches": [
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"0\"} )",
            "value": 97.99090930007772,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 154.79261044699234,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.EventSerializationBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 156.21371722159634,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.OutboxEventMapperBenchmark.mapPublishedEvent",
            "value": 0.176198627568539,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"10\"} )",
            "value": 16.65209650274495,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.ProjectionRunnerBenchmark.processBatch ( {\"batchSize\":\"100\"} )",
            "value": 149.2996005914239,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.commandBusThroughput",
            "value": 10.95878157265861,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"1\"} )",
            "value": 9.820572210062837,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"10\"} )",
            "value": 48.39497100953297,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreAppend ( {\"batchSize\":\"100\"} )",
            "value": 326.80226046763966,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"1\"} )",
            "value": 0.983324839899207,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"10\"} )",
            "value": 8.227043513838371,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoad ( {\"batchSize\":\"100\"} )",
            "value": 78.62459205649097,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"1\"} )",
            "value": 4.512709936558317,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"10\"} )",
            "value": 4.4504268715060125,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.StreamRuneBenchmark.eventStoreLoadWithSnapshot ( {\"batchSize\":\"100\"} )",
            "value": 4.865387663185179,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"NOOP\"} )",
            "value": 10.945770664298248,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_OFF\"} )",
            "value": 13.020775235960935,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.runtime.TracingBenchmark.commandBusThroughput ( {\"tracing\":\"SAMPLER_ON\"} )",
            "value": 13.46654577224121,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"1\"} )",
            "value": 636.2657717201978,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToExistingStream ( {\"batchSize\":\"10\"} )",
            "value": 802.4175249497346,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"1\"} )",
            "value": 638.0127361620841,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.appendToNewStream ( {\"batchSize\":\"10\"} )",
            "value": 785.2640516050511,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"10\"} )",
            "value": 331.98719802782205,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.load ( {\"streamLength\":\"100\"} )",
            "value": 770.9963932971007,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"1\"} )",
            "value": 799.4132507654624,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.PostgresEventStoreBenchmark.loadWithUpcasting ( {\"upcastSteps\":\"2\"} )",
            "value": 825.4187445380039,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 10.528244217999907,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.deserializeDecrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 50.32601492966393,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"1\"} )",
            "value": 14.483187924249895,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          },
          {
            "name": "org.streamrune.postgres.CryptoShreddingBenchmark.serializeEncrypting ( {\"encryptedFields\":\"5\"} )",
            "value": 70.3530755922069,
            "unit": "us/op",
            "extra": "iterations: 5\nforks: 2\nthreads: 1"
          }
        ]
      }
    ]
  }
}