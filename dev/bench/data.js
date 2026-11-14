window.BENCHMARK_DATA = {
  "lastUpdate": 1794643293046,
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
      }
    ]
  }
}