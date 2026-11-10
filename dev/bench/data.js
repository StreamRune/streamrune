window.BENCHMARK_DATA = {
  "lastUpdate": 1794338048635,
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
      }
    ]
  }
}