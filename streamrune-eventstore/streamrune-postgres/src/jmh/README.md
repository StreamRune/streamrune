# PostgreSQL event store benchmarks

`PostgresEventStoreBenchmark` measures `PostgresEventStore` against a real PostgreSQL. Every store is
built by `PostgresEventStoreFactory` on the schema the framework's Flyway migrations create, behind a
HikariCP pool of two connections, so each operation runs the production path end to end: on append
the Jackson serialization of payload and metadata, the global-offset reservation, the insert and the
commit; on load the query, the deserialization and the upcasting. One thread, so the numbers are
the latency of one operation, not the throughput of the single-writer append lane under contention.

| Benchmark | Parameter | What one operation is |
| --- | --- | --- |
| `appendToNewStream` | `batchSize` 1, 10 | an append of a batch to a stream that does not exist yet |
| `appendToExistingStream` | `batchSize` 1, 10 | an append of a batch at the head of a stream that has events |
| `load` | `streamLength` 10, 100 | a load of a stream, no snapshot |
| `loadWithUpcasting` | `upcastSteps` 1, 2 | a load of 100 events stored at schema version 1, each upcast through a chain of one or two steps |

`loadWithUpcasting` is the event read path with upcasting: the 100 events are written by a store
without upcasters, which stamps them version 1, and read by a store whose upcaster expects version 2
(`amount` renamed) or 3 (a `currency` added). Its difference from `load` at a length of 100 is the cost
of the upcasting.

## The database

With the environment variable `STREAMRUNE_BENCH_JDBC_URL` set, the benchmarks connect to that server
as `STREAMRUNE_BENCH_JDBC_USER` with `STREAMRUNE_BENCH_JDBC_PASSWORD`; the nightly workflow points
them at its PostgreSQL service this way. Without it, each JMH fork starts a Testcontainers
`PostgreSQLContainer` on `PostgresTestImage.NAME` (Docker required), as the module's tests do.

Every trial drops and recreates the schema `streamrune_bench` and migrates it, and writes nowhere
else, so a trial never sees the rows of another; point the URL at a database where that schema may
be dropped. The append state also empties the event stream and rewinds the global-offset counter
before every iteration. Without that, each iteration would append to a table and an index grown by
all the iterations before it, and the last iterations of a trial would measure a bigger table than
the first; with it, an iteration appends to a table that holds at most what that iteration wrote.
The load states write their streams once per trial and only read them afterwards.

## Running

```bash
./gradlew :streamrune-eventstore:streamrune-postgres:jmh
./gradlew :streamrune-eventstore:streamrune-postgres:jmh -PjmhIncludes=loadWithUpcasting
STREAMRUNE_BENCH_JDBC_URL=jdbc:postgresql://localhost:5432/bench \
  STREAMRUNE_BENCH_JDBC_USER=bench STREAMRUNE_BENCH_JDBC_PASSWORD=bench \
  ./gradlew :streamrune-eventstore:streamrune-postgres:jmh -PbenchCi
```

`-PbenchCi`, the nightly settings and the results file are described in
`streamrune-runtime/src/jmh/README.md`. `PostgresEventStoreBenchmarkSmokeTest` runs every method twice
outside the harness in `jmhSmokeTest`, part of `check`, against a Testcontainers PostgreSQL.
