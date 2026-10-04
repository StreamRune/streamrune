# Changelog

All notable changes to StreamRune are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0-alpha-SNAPSHOT] — unreleased preview

The first published build of StreamRune, a CQRS / Event Sourcing framework for Java 25:
PostgreSQL-native, multi-framework (Spring Boot, Quarkus, Micronaut), GDPR-ready. Explicit, readable
code — no annotation magic, no hidden behavior. This entry describes what the preview contains.

`1.0.0-alpha-SNAPSHOT` is an unreleased preview of `1.0.0`. It is rebuilt from `main` each time the
Build workflow passes on it and published only to the Maven Central snapshot repository
(`https://central.sonatype.com/repository/maven-snapshots/`), not to Maven Central itself. Snapshot
artifacts are not signed, and the API, the database schema and the configuration keys can change in
any later snapshot without a migration path. Every module shares the `org.streamrune` group and the
`1.0.0-alpha-SNAPSHOT` version and ships with sources and Javadoc jars.

```kotlin
repositories {
    mavenCentral()
    maven("https://central.sonatype.com/repository/maven-snapshots/") {
        mavenContent { snapshotsOnly() }
        content { includeGroup("org.streamrune") }
    }
}

dependencies {
    implementation("org.streamrune:streamrune-spring-boot-starter:1.0.0-alpha-SNAPSHOT") // Spring Boot
    // or pick modules: streamrune-core, streamrune-runtime, streamrune-postgres, streamrune-quarkus, …
    testImplementation("org.streamrune:streamrune-test:1.0.0-alpha-SNAPSHOT")
}
```

A complete e-commerce example — five aggregates, projections, sagas, crypto-shredding, the outbox and
the dead-letter queues, wired three times (Spring Boot, Quarkus, Micronaut) over one shared domain,
with a 17-chapter tutorial — lives in the separate repository
[streamrune-ecommerce-demo](https://github.com/StreamRune/streamrune-ecommerce-demo) under the Apache
License 2.0.

### Modules

Sixteen published artifacts, one per Gradle project in `settings.gradle.kts` except the four
aggregator projects (`streamrune-eventstore`, `streamrune-crypto`, `streamrune-integration`,
`streamrune-outbox`) and the unpublished end-to-end test project `streamrune-e2e`. The artifact id is
the project name. Each jar declares a stable JPMS module name (four ship a `module-info.java`, the
other twelve carry `Automatic-Module-Name`), bundles `LICENSE`, `NOTICE` and `LICENSE-COMMERCIAL.md`
under `META-INF`, and sets `SPDX-License-Identifier: BUSL-1.1` in its manifest.

| Artifact (`org.streamrune:`) | Gradle project | JPMS module | Contents |
|---|---|---|---|
| `streamrune-core` | `:streamrune-core` | `org.streamrune.core` | Contracts and value types: `EventStore`, `Decider`, `CommandBus`, `Projection`, `ProjectionDeliveryMode`, `QueryBus`, saga, subscription, outbox, audit, GDPR and `CryptoEngine` SPIs, typed ids, `MetricNames`. Depends only on Jackson. |
| `streamrune-runtime` | `:streamrune-runtime` | `org.streamrune.runtime` | `StreamRune` facade, `VirtualThreadCommandBus`, interceptors, projection runners, subscriptions, `SagaRunner` and its sweepers, outbox poller and replayer, GDPR services, retention sweepers, SSE publisher. |
| `streamrune-postgres` | `:streamrune-eventstore:streamrune-postgres` | `org.streamrune.postgres` | `PostgresEventStore` and its factory, the Flyway baseline, `SchemaValidator`, `PgAdvisoryLocker`, `LeaseBasedLeadership`, `JdbcProjectionRepository`, the PostgreSQL inbox, outbox, saga, dead-letter, audit and offset stores, LISTEN/NOTIFY subscriptions. |
| `streamrune-crypto-api` | `:streamrune-crypto:streamrune-crypto-api` | `org.streamrune.crypto` | `CryptoShreddingModule` (Jackson), `CachedCryptoEngine`, `ForgottenSubjectStore`, `CryptoConfigValidator`. |
| `streamrune-filesystem-crypto` | `:streamrune-crypto:streamrune-filesystem-crypto` | `org.streamrune.filesystem` | `FileSystemCryptoEngine` — AES-256-GCM keys on disk. |
| `streamrune-postgres-crypto` | `:streamrune-crypto:streamrune-postgres-crypto` | `org.streamrune.crypto.postgres` | `PostgresCryptoEngine`, `JdbcForgottenSubjectStore`, `PostgresCryptoForgetSignal`, the crypto Flyway baseline. |
| `streamrune-vault-crypto` | `:streamrune-crypto:streamrune-vault-crypto` | `org.streamrune.vault` | `VaultCryptoEngine` — HashiCorp Vault transit engine. |
| `streamrune-aws-kms-crypto` | `:streamrune-crypto:streamrune-aws-kms-crypto` | `org.streamrune.aws.kms.crypto` | `AwsKmsCryptoEngine` — AWS KMS customer managed key with per-subject tombstones. |
| `streamrune-test` | `:streamrune-test` | `org.streamrune.test` | `InMemoryEventStore`, fixtures, in-memory doubles of every store and bus, store contract suites. |
| `streamrune-integration-api` | `:streamrune-integration:streamrune-integration-api` | `org.streamrune.integration` | Shared integration pieces: `MicrometerStreamRuneMetrics`, `RequestIdentityPolicy`, `AuthenticatedUserResolver`, `SseAuthorizer`, `ProjectionProcessorResolution`, `CommandInboxWiringValidator`, `SagaRetentionValidator`. |
| `streamrune-spring` | `:streamrune-integration:streamrune-spring` | `org.streamrune.spring` | Spring Boot 4 auto-configuration, health indicator, SSE controller, request filter, native-image hints. |
| `streamrune-spring-boot-starter` | `:streamrune-integration:streamrune-spring-boot-starter` | `org.streamrune.spring.boot.starter` | Dependency aggregator: `streamrune-spring` + `streamrune-postgres` + `streamrune-runtime`. |
| `streamrune-quarkus` | `:streamrune-integration:streamrune-quarkus` | `org.streamrune.quarkus` | Quarkus (ArC) producers, lifecycle, readiness check, SSE controller, request filter, reflection config; ships `META-INF/beans.xml` and a Jandex index, no extension build step. |
| `streamrune-micronaut` | `:streamrune-integration:streamrune-micronaut` | `org.streamrune.micronaut` | Micronaut factories, lifecycle, health indicator, SSE controller, request filter, `reflect-config.json`. |
| `streamrune-kafka-outbox` | `:streamrune-outbox:streamrune-kafka-outbox` | `org.streamrune.kafka` | `KafkaOutboxPublisher`. |
| `streamrune-rabbitmq-outbox` | `:streamrune-outbox:streamrune-rabbitmq-outbox` | `org.streamrune.rabbitmq` | `RabbitMqOutboxPublisher` (publisher confirms). |

### Streams and aggregate types

- A stream is the typed pair `StreamId(AggregateType, AggregateId)`, shown as `<aggregateType>:<aggregateId>`
  in JSON, log lines and the Kafka message key. Two aggregate types may use the same id value without
  sharing a stream, a version counter, a lock, a snapshot, an idempotency binding, a dead-letter row
  or an outbox head.
- Every decider registration names its aggregate type:
  `register(AggregateType, Class<C>, Function<C, AggregateId>, Decider)` on `VirtualThreadCommandBus`,
  the `StreamRune` builder, `InMemoryCommandBus` and the integrations' `DeciderRegistration` beans.
  `AggregateType` syntax is `[a-z][a-z0-9_]{0,31}`. A registration that repeats a command type,
  overlaps a registered command root under a different aggregate type, or registers one decider
  instance under two types is refused with `IllegalArgumentException`; the bus logs each registered
  aggregate type when it is built.
- The command bus composes the `StreamId` once per command, before any interceptor runs.
  `CommandContext`, `AuditEntry` and `EventEnvelope` carry the aggregate type; `AggregateLocker`
  locks a `StreamId`; a dead-letter record holds one `streamId`; the span attribute
  `streamrune.aggregate.type` names it.
- `StreamId` has no bare-string factory. `StreamId.of(AggregateType, AggregateId)` builds one;
  `StreamId.parse` is the decode path for a stored `<type>:<id>` text form. An HTTP request carries the
  two parts as two values (SSE: `GET /api/sse/{aggregateType}/{aggregateId}`, `400` on an invalid
  part).
- `AggregateId.of(value)` is the ingress factory: it refuses a blank value, a value longer than 255
  UTF-16 code units and any C0, DEL or C1 control character with `IllegalArgumentException`, so a
  client-supplied id becomes a rejection before any interceptor runs instead of an SQL error at
  append. The `StreamId` constructor bounds the id by its `VARCHAR(255)` column as well.
- Every stream-keyed table keys on `(aggregate_type, aggregate_id)`; no table stores a composed stream
  id. The columns carry a syntax `CHECK` on the type and, where the stream is optional (dead-letter
  queue, outbox), a both-or-neither `CHECK`.

### Event store — PostgreSQL, gap-free global order

- `EventStore` contract: `load`, versioned `load(streamId, expectedSnapshotVersion)`,
  `append(streamId, events, expectedVersion)` with optimistic concurrency, `appendWithKey`
  (idempotency key committed with the events), `saveSnapshot`, `readStream`, `readGlobalStream`,
  `lastGlobalOffset`. `AppendResult` returns the store-assigned `GlobalOffset`s and the final
  `Version`.
- `PostgresEventStore` / `PostgresEventStoreFactory`: JSONB payload and metadata columns, the unique
  constraint `event_stream_stream_version_key` on `(aggregate_type, aggregate_id, version)` for
  optimistic locking (`OptimisticLockException` on conflict), and gap-free, commit-ordered global
  offsets reserved from a single counter row whose lock is held until the append transaction commits
  or rolls back. A committed offset N means every offset below N is committed; a rolled-back append
  leaves no hole.
- **The application's `DataSource` is used as given.** The factory builds no connection pool of its
  own, changes no setting of the `DataSource` and owns nothing that needs closing; the store borrows
  one connection per operation. Hand it a pooled `DataSource`.
- **Statement bound.** Every statement the store runs is bounded — `statementTimeout(Duration)`,
  `streamrune.event-store.statement-timeout`, 30 seconds by default, `0` for none — per transaction
  (`SET LOCAL statement_timeout`) or per statement (the JDBC query timeout), never on the pooled
  session.
- Store writes commit correctly on a pool configured with `auto-commit=false` as well as with the
  default: the store commits after its statement and rolls back on failure, leaving the pool's
  setting untouched. This covers the outbox, offsets, snapshots, dead letters, saga state, leases,
  audit, retention sweeps, crypto tombstones, the projection repository, `pg_notify` and `LISTEN`.
- One Flyway baseline per series, each on its own history table so it never collides with the
  application's own migrations: `classpath:db/streamrune-migration/V001__streamrune_baseline.sql`
  recorded in `flyway_schema_history_streamrune` creates `event_stream`, `global_offset_sequence`,
  `snapshot_store`, `projection_offset`, `subscription_leases`, `projection_dead_letters`,
  `command_inbox`, `dead_letter_queue`, `audit_log`, `event_audit_log`, `outbox_events`, `saga_state`
  and `saga_dead_letters`; the crypto series `classpath:db/crypto-migration/V001__crypto_baseline.sql`
  (in `streamrune-postgres-crypto`, history table `flyway_schema_history_crypto`) creates
  `encryption_keys`, `forgotten_subjects` and `erased_key_generations`, each only when absent. Every
  object is created unqualified in the connection's current schema.
- The factory applies the baseline at startup (`autoInitializeSchema`, default on; the integrations'
  `streamrune.event-store.schema.auto-initialize`), applies the crypto series when the configured
  `CryptoEngine` reports crypto tables, then validates (`validateSchema`, default on): a missing
  required table or column, a `global_offset_sequence` without its counter row or with a counter
  behind `max(global_offset)`, or a schema-history version behind the one this build ships is a
  `SchemaValidationException` and the application does not start. A version ahead of this build or a
  missing history table only logs a warning. Missing-column messages carry the exact `ALTER TABLE`.
  Schema auto-initialization works in a GraalVM native image; a missing migration script fails
  startup and names it.
- **PostgreSQL 17 or newer is enforced.** `PostgresEventStoreFactory.create()`, `initializeSchema()`
  and the static `PostgresEventStore.create(...)` read the server version first and throw
  `UnsupportedServerVersionException` on an older server, before anything is written;
  `PostgresServerVersion.requireSupported(dataSource)` is the same check for stores built by hand.
- `PostgresEventStore.create(...)` and `PostgresEventStore.builder().build()` refuse, before touching
  the database, a registered type with an `@Encrypted` component when no `CryptoEngine` is
  configured.
- `EventMetadata` on every envelope: `eventId`, `commandId`, `traceId`, `spanId`, `correlationId`,
  `causationId`, `userId`, `timestamp` and allow-listed W3C baggage, built by the command bus from
  the bound `StreamRuneContext`.
- `PgAdvisoryLocker` — distributed per-stream pessimistic locking via `pg_advisory_xact_lock` over the
  typed stream, with `withDedicatedPool(...)` so lock connections never starve the application pool.
- Typed identifiers: `StreamId`, `AggregateType`, `AggregateId`, `Version`, `GlobalOffset`, `EventId`,
  `CommandId`, `CorrelationId`, `CausationId`, `TraceId`, `SpanId`, `UserId`, `SubjectId`,
  `IdempotencyKey`, `ProjectionName`, `SubscriptionName`, `SagaId`, `SagaType`, `EventType`. Ingress
  factories (`AggregateId.of`, `AggregateType.of`, `IdempotencyKey.of`/`scopedTo`,
  `RequestContext.fromRequest`) refuse malformed values before any interceptor runs; the canonical
  constructors are the decode path for values read back from storage. Exception messages quote ids,
  names and configuration text sanitized.

### Snapshots, schema evolution and upcasting

- `SnapshotPolicy.never()` or `SnapshotPolicy.everyNEvents(n, snapshotVersion)` (`everyNEvents(n)` is
  schema version 1). `never()` is the `VirtualThreadCommandBus` builder default; the three
  integrations default to `everyNEvents(streamrune.snapshot-every-n-events)` — `100` events at
  snapshot schema version 1 — unless an application `SnapshotPolicy` bean replaces the policy. A
  stored snapshot whose schema version differs from the expected one is migrated through the
  registered `SnapshotMigration` chain when a complete chain exists, otherwise discarded and the
  aggregate rebuilt from all events. A migration step that throws discards the snapshot (metrics
  reason `migration_failure`) unless the failure is crypto-related, which propagates. Snapshot
  version `0` is rejected.
- `SnapshotMigration` steps (`fromVersion`, `toVersion`, `migrate(AggregateState)`, optional
  `toType()` result check) are registered on the store (`PostgresEventStoreFactory.snapshotMigrations(...)`,
  `InMemoryEventStore.withMigrations(...)`) and applied on load against the old state class resolved
  by the `EventTypeRegistry`.
- `EventUpcaster` / `UpcasterChain`: one upcaster owns every version step of one event type; a stored
  `schema_version` below the chain's version is upcast one step at a time before the payload is bound
  to its class. The stored version records the writer's chain, not the payload shape, so upcaster
  steps keep values the payload already has (`putIfAbsent`). A store refuses to read an event stored
  at a schema version above what its chain reaches rather than mis-binding it.
- `SimpleEventTypeRegistry` maps event-type and state-type names to classes; the `StreamRune`
  builder's `registerEventType` / `registerStateType` maintain one for you.
- `PostgresSnapshotStorePurger` — an opt-in `SubjectDataPurger` for `snapshot_store`, for derived
  plaintext an aggregate state copied out of personal data.

### Command bus, idempotency and the command inbox

- `VirtualThreadCommandBus` (implements `CommandBus` and `AsyncCommandBus`): load → `Decider.guard`
  → `Decider.decide` → append, under a per-stream lock (`LocalStripedLocker`, 1024 stripes by
  default, or `PgAdvisoryLocker`), with `RetryPolicy` retries for `OptimisticLockException` and
  `LockException` only (default 3 attempts, 50 ms, ×2, jitter). `lockTimeout` defaults to 5 s; `0`
  means a single non-blocking attempt; the builder refuses `null`, the lockers reject a negative value,
  and the integrations reject a negative `streamrune.lock-timeout` at startup.
- `CommandResult` carries the events, `streamId`, `finalVersion`, `globalOffsets`, the envelopes and
  a never-null `ShortCircuitReason` (`NONE`, `VETOED`, `IDEMPOTENT_REPLAY`) with the predicates
  `shortCircuited()`, `vetoed()`, `idempotentReplay()`.
- `executeAsync` returns a `CompletableFuture<CommandResult>` on a fresh virtual thread and is bounded
  by an in-flight admission budget (`maxInFlightAsyncCommands`, default 10,000;
  `streamrune.max-in-flight-async-commands` on all three integrations): past it the call returns an
  already-failed future carrying `CommandBusOverloadedException`. A command frees its admission slot
  before its future completes, so a caller that resubmits on completion is never refused by its own
  finished command. The synchronous `execute` is not gated. `close()` drains in-flight commands.
- Idempotent (effectively-once) execution: `execute(command, IdempotencyKey)` records the key inside
  the append transaction (`EventStore.appendWithKey`); a replay reconstructs the original result from
  the `command_inbox` row without running the handler and reports `idempotentReplay() == true`. The
  bus re-checks the key under the aggregate lock on every attempt and once more when a keyed decider
  rejects, so a duplicate that overlaps the original execution is answered from the inbox, never
  turned into a business rejection. A key is bound to its command type and its stream — reuse with
  another command or stream throws `IllegalArgumentException`. `IdempotencyKey.scopedTo(scope, key)`
  builds caller-scoped keys. `supportsIdempotentExecution()` reports whether a `CommandInbox` is
  wired; without one the keyed overload throws `IllegalStateException`.
- `CommandInbox` SPI (`find`, `deleteProcessedBefore`) with `PostgresCommandInbox`; the write side
  lives only inside `appendWithKey`. `InboxRetentionSweeper` prunes processed keys after
  `streamrune.inbox.retention-max-age` (default 7 days; zero or negative disables pruning). The window
  is a correctness setting: it must outlast the longest redelivery horizon.
- `CommandInterceptor` (`before` / `after` / `onError`, `compose`, `noop`), executed in registration
  order for `before` and reverse order for `after`/`onError`; when an interceptor vetoes a command,
  `after()` runs with a `VETOED` result on every interceptor ahead of it. Shipped interceptors:
  `AuditCommandInterceptor`, `AuthorizationCommandInterceptor`, `AnnotationAuthorizationInterceptor`,
  `CircuitBreakerCommandInterceptor`, `BeanValidationInterceptor` (Jakarta Validation →
  `ValidationException`), `OpenTelemetryCommandInterceptor`, `InlineProjectionInterceptor`.
- Circuit breaker: `CLOSED → OPEN` after `failureThreshold` consecutive infrastructure failures,
  `HALF_OPEN` after `cooldown` with exactly one probe (30 s probe timeout). Business rejections
  (`DomainException`, `IllegalArgumentException`, a `SubjectForgottenException` in the cause chain)
  and deterministic per-aggregate stored-data failures never count.
- Command dead-letter queue: after retries are exhausted, infrastructure failures (never business
  rejections) are published to the `DeadLetterQueue` (`PostgresDeadLetterQueue`; the command is
  stored as JSON through `DeadLetterRetryRunner.createObjectMapper(cryptoEngine)`, so its own
  `@Encrypted` components are ciphertext; a payload that cannot be serialized is stored as a
  metadata-only entry, which is never dispatched). A command that can be dead-lettered must
  round-trip through Jackson, because the retry runner rebuilds it from the stored JSON.
  `DeadLetterRetryRunner` re-dispatches entries under the command's original idempotency key (or a
  deterministic `dlq-replay:<commandId>` key), learns command types from the registered deciders
  (sealed roots expand to every permitted class), refuses to replay an entry whose `@Encrypted`
  fields read `[REDACTED]`, is leadership-gated, and offers an on-demand `retry(CommandId)` that runs
  on its own virtual thread with the context recorded on the entry, never the operator's identity,
  correlation, baggage or saga marker. `DeadLetterRetentionSweeper` prunes after
  `streamrune.dead-letter.retention-max-age` (default 30 days; zero or negative disables pruning) and
  is registered for any `DeadLetterQueue` bean; `streamrune.dead-letter.enabled=false` switches off
  only the automatic retry runner.

### Aggregates and deciders

- `Decider<C extends Command, S extends AggregateState, E extends DomainEvent>` — a pure function in
  three methods (`initialState`, `decide`, `evolve`) plus the optional `guard(command, state)`
  authorization hook, run on the loaded state before `decide` (also for the creating command, where
  `state` is `initialState()`). Commands implement the `Command` marker; sealed interfaces with one
  record per variant are the idiomatic shape, and one registration covers a whole sealed hierarchy.
- Business rules are rejected with `DomainException` (and its `ValidationException` /
  `AuthorizationException` subtypes); these propagate immediately, are never retried, never
  dead-lettered and never open the circuit breaker.
- `Authorization.requireOwner(ownerId)`, `Authorization.requireOwnerOrSaga(ownerId)` (admits a
  saga-dispatched command, which carries no end user), `@RequireRole`, `@RequirePermission` and
  `CommandAuthorizationPolicy` for ownership and role checks (see Security).
- `StreamRune` facade (`streamrune-runtime`): a fluent builder wiring the event store (`eventStore`,
  `eventStoreFactory`, or `eventStoreFactoryProvider`, which threads the crypto engine, upcasters and
  type registry into the store it constructs), deciders, locker, retry and snapshot policies,
  interceptors, metrics, dead-letter queue, projections and subscription config. It implements
  `CommandBus` and `AutoCloseable`.

### Projections and read models

- **Every projection registration declares a `ProjectionDeliveryMode`; there is no default.**
  - `TRANSACTIONAL_LOCAL` — the read-model write and the checkpoint commit in one transaction. Needs
    a processor with `supportsFencing()` and a projection that writes through the repository handed
    to `process(batch, repository)`.
  - `AT_LEAST_ONCE_IDEMPOTENT` — the write and the checkpoint commit separately; the projection is
    handed `null` as its repository under every processor and must dedup, version-guard or upsert.
    Declaring this mode is the only acceptance of at-least-once delivery.
  - `EXTERNAL_EFFECT` — the projection's job is a side effect outside the database: it writes an
    outbox row through the handed repository in the checkpoint's transaction, and a relay delivers it
    at-least-once. Mechanically identical to `TRANSACTIONAL_LOCAL`.
- The mode is required on `MultiProjectionRunner.Builder.register(name, projection[, strategy], mode)`,
  `ScheduledProjectionRunner.Builder.register(name, projection, cron[, strategy], mode)`,
  `StreamRune.Builder.registerProjection(name, projection, mode)`,
  `ProjectionRunner.run(name, projection, mode)`, the `ProjectionRegistration` constructors and
  `@ProjectionConfig(deliveryMode = …)`.
- Every runner requires an explicit `AtomicBatchProcessor` (the `PollingProjectionRunner`
  constructors take it as their fifth argument). `JdbcProjectionRepository` is the transactional,
  fencing processor; the non-transactional two-commit processor is reachable only as
  `AtomicBatchProcessor.nonAtomicAtLeastOnce()`, which refuses a non-zero leadership epoch.
- `ProjectionDeliveryPolicy` checks each registration at `build()`/`run()`, before any event is read:
  `TRANSACTIONAL_LOCAL`/`EXTERNAL_EFFECT` need a fencing processor, a projection that writes through
  the handed repository (`writesThroughRepository()`) and — when the projection exposes
  `Projection.writeTarget()` (`BaseProjection` returns its repository, the decorators forward it) — a
  processor whose `writesTo(target)` is true, compared by `ProjectionRepository.writeTargetIdentity()`
  (a `JdbcProjectionRepository`'s identity is its `(DataSource, ObjectMapper)` pair, so a bean proxy
  on either side verifies). Each executing runner logs one INFO line per registration and records the
  gauge `streamrune.projections.delivery_mode{projection.name, delivery.mode}`.
- Under `nonAtomicAtLeastOnce()` a failed checkpoint save after a successful `process` is a
  `ProjectionCheckpointSaveException`, retried from the unchanged checkpoint under every error
  strategy (on the next tick for `ScheduledProjectionRunner`); the range is never skipped and never
  dead-lettered.
- `BaseProjection` provides `save()`, `findById()` and `delete()` helpers that use the batch's handed
  repository and the captured one otherwise; inside `process` a projection writes only through them.
- `ProjectionFencingPolicy`: a leadership-aware runner refuses to build real (multi-replica)
  `SubscriptionLeadership` over a processor whose `supportsFencing()` is `false`.
- Runners: `ContinuousProjectionRunner` / `MultiProjectionRunner` (polling the global stream, one
  virtual thread per registration), `ScheduledProjectionRunner` (cron-triggered drain,
  `CronExpression`), `PollingProjectionRunner`; `ProjectionRunner.reset(name)` rewinds the offset
  only — read models and dead-letter entries are left in place; a clean rebuild deletes them first.
  Stopping a runner or a polling subscription never interrupts a store write in progress.
- `@ProjectionConfig(name, mode, cron, errorStrategy, batchSize, fetchSize, deliveryMode)` for the
  three integrations; per-projection properties `streamrune.projections.<name>.*` override the
  annotation. `Mode.CONTINUOUS` and `Mode.SCHEDULED` are durable, checkpointed and recover by offset
  replay; `Mode.INLINE` runs on the command thread after the command's events commit, ordered per
  aggregate within one JVM, with no checkpoint and no recovery, and must declare
  `AT_LEAST_ONCE_IDEMPOTENT`.
- Error strategies (`ProjectionErrorStrategy`): `HALT`, `SKIP`, `DLQ`. Under `DLQ`,
  `ProjectionErrorClassifier` separates `TRANSIENT` (`SQLException`, bare `CryptoOperationException`,
  `EventStoreException`, I/O — retried with capped backoff) from `POISON` (deserialization, unknown
  event type, `CryptoMappingException`, `SubjectForgottenException`, everything else —
  dead-lettered); the failed `[fromOffset, toOffset]` range lands in `projection_dead_letters` as
  metadata only (never the payload) and the runner continues past it.
  `ProjectionDeadLetterReplayer.replay(name, projection, max)` re-reads each range from the event
  store oldest-first, reports `replayed` / `failed` / `fenced`, never moves the checkpoint, and
  re-applies events at-least-once, outside the checkpoint transaction, for every mode.
- `JdbcProjectionRepository`: one table per projection name (`<name>_view`, names matching
  `[a-z_][a-z0-9_]{0,57}`, checked at startup, never rewritten); `executeAtomically` locks the
  checkpoint row `FOR UPDATE`, applies the epoch fence, the overlap guard (a batch starting at or
  before the committed checkpoint is rejected) and the monotonic guard, and commits read-model writes
  and the offset together. `afterCommit(...)` hooks run once the transaction committed and are dropped
  on rollback.
- `PostgresOffsetStore` ignores a backward or sideways `saveOffset` (a monotonic no-op);
  `OffsetStore.reset(name)` is the deliberate rewind for a rebuild.
- `WindowedProjection` — time-windowed aggregation over `Window(start, end)`, `WindowSink`,
  `LateDataPolicy` (`DROP`, `REOPEN`), self-fencing on the highest offset it has accumulated.
- Decorators `TracingProjectionDecorator`, `ValidatingProjectionDecorator` and `CacheAwareProjection`
  forward `process(List, ProjectionRepository)`, `writesThroughRepository()`, `writeTarget()` and
  `processDeadLetterReplay(List)` to their delegate.
- Projection names, subscription names and offsets share one `projection_offset` namespace; the modes
  use no extra table.

### Queries and the query cache

- `QueryBus` (`dispatch(Query<R>)`, `register(Class<Q>, QueryHandler<Q, R>)`) with `SimpleQueryBus`,
  `AuditingQueryBus` (for `@Auditable` queries) and `CachingQueryBus`; `Page`, `PageRequest`, `Sort`
  for paginated read models.
- `@Cacheable(ttlSeconds, maxEntries, scope, invalidateOn)` on a query record opts it into a
  per-query-type Caffeine cache (defaults: 60 s, 1000 entries, `Scope.USER` — keyed by the calling
  `UserId`; anonymous callers bypass the cache; `Scope.GLOBAL` keys by the query alone).
  `invalidateOn` evicts a query type's entries after the batch that processed a listed event type has
  committed (`CacheInvalidator`, wired through `CacheAwareProjection` or
  `ProjectionRepository.afterCommit`). `evict(Class)` and `evictAll()` for manual eviction.
- `QueryAuthorizer` runs before the cache is touched on every dispatch of a `@Cacheable` query, hit or
  miss. Without one the cache has no authorization opinion — a cache hit skips the handler and
  whatever check it performs.
- `streamrune.query-cache.enabled=true` (default `false`) makes the integrations produce the
  `CachingQueryBus` as the primary `QueryBus` plus a `CacheInvalidator` bean; an application
  `QueryAuthorizer` bean is auto-detected.

### Subscriptions

- `EventSubscription` / `SubscriptionLifecycle` (`CREATED → RUNNING ⇄ PAUSED → STOPPED`;
  `close()` is terminal) with `PollingEventSubscription` (global stream), `StreamEventSubscription`
  (one stream) and `HybridEventSubscription` (PostgreSQL LISTEN/NOTIFY push plus polling fallback —
  the recommended production subscription). `PostgresNotificationSubscription` listens on any
  `DataSource` it is given. Closing a subscription never interrupts a delivery in progress.
- `SubscriptionConfig.DEFAULT` = LISTEN/NOTIFY on, 5 s polling interval, 1 s jitter;
  `SubscriptionConfig.pollingOnly(interval)` for transaction-pooling proxies.
- `ResilientPollLoop`: a transient `RuntimeException` in the poll loop is logged and retried with
  capped exponential backoff plus jitter. `PgNotificationListener` reconnects with capped backoff and
  detects a connection whose server vanished without closing it: after 10 s without a notification it
  runs `SELECT 1` with a 5 s timeout and reconnects on failure. `HybridEventSubscription.pushHealthy()`
  and `pushPathStatus()` report the push path, which reads dead after 30 s without a LISTEN
  connection. `NOTIFY` is sent after commit and carries only the highest new offset; events are
  always read from `event_stream`, so a lost notification costs latency, never delivery.
- `SubscriptionHealthContributor` + `HealthTrackingEventListener`: per-subscription status.
  `DOWN` only for a stopped or terminally halted subscription; `DEGRADED` for lag at or above
  `streamrune.subscription.health.lag-threshold` (default 1000 events), consecutive delivery errors,
  `PAUSED`, a dead push path, or a checkpoint ahead of the stream head (one WARN per episode); `UP`
  otherwise. Lag never turns the health check `DOWN`, so a catch-up or a rebuild keeps every replica
  ready; alert on the `streamrune.subscriptions.lag` gauge.
- Single-active-consumer leadership: `LeaseBasedLeadership` is a persisted, database-clocked lease in
  `subscription_leases` with a strictly increasing fencing `epoch` — acquire, renew (at `lease-ttl / 3`)
  and resign are each one statement on a borrowed connection, correct under any connection-pooling
  mode including PgBouncer transaction mode; a renew queued behind a resign cannot extend the resigned
  lease. Failover completes within `streamrune.subscription.single-active-consumer.lease-ttl` (default
  15 s) regardless of how the leader died. The leader stamps its epoch on the checkpoint row at
  takeover before reading anything (a failed stamp fails closed), and `JdbcProjectionRepository`
  rejects any checkpoint commit whose epoch is below the stamped one. `single-active-consumer.enabled=false`
  selects `SubscriptionLeadership.NOOP` (always leader, epoch 0, unfenced) for single-instance
  deployments. The same leadership gates `DeadLetterRetryRunner` and the saga sweepers.

### Sagas

- `SagaOrchestrator<S extends SagaState>` (routing + logic) or `SagaDecider<S>` (logic only, routing
  configured on the builder with `startWhen`, `extractSagaId`, `correlateBy`), driven by `SagaRunner`
  as an `EventListener` on any subscription. `SagaCommand.of(command, aggregateId)` names each
  dispatch's target; correlation rides on `EventMetadata.correlationId`, set to the saga id on every
  saga-issued command. Domain code recognises a saga dispatch through
  `StreamRuneContext.isSagaDispatch()` (`StreamRuneContext.SAGA_OWNED`).
- Lifecycle: `STARTED`, `RUNNING`, `COMPLETED`, `COMPENSATING`, `COMPENSATED`, `FAILED`, `FAULTED`.
  The `saga_state` row's `status` is authoritative. Recovery facts live on the row and are written only
  by the code that establishes them: `genesis_applied`, `pre_fault_status`, `dead_letter_pending`,
  `last_applied_offset`, `last_replayed_offset`; the compensation episode is identified by
  `episode_version` and `episode_claimed_at`, stamped by the very write that lands a row at
  `COMPENSATING` and enforced by the `saga_state_episode_stamped` `CHECK` constraint. A row in a
  compensation episode without the stamp is refused with `SagaUnstampedCompensationEpisodeException`.
- Genesis-pending insert: a start event's row is written before its first command is dispatched and
  committed (`genesis_applied = TRUE`) after the dispatch loop, so a saga never dispatches without a
  row and a crashed start is visible to the timeout runner.
- Effectively-once dispatch: every saga command goes through `CommandBus.execute(command, key)` —
  forward commands keyed `saga:<correlationId>:<triggerOffset>:<index>`, every compensation command
  keyed `saga:<correlationId>:episode:<episodeVersion>:comp:<index>` — so a redelivery, a crash-resume
  or a concurrent re-driver dedups in the command inbox instead of re-executing a refund.
- Forward-command failure classification: a proven retry-later failure (`CircuitBreakerOpenException`,
  an interceptor veto, `CommandBusClosedException`, `CommandBusOverloadedException`,
  `OptimisticLockException`, `LockException`, `SQLException`, a bare `CryptoOperationException`)
  leaves the saga untouched and propagates so the event is redelivered; everything else claims
  `COMPENSATING` first, then calls `compensate(state, failure, failedCommand)` and dispatches the undo.
- Compensation outcomes: all undo commands succeed → `COMPENSATED`; a deterministic rejection or an
  empty `compensate()` list → `FAILED`; a transient failure or a veto → the episode stays
  `COMPENSATING` and is re-driven under the same keys by a redelivered correlated event, by
  `SagaTimeoutRunner`, or by the leadership-gated `SagaCompensationRetrySweeper` (default on, interval
  60 s, give-up horizon 1 h → `FAULTED`).
- `SagaTimeoutRunner` serves forward timeouts (by start instant) and `COMPENSATING` re-picks (by last
  write) as two populations interleaved in each batch, retries a failing poll with capped exponential
  backoff (up to one minute, jittered), and refuses at `build()` a decider without a `timeout()`, a
  non-positive `pollInterval` and a non-positive `batchSize`.
- Failure model: an exception (or a `null` router result) from `isStartEvent`, `correlate`,
  `extractSagaId`, `initialState`, `evolve`, `handle` or `compensate` is poison — the event is
  quarantined in the `SagaDeadLetterStore` as metadata only (never the payload), the saga is marked
  `FAULTED` with its `pre_fault_status`, and the batch continues. Store and dead-letter-store
  infrastructure failures propagate and the batch is retried.
- Held events: a correlated event its saga cannot consume yet is recorded under the saga, never
  dropped, and drained oldest-first by `replayAll`. Terminal sagas ignore further events; redeliveries
  at or below `last_applied_offset` are skipped before `evolve` runs.
- Operator recovery with `SagaDeadLetterReplayer`: `replayAll(sagaId)`, `replay(sagaId, offset)`,
  `discard(sagaId, offset)`, `resumeFaulted(sagaId)` for a give-up fault without a dead-letter entry,
  and `compensateFaulted(sagaId)` for a forward fault whose entry an operator discarded (metered as
  `streamrune.saga.compensate_faulted`). Replay outcomes: `REPLAYED`, `STILL_POISON`, `ENTRY_NOT_FOUND`,
  `EVENT_NOT_FOUND`, `STALE_REDRIVE_BLOCKED`, `STALE_COMPENSATION_BLOCKED`, `TARGET_PENDING`,
  `SAGA_ROW_PENDING`, `OLDER_ENTRY_PENDING`; resume outcomes `RESUMED`, `STILL_POISON`, `NO_PROGRESS`,
  `STALE_COMPENSATION_BLOCKED`, `SAGA_NOT_FOUND`, `NOT_FAULTED`, `FORWARD_FAULT`, `ENTRIES_PENDING`;
  compensate outcomes `TERMINAL`, `COMPENSATING`, `POISON`, `CLAIM_LOST`, `SAGA_NOT_FOUND`,
  `NOT_FAULTED`, `COMPENSATION_FAULT`, `ENTRIES_PENDING`. The `force` overloads lift only the key-age
  refusals, never ordering. `discard` logs a WARNING naming `compensateFaulted` or `resumeFaulted`
  when it leaves a `FAULTED` saga with no entry.
- Key-age guards: a compensation episode older than `streamrune.inbox.retention-max-age` (anchored on
  `episode_claimed_at`) is refused by the replayer and faulted without dispatch by the sweeper, the
  timeout runner and the event path, because its dedup keys may have been pruned. A zero or negative
  inbox window disables pruning and makes every guard inert.
- Retention: `SagaDeadLetterRetentionSweeper` prunes entries after
  `streamrune.saga.dead-letter-retention-max-age` (default 7 days), never a recovery handle.
  `SagaRetentionValidator` refuses startup when that window exceeds the inbox window.
- Saga-dispatched commands run as the saga system principal: both authorization interceptors skip the
  per-user check for a saga dispatch (every skip is logged); `Decider.guard` admits one through
  `Authorization.requireOwnerOrSaga`. Other interceptors still apply, and a veto decides nothing: the
  saga stays as it is and the work is redelivered, bounded by the saga's `timeout()`.
- `@Encrypted` saga-state fields are encrypted at rest and within forget scope when the
  `PostgresSagaStore` is built with `PostgresSagaStore.createObjectMapper(cryptoEngine)` — the mapper
  is an explicit, mandatory constructor argument.
- A `SagaStore` insert of a different saga type under the same `SagaId` throws
  `SagaTypeCollisionException`; lookups, drains and discards are saga-type scoped.

### Transactional outbox

- `OutboxEventMapper.toOutbox(EventEnvelope)` runs on the append path and its `OutboxEntry`s are saved
  on the append's own connection: a command's events and its outbox rows commit or roll back
  together. Set both the mapper and a `PostgresOutboxStore` (on the factory, or as two beans in an
  integration). `OutboxEntryId` is the idempotency key; saving the same id twice is a no-op.
- Ordering modes (`OutboxOrderingMode`, a constructor argument of the store, logged at relay start):
  - `STRICT_PER_AGGREGATE` (**default**) — `OutboxEntry.streamId` is mandatory
    (`OutboxOrderingViolationException` otherwise); a terminal `FAILED` head blocks every later entry
    of its stream until an operator replays or skips it.
  - `AVAILABILITY_FIRST` — `streamId` optional; successors are delivered past a `FAILED` head and a
    later replay arrives after them — for telemetry and order-tolerant, idempotent consumers.
- Order is per stream `(aggregate_type, aggregate_id)`: two aggregate types sharing an id value have
  independent heads. `seq` is commit order because every append holds the global-offset counter lock
  through commit. `loadPending` claims at most one entry per stream at a time — always its
  lowest-`seq` eligible entry — with `FOR UPDATE SKIP LOCKED` under a claim lease (default 4 minutes);
  the partial unique index `ux_outbox_one_inflight_per_stream` makes "at most one `IN_PROGRESS` per
  stream" a database invariant. Entries without a stream are claimed freely in parallel.
- `OutboxPoller` (virtual thread; `batchSize` 100, `pollInterval` 1 s, `RetryPolicy` 10 attempts,
  backoff capped at 60 s): every `mark*` write is a compare-and-set on the claiming identity; a lost
  lease is a logged no-op. Delivery outcomes are classified per publisher by hand-off phase: a
  **transport** failure (pre-hand-off) is retried forever and never counts toward `FAILED`; an
  **in-flight** failure (post-hand-off) holds the claim until the lease expires; a **per-entry**
  rejection burns an attempt and reaches terminal `FAILED` after `maxAttempts`. The per-cycle publish
  deadline is `min(80 % of the lease, lease − publisher.inFlightHorizon())`. `build()` refuses a lease
  that does not strictly exceed the publisher's in-flight horizon, a store whose `orderingMode()` or
  `claimLease()` returns `null` (a non-positive lease is the explicit opt-out), and a non-positive
  batch size or poll interval.
- Stopping the relay mid-batch records the deliveries the broker confirmed, keeps an entry whose
  publish the stop cut short claimed until its lease expires, and returns the untouched entries to
  `PENDING` at once.
- Status model: `PENDING`, `IN_PROGRESS`, `DELIVERED`, `FAILED` (unresolved until an operator acts),
  `SKIPPED` (terminal, audited with `skipped_at`, `skipped_by`, `skip_reason`). `OutboxStore.delete`
  refuses a `FAILED` row; `findByStatus(status, limit)` is the read-only listing.
- `OutboxFailedReplayer`: `replay(id)` / `replayFailed(max)` reset `FAILED → PENDING` at the original
  `seq`; `skip(id, operator, reason)` / `skipFailed(max, operator, reason)` mark `SKIPPED` and release
  the stream. A skip is not a delivery.
- Publishers: `HttpOutboxPublisher` (JSON `POST`, `X-Outbox-Entry-Id` and `X-Outbox-Payload-Type`
  headers), `KafkaOutboxPublisher` (synchronous `send().get(sendTimeout)`, caller-owned producer,
  record key = the stream's `<type>:<id>` text form), `RabbitMqOutboxPublisher` (publisher confirms,
  caller-owned channel; `build()` rejects a channel that does not enter confirm mode, a channel that
  left confirm mode is a transport failure, and an entry id or payload type longer than an AMQP short
  string — 255 UTF-8 bytes — is a per-entry failure). A custom `OutboxPublisher` that does not
  override `classifyFailure` treats every failure as transport and should override `inFlightHorizon()`.
- `OutboxRetentionSweeper` (hourly in the integrations) prunes `DELIVERED` rows after
  `streamrune.outbox.retention-max-age` (7 days) and `SKIPPED` rows after
  `streamrune.outbox.skipped-retention-max-age` (30 days); `FAILED` is never pruned. Integration
  properties: `streamrune.outbox.enabled` (default `false`), `batch-size` (100), `flush-interval-ms`
  (5000), `retry-max-attempts` (10), `retry-initial-delay-ms` (1000), `retry-multiplier` (2.0). On
  Micronaut, startup fails when an `OutboxEventMapper` is paired with a store the event store cannot
  write in the append transaction, or when `streamrune.outbox.enabled=true` has no `OutboxStore` or no
  `OutboxPublisher`.

### GDPR — encryption at rest and crypto-shredding

- `@Encrypted(subjectId = "…")` on a `String` record component encrypts that field under the named
  subject's key; `CryptoShreddingModule` (Jackson) encrypts on serialize and decrypts on deserialize,
  and a field whose key is gone reads as `CryptoShreddingModule.REDACTED` (`"[REDACTED]"`) so replays
  and rebuilds keep working. The record's other components read back exactly as written (`BigDecimal`
  scale, `Instant`/`Duration`/`OffsetDateTime` nanoseconds) whatever the mapper's float configuration.
  Startup refuses any registered type with an `@Encrypted` component when no `CryptoEngine` is
  configured.
- The module fails closed, with `CryptoOperationException`, on every Jackson shape that would bypass
  the encrypting writer or lose the subject id (non-`String` components, `@JsonFormat(shape = ARRAY)`,
  class-level `@JsonValue`/`@JsonSerialize`/`@JsonDeserialize`, the record as a `Map` key,
  `@JsonIgnore` on the subject-id component, filters and views that hide either component).
  `@JsonProperty` renames and `@JsonUnwrapped` prefixes/suffixes are supported (the latter needs
  jackson-databind 2.19+).
- `CryptoEngine` SPI: `encrypt`, `decrypt`, `deleteKey` (terminal — GDPR Article 17), `reinstate`,
  `isKeyAvailable`, `requiredCryptoTables`. After `deleteKey`, `encrypt` throws
  `SubjectForgottenException` and `decrypt` of old ciphertext reads `[REDACTED]` on every shipped
  backend. `CryptoMappingException` marks a deterministic verdict on a blob (malformed, tampered,
  unsupported version) that read paths quarantine instead of retry; a bare `CryptoOperationException`
  is an outage and is retried.
- Four backends, each via `builder()`:
  - **postgres** — `PostgresCryptoEngine`: per-subject AES-256-GCM keys in `encryption_keys`,
    tombstones in `forgotten_subjects`, destroyed key generations in `erased_key_generations`;
    `deleteKey` deletes the key, records its generation and writes the tombstone in one transaction.
  - **filesystem** — `FileSystemCryptoEngine`: keys under `keyDirectory(Path)`; every mint, erasure
    step and reinstate is fsynced in order, and a key file whose forces failed is forced again before
    the next encrypt uses it.
  - **vault** — `VaultCryptoEngine`: HashiCorp Vault transit (`subject-<SHA-256>` key names, token
    auth, retries with backoff on 429/5xx); `deleteKey` records a durable tombstone, sets
    `deletion_allowed=true` on the transit key and deletes it (the token needs `update` on
    `<mount>/keys/+/config` and `delete` on `<mount>/keys/+`).
  - **aws-kms** — `AwsKmsCryptoEngine`: one customer managed key encrypts every subject with a
    per-subject encryption context; erasure is a durable per-subject tombstone.
- Vault and AWS KMS `build()` fail closed without a `ForgottenSubjectStore`; `JdbcForgottenSubjectStore`
  is the durable one, auto-wired by the integrations whenever a `DataSource` is present.
- postgres and filesystem keep a key-generation byte in every ciphertext header, so a subject that is
  erased, reinstated and re-keyed still reads pre-erasure ciphertext as `[REDACTED]`.
- `CachedCryptoEngine` caches decrypts with a mandatory bounded TTL (`streamrune.crypto.cache.expire-after-write`,
  default 5 minutes) and a `CryptoForgetSignal` (`PostgresCryptoForgetSignal`, LISTEN/NOTIFY) so a
  `deleteKey` on one replica evicts its peers — on Quarkus through a one-connection Agroal pool.
  `streamrune.crypto.cache.enabled=false` skips the cache.
- `ForgetSubjectService.forget(subjectId, requestedBy)` deletes the key first, then runs every
  registered `SubjectDataPurger` (a failing purger is audited and counted on
  `streamrune.gdpr.purge_failed`, the rest still run), then evicts the query cache (a failed
  eviction is audited and counted the same way), and returns a `ForgetResult` with `fullyErased()`,
  `failedPurgers()` and `queryCacheEvicted()`. An interrupted erasure is completed by
  calling `forget` again. `ForgetSubjectService.reinstate(subjectId, requestedBy)` lifts a tombstone
  and records it as `GDPR_REINSTATE`. `ExportSubjectDataService.export(subjectId, requestedBy)`
  (Article 15) collects one JSON section per `SubjectDataCollector` into a `SubjectExport`. Both
  builders read each collector's or purger's `name()` once and refuse two under one name; a `name()`
  that throws or returns nothing is replaced by the class name with a WARN.
- Crypto-shredding covers events, snapshots, saga state and the `@Encrypted` components of dead-lettered
  commands. Saga and projection dead-letter stores hold metadata only and point at `event_stream`.
- `streamrune.crypto.subject_redacted` and `streamrune.crypto.keystore_systemic_failure` distinguish
  lawful per-subject redaction from a wiped or misconfigured key store.

### Security and identity

- `RequestContext.userId` — the identity every authorization check and the audit trust — is derived by
  one rule, `RequestIdentityPolicy`, in all three HTTP integrations, and it fails closed:
  - **Trusted gateway** with `streamrune.security.trust-user-id-header=true`: `X-User-Id` is the
    identity (even when a resolver is present) and `X-User-Role` becomes baggage `role`; a header
    sent more than once resolves to anonymous.
  - **Authenticated principal** when an `AuthenticatedUserResolver` is available (Spring Security,
    Quarkus Security, Micronaut Security, or your own bean) and the flag is off: `X-User-Id` is
    ignored and a disagreeing header is logged.
  - **Anonymous** otherwise: every request runs without a user and the headers are ignored.
  The mode is logged once at startup; startup is refused when command authorization is configured and
  the mode is anonymous.
- `CommandAuthorizationPolicy.authorize(userId, command)` via `AuthorizationCommandInterceptor`;
  `@RequireRole` / `@RequirePermission` on a command type or any supertype via
  `AnnotationAuthorizationInterceptor` and a `UserRoleResolver`. A `VirtualThreadCommandBus` refuses to
  build when a registered command declares `@RequireRole`/`@RequirePermission` and no
  `AnnotationAuthorizationInterceptor` is in its own chain; the Spring, Quarkus and Micronaut startup
  checks read the chain of every command bus bean, also one the application builds itself.
- Off-thread authorization: request-scoped resolvers are evaluated once at the request edge and the
  result travels in `RequestContext.authority()`, so `executeAsync` authorizes like synchronous
  execution; a dead-letter replay runs under a recorded system principal. The captured authority is
  process-local and never persisted.
- Server-Sent Events `GET /api/sse/{aggregateType}/{aggregateId}` (all three integrations) is off by
  default (`streamrune.sse.enabled`); when on, every stream is denied until an `SseAuthorizer` bean
  checks the caller against the requested `StreamId`.
- Only allow-listed OpenTelemetry baggage keys (`streamrune.metadata.baggage-allowlist`) reach event
  metadata. `LogSanitizer` strips control characters from every value the framework logs or persists
  as free text.

### Audit

- `AuditCommandInterceptor` writes an `AuditEntry` (`commandId`, `commandType`, `aggregateType`,
  `aggregateId`, `userId`, `occurredAt`, `outcome`, `errorMessage`, `eventCount`, `correlationId`) for
  every command — `SUCCESS`, `FAILURE` or `VETOED` — to an append-only `AuditStore`
  (`PostgresAuditStore`, `audit_log`). Auto-registered in every integration when a `DataSource` is
  present.
- Event audit trail (`streamrune.event-audit-enabled`, default `false`): `EventAuditEntry` per
  persisted event written by `PostgresEventAuditStore` in the append transaction, queried through
  `EventAuditQuery` / `CommandAuditQuery`, and summarised by `ComplianceReportQuery`.
- GDPR actions (forget, reinstate, export) are audited through the same store; `userId` is stored in
  plaintext by design so the trail survives the subject's erasure.

### Framework integrations

Spring Boot 4, Quarkus and Micronaut each auto-configure the same picture from a `DataSource` and a
set of `streamrune.*` properties with the same names on all three (`Duration`s accept `30s`/`7d` on
Spring and Micronaut and ISO-8601 `PT30S`/`PT168H` on Quarkus):

- The event store through `PostgresEventStoreFactory`, a `VirtualThreadCommandBus` with
  `PostgresCommandInbox`, `PostgresAuditStore`, `PostgresDeadLetterQueue` + `DeadLetterRetryRunner`,
  `LeaseBasedLeadership` + `JdbcProjectionRepository`, saga runners with `PostgresSagaStore` /
  `PostgresSagaDeadLetterStore` and a `SagaCompensationRetrySweeper` per saga runner, the retention
  sweepers, the outbox poller when `streamrune.outbox.enabled=true`, health, Micrometer metrics
  (`streamrune.metrics.enabled`, `streamrune.metrics.prefix`), OpenTelemetry command spans when an
  `OpenTelemetry` bean exists, the query cache, SSE, and the request filter that binds
  `StreamRuneContext` from `X-Trace-Id`, `X-Correlation-Id`, the resolved identity and allow-listed
  baggage.
- Deciders are registered as `DeciderRegistration` beans carrying their aggregate type;
  `@ProjectionConfig` beans are discovered into a `MultiProjectionRunner` (`CONTINUOUS`), a
  `ScheduledProjectionRunner` (`SCHEDULED`) and the `InlineProjectionInterceptor` (`INLINE`) while
  `streamrune.projections.auto-discovery.enabled` is `true` (the default). A runner, sweeper or retry
  runner bean you define replaces the framework's, and you own its lifecycle.
- A runner's processor is chosen by declared need, never by the presence of a `DataSource` or a bean:
  a runner gets the single `AtomicBatchProcessor` bean when one of its registrations declares a
  transactional mode or when leadership must fence, fails to start when it needs one and none (or
  more than one) exists, and otherwise runs on `nonAtomicAtLeastOnce()`.
- Crypto engines bind through `streamrune.crypto.<filesystem|postgres|vault|aws>.enabled` and their
  keys; the Vault and AWS KMS engines get a `JdbcForgottenSubjectStore` on the application `DataSource`.
- Startup validators, all fail-closed: `StreamRuneConfigValidator` (durations), `CommandInboxWiringValidator`,
  `SagaRetentionValidator`, `SagaStateCryptoValidator`, `StreamRuneAuthorizationValidator`,
  `StreamRuneRequestIdentityValidator`, the `@Encrypted`-without-engine check. A startup banner
  (`streamrune.startup-log`) reports the active configuration.
- Health: Spring Boot Actuator `streamRune` (`StreamRuneHealthIndicator`), Quarkus `@Readiness`
  `streamrune` (`StreamRuneHealthCheck`, always listed in `/q/health/ready`), Micronaut `streamrune`
  (`StreamRuneHealthIndicator`): `DOWN` when the database refuses a connection, when any subscription
  is `DOWN`, or when a started background relay's thread died; `subscription.<name>` and
  `relay.<component>` details.
- **Spring Boot**: `StreamRuneAutoConfiguration`, `StreamRuneProperties` (immutable records bound
  through their constructors), `@StreamRuneComponent` stereotype, `StreamRuneFailureAnalyzer`,
  `SmartLifecycle` runners, `streamrune.validation-enabled` (Jakarta Validation interceptor, default
  `true`), the starter. The SSE endpoint completes every open stream on shutdown, before the web
  server's graceful-shutdown drain. `StreamRuneRuntimeHints` exposes `registerDomainPackages(...)` /
  `registerSerializableTypes(...)` for native-image reflection.
- **Quarkus**: `StreamRuneProducers` (CDI), `ProjectionProducer`, `StreamRuneLifecycle`,
  `@ConfigMapping` properties, JAX-RS request filter, `StreamRuneReflectionConfig`, HikariCP
  reachability metadata. Application `CommandInterceptor` beans join the framework's chain; an
  application bean of a framework interceptor's type replaces that interceptor in its slot (also when
  it is normal-scoped); every interceptor survives build-time removal of unused beans. Crypto backend
  and SSE/query-cache selection are build-time properties (`@IfBuildProperty`).
- **Micronaut**: `StreamRuneMicronautModule`, `ProjectionFactory`, `StreamRuneLifecycle`,
  `StreamRuneContextFilter` with `StreamRuneContextHelper`, `BeanValidationInterceptor` with Micronaut's
  compile-time validator (`@Introspected` commands), `configuration-metadata.json`, `reflect-config.json`.
  The outbox store may be declared as `OutboxStore` or `PostgresOutboxStore`.
- **GraalVM native image** (measured on GraalVM CE 25.0.1): each integration ships reachability
  metadata for the framework's own types. The application registers its own events, state records and
  nested value objects, and lists every sealed command/event root and nested sealed level as a
  `{"type": "…"}` entry in its own `reachability-metadata.json` (on Spring, `registerDomainPackages`
  covers it). A sealed type the image reports with no permitted subclasses is refused at startup.
  Micronaut images need `-H:+SharedArenaSupport`. Every jar is compiled with `-parameters`.

### Observability

- Micrometer metrics through `MicrometerStreamRuneMetrics` (prefix `streamrune` by default), every meter
  named from `org.streamrune.core.metrics.MetricNames`, one label set per metric name (`unknown` where a
  call site has no value):
  - commands: `commands.dispatched`, `commands.succeeded`, `commands.failed`, `commands.duration`,
    `commands.retried`, `commands.short.circuited`, `commands.async.rejected`, `commands.in_flight`,
    `locks.wait`
  - events and snapshots: `events.appended`, `events.replayed`, `events.duration`,
    `snapshots.created`, `snapshots.loaded`, `snapshots.discarded{reason}`
  - command DLQ and inbox: `dlq.published`, `dlq.exhausted`, `dlq.pending`, `dlq.swept_rows`,
    `dlq.retry.consecutive_failures`, `inbox.replay_hits`, `inbox.swept_rows`
  - projections and subscriptions: `projections.processed`, `projections.failed`,
    `projections.dead_lettered`, `projections.dead_letter_backlog`, `projections.duration`,
    `projections.delivery_mode`, `subscriptions.events.received`, `subscriptions.delivery.latency`,
    `subscriptions.lag`, `subscriptions.listener.reconnects`
  - queries: `queries.dispatched`, `queries.duration`, `queries.cache.hits`, `queries.cache.misses`
  - sagas (tagged `saga.type`): `saga.quarantined`, `saga.faulted`, `saga.skipped_while_faulted`,
    `saga.event_held{hold.reason}`, `saga.replay_deferred`, `saga.compensation{outcome}`,
    `saga.compensation_retries`, `saga.cas_conflicts`, `saga.compensating`, `saga.timed_out`,
    `saga.faulted_backlog`, `saga.faulted_rows`, `saga.replayed{outcome}`,
    `saga.forced_stale_resume`, `saga.resume_faulted{outcome}`, `saga.compensate_faulted`,
    `saga.dead_letters_swept`
  - outbox: `outbox.pending`, `outbox.in_flight`, `outbox.in_flight_horizon_violation`,
    `outbox.delivery_failed`, `outbox.blocked_aggregates`, `outbox.blockage_age_seconds`,
    `outbox.skipped`, `outbox.skipped_swept`, `outbox.replayed`, `outbox.swept_rows`,
    `outbox.relay.consecutive_failures`
  - crypto and GDPR: `crypto.subject_redacted`, `crypto.keystore_systemic_failure`,
    `gdpr.purge_failed{purger}`
- OpenTelemetry spans, names and attribute keys defined in `org.streamrune.runtime.TraceAttributes`:
  `command/{CommandType}` (`OpenTelemetryCommandInterceptor`), `subscription/{name}/deliver`
  (`TracingEventListener`), `projection/{name}/process` (`TracingProjectionDecorator`); attributes
  are `streamrune.`-prefixed. Trace and span ids propagate into `EventMetadata`.
- Every production log line goes through SLF4J; bring the binding of your choice.
- `BackgroundRelayHealthContributor` for every background relay; a per-cycle aggregated WARN for
  transport and in-flight outbox outages.

### Operations

- Never add `classpath:db/streamrune-migration` or `classpath:db/crypto-migration` to your own
  `flyway.locations`; apply them out of band with `baseline-on-migrate=true`, `baseline-version=0`
  when the runtime role has DML-only grants (`streamrune.event-store.schema.auto-initialize=false`).
- Least privilege: `SELECT`, `INSERT`, `UPDATE`, `DELETE` on StreamRune tables plus `USAGE` on their
  sequences; `DELETE` is required by the retention sweepers, the discard/purge paths and crypto key
  deletion. Revoke `DELETE` only on `event_stream` and `audit_log` if immutability must be enforced by
  the database.
- Duration properties: a window or interval that can be switched off treats zero and negative as
  "off" (retention windows); a wait treats zero as "do not wait" and rejects negative
  (`streamrune.lock-timeout`); a period with no meaningful "off" rejects both (lease TTL, sweep
  intervals). A positive value below one millisecond is rejected wherever zero is a sentinel.
- Connection-pooling proxies: leadership, fencing and correctness hold under any PgBouncer mode;
  LISTEN/NOTIFY needs a session-stable connection — under transaction pooling use
  `SubscriptionConfig.pollingOnly(...)`, allow server-side prepared statements (PgBouncer 1.21+ with
  `max_prepared_statements` > 0, or `prepareThreshold=0`), set session parameters with `ALTER ROLE` /
  `ALTER DATABASE` instead of a pool's init SQL, and run schema migration over a direct connection.
- Runbooks in `docs/guide/production.md`: faulted sagas, incomplete GDPR erasures, projection dead
  letters, outbox `FAILED` entries, multi-replica deployment and write capacity, offset reset /
  rebuild, switching a projection's delivery mode, backup, restore and disaster recovery (erased
  subjects must stay erased — re-apply `deleteKey` from an external erasure record before taking
  traffic), a security checklist.

### Testing support — `streamrune-test`

- `InMemoryEventStore`: thread-safe, mirrors `PostgresEventStore`'s observable semantics — strict
  optimistic locking, serialized global ordering, snapshots with schema versions (`withMigrations(...)`),
  upcasting on read (`withUpcasters(...)`); `InMemoryEventStore.serializing(typeRegistry, cryptoEngine)`
  stores JSON so `@Encrypted` fields, crypto-shredding and type registration are exercised.
  `InMemoryEventStoreFactory` for DI.
- Fixtures: `DeciderFixture` (`given(...)` / `when(...)` / `expectEvents`, `expectNoEvents`,
  `expectException`, `expectFailedWith`, `expectState`, `expectStateMatches`, `expectProduced`),
  `EventStoreFixture`, `ProjectionFixture`, `MutableClock`.
- In-memory doubles: `InMemoryCommandBus` (registers with an aggregate type, runs `guard` before
  `decide`), `InMemoryQueryBus`, `InMemoryCommandInbox`, `InMemoryDeadLetterQueue`,
  `InMemoryCryptoEngine` (tests only), `InMemoryEventSubscription`, `InMemoryOutboxStore`,
  `InMemoryOffsetStore`, `InMemoryProjectionRepository` (a fencing-capable processor and its own offset
  store, for `TRANSACTIONAL_LOCAL` unit tests without Docker), `InMemoryProjectionDeadLetterStore`,
  `InMemorySagaStore`, `InMemorySagaDeadLetterStore`.
- Contract suites a custom store can extend: `SagaStoreContract`, `SagaDeadLetterStoreContract`,
  `OutboxStoreContract`, `AtomicBatchProcessorContract`.
- Every PostgreSQL-backed suite of the framework runs on PostgreSQL 18 in the default build and on 17
  in a second CI job (Testcontainers).

### Requirements

- **Java 25** (virtual threads and `ScopedValue` are used throughout; every jar is built with the Java
  25 toolchain).
- **PostgreSQL 17 or newer** for `streamrune-postgres` and `streamrune-postgres-crypto` — tested on 17
  and 18, and enforced at startup. The in-memory stores of `streamrune-test` need no database.
- **Jackson 2.19.2** (`jackson-databind` 2.19 or newer). `streamrune-core` depends on nothing else.
- `streamrune-postgres` brings Flyway 13.9.0 (`flyway-core`, `flyway-database-postgresql`), HikariCP
  6.2.1 (for its dedicated LISTEN and lock pools), the PostgreSQL JDBC driver 42.7.10 and SLF4J 2.0.
- Frameworks: Spring Boot 4.0.x (built and tested against 4.0.3, Spring Security 7.0.x), Quarkus
  3.32.x, Micronaut 4.10.x (Micronaut Security 4.14, Validation 4.12, Serde 2.16). Micrometer 1.16,
  OpenTelemetry API 1.47 (optional).
- Outbox brokers: Kafka clients 3.9 (`streamrune-kafka-outbox`), RabbitMQ `amqp-client` 5.25
  (`streamrune-rabbitmq-outbox`). Crypto backends: AWS SDK v2 KMS 2.30 (`streamrune-aws-kms-crypto`);
  HashiCorp Vault with the transit engine enabled, over its HTTP API (`streamrune-vault-crypto`).
  `streamrune-crypto-api` brings Caffeine 3.2.
- GraalVM native image: GraalVM CE 25.0.1.
- Docker is needed only to run the framework's own PostgreSQL-backed test suites (Testcontainers).

### Guarantees

Each statement below is a property of the shipped code and holds under the stated conditions.

1. **Per-stream optimistic concurrency.** `append(streamId, events, expectedVersion)` succeeds only
   when `expectedVersion` is the stream's current version; otherwise it throws
   `OptimisticLockException` and appends nothing. The database enforces it with a unique constraint on
   `(aggregate_type, aggregate_id, version)`.
2. **Gap-free, commit-ordered global offsets.** Within one event store, a committed `GlobalOffset` N
   implies every offset below N is committed; a rolled-back append releases its reservation.
3. **Streams of different aggregate types never mix.** Two aggregate types that use the same id value
   have separate streams, versions, locks, snapshots, idempotency bindings and outbox heads.
4. **Events are immutable.** The framework never updates or deletes a row of `event_stream`;
   corrections are new events. Crypto-shredding changes what a field decrypts to, not the stored bytes.
5. **A keyed command executes at most once per key.** The idempotency key is committed in the same
   transaction as the events it produced; every later execution under the key, including one that
   overlaps the original, returns the recorded result with `idempotentReplay() == true` and runs no
   handler. A key presented with another command type or another stream is refused. Holds for the
   retention window of `streamrune.inbox.retention-max-age`.
6. **Business rejections are terminal, never retried, never dead-lettered.** `DomainException` (with
   `ValidationException` and `AuthorizationException`), `IllegalArgumentException` and any failure
   with a `SubjectForgottenException` in its cause chain propagate to the caller, reach no
   `DeadLetterQueue` and do not count toward the circuit breaker.
7. **Outbox atomicity and order.** Entries produced by `OutboxEventMapper` for an append commit or
   roll back with that append's events. On a `STRICT_PER_AGGREGATE` channel, entries of one stream
   written by `append` are delivered in `seq` (commit) order, at most one in flight per stream — a
   database-level invariant — and a terminal `FAILED` entry blocks its stream until an operator
   replays or skips it. The framework never deletes a `FAILED` row. Delivery is at-least-once;
   receivers deduplicate on the entry id.
8. **A transport outage never terminal-fails the backlog.** Only a per-entry rejection
   (`OutboxPublisher.classifyFailure` → `ENTRY`) burns a retry attempt; an unclassified failure
   defaults to transport.
9. **Single-active-consumer with fencing.** Under `LeaseBasedLeadership` at most one replica holds a
   name's lease at a time; a standby takes over at most one lease TTL after the leader stops renewing;
   with a fencing `AtomicBatchProcessor` (`JdbcProjectionRepository`) a checkpoint commit from a
   superseded epoch is rejected by the database before any read-model row is written. A configuration
   that pairs real leadership with a processor that cannot fence does not start.
10. **A transactional projection's read-model write and its checkpoint commit together.** A
    `TRANSACTIONAL_LOCAL` or `EXTERNAL_EFFECT` registration that passes the startup check has its
    batch's writes through the handed repository and its checkpoint in one transaction; a crash before
    the commit rolls both back and the batch is redelivered; an overlapping, non-advancing or
    superseded-epoch batch is rejected before any write. `AT_LEAST_ONCE_IDEMPOTENT` is at-least-once.
    No mode makes an external effect inside `process` exactly-once.
11. **Saga dispatch is effectively-once per episode.** Every saga-issued command carries a
    deterministic idempotency key; redeliveries, crash-resumes and timeout or sweeper re-drives of the
    same episode dedup in the command inbox. Compensation is claimed (`COMPENSATING`, compare-and-swap)
    before any undo command is dispatched. Holds while the episode's keys are within the inbox
    retention window; the key-age guards refuse or fault an older episode instead of re-executing it.
12. **Poison never blocks the stream and never loses an event.** An orchestrator exception quarantines
    the triggering event's metadata, faults the saga and lets the batch continue; a held or quarantined
    event is kept until it is replayed or an operator discards it, and retention never prunes a
    recovery handle.
13. **Erasure is terminal on every backend.** After `deleteKey(subjectId)` returns, `encrypt` for that
    subject throws `SubjectForgottenException` and pre-erasure ciphertext reads as `[REDACTED]`;
    `reinstate` is the only, per-subject, way back.
14. **No `@Encrypted` field is persisted in plaintext by the framework's own serializers.** A
    registered `@Encrypted` type without a `CryptoEngine` fails startup; every Jackson shape that would
    bypass the encrypting writer fails closed at write time (see Known limitations for the one
    unguarded case).
15. **Request identity fails closed.** A client-supplied `X-User-Id` is an identity only with
    `streamrune.security.trust-user-id-header=true`; command authorization configured in anonymous
    mode is refused at startup; a role-annotated command on a bus without the annotation interceptor
    is refused when the bus is built.
16. **The schema is validated before traffic.** With validation on, a missing table or column, a
    missing or behind counter row or a schema-history version behind this build stops startup; an
    unsupported PostgreSQL version stops startup before any migration runs.
17. **Dead-letter replays never re-execute redacted commands.** A command dead-letter entry whose
    `@Encrypted` fields read `[REDACTED]`, or that cannot be checked, records a failed attempt and is
    never dispatched; a metadata-only entry is never dispatched.

### Known limitations

- **Preview status.** `1.0.0-alpha-SNAPSHOT` is unsigned, published only to the snapshot repository,
  and may change API, schema and configuration in any later snapshot with no upgrade path.
- **One global write lane per event store.** Every append reserves its offsets from a single counter
  row and holds that lock until commit, so appends to unrelated streams serialize and the global
  append rate does not grow with application instances. No measured capacity envelope is published.
  Scale by partitioning into separate event stores; there is no global order across partitions.
- **Single writable primary.** No multi-master; use primary/standby failover. No backup/restore drill
  against a production deployment has been performed and no RPO/RTO figures are published.
- **No embedded database mode.** `streamrune-postgres` needs a running PostgreSQL 17+;
  `InMemoryEventStore` is for tests, in-process only, not durable and not a throughput proxy.
- **LISTEN/NOTIFY needs a session-stable connection** and `NOTIFY` is best-effort; under PgBouncer
  transaction or statement pooling use `pollingOnly`.
- **Statement bounds.** Only the event store bounds its own statements, and only the first page (500
  events) of a long stream's load; the projection repository and the other stores run under the
  session's `statement_timeout`.
- **Call StreamRune outside JTA / `@Transactional`.** The stores commit their own writes on the
  connection they borrow; inside a JTA transaction (for example Quarkus with Agroal) that commit is
  refused. Event appends never join an application transaction; derive read models from events.
- **`LocalStripedLocker` is single-JVM.** Multi-instance deployments use `PgAdvisoryLocker`, which
  holds a lock connection for the whole command.
- **No cross-aggregate transaction.** `append` writes one stream; cross-aggregate workflows go through
  sagas.
- **`INLINE` projections have no checkpoint, no dead letters and no replay** — a failing `process` is
  logged and the command still succeeds.
- **Write-through is declared, not fully verified.** For a projection that exposes no
  `writeTarget()`, `writesThroughRepository()` answers by reflection whether the two-argument
  `process` is overridden, not whether the override writes through the handed repository. In a native
  image such a projection class must be registered for reflection.
- **The circuit breaker is per-JVM, in-memory state**; each replica opens and closes independently.
- **`SnapshotPolicy` is global per `VirtualThreadCommandBus`.**
- **Subscriptions are at-least-once across `pause()`/`resume()`**; listeners must be idempotent.
- **Projection dead-letter replay is an out-of-order patch** behind the checkpoint and requires an
  idempotent `process`.
- **The plain `StreamRune` facade has no health surface**: a projection thread started by
  `startProjections()` that dies is reported only as an ERROR log.
- **Outbox**: one channel per application in the shipped integrations, hence one ordering mode;
  `OutboxStore.save(entry)` outside `append` is outside the commit-order promise; replaying on an
  `AVAILABILITY_FIRST` channel reorders within a stream; a post-hand-off failure parks an entry for a
  full claim lease; a hot stream's delivery rate is one in flight; a change to a `payload` schema or
  `payloadType` breaks entries still pending under the old shape — drain the outbox first.
- **Sagas**: `SagaRunner` is designed for one subscription per saga type; write undo commands as
  idempotent cancel/void operations; a saga exposed to veto-style interceptors must configure
  `timeout()`; a drain assumes it is the only recovery actor on its saga; within one timeout
  population, many failing rows can delay the rest of a batch; the dead-letter `errorMessage` column
  is outside crypto-shredding — keep personal data out of orchestrator exception messages.
- **Crypto**: `@Encrypted` applies to `String` record components only; no key rotation; postgres and
  filesystem store key bytes unwrapped (no envelope encryption); the filesystem backend has no
  replication and shares a key directory safely only within one kernel's view of the filesystem;
  Vault supports token auth only and refuses `reinstate` after a completed erasure; on AWS KMS
  `reinstate` makes pre-erasure ciphertext readable again; `[REDACTED]` is an in-band marker — confirm
  erasure with `isKeyAvailable`; property-level `@JsonSerialize`/`@JsonDeserialize` on a field of an
  `@Encrypted`-carrying record type is unguarded and leaks plaintext on write; a restore brings
  erased keys back — re-apply the erasures; cross-replica cache eviction depends on the forget
  signal's LISTEN connection, bounded by the cache TTL.
- **GDPR**: `forget` does not touch `snapshot_store` unless `PostgresSnapshotStorePurger` is
  registered; a dead-lettered command's personal data is protected only by its own `@Encrypted`
  components (otherwise plaintext until the dead-letter retention sweeper deletes the row); the query
  cache eviction reaches this process only; `ForgetSubjectService` and `ExportSubjectDataService`
  record the requester but do not authorize it — authorize the caller first; nothing re-drives an
  interrupted erasure on its own — call `forget` again.
- **Audit**: an `AuditStore.save()` failure does not fail the command — it is logged at ERROR and that
  audit row is lost; `userId` is plaintext; the raw idempotency key is the primary key of
  `command_inbox` and is stored on a dead-letter row.
- **Query cache**: TTL and event-driven invalidation are caching controls, not security controls;
  eviction is per query type; without a `QueryAuthorizer` a cache hit skips the handler's
  authorization.
- **Metrics**: `stream.id` is never a metric tag; `subscriptions.listener.reconnects` cannot signal a
  dead push path — use the health detail.
- **Integrations**: `@StreamRuneComponent` needs component scan; the `StreamRune` facade is a
  programmatic bootstrap, not an injectable bean (inject `CommandBus`); SSE is basic — a slow
  subscriber is disconnected; Quarkus needs Quarkus REST for the request filter and SSE and selects
  crypto backends, SSE and the query cache at build time; a permanent takeover stamp failure keeps a
  leader in a WARN-and-retry standby loop rather than turning health `DOWN`.
- **Native image**: sealed hierarchies must be registered at the sealed type in
  `reachability-metadata.json`; Micronaut needs `-H:+SharedArenaSupport`.
- **JPMS**: `streamrune-core`, `streamrune-test`, `streamrune-crypto-api` and
  `streamrune-aws-kms-crypto` are explicit modules; the other twelve are automatic modules with
  declared names.

### License

StreamRune is dual-licensed.

- **Business Source License 1.1** (`LICENSE`; SPDX `BUSL-1.1`). Licensor: Martin Bednář. You may copy,
  modify, create derivative works, redistribute and make non-production use of the Licensed Work. The
  **Additional Use Grant** permits use by any organization whose total annual gross revenue, combined
  with that of its affiliates, does not exceed **USD 5,000,000**. **Change Date:** four years from the
  date the Licensed Work is published under the License, after which that version is governed by the
  **Change License, Apache License, Version 2.0**. The License applies separately to each version.
  BSL 1.1 is source-available, not an OSI open source license, until the Change Date.
- **Commercial license** (`LICENSE-COMMERCIAL.md`) for organizations above the revenue threshold:
  production use without the Additional Use Grant's restriction, negotiated per customer. Contact
  **licensing@streamrune.com** with your organization, intended use case, expected scale and a contact
  person.

Every published jar, sources jar and Javadoc jar bundles `LICENSE`, `NOTICE` and
`LICENSE-COMMERCIAL.md` under `META-INF`, and the POM's single `<license>` entry names `Business
Source License 1.1`. Contributions are accepted under BSL 1.1 with DCO sign-off (`CONTRIBUTING.md`).
