# Changelog

All notable changes to StreamRune are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The unsigned preview of the next release is a `-SNAPSHOT` version published from `main` (the
[Preview builds](README.md#preview-builds) section of the README on `main` names it): it is rebuilt
each time the Build workflow passes there and goes only to the Maven Central snapshot repository
(`https://central.sonatype.com/repository/maven-snapshots/`), never to Maven Central itself. What it
adds since the last release can change in any later snapshot without a migration path. From `1.0.0`
on, a migration a release has shipped never changes, and a schema change is a new versioned
migration; a migration that no release has shipped yet can still change between snapshots, so a
database created by an earlier snapshot can fail Flyway's checksum validation: recreate it (with
Docker Compose, `docker compose down -v`) when you move to a newer snapshot.

## [1.0.0] — unreleased

The first release of StreamRune, a CQRS / Event Sourcing framework for Java 25: PostgreSQL-native,
multi-framework (Spring Boot, Quarkus, Micronaut), GDPR-ready. Explicit, readable code — no
annotation magic, no hidden behavior. Every module shares the `org.streamrune` group and the `1.0.0`
version, is published to Maven Central with signed artifacts and ships with sources and Javadoc
jars. In brief:

- **Explicit wiring.** Every component exists because a builder or a constructor created it, and the
  Spring Boot, Quarkus and Micronaut integrations create theirs with the same builders. An
  annotation is configuration that a named component reads (`@Encrypted`, `@RequireRole`,
  `@ProjectionConfig`, …), and the integrations refuse to start on the misconfigurations their
  startup validators check.
- **GDPR crypto-shredding.** A `String` record component marked `@Encrypted(subjectId = "…")` is
  encrypted under its subject's key, held in PostgreSQL, in a key directory, in HashiCorp Vault's
  transit engine or under an AWS KMS key. `ForgetSubjectService.forget` deletes the key and then
  runs the registered purgers — the aggregate snapshots among them, unless the application declares
  that it keeps none. A field whose key is gone reads `[REDACTED]`, so replays and rebuilds keep
  working. The PostgreSQL, filesystem and AWS KMS engines verify every encryption before they
  return it, so a wrong cipher result is never stored, and retry a decryption that fails its
  authentication tag once. `ExportSubjectDataService` collects a subject's data for an access request.
- **Sagas with subscription leadership.** `SagaRunner` drives a saga from any subscription: every
  command it dispatches is keyed for the command inbox, a failed step opens a compensation episode,
  and the event store's forward fence (on by default) refuses a forward command once that episode
  has opened. A runner built with a `SubscriptionLeadership` (`SagaRunner.Builder.leadership`) has
  one replica per saga type deliver its events, claim its deadlines and retry its compensations, and
  another replica takes over within the lease TTL of a crash.
- **Parallel saga steps.** `SagaRunner` steps the sagas of a page at the same time, 4 by default
  (`SagaRunner.Builder.parallelism`, 1 to 64, `1` for serial), the events of one saga in offset
  order. A failed saga ends its own part of the page, a connection-class failure
  (`SagaStore.connectionFailureClassifier()`) or the third failure stops the page, and the page is
  acknowledged only when every saga ran to its end. Four meters report it
  (`streamrune.saga.steps.in_flight`, `streamrune.saga.page.pending`,
  `streamrune.saga.page.effective_parallelism`, `streamrune.saga.page.failed`). On Spring, Quarkus
  and Micronaut, `streamrune.saga.parallelism` sets it for the runners that name none and
  `streamrune.saga.subscriptions.<name>.parallelism` for one subscription's saga, over the builder;
  an unknown name fails startup. At startup each integration logs one line per saga runner with its
  parallelism and connection budget, `parallelism × (1 + AggregateLocker.applicationPoolConnectionsPerLock())`,
  refuses the start when the aggregate locks can take the whole pool (give the locker a pool of its
  own, raise the pool, or lower the parallelism), and warns above half the pool; a pool whose size
  cannot be read is neither refused nor warned about. The timeout runner built from a runner claims
  the sagas past their deadline in parallel on the same slots as the runner's steps, so steps and
  deadline claims of a saga type together stay within the budgeted steps (the parallelism per bound
  subscription), except the claim of a saga whose own step is in flight, made without a slot when
  none frees within its claim wait, and one slow compensation delays no other claim. The timeout runner, the
  compensation-retry sweeper and the command dead-letter retry runner query their next batch at once
  while their batches come back full and make progress, for at most 10 poll intervals per cycle. See
  the [production guide](docs/guide/production.md#pool-sizing-for-saga-parallelism) and the
  [saga guide](docs/guide/advanced/saga.md#saga-parallelism).
- **Command dead-letter queue with operator-confirmed replay.** A command that keeps failing on
  infrastructure, never one the domain rejected, is parked in the dead-letter queue, and its caller
  receives `CommandDeadLetteredException`, whose `pendingOutcome()` says whether the retry schedule
  replays the entry (`QUEUED_FOR_REPLAY`) or an operator decides (`AWAITING_OPERATOR`); a keyed
  command that fails again under its key stores no second entry, also when two replicas
  dead-letter it at the same moment, and every caller is answered with the one stored entry. The
  retry runner replays on its own only the entries the command inbox can deduplicate; an unkeyed
  entry, or a keyed one older than the inbox retention window, waits for an operator, who replays
  it with `DeadLetterRetryRunner.retry(commandId, true)` or discards it.
- **Outbox ordered per aggregate by default.** Outbox entries commit or roll back with the events
  that produced them. Under `STRICT_PER_AGGREGATE`, the default ordering mode, the entries of a stream
  are delivered in commit order with at most one in flight, and a terminally failed entry holds its
  stream until an operator replays or skips it; `AVAILABILITY_FIRST` is the explicit choice for
  consumers that tolerate reordering. The relay claims the next batch at once while its batches come
  back full or deliver entries of streams with more queued, so a backlog is not held to one batch
  per poll interval, nor a stream to one entry per poll interval. Publishers for Kafka, RabbitMQ
  and HTTP.
- **Explicit projection delivery modes.** Every projection registration declares
  `TRANSACTIONAL_LOCAL`, `AT_LEAST_ONCE_IDEMPOTENT` or `EXTERNAL_EFFECT` — there is no default — and
  the runner checks the declaration against its processor before it reads an event, so at-least-once
  delivery is always a declared choice.
- **Paged, filtered and indexed read models.** `ProjectionRepository.findAllBy` reads one page of
  the rows whose field holds a value, `findAll` one page in a sort order; both order by code-point
  text with the id as tiebreak and take the page and its total from one snapshot, on PostgreSQL and
  in memory alike. A `ProjectionIndex` declares the index such a read needs
  (`ProjectionRepository.declareIndex`, or a `ProjectionIndex` bean in the integrations), and the
  PostgreSQL repository creates it with the table.
- **Stable audit pages.** `CommandAuditQuery` and `EventAuditQuery` (PostgreSQL and SQLite) read a
  page and its total in one statement, so they agree under concurrent writes, and a page beyond the
  last reads only the count. They order by `occurredAt` with the row id as tiebreak, so entries with an equal
  timestamp keep a fixed order and appear once across the pages of a window that receives no entry
  between the reads. `CommandAuditQuery.findByCommandId` returns the later written of two entries with one timestamp.
- **Fail-closed identity.** One rule, `RequestIdentityPolicy`, derives the caller on all three
  integrations: a trusted gateway's `X-User-Id` header when
  `streamrune.security.trust-user-id-header=true`, otherwise the authenticated principal, otherwise
  anonymous. Startup is refused when command authorization is configured and the mode is anonymous.
- **Tracing wherever OpenTelemetry is present.** The command, store, lock, outbox, subscription,
  projection and saga paths emit named spans (`command/PlaceOrder`, `eventstore/append`,
  `lock/acquire`, `outbox/publish`, `saga/{type}/step`, …) through a seam that costs one branch when
  tracing is off. The integrations wire it from the application's `OpenTelemetry` bean or the
  OpenTelemetry Java agent, `streamrune.tracing.*` switches it off as a whole or per area, and the
  subscription, saga and outbox spans link to the span of the command that caused them, which the
  command bus writes into `EventMetadata.spanContext` from its own command span, never from a
  StreamRune header, the request context or baggage; its trace id is the request's, which a
  `traceparent` header chooses. Tracing never fails the work it traces: every component guards its
  tracing, so a sampler, a span processor or a hand-written `StreamRuneTracing` that throws costs
  the span — logged with a rate-limited `WARN`, and counted in `streamrune.tracing.failures`
  wherever the component has metrics (always for what an integration builds) — and never a command, a lock
  or a delivery.

```kotlin
repositories {
    mavenCentral()
}

dependencies {
    implementation("org.streamrune:streamrune-spring-boot-starter:1.0.0") // Spring Boot
    // or pick modules: streamrune-core, streamrune-runtime, streamrune-postgres, streamrune-quarkus, …
    testImplementation("org.streamrune:streamrune-test:1.0.0")
}
```

A complete e-commerce example — five aggregates, projections, sagas, crypto-shredding, the outbox and
the dead-letter queues, wired three times (Spring Boot, Quarkus, Micronaut) over one shared domain,
with a 17-chapter tutorial — lives in the separate repository
[streamrune-ecommerce-demo](https://github.com/StreamRune/streamrune-ecommerce-demo) under the Apache
License 2.0.

Every area of `1.0.0` — streams and aggregate types, the event store, snapshots and upcasting,
the command bus and its inbox, aggregates, projections, queries, subscriptions, sagas, the outbox,
GDPR, security and identity, audit, the framework integrations, observability, operations, the
test kit — and the guarantees of the shipped code are described in detail in
[docs/reference/release-1.0.0.md](docs/reference/release-1.0.0.md).
The parts below list its modules, its API stability policy, requirements, known limitations and
license.

Unreleased changes to read-model storage:

- **Read-model stores.** A `ReadModelStore` (name, repository, checkpoints, processor, plus
  `checkpointTarget()`) is one read-model backend, and a projection's checkpoint is stored with its
  read model. `PostgresReadModelStore.create(...)` and `PostgresEventStoreFactory#createReadModelStore()`
  build the PostgreSQL one; several stores, each on its own `DataSource`, can run in one application.
  Each registration is routed to a store (`@ProjectionConfig(readModelStore = "...")`, then its
  `writeTarget()`, then the only transactional store, then the event store's subscription checkpoint
  for an at-least-once projection). `StreamRune.Builder#registerProjection` takes a
  `ReadModelStore` or a `CheckpointTarget`; `ProjectionRunner#run` and `#reset` take a
  `CheckpointTarget`. See the
  [read-model stores guide](docs/guide/read-model-stores.md).
- **Live subscription factory.** `ContinuousProjectionRunner.SubscriptionFactory#create` takes a
  fourth argument, `OffsetStore checkpoints`: the checkpoints of the run (its `CheckpointTarget`'s,
  or the runner's own). A factory-built subscription must read and save through it, never through
  an offset store the factory captured. `SubscriptionHealthContributor#register(name, subscription)`
  keeps a checkpoint source the four-argument `register` gave the name, so a subscription that
  registers itself leaves a routed projection's lag on that projection's own checkpoint.
- **Checkpoint split.** `projection_offset` is renamed `subscription_checkpoint`, and
  `readmodel_checkpoint` (a separate Flyway series, `db/readmodel-migration`, history table
  `flyway_schema_history_readmodel`) holds the checkpoints of projections committed with their read
  model. `PostgresSubscriptionCheckpointProcessor` is the fencing at-least-once processor.
- **Processor capabilities.** `AtomicBatchProcessor#supportsFencing()` is replaced by
  `transactional()` and `fencesCheckpoint()`; every registration under real leadership needs
  `fencesCheckpoint()`.
- **Integrations.** The default store is named `postgres` (bean `streamRuneDefaultReadModelStore`)
  and only a store named `postgres` replaces it; other stores are added. Duplicate store names or
  identities fail startup. The property `streamrune.read-model.schema.auto-initialize` switches the
  read-model migration. Health reports a `store` detail per projection, and lag is computed against
  the projection's own checkpoint store. The one-processor-bean rule
  (`ProjectionProcessorResolution`) and the factory's separate projection-repository method are
  removed.

Unreleased changes to the schema: the event-store table `projection_offset` is renamed
`subscription_checkpoint`, and `streamrune-postgres` ships a separate read-model migration series
(`classpath:db/readmodel-migration`, its own Flyway history) that creates `readmodel_checkpoint`.
The unique key of `saga_dead_letters` is `(saga_id, saga_type, event_offset)`, and
`InMemorySagaDeadLetterStore` keys its entries the same way: two saga types that derive the same
`SagaId` keep one dead-letter entry each at an offset, so a hold or a poison record of one type
never replaces the other type's entry, and `SagaDeadLetterStore.publishShieldedIfAbsent` keeps only
an entry at its full key.

Unreleased changes to how the integrations open their stores (Changed):

- **Storage backend.** The new property `streamrune.backend` (`postgres`, the default, or `sqlite`)
  selects the backend every store is opened on; any other value fails the start.
  With a `DataSource`, Spring, Quarkus and Micronaut open the stores through it once and expose them
  as a `BackendStores` bean (`org.streamrune.core.backend`, beside `StreamRuneBackend`,
  `BackendContext`, `EventStoreOptions`, `OutboxOptions` and `BackendOptionException`). The
  framework's stores and crypto engines are built from the backend, and an application declares
  its optional infrastructure from the bean: `stores.outboxStore(new OutboxOptions(...))`,
  `stores.deadLetterQueue()`, `stores.snapshotPurger(...)`. See the
  [configuration reference](docs/guide/configuration.md#backend).
- **Store beans declared with their interface types.** On all three integrations the framework's
  command inbox (`CommandInbox`), saga dead-letter store (`SagaDeadLetterStore`), event audit store
  and query (`EventAuditStore`, `EventAuditQuery`) and projection dead-letter store
  (`ProjectionDeadLetterStore`) beans are declared with the interface, not the PostgreSQL class. On
  Spring the default read-model store (`ReadModelStore`) and the offset store (`OffsetStore`) are
  too (Quarkus and Micronaut declared them so already), and on Micronaut the event store factory
  `postgresEventStoreFactory` is declared as `EventStoreFactory`, not `PostgresEventStoreFactory`. A
  lookup, an injection point or a bean condition by the PostgreSQL class does not find them. On
  Quarkus the inbox, the saga dead-letter store, the event audit store and query and the projection
  dead-letter store are one instance each, shared by every injection point.
- **`ReadModelStore.DEFAULT`.** The default read-model store is named after the backend
  (`postgres`). `@ProjectionConfig(readModelStore = ReadModelStore.DEFAULT)`, the text `default`,
  routes a projection to it whatever that name is, and no store may be named `default`. The
  constant `ProjectionStoreRouting.DEFAULT_STORE` is removed: routing takes the default store's
  name from the backend.
- **A store the event store cannot write in its append fails the start.** The event store writes
  the outbox, the command inbox and the event audit store in the transaction of its append, so an
  `OutboxStore` paired with an `OutboxEventMapper`, a `CommandInbox` or an `EventAuditStore` bean
  that is not the backend's own type now fails when the framework's event store factory is built,
  with an `IllegalArgumentException` ending in `build it from this backend's BackendStores`. Before,
  such an outbox store disabled emission with a `WARN` on Spring and Quarkus and failed with a
  message of Micronaut's own there, such an inbox failed with the message of
  `CommandInboxWiringValidator`, and such an audit store was silently left out. The three
  integrations now refuse it the same way, and `CommandInboxWiringValidator` is removed from
  `streamrune-integration-api`.
- **The beans the event store takes are found by their interfaces.** The command bus and the event
  store take the same `CommandInbox` bean, so an application's `CommandInbox` bean of the backend's
  type is the event store's inbox too, whatever type its bean method declares. An application's
  `EventAuditStore` bean is wired into the event store whether or not `streamrune.event-audit-enabled`
  is set. On Spring and Quarkus an outbox store bean declared as `OutboxStore` now reaches the event
  store, not only one declared as `PostgresOutboxStore`.
- **Retention sweepers follow the backend's stores.** The outbox retention sweeper runs for any
  `OutboxStore` bean on all three integrations, so an outbox store built with
  `stores.outboxStore(...)` and declared as `OutboxStore` is pruned; on Spring and Quarkus it ran
  only for a bean declared as `PostgresOutboxStore`. On Spring, as on Quarkus and Micronaut, the
  inbox and saga dead-letter sweepers sweep an application bean declared as `PostgresCommandInbox`
  / `PostgresSagaDeadLetterStore`, and otherwise the backend's own store from `BackendStores`
  whenever a `DataSource` exists: an application bean that returns `stores.commandInbox()` keeps
  its sweeper, and an application store of another type is never swept. On Quarkus the WARN that
  no saga dead-letter sweeper exists fires only without a `DataSource`, and names that case.
- **Two unqualified inboxes on Quarkus.** With two unqualified `CommandInbox` beans on
  Quarkus, the event store gets no inbox, as the command bus already did, instead of the
  framework's PostgreSQL inbox.
- **`CryptoBackend` bean.** With a crypto engine enabled, each integration declares a
  `CryptoBackend` bean (`org.streamrune.crypto`) that builds the engine's stores for the selected
  backend. An application bean of that type makes the injection ambiguous.
- **Micronaut eager singletons.** With `eagerInitSingletons`, creating the default read-model store
  bean now opens the `BackendStores` bundle and creates the crypto engine; the store itself is still
  created only when the stores are selected, and opening the bundle touches no table.
- **Outbox warning.** With only one of an `OutboxEventMapper` and an `OutboxStore` bean, the `WARN`
  now asks for "an OutboxStore" (it named "a PostgresOutboxStore"), and on Spring and Quarkus it
  also fires for an outbox store of another type without a mapper, which was silent.
- **Crypto engine resolution.** Every framework store bean takes the `CryptoEngine` through the
  `BackendStores` bean. Several `CryptoEngine` beans with none marked primary now fail once, when the
  bundle opens, with one message naming the event store, the saga store and the default read-model
  store, in place of one message per store; the failure can surface through whichever store bean is
  created first, and on Quarkus through `StreamRuneConfigValidator`'s lookups at startup. On
  Micronaut, two engines fail the start also when the application supplies its own event store
  factory and command bus, because the retention sweepers open the bundle. An application
  `CryptoEngine` bean that injects a framework store is now a circular dependency.

Unreleased changes for the SQLite backend:

Added

- **SQLite backend.** `streamrune.backend=sqlite` runs StreamRune on one SQLite database file, for a
  small service that runs as one instance. The new module `streamrune-sqlite` implements every store
  of `streamrune-postgres` (event store and snapshots, offsets, sagas and saga dead letters, outbox,
  command inbox, the command and projection dead-letter stores, audit stores and queries) and
  `SqliteReadModelStore`, the default read-model store named `sqlite`, whose queries return
  PostgreSQL's rows in PostgreSQL's order. Each store passes the contract suite of its interface.
  `SqliteBackend` opens all of them on one `SqliteDatabase` handle, from an integration or with
  `new SqliteBackend().open(context)` in plain Java. See the
  [SQLite guide](docs/guide/sqlite.md).
- **Startup checks on SQLite.** The handle refuses, at startup and before anything is written, a
  `DataSource` that is not SQLite (naming the backend and the database), wrong connection settings
  (`journal_mode=WAL`, `synchronous=FULL`, `foreign_keys`, a `busy_timeout`,
  `transaction_mode=IMMEDIATE`; the message names the setting), a pool that hands out
  `autoCommit=false` connections (a store write on a connection in manual-commit mode is refused
  too, with a message that names both causes, such a pool and an application transaction), an
  in-memory or temporary database, attached databases, a file not encoded in UTF-8 and a SQLite
  library older than 3.45.0. The integrations run these checks when
  the application starts.
- **Instance guard.** An operating-system lock on `<database-file>.lock`, held from opening the
  stores until they are closed, makes a second process on the same file fail its start with
  `SqliteGuardException`. A database file with more than one hard link is refused. Start the JVM with
  `--enable-native-access=ALL-UNNAMED`: the guard calls `flock(2)` through the Foreign Function and
  Memory API.
- **SQLite crypto.** The new module `streamrune-sqlite-crypto` adds `SqliteCryptoEngine` (keys in
  the SQLite file), `SqliteForgottenSubjectStore`, an in-process forget signal and
  `SqliteCryptoBackend`; `streamrune.crypto.sqlite.enabled=true` switches the key-store engine on.
  The filesystem, Vault and AWS KMS engines run on SQLite too. On SQLite an erasure inside a
  projection batch is refused, and a key minted inside a batch commits or rolls back with it.
- **Single-instance defaults.** With `sqlite`, `streamrune.subscription.single-active-consumer.enabled`
  means `false` when it is not set and `true` fails the start; a set
  `streamrune.event-store.statement-timeout` is ignored with one WARN at startup.
- **Native image registration.** The three integrations register the two SQLite backend classes for
  reflection, the SQLite schema scripts as resources and the instance guard's foreign downcalls; a
  native image of a SQLite application has not been verified end-to-end. Pass
  `--enable-native-access=ALL-UNNAMED` to `native-image` as well.
- **`AtomicBatchProcessorContract.locksPerProjection()`.** A store whose write lock spans every
  projection returns `false`, which skips the one case that needs a lock per projection.

Changed

- **Retention sweepers wait for a sweep in flight on `close()`.** The outbox, inbox, command
  dead-letter and saga dead-letter retention sweepers interrupt their thread and wait up to 30
  seconds for a running sweep to return, on PostgreSQL too, so a store closed after its sweeper is
  never written by a sweep that outlived the close. With all four mid-sweep, shutdown can take about
  two minutes. A sweep that does not end in time logs a WARN. A sweep whose store swallows the
  interrupt (a pool or a driver that catches it and returns normally) ends the sweeper's thread
  when it returns, instead of sleeping out the sweep interval. A saga compensation-retry sweeper
  checks for a stop before every re-drive of a batch, not only between batches.
- **Quarkus opens the backend's stores at startup.** With a `DataSource` (every Quarkus application
  with Agroal has one), the `BackendStores` bean is created by a startup observer that runs after
  the startup validators, on PostgreSQL too: the `CryptoEngine` lookup and its ambiguity check, the
  metrics and the tracing are resolved at startup instead of at the first use of a store. On
  PostgreSQL no database is touched. Micronaut checks the stores in a startup listener after its
  validators; Spring creates the bean with the other singletons.
- **Quarkus stops the framework around the application's stop.** The framework's observers of
  `ShutdownEvent` and of `@BeforeDestroyed(ApplicationScoped.class)`, which also stop the
  framework's threads when the container shuts down after a refused start, run in two steps around
  the application's observers at the default priority: at `Interceptor.Priority.LIBRARY_BEFORE +
  100` the saga compensation-retry sweepers, which follow the lease of the saga's subscription, and
  the SSE event feed stop; at `Interceptor.Priority.LIBRARY_AFTER + 500` `StreamRuneLifecycle`
  stops its runners and then closes the subscription leadership and the command bus. A saga's
  subscription the application stops on either event keeps its lease and the command bus until its
  last batch ends, and a sweeper ends the re-drive in flight and starts no other before the
  subscription gives the lease up.
- **Spring health reads the head through the `EventStore` bean.** `StreamRuneHealthIndicator` reports
  `eventStore.lastGlobalOffset` from `EventStore.lastGlobalOffset()`, as the Quarkus and Micronaut
  checks do, instead of querying `event_stream` itself, so the detail is the same on every backend.
  The detail `eventStore.lastEventTimestamp` is gone, on PostgreSQL too, and without an `EventStore`
  bean the check reports no `eventStore.*` detail. The constructor that took the relay contributor
  third now takes the `EventStore` third and the relay contributor fourth.
- **Backend-neutral messages.** The WARN of all three integrations when a cached crypto engine has
  no forget signal asks for "a DataSource on the selected backend's database"; the Quarkus failure
  to build the key-store engine names the backend ("Failed to build the postgres key-store
  engine"); the Quarkus WARN that no saga dead-letter sweeper exists names "the framework's own saga
  dead-letter store".
- **Leadership default in the configuration metadata.** Spring's and Micronaut's metadata for
  `streamrune.subscription.single-active-consumer.enabled` declare no default, because it depends on
  the backend (`true` on `postgres`, `false` on `sqlite`); the value a PostgreSQL application gets is
  unchanged.
- **`CachedCryptoEngine` waits at most 250 ms for a lock stripe on a cache miss.** Past that, the
  decrypt calls the engine without the stripe and caches nothing, instead of waiting without bound
  behind an erasure of a subject on the same stripe. An interrupted wait takes the same path. It
  refuses a `null` subject in `deleteKey`, `reinstate` and `isKeyAvailable` with
  `IllegalArgumentException`.
- **The stores check their arguments as their interfaces document.** On PostgreSQL and in memory: a
  negative `maxCount` of `EventStore.readGlobalStream` and `readStream` fails with
  `IllegalArgumentException` before the query; the dead-letter queue, the projection dead-letter
  store, the audit stores and queries and the forgotten-subject stores refuse `null` arguments and
  non-positive limits up front; publishing a dead-letter `commandId` the queue already holds fails
  with `EventStoreException` and keeps the stored entry, in memory as on PostgreSQL; the in-memory
  event audit store refuses a duplicate `eventId` with `EventStoreException`, storing nothing of the
  batch; a `PostgresReadModelStore` named `default` is refused.
- **Contract suites.** `streamrune-test` adds contract suites for `DeadLetterQueue`,
  `ProjectionDeadLetterStore`, `AuditStore`, `EventAuditStore`, `CommandAuditQuery`,
  `EventAuditQuery`, `ComplianceReportQuery`, the crypto key store and `ForgottenSubjectStore`, and
  extends `ProjectionRepositoryQueryContract` with the text semantics of numbers, booleans, `null`
  and mixed types. Each runs on PostgreSQL, SQLite and the in-memory stores where they exist.

Unreleased changes to crypto erasure on PostgreSQL (Changed):

- **An erasure writes `forgotten_subjects.key_created_at`.** `PostgresCryptoEngine.deleteKey` and
  `JdbcForgottenSubjectStore.forget(subjectId)` insert the tombstone with the statement of
  `forget(subjectId, keyCreatedAt)`, binding `key_created_at` to NULL when there is no evidence of
  the erased key's creation. On the shipped schema (the column is nullable with no default) the
  stored row is the same. A hand-made `forgotten_subjects` table without that column,
  used with `validateSchema(false)`, now fails the erasure; the shipped crypto baseline and the DDL
  of the `streamrune-postgres-crypto` README have it.

Unreleased fixes to the Quarkus integration (Fixed):

- **Projection keys in a file or a system property.** `streamrune.projections.<name>.enabled`,
  `streamrune.projections.<name>.error-strategy` and `streamrune.projections.auto-discovery.enabled`
  are accepted in `application.properties` and as a `-D` system property, where SmallRye refused
  them (`SRCFG00050 … does not map to any root`): the configuration mapping declares the keys under
  `streamrune.projections.` (`StreamRuneQuarkusProperties#projections()`). A key there set anywhere
  but the process environment or a `.env` file fails the start, naming it, unless it is
  `auto-discovery.enabled`, or `<name>.enabled` or `<name>.error-strategy` for the name of a
  `@ProjectionConfig` projection bean of the running application (`ProjectionKeysValidator`; no
  name is checked while auto-discovery is off, and nothing with
  `quarkus.config.mapping.validate-unknown=false`, like SmallRye's own refusal).
- **Quarkus REST applications start.** An application with `quarkus-rest` passes its static
  initialization, which failed with `SRCFG00027: Could not find a mapping for
  StreamRuneQuarkusProperties`: Quarkus REST creates `StreamRuneRequestFilter` while the
  application initializes statically, and the filter reads
  `streamrune.metadata.baggage-allowlist`, the `RequestIdentityPolicy` and the `UserRoleResolver`
  when the first request arrives, from the runtime configuration — in a native image too, where
  `streamrune.security.trust-user-id-header` is the running application's setting, not the
  build's. Its injecting constructor takes `Instance`s of the three.

Unreleased changes to the PostgreSQL event store (Changed):

- **A null snapshot version is refused up front.** On PostgreSQL, `saveSnapshot` with a null
  version now fails with `NullPointerException("version is required")` before any serialization or
  connection, as on SQLite, also when the state would have been refused later (an unregistered or
  mismatched state class, an invalid type name, a serialization failure: it was an
  `EventStoreException` then).
- **A negative `global_offset` reads as a corrupt row.** On PostgreSQL, an otherwise readable
  `event_stream` row with a negative `global_offset` (only reachable by hand-written SQL) now reads
  as `EventDeserializationException`, after event-type resolution, as on SQLite (it was a raw
  `IllegalArgumentException`).

Unreleased fixes to the SQLite backend (Fixed):

- **A begin the driver fails after it took effect is rolled back.** When sqlite-jdbc throws after
  a store's `BEGIN IMMEDIATE` took effect (an out-of-memory error in its own code, a failed
  statement close), the store rolls the transaction back before it releases the file's write mutex
  and rethrows the failure unchanged; a failed rollback closes the physical connection. So the
  connection does not go back to the pool with the write transaction open, where, until the pool
  retired it, it would hold SQLite's write lock (every other writer failing with `SQLITE_BUSY`),
  refuse its next begin, and keep the writes of a one-statement write on it from ever committing.
  A begin refused because the connection is in manual-commit mode or already has a transaction
  open is not followed by a rollback: that transaction is not the store's.

### Modules

Eighteen published artifacts, one per Gradle project in `settings.gradle.kts` except the three
aggregator projects (`streamrune-crypto`, `streamrune-integration`, `streamrune-outbox`) and the
three test projects, which are not published: `streamrune-integration:streamrune-e2e` (the
end-to-end crash-recovery and broker suites),
`streamrune-integration:streamrune-quarkus-it` (a Quarkus application the Quarkus Gradle plugin
builds and starts on `streamrune-quarkus`) and
`streamrune-integration:streamrune-integration-tck` (the wiring contract, the configuration oracle
and the doc reading that the tests of the three integrations share). The artifact id is the project
name. Each jar declares a stable JPMS module name (four ship a `module-info.java`, the
other fourteen carry `Automatic-Module-Name`), bundles `LICENSE`, `NOTICE` and `LICENSE-COMMERCIAL.md`
under `META-INF`, and sets `SPDX-License-Identifier: BUSL-1.1` in its manifest.

| Artifact (`org.streamrune:`) | Gradle project | JPMS module | Contents |
|---|---|---|---|
| `streamrune-core` | `:streamrune-core` | `org.streamrune.core` | Contracts and value types: `EventStore`, `Decider`, `CommandBus`, `Projection`, `ProjectionDeliveryMode`, `QueryBus`, saga, subscription, outbox, audit, GDPR and `CryptoEngine` SPIs, typed ids, `MetricNames`. Depends only on Jackson. |
| `streamrune-runtime` | `:streamrune-runtime` | `org.streamrune.runtime` | `StreamRune` facade, `VirtualThreadCommandBus`, interceptors, projection runners, subscriptions, `SagaRunner` and its sweepers, outbox poller and replayer, GDPR services, retention sweepers, SSE publisher. |
| `streamrune-postgres` | `:streamrune-postgres` | `org.streamrune.postgres` | `PostgresEventStore` and its factory, the Flyway baseline, `SchemaValidator`, `PgAdvisoryLocker`, `LeaseBasedLeadership`, `JdbcProjectionRepository`, the PostgreSQL inbox, outbox, saga, dead-letter, audit and offset stores, LISTEN/NOTIFY subscriptions. |
| `streamrune-sqlite` | `:streamrune-sqlite` | `org.streamrune.sqlite` | `SqliteBackend`, the `SqliteDatabase` handle with its instance guard, the SQLite schema series, and every store on one SQLite file: event store, snapshots, offsets, sagas, outbox, inbox, dead letters, audit, `SqliteReadModelStore`. Brings the `sqlite-jdbc` driver. |
| `streamrune-crypto-api` | `:streamrune-crypto:streamrune-crypto-api` | `org.streamrune.crypto` | `CryptoShreddingModule` (Jackson), `CachedCryptoEngine`, `ForgottenSubjectStore`. |
| `streamrune-filesystem-crypto` | `:streamrune-crypto:streamrune-filesystem-crypto` | `org.streamrune.crypto.filesystem` | `FileSystemCryptoEngine` — AES-256-GCM keys on disk. |
| `streamrune-postgres-crypto` | `:streamrune-crypto:streamrune-postgres-crypto` | `org.streamrune.crypto.postgres` | `PostgresCryptoEngine`, `JdbcForgottenSubjectStore`, `PostgresCryptoForgetSignal`, the crypto Flyway baseline. |
| `streamrune-sqlite-crypto` | `:streamrune-crypto:streamrune-sqlite-crypto` | `org.streamrune.crypto.sqlite` | `SqliteCryptoEngine`, `SqliteForgottenSubjectStore`, the in-process forget signal, `SqliteCryptoBackend`, the SQLite crypto schema series. |
| `streamrune-vault-crypto` | `:streamrune-crypto:streamrune-vault-crypto` | `org.streamrune.crypto.vault` | `VaultCryptoEngine` — HashiCorp Vault transit engine. |
| `streamrune-aws-kms-crypto` | `:streamrune-crypto:streamrune-aws-kms-crypto` | `org.streamrune.crypto.aws` | `AwsKmsCryptoEngine` — AWS KMS customer managed key with per-subject tombstones. |
| `streamrune-test` | `:streamrune-test` | `org.streamrune.test` | `InMemoryEventStore`, fixtures (decider, event store, projection, saga, recorded event streams), in-memory doubles of every store and bus, fault-injecting forwarding stores, recording doubles, store contract suites. |
| `streamrune-integration-api` | `:streamrune-integration:streamrune-integration-api` | `org.streamrune.integration` | Shared integration pieces: `MicrometerStreamRuneMetrics`, `RequestIdentityPolicy`, `RequestEdgeAuthority`, `AuthenticatedUserResolver`, `SseAuthorizer`, the readers of the shared property values (`SingleActiveConsumerSetting`, `SagaForwardFenceSetting`, …), and the startup validators and wiring helpers the three integrations share, which are `@Internal`. |
| `streamrune-spring` | `:streamrune-integration:streamrune-spring` | `org.streamrune.spring` | Spring Boot 4 auto-configuration, health indicator, SSE controller, request filter, native-image hints. |
| `streamrune-spring-boot-starter` | `:streamrune-integration:streamrune-spring-boot-starter` | `org.streamrune.spring.boot.starter` | Dependency aggregator: `streamrune-spring` + `streamrune-postgres` + `streamrune-runtime`. |
| `streamrune-quarkus` | `:streamrune-integration:streamrune-quarkus` | `org.streamrune.quarkus` | Quarkus (ArC) producers, lifecycle, readiness check, SSE controller, request filter, reflection config; ships `META-INF/beans.xml` and a Jandex index, no extension build step. |
| `streamrune-micronaut` | `:streamrune-integration:streamrune-micronaut` | `org.streamrune.micronaut` | Micronaut factories, lifecycle, health indicator, SSE controller, request filter, `reflect-config.json`. |
| `streamrune-kafka-outbox` | `:streamrune-outbox:streamrune-kafka-outbox` | `org.streamrune.outbox.kafka` | `KafkaOutboxPublisher`. |
| `streamrune-rabbitmq-outbox` | `:streamrune-outbox:streamrune-rabbitmq-outbox` | `org.streamrune.outbox.rabbitmq` | `RabbitMqOutboxPublisher` (publisher confirms). |

The PostgreSQL event store module moved from `streamrune-eventstore/streamrune-postgres` to `streamrune-postgres` at the repository root, so its Gradle project is `:streamrune-postgres`; the artifact id `streamrune-postgres` and the package `org.streamrune.postgres` are unchanged.

### API stability

From `1.0.0` on, StreamRune follows Semantic Versioning: a minor or patch release does not break
code written against the public API. The public API is the public types and members of the
published modules, except those annotated `@Internal` (`org.streamrune.core.Internal`), plus the
configuration keys and their units, the metric names, the health component names and the database
schema the migrations create. `@Internal` marks what is public only because another StreamRune
module uses it; it may change in any release and is left out of the generated API documentation.
The `@Bean`, producer and factory methods of the three integrations are wiring the container
calls, not API. Interfaces that applications implement (`EventStore`, `CommandBus`,
`CommandInterceptor`, `SagaStore` and the other ports) may gain `default` methods in a minor
release. A `CommandBus` or `EventStore` decorator forwards every method, the defaults included, on
the calling thread (the class javadoc of each says why); `ForwardingCommandBus` and
`ForwardingEventStore` in `streamrune-test` do. See [API stability](README.md#api-stability).

### Requirements

- **Java 25** (virtual threads and `ScopedValue` are used throughout; every jar is built with the Java
  25 toolchain).
- **No Graal JIT on JDK 25.** The Graal JIT (GraalVM's default compiler) is not reliable with
  StreamRune on JDK 25 and is not supported: StreamRune refuses to start on a JDK 25 JVM that runs it,
  and on a later JDK release with it, as not verified. The check accepts a JVM only when it reads its
  JVMCI compiler as off (`UseJVMCICompiler=false`) or finds none; run GraalVM with
  `-XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler`, the documented way to turn it off, which
  every JDK 25 HotSpot build we ran accepts. JVMs whose JIT is C2 (Temurin, Zulu) and native images
  were unaffected in our runs. The check (`org.streamrune.runtime.GraalJitGuard`, an `@Internal` class
  that applications do not call) runs when a command bus is built and when an integration starts. A runtime without the module `jdk.management` that
  identifies as GraalVM is refused even with the JVMCI compiler off — add the module, or set the
  opt-out below once the compiler is off — and one that does not identify as GraalVM starts with a
  `WARN` line. `streamrune.jvm.allow-graal-jit=true` (default `false`; for a runtime built by hand the
  system property of that name, or `VirtualThreadCommandBus.Builder.allowGraalJit(true)`) starts
  anyway, at your own risk, with one `ERROR` line. See
  [Supported JVMs](docs/guide/production.md#supported-jvms).
- **PostgreSQL 17 or newer** for `streamrune-postgres` and `streamrune-postgres-crypto` — tested on 17
  and 18, and enforced at startup. The in-memory stores of `streamrune-test` need no database.
- **SQLite 3.45.0 or newer** for `streamrune-sqlite` and `streamrune-sqlite-crypto`, enforced at
  startup. `streamrune-sqlite` brings `org.xerial:sqlite-jdbc` 3.54.1.0 (an `api` dependency), which
  bundles SQLite 3.54.1, and Flyway 13.11.1 (`flyway-core`). Start the JVM with
  `--enable-native-access=ALL-UNNAMED`. The database file must be on a local disk, not NFS or SMB.
  The instance guard supports Linux, macOS and Windows; the project's tests run on Linux and macOS.
- **Jackson 2.19.2** (`jackson-databind` 2.19 or newer). `streamrune-core` depends on nothing else.
- `streamrune-postgres` brings Flyway 13.11.1 (`flyway-core`, `flyway-database-postgresql`), HikariCP
  6.2.1 (an `api` dependency, Maven `compile` scope: the module builds HikariCP pools for its
  dedicated LISTEN and lock pools, and the convenience
  `PgAdvisoryLocker.withDedicatedPool(url, user, password, max, MetricsTrackerFactory)` takes a
  HikariCP type, the only public signature that names one;
  `PgAdvisoryLocker.withDedicatedPool(DataSource)` takes a lock pool of any library and
  `PostgresNotificationSubscription.createListenDataSource` returns a `ListenDataSource`), the
  PostgreSQL JDBC driver 42.7.10 and SLF4J 2.0. At runtime scope only, it also brings Dropwizard
  Metrics 4.2.40 and 5.0.8 (`metrics-core` and `metrics-healthchecks` of each) and Micrometer
  1.16.3: HikariCP checks for those types, and a GraalVM native-image build of the Quarkus and
  Micronaut integrations fails unless they resolve. StreamRune never creates them, and HikariCP uses
  them only when the application hands it a registry of its own.
- `streamrune-runtime` brings, at runtime scope, cron-utils 9.2.1 (the cron schedules of
  `CronExpression`), the Jakarta Validation API 3.0.2 (`BeanValidationInterceptor` and
  `ValidatingProjectionDecorator`, which validate only with a provider the application adds), Caffeine
  3.2 and Jackson's `jackson-datatype-jsr310`.
- Frameworks: Spring Boot 4.1.x (built and tested against 4.1.2, Spring Security 7.1.x), Quarkus
  3.40.x, Micronaut 5.2.x (Micronaut Security 5.4, Validation 5.2, Serde 3.2). Micrometer 1.16 or
  newer: the framework declares 1.16.3, the floor it is built against, and an application runs the
  version its framework manages (1.17 with Spring Boot 4.1 and Quarkus 3.40). OpenTelemetry API 1.47 or
  newer (optional for Spring and Micronaut; Quarkus brings it).
- Outbox brokers: Kafka clients 3.9 (`streamrune-kafka-outbox`), RabbitMQ `amqp-client` 5.25
  (`streamrune-rabbitmq-outbox`). Crypto backends: AWS SDK v2 KMS 2.57.3 (`streamrune-aws-kms-crypto`);
  HashiCorp Vault with the transit engine enabled, over its HTTP API (`streamrune-vault-crypto`).
  `streamrune-crypto-api` brings Caffeine 3.2.
- GraalVM native image: GraalVM CE 25.0.1.
- `streamrune-test` runs on JUnit Jupiter 5.12.2 or newer, 6.x included: it requires
  `junit-jupiter-api` at 5.12.2 (an `api` dependency) and brings no JUnit BOM of its own, so an
  application's tests run on the JUnit (5.12.2 or newer) its own build manages.
- Docker is needed only to run the framework's own PostgreSQL-backed test suites (Testcontainers).

### Known limitations

- **One global write lane per event store.** Every append reserves its offsets from a single counter
  row and holds that lock until commit, so appends to unrelated streams serialize and the global
  append rate does not grow with application instances. No measured capacity envelope is published.
  Scale by partitioning into separate event stores; there is no global order across partitions.
- **SQLite serves one instance.** The SQLite backend has no subscription leadership, no statement
  timeout and no push subscription, and one write lock serializes every write to the file. Its
  native-image registration has not been verified with a native image of a whole application.
- **Single writable primary.** No multi-master; use primary/standby failover. No backup/restore drill
  against a production deployment has been performed and no RPO/RTO figures are published.
- **PostgreSQL needs a running server.** `streamrune-postgres` needs a running PostgreSQL 17+; the
  SQLite backend (`streamrune-sqlite`) runs in-process on one file, for one instance only;
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
  `writeTarget()`, the runner takes `writesThroughRepository()` at its word: it does not check that
  the two-argument `process` writes through the handed repository rather than through a repository
  the projection captured itself.
- **The circuit breaker is per-JVM, in-memory state**; each replica opens and closes independently.
- **`SnapshotPolicy` is global per `VirtualThreadCommandBus`.**
- **Subscriptions are at-least-once across `pause()`/`resume()`**; listeners must be idempotent.
- **Projection dead-letter replay is an at-least-once, out-of-order patch** behind the checkpoint
  and requires an idempotent `process`. The replay's commit and the entry's discard are two steps.
- **Nothing compares a projection processor's `DataSource` with the offset store's at startup.** The
  mismatch surfaces on the second batch, as a halted projection and health `DOWN`.
- **The plain `StreamRune` facade has no health surface**: a projection thread started by
  `startProjections()` that dies is reported only as an ERROR log.
- **Outbox**: one channel per application in the shipped integrations, hence one ordering mode;
  `OutboxStore.save(entry)` outside `append` is outside the commit-order promise; replaying on an
  `AVAILABILITY_FIRST` channel reorders within a stream; a post-hand-off failure parks an entry for a
  full claim lease; a hot stream's delivery rate is one in flight; a change to a `payload` schema or
  `payloadType` breaks entries still pending under the old shape — drain the outbox first.
- **Sagas**: `SagaRunner` is designed for one subscription per saga type; write undo commands as
  idempotent cancel/void operations; a saga exposed to veto-style interceptors must configure
  `timeout()` — without one, a retry-later refusal that persists holds the saga's subscription
  until its cause is fixed; a bus decorator hands the bus the saga's command instance or its key,
  since one that replaces both makes the saga's command neither compared nor fenced; a drain
  assumes it is the only recovery actor on its saga; within one timeout population, many failing
  rows can delay the rest of a batch; the dead-letter `errorMessage` column is outside
  crypto-shredding — keep personal data out of orchestrator exception messages.
- **Crypto**: `@Encrypted` applies to `String` record components only; no key rotation; postgres and
  filesystem store key bytes unwrapped (no envelope encryption); the filesystem backend has no
  replication and shares a key directory safely only within one kernel's view of the filesystem;
  Vault supports token auth only and refuses `reinstate` after a completed erasure; on AWS KMS
  `reinstate` makes pre-erasure ciphertext readable again, and cached plaintext data keys stay in a
  replica's heap for up to the data-key cache window plus the time to the next sweep (a replica
  that serves no encrypt or decrypt keeps them until it does); `[REDACTED]` is an in-band marker — confirm
  erasure with `isKeyAvailable`; property-level `@JsonSerialize`/`@JsonDeserialize` on a field of an
  `@Encrypted`-carrying record type is unguarded and leaks plaintext on write; a restore brings
  erased keys back — re-apply the erasures; cross-replica cache eviction depends on the forget
  signal's LISTEN connection, bounded by the cache TTL; with `PostgresCryptoEngine` on the
  application pool, a `TRANSACTIONAL_LOCAL` or `EXTERNAL_EFFECT` projection over `@Encrypted` read
  models holds two connections while its batch runs, so P such projections catching up at once
  need at least P+1 connections (2P recommended).
- **GDPR**: a dead-lettered command's personal data is protected only by its own `@Encrypted`
  components (otherwise plaintext until the dead-letter retention sweeper deletes the row); the query
  cache eviction reaches this process only; `ForgetSubjectService` and `ExportSubjectDataService`
  record the requester but do not authorize it — authorize the caller first; nothing re-drives an
  interrupted erasure on its own — call `forget` again.
- **Audit**: an `AuditStore.save()` failure does not fail the command — it is logged at ERROR and that
  audit row is lost; `userId` is plaintext; the raw idempotency key is the primary key of
  `command_inbox` and is stored on a dead-letter row.
- **Query cache**: TTL and event-driven invalidation are caching controls, not security controls;
  eviction is per query type; without a `QueryAuthorizer` a cache hit skips the handler's
  authorization. `QueryAuthorizer.authorize(UserId user, Query<?> query)` takes the caller first,
  as `CommandAuthorizationPolicy` does.
- **Metrics**: `stream.id` is never a metric tag; `subscriptions.listener.reconnects` cannot signal a
  dead push path — use the health detail.
- **Integrations**: `@StreamRuneComponent` needs component scan; the `StreamRune` facade is a
  programmatic bootstrap, not an injectable bean (inject `CommandBus`); SSE is live and not durable
  — at-most-once to connected clients, no replay, and a slow subscriber is disconnected; Quarkus needs Quarkus REST for the request filter and SSE and selects
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
Source License 1.1`. External code contributions are not accepted until a Contributor License
Agreement is in place; issues and bug reports are welcome (`CONTRIBUTING.md`).
