# Changelog

All notable changes to StreamRune are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

`1.0.0-alpha-SNAPSHOT` is the unsigned preview of the next release, published from `main`: it is
rebuilt each time the Build workflow passes there and goes only to the Maven Central snapshot
repository (`https://central.sonatype.com/repository/maven-snapshots/`, see
[Preview builds](README.md#preview-builds)), never to Maven Central itself. Its API, database schema
and configuration keys can change in any later snapshot without a migration path. A schema change
edits the baseline migrations in place, so a database created by an earlier snapshot fails Flyway's
checksum validation: recreate it (with Docker Compose, `docker compose down -v`) when you move to a
newer snapshot.

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
  working. `ExportSubjectDataService` collects a subject's data for an access request.
- **Sagas with subscription leadership.** `SagaRunner` drives a saga from any subscription: every
  command it dispatches is keyed for the command inbox, a failed step opens a compensation episode,
  and the event store's forward fence (on by default) refuses a forward command once that episode
  has opened. A runner built with a `SubscriptionLeadership` (`SagaRunner.Builder.leadership`) has
  one replica per saga type deliver its events, claim its deadlines and retry its compensations, and
  another replica takes over within the lease TTL of a crash.
- **Command dead-letter queue with operator-confirmed replay.** A command that keeps failing on
  infrastructure, never one the domain rejected, is parked in the dead-letter queue, and its caller
  receives `CommandDeadLetteredException`, whose `pendingOutcome()` says whether the retry schedule
  replays the entry (`QUEUED_FOR_REPLAY`) or an operator decides (`AWAITING_OPERATOR`); a keyed
  command that fails again under its key stores no second entry. The retry runner replays on its
  own only the entries the command inbox can deduplicate; an unkeyed entry, or a keyed one older than
  the inbox retention window, waits for an operator, who replays it with
  `DeadLetterRetryRunner.retry(commandId, true)` or discards it.
- **Outbox ordered per aggregate by default.** Outbox entries commit or roll back with the events
  that produced them. Under `STRICT_PER_AGGREGATE`, the default ordering mode, the entries of a stream
  are delivered in commit order with at most one in flight, and a terminally failed entry holds its
  stream until an operator replays or skips it; `AVAILABILITY_FIRST` is the explicit choice for
  consumers that tolerate reordering. Publishers for Kafka, RabbitMQ and HTTP.
- **Explicit projection delivery modes.** Every projection registration declares
  `TRANSACTIONAL_LOCAL`, `AT_LEAST_ONCE_IDEMPOTENT` or `EXTERNAL_EFFECT` — there is no default — and
  the runner checks the declaration against its processor before it reads an event, so at-least-once
  delivery is always a declared choice.
- **Fail-closed identity.** One rule, `RequestIdentityPolicy`, derives the caller on all three
  integrations: a trusted gateway's `X-User-Id` header when
  `streamrune.security.trust-user-id-header=true`, otherwise the authenticated principal, otherwise
  anonymous. Startup is refused when command authorization is configured and the mode is anonymous.

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
The parts below list its modules, requirements, known limitations and license.

### Modules

Sixteen published artifacts, one per Gradle project in `settings.gradle.kts` except the four
aggregator projects (`streamrune-eventstore`, `streamrune-crypto`, `streamrune-integration`,
`streamrune-outbox`) and the two test projects, which are not published:
`streamrune-integration:streamrune-e2e` (the end-to-end crash-recovery and broker suites) and
`streamrune-integration:streamrune-integration-tck` (the wiring contract, the configuration oracle
and the doc reading that the tests of the three integrations share). The artifact id is the project
name. Each jar declares a stable JPMS module name (four ship a `module-info.java`, the
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
| `streamrune-test` | `:streamrune-test` | `org.streamrune.test` | `InMemoryEventStore`, fixtures (decider, event store, projection, saga, recorded event streams), in-memory doubles of every store and bus, fault-injecting forwarding stores, recording doubles, store contract suites. |
| `streamrune-integration-api` | `:streamrune-integration:streamrune-integration-api` | `org.streamrune.integration` | Shared integration pieces: `MicrometerStreamRuneMetrics`, `RequestIdentityPolicy`, `AuthenticatedUserResolver`, `SseAuthorizer`, `ProjectionProcessorResolution`, `CommandInboxWiringValidator`, `SagaRetentionValidator`. |
| `streamrune-spring` | `:streamrune-integration:streamrune-spring` | `org.streamrune.spring` | Spring Boot 4 auto-configuration, health indicator, SSE controller, request filter, native-image hints. |
| `streamrune-spring-boot-starter` | `:streamrune-integration:streamrune-spring-boot-starter` | `org.streamrune.spring.boot.starter` | Dependency aggregator: `streamrune-spring` + `streamrune-postgres` + `streamrune-runtime`. |
| `streamrune-quarkus` | `:streamrune-integration:streamrune-quarkus` | `org.streamrune.quarkus` | Quarkus (ArC) producers, lifecycle, readiness check, SSE controller, request filter, reflection config; ships `META-INF/beans.xml` and a Jandex index, no extension build step. |
| `streamrune-micronaut` | `:streamrune-integration:streamrune-micronaut` | `org.streamrune.micronaut` | Micronaut factories, lifecycle, health indicator, SSE controller, request filter, `reflect-config.json`. |
| `streamrune-kafka-outbox` | `:streamrune-outbox:streamrune-kafka-outbox` | `org.streamrune.kafka` | `KafkaOutboxPublisher`. |
| `streamrune-rabbitmq-outbox` | `:streamrune-outbox:streamrune-rabbitmq-outbox` | `org.streamrune.rabbitmq` | `RabbitMqOutboxPublisher` (publisher confirms). |

### Requirements

- **Java 25** (virtual threads and `ScopedValue` are used throughout; every jar is built with the Java
  25 toolchain).
- **PostgreSQL 17 or newer** for `streamrune-postgres` and `streamrune-postgres-crypto` — tested on 17
  and 18, and enforced at startup. The in-memory stores of `streamrune-test` need no database.
- **Jackson 2.19.2** (`jackson-databind` 2.19 or newer). `streamrune-core` depends on nothing else.
- `streamrune-postgres` brings Flyway 13.11.1 (`flyway-core`, `flyway-database-postgresql`), HikariCP
  6.2.1 (for its dedicated LISTEN and lock pools; an `api` dependency, Maven `compile` scope, since
  `PgAdvisoryLocker.withDedicatedPool` takes a HikariCP `MetricsTrackerFactory`), the PostgreSQL JDBC
  driver 42.7.10 and SLF4J 2.0.
- Frameworks: Spring Boot 4.1.x (built and tested against 4.1.1, Spring Security 7.1.x), Quarkus
  3.40.x, Micronaut 5.2.x (Micronaut Security 5.4, Validation 5.2, Serde 3.2). Micrometer 1.16 or
  newer: the framework declares 1.16.3, the floor it is built against, and an application runs the
  version its framework manages (1.17 with Spring Boot 4.1 and Quarkus 3.40). OpenTelemetry API 1.47 (optional).
- Outbox brokers: Kafka clients 3.9 (`streamrune-kafka-outbox`), RabbitMQ `amqp-client` 5.25
  (`streamrune-rabbitmq-outbox`). Crypto backends: AWS SDK v2 KMS 2.57.3 (`streamrune-aws-kms-crypto`);
  HashiCorp Vault with the transit engine enabled, over its HTTP API (`streamrune-vault-crypto`).
  `streamrune-crypto-api` brings Caffeine 3.2.
- GraalVM native image: GraalVM CE 25.0.1.
- Docker is needed only to run the framework's own PostgreSQL-backed test suites (Testcontainers).

### Known limitations

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
  `timeout()`; a drain assumes it is the only recovery actor on its saga; within one timeout
  population, many failing rows can delay the rest of a batch; the dead-letter `errorMessage` column
  is outside crypto-shredding — keep personal data out of orchestrator exception messages.
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
  signal's LISTEN connection, bounded by the cache TTL.
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
  authorization.
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
