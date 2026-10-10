# StreamRune Quick Start

Step-by-step guide to building your first event-sourced application with StreamRune.

## Prerequisites

- **Java 25** — StreamRune uses virtual threads and modern APIs
- **Gradle 8+** — or a Gradle wrapper generated from this project
- **PostgreSQL 17 or newer** (only if using `streamrune-postgres`; the test module works without it) —
  tested on 17 and 18; an older server is refused at startup

---

## 1. Add the StreamRune preview build

StreamRune's modules all share the `org.streamrune` group and one version. StreamRune is not
released yet: the published build is `1.0.0-alpha-SNAPSHOT`, an unreleased preview that lives in the
Maven Central snapshot repository, not in Maven Central itself (see
[Preview builds](../README.md#preview-builds) and the [CHANGELOG](../CHANGELOG.md)). Add that
repository next to `mavenCentral()` — there is no local build step, and each module pulls in the
rest of the framework it needs transitively. The next section shows a complete `build.gradle`.

---

## 2. Create a New Project

```groovy
// build.gradle
plugins {
    id 'java-library'
}

java {
    sourceCompatibility = '25'
    targetCompatibility = '25'
}

repositories {
    mavenCentral()
    // 1.0.0-alpha-SNAPSHOT lives in the Maven Central snapshot repository only.
    maven {
        url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        mavenContent { snapshotsOnly() }
        content { includeGroup("org.streamrune") }
    }
}

dependencies {
    implementation("org.streamrune:streamrune-core:1.0.0-alpha-SNAPSHOT")
    implementation("org.streamrune:streamrune-runtime:1.0.0-alpha-SNAPSHOT")
    implementation("org.streamrune:streamrune-postgres:1.0.0-alpha-SNAPSHOT")
}
```

> These three coordinates are enough: `streamrune-runtime` and `streamrune-postgres` pull in
> `streamrune-crypto-api` (and anything else they need) transitively from the same repositories, so you do
> not have to declare the framework's internal modules yourself.

---

## 3. Define a Domain Event

Domain events implement the `org.streamrune.core.DomainEvent` marker interface and must be
Jackson-serializable (records are ideal). A sealed interface plus one record per variant keeps the
`switch` exhaustive.

```java
package com.example;

import org.streamrune.core.DomainEvent;

public sealed interface OrderEvent extends DomainEvent
        permits OrderPlaced, OrderConfirmed, OrderCancelled {
    String orderId();
}

public record OrderPlaced(String orderId, String productId, int quantity) implements OrderEvent {}
public record OrderConfirmed(String orderId) implements OrderEvent {}
public record OrderCancelled(String orderId, String reason) implements OrderEvent {}
```

---

## 4. Define a Command and Decider

Commands implement the `org.streamrune.core.Command` marker interface — it is required on every
command type. A `Decider` is a pure function with three explicit methods: `initialState()` seeds a
fresh aggregate, `decide(command, state)` returns the events to append (an empty `List` for a valid
no-op — never `null`), and `evolve(state, event)` folds each event into the next state. There is no
`CommandResult` return type here; deciders return `List<E>` and the runtime builds the result.

```java
import java.util.List;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;

// Commands
public sealed interface OrderCommand extends Command
        permits PlaceOrder, ConfirmOrder, CancelOrder {
    String orderId();
}

public record PlaceOrder(String orderId, String productId, int quantity) implements OrderCommand {}
public record ConfirmOrder(String orderId) implements OrderCommand {}
public record CancelOrder(String orderId, String reason) implements OrderCommand {}

// State (an AggregateState; cumulative, built by evolve())
public record OrderState(String orderId, boolean placed, boolean confirmed, boolean cancelled)
        implements AggregateState {
    public static final OrderState EMPTY = new OrderState(null, false, false, false);
}

// Decider (pure function: initialState + decide + evolve)
public final class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {

    @Override
    public OrderState initialState() {
        return OrderState.EMPTY;
    }

    @Override
    public List<OrderEvent> decide(OrderCommand command, OrderState state) {
        return switch (command) {
            case PlaceOrder cmd when !state.placed() ->
                List.of(new OrderPlaced(cmd.orderId(), cmd.productId(), cmd.quantity()));

            case ConfirmOrder cmd when state.placed() && !state.confirmed() ->
                List.of(new OrderConfirmed(cmd.orderId()));

            case CancelOrder cmd when state.placed() && !state.cancelled() ->
                List.of(new OrderCancelled(cmd.orderId(), cmd.reason()));

            default -> List.of();   // valid no-op
        };
    }

    @Override
    public OrderState evolve(OrderState state, OrderEvent event) {
        return switch (event) {
            case OrderPlaced e -> new OrderState(e.orderId(), true, false, false);
            case OrderConfirmed e -> new OrderState(state.orderId(), true, true, state.cancelled());
            case OrderCancelled e -> new OrderState(state.orderId(), true, state.confirmed(), true);
        };
    }
}
```

---

## 5. Set Up the Event Store

```java
// Map event-type names to your event classes so payloads can be (de)serialized.
EventTypeRegistry typeRegistry = SimpleEventTypeRegistry.builder()
    .registerEvent("OrderPlaced", OrderPlaced.class)
    .registerEvent("OrderConfirmed", OrderConfirmed.class)
    .registerEvent("OrderCancelled", OrderCancelled.class)
    .build();

// PostgreSQL-backed EventStore. create() auto-configures the ObjectMapper (incl. JavaTimeModule);
// pass a CryptoEngine for encryption at rest and upcasters for schema evolution, or null/empty.
// With a null CryptoEngine, create() refuses a registered type that has an @Encrypted field.
PostgresEventStore eventStore = PostgresEventStore.create(
    dataSource, typeRegistry, /* cryptoEngine */ null, /* upcasters */ List.of());
```

`dataSource` is your application's connection pool (a HikariCP `HikariDataSource`, for example):
the store borrows one connection per operation, builds no pool of its own, and bounds each of its
statements at 30 seconds without changing the connection's session (`PostgresEventStore.builder()`
and `PostgresEventStoreFactory` take a different `statementTimeout`).

`PostgresEventStore.create(...)` does not create the tables — it expects the StreamRune schema to
exist. Provision it first with
`new PostgresEventStoreFactory(dataSource, typeRegistry).initializeSchema()`, which applies the
framework's Flyway baseline on its own history table (so it never collides with your application's
migrations), or build the store through `PostgresEventStoreFactory.create()`, which initializes and
validates the schema by default. The Spring, Quarkus and Micronaut integrations build the store
through the factory, so they provision the schema at startup unless
`streamrune.event-store.schema.auto-initialize=false` (see the production guide's Flyway section).
All three — `PostgresEventStore.create(...)`, `initializeSchema()` and the factory's `create()` —
refuse a server older than PostgreSQL 17 with `UnsupportedServerVersionException` before they
write anything.

For testing, use the in-memory store (no database, no registry required — it keeps events by reference):

```java
InMemoryEventStore eventStore = new InMemoryEventStore();
```

---

## 6. Build and Execute Commands

Register each command type under its **aggregate type** — the name the aggregate's streams carry
(`[a-z][a-z0-9_]{0,31}`; the three order commands are one aggregate, so they share the type
`order`) — together with a decider and an **id extractor**. The extractor returns a typed
`AggregateId` (not a raw String), so its body calls `AggregateId.of(...)`, which refuses a blank id,
one longer than 255 characters, or one containing a control character with
`IllegalArgumentException` (map it to a `400`). One
`OrderDecider` instance handles all three `OrderCommand` variants.

```java
import org.streamrune.core.CommandBus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.AggregateHistory;
import org.streamrune.runtime.StreamRune;

static final AggregateType ORDER = AggregateType.of("order");

StreamRune streamRune = StreamRune.builder()
    .eventStore(eventStore)
    .register(ORDER, PlaceOrder.class,   cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
    .register(ORDER, ConfirmOrder.class, cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
    .register(ORDER, CancelOrder.class,  cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
    .build();

// Execute a command — execute(...) returns a CommandBus.CommandResult built by the runtime.
CommandBus.CommandResult result = streamRune.execute(
    new PlaceOrder("order-42", "product-7", 3)
);
StreamId stream = result.streamId();       // where the events landed: order:order-42
boolean  noop   = result.events().isEmpty();

// Reload the aggregate's event history from the store.
AggregateHistory history = eventStore.load(StreamId.of(ORDER, AggregateId.of("order-42")));
System.out.println(history.events());      // the persisted EventEnvelopes
System.out.println(history.version());     // current stream version

// Clean up on shutdown
streamRune.close();
```

---

## 7. Add a Projection (Read Model)

A `Projection` is an interface with a single abstract method — `void process(List<EventEnvelope> batch)` —
that a **projection runner** drives. Every registration declares a `ProjectionDeliveryMode`, with no
default. The projection below writes to its own store with upsert semantics, so it is
`AT_LEAST_ONCE_IDEMPOTENT`: a redelivered batch is applied again and must leave the same rows, and its
runner takes `AtomicBatchProcessor.nonAtomicAtLeastOnce()` — process, then save the checkpoint, two
commits. For a read model whose writes must commit in one transaction with the checkpoint, extend
`BaseProjection` over a `JdbcProjectionRepository`, declare `TRANSACTIONAL_LOCAL` and pass the same
repository as the runner's `atomicProcessor(...)` — see
[Delivery modes](guide/concepts.md#delivery-modes--what-a-projection-promises). Registering a
projection alone is not enough: you must also configure a runner (`projectionRunner(...)` or
`projectionRunnerFactory(...)`) and call `startProjections()` **on the built instance** — without a
runner, `startProjections()` is a silent no-op.

```java
import java.util.List;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.runtime.ContinuousProjectionRunner;

// Your own read-model write target (a table, a repository, an in-memory map, ...).
Projection orderSummary = batch -> {
    for (EventEnvelope envelope : batch) {
        if (envelope.event() instanceof OrderPlaced e) {
            orderSummaryRepository.upsert(e.orderId(), e.productId(), e.quantity());
        }
    }
};

var offsetStore = new PostgresOffsetStore(dataSource);   // tracks each projection's progress

StreamRune streamRune = StreamRune.builder()
    .eventStore(eventStore)
    .register(ORDER, PlaceOrder.class,   cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
    .register(ORDER, ConfirmOrder.class, cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
    .register(ORDER, CancelOrder.class,  cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
    .registerProjection("order_summary", orderSummary, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    .projectionRunnerFactory((store, subscriptionConfig) ->
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsetStore)
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce()) // required; the named non-transactional choice
            .subscriptionConfig(subscriptionConfig)
            .build())
    .build();

streamRune.startProjections();   // starts the runner; no-op without a runner factory
// ... later, on shutdown:
streamRune.stopProjections();
```

Query the read model from wherever the projection wrote it (here, `orderSummaryRepository`) — the
facade does not expose a generic projection accessor.

---

## 8. Run with Spring Boot

```groovy
// build.gradle
dependencies {
    implementation("org.streamrune:streamrune-spring:1.0.0-alpha-SNAPSHOT")
}
```

> `streamrune-spring` pulls in the rest of the framework — core, runtime, the Postgres event store,
> and the crypto backends — transitively from the same repositories, so this single coordinate is all a
> Spring Boot app needs (see [Step 1](#1-add-the-streamrune-preview-build)).

```yaml
# application.yml
streamrune:
  snapshot-every-n-events: 100
  retry-max-attempts: 3
  lock-timeout: 5s
  stripe-count: 1024
```

Inject the framework-agnostic `org.streamrune.core.CommandBus`. The Spring integration produces a
`VirtualThreadCommandBus` (which implements `CommandBus`) as a bean — it does **not** expose the
`StreamRune` facade as an injectable bean, so inject `CommandBus`, not `StreamRune`.

```java
import org.streamrune.core.CommandBus;

@RestController
@RequestMapping("/orders")
public class OrderController {
    private final CommandBus commandBus;

    public OrderController(CommandBus commandBus) {
        this.commandBus = commandBus;
    }

    @PostMapping
    public String placeOrder(@RequestBody PlaceOrder command) {
        CommandBus.CommandResult result = commandBus.execute(command);
        return result.streamId().toString();
    }
}
```

Read models are served from wherever your projections write them (a repository, a query bus); the
command side only returns a `CommandResult`.

---

## 9. Encryption-at-Rest (Crypto Engine)

StreamRune supports encrypting event payload data at rest per subject: the in-memory, filesystem
and PostgreSQL engines keep one AES-256-GCM key per subject, Vault uses one transit key per subject,
and AWS KMS encrypts every subject under a single shared CMK. Five engine implementations are
available:

| Engine | Module | Best for |
|---|---|---|
| In-Memory | `streamrune-test` | Unit tests only |
| FileSystem | `streamrune-filesystem-crypto` | Single-node deployments |
| PostgreSQL | `streamrune-postgres-crypto` | Shared key store, HA |
| HashiCorp Vault | `streamrune-vault-crypto` | Enterprise key management |
| AWS KMS | `streamrune-aws-kms-crypto` | AWS-native key management |

Each engine implements the `CryptoEngine` interface from `streamrune-core`.

#### Engine Comparison

| Engine | Key deletion | Caching | Setup |
|---|---|---|---|
| InMemory | `deleteKey` (test utility) | No | None |
| FileSystem | `deleteKey`: tombstone file, key file removed | Optional (via CachedCryptoEngine) | Directory |
| Postgres | `deleteKey`: key row deleted, tombstone row recorded | Recommended (CachedCryptoEngine) | the `db/crypto-migration` tables |
| Vault | `deleteKey`: tombstone recorded, transit key deleted | Recommended (CachedCryptoEngine) | Vault Transit engine |
| AWS KMS | `deleteKey`: tombstone only (shared CMK) | Recommended (CachedCryptoEngine) | KMS CMK |

#### Usage

```java
import java.nio.file.Path;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.filesystem.FileSystemCryptoEngine;  // or other engine
import org.streamrune.postgres.PostgresEventStoreFactory;

CryptoEngine crypto = FileSystemCryptoEngine.builder()
    .keyDirectory(Path.of("/var/streamrune/keys"))
    .build();

// The facade only threads a CryptoEngine into event stores it CONSTRUCTS. You must therefore let it
// build the store via eventStoreFactoryProvider(...) — combining a pre-built .eventStore(...) with
// .cryptoEngine(...) throws IllegalStateException at build(). The provider's lambda returns the
// FACTORY (not .create()'d); the facade calls create() on it.
StreamRune streamRune = StreamRune.builder()
    .eventStoreFactoryProvider(settings ->                // ConfiguredEventStoreFactoryProvider
        new PostgresEventStoreFactory(dataSource, settings.typeRegistry())
            .cryptoEngine(settings.cryptoEngine()))       // engine reaches the store here
    .cryptoEngine(crypto)                                 // collected into EventStoreSettings
    .registerEventType("OrderPlaced", OrderPlaced.class)
    .register(ORDER, PlaceOrder.class, cmd -> AggregateId.of(cmd.orderId()), new OrderDecider())
    .build();
```

#### Key Deletion (GDPR)

**Key deletion is part of the `CryptoEngine` interface and is terminal.** `deleteKey(subjectId)` is
the GDPR Article 17 operation, and every engine in the table above implements it. It is typically
triggered by a GDPR "right to be forgotten" request workflow — `ForgetSubjectService` (in
`streamrune-runtime`) deletes the key and then runs the registered read-model purgers — or by a
scheduled compliance job. Once `deleteKey` returns, `encrypt` for that subject throws
`SubjectForgottenException`, and decrypting its existing ciphertext throws `KeyNotFoundException`,
which the crypto-shredding module reads back as `[REDACTED]`. What each engine removes:

- **FileSystem** — writes a tombstone file, then deletes the subject's key file.
- **PostgreSQL** — deletes the `encryption_keys` row and records a `forgotten_subjects` tombstone in
  one transaction.
- **Vault** — records a tombstone in its forgotten-subject store, then sets `deletion_allowed=true`
  on the subject's transit key and deletes it. The Vault token needs `update` on
  `<mount>/keys/+/config` and `delete` on `<mount>/keys/+`; the
  [Vault module README](../streamrune-crypto/streamrune-vault-crypto/README.md) lists the full policy.
- **AWS KMS** — records a tombstone only: one shared CMK encrypts every subject, so there is no
  per-subject key to delete.

Deleting a key directly in the provider's console records no tombstone: Vault's transit engine
re-creates a deleted key on the next encrypt, and the AWS KMS CMK is shared by every subject.
`reinstate(subjectId)` lifts a tombstone for a legitimate re-registration; what the subject's
pre-erasure ciphertext reads as afterwards differs per engine (see the `CryptoEngine.reinstate`
javadoc). Call it through `ForgetSubjectService.reinstate(subjectId, requester)`, which records the
reinstate in the audit log as `GDPR_REINSTATE`; the engine method alone writes no audit record.

Concurrent-safe: key creation (`loadOrCreateKey`) and deletion are mutually exclusive per subject
(PostgreSQL: a per-subject advisory lock; filesystem: a per-subject lock within one JVM, and across
processes the tombstone is written before the key file is deleted and re-checked after every mint),
so a subject forgotten mid-request cannot have its key resurrected by a racing encrypt. On a
filesystem shared across NFS clients the cross-process re-check narrows that race but cannot close
it — see `docs/guide/advanced/gdpr-erasure.md`.

#### GDPR crypto-shredding coverage and hardening

`@Encrypted` fields are encrypted on every path that persists the record carrying them — not just
domain events — as long as that path serializes with the `CryptoEngine`:

- **Events, saga state, and dead-lettered commands.** `PostgresSagaStore` and the command DLQ
  (`dead_letter_queue` table) serialize through the same crypto-shredding Jackson module as the
  event store, so `@Encrypted` fields on `SagaState` and on a dead-lettered command are ciphertext
  at rest and unreadable after `deleteKey`. For the DLQ this holds only when the command bus
  serializes with `DeadLetterRetryRunner.createObjectMapper(cryptoEngine)`: the Spring, Quarkus and
  Micronaut buses always do, and a bus you build yourself needs it passed to `.objectMapper(...)`.
  The payload is the command record, so only the command's own `@Encrypted` components are
  encrypted (if serializing the command fails, the payload is a JSON `null`). The rest of the row
  is never encrypted: the command type, stream and aggregate ids, user, correlation and trace ids,
  idempotency key, and the failure's exception type and message. `deleteKey` does not reach those
  columns; they go only with the row — a successful replay, a discard, or the retention sweeper
  below. An entry whose subject is forgotten while it waits is never re-executed: its `@Encrypted`
  fields decrypt to `[REDACTED]`, so the replay is refused and counted as a failed attempt (the
  scan recurses into nested `@Encrypted` records, so a redacted field nested inside another record
  is still caught), and the row stays until the retention sweeper deletes it (or, with a retry
  policy that discards after its last retry, until the retries are used up).
- **A dead-lettered command is protected only by its own `@Encrypted` components.** The DLQ stores
  the command record, not the events it would have produced, so annotate the personal data on every
  command that carries it, exactly as on its events. A command without `@Encrypted` is stored in
  `dead_letter_queue.payload` as plaintext and stays readable after `deleteKey` until the retention
  sweeper deletes the row. A command bus you build yourself must serialize dead letters with
  `DeadLetterRetryRunner.createObjectMapper(cryptoEngine)`, and a retry runner you build must read
  them with the same kind of mapper (the auto-configured bus and runner already do).
- **Building the event store fails fast on misconfiguration.** If an event or state type registered
  with the event store has an `@Encrypted` field (directly or in a nested record) and no
  `CryptoEngine` is configured, `PostgresEventStore.create(...)`,
  `PostgresEventStore.builder().build()` and `PostgresEventStoreFactory.create()` throw
  `IllegalStateException` naming each such field (`CryptoConfigValidator`) before they open a
  connection, instead of silently persisting plaintext PII. A custom `EventTypeRegistry` that does
  not implement `registeredTypes()` is refused too, because it cannot be checked. The Spring, Quarkus
  and Micronaut integrations build the store through the factory and also check the registered
  command types at startup. Not checked: the `PostgresEventStore` constructors that take a
  ready-made `ObjectMapper` (build it with `PostgresEventStore.createObjectMapper(cryptoEngine,
  metrics)`), and command types registered with the plain-Java `StreamRune` builder. Configure a
  `CryptoEngine` — see [Usage](#usage) above — before registering any `@Encrypted` type.
- **OTel baggage is allowlisted, not persisted verbatim.** `EventMetadata#baggage()` is immutable
  and cannot be crypto-shredded (a `Map` field can't carry `@Encrypted`), yet OTel baggage is
  populated from the client-supplied, unauthenticated W3C `baggage:` header — without filtering, a
  caller could smuggle arbitrary PII permanently into the event log. Each framework's request
  filter allowlists baggage before it reaches request context or event metadata. The default
  allowlist covers only the framework's own key (`streamrune.causation-id`); add application keys via
  `streamrune.metadata.baggage-allowlist` (a list, added on top of the defaults). Baggage `role` is
  never taken from the `baggage:` header, even if you list it: it is the `X-User-Role` header, and
  only in the trusted-gateway identity mode (see
  [Authorization](guide/advanced/authorization.md#where-the-identity-comes-from-security)).
- **Allowlisted baggage values are bounded, not just allowlisted keys.** The allowlist is
  key-scoped — so an unauthenticated caller could send `baggage: <allowlisted key>=<8 KB of a
  victim's PII>` (or an `X-User-Role` header, which the request filters add *after* the allowlist
  runs) and have it copied verbatim into the immutable `event_stream.metadata`. A value policy applies to every retained entry, enforced both in
  `BaggageAllowlist.filter` and in `StreamRuneContext.RequestContext` (the boundary every path
  crosses, so the header merge cannot bypass it): values longer than 256 characters are **dropped**
  (not truncated — a truncated PII string is still PII), control characters are stripped, and an
  entry left blank is dropped. `X-Correlation-Id`, `X-Trace-Id` and the user id (the resolved principal, or
  the `X-User-Id` header in the trusted-gateway mode) are bounded on the **ingress door** —
  `StreamRuneContext.RequestContext.fromRequest(...)`, which every request filter calls with the
  values it just read off the wire: at most 255 characters (matching the `VARCHAR(255)` columns
  they land in) and no control characters. An over-long or control-bearing id is
  **rejected**, because a truncated identifier is a wrong identifier. *If you write your own edge
  filter, call `fromRequest` — not the constructor.* The bound is deliberately *not* enforced in
  `CorrelationId`/`TraceId` themselves: those constructors also run on the **read** path — Jackson
  rebuilds `EventMetadata` on every aggregate load, projection page and subscription delivery — so a
  bound there would turn any over-long id already at rest into a permanently unloadable aggregate
  and halt projections at that offset, while preventing no write at all. For the same reason it is
  not in the `RequestContext` *constructor* either: that is also the **reconstruction** path — the
  DLQ retry runner rebuilds a context from the persisted `dead_letter_queue` columns and saga
  dispatch rebuilds one from a `saga_id` read out of `saga_state`, and rejecting there would burn a
  DLQ entry's whole retry ladder and terminally fail an in-flight saga. Ids already in the log, the
  DLQ or the saga store are read back verbatim.
- **DLQ retention is bounded.** The `streamrune.dead-letter.retention-max-age` property (default 30
  days) prunes `dead_letter_queue` rows older than the window for any `DeadLetterQueue` bean you
  declare — even with `streamrune.dead-letter.enabled=false`, which turns off only the automatic
  retry runner — mirroring `streamrune.saga.dead-letter-retention-max-age` (see
  `docs/guide/production.md`'s "Retention and GDPR" section). Zero or negative disables pruning
  **for this property**; the same is NOT true of
  `streamrune.saga.dead-letter-retention-max-age` — zero/negative there means unbounded retention,
  which with the default inbox retention window violates `SagaRetentionValidator`'s invariant and
  makes the application refuse to start. See production.md's retention table for the details.
- **Snapshots resume after forget.** Snapshotting a forgotten subject's state writes `[REDACTED]`
  instead of throwing, so snapshot cadence resumes rather than forcing full replay from genesis on
  every load. New PII for an already-forgotten subject (not merely re-persisting redacted state)
  still throws `SubjectForgottenException`.
- **`CachedCryptoEngine` has a mandatory default TTL.** Decrypted plaintext held in the cache is
  bounded by a default 5-minute `expireAfterWrite` even if you don't configure one explicitly —
  defense-in-depth against PII lingering in heap (and heap dumps). Set `expireAfterWrite` /
  `expireAfterAccess` explicitly to override. A per-subject striped lock also closes a race where a
  decrypt in flight for a subject could re-populate the cache just after a concurrent `deleteKey`
  invalidated it.
- **Vault error mapping is precise.** Only Vault's actual "encryption key not found" 400 response
  maps to `KeyNotFoundException` (treated as forgotten); any other 400 (malformed request,
  misconfigured transit engine, etc.) maps to `CryptoOperationException` instead, so backend
  corruption can no longer masquerade as a forgotten subject.

#### Auto-Configuration

Each framework integration auto-configures crypto engines from `streamrune.crypto.*` properties:

**Spring Boot:**
```yaml
streamrune:
  crypto:
    filesystem:
      enabled: true
      key-directory: /var/streamrune/keys
    cache:
      maximum-size: 10000
      expire-after-write: 5m
```

**Quarkus:**
```properties
streamrune.crypto.postgres.enabled=true
streamrune.crypto.cache.expire-after-write=5m
```

**Micronaut:**
```yaml
streamrune:
  crypto:
    aws:
      enabled: true
      region: eu-central-1
      kms-key-id: arn:aws:kms:...
```

The AWS KMS engine records erasure tombstones in PostgreSQL (the crypto baseline's
`forgotten_subjects` table), so it needs a `DataSource` bean: without one, the engine refuses to
build and startup fails, rather than keeping tombstones in memory where a restart would lose them.

#### Quick Start with Encryption

```java
// 1. Choose an engine
Path keyDir = Path.of("/var/streamrune/keys");
CryptoEngine crypto = new CachedCryptoEngine.Builder()
    .delegate(FileSystemCryptoEngine.builder().keyDirectory(keyDir).build())
    .maximumSize(5_000)
    .expireAfterWrite(Duration.ofMinutes(10))
    .build();

// 2. Let the facade construct the store so it can thread the engine into it
//    (eventStoreFactoryProvider — see the Usage section above for why).
static final AggregateType CUSTOMER = AggregateType.of("customer");

StreamRune streamRune = StreamRune.builder()
    .eventStoreFactoryProvider(settings ->
        new PostgresEventStoreFactory(dataSource, settings.typeRegistry())
            .cryptoEngine(settings.cryptoEngine()))
    .cryptoEngine(crypto)
    .registerEventType("CustomerRegistered", CustomerRegistered.class)
    .register(CUSTOMER, RegisterCustomer.class, cmd -> AggregateId.of(cmd.customerId()), new CustomerDecider())
    .build();

// 3. Use @Encrypted on a String event field; subjectId names the sibling component
//    that holds the subject id used to derive the per-subject key.
public record CustomerRegistered(
    String customerId,
    @Encrypted(subjectId = "customerId") String email  // ← encrypted at rest
) implements DomainEvent {}
```

---

## 10. GraalVM Native Image

StreamRune runs as a GraalVM native image. The framework ships reachability metadata for its
**own** types, but **your application must register its own serialisable types** — domain events,
snapshot state records, and any value objects nested in them.

Why: StreamRune serialises every event through Jackson, and its crypto-shredding module inspects
each record via `Class.getRecordComponents()` to find `@Encrypted` components. That call throws
`UnsupportedFeatureError` in a native image unless the record's accessor methods were registered at
build time. Spring Boot AOT auto-registers types reachable from `@RestController` signatures, but
your events are reached only dynamically through the event store — so AOT cannot see them, and the
binary builds fine but crashes on the first event append.

Register them with a `RuntimeHintsRegistrar` that calls the framework helper, wired via
`@ImportRuntimeHints`:

```java
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.streamrune.spring.StreamRuneRuntimeHints;

public class MyAppNativeHints implements RuntimeHintsRegistrar {
    @Override
    public void registerHints(RuntimeHints hints, ClassLoader classLoader) {
        // Option A — scan your domain packages (new events are covered automatically):
        StreamRuneRuntimeHints.registerDomainPackages(hints, classLoader, "com.example.shop.domain");

        // Option B — list the types explicitly, for full control:
        // StreamRuneRuntimeHints.registerSerializableTypes(hints, List.of(OrderPlaced.class));
    }
}

@Configuration
@ImportRuntimeHints(MyAppNativeHints.class)
class NativeHintsConfig {}
```

`registerDomainPackages` scans the named packages at build time and registers every concrete type
(records, enums, value objects) with the reflection categories Jackson and the crypto module need,
plus every sealed interface and sealed abstract class (see below). `registerSerializableTypes` does
the same for an explicit list. Over-registering a never-serialised type is harmless.

**Sealed hierarchies must be registered at the sealed type itself.** StreamRune walks sealed
hierarchies through `Class.getPermittedSubclasses()`: `DeadLetterRetryRunner.Builder.registerCommand(OrderCommand.class)`
registers every command the root permits (a dead-letter entry names the concrete command class),
and the authorization and `@Encrypted` startup checks inspect every permitted subtype. A GraalVM
native image answers that call only for a sealed type that is registered for reflection; for any
other it reports the type as sealed with **no** permitted subclasses (measured on GraalVM CE
25.0.1 — registering only the subclasses does not help, and every nested sealed level needs its own
registration). StreamRune does not treat such a type as a leaf: the walk refuses it with an
`IllegalStateException` naming the type, so the application fails at startup instead of losing the
hierarchy silently. `registerDomainPackages` registers the sealed types of the scanned packages for
you; with `registerSerializableTypes`, list the sealed roots and their nested sealed levels too. (An
image built with `--exact-reachability-metadata` throws GraalVM's own
`MissingReflectionRegistrationError` from the same call instead.)

**Schema auto-initialization.** The factory looks up each Flyway migration script it ships by its
exact name rather than letting Flyway list the migration directory: Flyway cannot list a directory
in a native image, so it would find no migration and startup would then refuse every framework
table as missing. The scripts must therefore be in the image. The Spring, Quarkus and Micronaut
integrations register `db/streamrune-migration/*.sql`, `db/crypto-migration/*.sql` and Flyway's
own `org/flywaydb/core/internal/version.txt` as resources, and `streamrune-postgres` adds the build
argument `--enable-url-protocols=https`, without which Flyway cannot construct itself in a native
image (it builds its help links as `https` URLs; nothing is downloaded). Flyway also reaches
parts of itself by name at run time: it finds its plugins with `ServiceLoader`, copies its
configuration extensions field by field, and instantiates its log creator from a class name (the
factory tells it to log through SLF4J, so the back end does not depend on what the image registers).
The Quarkus and Micronaut integrations ship the reachability metadata for that
(`META-INF/native-image/org.streamrune/streamrune-quarkus-flyway` and `streamrune-micronaut-flyway`);
a Spring Boot build takes it from the GraalVM reachability-metadata repository, which
native-build-tools applies. A plain-Java native application registers the resources in its own
`reachability-metadata.json`:

```json
{
  "resources": [
    { "glob": "db/streamrune-migration/*.sql" },
    { "glob": "db/crypto-migration/*.sql" },
    { "glob": "org/flywaydb/core/internal/version.txt" }
  ]
}
```

A script missing from the image fails `initializeSchema()` and `create()` with an
`IllegalStateException` that names it, before anything is written. Your application's own Flyway
migrations are yours to register.

**Projections that declare `process(List, ProjectionRepository)` themselves.** For a
`TRANSACTIONAL_LOCAL` or `EXTERNAL_EFFECT` registration under a transactional processor (the
`JdbcProjectionRepository` bean), and for an `AT_LEAST_ONCE_IDEMPOTENT` registration whose
`writeTarget()` is that processor's repository (only to choose the startup INFO line's verdict), the
runner asks `Projection#writesThroughRepository()`, which resolves that method with
`getClass().getMethod(...)`. All three integrations register `Projection` and `BaseProjection`
for it; an application projection that declares the two-argument overload itself must be
registered too, or the image resolves the inherited default and the runner refuses a transactional
registration as not writing through its repository (an at-least-once one only loses the INFO
line's hint). `registerDomainPackages` covers it when the projection lives in a scanned package.

**Quarkus.** Register your records with `@RegisterForReflection(targets = {...})` on a class of
your application; `StreamRuneReflectionConfig` and the HikariCP metadata the Quarkus integration
ships cover the framework's own types. Sealed interfaces are the exception: `@RegisterForReflection`
on a sealed type does **not** make the image list its permitted subclasses (measured on Quarkus 3.32:
it writes a legacy `reflect-config.json` entry without `allPermittedSubclasses`, and the binary still
refused the root). List each sealed root, and each nested sealed level, in your own
`src/main/resources/META-INF/native-image/<groupId>/<artifactId>/reachability-metadata.json`:

```json
{
  "reflection": [
    { "type": "com.example.shop.domain.OrderCommand" },
    { "type": "com.example.shop.domain.OrderEvent" }
  ]
}
```

**Micronaut.** Register your records in a `reflect-config.json` of your own (or with `@TypeHint` /
`@ReflectiveAccess`); the `reflect-config.json` the Micronaut integration ships covers the
framework's own types. Sealed interfaces need the same `reachability-metadata.json` entries as on
Quarkus: `@TypeHint` and `@ReflectiveAccess` register a type through
`RuntimeReflection.register(Class)`, which leaves its permitted subclasses unregistered (measured on
Micronaut 4.10: with all ten demo roots named in an `@TypeHint` the binary still refused the root at
startup).

**Micronaut: build the image with `-H:+SharedArenaSupport`.** Micronaut 4.10's HTTP server runs on
Netty 4.2, which on Java 25 frees each direct buffer it allocates outside its pool through
`Arena.ofShared().close()`: a buffer above 1 MiB (a large request or response body) as soon as it is
released, and every pooled chunk when an event loop exits at shutdown. A GraalVM 25 native image
supports shared arenas only when built with `-H:+SharedArenaSupport`, an experimental option that is
off by default. Without it each such free throws `UnsupportedFeatureError: Support for
Arena.ofShared is not active` on the event loop, and the memory is never returned. Measured with the
demo's Micronaut app on GraalVM CE 25.0.1: a request with a 1.5 MiB body answered 500, and six
event-loop threads threw at shutdown. Netty's own fallback does not apply in an image, because `netty-common`
initializes the class that probes for shared-arena support (`CleanerJava25`) at image build time,
so the probe runs on the build JVM, which supports shared arenas. Add the option to the native
build (native-build-tools, Gradle):

```kotlin
graalvmNative {
    binaries {
        named("main") {
            buildArgs.addAll(
                "-H:+UnlockExperimentalVMOptions",
                "-H:+SharedArenaSupport",
                "-H:-UnlockExperimentalVMOptions",
            )
        }
    }
}
```

StreamRune itself allocates no shared arena. The requirement comes with Netty 4.2 on Java 25 in any
native image, not with Micronaut as such. The demo's Spring app (Tomcat) and Quarkus app (Netty 4.1)
do not take this path.

> One more native-build note for Spring Boot: do **not** disable the `jar` task. Spring Boot's AOT
> puts your module's classes on the native-image classpath via the plain `-plain.jar` artifact, so
> disabling `jar` makes `nativeCompile` fail with *"Main entry point class … neither found"*.

---

## Next Steps

| Topic | Where to learn more |
|---|---|
| Architecture & concepts | [streamrune-core/README.md](../streamrune-core/README.md) |
| Command bus & projections | [streamrune-runtime/README.md](../streamrune-runtime/README.md) |
| PostgreSQL event store | [streamrune-eventstore/streamrune-postgres/README.md](../streamrune-eventstore/streamrune-postgres/README.md) |
| Encryption (GDPR) | [streamrune-crypto/README.md](../streamrune-crypto/README.md) |
| Spring / Quarkus / Micronaut | [streamrune-integration/README.md](../streamrune-integration/README.md) |
| Full CQRS/ES demo | [streamrune-ecommerce-demo](https://github.com/StreamRune/streamrune-ecommerce-demo) (separate repository) |
