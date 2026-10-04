# StreamRune Test

In-memory event store and test fixtures for StreamRune.

## Overview

Provides `InMemoryEventStore` for unit tests, fixtures that simplify testing deciders and projections, and in-memory doubles of the other framework stores and buses (`InMemoryCommandBus`, `InMemoryQueryBus`, `InMemorySagaStore`, `InMemoryDeadLetterQueue`, `InMemoryCryptoEngine`, …).

## Installation

```groovy
// build.gradle
testImplementation("org.streamrune:streamrune-test:1.0.0-alpha-SNAPSHOT")
```

```kotlin
// build.gradle.kts
testImplementation("org.streamrune:streamrune-test:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../README.md#preview-builds). See
> the [CHANGELOG](../CHANGELOG.md) for what the preview contains.

## InMemoryEventStore

Thread-safe in-memory implementation of `EventStore` that mirrors `PostgresEventStore`'s observable semantics: strict optimistic locking, serialized global ordering, snapshots with schema versions (`withMigrations(...)` for snapshot migrations) and upcasting on read (`withUpcasters(...)`). `InMemoryEventStore.serializing(typeRegistry, cryptoEngine)` stores JSON instead of object references, so `@Encrypted` fields, crypto-shredding and type registration are exercised too. Use in unit tests — events are lost on JVM shutdown.

```java
var store = new InMemoryEventStore();

StreamId streamId = StreamId.of(AggregateType.of("order"), AggregateId.of("order-1"));
// EventStoreFixture.event(...) fills in test metadata; the store assigns version and offset on append.
var envelope = EventStoreFixture.event(
    streamId, new Version(1), new EventType("OrderPlaced"),
    new OrderPlaced("order-1", "SKU-A", 2));

store.append(streamId, List.of(envelope), Version.initial());
var history = store.load(streamId);

assertEquals(1, history.events().size());
```

## Test Fixtures

| Fixture | Purpose |
|---|---|
| `EventStoreFixture` | Given/when/then for event-store appends; `event(...)` builds test envelopes |
| `DeciderFixture` | Given/when/then testing for deciders |
| `ProjectionFixture` | Test projections with events fed in sequence |
| `InMemoryEventStoreFactory` | `EventStoreFactory` for DI environments |
| `InMemoryCommandBus` | Minimal `CommandBus` (no locking, retries, interceptors or metrics) that runs each decider's `guard` before `decide`, like the runtime bus |
| `InMemoryProjectionRepository` | A `ProjectionRepository`, a fencing-capable `AtomicBatchProcessor` and the `OffsetStore` of its own checkpoint in one object: pass the same instance as the projection's repository and the runner's `offsetStore(...)`/`atomicProcessor(...)` to run a `TRANSACTIONAL_LOCAL` projection, rollback included, without PostgreSQL |
| `InMemoryOffsetStore` | `OffsetStore` for a runner on `AtomicBatchProcessor.nonAtomicAtLeastOnce()` (`AT_LEAST_ONCE_IDEMPOTENT` projections); mirrors the monotonic guard |
| `AtomicBatchProcessorContract` | Abstract JUnit contract suite of what `AtomicBatchProcessor.supportsFencing()` promises; `InMemoryProjectionRepository` and `JdbcProjectionRepository` pass it — extend it to prove a custom transactional processor |
| `SagaStoreContract`, `SagaDeadLetterStoreContract` | Abstract JUnit contract suites the shipped saga stores pass; extend one and implement its `newStore(...)` factory method(s) to run it against a custom `SagaStore` / `SagaDeadLetterStore` |

## DeciderFixture example

```java
DeciderFixture.of(new OrderDecider())
    .given()                                            // prior events, folded in with evolve()
    .when(new PlaceOrder("order-1", "SKU-A", 2))
    .expectEvents(new OrderPlaced("order-1", "SKU-A", 2))
    .expectState(state -> assertEquals("SKU-A", state.product()));

DeciderFixture.of(new OrderDecider())
    .given(new OrderPlaced("order-1", "SKU-A", 2))
    .when(new PlaceOrder("order-1", "SKU-A", 2))
    .expectNoEvents();
```

`given(E...)` takes events, `when(C)` runs `decide`, and the `expect...` methods (`expectEvents`,
`expectNoEvents`, `expectException`, `expectFailedWith`, `expectState`, `expectStateMatches`,
`expectProduced`) assert with JUnit and chain. `DeciderFixture` calls `decide` only, not `guard` —
test authorization through `InMemoryCommandBus`.

## Requirements

- Java 25
- JUnit 5 (`junit-jupiter-api`, exposed as an `api` dependency)
- Depends on `streamrune-core` and `streamrune-crypto-api`, plus Jackson

## Known Limitations

- **`InMemoryEventStore` is not a real EventStore** — use it only in tests. It is in-process only: nothing is durable and nothing is shared across processes, and it does not model the single-writer append-throughput ceiling of `PostgresEventStore`.
