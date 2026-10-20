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

Thread-safe in-memory implementation of `EventStore` that mirrors `PostgresEventStore`'s observable semantics: strict optimistic locking, serialized global ordering, snapshots with schema versions (`withMigrations(...)` for snapshot migrations) and upcasting on read (`withUpcasters(...)`). `InMemoryEventStore.serializing(typeRegistry, cryptoEngine)` stores JSON instead of object references, so `@Encrypted` fields, crypto-shredding and type registration are exercised too. Use in unit tests — events are lost on JVM shutdown. `withCommandInbox(inbox).withSagaStore(sagaStore)` wires the command inbox and the `InMemorySagaStore` whose entries a saga's forward command is fenced against, as `PostgresEventStore` fences it against `saga_state`: a keyed append with a saga origin is refused (`SagaForwardFencedException`, nothing stored, key unclaimed) once that saga's entry carries a compensation episode stamp, is terminal or is gone.

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
| `InMemoryCommandBus` | Minimal `CommandBus` (no locking, retries, interceptors or metrics) that runs each decider's `guard` before `decide`, like the runtime bus, and refuses a dispatch bound to a reserved correlation id that is not the bound saga origin's (`ReservedCorrelationIdException`), like every shipped bus. Test a listener that dispatches commands against it rather than against a hand-written `CommandBus` double: a double makes that check only when its `execute` begins with `StreamRuneContext.requireNoForeignReservedCorrelationId()` |
| `InMemoryQueryBus` | Minimal `QueryBus`; makes the same check of the bound correlation id before it looks up a handler |
| `InMemoryProjectionRepository` | A `ProjectionRepository`, a fencing-capable `AtomicBatchProcessor` and the `OffsetStore` of its own checkpoint in one object: pass the same instance as the projection's repository and the runner's `offsetStore(...)`/`atomicProcessor(...)` to run a `TRANSACTIONAL_LOCAL` projection, rollback included, without PostgreSQL |
| `InMemoryOffsetStore` | `OffsetStore` for a runner on `AtomicBatchProcessor.nonAtomicAtLeastOnce()` (`AT_LEAST_ONCE_IDEMPOTENT` projections); mirrors the monotonic guard |
| `AtomicBatchProcessorContract` | Abstract JUnit contract suite of what `AtomicBatchProcessor.supportsFencing()` promises; `InMemoryProjectionRepository` and `JdbcProjectionRepository` pass it — extend it to prove a custom transactional processor |
| `SagaEnvelopes` | `metadataCausedBy(stateType, sagaId)` and `causedBy(stateType, sagaId, streamId, event)` build the metadata and the envelope of an event a saga's own command produced (`EventMetadata.sagaOrigin` at hop 0), to feed a saga, a projection or a listener under test. `metadataOfContinuation(cause)` and `continuationOf(cause, streamId, event)` build those of an event a listener's command produced inside `StreamRuneContext.continuing(cause)` — the origin of `cause` one hop further, the id of `cause` as `causationId` — taken from `InMemoryCommandBus` under the bindings of `continuing`, and `causedBy(stateType, sagaId, hop, streamId, event)` chains `hop` continuations. A `SagaRunner` routes all of them alike: the hop takes no part in routing. An envelope sits at global offset 0: append it to the event store first (or build an envelope at a later offset around `metadataCausedBy`) before handing it to a `SagaRunner` that already applied an event, which skips offsets it has passed |
| `CommandBusProvenanceContract` | Abstract JUnit contract suite of where a `CommandBus` takes `EventMetadata.sagaOrigin` and `causationId` from (the bound `StreamRuneContext.CAUSE` and nothing else: with no cause bound an event carries neither, whatever its baggage holds) and of what a command executed inside `StreamRuneContext.continuing(event)` stores (the event's id as `causationId`, its origin one hop further, no user). `InMemoryCommandBus` and `VirtualThreadCommandBus` pass it — extend it and implement `newBus(eventStore)` for a custom bus. What it requires of the bus: (1) with no cause bound, no stored event carries an origin or a causation id, whatever the correlation id, baggage, user, captured authority or saga flag; (2) a bound cause decides both for every stored event; (3) inside `continuing` the bus stores what that helper binds; (4) with a saga origin bound the stored correlation id is the origin's reserved one, whatever id the bound context holds, so a stored event has a reserved correlation id if and only if it has an origin; (5) a dispatch bound to a reserved correlation id that is not the bound origin's — no origin, or another saga's — throws `ReservedCorrelationIdException` and stores nothing, also inside a continuation of an event that holds a reserved correlation id and no origin (call `StreamRuneContext.requireNoForeignReservedCorrelationId()` first); (6) a task wrapped with `StreamRuneContext.wrap` inside a saga dispatch executes on another thread without being refused; (7) a context bound to `null` is no context — the command executes with no user, a non-reserved correlation id of the bus's own and no origin |
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
