# StreamRune Test

In-memory event store and test fixtures for StreamRune.

## Overview

Provides `InMemoryEventStore` for unit tests, fixtures that simplify testing deciders, projections and sagas, in-memory doubles of the other framework stores and buses (`InMemoryCommandBus`, `InMemoryQueryBus`, `InMemorySagaStore`, `InMemoryDeadLetterQueue`, `InMemoryCryptoEngine`, …), forwarding stores that inject faults, and recording doubles of the metrics port and the leadership.

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
| `SagaForwardFenceContract` | Abstract JUnit contract suite of an `EventStore` that fences a saga's forward command against the rows of a `SagaStore`; extend it to prove a custom event store |
| `OutboxStoreContract` | Abstract JUnit contract suite every `OutboxStore` honours (status transitions, both ordering modes, retention, replay); `InMemoryOutboxStore` and `PostgresOutboxStore` pass it |
| `InMemoryOutboxStore` | `OutboxStore` with the claim, lease and ordering rules of `PostgresOutboxStore`; retry times come from the `Clock` it is given |
| `FaultSchedule` | The faults a forwarding store injects, by the Java name of the method: `failOnce`, `failAt(method, n)` and `failAlways` throw before the delegate runs, `crashAfter` lets the delegate write and then throws (a crash between two steps); `throwing(supplier)` names the failure, `calls(method)` and `calls()` count every call, failed ones included. A name the decorated interface does not have is refused when the decorator is built |
| `InjectedFaultException` | What a `FaultSchedule` throws unless `throwing(...)` names another failure; its message names the method, the call and whether the delegate ran |
| `ForwardingEventStore`, `ForwardingSagaStore`, `ForwardingSagaDeadLetterStore`, `ForwardingDeadLetterQueue`, `ForwardingOffsetStore`, `ForwardingProjectionDeadLetterStore` | A store that forwards every method of its interface — the default methods included, so `appendWithKey`, `load(id, int)`, `lastGlobalOffset()` and the counts reach the delegate, never a throwing default — and consults its `FaultSchedule` on each call: `new ForwardingEventStore(store, FaultSchedule.none().crashAfter("append"))`. Subclass one to override a single call |
| `ForwardingOutboxStore` | `OutboxStore` that forwards every call to a delegate, with a `FaultSchedule` like the other forwarding stores (`new ForwardingOutboxStore(store, schedule)`); or subclass it and override the one call a test makes fail, block or record |
| `RecordingStreamRuneMetrics` | `StreamRuneMetrics` that records every call under its `MetricNames` meter, with the tags it carries and its amount: `count`, `total`, `lastGauge` (a supplied gauge is read when asked), `samples` and `assertIncremented(meter, tagKey, tagValue, …)`, which fails listing what was recorded. A meter name or tag key outside `MetricNames` is refused |
| `ScriptedSubscriptionLeadership` | `SubscriptionLeadership` the test scripts: it denies until `grant()`, keeps the held leases under `deny()`, drops them on `revoke()` (the next `current()` reports the loss), hands out a greater epoch for every acquisition, and counts `attempts()`, `acquisitions()` and `resignations()` |
| `SagaFixture` | Given events, when an event, then the commands a saga dispatched and the state it keeps: `SagaFixture.of(State.class, builder -> builder.orchestrator(saga))` builds the `SagaRunner` over in-memory stores, a bus that records the commands and executes none, and a `MutableClock`; `given`, `when`, `replyingTo`, `redeliver`, `advance(...).runTimeouts()`, `expectCommands`, `expectState`, `expectStatus`, `expectCompensating`, `expectCompleted`, `expectDeadLettered`; `failing(commandType, failure)` rejects a command, `sagaStoreFaults()` scripts the saga store |
| `EventStreamFixtures` | Event streams in files: `write(store, file)` records every event of an `InMemoryEventStore` as JSON (stream, version, type, schema version, payload, metadata), `read(file)` parses it, `appendTo(store, events)` replays it into a fresh serializing store — the store's registry resolves each type, its upcasters upcast an older payload — and a stream written again is the same file |
| `InMemoryCommandInbox` | `CommandInbox`; `InMemoryEventStore.withCommandInbox(inbox)` claims a key and appends under one lock |
| `InMemorySagaDeadLetterStore` | `SagaDeadLetterStore` whose `publish` is an upsert per saga and event offset, as in PostgreSQL |
| `InMemoryProjectionDeadLetterStore` | `ProjectionDeadLetterStore`; `all()` returns every stored entry |
| `InMemoryEventSubscription` | `EventSubscription` with the at-most-once notification semantics of PostgreSQL `LISTEN`/`NOTIFY`, for push-driven wiring without Docker |
| `MutableClock` | `Clock` the test sets and advances (`startingAt(instant)`, `advance(duration)`): backoff, timeouts and timestamps without sleeping |
| `EventPayloadAssertions` | `assertRoundTrips(event)` and `assertUpcastBinds(upcaster, payload, fromVersion, eventClass)` bind payloads strictly, as the event stores do |
| `NativeReflectionTypes` | Derives from code the framework types a GraalVM native image must register for reflection; the integrations' native-hint tests compare their hints with it |
| `ShippedMigrationScripts` | Lists the shipped Flyway migration scripts from the class path, for the native-image resource tests |
| `UnregisteredSealedTypes` | Builds a sealed type shaped like one a native image did not register (`isSealed()` with no permitted subclasses) |

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

## SagaFixture example

```java
SagaFixture<OrderSaga.State> saga =
    SagaFixture.of(OrderSaga.State.class, builder -> builder.orchestrator(new OrderSaga()));
SagaId orderSaga = SagaId.of("order-saga-1");

saga.when(StreamId.of(AggregateType.of("order"), AggregateId.of("1")), new OrderPlaced("1"))
    .expectCommands(new ReserveStock("1"))
    .replyingTo(orderSaga, new StockReserved("1"))
    .expectCompleted(orderSaga);
```

Each step stores its events at the next global offset and delivers them through the runner's
`asEventListener()`; `expectCommands` reads the commands of the latest step. The saga guide and the
testing guide show the timeout runner (`withTimeouts`, `advance`, `runTimeouts`) and a crash point
(`sagaStoreFaults().crashAfter(...)`, `redeliver()`).

## A crash point with FaultSchedule

```java
var faults = FaultSchedule.none().crashAfter("saveOffset");
var offsets = new ForwardingOffsetStore(new InMemoryOffsetStore(), faults);
// ... the subscription delivers a page and saves its checkpoint; the save is stored and then
// InjectedFaultException stops the flow, as a crash after the write would ...
// restart the subscription over the same stores and assert that the projection converges
assertEquals(1, faults.calls("saveOffset"));
```

## Requirements

- Java 25
- JUnit 5 (`junit-jupiter-api`, exposed as an `api` dependency)
- Depends on `streamrune-core`, `streamrune-runtime` (for `SagaFixture`) and `streamrune-crypto-api`, plus Jackson

## Known Limitations

- **`InMemoryEventStore` is not a real EventStore** — use it only in tests. It is in-process only: nothing is durable and nothing is shared across processes, and it does not model the single-writer append-throughput ceiling of `PostgresEventStore`.
