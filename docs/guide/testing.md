# Testing StreamRune Applications

StreamRune is designed for testability. Because `Decider` is a pure function, unit tests
need no framework setup, no Spring context, and no database — they start in milliseconds.
This guide covers all test layers from fast unit tests to full TestContainers integration
tests.

---

## Unit Testing Deciders

`Decider.decide` and `Decider.evolve` are plain Java methods. Call them directly.

```java
package com.example.order;

import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainException;

class OrderDeciderTest {

  private final OrderDecider decider = new OrderDecider();

  // --- Happy path ---

  @Test
  void createOrder_emitsOrderCreated() {
    var cmd   = new OrderCommand.CreateOrder("ord-1", "cust-42");
    var state = decider.initialState();

    List<OrderEvent> events = decider.decide(cmd, state);

    assertEquals(1, events.size());
    assertInstanceOf(OrderEvent.OrderCreated.class, events.getFirst());

    var created = (OrderEvent.OrderCreated) events.getFirst();
    assertEquals("ord-1",   created.orderId());
    assertEquals("cust-42", created.customerId());
  }

  @Test
  void confirmOrder_emitsOrderConfirmed_whenStateIsNew() {
    var state = decider.evolve(
        decider.initialState(),
        new OrderEvent.OrderCreated("ord-1", "cust-42"));

    List<OrderEvent> events = decider.decide(
        new OrderCommand.ConfirmOrder("ord-1"), state);

    assertEquals(1, events.size());
    assertInstanceOf(OrderEvent.OrderConfirmed.class, events.getFirst());
  }

  // --- Business rule rejection ---

  @Test
  void confirmOrder_throwsDomainException_whenAlreadyConfirmed() {
    // Bring state to CONFIRMED
    var state = decider.initialState();
    state = decider.evolve(state, new OrderEvent.OrderCreated("ord-1", "cust-42"));
    state = decider.evolve(state, new OrderEvent.OrderConfirmed("ord-1"));

    var confirmedState = state;
    var cmd = new OrderCommand.ConfirmOrder("ord-1");

    assertThrows(DomainException.class, () -> decider.decide(cmd, confirmedState));
  }

  // --- Evolve / state transitions ---

  @Test
  void evolve_setsStatusToConfirmed() {
    var state = decider.initialState();
    state = decider.evolve(state, new OrderEvent.OrderCreated("ord-1", "cust-42"));
    state = decider.evolve(state, new OrderEvent.OrderConfirmed("ord-1"));

    assertEquals(OrderStatus.CONFIRMED, state.status());
  }

  @Test
  void evolve_setsStatusToCancelled() {
    var state = decider.initialState();
    state = decider.evolve(state, new OrderEvent.OrderCreated("ord-1", "cust-42"));
    state = decider.evolve(state, new OrderEvent.OrderCancelled("ord-1", "customer request"));

    assertEquals(OrderStatus.CANCELLED, state.status());
  }

  @Test
  void initialState_hasNoOrderId() {
    assertNull(decider.initialState().orderId());
  }
}
```

No Spring context. No mock framework needed. The entire suite runs in under 50 ms.

### The same tests with DeciderFixture

`DeciderFixture` (in `streamrune-test`) wraps that pattern in a given/when/then DSL: `given(...)`
folds events into the decider's initial state with `evolve`, `when(command)` runs `decide` and
captures the produced events — or the thrown exception — and the `expect*` methods assert on them.
It calls `decide` and `evolve` only, not the decider's `guard` hook, so it tests business rules
rather than authorization.

```java
import static org.junit.jupiter.api.Assertions.assertEquals;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainException;
import org.streamrune.test.DeciderFixture;

class OrderDeciderFixtureTest {

  @Test
  void confirm_emitsOrderConfirmed() {
    DeciderFixture.of(new OrderDecider())
        .given(new OrderEvent.OrderCreated("ord-1", "cust-42"))
        .when(new OrderCommand.ConfirmOrder("ord-1"))
        .expectEvents(new OrderEvent.OrderConfirmed("ord-1"))
        .expectState(state -> assertEquals(OrderStatus.CONFIRMED, state.status()));
  }

  @Test
  void confirmTwice_isRejected() {
    DeciderFixture.of(new OrderDecider())
        .given(new OrderEvent.OrderCreated("ord-1", "cust-42"),
               new OrderEvent.OrderConfirmed("ord-1"))
        .when(new OrderCommand.ConfirmOrder("ord-1"))
        .expectFailedWith(DomainException.class, "Cannot confirm");
  }
}
```

The other assertions are `expectNoEvents()`, `expectException(type)`, `expectStateMatches(expected)`
and `expectProduced(eventsAssertion, stateAssertion)`; `givenEvents(...)` and `whenCommand(...)` are
aliases of `given(...)` and `when(...)`.

---

## Integration Testing with InMemoryEventStore

Use `InMemoryEventStore` from the `streamrune-test` module to run end-to-end command
pipelines without a database. Create a fresh store in `@BeforeEach` so tests do not
share state.

```java
package com.example.order;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.DomainException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.StreamRune;
import org.streamrune.test.InMemoryEventStore;

class OrderIntegrationTest {

  private static final AggregateType ORDER = AggregateType.of("order");

  private StreamRune streamRune;

  @BeforeEach
  void setUp() {
    // One registration for the whole sealed OrderCommand hierarchy: register(...) takes a
    // Decider<C, ?, ?> for the registered Class<C>, so the class must be OrderCommand — the
    // decider's own command type — not one of its records. The registration also names the
    // aggregate type, so the events land in streams like "order:ord-1".
    streamRune = StreamRune.builder()
        .eventStore(new InMemoryEventStore())
        .register(ORDER, OrderCommand.class, cmd -> AggregateId.of(switch (cmd) {
              case OrderCommand.CreateOrder c  -> c.orderId();
              case OrderCommand.ConfirmOrder c -> c.orderId();
              case OrderCommand.CancelOrder c  -> c.orderId();
            }), new OrderDecider())
        .build();
  }

  // --- Happy path: create then confirm ---

  @Test
  void createAndConfirm_succeedInSequence() {
    streamRune.execute(new OrderCommand.CreateOrder("ord-1", "cust-42"));

    CommandResult result = streamRune.execute(
        new OrderCommand.ConfirmOrder("ord-1"));

    assertEquals(1, result.events().size());
    assertInstanceOf(OrderEvent.OrderConfirmed.class, result.events().getFirst());
  }

  // --- Failure path: business rule violation ---

  @Test
  void cancelTwice_throwsDomainException() {
    // Create then cancel — order is now CANCELLED.
    // A second cancel must be rejected by the business rule.
    streamRune.execute(new OrderCommand.CreateOrder("ord-3", "cust-77"));
    streamRune.execute(new OrderCommand.CancelOrder("ord-3", "changed my mind"));

    assertThrows(DomainException.class, () ->
        streamRune.execute(new OrderCommand.CancelOrder("ord-3", "again")));
  }

  @Test
  void doubleConfirm_throwsDomainException() {
    streamRune.execute(new OrderCommand.CreateOrder("ord-2", "cust-99"));
    streamRune.execute(new OrderCommand.ConfirmOrder("ord-2"));

    assertThrows(DomainException.class, () ->
        streamRune.execute(new OrderCommand.ConfirmOrder("ord-2")));
  }
}
```

---

## Using EventStoreFixture

`EventStoreFixture` provides a fluent given/when/then DSL for testing event store
operations directly — optimistic lock checks, event counts, and append semantics —
without going through the command bus.

The fixture is available from `streamrune-test`. The static `event(...)` factory takes a
typed `EventType` and a typed `Version` (there is no bare-int or bare-String overload):

```java
import java.util.List;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

AggregateType ORDER = AggregateType.of("order");   // declared once; the examples below use it
```

Each stage executes against the store immediately: `given().append(...)` seeds state and
lets failures propagate as setup errors; `when().append(...)` captures the outcome and
returns the `ThenStage` directly (there is no separate `.then()` call).

### Example 1: Testing optimistic lock exception

When two concurrent appends target the same stream version, the second must be rejected
with `OptimisticLockException`:

```java
@Test
void append_throwsOptimisticLockException_onVersionConflict() {
  var streamId = StreamId.of(ORDER, AggregateId.of("ord-1"));

  // given: one event already appended (stream now at version 1)
  // when:  a second append claims the stale initial version
  // then:  OptimisticLockException is thrown
  EventStoreFixture.store()
      .given()
          .append(streamId, List.of(
              EventStoreFixture.event(streamId, new Version(1), EventType.of("OrderCreated"),
                  new OrderEvent.OrderCreated("ord-1", "cust-1"))))
      .when()
          .append(streamId, List.of(
              EventStoreFixture.event(streamId, new Version(1), EventType.of("OrderCreated"),
                  new OrderEvent.OrderCreated("ord-1", "cust-2"))),
              Version.initial())  // stale expected version
          .expectOptimisticLockException();
}
```

### Example 2: Testing event count after append

After two sequential appends the stream should contain exactly two events. The second
append expects the stream to be at version 1 (the version left by the seeded event):

```java
@Test
void append_persistsEvents_andCountMatches() {
  var streamId = StreamId.of(ORDER, AggregateId.of("ord-2"));

  EventStoreFixture.store()
      .given()
          .append(streamId, List.of(
              EventStoreFixture.event(streamId, new Version(1), EventType.of("OrderCreated"),
                  new OrderEvent.OrderCreated("ord-2", "cust-3"))))
      .when()
          .append(streamId, List.of(
              EventStoreFixture.event(streamId, new Version(2), EventType.of("OrderConfirmed"),
                  new OrderEvent.OrderConfirmed("ord-2"))),
              new Version(1))
          .eventsPersisted()
          .expectEventCount(streamId, 2);
}
```

### Example 3: Asserting the loaded version

`when().load(streamId)` captures the aggregate history so the `then` stage can assert its
version or inspect it directly:

```java
@Test
void load_reportsCurrentVersion() {
  var streamId = StreamId.of(ORDER, AggregateId.of("ord-4"));

  EventStoreFixture.store()
      .given()
          .append(streamId, List.of(
              EventStoreFixture.event(streamId, new Version(1), EventType.of("OrderCreated"),
                  new OrderEvent.OrderCreated("ord-4", "cust-9"))))
      .when()
          .load(streamId)
          .expectLoadedVersion(1);
}
```

The `ThenStage` methods:

| Method | Description |
|---|---|
| `eventsPersisted()` | Asserts the `when().append(...)` succeeded (no exception) |
| `expectOptimisticLockException()` | Asserts the `when().append(...)` threw `OptimisticLockException` |
| `expectEventCount(streamId, n)` | Loads the stream and asserts it contains exactly `n` events |
| `expectLoadedVersion(v)` | Asserts the version of the history captured by `when().load(...)` |
| `loadedHistory()` | Returns the `AggregateHistory` captured by `when().load(...)` for custom assertions |
| `getEventStore()` | Returns the underlying store for custom assertions |

---

## Integration Testing with TestContainers

For tests that must run against real PostgreSQL semantics (transaction isolation, advisory
locks, Flyway migrations) use the TestContainers JUnit 5 extension.

Start the PostgreSQL major version you run in production. StreamRune requires 17 or newer — an
older server is refused at startup — so the image below is `postgres:17-alpine`.

### Gradle dependencies

```kotlin
testImplementation("org.testcontainers:junit-jupiter:1.20.4")
testImplementation("org.testcontainers:postgresql:1.20.4")
testImplementation("org.postgresql:postgresql:42.7.4")
```

### Test class skeleton

```java
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class OrderPostgresIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>("postgres:17-alpine")
          .withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initSchema() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    // StreamRune ships Flyway migrations in streamrune-postgres.
    // Use PostgresEventStoreFactory which runs migrations automatically.
    // Or run them manually as the existing test suite does.
  }

  @BeforeEach
  void setUp() {
    // Construct PostgresEventStore from dataSource, then wire StreamRune.builder()
    // exactly as shown in the getting-started guide, swapping InMemoryEventStore
    // for the Postgres implementation.
  }

  @Test
  void createAndConfirm_persistAcrossRestarts() {
    // assertions here use the real Postgres store
  }
}
```

See the existing tests in
`streamrune-eventstore/streamrune-postgres/src/test/java/org/streamrune/postgres/`
for the full pattern including Flyway migration setup.

---

## Testing Projections

Projections implement a single method: `process(List<EventEnvelope>)`. Test them
directly — no runner needed.

### Build test envelopes

```java
import org.streamrune.test.EventStoreFixture;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

AggregateType ORDER = AggregateType.of("order");
StreamId streamId = StreamId.of(ORDER, AggregateId.of("ord-1"));

var envelope = EventStoreFixture.event(
    streamId, new Version(1), EventType.of("OrderCreated"),
    new OrderEvent.OrderCreated("ord-1", "cust-42"));
```

A stream id is the typed pair `order:ord-1`; assert `envelope.aggregateType()` when a test needs to know
which aggregate an event belongs to.

### Happy path test

```java
@Test
void process_buildsReadModel_fromOrderCreated() {
  var projection = new OrderRegistry();
  var streamId   = StreamId.of(ORDER, AggregateId.of("ord-1"));

  projection.process(List.of(
      EventStoreFixture.event(streamId, new Version(1), EventType.of("OrderCreated"),
          new OrderEvent.OrderCreated("ord-1", "cust-42"))));

  assertEquals("NEW", projection.statusFor("ord-1"));
}
```

### Idempotency test

Processing the same event twice must produce the same result (the `AT_LEAST_ONCE_IDEMPOTENT`
contract, and what a dead-letter replay needs under every mode, because it applies a range at least
once):

```java
@Test
void process_isIdempotent_forDuplicateEvents() {
  var projection = new OrderRegistry();
  var streamId   = StreamId.of(ORDER, AggregateId.of("ord-1"));

  var envelope = EventStoreFixture.event(
      streamId, new Version(1), EventType.of("OrderCreated"),
      new OrderEvent.OrderCreated("ord-1", "cust-42"));

  projection.process(List.of(envelope));
  projection.process(List.of(envelope));   // same event again

  // Must still be "NEW" — not doubled, not errored
  assertEquals("NEW", projection.statusFor("ord-1"));
}
```

### Unit-testing a TRANSACTIONAL_LOCAL projection without PostgreSQL

`InMemoryProjectionRepository` (in `streamrune-test`) is a `ProjectionRepository`, a fencing-capable
`AtomicBatchProcessor` and the `OffsetStore` of its own checkpoint in one object, so one instance
plays the parts a `JdbcProjectionRepository` and a `PostgresOffsetStore` play in production — the
rollback included. Construct the projection over it, pass the same instance to the runner as its
offset store and its processor, and run the projection `TRANSACTIONAL_LOCAL`. The assertions use
AssertJ (`org.assertj:assertj-core`, test scope), which `streamrune-test` does not bring:

```java
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.PollingProjectionRunner;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;

static final AggregateType ORDER = AggregateType.of("order");   // the aggregate type the events are filed under

record OrderView(String orderId, String status) {}

class OrderProjection extends BaseProjection {
  OrderProjection(ProjectionRepository repository) {
    super(repository, "orders");   // saves under the name it is registered under
  }

  @Override
  public void process(List<EventEnvelope> batch) {
    for (var envelope : batch) {
      if (envelope.event() instanceof OrderEvent.OrderCreated e) {
        save(e.orderId(), new OrderView(e.orderId(), "NEW"));
      }
    }
  }
}

@Test
void transactionalLocal_commitsTheRowAndTheCheckpointTogether() {
  var eventStore = new InMemoryEventStore();
  var streamId = StreamId.of(ORDER, AggregateId.of("ord-1"));
  eventStore.append(streamId, List.of(EventStoreFixture.event(streamId, new Version(1),
      EventType.of("OrderCreated"), new OrderEvent.OrderCreated("ord-1", "cust-42"))),
      Version.initial());

  var repo = new InMemoryProjectionRepository();           // ProjectionRepository + AtomicBatchProcessor + OffsetStore
  var projection = new OrderProjection(repo);               // extends BaseProjection
  var runner = new PollingProjectionRunner(eventStore, repo, 100, 50, repo);
  runner.run(ProjectionName.of("orders"), projection, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);

  assertThat(repo.findById(ProjectionName.of("orders"), "ord-1", OrderView.class)).isPresent();
  assertThat(repo.committedOffset(ProjectionName.of("orders"))).isEqualTo(GlobalOffset.of(1));
}
```

The rollback: a projection that throws mid-batch leaves neither the row nor the checkpoint.

```java
@Test
void transactionalLocal_rollsBackTheRowAndTheCheckpointTogether() {
  // eventStore holds the OrderCreated event of ord-1, appended as above
  var repo = new InMemoryProjectionRepository();
  var failing = new BaseProjection(repo, "orders") {
    @Override
    public void process(List<EventEnvelope> batch) {
      save("ord-1", new OrderView("ord-1", "NEW"));         // written in the batch's transaction ...
      throw new IllegalStateException("projection bug");    // ... and rolled back with it
    }
  };
  var runner = new PollingProjectionRunner(eventStore, repo, 100, 50, repo);

  assertThatThrownBy(() -> runner.run(
          ProjectionName.of("orders"), failing, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL))
      .isInstanceOf(IllegalStateException.class);
  assertThat(repo.findById(ProjectionName.of("orders"), "ord-1", OrderView.class)).isEmpty();
  assertThat(repo.committedOffset(ProjectionName.of("orders"))).isEqualTo(GlobalOffset.initial());
}
```

The same runner refuses the projection when the pairing cannot keep the promise — a second
`InMemoryProjectionRepository` as the processor is another store, and
`AtomicBatchProcessor.nonAtomicAtLeastOnce()` cannot hold a transaction — so a test of that wiring
is an `assertThatThrownBy(...).isInstanceOf(IllegalArgumentException.class)` on `run`. Use
`org.streamrune.test.InMemoryOffsetStore` beside `nonAtomicAtLeastOnce()` for an
`AT_LEAST_ONCE_IDEMPOTENT` projection; with `InMemoryProjectionRepository` as the processor, pass the
repository itself as the offset store, because its checkpoint is the one it advances.

`AtomicBatchProcessorContract` (also in `streamrune-test`) is what a custom transactional processor
extends to prove it honours `supportsFencing()`: a JUnit suite that checks the handed repository is
never `null`, an updater exception commits nothing, the write and the checkpoint become visible
together, a stale epoch and an overlapping batch are rejected before the updater writes, after-commit
actions run only after a commit, and `writesTo` holds through a forwarding proxy.
`InMemoryProjectionRepository` and `JdbcProjectionRepository` both pass it.

---

## Testing a listener that dispatches commands

A listener that reacts to an event with a command — a process manager — is tested against a real
bus: `InMemoryCommandBus` runs the decider and stores the events as `VirtualThreadCommandBus` does,
and makes the same check of the bound context first.

```java
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.test.InMemoryCommandBus;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.SagaEnvelopes;

AggregateType PAYMENT = AggregateType.of("payment");
StreamId payment = StreamId.of(PAYMENT, AggregateId.of("pay-1"));

var eventStore = new InMemoryEventStore();
CommandBus commandBus = InMemoryCommandBus.builder()
    .eventStore(eventStore)
    .register(new InMemoryCommandBus.DeciderRegistration<>(
        PAYMENT, CapturePayment.class, c -> AggregateId.of(c.paymentId()), new PaymentDecider()))
    .build();

// The listener under test: it captures a payment the order saga initiated.
EventListener processManager = events -> {
    for (EventEnvelope event : events) {
        if (event.event() instanceof PaymentInitiated initiated) {
            StreamRuneContext.continuing(event).run(() ->
                commandBus.execute(
                    new CapturePayment(initiated.paymentId()),
                    IdempotencyKey.of("capture:" + event.metadata().eventId().value())));
        }
    }
};

// The reply to a command the saga dispatched itself.
EventEnvelope initiated = SagaEnvelopes.causedBy(
    OrderSaga.State.class, SagaId.of("order-saga-42"), payment, new PaymentInitiated("pay-1"));

processManager.onEvents(List.of(initiated));

EventMetadata captured = eventStore.load(payment).events().getLast().metadata();
assertEquals(SagaId.of("order-saga-42"), captured.sagaOrigin().sagaId());
assertEquals(1, captured.sagaOrigin().hop());
assertEquals(CausationId.of(initiated.metadata().eventId().value()), captured.causationId());
```

### A hand-written `CommandBus` double is a bus of your own

Every shipped command bus and query bus begins a dispatch with
`StreamRuneContext.requireNoForeignReservedCorrelationId()`: a dispatch bound to a correlation id in
the reserved `streamrune:` form that is not the bound saga origin's is refused with
`ReservedCorrelationIdException`. A test double you write — a recording `CommandBus`, a mock — makes
that check only if you put it there. A listener that copies the reserved id of a saga-caused event
into a `RequestContext` of its own, instead of dispatching inside `continuing`, then passes its unit
test against the double and is refused by the bus it runs on in production, on every delivery: the
subscription keeps its checkpoint and delivers the event again without end, the bus logs one `WARN`
per attempt, the poll loop one `ERROR`, and the subscription's health entry stays `DEGRADED`.

Use `InMemoryCommandBus`, or make the check the first statement of the double:

```java
final class RecordingCommandBus implements CommandBus {
    final List<Command> executed = new ArrayList<>();

    @Override
    public boolean supportsIdempotentExecution() {
        return true;
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
        return execute(command, null);
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
        StreamRuneContext.requireNoForeignReservedCorrelationId();   // what every shipped bus does first
        executed.add(command);
        return new CommandResult(List.of(), null, null, null);
    }
}
```

A custom bus that is more than a double extends `CommandBusProvenanceContract`, which holds the rule
among its cases (see the `streamrune-test` README).

### Envelopes for a saga's reply and for a continuation's

`SagaEnvelopes` builds, without a saga or a bus, what the command bus stores:

| Call | The event it stands for |
|---|---|
| `causedBy(stateType, sagaId, streamId, event)` | the reply to a command the saga dispatched itself: the saga's origin at hop 0, no causation id |
| `continuationOf(cause, streamId, event)` | the event of a command a listener dispatched inside `StreamRuneContext.continuing(cause)`: the origin of `cause` one hop further, the id of `cause` as the causation id, no user, no baggage |
| `causedBy(stateType, sagaId, hop, streamId, event)` | the same saga-caused event `hop` continuations away: the last of a chain of `continuationOf` envelopes |
| `metadataCausedBy(stateType, sagaId)`, `metadataOfContinuation(cause)` | the metadata alone, for an envelope you build at an offset of your choosing |

`continuationOf` binds `continuing(cause)` and asks `InMemoryCommandBus` for the metadata it writes
under those bindings, so a hand-built continuation and a stored one cannot differ. A `SagaRunner`
routes by the origin's saga type and saga id; the hop count and the causation id take no part in
that, so a hop-0 envelope reaches the saga exactly as a continuation's reply does. Build the later
hop where the code under test reads the hop, the causation id, or the stored shape of the event.

Each envelope sits at global offset 0. A `SagaRunner` skips an event at or below the offset its saga
applied last, so append the envelope to an event store first (and read it back), or build an
envelope at a later offset around the metadata.

---

## Testing a saga with SagaFixture

`SagaFixture` runs a saga without a subscription, an event store or command handlers. `of(...)`
hands your code the saga's own `SagaRunner.Builder`, already wired with an `InMemorySagaStore`, an
`InMemorySagaDeadLetterStore`, a bus that records every command the saga dispatches and executes
none, and a `MutableClock` at `SagaFixture.START`; you add the saga. Each step stores its events at
the next global offset and delivers them as one page through `asEventListener()`, as a subscription
does, and `expectCommands` reads the commands of the latest step:

```java
var decider = new OrderSagaDecider();   // a SagaDecider with timeout() of 30 minutes
SagaFixture<OrderSaga.State> saga =
    SagaFixture.of(
            OrderSaga.State.class,
            builder ->
                builder
                    .decider(decider)
                    // OrderSaga.sagaOf answers "no saga" for an order id no saga id can hold
                    .startWhen(event ->
                        event.event() instanceof OrderPlaced e && OrderSaga.sagaOf(e.orderId()).isPresent())
                    .extractSagaId(event ->
                        OrderSaga.sagaOf(((OrderPlaced) event.event()).orderId()).orElseThrow()))
        .withTimeouts(builder -> builder.decider(decider));
StreamId order = StreamId.of(AggregateType.of("order"), AggregateId.of("1"));
SagaId orderSaga = OrderSaga.sagaOf("1").orElseThrow();

saga.when(order, new OrderPlaced("1"))                 // an event a request stored
    .expectCommands(new ReserveStock("1"))
    .expectState(orderSaga, state -> state.status() == SagaStatus.RUNNING)
    .replyingTo(orderSaga, new StockReserved("1"))     // the reply to the saga's own command
    .expectCommands(new ChargePayment("1"))
    .advance(Duration.ofMinutes(31))
    .runTimeouts()                                     // one poll of the SagaTimeoutRunner
    .expectCommands(new ReleaseStock("1"))
    .expectStatus(orderSaga, SagaStatus.COMPENSATED);
```

| Call | What it does |
|---|---|
| `given(events...)`, `when(event)`, `when(streamId, event)` | Steps: deliver stored events. `event(streamId, payload)` builds one as a request stores it (no saga origin, the fixture clock's instant) |
| `replyingTo(sagaId, event)`, `replyingTo(sagaId, streamId, event)` | A step: delivers the reply to a command the saga dispatched (`reply(...)`, from `SagaEnvelopes`) |
| `advance(duration)`, `runTimeouts()` | Moves the clock; one poll of the timeout runner `withTimeouts(...)` built over the same stores and clock |
| `redeliver()` | A step: the events of the last delivery again, at the same offsets — what a subscription delivers after a crash before its checkpoint |
| `failing(commandType, failure)` | Every later dispatch of that command type throws: a rejected command, or a compensation that cannot be dispatched |
| `expectCommands(...)`, `expectNoCommands()` | The commands of the latest step, in order |
| `expectState(id, predicate)`, `expectStatus(id, status)`, `expectCompensating(id)`, `expectCompleted(id)`, `expectDeadLettered(id)` | The saga's row and its dead-letter entries |
| `saga(id)`, `dispatched()`, `delivered()`, `runner()`, `sagaStore()`, `deadLetterStore()`, `clock()`, `sagaStoreFaults()` | The parts, for an assertion of your own |

The builder starts with `forwardFence(false)`: the recording bus stores nothing, so it fences
nothing. A crash point of a step is a fault of the saga store the runner writes through:

```java
saga.sagaStoreFaults().crashAfter("createGenesisPending");   // stop after the first write
assertThrows(InjectedFaultException.class, () -> saga.when(order, new OrderPlaced("1")));
saga.redeliver()                                             // the subscription delivers it again
    .expectCommands(new ReserveStock("1"))
    .expectStatus(orderSaga, SagaStatus.RUNNING);
```

The saga guide's quick example is tested this way (`SagaGuideQuickExampleTest` in
`streamrune-runtime`).

---

## Testing Failure Modes

### DomainException via assertThrows

```java
@Test
void cancelCancelledOrder_throwsDomainException() {
  var decider = new OrderDecider();
  var state   = decider.initialState();

  // Evolve to CANCELLED
  state = decider.evolve(state, new OrderEvent.OrderCreated("ord-1", "cust-42"));
  state = decider.evolve(state, new OrderEvent.OrderCancelled("ord-1", "changed my mind"));

  var cancelledState = state;
  assertThrows(DomainException.class, () ->
      decider.decide(new OrderCommand.CancelOrder("ord-1", "again"),
                     cancelledState));
}
```

### OptimisticLockException via EventStoreFixture

(`ORDER` is the `AggregateType` constant declared above.)

```java
@Test
void concurrentAppend_throwsOptimisticLockException() {
  var streamId = StreamId.of(ORDER, AggregateId.of("ord-x"));

  EventStoreFixture.store()
      .given()
          .append(streamId, List.of(
              EventStoreFixture.event(streamId, new Version(1), EventType.of("OrderCreated"),
                  new OrderEvent.OrderCreated("ord-x", "cust-1"))))
      .when()
          .append(streamId, List.of(
              EventStoreFixture.event(streamId, new Version(1), EventType.of("OrderCreated"),
                  new OrderEvent.OrderCreated("ord-x", "cust-2"))),
              Version.initial())
          .expectOptimisticLockException();
}
```

### Store failures and crash points with FaultSchedule

Every store port has a forwarding decorator in the kit — `ForwardingEventStore`,
`ForwardingSagaStore`, `ForwardingSagaDeadLetterStore`, `ForwardingDeadLetterQueue`,
`ForwardingOffsetStore`, `ForwardingProjectionDeadLetterStore`, `ForwardingOutboxStore` — that
forwards every method of its interface, the default methods included, and consults a
`FaultSchedule` on each call. A method is named by its Java name; the overloads of a name share it:

```java
var faults = FaultSchedule.none()
    .failOnce("append")                 // the next append throws before anything is stored
    .crashAfter("saveOffset");          // the next checkpoint is stored, then the call throws
var events = new ForwardingEventStore(new InMemoryEventStore(), faults);
var offsets = new ForwardingOffsetStore(new InMemoryOffsetStore(), faults);

// ... run the code under test, catch InjectedFaultException where the "crash" stops it,
// run it again over the same stores, assert that it converges ...
assertEquals(2, faults.calls("append"));
```

`failAt(method, n)` fails the n-th call, `failAlways(method)` every later one, and
`throwing(() -> new EventStoreException("down", new SQLException()))` replaces
`InjectedFaultException`, to exercise how a transient failure is classified. A misspelled method
name fails where it is written. A flow that writes in more than one step has a crash point after
each write; `crashAfter` stops it there.

---

## Recording doubles

`RecordingStreamRuneMetrics` records every call of the metrics port under its `MetricNames` meter,
with the tags the call carries; its queries refuse a name outside `MetricNames`:

```java
var metrics = new RecordingStreamRuneMetrics();
// ... build the runner with .metrics(metrics) and drive it ...
metrics.assertIncremented(MetricNames.PROJECTIONS_DEAD_LETTERED,
    MetricNames.TAG_PROJECTION_NAME, "orders");
assertEquals(3, metrics.total(MetricNames.INBOX_SWEPT_ROWS));         // rows, summed
assertEquals(OptionalLong.of(0), metrics.lastGauge(MetricNames.OUTBOX_PENDING));
```

`ScriptedSubscriptionLeadership` is a `SubscriptionLeadership` the test drives through a failover:

```java
var leadership = new ScriptedSubscriptionLeadership().grant();   // it denies until granted
// ... a runner built with .leadership(leadership) leads ...
leadership.revoke();     // the lease is gone: the runner stands by
leadership.grant();      // free again: the runner takes it, with a greater epoch
assertEquals(2, leadership.acquisitions());
```

---

## Recorded event streams

`EventStreamFixtures` keeps an event stream in a file: record it once from an `InMemoryEventStore`,
check the file in, and replay it into a fresh store in a later test, so a projection runs over a
history nobody rebuilds by hand:

```java
EventStreamFixtures.write(store, Path.of("src/test/resources/orders.json"));   // once

var replayed = InMemoryEventStore.serializing(typeRegistry, null);
EventStreamFixtures.appendTo(replayed, EventStreamFixtures.read(Path.of("src/test/resources/orders.json")));
```

The file is a JSON array of `{streamId, version, eventType, schemaVersion, payload, metadata}`,
written with sorted keys, so the same stream is always the same file, and its ids and timestamps
replay unchanged. `appendTo` binds each payload as the serializing store binds a stored one: the
store's `EventTypeRegistry` resolves the type (an unregistered one fails with
`UnknownEventTypeException`) and its upcasters upcast a payload written at an older schema version.
`write` serializes the payloads of a serializing store with a copy of the store's own
`ObjectMapper`, so a type your application serializes its own way (a module, a custom serializer)
reads back, and leaves the fields of an `@Encrypted` event in the clear: append such a file to a
store without a crypto engine.

---

## The kit, type by type

Every public type of `streamrune-test` (package `org.streamrune.test`):

| Type | What it is for |
|---|---|
| `InMemoryEventStore` | An `EventStore` with the semantics of `PostgresEventStore`: optimistic locking, one global order, snapshots, upcasting, and with `serializing(...)` JSON payloads and crypto-shredding. |
| `InMemoryEventStoreFactory` | An `EventStoreFactory` that creates `InMemoryEventStore`s, for wiring that asks for a factory. |
| `InMemoryCommandBus` | A `CommandBus` without locks, retries or interceptors that runs each decider's `guard` before `decide`. |
| `InMemoryQueryBus` | A `QueryBus` that dispatches by the exact query class. |
| `InMemoryEventSubscription` | An `EventSubscription` with the at-most-once notification semantics of PostgreSQL `LISTEN`/`NOTIFY`, to test push-driven wiring without Docker. |
| `InMemoryOffsetStore` | An `OffsetStore` with the monotonic guard of the PostgreSQL one. |
| `InMemoryProjectionRepository` | A `ProjectionRepository`, a fencing `AtomicBatchProcessor` and its own checkpoint in one object, to run a `TRANSACTIONAL_LOCAL` projection without PostgreSQL. |
| `InMemoryProjectionDeadLetterStore` | A `ProjectionDeadLetterStore`; `all()` returns every entry for an assertion. |
| `InMemoryCommandInbox` | A `CommandInbox`; the in-memory event store appends and claims under one lock. |
| `InMemoryDeadLetterQueue` | A `DeadLetterQueue` that stamps `publishedAt` from the `Clock` it is given. |
| `InMemoryOutboxStore` | An `OutboxStore` with the claim, lease and ordering rules of `PostgresOutboxStore`, its retry times taken from the `Clock` it is given. |
| `ForwardingOutboxStore` | An `OutboxStore` that forwards every call to a delegate and injects the faults of a `FaultSchedule`; or override one method to make that call fail, block or record. |
| `ForwardingEventStore`, `ForwardingSagaStore`, `ForwardingSagaDeadLetterStore`, `ForwardingDeadLetterQueue`, `ForwardingOffsetStore`, `ForwardingProjectionDeadLetterStore` | The same for the other store ports: every method forwarded, the default methods included, each call consulting the `FaultSchedule`. |
| `FaultSchedule` | The faults a forwarding store injects — `failOnce`, `failAt`, `failAlways`, `crashAfter`, `throwing` — and the count of every call. |
| `InjectedFaultException` | What a `FaultSchedule` throws unless it names another failure. |
| `RecordingStreamRuneMetrics` | A `StreamRuneMetrics` that records every call under its `MetricNames` meter and tags; `assertIncremented`, `count`, `total`, `lastGauge`. |
| `ScriptedSubscriptionLeadership` | A `SubscriptionLeadership` the test grants, denies and revokes, counting attempts, acquisitions and resignations. |
| `InMemorySagaStore` | A `SagaStore` that keeps the state objects themselves and stamps their times from the `Clock` it is given. |
| `InMemorySagaDeadLetterStore` | A `SagaDeadLetterStore` whose `publish` is an upsert per saga and event offset, as in PostgreSQL. |
| `InMemoryCryptoEngine` | An AES-256-GCM `CryptoEngine` with its keys in memory and the terminal erasure of the production engines. |
| `MutableClock` | A `Clock` the test sets and advances: retry backoff, timeouts and timestamps without sleeping. |
| `DeciderFixture` | Given events, when a command, then events or a domain exception, for a decider. |
| `EventStoreFixture` | Given/when/then over an event store's appends; `event(...)` builds a test envelope. |
| `ProjectionFixture` | Feeds a projection events in sequence and asserts on its state. |
| `SagaFixture` | Given events, when an event, then the commands a saga dispatched and its state, over in-memory stores, a recording bus and a `MutableClock`. |
| `EventStreamFixtures` | Writes the events of an `InMemoryEventStore` to a JSON file, reads it back, and appends it to a fresh store. |
| `SagaEnvelopes` | The metadata and envelopes of the events a saga's commands, and the commands of a continuation, produce. |
| `EventPayloadAssertions` | `assertRoundTrips(event)` and `assertUpcastBinds(...)`: an event that writes what it cannot read, or an upcaster that writes a misspelled key, fails the test instead of a read in production. |
| `AtomicBatchProcessorContract` | The contract suite of `AtomicBatchProcessor.supportsFencing()`; extend it to prove a custom processor. |
| `CommandBusProvenanceContract` | The contract suite of where a `CommandBus` takes an event's saga origin and causation id from; extend it for a custom bus. |
| `OutboxStoreContract` | The contract suite of every `OutboxStore`: status transitions, both ordering modes, retention and replay. |
| `SagaStoreContract` | The contract suite of every `SagaStore`: the recovery state a saga runner, step executor and replayer depend on. |
| `SagaDeadLetterStoreContract` | The contract suite of every `SagaDeadLetterStore`: stored facts win, and the shield decides what retention removes. |
| `SagaForwardFenceContract` | The contract suite of an `EventStore` that fences a saga's forward command against the rows of a `SagaStore`. |
| `NativeReflectionTypes` | Derives from code the StreamRune types a GraalVM native image registers for reflection; the integrations' native-hint tests compare their hints with it. |
| `ShippedMigrationScripts` | Lists the Flyway migration scripts StreamRune ships, from the class path, so a native-image test can check each is a registered resource. |
| `UnregisteredSealedTypes` | Builds a sealed type that looks like one a native image did not register (`isSealed()` with no permitted subclasses), to test the code that refuses it. |

A test that controls time passes a `MutableClock` to the store it exercises and advances it:

```java
var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
var outbox = new InMemoryOutboxStore(clock);
// ... an entry marked for retry 5 s from now is not pending yet ...
clock.advance(Duration.ofSeconds(5));
// ... and is pending now
```

The runners, the outbox poller, the subscriptions and `StreamRune` take a clock as well:
`clock(Clock)` on their builders decides the timestamps they record and the durations they measure
(without one they read the system clock and time with `System.nanoTime()`):

```java
var subscription = PollingEventSubscription.builder()
    // ... the store, the offsets, a listener that advances the clock by 250 ms ...
    .metrics(metrics)
    .clock(clock)
    .build();
// streamrune.subscriptions.delivery.latency records 250 ms for that delivery
```

A test of what happens when one store call fails schedules the fault, or overrides that call alone:

```java
var failingDelivery =
    new ForwardingOutboxStore(new InMemoryOutboxStore(), FaultSchedule.none().failOnce("markDelivered"));

var customised =
    new ForwardingOutboxStore(new InMemoryOutboxStore()) {
      @Override
      public boolean markDelivered(OutboxEntryId id, String claimedBy) {
        throw new IllegalStateException("simulated crash after the publish");
      }
    };
```

A test of an event class or an upcaster binds the payload the way the event store does:

```java
OrderPlaced placed = new OrderPlaced("order-1", "SKU-A", 2);
EventPayloadAssertions.assertRoundTrips(placed);
EventPayloadAssertions.assertUpcastBinds(
    new OrderPlacedUpcaster(), Map.of("orderId", "order-1", "sku", "SKU-A"), 1, OrderPlaced.class);
```

---

## Test Dependency Matrix

| Test type | Modules required | Database? |
|---|---|---|
| Decider unit test | `streamrune-core` | No |
| DeciderFixture test | `streamrune-test` | No |
| Integration test (in-memory) | `streamrune-core`, `streamrune-runtime`, `streamrune-test` | No |
| EventStoreFixture test | `streamrune-test` | No |
| Projection unit test | `streamrune-core`, `streamrune-test` | No |
| Saga test (`SagaFixture`) | `streamrune-test` (it brings `streamrune-runtime`) | No |
| `TRANSACTIONAL_LOCAL` projection test (runner, rollback) | `streamrune-core`, `streamrune-runtime`, `streamrune-test`, AssertJ | No |
| Postgres integration test | `streamrune-postgres`, TestContainers | Yes (container) |

---

## What Next?

| Guide | Description |
|---|---|
| [concepts.md](concepts.md) | CQRS/ES theory and the Decider pattern |
| [getting-started.md](getting-started.md) | Build a full application from scratch |
| [streamrune-ecommerce-demo](https://github.com/StreamRune/streamrune-ecommerce-demo) (separate repository) | Real-world domain with projections and three frameworks |
