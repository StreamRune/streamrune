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

---

## Test Dependency Matrix

| Test type | Modules required | Database? |
|---|---|---|
| Decider unit test | `streamrune-core` | No |
| DeciderFixture test | `streamrune-test` | No |
| Integration test (in-memory) | `streamrune-core`, `streamrune-runtime`, `streamrune-test` | No |
| EventStoreFixture test | `streamrune-test` | No |
| Projection unit test | `streamrune-core`, `streamrune-test` | No |
| `TRANSACTIONAL_LOCAL` projection test (runner, rollback) | `streamrune-core`, `streamrune-runtime`, `streamrune-test`, AssertJ | No |
| Postgres integration test | `streamrune-postgres`, TestContainers | Yes (container) |

---

## What Next?

| Guide | Description |
|---|---|
| [concepts.md](concepts.md) | CQRS/ES theory and the Decider pattern |
| [getting-started.md](getting-started.md) | Build a full application from scratch |
| [streamrune-ecommerce-demo](https://github.com/StreamRune/streamrune-ecommerce-demo) (separate repository) | Real-world domain with projections and three frameworks |
