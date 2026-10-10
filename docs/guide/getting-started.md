# Getting Started with StreamRune

This guide walks you from an empty Gradle project to a running event-sourced application
with a projection and a command bus. By the end you will have a working `OrderDecider`,
an in-memory event store, and a simple read model — all tested without a database.

---

## Prerequisites

| Requirement | Minimum | Check |
|---|---|---|
| Java | 25 | `java -version` |
| Gradle | 9.1 | `gradle --version`. 9.1 is the first Gradle release that runs on Java 25; StreamRune itself is built and tested with Gradle 9.3.1 (its wrapper) |
| PostgreSQL | 17 | Only required for production; unit tests use `InMemoryEventStore`. Tested on 17 and 18; an older server is refused at startup |

---

## Add the StreamRune preview build

StreamRune's modules all share the `org.streamrune` group and one version. StreamRune is not
released yet: the published build is `1.0.0-alpha-SNAPSHOT`, an unreleased preview that lives in the
Maven Central snapshot repository, not in Maven Central itself (see
[Preview builds](../../README.md#preview-builds) and the [CHANGELOG](../../CHANGELOG.md)). Add that
repository next to `mavenCentral()` — there is no local build step, and each module pulls in the
rest of the framework it needs transitively. The next section shows a complete `build.gradle.kts`.

---

## Gradle Dependencies

Create a new Gradle project. A minimal `build.gradle.kts` that pulls in all required
modules looks like this:

```kotlin
plugins {
    java
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

repositories {
    mavenCentral()
    // 1.0.0-alpha-SNAPSHOT lives in the Maven Central snapshot repository only.
    maven("https://central.sonatype.com/repository/maven-snapshots/") {
        mavenContent { snapshotsOnly() }
        content { includeGroup("org.streamrune") }
    }
}

dependencies {
    // Core abstractions: Decider, EventStore, DomainEvent, AggregateState
    implementation("org.streamrune:streamrune-core:1.0.0-alpha-SNAPSHOT")

    // Runtime: StreamRune facade, VirtualThreadCommandBus, ProjectionRunner
    implementation("org.streamrune:streamrune-runtime:1.0.0-alpha-SNAPSHOT")

    // Testing: InMemoryEventStore, EventStoreFixture
    testImplementation("org.streamrune:streamrune-test:1.0.0-alpha-SNAPSHOT")

    // JUnit 5
    testImplementation("org.junit.jupiter:junit-jupiter:5.11.0")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
```

> Every `org.streamrune` coordinate is at version `1.0.0-alpha-SNAPSHOT`, resolved from the snapshot
> repository above — Gradle
> pulls in each module's own framework dependencies (e.g. `streamrune-crypto-api`) transitively, so
> you only declare the modules you use directly.

For production persistence add the PostgreSQL module:

```kotlin
// PostgreSQL-backed event store
implementation("org.streamrune:streamrune-postgres:1.0.0-alpha-SNAPSHOT")
```

---

## Define Domain Events as a Sealed Interface

Events are immutable facts. Use a sealed interface so the compiler warns when a switch is
missing a variant.

```java
package com.example.order;

import org.streamrune.core.DomainEvent;

public sealed interface OrderEvent extends DomainEvent
    permits OrderEvent.OrderCreated,
            OrderEvent.OrderConfirmed,
            OrderEvent.OrderCancelled {

  /** Emitted when an order is first placed. */
  record OrderCreated(String orderId, String customerId) implements OrderEvent {}

  /** Emitted when an order is confirmed for fulfilment. */
  record OrderConfirmed(String orderId) implements OrderEvent {}

  /** Emitted when an order is cancelled, preserving the reason for audit. */
  record OrderCancelled(String orderId, String reason) implements OrderEvent {}
}
```

---

## Define Aggregate State

State is derived from events. It is never stored directly in the event store (unless you
enable snapshots for performance). Use a record for immutability and provide a no-arg
constructor that represents the initial state before any events have been applied.

```java
package com.example.order;

import org.streamrune.core.AggregateState;

public record OrderState(String orderId, String customerId, OrderStatus status)
    implements AggregateState {

  /** Initial state — no fields populated, stream version is 0. */
  public OrderState() {
    this(null, null, OrderStatus.NEW);
  }

  public OrderState withStatus(OrderStatus newStatus) {
    return new OrderState(orderId, customerId, newStatus);
  }
}
```

```java
package com.example.order;

public enum OrderStatus { NEW, CONFIRMED, CANCELLED }
```

---

## Implement a Decider

Commands go in a sealed interface, then the `OrderDecider` handles each variant:

Every command type must implement the `org.streamrune.core.Command` marker interface — the
builder's `register(...)` is bounded `<C extends Command>`, so a command that does not extend
`Command` will not compile. `Command` is a pure marker, and commands are transient input: the
framework never writes one to the event store. The one exception is the dead letter queue. When
you configure a `DeadLetterQueue`, a command that fails on an infrastructure error after its
retries is stored as JSON (written with the bus's `ObjectMapper`) and rebuilt from that JSON on
every replay, so its type has to round-trip through Jackson. Records do by default; a plain class
needs a default constructor or a `@JsonCreator`. Nothing checks this at registration; see
[Retry and resilience › Dead-letter queue](advanced/retry-and-resilience.md#dead-letter-queue).

```java
package com.example.order;

import org.streamrune.core.Command;

public sealed interface OrderCommand extends Command
    permits OrderCommand.CreateOrder,
            OrderCommand.ConfirmOrder,
            OrderCommand.CancelOrder {

  record CreateOrder(String orderId, String customerId) implements OrderCommand {}
  record ConfirmOrder(String orderId) implements OrderCommand {}
  record CancelOrder(String orderId, String reason) implements OrderCommand {}
}
```

```java
package com.example.order;

import java.util.List;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainException;

public class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {

  @Override
  public OrderState initialState() {
    return new OrderState();
  }

  @Override
  public List<OrderEvent> decide(OrderCommand cmd, OrderState state) {
    return switch (cmd) {

      case OrderCommand.CreateOrder c -> {
        if (state.orderId() != null)
          throw new DomainException("Order " + c.orderId() + " already exists");
        yield List.of(new OrderEvent.OrderCreated(c.orderId(), c.customerId()));
      }

      case OrderCommand.ConfirmOrder c -> {
        if (state.status() != OrderStatus.NEW)
          throw new DomainException(
              "Cannot confirm an order with status " + state.status());
        yield List.of(new OrderEvent.OrderConfirmed(c.orderId()));
      }

      case OrderCommand.CancelOrder c -> {
        if (state.status() == OrderStatus.CANCELLED)
          throw new DomainException("Order is already cancelled");
        yield List.of(new OrderEvent.OrderCancelled(c.orderId(), c.reason()));
      }
    };
  }

  @Override
  public OrderState evolve(OrderState state, OrderEvent evt) {
    return switch (evt) {
      case OrderEvent.OrderCreated e  ->
          new OrderState(e.orderId(), e.customerId(), OrderStatus.NEW);
      case OrderEvent.OrderConfirmed e -> state.withStatus(OrderStatus.CONFIRMED);
      case OrderEvent.OrderCancelled e -> state.withStatus(OrderStatus.CANCELLED);
    };
  }
}
```

`decide` and `evolve` have no dependencies — no constructor injection required. The
business rules live here, and here alone.

`CreateOrder` first checks that the order does not exist yet. The bus runs `decide` for any
command whose id it can extract, whether or not that stream already has events, so without the
check a second `CreateOrder` for the same id would append another `OrderCreated`, and `evolve`
would reset a confirmed order to `NEW`.

---

## Wire with StreamRune.builder()

Start with the `InMemoryEventStore` so nothing needs a database to run:

```java
import java.util.function.Function;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.StreamRune;
import org.streamrune.test.InMemoryEventStore;

var eventStore = new InMemoryEventStore();

// The aggregate type names the aggregate; every stream is "<type>:<id>".
static final AggregateType ORDER = AggregateType.of("order");

// The id extractor returns a typed AggregateId (not a raw String):
// register(...) expects Function<C, AggregateId>. Each variant carries its
// own orderId, so match over the sealed command to pull it out.
Function<OrderCommand, AggregateId> orderId = cmd -> switch (cmd) {
  case OrderCommand.CreateOrder c  -> AggregateId.of(c.orderId());
  case OrderCommand.ConfirmOrder c -> AggregateId.of(c.orderId());
  case OrderCommand.CancelOrder c  -> AggregateId.of(c.orderId());
};

var streamRune = StreamRune.builder()
    .eventStore(eventStore)
    // Register the aggregate type with the command type, the id extractor and the decider.
    // One registration for the whole sealed OrderCommand hierarchy.
    .register(ORDER, OrderCommand.class, orderId, new OrderDecider())
    .build();
```

### Aggregate types

The `AggregateType` is the name an aggregate's streams carry: `order` plus the id `ord-1` is the
stream `order:ord-1`. Two aggregates may therefore use the same id value without sharing a stream.

- **Syntax:** a lowercase letter, then lowercase letters, digits and underscores, at most 32
  characters (`[a-z][a-z0-9_]{0,31}`). `AggregateType.of(...)` refuses anything else.
- **It is permanent.** The type is part of every stored stream key, so renaming it renames the
  streams.
- **One constant per aggregate**, kept beside the aggregate's state in the domain module
  (`Order.TYPE`), and used by every registration and every `StreamId.of(...)` for that aggregate.
- **One registration per command type.** `register(...)` refuses a command type that is already
  registered.
- **Overlapping command roots share a type.** If one registered command type is assignable to
  another (a supertype and its subtype), both must be registered under the same aggregate type.
  Unrelated command roots may share a type too.
- **One decider instance belongs to one type.** Registering the same decider object under two
  types is refused.

Key builder methods:

| Method | Purpose |
|---|---|
| `.eventStore(EventStore)` | Sets the persistence backend |
| `.register(aggregateType, commandType, idExtractor, decider)` | Maps a command class to its decider under an aggregate type; `idExtractor` is `Function<C, AggregateId>` and locates the aggregate stream. Build the id with `AggregateId.of(...)`: it refuses a blank value, one longer than 255 characters, or one containing a control character (see below) |
| `.registerProjection(name, projection, mode)` | Registers a read-model projection under its `ProjectionDeliveryMode` — required, no default (see [Delivery modes](concepts.md#delivery-modes--what-a-projection-promises)) |
| `.projectionRunner(...)` / `.projectionRunnerFactory(...)` | Supplies the runner that drives registered projections (see below) |
| `.build()` | Constructs and validates the facade |

`AggregateId.of(...)` is the *ingress* factory for a client-supplied id. It throws `IllegalArgumentException` for a blank value, one longer than 255 characters, or one containing a control character — C0 (`U+0000`–`U+001F`, which includes CR, LF, TAB and NUL), DEL (`U+007F`) or C1 (`U+0080`–`U+009F`) — without echoing the value. An aggregate id is stored verbatim in the `aggregate_id` column of `event_stream` (beside its `aggregate_type`) and of the audit and dead-letter tables — no table holds a composed id; `order:ord-1` is only how Java and JSON show the pair — so a CR/LF there forges a line for every reader and a NUL makes the PostgreSQL write fail. The `aggregate_id` columns are `VARCHAR(255)`, so without the length check a longer id would fail the append with an SQL error instead of a client error; a stream id built from a longer id is refused before any interceptor runs. The length is counted in UTF-16 code units (`String.length()`): a character outside the Basic Multilingual Plane, such as an emoji, counts as two, so the check is slightly stricter than the column and every id it accepts fits. Called in the id extractor, `of` fails the command inside the bus before any interceptor runs: nothing is audited, appended or dead-lettered. Map the exception to a `400`, as you would for a blank id. Bind the client value as a `String`, not as an `AggregateId` field: Jackson builds the record through its canonical constructor, which is the *decode* path and keeps only the blank rule, because the framework rebuilds stored ids through it. Use the constructor yourself only for an id derived from data already stored, such as a saga's state (see [Sagas](advanced/saga.md)).

---

## Execute Commands and Inspect Results

`StreamRune.execute(command)` loads the aggregate history, runs `decide`, appends the
resulting events, and returns a `CommandResult`:

```java
import org.streamrune.core.CommandBus.CommandResult;

// Create an order
CommandResult result = streamRune.execute(
    new OrderCommand.CreateOrder("ord-1", "cust-42"));

System.out.println(result.events());         // [OrderCreated[orderId=ord-1, ...]]
System.out.println(result.streamId());       // order:ord-1
System.out.println(result.finalVersion());   // Version[value=1]
System.out.println(result.globalOffsets());  // [GlobalOffset{1}]

// Confirm the same order
streamRune.execute(new OrderCommand.ConfirmOrder("ord-1"));

// Attempt a duplicate confirmation — throws DomainException
try {
    streamRune.execute(new OrderCommand.ConfirmOrder("ord-1"));
} catch (DomainException e) {
    System.out.println("Rejected: " + e.getMessage());
}
```

`CommandResult` fields:

| Field | Type | Description |
|---|---|---|
| `events()` | `List<? extends DomainEvent>` | Events produced by this command |
| `streamId()` | `StreamId` | The aggregate stream that was updated |
| `finalVersion()` | `Version` | Stream version after the append |
| `globalOffsets()` | `List<GlobalOffset>` | Position of each event in the global log |
| `envelopes()` | `List<EventEnvelope>` | The appended events with their metadata |
| `reason()` | `ShortCircuitReason` | `NONE` (executed), `VETOED` (rejected — no events), or `IDEMPOTENT_REPLAY` (a prior keyed execution's events — a success, not a rejection). See [API reference §2.3](../reference/api.md#23-commandbus) and [Idempotency](advanced/idempotency.md) |

---

## Add a Projection

A projection implements `Projection.process(List<EventEnvelope>)` and builds a read
model. Here is a simple in-memory order registry:

```java
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.projection.Projection;

public class OrderRegistry implements Projection {

  private final Map<String, String> statusByOrderId = new ConcurrentHashMap<>();

  @Override
  public void process(List<EventEnvelope> batch) {
    for (var envelope : batch) {
      switch (envelope.event()) {
        case OrderEvent.OrderCreated e   -> statusByOrderId.put(e.orderId(), "NEW");
        case OrderEvent.OrderConfirmed e -> statusByOrderId.put(e.orderId(), "CONFIRMED");
        case OrderEvent.OrderCancelled e -> statusByOrderId.put(e.orderId(), "CANCELLED");
        default -> {} // ignore unrelated events
      }
    }
  }

  public String statusFor(String orderId) {
    return statusByOrderId.get(orderId);
  }
}
```

> **Note:** `OrderRegistry` is registered `AT_LEAST_ONCE_IDEMPOTENT` — a `ConcurrentHashMap.put`
> of the same status is idempotent, so a redelivered batch leaves the registry as it was. The runner
> hands an `AT_LEAST_ONCE_IDEMPOTENT` projection no transaction-scoped repository, so the one-arg
> `process(List)` above is all it needs. In a framework integration the same bean declares
> `@ProjectionConfig(name = "order_registry", deliveryMode = AT_LEAST_ONCE_IDEMPOTENT)`; for a
> PostgreSQL read model extend `org.streamrune.core.projection.BaseProjection` over the
> `JdbcProjectionRepository` bean and declare `TRANSACTIONAL_LOCAL` — its `save()`/`findById()`/`delete()`
> helpers then work in the same transaction as the offset checkpoint. See
> [Delivery modes](concepts.md#delivery-modes--what-a-projection-promises).

Registering a projection is not enough on its own: the builder also needs a **projection
runner**. `build()` throws `IllegalStateException` when a projection is registered and no runner
is configured, because that projection would never see an event. Supply one with
`projectionRunnerFactory(...)`, which receives the built event store
and the subscription config and returns a runner such as `ContinuousProjectionRunner`. The factory
is called once per registered projection, so each projection gets its own runner instance, and
`startProjections()` runs each one on its own virtual thread.

A runner needs an `OffsetStore` to remember how far it has read, and an `AtomicBatchProcessor` —
required, no default — that says whether a batch's read-model writes and its checkpoint commit
together. The framework ships a PostgreSQL-backed `PostgresOffsetStore`; for a database-free example
use `org.streamrune.test.InMemoryOffsetStore` from `streamrune-test` (test scope, or write the
twelve lines yourself — `OffsetStore` is a small interface). `OrderRegistry` writes to its own map,
so the runner gets `AtomicBatchProcessor.nonAtomicAtLeastOnce()`: `process`, then the checkpoint
save, two commits.

Register the projection, wire the runner, then start projections after `build()`:

```java
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.runtime.ContinuousProjectionRunner;
import org.streamrune.test.InMemoryOffsetStore;

var registry = new OrderRegistry();
var offsetStore = new InMemoryOffsetStore();

var streamRune = StreamRune.builder()
    .eventStore(eventStore)
    .register(ORDER, OrderCommand.class, orderId, new OrderDecider())
    .registerProjection("order_registry", registry, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    // Required: build() refuses a registered projection that has no runner.
    .projectionRunnerFactory((store, config) ->
        ContinuousProjectionRunner.builder()
            .eventStore(store)
            .offsetStore(offsetStore)
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce()) // required; the named non-transactional choice
            .subscriptionConfig(config)
            .build())
    .build();

streamRune.startProjections();   // now the runner drives "order_registry"
```

---

## Framework Integration (Brief)

StreamRune integrates with Spring Boot, Quarkus, and Micronaut through dedicated modules
that auto-configure the event store, command bus, and projections from application
properties. Add the appropriate starter and the integration wires everything up — no manual
`builder()` call required.

The injectable command sender is **`org.streamrune.core.CommandBus`** (each integration produces
a `VirtualThreadCommandBus` that implements it). There is no injectable `StreamRune` bean — the
`StreamRune` facade is the programmatic bootstrap API only. Inject `CommandBus` and call
`bus.execute(command)`:

```java
// Spring
@RestController
@RequestMapping("/orders")
class OrderController {
  private final org.streamrune.core.CommandBus bus;
  OrderController(org.streamrune.core.CommandBus bus) { this.bus = bus; }

  @PostMapping
  String place(@RequestBody OrderCommand.CreateOrder cmd) {
    return bus.execute(cmd).streamId().toString();
  }
}
```

The same `CommandBus` injection works in Quarkus (`@jakarta.inject.Inject`) and Micronaut
(constructor injection).

| Framework | Starter module |
|---|---|
| Spring Boot | `streamrune-spring` |
| Quarkus | `streamrune-quarkus` |
| Micronaut | `streamrune-micronaut` |

See the [streamrune-integration README](../../streamrune-integration/README.md) for
configuration properties and examples. The
[ecommerce demo](https://github.com/StreamRune/streamrune-ecommerce-demo) — a separate repository —
shows all three frameworks wired end to end.

---

## Shutdown

`StreamRune` implements `AutoCloseable`. Use try-with-resources or call `close()` on
shutdown:

```java
streamRune.close();   // stops projections, closes the command bus, releases threads
```

This applies to the programmatic facade. When you use one of the framework integrations you do
not build or close `StreamRune` yourself — the integration owns the lifecycle of the command bus
and projections and shuts them down with the application context.

---

## What Next?

| Guide | Description |
|---|---|
| [concepts.md](concepts.md) | Deep-dive on CQRS/ES theory |
| [testing.md](testing.md) | Unit, integration, and fixture-based tests |
| [advanced/idempotency.md](advanced/idempotency.md) | Effectively-once command execution with `IdempotencyKey` |
| [advanced/gdpr-erasure.md](advanced/gdpr-erasure.md) | Crypto-shredding and GDPR right-to-erasure |
| [../QUICKSTART.md](../QUICKSTART.md) | Five-minute copy-paste bootstrap |
| [streamrune-ecommerce-demo](https://github.com/StreamRune/streamrune-ecommerce-demo) | Full demo with Spring, Quarkus, and Micronaut (separate repository) |
