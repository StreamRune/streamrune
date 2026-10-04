# StreamRune Core

## Overview

StreamRune Core provides the foundational abstractions for event-sourced systems in Java: the `EventStore` contract, aggregate identity and versioning types, the event envelope that wraps domain events with metadata, and a schema-migration system via upcasters. It also defines the contracts of the read and process side — projections (`Projection`, `ProjectionRunner`, `OffsetStore`, windowed results), queries (`QueryBus`, `QueryHandler`, `Cacheable`), sagas (`SagaDecider`, `SagaOrchestrator`, `SagaStore`), subscriptions, the transactional outbox, auditing, GDPR subject export/purge and the `CryptoEngine` SPI. The implementations live in `streamrune-runtime`, `streamrune-postgres` and the other modules. Its only dependencies are Jackson (`jackson-annotations` and `jackson-databind`, both exposed as `api`).

## Key Interfaces

**`StreamId`** — the typed identity of one stream: the pair `(AggregateType, AggregateId)`, shown as `<type>:<id>` (for example `order:ord-42`) in Java, JSON and logs and stored as the two columns `aggregate_type` and `aggregate_id`. Two aggregate types may share an id value without sharing a stream. `AggregateType` is a validated name (`[a-z][a-z0-9_]{0,31}`) that every decider registration declares.

**`EventStore`** — the central persistence contract. Implementations append events to an append-only stream and support point-in-time reconstruction of aggregate state.

```java
interface EventStore {
    AggregateHistory load(StreamId id);
    AppendResult append(StreamId id, List<EventEnvelope> events, Version expectedVersion);
    void saveSnapshot(StreamId id, Version version, AggregateState state);
    List<EventEnvelope> readStream(StreamId id, Version afterVersion, int maxCount);
    List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount);
}
```

**`AggregateHistory`** — returned on `load`; contains the last snapshot state (if any), the events applied since that snapshot, the current version, and the version at which the last snapshot was taken.

**`Decider<C, S, E>`** — pure domain logic with three type parameters: `C` the command type (extends `Command`), `S` the aggregate state (extends `AggregateState`), `E` the event type (extends `DomainEvent`). `initialState()` seeds a fresh aggregate, `decide(command, state)` returns the `List<E>` of events to append (an empty list for a valid no-op, never `null`), and `evolve(state, event)` returns the next state. State is never mutated in place; `evolve` returns a new instance. An optional `guard(command, state)` hook runs before `decide` for authorization checks.

**`EventUpcaster`** — schema migration for event payloads. Exactly one upcaster owns all version steps of one event type (`UpcasterChain` rejects a second one for the same type). When an event is loaded at a stored schema version below the upcaster's `currentVersion()`, the chain calls `upcast(eventData, fromVersion)` once per step — each call transforms the payload map from `fromVersion` to `fromVersion + 1` — and the result is bound to the event class. Upcasting never drops an event. Event types without an upcaster are schema version 1.

**`CommandBus`** — executes commands by resolving the appropriate `Decider` for the target aggregate, loading history, applying the decider, and appending the resulting events. Plugins are exposed via `CommandInterceptor` for cross-cutting concerns such as logging, validation, or metrics.

**`EventTypeRegistry`** — resolves the logical event and state type names stored with each event/snapshot to the Java classes they deserialize into. `SimpleEventTypeRegistry` is the builder-based implementation (`registerEvent(name, class)`, `registerState(name, class)`); it is immutable after `build()`.

## Usage

Define your commands, domain events and aggregate state as immutable value types implementing `Command`, `DomainEvent` and `AggregateState`. Implement `Decider` for each aggregate:

```java
public sealed interface OrderCommand extends Command permits PlaceOrder {}
public record PlaceOrder(String orderId, String product, int qty) implements OrderCommand {}

public sealed interface OrderEvent extends DomainEvent permits OrderPlaced {}
public record OrderPlaced(String orderId, String product, int qty) implements OrderEvent {}

public record OrderState(String product, int qty) implements AggregateState {
    public static final OrderState EMPTY = new OrderState(null, 0);
}

public class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {
    @Override
    public OrderState initialState() {
        return OrderState.EMPTY;
    }

    @Override
    public List<OrderEvent> decide(OrderCommand command, OrderState state) {
        return switch (command) {
            case PlaceOrder c when state.product() == null ->
                List.of(new OrderPlaced(c.orderId(), c.product(), c.qty()));
            case PlaceOrder c -> List.of(); // already placed: a valid no-op
        };
    }

    @Override
    public OrderState evolve(OrderState state, OrderEvent event) {
        return switch (event) {
            case OrderPlaced e -> new OrderState(e.product(), e.qty());
        };
    }
}
```

Wire the `EventStore` implementation and `CommandBus` via a factory or DI container. Build the `EventTypeRegistry` with every concrete event and state type before the store is created.


## Installation

```groovy
// build.gradle
implementation("org.streamrune:streamrune-core:1.0.0-alpha-SNAPSHOT")
```

```kotlin
// build.gradle.kts
implementation("org.streamrune:streamrune-core:1.0.0-alpha-SNAPSHOT")
```

> `1.0.0-alpha-SNAPSHOT` is an unreleased preview, published only to the Maven Central snapshot
> repository: add that repository as shown in [Preview builds](../README.md#preview-builds). See
> the [CHANGELOG](../CHANGELOG.md) for what the preview contains.

## Best Practices

- Keep aggregate state immutable; each `evolve` call returns a new instance.
- Snapshots should be saved at deterministic version intervals (e.g., every 100 events) via the `SnapshotPolicy` hook.
- Register each event type's class in its current shape: upcasters transform the stored payload map before it is bound to that class. The PostgreSQL and in-memory event stores refuse to read an event stored at a schema version above what their upcaster chain reaches, rather than mis-binding the payload — register the same upcasters on every store that reads the data.
- Write every upcaster step so it keeps what the payload already has. An event is stamped with the schema version its writing store's chain had for the type at append time (1 with no upcaster), not with its payload's shape, so an event appended before its upcaster was registered reaches the 1→2 step even if it already carries the new field: use `putIfAbsent` for a new field with a default, not `put`.
- For production deployments, use the retry and locker policies to handle transient failures and optimistic-lock conflicts.
- Use `GlobalOffset` for cross-aggregate projections only; per-stream `Version` is sufficient for aggregate reads.

## Known Limitations

- No transaction coordination across multiple aggregate streams in a single append: `append` writes one stream. Cross-aggregate workflows go through sagas.
- Snapshot persistence is delegated to the `EventStore` implementation; the core API does not prescribe a snapshot store.
