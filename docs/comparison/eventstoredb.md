# StreamRune vs EventStoreDB / Kurrent

> EventStoreDB rebranded to Kurrent in 2024. This guide uses "EventStoreDB" for brevity after the first mention.

## At a Glance

- EventStoreDB / Kurrent is a purpose-built event store database; StreamRune is a Java framework that uses PostgreSQL as its event store. They operate at different layers of the stack.
- EventStoreDB has a built-in server-side projection engine (JavaScript); StreamRune runs projections in-process using plain Java.
- EventStoreDB provides competing consumers and persistent subscriptions natively; StreamRune Horizon 1 has polling-based subscriptions (`PollingEventSubscription`) and hybrid subscriptions, but no competing consumer protocol.
- EventStoreDB uses its own wire protocol (gRPC); StreamRune uses JDBC/SQL — any Java application that can reach PostgreSQL can use StreamRune without a new network protocol.
- StreamRune adds aggregate locking, snapshotting, saga orchestration, and outbox — features that EventStoreDB does not provide, leaving them to application code.

## Concept Mapping

| EventStoreDB | StreamRune | Notes |
|---|---|---|
| Stream (e.g. `order-1`) | `StreamId`-keyed event log in PostgreSQL (e.g. `order:order-1`) | Both key event history by a stream identifier; StreamRune's is the typed pair of aggregate type and aggregate id, stored as two columns |
| Global `$all` stream | `EventStore.readGlobalStream(afterOffset, maxCount)` | StreamRune uses a monotonic `global_offset` column in PostgreSQL |
| Persistent subscriptions | Not in Horizon 1 | Competing consumer semantics planned for future |
| Catchup subscriptions | `PollingEventSubscription` | StreamRune polls `readGlobalStream()` on a virtual thread |
| Push subscriptions | `HybridEventSubscription` | Hybrid uses PostgreSQL LISTEN/NOTIFY then falls back to polling |
| Server-side projections (JavaScript) | `BaseProjection` (Java, in-process) | StreamRune projections run in the same JVM, not the database |
| EventStoreDB cluster | PostgreSQL (primary + replicas) | StreamRune inherits PostgreSQL's HA story |
| `@json` content type | Jackson-serialized JSON in `payload` column | Both store events as JSON; StreamRune uses Jackson |
| Optimistic concurrency (`expectedRevision`) | `OptimisticLockException` on version mismatch | Same concept, different API surface |

## Key Differences

**1. Database vs. framework.**
EventStoreDB is a database. You deploy it as a server process (or cluster), connect your application via a gRPC client, and call its API to append and read events. StreamRune is a Java framework: it runs inside your JVM and persists events to a PostgreSQL schema it manages (via Flyway migrations). Choosing EventStoreDB means adding a new stateful service to operate, monitor, back up, and scale. Choosing StreamRune means extending your existing PostgreSQL operational footprint.

**2. Projection execution model.**
EventStoreDB's projection engine is built into the server and runs JavaScript. It can compute derived state across streams server-side without any client application. StreamRune projections (`BaseProjection`, `ProjectionRunner`) are Java classes that run in-process inside the application. This means StreamRune projections benefit from the full Java type system and your existing domain model classes — but they require the application to be running to process events.

**3. Subscription models.**
EventStoreDB supports competing consumers (persistent subscriptions): multiple consumer instances each claim a slot and process events in parallel, with acknowledgement and retry built into the server protocol. StreamRune Horizon 1 does not implement competing consumers. `PollingEventSubscription` runs a single consumer per projection. For horizontal scaling of event consumption, teams must partition by stream or use the outbox pattern to fan out to a message broker.

**4. Operational dependency profile.**
A StreamRune application depends on PostgreSQL — a well-understood, ubiquitous database with managed offerings on every major cloud (AWS RDS, Cloud SQL, Azure Database for PostgreSQL). Local development needs a single Docker container. EventStoreDB requires its own container or managed instance (EventStoreDB Cloud / Kurrent Cloud), its own backup strategy, its own monitoring dashboards, and familiarity with its administration UI.

**5. Framework completeness.**
EventStoreDB is purely a storage engine. Aggregate locking, snapshotting, optimistic concurrency (beyond version checks), saga orchestration, outbox polling, command interceptors, and circuit breakers are not EventStoreDB concerns — they are left to application code or other libraries. StreamRune bundles all of these as first-class features built on PostgreSQL.

## Code Comparison

### Appending events

**EventStoreDB (Java client):**
```java
var client = EventStoreDBClient.create(settings);
var event = EventData.builderAsJson(
    UUID.randomUUID(),
    "OrderPlaced",
    new OrderPlaced("order-1", "customer-1")
).build();
client.appendToStream(
    "order-1",
    AppendToStreamOptions.get().expectedRevision(ExpectedRevision.noStream()),
    event
).get();
```

**StreamRune:**
```java
// Append is handled by VirtualThreadCommandBus — no manual envelope construction
var result = commandBus.execute(new PlaceOrder("order-1", "customer-1"));
// result.events() → List<OrderEvent>
// result.globalOffsets() → List<GlobalOffset> (assigned by PostgreSQL)
```

### Reading from the global stream

**EventStoreDB:**
```java
var result = client.readAll(
    ReadAllOptions.get()
        .fromPosition(Position.END)
        .direction(Direction.Forwards)
        .maxCount(100)
).get();
result.getEvents().forEach(e -> process(e));
```

**StreamRune:**
```java
List<EventEnvelope> events = eventStore.readGlobalStream(
    GlobalOffset.of(lastProcessedOffset),
    100
);
events.forEach(envelope -> handle(envelope));
```

### Defining a subscription

**EventStoreDB (catch-up):**
```java
client.subscribeToAll(
    (subscription, event) -> handleEvent(event),
    SubscribeToAllOptions.get().fromPosition(Position.START)
);
```

**StreamRune:**
```java
var subscription = PollingEventSubscription.builder()
    .subscriptionName("order-projection")
    .eventStore(eventStore)
    .offsetStore(offsetStore)
    .listener(batch -> batch.forEach(this::handleEnvelope))
    .config(SubscriptionConfig.DEFAULT)
    .fetchSize(100)
    .build();
subscription.start();
```

## When to Choose StreamRune

- PostgreSQL is already in your operational stack. You want event sourcing without adding a new database technology to learn, deploy, and operate.
- You want projections written in plain Java using your existing domain model — no separate JavaScript projection runtime.
- You need a complete framework: aggregate locking, snapshotting, saga orchestration, outbox pattern, and circuit breakers are all included.
- Your team prefers SQL for debugging: events stored in PostgreSQL can be inspected with `psql`, `pgAdmin`, or any SQL client.
- Local development simplicity matters: one `docker run postgres` is the entire infrastructure requirement.

## When to Choose EventStoreDB

- The global event log is a first-class requirement. EventStoreDB is purpose-built for this; its storage engine is optimized for append-only sequential I/O in ways PostgreSQL (a general-purpose RDBMS) is not.
- You need server-side projections that run independently of your application — EventStoreDB's projection engine can compute derived state without client applications running.
- You need competing consumers with server-managed acknowledgement, retry, and dead-letter queuing built into the event store protocol.
- Your team already operates EventStoreDB or has experience with it. The operational cost is already paid.
- You have very high append throughput requirements where EventStoreDB's write-optimized storage engine may outperform PostgreSQL's general-purpose B-tree indexes.
