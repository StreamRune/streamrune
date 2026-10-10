# Concepts: CQRS and Event Sourcing in StreamRune

Before writing any code, this guide explains the model StreamRune is built on. Read it
top to bottom if you are new to event sourcing — it lays the vocabulary used throughout
every other guide. If you are evaluating StreamRune and already know ES/CQRS, skip ahead
to [The Decider Pattern](#the-decider-pattern).

---

## Why Event Sourcing?

**Auditability.** Traditional databases store the current state of a row. When the row
changes, the previous value is gone. Event sourcing stores every state change as an
immutable fact — an event — appended to a log that never shrinks. Any past state can be
reconstructed by replaying the log up to a chosen point in time.

**Temporal queries.** Because the full history is preserved, questions like "What did
order `ord-42` look like at 14:00 last Tuesday?" are straightforward: replay the event
stream for `ord-42` and stop at the chosen timestamp. Answering the same question from a
CRUD store usually requires either audit triggers, log mining, or accepting that the
answer is gone forever.

**Decoupling.** Projections subscribe to the global event stream independently of the
write model. A new reporting view, a search index, or a real-time dashboard can be added
without touching the domain code. Because projections are rebuilt from the same immutable
stream, they can be torn down, fixed, and caught up at any time.

**Contrast with CRUD.** A CRUD system issues `UPDATE orders SET status = 'CONFIRMED' WHERE
id = 'ord-42'`, discarding all trace of the previous status. An event-sourced system
appends `OrderConfirmed(orderId="ord-42")` to the stream, preserving the entire
transition history alongside the new fact.

StreamRune gives you these properties without ceremony — a `Decider` is three method
overrides.

---

## Core Concepts: Commands, Events, and Aggregates

### Commands

A command is an expression of intent — something a user or system *wants* to happen. It
can be rejected. Commands are named in the imperative: `CreateOrder`, `ConfirmOrder`,
`CancelOrder`. The `decide` method receives a command and either produces events (success)
or throws a `DomainException` (business rule violation).

Every command type must implement the marker interface `org.streamrune.core.Command`. It
is a pure marker with no methods — the framework uses it to bound the type parameters on
`Decider` and the command bus. The idiomatic shape is a sealed interface that extends
`Command`, with one record per variant.

### Events

An event is a fact that *already happened* — it is immutable and recorded forever. Events
are named in the past tense: `OrderCreated`, `OrderConfirmed`, `OrderCancelled`. Once
written to the event store, events are never updated or deleted.

### Aggregates

An aggregate is the consistency boundary around a stream of events that share a single
`StreamId`. All reads and writes for one order go through the stream `order:ord-42`: the
aggregate type you register (`order`) plus the id your extractor returns (`ord-42`). Two
aggregate types may use the same id value — `product:p-1` and `inventory:p-1` are two streams,
with their own version counters, locks and snapshots. There is no shared locking between
different aggregates — two orders can be updated concurrently without interfering with each other.

A `StreamId` is the typed pair `(AggregateType, AggregateId)`; read the parts with
`aggregateType()` and `aggregateId()`. `order:ord-42` is how Java, JSON and logs show the pair;
the database stores the two parts as two columns (`aggregate_type`, `aggregate_id`) and no table
stores the text `order:ord-42`.

### Example types

```java
// Command hierarchy: intent — can be rejected. The sealed interface extends the
// org.streamrune.core.Command marker so the framework can bound its type parameters.
sealed interface OrderCommand extends org.streamrune.core.Command
    permits CreateOrder, ConfirmOrder, CancelOrder {}

record CreateOrder(String orderId, String customerId, int itemCount)
    implements OrderCommand {}

// Event: what happened, recorded forever, named in the past tense
record OrderCreated(String orderId, String customerId, int itemCount)
    implements OrderEvent {}

// State: derived from events — never stored directly in the event store
record OrderState(String orderId, OrderStatus status) implements AggregateState {}
```

The sealed interface pattern — `sealed interface OrderEvent permits OrderCreated,
OrderConfirmed, OrderCancelled` — means the Java compiler warns when a switch on events
has a missing variant. This is one of the reasons StreamRune recommends sealed interfaces
for both command and event hierarchies.

---

## The Decider Pattern

Domain logic in StreamRune lives in a `Decider`. It is a pure function: no database calls,
no HTTP requests, no static mutable state, no `Thread.sleep`. Given a command and a state
it produces events. Given a state and an event it produces the next state. That is all.

### The `Decider` interface

```java
public interface Decider<C extends Command, S extends AggregateState, E extends DomainEvent> {

  /** Returns the initial state before any events have been applied. */
  S initialState();

  /** Evaluates a command against the current state, producing zero or more events. */
  List<E> decide(C command, S state);

  /** Applies a single event to the current state, returning the new state. */
  S evolve(S state, E event);

  /** Authorization hook, called after state is loaded and before decide. Permits by default. */
  default void guard(C command, S state) {}
}
```

Three methods to implement:

| Method | Signature | Purpose |
|---|---|---|
| `initialState` | `() → S` | Provides the zero-event baseline state |
| `decide` | `(C, S) → List<E>` | Business logic: validate + emit events |
| `evolve` | `(S, E) → S` | State machine: fold one event into the state |

### A complete `OrderDecider`

```java
public class OrderDecider implements Decider<OrderCommand, OrderState, OrderEvent> {

  @Override
  public OrderState initialState() {
    return new OrderState(null, OrderStatus.NEW);   // no order yet: no id
  }

  @Override
  public List<OrderEvent> decide(OrderCommand cmd, OrderState state) {
    return switch (cmd) {
      case OrderCommand.CreateOrder c -> {
        if (state.orderId() != null)
          throw new DomainException("Order " + c.orderId() + " already exists");
        yield List.of(new OrderEvent.OrderCreated(c.orderId(), c.customerId(), c.itemCount()));
      }

      case OrderCommand.ConfirmOrder c -> {
        if (state.status() != OrderStatus.NEW)
          throw new DomainException(
              "Can only confirm NEW orders; current: " + state.status());
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
      case OrderEvent.OrderCreated e  -> new OrderState(e.orderId(), OrderStatus.NEW);
      case OrderEvent.OrderConfirmed e -> state.withStatus(OrderStatus.CONFIRMED);
      case OrderEvent.OrderCancelled e -> state.withStatus(OrderStatus.CANCELLED);
    };
  }
}
```

`CreateOrder` first checks that the order does not exist yet: the bus runs `decide` whether or not
the stream already has events, so without the check a second `CreateOrder` for the same id would
append another `OrderCreated` and reset the order.

The optional `guard(command, state)` hook runs on the loaded state before `decide` — override it
for ownership checks (see [Authorization](advanced/authorization.md#ownership-checks-in-deciderguard)).
It also runs for the command that creates the aggregate, where `state` is still `initialState()`, so
branch on the creation command rather than requiring an owner unconditionally. And it runs for
commands a saga dispatches, which carry no user: `Authorization.requireOwner` denies those, so use
`Authorization.requireOwnerOrSaga` for every command a saga sends to the aggregate.

Because `decide` and `evolve` are plain Java methods with no dependencies, you can test
them with a single `new OrderDecider().decide(cmd, state)` call — no Spring context, no
database, tests start in milliseconds.

---

## Read Models and Projections

A projection consumes a sequence of events and builds a query-optimized view — a table,
a document, or an in-memory map. Projections are independent of the write model and can
be rebuilt at any time by replaying the event stream from the beginning.

### The `Projection` interface

```java
public interface Projection {

  /** Processes a batch of events. Must be idempotent. */
  void process(List<EventEnvelope> batch);
}
```

The baseline contract requirement is **idempotency**: processing the same event twice must
produce the same result. Use upsert semantics in projection repositories rather than plain
inserts so that a redelivered event does not create a duplicate row. A `TRANSACTIONAL_LOCAL`
projection is protected from applying a redelivered batch twice by its checkpoint transaction, but a
[dead-letter replay](advanced/retry-and-resilience.md#projection-dead-letter-replay) re-applies a
range under every [delivery mode](#delivery-modes--what-a-projection-promises), so idempotency stays
the baseline.

Idempotency covers re-application, not concurrency. Two writers that each read a row, change it and
save it lose one of the two changes even when neither applies anything twice. The framework keeps
that from happening to a projection: every batch and every dead-letter replay of one projection
name runs under that projection's checkpoint lock in the runner's `AtomicBatchProcessor`, so they
never interleave. A projection therefore needs no locking of its own for its own rows, as long as
nothing but its runner and the replayer writes them.

**Two `process` overloads.** `Projection` also declares `process(List<EventEnvelope>,
ProjectionRepository)`, whose default ignores the repository and calls the one-arg `process` above.
A runner always calls the two-argument form: for a `TRANSACTIONAL_LOCAL` or `EXTERNAL_EFFECT`
registration it hands the processor's transaction-scoped repository, for an
`AT_LEAST_ONCE_IDEMPOTENT` registration it hands `null`. `org.streamrune.core.projection.BaseProjection`
overrides it and routes its `save()`/`findById()`/`delete()` helpers through the handed repository,
or through the repository it was constructed with when it is handed `null`. Which of the two a
registration gets is its [delivery mode](#delivery-modes--what-a-projection-promises). Inside
`process`, write only through these helpers: the `repository` field is the repository the
projection was constructed with, outside the checkpoint transaction, so a `repository.delete(...)`
there misses a row the same batch inserted and, on JDBC, can wait forever on a row lock the batch
holds.

**Projection names under `JdbcProjectionRepository`.** Each name is stored in its own table,
`<name>_view`, created by the first read or write of that name through the repository, or by the
runner before the first batch of a `TRANSACTIONAL_LOCAL` / `EXTERNAL_EFFECT` registration. An
`AT_LEAST_ONCE_IDEMPOTENT` registration that keeps its read models elsewhere gets no table: the
processor only commits its checkpoint. A name must match `[a-z_][a-z0-9_]{0,57}`: lower-case ASCII letters, digits and
underscores, starting with a letter or an underscore, at most 58 characters. The name is used
exactly as given, never rewritten, so no two projection names share a table — `order-summary` or
`OrderSummary` is rejected with an `IllegalArgumentException`, not mapped onto `order_summary_view`.
The runners check the name a projection is registered under through
`AtomicBatchProcessor.checkProjectionName` when they are built (`MultiProjectionRunner`,
`ScheduledProjectionRunner`) or when `run()` starts (`ContinuousProjectionRunner`,
`PollingProjectionRunner`), so a bad name fails at startup, in the three integrations too. Through
the `StreamRune` facade, `startProjections()` runs each `ContinuousProjectionRunner` on a thread of
its own, so there a bad name is logged at ERROR when that thread starts (still before any event is
read) and `startProjections()` itself returns normally. The name a `BaseProjection` saves under is
checked on its first write; its class-derived default is snake_case (`CartViewProjection` →
`cart_view`, `OrderProjection` → `order`) and always valid, and a class name with no valid
snake_case form fails in the constructor. The runner builders and the integrations refuse a name
registered twice within one runner or one application context. As long as each projection saves
under the name it is registered under, that also refuses two projections that would write one
table; a projection that saves under another name, or two runners in one JVM, are not covered.

**Wrapping a projection.** A decorator around another projection (tracing, validation, cache
invalidation) must forward more than `process`: it must also override `writesThroughRepository()`,
`writeTarget()` and `processDeadLetterReplay(List, ProjectionRepository)` and return its delegate's
answer. Inherited,
each answers for the wrapper instead of the projection it wraps — the reflective
`writesThroughRepository()` default inspects the wrapper's own class, so a wrapper that forwards the
two-arg `process` reads as write-through even around a projection that is not; the default
`writeTarget()` is empty, so the startup check can no longer verify that the delegate writes to the
runner's processor; and the default `processDeadLetterReplay`
reports "applied" for a dead-lettered range a self-fencing delegate ignored, so the
[dead-letter replayer](advanced/retry-and-resilience.md#projection-dead-letter-replay) discards the
range's only record. The shipped decorators (`TracingProjectionDecorator`,
`ValidatingProjectionDecorator`, `CacheAwareProjection`) forward all of them.

### Delivery modes — what a projection promises

Every projection registration declares a `ProjectionDeliveryMode`: on a runner's `register(...)`,
on `StreamRune.Builder.registerProjection(name, projection, mode)`, on
`ProjectionRunner.run(name, projection, mode)` and as the required `@ProjectionConfig(deliveryMode =
…)` attribute in the Spring, Quarkus and Micronaut integrations. There is no default: which value is
right depends on where the projection writes, never on which beans exist.

| mode | what the framework guarantees | what the projection promises | typical write target |
|---|---|---|---|
| `TRANSACTIONAL_LOCAL` | the batch's read-model writes and the checkpoint are all-or-nothing; a redelivered or split-brain batch is rejected before any write | writes only through the handed repository during `process` | `JdbcProjectionRepository` (`<name>_view` tables) in the framework's database |
| `AT_LEAST_ONCE_IDEMPOTENT` | the checkpoint advances only after `process` returned; a crash or a rejected checkpoint redelivers the batch from the last checkpoint; the projection is never handed a transaction-scoped repository | applying the same event twice yields the same read model | anything: another `DataSource`, JPA, Redis, memory — or the framework's own `JdbcProjectionRepository`, written autocommit through the projection's captured reference |
| `EXTERNAL_EFFECT` | as `TRANSACTIONAL_LOCAL`, for the outbox row | no external call in `process`; the receiver dedups by event id | an outbox row through the handed repository, relayed later by the application's relay |

**Choosing a mode:**

| the projection… | mode |
|---|---|
| extends `BaseProjection` over the `JdbcProjectionRepository` the app passes as the processor | `TRANSACTIONAL_LOCAL` |
| extends `BaseProjection` over any *other* repository (another database, an in-memory repository, a second `JdbcProjectionRepository` with another `ObjectMapper`) | `AT_LEAST_ONCE_IDEMPOTENT` — the runner hands it nothing; it writes to its own store; it must upsert/dedup/version-guard. It may share a runner with `TRANSACTIONAL_LOCAL` siblings. Declaring it `TRANSACTIONAL_LOCAL` is refused (identity) |
| writes to JPA, Redis, a file, or memory without `BaseProjection` | `AT_LEAST_ONCE_IDEMPOTENT` — and it must upsert/dedup/version-guard |
| exists to call an HTTP API, produce to Kafka, send an e-mail | `EXTERNAL_EFFECT` — write an outbox row through the handed repository; relay it with a relay you provide; the receiver dedups by `eventId` |
| is a lambda or a plain `implements Projection` in a test | `AT_LEAST_ONCE_IDEMPOTENT` with `nonAtomicAtLeastOnce()` |
| is `@ProjectionConfig(mode = INLINE)` | `AT_LEAST_ONCE_IDEMPOTENT` — the only legal value; INLINE has no checkpoint |
| needs unit tests of a `TRANSACTIONAL_LOCAL` path without Docker | the same projection over `InMemoryProjectionRepository`, which is also the processor (see [Testing](testing.md#unit-testing-a-transactional_local-projection-without-postgresql)) |

**Hand-built runner recipe:**

```java
MultiProjectionRunner.builder()
    .eventStore(eventStore)
    .offsetStore(offsetStore)
    .atomicProcessor(jdbcRepo)                       // the JdbcProjectionRepository the TRANSACTIONAL_LOCAL projections write to
    .register("orders", new OrderProjection(jdbcRepo), ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
    .register("audit",  auditProjection,              ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    .build();
```

The same `jdbcRepo` object in both places is what the startup check verifies; `auditProjection` is
handed no repository and writes to its own store. (`OrderProjection` extends `BaseProjection` and
saves under the name it is registered under. `offsetStore` is a `PostgresOffsetStore` over the same
`DataSource` as `jdbcRepo`: the runner must read the checkpoint the processor advances. Nothing
checks that at startup; see [a processor and an offset store that disagree](#a-processor-and-an-offset-store-that-disagree).)

**Integration recipe:** declare the `JdbcProjectionRepository` bean by its concrete type (or accept
the framework default; any bean scope — identity survives proxies); annotate
`@ProjectionConfig(name = "orders", deliveryMode = TRANSACTIONAL_LOCAL)`; construct the projection
over that bean. Startup refuses anything else. The bean must use the `DataSource` whose
`projection_offset` table the runner's `OffsetStore` bean reads — the integrations build the
`PostgresOffsetStore` over the primary `DataSource`. Nothing checks that at startup.

#### A processor and an offset store that disagree

A processor over a separate read-model `DataSource` advances a checkpoint the runner never reads.
The first batch commits there; the runner reads its own `OffsetStore`, still at the old checkpoint,
reads the same batch again, and the processor rejects it as an overlap before any write. The runner
tolerates one such rejection — a commit whose acknowledgement was lost is rejected when it is
retried, and the re-read that follows starts after the moved checkpoint. A second consecutive
overlap or monotonic rejection while the checkpoint the runner reads has not moved **halts the
projection**: `ContinuousProjectionRunner` ends in `ProjectionState.ERROR`, `ScheduledProjectionRunner`
puts the registration in `ERROR`, the health contributor reports the subscription `DOWN`, and
`lastError` carries a `ProjectionCheckpointDivergedException` message naming the two likely causes:

- the `AtomicBatchProcessor` and the `OffsetStore` are not over the same `DataSource` (or the same
  `projection_offset` table);
- two runners are registered under one projection name and commit through one processor while
  reading their checkpoint from different offset stores.

The read model is not written twice: every rejected batch rolls back before its first write. Give
the runner an `OffsetStore` over the processor's `DataSource`, register the name once, and restart
the projection; it resumes from the checkpoint.

A rejection by the **epoch fence** is a different signal and never halts: it means a newer leader
holds the lease, and the superseded runner stands by.

An at-least-once projection is never handed the checkpoint transaction: the runner passes `null` to
`process(batch, repository)` under every processor, so its writes go where it sends them. A
transaction with the checkpoint makes the *local write* exactly-once; an HTTP call, a Kafka produce
or an e-mail inside `process` is at-least-once under every mode and belongs behind an outbox.

A runner without an `AtomicBatchProcessor` does not build — `AtomicBatchProcessor.nonAtomicAtLeastOnce()`
is the named non-transactional choice for a runner of `AT_LEAST_ONCE_IDEMPOTENT` projections without
leadership. It runs `process(batch, null)` and then saves the checkpoint: two commits. When that
save fails after `process` succeeded, the runner retries the batch from the unchanged checkpoint —
with backoff, or on the next tick for `ScheduledProjectionRunner` — under every error strategy; the
range is never skipped and never dead-lettered.

**When a batch fails.** The runner's `ProjectionErrorStrategy` decides. `HALT` retries the batch
from the checkpoint and stops the runner with an error once the same batch keeps failing. `DLQ`
records the batch's offset range in the dead-letter store and moves the checkpoint past it; the
[replayer](advanced/retry-and-resilience.md#projection-dead-letter-replay) applies the range later.
`SKIP` moves the checkpoint past the batch and records nothing. What a skipped or dead-lettered
batch leaves behind depends on the delivery mode: under `TRANSACTIONAL_LOCAL` and `EXTERNAL_EFFECT`
the failed batch's writes roll back, so the whole batch is missing from the read model; under
`AT_LEAST_ONCE_IDEMPOTENT` the writes the projection made before it threw stand, so the batch may be
**partially applied**. Prefer `DLQ` to `SKIP` for an at-least-once projection: the entry records the
range, and the replay applies all of it again, which an idempotent projection completes.

**What startup refuses.** Every runner checks each registration before it reads an event — at
`build()` (`MultiProjectionRunner`, `ScheduledProjectionRunner`) or when `run()` starts
(`ContinuousProjectionRunner`, `PollingProjectionRunner`) — and throws an `IllegalArgumentException`
naming the runner, the projection, the mode, the processor and the fix when a `TRANSACTIONAL_LOCAL`
or `EXTERNAL_EFFECT` registration:

- runs under a processor whose `supportsFencing()` is `false`, such as `nonAtomicAtLeastOnce()` —
  it cannot put the read-model write and the checkpoint in one transaction;
- does not write through the handed repository — its `process(List, ProjectionRepository)` is the
  inherited default, which ignores the repository and falls back to `process(List)`. Extend
  `BaseProjection`, or override the two-argument `process` and write through the repository it is
  given;
- exposes a `Projection.writeTarget()` (a `BaseProjection` returns the repository it was constructed
  with) that is not the processor's store. The two are compared through
  `ProjectionRepository.writeTargetIdentity()`, which a bean proxy forwards, so a proxied bean on
  either side still verifies. A `JdbcProjectionRepository`'s identity is its `(DataSource,
  ObjectMapper)` pair: a second repository over the same `DataSource` with another `ObjectMapper`
  (one without the crypto module, say) is a different store, because the processor's mapper would
  serialise the projection's rows.

Through the `StreamRune` facade, as with a bad name, a refusal from `run()` is logged at ERROR when
the projection's thread starts and `startProjections()` returns normally; the projection does not
run. An `AT_LEAST_ONCE_IDEMPOTENT` registration is accepted under every processor. Each executing runner
logs one INFO line per registration when it starts — `Projection '<name>' delivery mode <MODE> on
<processor> — <verdict>` — where the verdict says whether the write target was verified, only
declared (the projection exposes no `writeTarget()`), or at-least-once; for an at-least-once
projection that already writes to the processor's own store it adds that `TRANSACTIONAL_LOCAL` is
available at no cost. Independently of the modes, a runner under single-active-consumer leadership
needs a processor that fences (see
[Single-active-consumer leadership](production.md#single-active-consumer-leadership)).

### Execution modes

Every projection runs under one of three modes (`ProjectionConfig.Mode`):

| Mode | Runs on | Ordering | Recovery |
|---|---|---|---|
| `CONTINUOUS` | `MultiProjectionRunner`, polling the global stream | durable checkpoint; catches up after a crash | yes — offset replay |
| `SCHEDULED` | `ScheduledProjectionRunner`, cron-triggered drain | durable checkpoint; catches up after a crash | yes — offset replay |
| `INLINE` | the command's own thread (`InlineProjectionInterceptor`), immediately after its events commit | per-aggregate only, same JVM (see below) | **none** |

**INLINE's contract, explicitly:** post-commit (events are durable before a projection ever
runs), same-JVM (no cross-process coordination), and ordered per aggregate — the command bus
holds the target aggregate's lock for the whole inline projection call, so two commands
committed against the *same* aggregate apply their inline updates in commit order. Commands on
*different* aggregates give no ordering guarantee relative to each other. There is **no recovery
path**: a throwing `process()` is logged and swallowed, and the command still reports success —
the event log, not the projection, is the source of truth. Treat INLINE as a low-latency,
best-effort head start only. An INLINE registration therefore declares
`deliveryMode = AT_LEAST_ONCE_IDEMPOTENT`; `TRANSACTIONAL_LOCAL` and `EXTERNAL_EFFECT` are refused at
discovery, because INLINE runs with no checkpoint and no transaction of its own. For a read model
that must stay correct after a transient failure — an **authoritative** read model — register the
same projection logic under CONTINUOUS or SCHEDULED, declared `TRANSACTIONAL_LOCAL` over the
`JdbcProjectionRepository`; that runner's offset replay reconciles
whatever INLINE missed (see the [dead-letter runbook](production.md#operations-runbook-projection-dead-letters),
which covers CONTINUOUS/SCHEDULED only — INLINE has nothing equivalent).

### When to rebuild

Because the source of truth is the event log, a projection can always be reconstructed.
Drop the projection table, reset the checkpoint offset to zero, and let the runner replay
all events. This makes schema migrations for read models a non-event: just rebuild.

Use `OffsetStore.reset(projectionName)` to rewind the checkpoint — not `saveOffset`. The
PostgreSQL/JDBC offset stores reject any `saveOffset` that would move a checkpoint backward
(a defense-in-depth guard against a stale replica regressing another instance's progress; see
[Multi-replica deployment](production.md#multi-replica-deployment)), so an intentional rewind
needs `reset`'s distinct, unguarded code path instead.

Running more than one instance of the same projection is safe: single-active-consumer
leadership (also covered in the production guide) ensures only one instance processes and
advances a given projection's offset at a time, and the others sit in standby.

---

## CQRS: Command Side vs Query Side

Command Query Responsibility Segregation (CQRS) separates the paths through which
commands are written and queries are read.

**Command path:**
`StreamRune.execute(command)` → load event history → run `Decider.decide` → append events → done.

**Query path:**
HTTP request → read from projection table → return view. The event store is never touched
by a query.

```
 Client
   │
   ├─── Command ──► StreamRune.execute() ──► EventStore (append)
   │                      │
   │                 Decider.decide()
   │
   └─── Query   ──► ProjectionRepository.findById() ──► Projection table
```

The CQRS split is optional for simple domains. If a system has very few read patterns a
single `Decider` plus an in-memory query over recent events may suffice. The separation
pays off as query patterns diversify — each projection can be optimised independently
without touching the write model.

---

## Common Misconceptions

**ES is not event-driven messaging.** Domain events in the event store are internal to the
service. Publishing events to other services — for example via Kafka — is a separate
concern handled by the Outbox pattern. The event store is a durable log, not a message
broker.

**Aggregates are not database rows.** An aggregate stream has no shared locking with any
other stream. You cannot perform a single atomic transaction that touches two different
stream IDs. Cross-aggregate coordination uses Sagas or process managers.

**Events are immutable.** If a fact was recorded incorrectly, you append a correcting
event — for example `OrderLineQuantityAdjusted` — rather than modifying the original
event. History is never rewritten.

**`decide` and `evolve` must be pure.** Any I/O in these methods — database reads,
network calls, `Instant.now()` — breaks testability and reproducibility. Timestamps and
external identifiers should be passed in through the command, not fetched inside `decide`.

---

## What Next?

| Guide | Description |
|---|---|
| [getting-started.md](getting-started.md) | Build a running application from scratch |
| [testing.md](testing.md) | Unit, integration, and fixture-based testing |
| [advanced/idempotency.md](advanced/idempotency.md) | Effectively-once command execution with an idempotency key |
| [advanced/query-cache.md](advanced/query-cache.md) | Caching read-side queries with `@Cacheable` |
| [advanced/gdpr-erasure.md](advanced/gdpr-erasure.md) | Crypto-shredding and GDPR subject erasure |
| [../QUICKSTART.md](../QUICKSTART.md) | Five-minute copy-paste bootstrap |
| [streamrune-ecommerce-demo](https://github.com/StreamRune/streamrune-ecommerce-demo) (separate repository) | Full CQRS/ES demo with Spring, Quarkus, and Micronaut |
