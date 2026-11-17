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
registration gets is its [delivery mode](#delivery-modes--what-a-projection-promises).

A projection declares that it writes through the handed repository with
`Projection.writesThroughRepository()`: `false` by default, `true` in `BaseProjection`. **A
projection that overrides the two-argument `process` itself, without extending `BaseProjection`,
must also override `writesThroughRepository()` to return `true`.** The runner takes that answer at
its word: it is not read back from the class, because the object a container hands the runner can be
a generated subclass of the bean (an interceptor subclass in Quarkus or Micronaut) whose class
declares the two-argument `process` whether or not the projection does. A `TRANSACTIONAL_LOCAL` or
`EXTERNAL_EFFECT` projection that does not declare it is refused at startup.

Inside `process`, write only through these helpers: the `repository` field is the repository the
projection was constructed with, outside the checkpoint transaction, so a `repository.delete(...)`
there misses a row the same batch inserted and, on JDBC, can wait forever on a row lock the batch
holds.

**Projection names under `JdbcProjectionRepository`.** Each name is stored in its own table,
`<name>_view`, created by the first read or write of that name through the repository, by
`declareIndex` (see [Reading read models](#reading-read-models-pages-filters-and-indexes)), or by the
runner before the first batch of a `TRANSACTIONAL_LOCAL` / `EXTERNAL_EFFECT` registration; replicas
that create it at once all succeed. An
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
each answers for the wrapper instead of the projection it wraps — the default
`writesThroughRepository()` is `false`, so a wrapper that forwards the two-arg `process` reads as
not writing through even around a projection that does; the default
`writeTarget()` is empty, so the startup check cannot verify that the delegate writes to the
runner's processor; and the default `processDeadLetterReplay`
reports "applied" for a dead-lettered range a self-fencing delegate ignored, so the
[dead-letter replayer](advanced/retry-and-resilience.md#projection-dead-letter-replay) discards the
range's only record. The shipped decorators (`TracingProjectionDecorator`,
`ValidatingProjectionDecorator`, `CacheAwareProjection`) forward all of them.

### Reading read models: pages, filters and indexes

A query handler reads a projection's rows through its `ProjectionRepository`: one row by id
(`findById`), every row (`findAll(name, type)`), one page of every row in a sort order
(`findAll(name, type, pageRequest)`), or one page of the rows whose top-level field holds a value
(`findAllBy(name, type, filter, pageRequest)`):

```java
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.projection.FieldEquals;

Sort newestFirst = Sort.by("createdAt", SortDirection.DESC);
Page<OrderView> mine =
    repository.findAllBy(
        ORDERS, OrderView.class, FieldEquals.of("customerId", customerId),
        PageRequest.of(page, 20, newestFirst));
```

What both paged reads promise, on `JdbcProjectionRepository` and on the in-memory repository of the
test kit alike (`ProjectionRepositoryQueryContract` runs against both):

- **The filter compares text.** `FieldEquals.of("customerId", id)` selects the rows whose field, as
  its stored JSON renders it as text (`data->>'customerId'`), equals the value character by
  character: the number `42` matches `"42"`, not `"042"`; a decimal is the text PostgreSQL prints
  for `numeric`, without an exponent and with the scale as written (`BigDecimal` 19.90 is `"19.90"`,
  the `double` 1.2345678E7 is `"12345678"`); a missing or `null` field matches no value. Filter on a
  scalar field. One field per read: a second condition, a range or a nested path is not offered.
- **The order is code-point text order, with the id last.** A sort order names a top-level field
  or one of the columns `id`, `version`, `updated_at`. A field sorts by its JSON text compared by
  Unicode code point — PostgreSQL's `COLLATE "C"`, whatever the database's collation is — so `"B"`
  sorts before `"a"` and the number `10` before `9`, and an ISO-8601 instant whose printed fraction
  is the beginning of another's sorts after it, though it is earlier (`…00.123Z` after
  `…00.123456Z`, `…00Z` after `…00.500Z`); a missing field sorts last ascending and first
  descending. The id breaks every tie (it is appended ascending unless the sort names it), so the
  order is total and a row of an unchanged table is on exactly one page. An unsorted read orders by
  id; the `id` column is created `COLLATE "C"`, so its primary key serves that order.
- **Three reserved names.** Every row has the columns `id` (the row id), `version` (the number of
  saves after the first) and `updated_at` (the time of the last save; on PostgreSQL the start of the
  saving transaction, so the rows of one transaction tie). A sort or index key of one of those names
  orders by the column, and a filter on one is refused, so a read-model field named `id`,
  `version` or `updated_at` can be neither sorted, filtered nor indexed.
- **One snapshot.** The page and its `totalElements` come from one statement: a write committed
  meanwhile changes both or neither. A page beyond the last is empty and carries the total, and
  `Page.hasNext()` is computed in `long`, so it is `false` there, also at `page ==
  Integer.MAX_VALUE`; `PageRequest.offset()` is a `long`.
- **Refused with an `IllegalArgumentException`:** a field outside `[A-Za-z0-9_]+` (`customer.id`,
  a quote), and a filter or sort on an `@Encrypted` record component of the read-model type: its
  stored text is ciphertext under a crypto-aware mapper, so keep a separate, non-sensitive field for
  the query.

Offset paging has its costs and its limits:

- A page reads every row before its offset: a deep page costs its depth (tens of milliseconds a
  hundred thousand rows deep), so an endpoint should bound the page index. A page beyond the last
  reads only the count.
- The total of an unfiltered read counts the whole table on every page (milliseconds per hundred
  thousand rows), index or not; a filtered total counts only the matches, through the index.
- Under concurrent writes a row inserted before the page you are on shifts the next page by one, so
  a client reading on can see a row twice; and a save that changes a sort or filter field (a
  status, `updated_at`, `version`) moves its row across the page boundary, so a client reading on
  can miss it.

**Declare the indexes the reads need.** Without an index, PostgreSQL reads the whole table for
every filtered or field-sorted page and for a filtered count — tens of milliseconds at a hundred
thousand rows, growing with the table. A `ProjectionIndex` declares one, built for the read it
serves, through `ProjectionRepository.declareIndex`:

```java
import org.streamrune.core.projection.ProjectionIndex;

// findAllBy(ORDERS, …, FieldEquals.of("customerId", …), PageRequest.of(p, n, newestFirst))
ProjectionIndex byCustomerNewestFirst = ProjectionIndex.forFilter(ORDERS, "customerId", newestFirst);
// findAll(ORDERS, …, PageRequest.of(p, n, newestFirst))
ProjectionIndex newestFirstIndex = ProjectionIndex.forSort(ORDERS, newestFirst);

projectionRepository.declareIndex(byCustomerNewestFirst);
projectionRepository.declareIndex(newestFirstIndex);
```

On `JdbcProjectionRepository`, `declareIndex` creates the projection's table if needed and then
the index, `CREATE INDEX IF NOT EXISTS <index name> ON <projection>_view ((data->>'customerId')
COLLATE "C", (data->>'createdAt') COLLATE "C" DESC, id COLLATE "C")` — the expressions the reads
compare, so the planner uses the index for the filter, the order and the count; the id is always
the last key. An index also serves the exactly reversed order: `(createdAt DESC, id)` serves
`[createdAt ASC, id DESC]`. Call it at
wiring, before the application serves reads. In the Spring, Quarkus and Micronaut integrations,
define each index as a `ProjectionIndex` bean: at startup, before the projection runners start and
before the first request, every one is declared on the application's `ProjectionRepository` bean —
the framework's or one the application defines itself; index beans without a repository bean, a
refused key or a failed creation fail the start. It is idempotent and safe on several replicas at
once: a valid index of that name is left alone without locking the table, and a replica that loses
the creation race finds the winner's index.

- **Name.** `<projection>_<keys>_ix_<hash>`, cut to PostgreSQL's 63-byte limit, the hash taken over
  the exact column list. `JdbcProjectionRepository.createIndexSql(index)` returns the statement
  (`System.out.println(JdbcProjectionRepository.createIndexSql(byCustomerNewestFirst))`), and it is
  logged in full at INFO when the index is created.
- **Not `CONCURRENTLY`.** The build holds a `SHARE` lock on the table: reads continue, the
  projection's writes wait until the build commits — milliseconds on a new or small table, longer
  on a large one, and the session's `statement_timeout` bounds it (a build that takes longer fails
  every start). `CREATE INDEX CONCURRENTLY` cannot run in a transaction and leaves an `INVALID`
  index behind when it fails. For a large existing table, run `createIndexSql(index)`'s statement
  with `CONCURRENTLY` inserted after `INDEX` by hand before the deploy that declares the index;
  `declareIndex` then finds it and does nothing. An `INVALID` index of that name (a failed or still
  running manual build) is not taken for the index: `declareIndex` fails, naming the `DROP INDEX`
  to run once no build is running.
- **Index short scalar fields.** A B-tree entry holds at most 2704 bytes: once an index exists, a
  save whose indexed text is longer fails, and its projection batch with it.
- **Cost.** Each index is written on every save of the projection.
- **In memory**, `InMemoryProjectionRepository.declareIndex` checks the keys and records the index
  (`declaredIndexes()`), creating nothing; a transaction-scoped repository refuses `declareIndex`.

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
// The read-model store: read models in <name>_view tables, their checkpoints in readmodel_checkpoint,
// and the JdbcProjectionRepository that commits both in one transaction, all on one DataSource.
// create(...) applies and validates the read-model schema (migrate = true, validate = true).
PostgresReadModelStore store = PostgresReadModelStore.create(
    "postgres", dataSource, JdbcProjectionRepository.createObjectMapper(cryptoEngine), true, true);
ProjectionRepository jdbcRepo = store.repository();

MultiProjectionRunner.builder()
    .eventStore(eventStore)
    .register(ProjectionName.of("orders"), new OrderProjection(jdbcRepo),
        ProjectionErrorStrategy.HALT, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL,
        store.checkpointTarget())                     // readmodel_checkpoint + the JDBC processor
    .register(ProjectionName.of("audit"), auditProjection,
        ProjectionErrorStrategy.HALT, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
        offsetStore.subscriptionCheckpoint())         // offsetStore: a PostgresOffsetStore
    .build();
```

The same repository object as the projection's store and as the processor of
`store.checkpointTarget()` is what the startup check verifies; `auditProjection` is handed no
repository and writes to its own store. (`OrderProjection` extends `BaseProjection` and saves under
the name it is registered under.) Each registration reads its checkpoint from the store its
processor commits to: `store.checkpointTarget()` pairs the `JdbcProjectionRepository` with
`PostgresReadModelCheckpoints` over `readmodel_checkpoint`, and `PostgresOffsetStore#subscriptionCheckpoint()`
pairs a `PostgresOffsetStore` with the fencing `PostgresSubscriptionCheckpointProcessor` over the
event store's `subscription_checkpoint`. Never pair a `JdbcProjectionRepository` with a
`PostgresOffsetStore`: the runner would read a table the processor never writes (see
[a processor and an offset store that disagree](#a-processor-and-an-offset-store-that-disagree)).
A runner-wide `offsetStore(...)` + `atomicProcessor(...)` pair still serves registrations given no
target; set both from one pair (`store.checkpoints()` + `store.processor()`) or neither. The facade
takes the same declaration: `StreamRune.Builder.registerProjection(name, projection, mode, store)`
(a `ReadModelStore`) or `(…, target)` (a `CheckpointTarget`), which `startProjections()` hands to
`ProjectionRunner.run(name, projection, mode, target)`; `ContinuousProjectionRunner` and
`PollingProjectionRunner` route it, and a runner that cannot route refuses it instead of running
the projection on its own pair.

**Integration recipe:** with a `DataSource`, the integrations create the default read-model store
`postgres` (a `PostgresReadModelStore` on the primary `DataSource`, which applies the read-model
series under `streamrune.read-model.schema.auto-initialize`), and the injectable
`ProjectionRepository` bean is its repository. Annotate
`@ProjectionConfig(name = "orders", deliveryMode = TRANSACTIONAL_LOCAL)` and construct the
projection over that bean (any bean scope — identity survives proxies): routing sends it to the
store whose repository it writes to, and its checkpoint lives in that store's `readmodel_checkpoint`.
An `AT_LEAST_ONCE_IDEMPOTENT` projection that writes elsewhere checkpoints in the event store's
`subscription_checkpoint`. Startup refuses anything else. The routing rules, several stores in
one application and the leadership requirements are in [Read-model stores](read-model-stores.md).

#### A processor and an offset store that disagree

A processor whose checkpoint table the runner's offset store does not read — a
`JdbcProjectionRepository` (`readmodel_checkpoint`) with a `PostgresOffsetStore`
(`subscription_checkpoint`), or the two on different `DataSource`s — advances a checkpoint the
runner never reads.
The first batch commits there; the runner reads its own `OffsetStore`, still at the old checkpoint,
reads the same batch again, and the processor rejects it as an overlap before any write. The runner
tolerates one such rejection — a commit whose acknowledgement was lost is rejected when it is
retried, and the re-read that follows starts after the moved checkpoint. A second consecutive
overlap or monotonic rejection while the checkpoint the runner reads has not moved **halts the
projection**: `ContinuousProjectionRunner` ends in `ProjectionState.ERROR`, `ScheduledProjectionRunner`
puts the registration in `ERROR`, the health contributor reports the subscription `DOWN`, and
`lastError` carries a `ProjectionCheckpointDivergedException` message naming the two likely causes:

- the `AtomicBatchProcessor` and the `OffsetStore` are not over the same checkpoint table;
- two runners are registered under one projection name and commit through one processor while
  reading their checkpoint from different offset stores.

The read model is not written twice: every rejected batch rolls back before its first write. Give
the runner a matching pair — `PostgresReadModelStore#checkpointTarget()` for a
`JdbcProjectionRepository`, `PostgresOffsetStore#subscriptionCheckpoint()` for the subscription
checkpoint — register the name once, and restart the projection; it resumes from the checkpoint.

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

- runs under a processor whose `transactional()` is `false`, such as `nonAtomicAtLeastOnce()` —
  it cannot put the read-model write and the checkpoint in one transaction;
- does not write through the handed repository — `writesThroughRepository()` answers `false`, so its
  `process(List, ProjectionRepository)` is taken to be the inherited default, which ignores the
  repository and falls back to `process(List)`. Extend `BaseProjection`, or override the
  two-argument `process`, write through the repository it is given and override
  `writesThroughRepository()` to return `true`;
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
