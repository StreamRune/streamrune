# Read-model stores

A projection builds a read model: a set of rows that queries read. Where those rows live, and where
the projection records how far it has read, is decided by a **read-model store**. This guide
explains what a store is, which store each projection uses, and how to run more than one store in
an application.

Most applications never need this page: with a `DataSource`, the integrations create one store,
named after the backend (`postgres`, or `sqlite` on the [SQLite backend](sqlite.md)), and every projection that writes to it checkpoints there. Read on when a projection
writes somewhere else, when you want a read model in a second database, or when startup refuses a
projection and names a store.

- [What a store is](#what-a-store-is)
- [The checkpoint lives with the read model](#the-checkpoint-lives-with-the-read-model)
- [The two checkpoint tables](#the-two-checkpoint-tables)
- [How a projection finds its store](#how-a-projection-finds-its-store)
- [Leadership needs a fencing store](#leadership-needs-a-fencing-store)
- [Several stores in one application](#several-stores-in-one-application)
- [Replacing the default store](#replacing-the-default-store)
- [Without a container](#without-a-container)
- [Dead-letter replay per store](#dead-letter-replay-per-store)
- [Health](#health)
- [On SQLite](#on-sqlite)
- [What is next](#what-is-next)

## What a store is

A `ReadModelStore` (`org.streamrune.core.projection`) is one read-model backend. It bundles four
things:

| Member | What it is |
|---|---|
| `name()` | A stable, user-visible name (`"postgres"`, `"search"`) used in logs, health, errors and explicit routing. |
| `repository()` | The `ProjectionRepository` that projections routed here write to. Its `writeTargetIdentity()` identifies the store. |
| `checkpoints()` | The `OffsetStore` that holds the checkpoints of the projections routed here. |
| `processor()` | The `AtomicBatchProcessor` that commits a batch. Its capabilities decide which projections the store may take. |

`checkpointTarget()` returns the last three as a `CheckpointTarget`, the pair a runner registration
reads and commits through.

The PostgreSQL implementation is `PostgresReadModelStore` (`streamrune-postgres`): read models in
`<name>_view` tables, checkpoints in `readmodel_checkpoint`, and a `JdbcProjectionRepository` that
is both the repository and the processor. It has its own migration series
(`classpath:db/readmodel-migration`, history table `flyway_schema_history_readmodel`), separate from
the event store's, because a store can live on a database that has no event tables.

```java
PostgresReadModelStore store = PostgresReadModelStore.create(
    "postgres", dataSource, JdbcProjectionRepository.createObjectMapper(cryptoEngine),
    true,   // migrate the read-model series
    true);  // validate it
```

`PostgresEventStoreFactory#createReadModelStore()` does the same on the event store's own
`DataSource`, under the same schema switch, and names the store `postgres`.

The SQLite implementation is `SqliteReadModelStore` (`streamrune-sqlite`), with the same parts on
one SQLite file: `<name>_view` tables, checkpoints in `readmodel_checkpoint`, and a
`SqliteProjectionRepository` that is both the repository and the processor, with its own series
(`db/sqlite-readmodel-migration`). With `streamrune.backend=sqlite` it is the default store, named
`sqlite`. See [On SQLite](#on-sqlite).

The event store itself is not a read-model store. It holds what must commit with the event log or
coordinate its consumers: events, snapshots, subscriptions, leases, sagas, the outbox, the inbox,
the dead-letter stores. There is exactly one per application.

## The checkpoint lives with the read model

A projection's checkpoint is stored in the same place as the read model it advances. That is the
rule the rest of this page follows from.

- **One transaction.** For a `TRANSACTIONAL_LOCAL` or `EXTERNAL_EFFECT` projection the batch's
  writes and the checkpoint commit together, or not at all. That is only possible when both are in
  one database.
- **One truth.** If the read model is rebuilt, restored from a backup or dropped, its checkpoint
  goes with it, and the projection replays what the restored read model is missing. A checkpoint in
  another database would keep claiming progress over rows the restored read model does not hold.
- **A store can move.** A read model in a second database brings its own checkpoint. Nothing in the
  event store knows it exists.

The runner enforces it: for every registration, the `OffsetStore` it reads and the processor that
commits come from the same pair. Pairing a processor with an offset store over another table is
refused or halts the projection (see
[a processor and an offset store that disagree](concepts.md#a-processor-and-an-offset-store-that-disagree)).

## The two checkpoint tables

| Table | In | Holds | Written by |
|---|---|---|---|
| `readmodel_checkpoint` | the read-model schema (`db/readmodel-migration`) | checkpoints of `TRANSACTIONAL_LOCAL` and `EXTERNAL_EFFECT` projections routed to a store | `JdbcProjectionRepository`, in the batch's transaction (`PostgresReadModelCheckpoints` reads it) |
| `subscription_checkpoint` | the event-store schema (`db/streamrune-migration`) | positions of event listeners and the SSE feed, and of `AT_LEAST_ONCE_IDEMPOTENT` projections routed to no store | `PostgresOffsetStore`, and `PostgresSubscriptionCheckpointProcessor` for projections |

`PostgresSubscriptionCheckpointProcessor` is the fencing at-least-once processor: under the
checkpoint row's lock it checks the epoch, runs the projection (which writes wherever it writes),
then saves the checkpoint. It is not transactional with the projection's writes, which is what
`AT_LEAST_ONCE_IDEMPOTENT` promises. `PostgresOffsetStore#subscriptionCheckpoint()` returns it
paired with the `PostgresOffsetStore`.

A `JdbcProjectionRepository` pairs only with `PostgresReadModelCheckpoints`, never with a
`PostgresOffsetStore`.

## How a projection finds its store

Routing runs once per registration at startup. The first rule that matches wins; a registration no
rule accepts is a startup error naming the projection and the stores it could have used.

**1. An explicit declaration.** `@ProjectionConfig(readModelStore = "search")` names the store.
An unknown name is an error, and so is a declaration that contradicts the projection's own
`writeTarget()`.

```java
@ProjectionConfig(name = "product_search",
    deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL,
    readModelStore = "search")
public class ProductSearchProjection extends BaseProjection { /* ... */ }
```

`ReadModelStore.DEFAULT` (the text `default`) is the alias of the default store:
`@ProjectionConfig(readModelStore = ReadModelStore.DEFAULT)` routes to the default store whatever
the backend names it (`postgres` with the PostgreSQL backend, `sqlite` with the SQLite one), so the
declaration survives a switch of backend. The alias is reserved: a store whose
`name()` is `default` fails the start.

Use it when routing cannot decide alone: several transactional stores. A `writeTarget()` that no store
owns is refused in two cases: for a transactional mode (rule 3), and whenever a `readModelStore`
declaration is given. Define a `ReadModelStore` over that repository, or make the projection write
to `store.repository()`. An at-least-once projection without a declaration is accepted (rule 4).

**2. The projection's `writeTarget()` matches a store.** A `BaseProjection` exposes the repository
it was built with; the store whose `repository().writeTargetIdentity()` equals it is the store. This
is how a projection over the injected `ProjectionRepository` reaches the default store with no
annotation, and it survives bean proxies.

```java
@ProjectionConfig(name = "orders", deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
public class OrderProjection extends BaseProjection {
  public OrderProjection(ProjectionRepository repository) { super(repository, "orders"); }
}
```

**3. Exactly one transactional store.** A `TRANSACTIONAL_LOCAL` or `EXTERNAL_EFFECT` projection
that exposes no `writeTarget()` and matches nothing goes to the one store whose processor is
`transactional()`. With two transactional stores, use rule 1. A `writeTarget()` that matches no store is refused:
define a `ReadModelStore` over that repository, or write through `store.repository()`.

**4. At-least-once, nothing matched.** An `AT_LEAST_ONCE_IDEMPOTENT` projection with no
`writeTarget()`, or one that belongs to no store (JPA, Redis, a file, another database you manage
yourself), checkpoints in the event store's `subscription_checkpoint`. A projection that
writes elsewhere is therefore never a startup error.

```java
// Writes to Redis through its own client; hands the runner nothing to commit with.
@ProjectionConfig(name = "recent_orders", deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
public class RecentOrdersProjection implements Projection { /* ... */ }
```

After routing, each registration is checked once more: a transactional mode needs a store whose
processor is `transactional()` and that the projection writes to, and real leadership needs a
processor that fences its checkpoint.

## Leadership needs a fencing store

With more than one replica, a lease elects one runner per projection and each acquisition carries
a strictly increasing epoch. The epoch protects the read model only if the processor that commits
the batch rejects a superseded epoch before it runs the projection, and makes the checkpoint write
conditional on it. A processor declares that with `fencesCheckpoint()`.

| Capability | Meaning |
|---|---|
| `transactional()` | The projection's writes through the handed repository and the checkpoint commit in one transaction. Required by `TRANSACTIONAL_LOCAL` and `EXTERNAL_EFFECT`. |
| `fencesCheckpoint()` | The epoch is stored durably, a stale epoch is rejected before the projection runs, and the checkpoint write is conditional on the epoch. Required by **every** registration under real leadership. |

`JdbcProjectionRepository`, `SqliteProjectionRepository` and `PostgresSubscriptionCheckpointProcessor` fence; `AtomicBatchProcessor.nonAtomicAtLeastOnce()` does not, and is legal only with
`SubscriptionLeadership.NOOP` (one instance per projection). A store whose processor cannot fence
fails startup under real leadership, and the message names the store.

The lease lives in the event store whatever store a projection uses. The epoch is just a number
handed to each registration's processor, so a read-model store in a different database is fenced
exactly like a local one.

## Several stores in one application

Every `ReadModelStore` bean you define is a routing target beside the default store (`postgres`
or `sqlite`), named by its `name()`. A second PostgreSQL database is the common case: define its `DataSource`
and a store over it, then point a projection at it with `readModelStore`.

The default store and the event store use the primary `DataSource`, so with a second one you must
declare the primary explicitly (see the Spring snippet). Two stores with the same name, or over the same repository, fail startup. Each store applies
its own read-model migration series on its own database under
`streamrune.read-model.schema.auto-initialize` (the `migrate` argument of `create`).

**Spring Boot**

```java
@Configuration
class SearchStoreConfig {

  // Boot's DataSource auto-configuration backs off once any DataSource bean exists, so declare the
  // primary one too: the event store and the default "postgres" store use it.
  @Bean
  @Primary
  @ConfigurationProperties("spring.datasource")
  DataSourceProperties primaryDataSourceProperties() { return new DataSourceProperties(); }

  // .type(HikariDataSource.class) + @ConfigurationProperties binds spring.datasource.hikari.*
  // (pool size, timeouts, connection-init-sql) onto the pool, as Boot's own auto-configuration does.
  @Bean
  @Primary
  @ConfigurationProperties("spring.datasource.hikari")
  HikariDataSource dataSource(
      @Qualifier("primaryDataSourceProperties") DataSourceProperties props) {
    return props.initializeDataSourceBuilder().type(HikariDataSource.class).build();
  }

  @Bean
  @ConfigurationProperties("search.datasource")
  DataSourceProperties searchDataSourceProperties() { return new DataSourceProperties(); }

  @Bean
  @ConfigurationProperties("search.datasource.hikari")
  HikariDataSource searchDataSource(
      @Qualifier("searchDataSourceProperties") DataSourceProperties props) {
    return props.initializeDataSourceBuilder().type(HikariDataSource.class).build();
  }

  @Bean
  ReadModelStore searchStore(@Qualifier("searchDataSource") DataSource searchDataSource,
                             Optional<CryptoEngine> cryptoEngine) {
    return PostgresReadModelStore.create("search", searchDataSource,
        JdbcProjectionRepository.createObjectMapper(cryptoEngine.orElse(null)), true, true);
  }
}

@Component
@ProjectionConfig(name = "product_search",
    deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL, readModelStore = "search")
class ProductSearchProjection extends BaseProjection {
  ProductSearchProjection(@Qualifier("searchStore") ReadModelStore search) {
    super(search.repository(), "product_search");
  }
}
```

Without the `@Primary` pair, Boot would create no primary `DataSource`, and the event store, the
leases and the default store would bind to the search database without any error. Set
`spring.datasource.*` for the primary database and `search.datasource.*` for the second; the pool
settings stay where Boot reads them, `spring.datasource.hikari.*` and `search.datasource.hikari.*`.
Without `.type(HikariDataSource.class)` and the `@ConfigurationProperties` on the pool bean, the
rebuilt primary pool would silently drop `spring.datasource.hikari.*` — its size, its timeouts and
any `connection-init-sql`, such as a session `statement_timeout` (see the
[production guide](production.md)).

**Quarkus**

```java
@ApplicationScoped
class SearchStoreProducer {

  @Produces
  @Singleton
  @Named("search")
  ReadModelStore searchStore(@io.quarkus.agroal.DataSource("search") AgroalDataSource ds,
                             Instance<CryptoEngine> cryptoEngine) {
    return PostgresReadModelStore.create("search", ds,
        JdbcProjectionRepository.createObjectMapper(
            cryptoEngine.isResolvable() ? cryptoEngine.get() : null), true, true);
  }
}

@ApplicationScoped
@ProjectionConfig(name = "product_search",
    deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL, readModelStore = "search")
class ProductSearchProjection extends BaseProjection {
  @Inject
  ProductSearchProjection(@Named("search") ReadModelStore search) {
    super(search.repository(), "product_search");
  }
}
```

```properties
quarkus.datasource."search".db-kind=postgresql
quarkus.datasource."search".jdbc.url=jdbc:postgresql://search-db:5432/search
quarkus.datasource."search".username=search
quarkus.datasource."search".password=${SEARCH_DB_PASSWORD}
```

The producer is `@Named("search")` so the projection can inject this store by name beside the
default one.

**Micronaut**

```java
@Factory
class SearchStoreFactory {

  @Singleton
  @Named("search")
  ReadModelStore searchStore(@Named("search") DataSource ds, Optional<CryptoEngine> cryptoEngine) {
    return PostgresReadModelStore.create("search", ds,
        JdbcProjectionRepository.createObjectMapper(cryptoEngine.orElse(null)), true, true);
  }
}

@Singleton
@ProjectionConfig(name = "product_search",
    deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL, readModelStore = "search")
class ProductSearchProjection extends BaseProjection {
  ProductSearchProjection(@Named("search") ReadModelStore search) {
    super(search.repository(), "product_search");
  }
}
```

Declare the second pool under `datasources.search.*`. The module collects store beans with
`getBeansOfType`, so a store you register as a singleton before the context starts counts as well.

In all three, inject your own store rather than every `ReadModelStore` bean, and reach another
store's repository through `store.repository()`. The injectable `ProjectionRepository` bean stays
the repository of the store named after the backend (`postgres` or `sqlite`), so a single-store
application needs nothing else. A
`ProjectionIndex` is declared on the store of its projection; an index for a projection routed to
no store goes to the `ProjectionRepository` bean. See each integration's README for the bean names
and the exact collection rules.

## Replacing the default store

The default store is replaced by name, and only by name. Its name is the backend's, `postgres` or
`sqlite`; its
bean is the `ReadModelStore` named `streamRuneDefaultReadModelStore`, declared with that interface
type. Define a `ReadModelStore` whose `name()` is `postgres` and it takes the default's place: the default is never created, so its database is never
touched. A store with any other name is added beside the default and never removes it. This is the
way to put the `postgres` store on another database, or to give an application that uses no
database a store of its own (Quarkus always provides a `DataSource` bean, so such an application
must define its own `postgres` store).

The property that switches the default store's migration is
`streamrune.read-model.schema.auto-initialize`. Unset, it follows
`streamrune.event-store.schema.auto-initialize`. See [Configuration](configuration.md).

## Without a container

Outside an integration, hand the store to the registration yourself.

```java
PostgresReadModelStore store = eventStoreFactory.createReadModelStore();

StreamRune streamRune = StreamRune.builder()
    .eventStore(eventStore)
    .registerProjection(ProjectionName.of("orders"), new OrderProjection(store.repository()),
        ProjectionDeliveryMode.TRANSACTIONAL_LOCAL, store)           // a ReadModelStore
    .registerProjection(ProjectionName.of("audit"), auditProjection,
        ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
        offsetStore.subscriptionCheckpoint())                        // a CheckpointTarget
    /* runner configuration */
    .build();
```

`ProjectionRunner.run(name, projection, mode, target)` takes the same `CheckpointTarget`, and a
runner that cannot route to it refuses the registration instead of running the projection on its
own pair.

To rebuild a projection, reset it on the target it checkpoints in:

```java
runner.reset(ProjectionName.of("orders"), store.checkpointTarget());
```

`reset(name)` without a target rewinds the checkpoint the runner itself holds; `reset(name,
target)` rewinds the one in that target, whether or not this runner instance has run the
projection. `store.checkpoints().reset(name)` does the same without a runner. In a Spring, Quarkus or
Micronaut application the `OffsetStore` bean is the event store's `subscription_checkpoint`: reset
through it only a projection whose `checkpoint target` (startup INFO line) or `store` health detail
is `subscription_checkpoint`; the framework's `MultiProjectionRunner` / `ScheduledProjectionRunner`
beans reset each registration on its own target, once stopped. See
[Offset reset / rebuild path](production.md#offset-reset--rebuild-path).

## Dead-letter replay per store

A dead-lettered range has to be replayed on the processor of the store the projection commits to,
not on another store's. `ProjectionDeadLetterReplayer` takes a `Function<ProjectionName,
CheckpointTarget>` so one replayer serves projections of several stores:

```java
Map<ProjectionName, CheckpointTarget> targets = Map.of(
    ProjectionName.of("orders"), store.checkpointTarget(),
    ProjectionName.of("audit"), offsetStore.subscriptionCheckpoint()); // writes outside every store

var replayer = new ProjectionDeadLetterReplayer(eventStore, projectionDeadLetterStore, targets::get);
replayer.replay(ProjectionName.of("orders"), ordersProjection,
    ProjectionDeliveryMode.TRANSACTIONAL_LOCAL, 100);
```

A projection the function returns `null` for is refused before any entry is read. The dead-letter
entries themselves stay in the event store whichever store the projection uses. Details are in
[Projection dead-letter replay](advanced/retry-and-resilience.md#per-store-replay).

**Build the map from the routes, not from the mode — a wrong map fails silently.** Nothing checks
the function against the runner's routing (an integration keeps the routed targets to itself), so
read each projection's target from the runner's startup INFO line (`… (checkpoint target
'<target>') …`) or its `store` health detail. The trap is an `AT_LEAST_ONCE_IDEMPOTENT`
`BaseProjection` over the injected `ProjectionRepository`: that repository is the `postgres`
store's, so rule 2 routes the projection to the `postgres` store, and it checkpoints in
`readmodel_checkpoint`, not on the subscription checkpoint. Mapped to
`offsetStore.subscriptionCheckpoint()`, its replay locks the `subscription_checkpoint` row (seeding
a stray one) while its live batches lock `readmodel_checkpoint`: replay and live batches do not
exclude each other, and a read-modify-write update can be lost — exactly what the replayer's lock
exists to prevent. Only an at-least-once projection that writes outside every store belongs on the
subscription checkpoint. A transactional projection mapped to the wrong store has its range written
into that store's database instead of its own.

## Health

The health contributor of each projection reports a `store` detail: the read-model store it is
routed to (`postgres`, `search`), or `subscription_checkpoint` when it is routed to the event
store's subscription checkpoint. Quarkus reports it as the flat key
`subscription.<name>.store`. Lag is computed against each projection's **own** checkpoint, in
`readmodel_checkpoint` or `subscription_checkpoint`, so a projection on a second database shows the
lag of that database's checkpoint. The store name also labels the processor in metrics. See
[Production](production.md#health-checks) for the details and thresholds.

## On SQLite

With `streamrune.backend=sqlite` the default store is a `SqliteReadModelStore` named `sqlite`, on the
application's SQLite file; `ReadModelStore.DEFAULT` routes to it, and a store of your own named
`sqlite` replaces it. Its queries return the rows, in the order, that PostgreSQL returns for the
same data, and it fences its checkpoint as the PostgreSQL store does, with the epoch always `0`
because SQLite has no leadership.

One thing differs: **SQLite has one write lock for the whole file**, not one per projection. A
batch or a dead-letter replay holds it from its `BEGIN IMMEDIATE` to its `COMMIT`, so a replay of
one projection waits for an open batch of any other projection on the same file, and the other way
round. It succeeds once that batch commits within the connection's `busy_timeout` and fails with
the write-lock message otherwise, so `busy_timeout` must exceed the longest batch or replay the
file runs. Reads never wait.

A read-model store on a second SQLite file opens its own `SqliteDatabase` handle on that file, which
takes that file's instance guard, and is built with `SqliteReadModelStore.create(name, database,
mapper, migrate, validate)`; the [SQLite guide](sqlite.md#plain-java) shows it. Everything else
SQLite does differently is in that guide.

## What is next

The store interface is the seam for more backends. An Elasticsearch read-model store and an evitaDB
one are planned; they would be non-transactional stores that fence their checkpoint. Neither is
shipped, and this page will describe each when it is.
