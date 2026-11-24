# SQLite backend

StreamRune runs on one SQLite database file as well as on PostgreSQL. The programming model is the
same: aggregates and deciders, the command inbox, projections and read models, sagas, the outbox,
the dead-letter queues, audit and GDPR crypto-shredding. Application code switches with one
property, `streamrune.backend=sqlite`, and a `DataSource` on a SQLite file.

This page says when SQLite fits, how to configure it on Spring Boot, Quarkus, Micronaut and plain
Java, and every place where it behaves differently from PostgreSQL.

- [When to choose SQLite](#when-to-choose-sqlite)
- [Modules](#modules)
- [Connection settings](#connection-settings)
- [One instance per database file](#one-instance-per-database-file)
- [Application transactions are never joined](#application-transactions-are-never-joined)
- [What differs from PostgreSQL](#what-differs-from-postgresql)
- [Crypto and GDPR erasure](#crypto-and-gdpr-erasure)
- [Startup and shutdown](#startup-and-shutdown)
- [Plain Java](#plain-java)
- [Backup and restore](#backup-and-restore)
- [Native access and native image](#native-access-and-native-image)
- [Operations](#operations)

## When to choose SQLite

Choose SQLite for a small service that runs as **one instance**: hundreds to a few thousand events,
one process, no database server to run. Everything StreamRune stores lives in one file next to the
application.

Choose PostgreSQL when the service runs as more than one replica (SQLite has no subscription
leadership and refuses a second process on the file), when writes must not queue behind each other
(SQLite has one write lock per file), or when the database must be on a network file system.

SQLite is not a test substitute for the in-memory stores of `streamrune-test`: use those in unit
tests.

## Modules

| Module (`org.streamrune:`) | What it holds |
|---|---|
| `streamrune-sqlite` | `SqliteBackend`, the `SqliteDatabase` handle with its instance guard, the three schema series, and every store: event store and snapshots, offsets, sagas and saga dead letters, outbox, command inbox, dead-letter queues, audit stores and queries, `SqliteReadModelStore`. It brings the `org.xerial:sqlite-jdbc` driver, which bundles the SQLite library. |
| `streamrune-sqlite-crypto` | `SqliteCryptoBackend`, `SqliteCryptoEngine` (keys in the SQLite file), `SqliteForgottenSubjectStore` and the in-process forget signal. Needed only with a crypto engine. |

The integrations do not depend on either module: an application that selects SQLite adds them.

```kotlin
dependencies {
    implementation("org.streamrune:streamrune-sqlite:1.0.0")
    implementation("org.streamrune:streamrune-sqlite-crypto:1.0.0") // with a crypto engine
    implementation("com.zaxxer:HikariCP:6.2.1")                      // when nothing else brings a pool
}
```

`streamrune.backend=sqlite` without `streamrune-sqlite` on the class path fails the start naming the
module; with a crypto engine enabled and no `streamrune-sqlite-crypto`, the start fails naming that
module.

The SQLite library must be 3.45.0 or newer; the driver StreamRune is built with bundles 3.54.1.
`SqliteDatabase` reads the version at startup and refuses an older library.

## Connection settings

The application owns the connection pool, as on PostgreSQL. Its connections must carry five
settings, which `SqliteDatabase` reads back from a borrowed connection at startup; a wrong value
fails the start with a message that names the setting, its value and the value required:

| Setting | Required value | Why |
|---|---|---|
| `journal_mode` | `WAL` | Readers run while the one writer writes. |
| `synchronous` | `FULL` (`EXTRA` is accepted too) | A committed append survives a power loss. |
| `foreign_keys` | `true` | The schema relies on its foreign keys. |
| `busy_timeout` | above 0; 5000 ms is the documented value | How long a writer waits for the write lock before it fails. |
| `transaction_mode` | `IMMEDIATE` | A transaction the application starts on the same `DataSource` takes the write lock up front instead of failing on a read-to-write upgrade. |

The pool must hand out connections in **autocommit mode** (HikariCP's default, `autoCommit=true`).
With `transaction_mode=IMMEDIATE` the driver begins a write transaction as soon as a connection
leaves autocommit, so a pool of `autoCommit=false` connections would hold the write lock with every
idle connection; `SqliteDatabase` refuses such a pool at startup. StreamRune's own stores run each
write transaction with explicit `BEGIN IMMEDIATE`, `COMMIT` and `ROLLBACK` statements on an
autocommit connection, and never change the connection's autocommit flag.

Also refused at startup:

- an in-memory or temporary database (`jdbc:sqlite::memory:`, `mode=memory`, an empty file name):
  each pooled connection would get a private database, and nothing would survive a restart;
- a connection with other databases attached (`ATTACH DATABASE`, for example in the pool's
  connection initialization SQL): their files would be written outside the instance guard;
- a database file whose text encoding is not UTF-8. The encoding is fixed when the file is created,
  and UTF-8 is SQLite's default, so this only concerns a file created elsewhere with UTF-16.

The settings go into the JDBC URL as parameters. Every example below uses this URL, with the
database file at `/var/lib/app/app.db`:

```
jdbc:sqlite:file:/var/lib/app/app.db?journal_mode=WAL&synchronous=FULL&foreign_keys=true&busy_timeout=5000&transaction_mode=IMMEDIATE
```

The directory of the database file must be writable: SQLite creates its `-wal` and `-shm` files
there, and StreamRune its lock file.

### Spring Boot

Spring Boot's HikariCP pool, from `spring-boot-starter-jdbc`:

```properties
spring.datasource.url=jdbc:sqlite:file:/var/lib/app/app.db?journal_mode=WAL&synchronous=FULL&foreign_keys=true&busy_timeout=5000&transaction_mode=IMMEDIATE
spring.datasource.driver-class-name=org.sqlite.JDBC
# autoCommit=true is HikariCP's default; leave it so
spring.datasource.hikari.auto-commit=true

streamrune.backend=sqlite
```

The integrations reference the PostgreSQL backend directly, so `streamrune-postgres` stays on the
class path (the starter `streamrune-spring-boot-starter` brings it); add `streamrune-sqlite` beside
it. `streamrune.backend` decides which backend opens the stores, never the class path.

### Quarkus

Produce the HikariCP pool as the application's `DataSource` bean, with no `quarkus.datasource`
configured: a produced `DataSource` bean, as the framework's Quarkus tests use (they produce a plain
`SQLiteDataSource`). An Agroal datasource on the SQLite driver is not tested.

```java
@ApplicationScoped
public class SqliteDataSourceProducer {

  @Produces
  @Singleton
  DataSource dataSource() {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(
        "jdbc:sqlite:file:/var/lib/app/app.db?journal_mode=WAL&synchronous=FULL"
            + "&foreign_keys=true&busy_timeout=5000&transaction_mode=IMMEDIATE");
    return new HikariDataSource(config);
  }

  void close(@Disposes DataSource dataSource) {
    ((HikariDataSource) dataSource).close();
  }
}
```

```properties
streamrune.backend=sqlite
```

### Micronaut

Micronaut's HikariCP pool, from `io.micronaut.sql:micronaut-jdbc-hikari`:

```yaml
datasources:
  default:
    url: "jdbc:sqlite:file:/var/lib/app/app.db?journal_mode=WAL&synchronous=FULL&foreign_keys=true&busy_timeout=5000&transaction_mode=IMMEDIATE"
    driver-class-name: org.sqlite.JDBC

streamrune:
  backend: sqlite
```

### Plain Java

```java
HikariConfig config = new HikariConfig();
config.setJdbcUrl(
    "jdbc:sqlite:file:/var/lib/app/app.db?journal_mode=WAL&synchronous=FULL"
        + "&foreign_keys=true&busy_timeout=5000&transaction_mode=IMMEDIATE");
HikariDataSource pool = new HikariDataSource(config);
```

The rest of the wiring is under [Plain Java](#plain-java).

### Pool size

Reads run concurrently in WAL mode, and writes queue for the one write lock whatever the pool size,
so a large pool buys nothing. HikariCP's default of 10 connections is a sound start. A smaller pool
works: the stores pass their contract suites on a pool of one connection. But every caller then also
waits for a free connection, and a caller that waits longer than HikariCP's `connectionTimeout`
fails with the pool's exception rather than with the write-lock message.

## One instance per database file

A SQLite database serves **exactly one application instance**. A second instance on the same file
would apply projections twice and deliver outbox entries twice, so StreamRune prevents it with an
**instance guard**: an exclusive operating-system lock on a side file, `<database-file>.lock` (for
`/var/lib/app/app.db`, `/var/lib/app/app.db.lock`).

- The guard is taken when the backend's stores are opened, at startup, and held until they are
  closed at shutdown. A second process that opens the same file fails at once with
  `SqliteGuardException`, whose message says the database is in use by another process.
- Inside one JVM every handle on the file shares the one lock, so the backend's stores, the crypto
  engine and a read-model store on the same file all work under it.
- The operating system releases the lock when the process ends, so a crashed instance never blocks
  its restart.
- On Linux and macOS the lock is a `flock(2)`, taken through the Java Foreign Function and Memory
  API, so other code that opens and closes the lock file does not release it. On Windows it is an
  exclusive `FileChannel` lock. Any other operating system is refused with a message naming it. The
  project's tests run on Linux and macOS; Windows is not covered by them.
- StreamRune never deletes the lock file; a restart finds it and locks it again.

The guard identifies the database by the path it is opened under, so it cannot see through these
setups, which therefore must not be used:

- **Deleting or replacing the lock file while an instance runs.** A second instance would lock a
  new file of that name. The running JVM refuses to open the database again once it notices, and
  asks for every instance to be stopped and one restarted.
- **Renaming or moving the database file while an instance runs.** The running instance keeps
  writing the file under its new name, while a second instance opened under the new name takes the
  lock file of the new name: two holders on one database.
- **Hard links.** A second name of the database file would get a lock file of its own. A database
  file with more than one link is refused on Linux and macOS.
- **Mounting the database file on its own** into two containers (a single-file bind mount). Each
  container creates its own lock file next to its mount point. Mount the directory that holds the
  database instead. This cannot be detected.
- **Attached databases.** Refused at startup (see above); open each database file as its own
  `SqliteDatabase` instead.
- **A network file system** (NFS, SMB/CIFS). Operating-system locks there are unreliable, as SQLite
  itself documents, and so are SQLite's own locks: keep the database on a local disk.

A rolling deployment that starts the new instance while the old one still holds the file fails the
new instance's start. Stop the old instance first (on Kubernetes, `strategy: Recreate`).

## Application transactions are never joined

**Do not call StreamRune inside a transaction your application or framework holds on the same
SQLite file.** StreamRune's stores never join an application's transaction: each borrows its own
connection and runs its own `BEGIN IMMEDIATE` ... `COMMIT`.

SQLite has one write lock per file, and with `transaction_mode=IMMEDIATE` a framework transaction
takes it when it begins, before it writes anything, even one that only reads: a Spring
`@Transactional` method on the same `DataSource`, a JPA or Hibernate transaction, a Quarkus or
Micronaut `@Transactional` method. While it is open:

- a store write on its own connection waits `busy_timeout` and then fails, storing nothing; the
  event store's message says that an application transaction on the same file is the likely cause;
- the SQLite crypto engine cannot mint a key, with the same wait and failure;
- a `DataSource` proxy that hands the store the transaction's own connection gives it a connection
  in manual-commit mode, which the store refuses at once, naming that cause.

So keep StreamRune calls outside such transactions: write the application's own tables in short
transactions of their own, before or after the command, or keep the application's tables in another
database. A store or the command bus called outside every transaction works as on PostgreSQL.

## What differs from PostgreSQL

### One write lock for the whole file

SQLite allows one writer at a time per database file. Every StreamRune write transaction holds the
write lock from its `BEGIN IMMEDIATE` to its `COMMIT`, so writes of every kind (appends, saga steps,
outbox claims, projection batches, retention sweeps) run one after another. A writer waits for the
lock at most the connection's `busy_timeout`, then fails with an `EventStoreException` saying the
write lock was not granted.

The stores' writers of one process queue for the lock in arrival order, and they wait in Java, not
inside the driver: a waiting writer on a virtual thread parks and frees its carrier. So on a machine
with one or two processors a transaction that is itself waiting for something (a projection updater
making a blocking call) is not starved by the writers behind it. A writer outside the stores, an
application transaction or a schema migration, holds SQLite's lock without joining that queue; the
store writer at the head of the queue then waits for it inside the driver, as any SQLite writer
does.

A read does not wait: in WAL mode it sees the last committed state. The one read that does is the
first read of a projection whose read-model table does not exist yet: it creates the table, which is
a write. A table that exists is found without the lock, so after a restart the first read of each
projection's read model does not wait either.

The consequences:

- **Size `busy_timeout` above the longest write transaction the file runs.** The longest are
  usually projection batches and dead-letter replays, which hold the lock while the projection's
  updater runs.
- **A replay waits behind an open batch of any projection.** On PostgreSQL a projection's batch and
  replay take a lock on that projection's checkpoint row only; on SQLite a dead-letter replay of one
  projection waits for an open batch of any other projection, and the other way round. It succeeds
  once the batch commits within `busy_timeout`, and fails with the write-lock message otherwise.
- **An updater writes through the repository it is handed, or through the store's own repository.**
  A call the updater makes on the `ProjectionRepository` of a SQLite store of the same file (the
  injected one, for example), on the updater's own thread, joins the batch's transaction: it runs
  on the batch's connection, sees the batch's uncommitted writes, and commits or rolls back with
  the checkpoint; a statement that fails there aborts the batch. So a lookup of another
  projection's read model from an updater works, and an `AT_LEAST_ONCE_IDEMPOTENT` projection
  routed to a SQLite store, which the runner hands no repository, is applied in one transaction
  with its checkpoint, a stronger promise than the mode makes on PostgreSQL. Everything else does
  not join: a write through the event store or any other store of the same file, or from a thread
  the updater started, waits for the batch's own transaction and fails after `busy_timeout`.
- **Global offsets stay gap-free and appear in commit order**, as on PostgreSQL: the event store
  keeps a one-row counter that each append advances inside its own transaction.
- **Saga fencing uses the write lock.** The forward fence reads the saga row inside the append's own
  write transaction, so the write lock is the fence; there is no separate lock as on PostgreSQL.

### No subscription leadership

SQLite serves one process, so it needs no lease. `streamrune.subscription.single-active-consumer.enabled`
means `false` on SQLite when it is not set, and an explicit `true` fails the start with a message
saying SQLite is single-instance. Without an integration, `BackendStores.leadership(ttl)` returns
`SubscriptionLeadership.NOOP`. The saga forward fence's `auto` setting
(`streamrune.saga.forward-fence`) follows the leadership switch, so it is off on SQLite unless set
to `on`; `on` works on SQLite.

### Polling only

Projections and sagas read new events by polling (`PollingEventSubscription`), which is also the
PostgreSQL default, and the SSE feed polls the global stream on its own interval
(`streamrune.sse.polling-interval`). SQLite has no `LISTEN`/`NOTIFY`, so there is no push
subscription. New events reach a projection within `streamrune.polling-interval` plus its jitter.

### No statement timeout

SQLite has no statement timeout: the driver's query timeout only shortens `busy_timeout` for a while
and does not stop a statement that is computing. `streamrune.event-store.statement-timeout` is
ignored on SQLite, and when it is set the integrations log one WARN at startup saying so and asking
to remove the property. Waiting for the write lock stays bounded by `busy_timeout`.

### No in-memory databases

In-memory and temporary databases are refused at startup (see
[Connection settings](#connection-settings)).

### Read models: PostgreSQL's query results, PostgreSQL's limits

The SQLite read-model store returns the rows, in the order, that PostgreSQL returns for the same
data: filters and sorting compare a field's JSON value as text, as PostgreSQL's `data->>'field'`
does, ids compare byte-wise as PostgreSQL's `COLLATE "C"` does, and declared indexes are expression
indexes on exactly that text. The one exception is an object or array field, whose text is compact
JSON on SQLite and PostgreSQL's `jsonb` layout there.

A read model's JSON is stored as PostgreSQL's `jsonb` stores it: numbers in plain decimal notation
with their scale (`19.90` stays `19.90`, `1.0E-4` reads back as `0.00010`), and of two equal keys
the last one kept. What `jsonb` refuses is refused before anything is written:

- text that is not JSON;
- a string or a key holding U+0000;
- a number whose written exponent is beyond ±1073741823 (even a zero);
- a number with more than 131072 digits before the decimal point or 16383 after it.

One limit is SQLite's own: its JSON functions nest at most **1000 levels** deep, so a deeper
document is refused too. Inside a projection batch each of these refusals aborts the batch, as the
failed insert does on PostgreSQL.

A projection name may not start with `sqlite_`, the prefix SQLite reserves for its own tables; such
a projection is refused when it is registered with a runner over the SQLite store. The other
projection-name rules are PostgreSQL's: `[a-z_][a-z0-9_]{0,57}`.

### Timestamps

The SQLite schema stores every instant as an `INTEGER` of microseconds since the epoch, stamped
with the application's `Clock`: with one process there is no database clock to agree with.
Instants round-trip at microsecond precision, as on PostgreSQL.

### Schema

Three schema series, each with its own Flyway history table, as on PostgreSQL. Several series and
the application's own tables can share the file, as long as no application table takes a
StreamRune table's name.

| Series | Location | History table | Applied by |
|---|---|---|---|
| Event store | `db/sqlite-migration` | `flyway_schema_history_streamrune` | the event store factory, under `streamrune.event-store.schema.auto-initialize` |
| Read models | `db/sqlite-readmodel-migration` | `flyway_schema_history_readmodel` | the default read-model store, under `streamrune.read-model.schema.auto-initialize` |
| Crypto | `db/sqlite-crypto-migration` | `flyway_schema_history_crypto` | the event store factory, when the crypto engine reports crypto tables |

## Crypto and GDPR erasure

Crypto-shredding works on SQLite with every crypto engine. The **SQLite key-store engine**
(`streamrune.crypto.sqlite.enabled=true`, module `streamrune-sqlite-crypto`) keeps one AES-256 key
per subject in the `encryption_keys` table of the application's SQLite file, with the semantics of
the PostgreSQL engine: a terminal `deleteKey`, key generations that keep pre-erasure ciphertext
reading `[REDACTED]` after a reinstate, verified encryption. The filesystem, Vault and AWS KMS
engines work on SQLite too. The integrations take the forget signal of every engine, and the
forgotten-subject store of Vault and AWS KMS, from the selected backend: on SQLite the tombstones of
Vault and AWS KMS are kept in the SQLite file, and the forget signal is an in-process one (one
instance has no peer to tell).
`streamrune.crypto.postgres.enabled=true` with `streamrune.backend=sqlite` fails the start, as
`streamrune.crypto.sqlite.enabled=true` does with `postgres`.

With the SQLite key-store engine, the keys live in the same file as the ciphertext: protect the
file, and its backups, as you protect a key store.

Where to erase, and what a projection batch does, is specific to SQLite:

- **Never erase inside a projection batch.** `deleteKey`, `reinstate` and the forgotten-subject
  store's `forget` and `reinstate` refuse at once with an `IllegalStateException` when the current
  thread runs a projection batch or replay on the same file: an erasure that committed or rolled
  back with the batch would not be durable when it returns. Run `ForgetSubjectService.forget` from a
  command handler, a saga step or other application code.
- **A key minted inside a batch joins the batch.** When a projection writes an `@Encrypted` field of
  a subject that has no key yet, the engine mints the key on the batch's own connection, inside the
  batch's transaction, so the key commits or rolls back with the read models encrypted under it. A
  batch that rolls back after such a mint leaves, in a `CachedCryptoEngine`, plaintext cached under
  a ciphertext that no row holds; it is unreachable and expires with the cache's write TTL. Until
  the batch commits, `isKeyAvailable` read on another connection does not see the key: treat it as
  advisory.
- **An application transaction on the same file cannot mint a key**: see
  [Application transactions are never joined](#application-transactions-are-never-joined).
- **A cache miss waits a bounded time.** `CachedCryptoEngine` serializes a cache-miss decrypt and an
  erasure of the same subject on a lock stripe. An erasure on SQLite waits for the write lock, which
  a projection batch holds while its updater decrypts; so a cache-miss decrypt waits at most 250 ms
  for its stripe, then decrypts without caching the result. An erasure called inside a batch (which
  the engine refuses) can still wait for the stripe while another thread's erasure of a subject on
  the same stripe holds it: that erasure then fails after `busy_timeout`.

See [GDPR erasure](advanced/gdpr-erasure.md) for the forget service, purgers and audit trail, which
are the same on both backends.

## Startup and shutdown

**At startup** each integration opens the backend's stores when the application starts, not when
the first command arrives: Spring creates the `BackendStores` bean with the other singletons,
Quarkus opens it in a startup observer that runs after its startup validators, and Micronaut in a
startup listener that runs after its validators. Opening first verifies the `DataSource`: a
database that is not SQLite fails the start naming the `sqlite` backend and the database's product
and version (the other way round, `postgres` on a SQLite `DataSource` fails its PostgreSQL version
check when its stores first reach the database, which need not be at startup). Then it checks every setting above, and then takes the instance guard, so another process
on the file fails the start.
Verification alone takes no guard: only opening finds another holder.

**At shutdown** the bundle closes last, after every user of its stores, because a store that kept
running after the bundle released the guard would write without it while a second process may
already hold the file:

- the framework's runners, sweepers and pollers stop first, in the shutdown phase of each
  integration;
- a retention sweeper's `close()` waits up to 30 seconds for a sweep in flight. It returns earlier
  once the sweep ends; after 30 seconds, or when the closing thread is interrupted, it logs a WARN
  and returns while the sweep may still run;
- Spring destroys the store beans, and every bean that injects them, before the bundle; Micronaut
  closes the bundle after every user of its stores; on Quarkus an `@ApplicationScoped` bean's
  `@PreDestroy` runs before the bundle closes, while the order among `@Singleton` beans is
  unspecified, so an application `@Singleton` must not write through a store in its `@PreDestroy`:
  it observes the shutdown event instead.

## Plain Java

Without a container, the application wires what an integration would. The framework's own
end-to-end test (`SqliteEndToEndIT`) runs this wiring, and every store comes from the bundle: no
SQLite store class is constructed by hand.

```java
HikariDataSource pool = new HikariDataSource(config); // the URL under "Connection settings"

// 1. The crypto engine first: the bundle takes it in its context.
//    The call checks the file and its guard, and holds neither.
CryptoEngine engine = new SqliteCryptoBackend()
    .keyStoreEngine(pool, new CryptoBackend.KeyStoreEngineSettings(null, StreamRuneMetrics.NOOP));

// 2. The bundle: one handle on the file; the instance guard is held until stores.close().
BackendStores stores = new SqliteBackend().open(new BackendContext(
    pool, engine, Clock.systemUTC(), StreamRuneMetrics.NOOP, StreamRuneTracing.NOOP,
    true,    // migrate the event-store (and crypto) series
    true,    // migrate the read-model series
    true));  // validate the schema

ReadModelStore readModels = stores.defaultReadModelStore();
OutboxStore outbox = stores.outboxStore(
    new OutboxOptions(OutboxOptions.DEFAULT_CLAIM_LEASE, OutboxOrderingMode.STRICT_PER_AGGREGATE));
CommandInbox inbox = stores.commandInbox();

// 3. The event store: the outbox, its mapper and the inbox are written in each append.
EventStore eventStore = stores.eventStoreFactory(new EventStoreOptions(
        typeRegistry, List.of(), List.of(), outbox, outboxEventMapper, inbox,
        null,     // no event audit store
        null))    // statement timeout: ignored on SQLite
    .create();

// 4. The command bus takes the same inbox.
VirtualThreadCommandBus commandBus = VirtualThreadCommandBus.builder()
    .eventStore(eventStore)
    .commandInbox(inbox)
    .register(/* deciders */)
    .build();

// 5. A projection over the default store checkpoints in that store.
SubscriptionConfig polling = SubscriptionConfig.pollingOnly(Duration.ofMillis(500));
MultiProjectionRunner projections = MultiProjectionRunner.builder()
    .eventStore(eventStore)
    .subscriptionConfig(polling)
    .leadership(stores.leadership(Duration.ofSeconds(30)))   // NOOP on SQLite
    .register(ProjectionName.of("customer_summary"), projection, ProjectionErrorStrategy.HALT,
        ProjectionDeliveryMode.TRANSACTIONAL_LOCAL, readModels.checkpointTarget())
    .build();

// 6. A saga on the bundle's saga stores, fed by a polling subscription on its offset store.
SagaRunner<WelcomeSagaState> sagaRunner = SagaRunner.<WelcomeSagaState>builder()
    .orchestrator(new WelcomeSaga())
    .sagaStore(stores.sagaStore())
    .sagaDeadLetterStore(stores.sagaDeadLetterStore())
    .commandBus(commandBus)
    .build();
PollingEventSubscription sagas = new PollingEventSubscription(
    SubscriptionName.of("welcome-saga"), eventStore, stores.offsetStore(),
    sagaRunner.asEventListener(), polling, 50);

projections.start();
sagas.start();
```

What the wiring has to get right:

- **The engine before the bundle, on the same pool.** `BackendContext` carries the crypto engine,
  so build the engine first. `SqliteCryptoBackend.keyStoreEngine` opens a handle for the call only:
  it checks the file and that no other process holds its guard, and releases its reference before
  it returns. The bundle then takes the guard for the run. Pass `null` as the engine for an
  application without `@Encrypted` fields.
- **One command inbox, given twice.** `stores.commandInbox()` goes into `EventStoreOptions`, where
  the event store claims the key inside its append, and into the command bus builder, where the bus
  short-circuits a repeated key. Without it on the bus, `SagaRunner.build()` refuses the bus; without
  it in the options, the event store refuses every keyed append with an
  `UnsupportedOperationException`.
- **The outbox claim lease.** `OutboxOptions` takes the lease and the ordering mode explicitly;
  `OutboxOptions.DEFAULT_CLAIM_LEASE` (4 minutes) is the value the integrations use.
- **The projection's checkpoint target.** Register a projection that writes to the default store
  with `readModels.checkpointTarget()`, so its checkpoint commits with its read models. The runner
  needs no runner-wide offset store or batch processor of its own.
- **The forget service's snapshot decision.** `ForgetSubjectService` insists on one: give it
  `stores.snapshotPurger(subject -> streamsOf(subject))` among its purgers, or call
  `withoutSnapshotPurge()` when the application never stores snapshots. Give it
  `stores.auditStore()` for the audit trail, and call it from application code, never from inside a
  projection's updater.
- **Close in order, and on every path.** Stop the runners and subscriptions, then the command bus,
  then close the bundle (which releases the guard), then the pool. A store still in use after
  `stores.close()` keeps writing the file without the guard. Wrap the start in `try`/`finally` (or
  try-with-resources) so a failure half-way through closes what was already opened: an open bundle
  keeps holding the guard until the process ends.

  ```java
  projections.close();
  sagas.close();
  commandBus.close();
  stores.close();
  pool.close();
  ```

- **The JVM option** `--enable-native-access=ALL-UNNAMED` (see below).

A read-model store on a second SQLite file opens its own `SqliteDatabase` handle, so that file has
its own guard:

```java
SqliteDatabase searchFile = SqliteDatabase.open(searchPool);
SqliteReadModelStore search = SqliteReadModelStore.create(
    "search", searchFile, SqliteProjectionRepository.createObjectMapper(engine, metrics),
    true,   // migrate the read-model series
    true);  // validate it
// ... and searchFile.close() after every user of the store has stopped
```

## Backup and restore

Back up a running database with SQLite's own tools, which take a consistent copy while the
application writes:

- `VACUUM INTO '/backup/app-2026-11-24.db'`, an SQL statement run on any connection of the file; it
  writes a compacted, self-contained copy;
- the SQLite online backup API, for example the `sqlite3` shell's
  `.backup '/backup/app-2026-11-24.db'`.

**Never copy the database file alone while the application runs.** In WAL mode committed
transactions live in the `-wal` file until a checkpoint moves them into the database file, so a copy
of the database file without its `-wal` file loses commits or is unreadable. A file-system copy is
consistent only with the application stopped, or as a snapshot of the volume that holds all three
files at one instant.

To restore, stop the application, replace the database file with the backup, delete the old `-wal`
and `-shm` files of the replaced database, and start the application. The backup holds every
StreamRune table at one point in time, which is what the tables need (see
[Backup, restore and disaster recovery](production.md#backup-restore-and-disaster-recovery)). With
the SQLite key-store engine the backup holds the keys too: a subject forgotten after the backup was
taken can be read again from the backup, so keep backups only as long as your erasure policy
allows, and re-run the erasures recorded since the backup after a restore.

## Native access and native image

**JVM.** The instance guard calls `flock(2)` through the Foreign Function and Memory API, and the
sqlite-jdbc driver loads a native library; both are restricted operations. Start the JVM with
`--enable-native-access=ALL-UNNAMED` (or put `Enable-Native-Access: ALL-UNNAMED` in the manifest of
an executable JAR). Without it, Java 25 prints a warning about restricted methods on first use, and
under `--illegal-native-access=deny` the guard refuses to start with a `SqliteGuardException` that
names the option; nothing falls back to a weaker lock.

**Native image: registered, not yet verified end-to-end.** Each integration registers what a native
image of a SQLite application needs:

- the two backend classes, `org.streamrune.sqlite.SqliteBackend` and
  `org.streamrune.crypto.sqlite.SqliteCryptoBackend`, for reflection (they are loaded by name);
- the scripts of the three SQLite schema series as resources;
- the four foreign downcalls of the instance guard (`open`, `flock`, `close`, `strerror`).

The sqlite-jdbc driver ships its own native-image metadata. Pass `--enable-native-access=ALL-UNNAMED`
to `native-image` as well. The integrations' tests check each registration, and the guard's
downcalls were checked in a native image built with GraalVM 25; no native image of a whole SQLite
application has been built and run by the project yet.

## Operations

- **`busy_timeout` and the longest batch.** A write that fails with the write-lock message waited
  `busy_timeout` for the lock. Look first for an application transaction on the same file, then for
  a projection batch or replay that runs longer than `busy_timeout`, then for a schema migration.
- **After a failed `ROLLBACK`.** When a store's `ROLLBACK` fails, it closes the physical connection,
  so SQLite rolls back and releases the write lock. HikariCP does not learn of the close when the
  connection is returned: it hands that connection out again until it validates it on borrow (it
  does so only when the connection was idle longer than its 500 ms alive-bypass window), validates
  it in its keepalive check, or retires it at `maxLifetime`. Until then each call on it fails
  cleanly with an exception, and it holds no lock.
- **Disk full.** SQLite may roll a transaction back on its own when the disk fills (`SQLITE_FULL`);
  the stores handle both outcomes. A projection batch that meets it fails as a whole, and its rerun starts from the committed
  checkpoint.
- **WAL growth.** SQLite moves the `-wal` file's pages into the database file at checkpoints, which
  a long-running read can hold back; a `-wal` file that keeps growing points at a read that does not
  end.
- **Log lines after an erasure.** Every read of a forgotten subject's `@Encrypted` field logs a WARN
  that the decrypt fell back to `[REDACTED]`, and counts `streamrune.crypto.subject_redacted`. An
  application that lists read models of forgotten subjects logs one line per such field read; watch
  the metric, and lower the level of that logger if the lines are noise for you.
- **Polling interval.** Polling a local file is cheap, so a lower `streamrune.polling-interval`
  shortens how long a projection lags behind the event store.
- **Multi-replica guidance does not apply.** The aggregate lock across replicas, single-active
  consumer leadership and connection-pooling proxies in [Production](production.md) concern
  PostgreSQL. An aggregate's commands serialize on the in-process `LocalStripedLocker`, the
  integrations' default.
