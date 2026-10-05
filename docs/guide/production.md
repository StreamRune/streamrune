# Production Deployment Guide

## PostgreSQL setup

StreamRune requires **PostgreSQL 17 or newer**. Every PostgreSQL-backed test suite of the default build runs against PostgreSQL 18 (the `postgres:18-alpine` Testcontainers image) and again against PostgreSQL 17 (`postgres:17-alpine`) in the `postgres-17` CI job (the `@Tag("broker")` end-to-end suite, run by its own workflow, only on 18); no older server is tested, so none is supported.

The runtime enforces it: `PostgresEventStoreFactory.create()` — the entry point the Spring, Quarkus and Micronaut integrations all build their event store through — reads the server version before it migrates anything and throws `UnsupportedServerVersionException` below 17 (`StreamRune requires PostgreSQL 17 or newer`), and so do the plain-Java entry points the QUICKSTART uses: `PostgresEventStoreFactory.initializeSchema()` before its first migration and the static `PostgresEventStore.create(...)` before it builds the store. An unsupported server is therefore never written to through any of them. `autoInitializeSchema(false)` and `validateSchema(false)` do not switch the check off; they govern the schema, not the product. If you build stores straight from a `DataSource` any other way (`PostgresEventStore.builder()`, the saga store, the projection repository), call `PostgresServerVersion.requireSupported(dataSource)` once at startup.

**One pool: your application's.** The event store and every other StreamRune store borrow their connections from the application's `DataSource` exactly as it is given — the Spring Boot HikariCP pool, the Quarkus Agroal pool, the Micronaut pool, or the one you pass to `PostgresEventStoreFactory` yourself. StreamRune builds no pool of its own around it and changes none of its settings (auto-commit, prepared-statement threshold, timeouts), so the size you configure below is the whole budget for commands, projections, the outbox relay, saga sweepers and leadership renewals alike. The only connections opened outside that pool are the dedicated `LISTEN` connections described under [Subscription resilience configuration](#subscription-resilience-configuration), the single-connection `LISTEN` pool of the crypto-shredding forget signal (`PostgresCryptoForgetSignal`) when you use it, and `PgAdvisoryLocker.withDedicatedPool(...)` when you opt into it. A subscription opens its own `LISTEN` connection only when the pool is a `HikariDataSource`; on any other pool — Agroal on Quarkus, or a proxy around a HikariCP pool — each push subscription keeps one of the pool's own connections for its `LISTEN` for as long as it runs, so add one connection per subscription to the pool size. Hand the factory a pooling `DataSource`: a plain `PGSimpleDataSource` works but opens a new physical connection for every operation.

**The event store bounds its own statements.** `streamrune.event-store.statement-timeout` (default `30s`; `PostgresEventStoreFactory.statementTimeout(...)` without an integration) cancels any event-store statement that runs longer — a load, a read, a snapshot write, or an append queued on the global-offset counter (see [Write capacity](#write-capacity-one-global-write-lane)) — and the operation fails with an `EventStoreException` caused by SQLState `57014`. An append transaction carries the bound as `SET LOCAL statement_timeout`, which also covers the outbox, inbox and audit rows written in the same transaction and ends with it; every other statement carries it as its JDBC query timeout. A load reads a stream in pages of 500 events, and the driver applies the query timeout to the first page only: the later pages of a longer stream are fetched under your session's own `statement_timeout`. Neither is a session setting, so the connection goes back to your pool exactly as it was borrowed and your own `statement_timeout` (a role setting, `connection-init-sql`, `new-connection-sql`) still governs everything else, including the other StreamRune stores. `0` leaves event-store statements to that session setting too.

**Projection read models are bounded by your session setting, not by this property.** `JdbcProjectionRepository` (the repository `createProjectionRepository()` and the Spring, Quarkus and Micronaut projection-repository beans build) sets no timeout of its own. A projection batch runs your projection's writes inside the transaction that holds the projection's checkpoint-row lock, and their cost depends on your read model, not on the event store; the same repository serves your application's queries, `findAll` over a whole view included. A bound sized for an event-store statement would cancel legitimate work there, and a cancelled batch is retried and cancelled again, stalling the projection. Size the session `statement_timeout` (role, database or pool setting) for the largest projection batch and view query you run; it is what bounds them.

Connection pooling with HikariCP recommended settings:

- `maximumPoolSize`: `(cores * 2) + spindle_count`. For 4 CPU cores, start with 10.
- `minimumIdle`: equal to `maximumPoolSize`.
- `connectionTimeout`: 30000ms
- `idleTimeout`: 600000ms
- `maxLifetime`: 1800000ms
- `keepaliveTime`: 60000ms
- Projections on the JDBC processor: an at-least-once projection that writes to the processor's own `DataSource` borrows a second connection while the processor's checkpoint transaction holds one, so P such projections catching up at once need more than P connections, or they wait for `connectionTimeout` and record `TRANSIENT` failures.

Spring example:

```properties
spring.datasource.hikari.maximum-pool-size=10
spring.datasource.hikari.minimum-idle=10
spring.datasource.hikari.connection-timeout=30000
spring.datasource.hikari.idle-timeout=600000
spring.datasource.hikari.max-lifetime=1800000
spring.datasource.hikari.keepalive-time=60000
```

On Quarkus the same budget is `quarkus.datasource.jdbc.max-size` / `min-size` (Agroal); on Micronaut, `datasources.default.maximum-pool-size` / `minimum-idle`.

> **Pool auto-commit mode: either setting works.** The stores do not depend on the pool handing out `autoCommit=true` connections. Multi-statement writes (an append with its inbox, outbox and audit rows; a saga dead-letter publish with its shield; the projection batch commit) have always run as explicit transactions. Every single-statement write — an outbox claim or `markDelivered`, a projection offset, a projection read-model row written outside a batch, a projection table creation, a fencing-epoch stamp, a snapshot, a dead-letter row, a saga state row, a lease acquire or renew, an audit row, a retention `DELETE`, a crypto tombstone, a `pg_notify` — checks the borrowed connection's mode: with `autoCommit=true` the statement commits itself, with `autoCommit=false` (for example `spring.datasource.hikari.auto-commit=false`, the usual Hibernate tuning, or a Quarkus/Micronaut pool configured the same way) the store commits after the statement and rolls back on failure. The pool's setting itself is never changed. `LISTEN` on a pooled connection is committed the same way, so push notifications arrive whichever mode the pool uses. Reads leave an `autoCommit=false` connection's implicit transaction to the pool's rollback-on-return, which is what the pool does with any read-only transaction.
>
> Call the stores, the command bus and the operator APIs (outbox replay or skip, dead-letter discard) **outside** any transaction your framework manages: not from a Quarkus `@Transactional` method, nor anywhere else a JTA or framework transaction owns the `DataSource`'s connections. A store commits its own writes on the connection it borrows, and a pool that has enlisted that connection in the surrounding transaction refuses the commit, which fails the write and breaks the surrounding transaction. Start the call outside the transaction, or on its own thread.

> **PgBouncer / connection-pooling proxies.** If you are running many application instances and PostgreSQL max_connections is a limiting factor, add PgBouncer (or pgcat/Odyssey) in front of PostgreSQL. **No particular pooling mode is required for correctness of the event log or of leadership** — the next note. What a transaction-pooling proxy does change is listed in the notes after it: push notifications, server-side prepared statements, session-level settings and schema migration. They describe **transaction** pooling, the mode the project tests (`LeaseLeadershipPgBouncerTxModeIT` and `EventStorePgBouncerTxModeIT` run against PgBouncer 1.23); **statement** pooling disallows multi-statement transactions — which every append, projection commit and outbox claim is — and is not an option.
>
> **Leadership and fencing are correct under ANY pooling mode, including PgBouncer transaction mode.** Single-active-consumer leadership (`LeaseBasedLeadership`) is a persisted, DB-clocked lease in the `subscription_leases` table — not a PostgreSQL session-scoped advisory lock. Acquire and renew are each a single atomic upsert/update statement, so they need no session-stable connection and are unaffected by a rotating pool of backends. The projection-offset commit (`JdbcProjectionRepository.executeAtomically`) additionally checks the caller's held lease **epoch** — a monotonic fencing token — against the epoch already stamped on the checkpoint row, inside the same locked transaction, and rejects (rolls back) any commit from a superseded leader. Split-brain double-apply is therefore not just less likely under transaction pooling — it is **structurally impossible**: a stale leader's write is refused by the database itself before a single read-model row is touched, not merely detected after the fact. That guarantee has exactly one precondition, and the framework now **enforces** it instead of assuming it: an epoch is only a guarantee if the configured `AtomicBatchProcessor` can honour it, so the projection runners **refuse to start** when leadership is enabled and the processor cannot fence (see [the startup guard](#single-active-consumer-leadership) below). A deployment therefore either fences or does not boot — it can no longer run the leadership protocol against a processor that ignores the epoch. This is proven by an acceptance IT, `LeaseLeadershipPgBouncerTxModeIT`, which drives two replicas through a real PgBouncer container in **transaction mode** with `pool_size=1` — the exact configuration that split-brained the retired advisory-lock design. See [Single-active-consumer leadership](#single-active-consumer-leadership) for the full mechanics.
>
> **LISTEN/NOTIFY.** Transaction- or statement-pooling breaks PostgreSQL `LISTEN/NOTIFY`, which `HybridEventSubscription` relies on for low-latency push: a listening connection must stay attached to one backend session to receive notifications, and a transaction-pooling proxy multiplexes statements across a rotating set of backends. If you run a transaction-pooling proxy, set `SubscriptionConfig.pollingOnly(Duration.ofSeconds(5))` to disable LISTEN/NOTIFY and fall back to polling — delivery still works, just at polling latency instead of push latency. It affects push latency only, not leadership, fencing, or correctness.
>
> **Prepared statements — a driver and pooler setting, not a StreamRune one.** StreamRune leaves the driver's prepared-statement settings alone (see "One pool" above), and pgjdbc's default switches a statement to a named server-side prepared statement from its fifth execution on a connection. A transaction pooler that does not track those links the next transaction to a server connection that never saw the `Parse`, and the call fails with `prepared statement "S_1" does not exist` (SQLState `26000`) — an `EventStoreException` on a command or a read, after the first few executions rather than at startup, so it shows up under load. PgBouncer tracks prepared statements from 1.21 **when `max_prepared_statements` is above `0`**, and that setting is `0` (off) in the 1.23 the project tests with; pgcat and Odyssey need their own prepared-statement support enabled, which the project does not test. Either run PgBouncer 1.21 or newer with `max_prepared_statements` set above `0`, or switch the driver's server-side prepared statements off on the application's JDBC URL, `jdbc:postgresql://pooler:6432/app?prepareThreshold=0`, which works behind any pooler at the price of parsing each statement every time. `EventStorePgBouncerTxModeIT` pins the failure and both remedies.
>
> **Session-level settings do not survive transaction pooling.** StreamRune sets none: the event store's statement bound is `SET LOCAL` inside the append transaction or a JDBC query timeout on the other statements (see above), so no server connection keeps it after the transaction, and the query timeout is a cancel request that PgBouncer forwards to the server connection running the statement (a proxy that does not forward cancels leaves reads unbounded; appends do not depend on it). What your application sets on its own is different: PgBouncer does not track a `SET` in transaction mode, so a pool's `connection-init-sql` / `new-connection-sql` such as `SET statement_timeout = 30000` runs once on whichever server connection the client happened to be linked to, stays there for every other client linked to it later — cancelling their statements at 30 s with nothing pointing at the cause — and is absent on every other server connection, including the ones carrying your own queries. Set such defaults where every server connection gets them: `ALTER ROLE app SET statement_timeout = '30s'` or `ALTER DATABASE app SET statement_timeout = '30s'`. That is also the setting that governs the stores with no bound of their own (outbox, offsets, sagas, dead letters, leases, audit, projection read models) behind a pooler.
>
> **Run schema migration on a direct connection, not through the pooler.** Flyway 13 takes its PostgreSQL lock as a transaction-scoped advisory lock and, in that mode, works on two connections at once: one holds the lock transaction while the other runs the migration. Behind a transaction pooler that is two server connections at the same moment, and every replica waiting for the lock keeps one for itself. A pool with a single server connection therefore **can never migrate**: `streamrune.event-store.schema.auto-initialize=true` (`initializeSchema()`) waits for the second connection until the pooler's `query_wait_timeout`, fails with `FATAL: query_wait_timeout` and leaves the schema untouched. The same happens when the replicas that start together are as many as the pool has server connections: the one holding the lock finds every connection taken by the others waiting for it, and its startup fails after the timeout (measured with four replicas against four server connections; the others then migrate). With server connections to spare the lock holds — the pooler keeps the lock transaction on one server connection — and `EventStorePgBouncerTxModeIT` migrates three replicas started together through a four-connection pooler exactly once (a replica that loses the race for the new history table runs the migration again, see [Flyway in production](#flyway-in-production)); but that is a sizing coincidence, not a guarantee. With a pooler in front, set `streamrune.event-store.schema.auto-initialize=false` and apply the shipped series from a migration job that connects straight to PostgreSQL with a DDL-capable role (see the grants note below) — the way `LeaseLeadershipPgBouncerTxModeIT` provisions its schema.

> **Crypto and the pool:** if you use `@Encrypted` with a network-backed `CryptoEngine` (Vault, AWS KMS), encrypt/decrypt runs entirely outside the event store's JDBC connection scope on all append, load, read, and snapshot paths — a pooled connection is never held open across a KMS round trip, so the `maximumPoolSize` guidance above doesn't need to be inflated to cover crypto latency. The one place KMS latency still matters is the command-bus's per-aggregate lock: a command handling a same-aggregate write still holds that in-process lock for the duration of its own KMS calls, serializing concurrent commands against the *same* aggregate. That's inherent to the pessimistic-lock model, not pool starvation, and does not tie up a database connection.

> **`PgAdvisoryLocker`'s dedicated lock pool: budget for cross-aggregate nested dispatches too.** The command bus is reentrant for a same-thread, same-aggregate nested dispatch (an INLINE process-manager projection dispatching a follow-up command against the aggregate it is already processing costs no extra lock connection), but a nested dispatch against a **different** aggregate from inside `after()` is a genuinely independent acquisition: it checks out a second lock connection while the outer one is still held for the whole outer command, so that in-flight command needs three connections concurrently (two lock, one event-store) instead of the two `PgAdvisoryLocker`'s javadoc otherwise budgets for. Size `withDedicatedPool(...)` accordingly if any INLINE projection dispatches cross-aggregate commands, or register those process managers `CONTINUOUS`/`SCHEDULED` instead, where there is no held outer lock to nest under. See `PgAdvisoryLocker`'s class javadoc for the full connection-budgeting math.

Grant minimal database privileges: `SELECT`, `INSERT`, `UPDATE`, `DELETE` on StreamRune tables, plus `USAGE` on their sequences (`audit_log.id`, `event_audit_log.id` and `outbox_events.seq` are `BIGSERIAL`, and drawing from a sequence needs `USAGE`). `DELETE` is required — the retention sweepers and the discard/delete/purge paths issue `DELETE FROM` against the command inbox, outbox, saga dead-letters, command dead-letter queue, saga state (`SagaStore.delete`), projection dead-letters, projection read-model rows, snapshot rows (the GDPR snapshot purger), and the crypto key and tombstone tables.

> **These DML-only grants assume the schema is migrated out-of-band.** With the default `streamrune.event-store.schema.auto-initialize=true` (see the Flyway section below), `PostgresEventStoreFactory.create()` runs Flyway itself at startup and additionally needs `CREATE` on the schema plus `ALTER`/`CREATE INDEX` on its own tables — a role granted only `SELECT`/`INSERT`/`UPDATE`/`DELETE` fails startup the first time a migration is pending. For a DDL-less runtime role, set `streamrune.event-store.schema.auto-initialize=false` and apply `classpath:db/streamrune-migration` (recorded on its own `flyway_schema_history_streamrune` table; plus `classpath:db/crypto-migration` / `flyway_schema_history_crypto` if using a crypto backend) with a separate, DDL-capable migration role before the deploy.

Enable `pg_stat_statements` for query performance monitoring.

## Flyway in production

StreamRune ships its schema as **one baseline migration per Flyway location**, each recorded in its own history table:

| Series | Location | Migration | History table | Creates |
|---|---|---|---|---|
| Event store (`streamrune-postgres`) | `classpath:db/streamrune-migration` | `V001__streamrune_baseline.sql` | `flyway_schema_history_streamrune` | `event_stream`, `global_offset_sequence` (seeded with its single counter row), `snapshot_store`, `projection_offset`, `subscription_leases`, `projection_dead_letters`, `command_inbox`, `dead_letter_queue`, `audit_log`, `event_audit_log`, `outbox_events`, `saga_state`, `saga_dead_letters` |
| Crypto (`streamrune-postgres-crypto`) | `classpath:db/crypto-migration` | `V001__crypto_baseline.sql` | `flyway_schema_history_crypto` | `encryption_keys`, `forgotten_subjects`, `erased_key_generations` — each only when absent (`CREATE TABLE IF NOT EXISTS`) |

**Stream ids.** `(aggregate_type, aggregate_id)` is the stream key of every stream-keyed table; no table stores a composed id. Logs, JSON and the Kafka key show `<type>:<id>` — split it at the first `:` (a type never contains one). To read one aggregate type's events: `SELECT aggregate_type, aggregate_id, event_type, version FROM event_stream WHERE aggregate_type = 'inventory' ORDER BY global_offset;`

Every object is created unqualified, in the connection's current schema (`currentSchema` / `search_path`). `SchemaValidator.EXPECTED_VERSION` is the highest event-store version this build ships — the baseline, `1` — and startup validation (below) compares the namespaced history against it. A baseline is frozen once released; later schema changes ship as new versioned migrations on top of it.

> **StreamRune never touches Flyway's defaults — `classpath:db/migration` and `flyway_schema_history` are yours.** Both StreamRune series are namespaced: the event store at `db/streamrune-migration` / `flyway_schema_history_streamrune`, the crypto backends at `db/crypto-migration` / `flyway_schema_history_crypto`. Flyway locations share ONE version namespace, so a framework series sitting at your application's default location merges the two: your `V1__create_orders.sql` and StreamRune's `V001__streamrune_baseline.sql` are the same `MigrationVersion` and the run aborts with `Found more than one migration with version 1`. Numbering around the collision is worse, not better — `PostgresEventStoreFactory.initializeSchema()` would then execute *your* DDL under the framework's DataSource and the least-privilege credentials above. Do **not** add either StreamRune location to your own `flyway.locations`.
>
> **`streamrune.event-store.schema.auto-initialize`** (Spring `StreamRuneProperties.Schema.autoInitialize`, and the equivalent property on Quarkus/Micronaut) gates whether StreamRune runs this Flyway migration itself at startup. **Defaults to `true` on all three integrations** (`PostgresEventStoreFactory.autoInitializeSchema` also defaults `true` if you build the factory directly) — the common case needs no configuration, but it means the runtime database role needs `CREATE`/`ALTER`/`CREATE INDEX` on the schema, not just DML, unless you turn this off. Set it to `false` for a least-privilege runtime role that only ever does DML; see the Security checklist below for the DDL-less setup. It works the same way in a GraalVM native image: the factory looks up each shipped script by name, and the three integrations register the scripts as native resources (Quickstart §10).
>
> **Two consequences worth knowing.**
> 1. If StreamRune's schema auto-initialization runs *before* your own Flyway (the default; `streamrune.event-store.schema.auto-initialize=true`), your Flyway then meets a non-empty schema with no `flyway_schema_history` and refuses. Set `baseline-on-migrate=true` with `baseline-version=0`, exactly as the baseline strategy below prescribes — or let your own Flyway run first, which needs nothing.
> 2. StreamRune's own run baselines at 0 for the same reason, so it tolerates a schema that already carries your tables — as long as none of them shares a name with a StreamRune table: the event-store baseline uses plain `CREATE TABLE`, so a name collision fails the migration. Baselining at 0 skips nothing: the `V001` baseline is greater than 0.
> 3. Replicas that start together on a new database may all run it. Flyway reads whether its history table exists before it takes its lock, so one replica can read the table as absent, then find the schema non-empty because another replica has just created that table, and Flyway refuses to baseline it (`Unable to baseline schema history table … as it already exists, and is empty`). `initializeSchema()` recognises that refusal and runs the migration once more; the table now exists, so Flyway takes its lock before it reads the history and applies what is still pending, usually nothing. Every replica starts and the series is recorded once (`ConcurrentSchemaInitializationIT` starts eight replicas together on a new database, twenty times over). Any other migration failure is reported as it is.
>
> **The crypto series is applied automatically when it is needed.** When schema auto-initialization is on (the default) and the configured `CryptoEngine` reports crypto tables (`CryptoEngine.requiredCryptoTables()` is non-empty), `PostgresEventStoreFactory.initializeSchema()` applies `classpath:db/crypto-migration` as well, on its own `flyway_schema_history_crypto` table and baselined at 0. Because that baseline creates each table only when it is absent, the same run fits a schema where the application already provisioned some or all of the crypto tables: missing ones are created, present ones are left as they are, and startup validation reports any runtime-required column a pre-provisioned table lacks.

Avoid `outOfOrder=true` in production.

Baseline strategy for existing schemas: `flyway.baseline-on-migrate=true` with `flyway.baseline-version=0`.

The snippets below configure **your application's own** Flyway. StreamRune's series is applied by `PostgresEventStoreFactory.initializeSchema()` on its own configuration and needs no entry here.

Spring:

```properties
spring.flyway.enabled=true
spring.flyway.locations=classpath:db/migration
# needed only when StreamRune's schema auto-initialization runs before this Flyway
spring.flyway.baseline-on-migrate=true
spring.flyway.baseline-version=0
```

Quarkus:

```properties
quarkus.flyway.migrate-at-start=true
quarkus.flyway.baseline-on-migrate=true
quarkus.flyway.baseline-version=0
```

Micronaut:

```yaml
flyway:
  datasources:
    default:
      enabled: true
      baseline-on-migrate: true
      baseline-version: 0
```

> **Never append `classpath:db/streamrune-migration` or `classpath:db/crypto-migration` to the locations above.** Each StreamRune series is
> a *separate Flyway history*, not a second location: Flyway locations share one version namespace
> and both StreamRune series start at `V001` (as, typically, does your own), so a single configuration
> resolving more than one aborts with `FlywayException: Found more than one migration with version 1`.
> Apply them with their own Flyway runs, whose `table` is `flyway_schema_history_streamrune` and
> `flyway_schema_history_crypto` respectively — that is what
> `PostgresEventStoreFactory.initializeSchema()` does automatically when schema auto-initialization
> is on. Self-managed schemas: see *Applying the crypto migrations* in
> `streamrune-crypto/streamrune-postgres-crypto/README.md` for the Flyway CLI, Spring Boot
> (`FlywayMigrationStrategy`), Quarkus (second named datasource) and Micronaut
> (`flyway.datasources.<name>.table`) recipes.

> **Startup schema validation.** With `validateSchema` on (the default), `PostgresEventStoreFactory.create()`
> runs `SchemaValidator.validate(dataSource)` against the connection's current schema and throws
> `SchemaValidationException` on any `ERROR` — **the application refuses to start**; this is fatal,
> not advisory. It reports as `ERROR`:
>
> - a current schema that cannot be determined (the `search_path` names no existing schema);
> - a missing required table or a missing required column. The check covers every table the
>   event-store baseline creates — `event_stream`, `global_offset_sequence`, `snapshot_store`,
>   `projection_offset`, `subscription_leases`, `projection_dead_letters`, `command_inbox`,
>   `dead_letter_queue`, `audit_log`, `event_audit_log`, `outbox_events`, `saga_state` and
>   `saga_dead_letters` — so a schema missing any feature table (or a role that cannot see it) fails
>   startup outright rather than on first use of the feature;
> - a `global_offset_sequence` without its counter row, or a counter whose `next_value` is behind
>   `max(event_stream.global_offset)` (every append would collide) — the message carries the
>   statement that re-seeds it;
> - a `flyway_schema_history_streamrune` version *behind* `SchemaValidator.EXPECTED_VERSION`.
>
> Only two things validate as `WARNING` — logged, and tolerated: the recorded version being *ahead*
> of what this build expects, and the `flyway_schema_history_streamrune` table not being found at
> all (schema not Flyway-managed). When a `CryptoEngine` is configured, `create()` additionally runs
> `SchemaValidator.validateCryptoTables(...)` over the tables the engine reports: a missing crypto
> table or runtime-required crypto column is an `ERROR` too, and a missing-column message carries
> the exact `ALTER TABLE … ADD COLUMN` to run (the crypto baseline never alters an existing table).

## Health checks

| Framework | Endpoint | Check name | Class |
|---|---|---|---|
| Spring Boot Actuator | `GET /actuator/health` | `streamRune` | `StreamRuneHealthIndicator` |
| Quarkus MicroProfile Health | `GET /q/health/ready` | `streamrune` | `StreamRuneHealthCheck` (`@Readiness`) |
| Micronaut Management | `GET /health` | `streamrune` | `StreamRuneHealthIndicator` |

Spring:

```properties
management.endpoints.web.exposure.include=health,info,metrics
management.endpoint.health.show-details=always
management.endpoint.health.group.readiness.include=readinessState,streamRune
```

Spring Boot names a health contributor after its bean minus the `HealthIndicator` suffix, so the auto-configured `streamRuneHealthIndicator` bean is the `streamRune` check (Quarkus and Micronaut name theirs `streamrune`). Spring's `liveness` and `readiness` probe groups contain only `livenessState` and `readinessState` by default, and StreamRune adds its check to neither — without the `group.readiness.include` line above, the readiness probe below never sees a `DOWN` from StreamRune. Group members are matched by exact name and validated at startup (`management.endpoint.health.validate-group-membership` defaults to `true`), so a misspelled name fails the start instead of silently dropping the check.

Quarkus: `StreamRuneHealthCheck` is an ordinary `@Readiness` bean, so `/q/health/ready` (and `/q/health`) lists it beside every other readiness check — the datasource check Quarkus registers by default, and your own — and a Kubernetes readiness probe on `/q/health/ready` sees its `DOWN`. To switch it off, set `quarkus.smallrye-health.check."org.streamrune.quarkus.StreamRuneHealthCheck".enabled=false`; to replace it, switch it off and declare your own `@Readiness` check.

Micronaut: `/health/readiness` runs every health indicator that is not annotated `@Liveness`, so the StreamRune indicator is part of readiness with no configuration, and `/health/liveness` does not run it.

Kubernetes probes (Spring paths shown; Quarkus serves `/q/health/live` and `/q/health/ready`, Micronaut `/health/liveness` and `/health/readiness`):

```yaml
livenessProbe:
  httpGet:
    path: /actuator/health/liveness
    port: 8080
  initialDelaySeconds: 30
readinessProbe:
  httpGet:
    path: /actuator/health/readiness
    port: 8080
  initialDelaySeconds: 10
```

Keep the StreamRune check out of the liveness probe: a database outage turns it `DOWN`, and a liveness failure restarts the pod, which does not bring the database back.

**What turns the check `DOWN`.** All three integrations build the same picture: the check is `DOWN`
when the database does not accept a connection, when a subscription is stopped or terminally halted
(a projection that exhausted its error strategy or stopped on an event it can never read, a live
subscription that died), or when a started background relay's thread has died; otherwise it is `UP`.
These are the conditions that do not clear on their own. Everything that is still running but
behind or failing is `DEGRADED`: a subscription whose lag is at or above the lag threshold, one with
a run of consecutive delivery errors, a paused one, one whose LISTEN/NOTIFY push path is dead, and a
relay in backoff. `DEGRADED` shows in that component's own `status` detail and never changes the
overall status, so alert on the details and on the metrics below as well as on the check.

**Lag never makes the check `DOWN`.** Lag is measured against the `projection_offset` checkpoint
that every replica shares, so every replica reports the same value. A new projection replaying the
store from offset 0, a projection reset for a rebuild (see [Offset reset / rebuild
path](#offset-reset--rebuild-path)), or a large burst of appends puts it behind on all replicas at
once; a lag-driven `DOWN` would take every replica out of the readiness probe until the one active
consumer caught up, and a rolling deploy would never get a Ready pod. A subscription at or above the
threshold reads `DEGRADED` with its `lag` in the detail, and the `streamrune.subscriptions.lag`
gauge (events between the global head and the last committed offset) carries the same number — alert
on a sustained rise of that gauge. The threshold is one property, with the same key on Spring,
Quarkus and Micronaut:

```properties
# Lag, in events, at or above which a running subscription reads DEGRADED (default 1000, at least 1)
streamrune.subscription.health.lag-threshold=1000
```

> **The StreamRune check reflects real subscription health.** Projection runners register their
> subscription with `SubscriptionHealthContributor` and wrap the listener in
> `HealthTrackingEventListener`, so each subscription carries its own status rather than an
> always-`UP`. The same wiring emits the `streamrune.subscriptions.lag` gauge. On a bus with no
> health contributor wired (a single-instance / no-health setup) the check stays `UP` and the gauge
> is not emitted.

The details are:

- `subscription.<name>` — `state`, `lag`, `errorCount`, `status`, one per registered subscription.
- `relay.<component>` — `status` (`UP` / `DEGRADED` / `DOWN`), `started`, `alive`,
  `consecutiveFailures`, one per background relay. A relay is `DOWN` when it was started but its
  thread is no longer alive, `DEGRADED` while it is running with consecutive failed poll cycles.
  (Quarkus flattens each detail into keys such as `relay.<component>.status`.)

| Relay component | Present when |
|---|---|
| `outbox-relay` | the outbox relay (`OutboxPoller`) is wired (`streamrune.outbox.enabled=true`) |
| `dead-letter-retry` | the command dead-letter retry runner (`DeadLetterRetryRunner`) is wired |
| `saga-compensation-retry:<saga type>` | one per `SagaRunner`, named after its saga type — the saga state's fully-qualified class name, e.g. `saga-compensation-retry:com.shop.fulfillment.OrderSagaState` (a nested class renders with `$`). Two saga-state classes that share a simple name in different packages are two saga types and report as two components. With `streamrune.saga.compensation-retry-enabled=false` the name carries a `:sampling-only` suffix — that sweeper re-drives nothing and only samples the saga backlog gauges. With the knob off **and** no `StreamRuneMetrics` wired, no sweeper is built at all and the component is **absent** — "not configured", not a failure |
| `saga-timeout:<saga type>` | one per `SagaTimeoutRunner` bean, named after its saga type like the compensation-retry sweeper |
| `inbox-retention-sweeper`, `dead-letter-retention-sweeper`, `outbox-retention-sweeper`, `saga-dead-letter-retention-sweeper` | the framework's own retention sweepers. A sweeper whose retention is disabled never starts its thread and reads `UP` |

## Metrics and tracing

### Metrics

Provided by Micrometer via `StreamRuneMetrics` (`org.streamrune.core.metrics.MetricNames` is the
single source of truth — the table below tracks it):

| Metric name | Type | Description |
|---|---|---|
| `streamrune.commands.dispatched` | Counter | Total commands submitted |
| `streamrune.commands.succeeded` | Counter | Commands that completed successfully |
| `streamrune.commands.failed` | Counter | Commands that failed (after retries) |
| `streamrune.commands.duration` | Timer | Command execution duration |
| `streamrune.commands.retried` | Counter | Command retry attempts (optimistic-lock conflicts, lock acquisition failures). Tagged by `command.type` (simple class name) |
| `streamrune.commands.short.circuited` | Counter | Commands short-circuited by an interceptor (not executed). Tagged by `command.type` (simple class name) |
| `streamrune.commands.async.rejected` | Counter | `executeAsync` calls rejected past the in-flight admission budget (see [Async command admission control](#async-command-admission-control)) — the caller gets an already-failed future carrying `CommandBusOverloadedException`, no command was attempted. A rising rate means sustained overload; tune `streamrune.max-in-flight-async-commands` or add capacity |
| `streamrune.commands.in_flight` | Gauge | Commands currently admitted through `executeAsync` and not yet complete — the live numerator against the configured `streamrune.max-in-flight-async-commands` ceiling. First `VirtualThreadCommandBus` to register this gauge against a shared `MicrometerStreamRuneMetrics` wins; each of the three integrations wires one bus per context, so this is a note, not a caveat to plan around |
| `streamrune.dlq.published` | Counter | Commands published to the dead-letter queue. Tagged by `command.type` (simple class name) so it joins `streamrune.dlq.exhausted` per command type — `exhausted / published` for one type is that command's permanent-loss rate through the DLQ |
| `streamrune.dlq.exhausted` | Counter | Commands that exhausted every DLQ retry and were **permanently dropped** — the business operation is lost and its domain events were never produced. **Alert on this** (the command-path analog of `outbox.delivery_failed`). Tagged by `command.type` |
| `streamrune.dlq.pending` | Gauge | Current command dead-letter-queue depth (entries queued for retry/inspection), sampled by `DeadLetterRetryRunner` each poll cycle — a sustained rise means commands are failing faster than they are retried/resolved; alert alongside `dlq.exhausted` |
| `streamrune.dlq.swept_rows` | Counter | Command dead-letter-queue rows swept by the housekeeping job |
| `streamrune.dlq.retry.consecutive_failures` | Gauge | Consecutive poll-cycle failures of the `DeadLetterRetryRunner` — `0` when healthy, rising while the runner is in backoff (DLQ store unreachable); a nonzero value means the runner is DEGRADED but still running |
| `streamrune.outbox.relay.consecutive_failures` | Gauge | Consecutive poll-cycle failures of the outbox relay (`OutboxPoller`) — `0` when the last cycle succeeded, rising while the relay is in backoff (store/broker unreachable); combine with the relay-liveness health check to catch a wholly dead relay thread |
| `streamrune.locks.wait` | Timer | Time spent waiting to acquire an aggregate lock |
| `streamrune.events.appended` | Counter | Total domain events persisted |
| `streamrune.events.replayed` | Counter | Events folded into aggregate state during reconstruction (the events after the snapshot, or the whole stream when none exists). `events.replayed / commands.dispatched` is the average replay depth — a rising ratio means the snapshot interval is too coarse |
| `streamrune.events.duration` | Timer | Event append duration — the event-store round trip, sampled on both the success and the failure path (a keyed zero-event inbox claim appends nothing and is not sampled) |
| `streamrune.projections.processed` | Counter | Events processed by projections |
| `streamrune.projections.failed` | Counter | Projection processing failures |
| `streamrune.projections.dead_lettered` | Counter | Projection batches dead-lettered under the DLQ error strategy — the read model advances **past** the failed range, so **alert on this** (the one projection path that skips data) |
| `streamrune.projections.dead_letter_backlog` | Gauge | Current projection dead-letter backlog — total un-replayed dead-letter entries across all projections, sampled once per cycle by the projection runners. Unlike `projections.dead_lettered` (a counter fired once at dead-letter time), this is the standing depth: the runner advances the checkpoint **past** every dead-lettered range (permanent read-model holes) and stays health-UP, so once a failure burst ends nothing else signals that N ranges are un-replayed. **Alert on any sustained nonzero value** — every unit is a permanent hole awaiting an operator replay. Fleet-wide (untagged) |
| `streamrune.projections.duration` | Timer | Projection processing duration |
| `streamrune.projections.delivery_mode` | Gauge (value 1) | One sample per started projection registration, INLINE included, tagged `projection.name` and `delivery.mode` — what each projection promises (see [Delivery modes](concepts.md#delivery-modes--what-a-projection-promises)). Alert when a projection you consider authoritative reports a mode other than `TRANSACTIONAL_LOCAL` |
| `streamrune.subscriptions.events.received` | Counter | Events received by subscriptions |
| `streamrune.subscriptions.delivery.latency` | Timer | Subscription delivery latency |
| `streamrune.subscriptions.lag` | Gauge | Events between the global head and the last committed offset |
| `streamrune.subscriptions.listener.reconnects` | Counter | LISTEN/NOTIFY listener reconnect attempts — a **rising** count signals a *flapping* push path. A LISTEN connection whose server vanished without closing it is detected by the listener's liveness probe and counts here too. It cannot tell you the push path is **dead**: a listener that failed at startup never increments it and one that exhausted a finite reconnect cap stops incrementing it, so both read exactly what a perfectly healthy listener reads. For a dead push path use the subscription health signal below, not this counter |
| `streamrune.snapshots.created` | Counter | Snapshots created |
| `streamrune.snapshots.loaded` | Counter | Snapshots loaded during aggregate reconstruction |
| `streamrune.snapshots.discarded` | Counter | Stored snapshots discarded on load — the aggregate is rebuilt by replaying all events instead of wedging. Tagged by `reason`: `deserialize_failure` (the payload could not be deserialized — a plain Jackson/IO failure, e.g. an aggregate-state field renamed/removed without a snapshot-version bump) or `migration_failure` (a registered `SnapshotMigration` step threw). Expected near-zero; a nonzero rate flags a snapshot-schema evolution mistake |
| `streamrune.crypto.subject_redacted` | Counter | A decrypt fell back to the `[REDACTED]` tombstone because the subject's key was absent. Expected at a low, steady rate for lawfully crypto-shredded (GDPR-erased) subjects; a **spike** signals a whole key store is unavailable/empty (restored DB without key rows, truncated key table, wrong datasource) — every `@Encrypted` field would then be silently redacted. Alert on the rate |
| `streamrune.crypto.keystore_systemic_failure` | Counter | A **systemic** key-store failure was detected: many distinct subjects redacted with no successful decrypt in between — the signature of a wiped/misconfigured key store, NOT lawful per-subject erasure. Emitted once per detected outage episode alongside an `ERROR` log — **page on any nonzero value** |
| `streamrune.gdpr.purge_failed` | Counter | A GDPR right-to-erasure read-model purge FAILED after the subject's key was already crypto-shredded — unencrypted derived PII may remain in that read model (an Article-17 gap that must not be reported as fully erased). Terminal and alert-worthy — **page on any nonzero value**. Tagged by `purger` |
| `streamrune.queries.dispatched` | Counter | Queries dispatched on the query bus |
| `streamrune.queries.duration` | Timer | Query handling duration |
| `streamrune.queries.cache.hits` | Counter | Query results served from the query cache |
| `streamrune.queries.cache.misses` | Counter | Query cache misses (result computed by the handler) |
| `streamrune.saga.quarantined` | Counter | Events quarantined into the saga dead-letter store — every entry the saga runner writes, whatever the cause: a poison event, or one of the holds counted by `saga.event_held` and `saga.skipped_while_faulted`. Tagged by `saga.type` |
| `streamrune.saga.faulted` | Counter | Sagas that transitioned into `FAULTED`, from any path (step execution, the saga runner's poison handler, the compensation-retry sweeper giving up, the timeout runner) — **alert on this** to detect faulted sagas (see [Operations runbook](#operations-runbook-faulted-sagas) below). Tagged by `saga.type` |
| `streamrune.saga.skipped_while_faulted` | Counter | Correlated events delivered to an already-`FAULTED` saga and therefore **recorded instead of processed**. A strict subset of `saga.quarantined`, as is `saga.event_held`, so `quarantined − skipped_while_faulted − event_held` is the genuine-poison count (a NEW defect — except the event path's refusals to resume a compensation episode older than the command-inbox retention window, which go through the same poison handler and are recognizable by the entry's error type `org.streamrune.runtime.SagaStaleCompensationEpisodeException`) while this series is the **blast radius** of a fault you already know about — a rising rate means business events are piling up behind a halted saga, i.e. how urgent the pending `SagaDeadLetterReplayer.replayAll(sagaId)` is. Replayed events are not counted. Tagged by `saga.type` |
| `streamrune.saga.event_held` | Counter | Correlated events **held** — recorded in the saga dead-letter store instead of processed — because their known saga cannot consume them yet. Tagged by `saga.type` and `hold.reason`: `GENESIS_PENDING` (the saga's start step has not committed), `BACKLOG_PENDING` (the saga already owns dead-letter entries, and later events wait behind them), `BEFORE_START` (the saga has no row but already owns a dead-letter entry). A strict subset of `saga.quarantined`; the `FAULTED` hold keeps its own series, `saga.skipped_while_faulted`. Every held event waits for a `SagaDeadLetterReplayer.replayAll(sagaId)` |
| `streamrune.saga.replay_deferred` | Counter | Dead-letter entries a `replayAll` drain deferred behind a saga genesis that is not applied yet, without feeding them (outcome `SAGA_ROW_PENDING`). Never a `saga.replayed` sample. Tagged by `saga.type` |
| `streamrune.saga.compensation` | Counter | Saga compensation attempts, tagged by `saga.type` and `outcome`: `COMPENSATED` and `FAILED` (terminal), `RETRY` (a transient failure — the saga stays `COMPENSATING` for a resume) and `RETRY_REFUSED` (every failed compensation command was refused admission — open circuit breaker, closing bus, interceptor veto — so nothing was attempted; the saga also stays `COMPENSATING`). A dashboard that counts only `outcome="RETRY"` misses the refused attempts |
| `streamrune.saga.compensation_retries` | Counter | Compensation episodes re-driven out of band by `SagaCompensationRetrySweeper` — a no-timeout saga whose compensation failed transiently. A sustained nonzero rate means compensations are not completing on the first attempt; alert alongside `saga.faulted`. Counts only re-drives that actually dispatched: one refused admission wholesale (open circuit breaker, closing bus, interceptor veto) is not counted — it shows as `saga.compensation{outcome="RETRY_REFUSED"}` and an INFO log line. Tagged by `saga.type` |
| `streamrune.saga.cas_conflicts` | Counter | Optimistic-concurrency (CAS) conflicts while persisting saga state, tagged by `saga.type` |
| `streamrune.saga.compensating` | Gauge | Current `COMPENSATING` backlog (refunds/stock releases not yet completed), sampled by `SagaCompensationRetrySweeper` each cycle — a sustained rise means compensations are not completing (a stalled/dead saga driver or a stuck downstream). Sampled whenever sagas are enabled and a `SagaRunner` bean exists: `streamrune.saga.compensation-retry-enabled=false` only switches the sweeper to sampling-only (the re-drive stops, the sample does not). Tagged by `saga.type` |
| `streamrune.saga.timed_out` | Gauge | Sagas picked up by `SagaTimeoutRunner` in its most recent poll — forward timeouts plus `COMPENSATING` re-picks, bounded by its batch size, each population getting at least half the batch when both are backlogged — a sustained nonzero value means they are not being drained: a stalled runner, timeouts arriving faster than compensation completes, or re-picked episodes whose compensation keeps failing (compare `streamrune.saga.compensating`). Tagged by `saga.type` |
| `streamrune.saga.faulted_backlog` | Gauge | **Stranded recovery handles**: the saga dead-letter entries the retention guard currently protects — entries whose owning saga row is `FAULTED` or carries the dead-letter shield (`dead_letter_pending`), named entries whose saga has no row at all, and every null-saga entry the replayer has resolved to a target (`target_saga_id`). Sampled by `SagaDeadLetterRetentionSweeper` each cycle. This is the retention guard's exact complement **minus the age filter** — the question is "how many recovery handles are stranded right now?", not "how old are they?", so an entry too young to be a prune candidate is counted. Because the guard protects that population rather than pruning it, it grows silently — `saga.faulted` fires once at fault time and is not a standing depth. **Alert on any sustained nonzero value**: every unit is a halted or held business process waiting on an operator drain or discard. Fleet-wide (untagged); not sampled while dead-letter retention is disabled, and disabled for a `SagaDeadLetterStore` that does not implement `countFaultedBacklog` |
| `streamrune.saga.faulted_rows` | Gauge | Current count of saga rows sitting in `FAULTED`, per saga type, sampled by `SagaCompensationRetrySweeper` each cycle (also with `compensation-retry-enabled=false` — sampling-only mode). The **full** halted population — whereas `saga.faulted_backlog` counts dead-letter entries, the framework's automatic quarantine paths fault a saga with **no dead-letter entry at all** (stuck-compensation give-up, stale/past-dwell timeout, a `compensate()` that threw on a sweeper/timeout resume), and those show on no other standing series. **Alert on any sustained nonzero value**; read together with `saga.faulted_backlog` (the entries a drain can work off). Tagged by `saga.type` |
| `streamrune.saga.replayed` | Counter | `SagaDeadLetterReplayer` steps, tagged by `saga.type` and `outcome` (`REPLAYED`/`STILL_POISON`/`ENTRY_NOT_FOUND`/`EVENT_NOT_FOUND`/`STALE_REDRIVE_BLOCKED`/`STALE_COMPENSATION_BLOCKED`/`TARGET_PENDING`/`OLDER_ENTRY_PENDING` — see the [outcome table](#replay)). A deferral (`SAGA_ROW_PENDING`) is never a sample here; it is counted by `saga.replay_deferred` |
| `streamrune.saga.forced_stale_resume` | Counter | Stale compensation episodes an operator **forced** to resume: a `SagaDeadLetterReplayer.replay(..., true)` / `replayAll(sagaId, true)` feed, or a `resumeFaulted(sagaId, true)`, reached the step executor over a compensation episode older than `streamrune.inbox.retention-max-age`, and the executor honoured the operator's at-least-once acknowledgement instead of refusing the resume. Every sample is an episode whose already-succeeded compensations may have re-executed (their dedup keys were pruned) — each should match a reconciliation someone performed. Recorded on the `SagaRunner`'s metrics; a forced feed over a fresh episode is not counted. Tagged by `saga.type` |
| `streamrune.saga.resume_faulted` | Counter | `SagaDeadLetterReplayer.resumeFaulted` calls — the operator resume of a give-up-faulted compensation episode — tagged by `saga.type` and `outcome` (`RESUMED`/`STILL_POISON`/`NO_PROGRESS`/`STALE_COMPENSATION_BLOCKED`/`SAGA_NOT_FOUND`/`NOT_FAULTED`/`FORWARD_FAULT`/`ENTRIES_PENDING` — see [Resume a give-up fault](#resume-a-give-up-fault)). Refusals are samples too |
| `streamrune.saga.compensate_faulted` | Counter | `SagaDeadLetterReplayer.compensateFaulted` calls — the operator compensation of a forward-faulted saga with no dead-letter entry left — tagged by `saga.type` and `outcome` (`TERMINAL`/`COMPENSATING`/`POISON`/`CLAIM_LOST`/`SAGA_NOT_FOUND`/`NOT_FAULTED`/`COMPENSATION_FAULT`/`ENTRIES_PENDING` — see [Compensate a stranded forward fault](#compensate-a-stranded-forward-fault)). Refusals are samples too; every other sample is an undo an operator decided on |
| `streamrune.saga.dead_letters_swept` | Counter | Saga dead-letter rows swept by `SagaDeadLetterRetentionSweeper` |
| `streamrune.inbox.replay_hits` | Counter | Commands recognized as replays via the command inbox and not re-executed |
| `streamrune.inbox.swept_rows` | Counter | Command-inbox rows swept by `InboxRetentionSweeper` |
| `streamrune.outbox.swept_rows` | Counter | Outbox rows swept by `OutboxRetentionSweeper` |
| `streamrune.outbox.pending` | Gauge | Current `PENDING` outbox backlog (events not yet delivered to the broker), sampled by `OutboxPoller` each poll cycle — a rising value flags a lagging or stalled relay *before* any entry exhausts its retry ladder, complementing `delivery_failed` |
| `streamrune.outbox.in_flight` | Gauge | Current `IN_PROGRESS` outbox count, sampled by `OutboxPoller` each poll cycle — entries claimed for delivery whose transport outcome is unknown (e.g. a load balancer accepting and resetting the connection after the receiver processed the request but before the response returned). During a sustained post-hand-off transport outage this is the **only** series that moves: `outbox.pending` reads ~0 (entries are `IN_PROGRESS`, not `PENDING`) and `delivery_failed` never fires (an in-flight outcome burns no retry attempt). **Alert on it staying high across several poll intervals** — see [outbox.md's "Alert on `streamrune.outbox.in_flight`"](advanced/outbox.md#alert-on-streamruneoutboxin_flight-not-only-on-streamruneoutboxpending) |
| `streamrune.outbox.in_flight_horizon_violation` | Counter | An `OutboxPublisher` contract violation caught at delivery time: `classifyFailure` returned `IN_FLIGHT` while the publisher's `inFlightHorizon()` still reports the `ZERO` default, so the claim lease was never validated against the transport's real in-flight window. The poller fails safe (it holds the claim for the full lease), but any nonzero rate means that custom publisher must override `inFlightHorizon()` with an honest bound. Incremented once per held entry |
| `streamrune.outbox.delivery_failed` | Counter | Outbox entries that reached terminal `FAILED` after exhausting retries — alert on this (see the [outbox-FAILED runbook](#operations-runbook-outbox-failed-entries) below) |
| `streamrune.outbox.blocked_aggregates` | Gauge | Distinct non-null streams with an unresolved `FAILED` entry — blocked on a strict channel, gapped on an availability-first one; ticket when > 0 for longer than one poll interval. Sampled by `OutboxPoller` each poll cycle |
| `streamrune.outbox.blockage_age_seconds` | Gauge | Age of the oldest unresolved `FAILED` entry of any aggregate, `0` when none; **the primary alert**, e.g. > 900 s; rises until that entry is replayed or skipped (see the [outbox-FAILED runbook](#operations-runbook-outbox-failed-entries) below). Sampled by `OutboxPoller` each poll cycle |
| `streamrune.outbox.skipped` | Counter | Operator skips, one per row; a rising rate means messages are dropped routinely — review the mapper or the consumer |
| `streamrune.outbox.skipped_swept` | Counter | `SKIPPED` rows pruned by `OutboxRetentionSweeper`; `FAILED` rows are never swept |
| `streamrune.outbox.replayed` | Counter | `FAILED` outbox entries reset to `PENDING` by `OutboxFailedReplayer` |

Key tags: `command.type`, `event.type`, `stream.id` (log/exemplar correlation only — never a metric
tag, unbounded cardinality), `projection.name`, `subscription.name`, `query.type`, `saga.type`
(bounded: the saga type — the saga state's fully-qualified class name, the value `saga_state.saga_type`
holds, so two saga-state classes sharing a simple name never share a series), `outcome` (bounded:
enum name).

**One metric name, one label set.** Every series StreamRune emits under a given
metric name carries the same label keys — Prometheus maps a metric name onto one family and rejects
mixing shapes within it, and a mismatched series is silently dropped from the scrape rather than
reported. Where a call site has no value for a family's dimension it reports the literal `unknown`
rather than omitting the label. In the default wiring you will therefore see
`streamrune_commands_dispatched_total{command_type="unknown"}` and
`streamrune_events_appended_total{event_type="unknown"}`: the command bus counts dispatches and
appends fleet-wide and does not break them down by type. That is the metric behaving as designed,
not a misconfiguration. Aggregates (`sum(rate(streamrune_commands_dispatched_total[5m]))`) are
unaffected, and if a future release starts reporting the real type, traffic simply moves out of the
`unknown` bucket — the metric's shape, and the dashboards written against it, stay valid.

The command-type family follows the same rule the other way round: `commands.retried`,
`commands.short.circuited` and `dlq.published` are registered at startup as a
`command_type="unknown"` zero series and then carry the real `command.type` on every sample, and
`dlq.exhausted` registers each type on first use. Write queries over them as
`sum(rate(streamrune_commands_retried_total[5m]))` for the total or
`sum by (command_type) (rate(streamrune_commands_retried_total[5m]))` per type — a bare `rate(...)`
returns one series per command type.

### Tracing

Every span name and attribute key below is a constant in `org.streamrune.runtime.TraceAttributes`,
and the emitters' tests assert the emitted name and the **exact** attribute-key set against those
constants — the span-side equivalent of the `MetricNames` guard, so this table cannot drift from
what is actually exported. All StreamRune span attributes carry the `streamrune.` prefix. Span
names are path-shaped (`command/PlaceOrder`, not `command.PlaceOrder`).

| Emitter | Span name | Kind | Attributes |
| --- | --- | --- | --- |
| `OpenTelemetryCommandInterceptor` | `command/{CommandType}` | `INTERNAL` | `streamrune.command.type`, `streamrune.command.id`, `streamrune.aggregate.type`, `streamrune.aggregate.id`, `streamrune.stream.id` |
| `TracingEventListener` | `subscription/{name}/deliver` | `CONSUMER` | `streamrune.subscription.name`, `streamrune.batch.size` |
| `TracingProjectionDecorator` | `projection/{name}/process` | `INTERNAL` | `streamrune.projection.name`, `streamrune.batch.size` |

- `streamrune.stream.id` (`<type>:<id>`, for example `order:ord-42`) is set when the span starts,
  so a command that fails before committing carries its stream too.
- `streamrune.aggregate.type` and `streamrune.aggregate.id` are the two parts of that stream; both
  are absent for a command with no aggregate id.
- `TracingEventListener` links its span back to the originating command trace when the first
  event's metadata carries a `traceId`/`spanId`.
- The command interceptor is auto-registered when an `OpenTelemetry` bean is present; the
  subscription and projection decorators are opt-in wrappers.

```properties
otel.exporter.otlp.endpoint=http://collector:4317
otel.service.name=my-streamrune-app
```

## Snapshot strategy recommendations

Start with `SnapshotPolicy.never()` and profile aggregate load times before enabling snapshots.

Interval guidance:

- High-write aggregates (shopping carts, counters): every 50–100 events.
- Medium-write aggregates (orders, accounts): every 100–500 events.
- Low-write aggregates (user profiles, product catalog): every 1000+ events or `never()`.

Always set `snapshotVersion = 1` initially. Increment only when your state class changes.

> **Both of those are reached through a `SnapshotPolicy` bean, not a property.** `streamrune.snapshot-every-n-events` only feeds the framework's *default* policy — `everyNEvents(n)` at snapshot schema version 1 — and it is validated `> 0`, so it can neither bump the schema version nor turn snapshots off. Define a `SnapshotPolicy` bean (Spring `@Bean`, Quarkus `@Produces`, Micronaut `@Singleton` on a `@Factory`) and it replaces the default wholesale; see [snapshot versioning](advanced/snapshot-versioning.md#configuration). Until that hook existed the version was pinned at 1 on all three integrations, which also made every registered `SnapshotMigration` unreachable.

Monitor `streamrune.snapshots.created` and `streamrune.snapshots.loaded` metrics. `created`
rises each time the policy fires and the store accepts the write; `loaded` rises each time an
aggregate is rehydrated from a stored snapshot. A `created` count that climbs while `loaded`
stays flat means the expected `snapshotVersion` and the stored one disagree, so every load
discards the snapshot and replays the full stream — watch `streamrune.events.replayed` for the
confirming rise.

## Subscription resilience configuration

A transient `RuntimeException` in the polling loop no longer terminates the subscription. The poll loop runs inside `ResilientPollLoop`, which logs the exception and retries with capped exponential backoff plus jitter; the loop exits only when its `keepRunning` condition becomes false or the thread is interrupted. There is no `STOPPED` state to watch for on a transient error, so a restart watchdog is no longer needed.

Instead, treat a subscription that keeps failing as **degraded but still running**, and watch the **exposed** signals rather than the internal poll-loop counter (`ResilientPollLoop` and its `consecutiveFailures()` are package-private — not an operator API):

- **Health status.** Wire the subscription with `SubscriptionHealthContributor` (and wrap the listener in `HealthTrackingEventListener`); the health check (`GET /actuator/health`, `/q/health/ready`, or `/health` — see [Health checks](#health-checks) above) then carries a `subscription.<name>` detail whose status is `DEGRADED` while the subscription is running but paused, failing (any run of consecutive errors) or lagging (lag at or above `streamrune.subscription.health.lag-threshold`), and `DOWN` only when it is stopped or terminally halted — a `DOWN` subscription also turns the whole check `DOWN`. Alert on the check going `DOWN` and on a sustained `DEGRADED` detail.
- **Metrics.** Alert on rising subscription lag (`streamrune.subscriptions.lag`) to catch a subscription that is retrying but not making progress — for example a poisoned event, an unavailable database, or a broken projection handler — and on `streamrune.subscriptions.listener.reconnects` to catch a flapping LISTEN/NOTIFY push path. A failed lag read no longer resets the gauge to `0` (the caught-up value): it holds its last known value and logs a `WARN`, so the alert stays firable during exactly the store trouble it exists to catch.
- **A dead push path reports `DEGRADED`, not `UP`.** A `HybridEventSubscription` whose LISTEN/NOTIFY listener is not delivering — it failed at startup (its LISTEN data source could not be created), exhausted a finite reconnect cap, or has been without a LISTEN connection for longer than its 30-second reconnect grace (`max_connections` reached, credentials scoped to the main pool only, TLS handshake failure, a failed-over primary that is not reachable yet) — keeps delivering, because polling guarantees delivery. But it delivers at the *poll interval* instead of in milliseconds until the push path is back. That degradation is folded into subscription health as `DEGRADED` (never `DOWN` — data still flows) with a one-shot `WARN` naming the subscription, and it appears in the `subscription.<name>` health detail; it returns to `UP` once the listener has a LISTEN connection again. A routine reconnect that succeeds within the grace, and the first connect after start, do not degrade it. `streamrune.subscriptions.listener.reconnects` **cannot** tell you this (see the metrics table); the health status can. A `pollingOnly` subscription has no push path and is never degraded for it.

> **Single-active-consumer, now enforced.** A given subscription/projection name must still be *processed* by exactly one instance at a time, but this is no longer just a documented requirement — it is enforced automatically. See [Multi-replica deployment](#multi-replica-deployment) below.

Polling interval: `pollingIntervalMs` default 5000ms. For near-real-time delivery, use `HybridEventSubscription` rather than reducing below 1000ms.

Jitter: keep at 20–30% of polling interval.

`fetchSize`: increase to 500–1000 for high-throughput catch-up, reduce to 100 for steady-state.

Use `SubscriptionConfig.DEFAULT` for production (LISTEN/NOTIFY enabled). Use `pollingOnly()` only if PostgreSQL `LISTEN/NOTIFY` is not available (e.g., PgBouncer in transaction pooling mode). Note that `pollingOnly()` addresses only the push path — single-active-consumer leadership and its epoch fencing are already safe under any pooling mode, including transaction mode, so there is nothing else to configure for leadership. Prepared statements, session-level settings and schema migration behind a pooler are separate settings, covered in the [PgBouncer note](#postgresql-setup) above; see also [Single-active-consumer leadership](#single-active-consumer-leadership).

> **The LISTEN/NOTIFY listener survives failover.** `PgNotificationListener` reconnects with capped
> exponential backoff and, by default, **never permanently gives up** — a PostgreSQL failover of any
> length does not strand the push notifications. Each reconnect attempt logs at WARN and increments
> the `streamrune.subscriptions.listener.reconnects` metric, so a **rising** count is a flapping push
> path and a **flat, stuck** count on a listener that stopped is the signal that the push path is
> dead. Delivery never depends on it: polling remains the delivery-liveness guarantee, so LISTEN/NOTIFY
> only lowers latency. `HybridEventSubscription.pushHealthy()` exposes push liveness independently of
> `isRunning()` (which stays polling-based) — a `false` there means notifications are no longer
> lowering latency and warrants attention, but the subscription is still delivering via polling. A
> finite `HybridEventSubscription.Builder`/`PgNotificationListener` reconnect cap is opt-in, for
> tests or a deliberate fail-fast; leave it unbounded in production.
>
> **A failover that leaves the old connection open is detected too.** When the database host dies
> without closing its connections — a crashed node, an instance replaced behind a virtual IP or a
> DNS name — the LISTEN connection stays open on the client and simply never receives anything
> again; no error ever arrives. The listener therefore runs `SELECT 1` on its LISTEN connection
> after 10 seconds without a notification and waits at most 5 seconds for the answer. A connection
> that cannot answer is replaced through the reconnect path above, so a silent loss is noticed
> within about 15 seconds; a busy channel proves its connection alive with every notification and
> is never probed. `PostgresNotificationSubscription` probes its
> LISTEN connection the same way. The dedicated LISTEN pool that `createListenDataSource` builds
> from a HikariCP pool inherits that pool's JDBC URL (or data source class), credentials and
> `dataSourceProperties` (TLS settings, application name, timeouts), and turns on the driver's
> `tcpKeepAlive` unless the main pool sets it — nothing to configure for the LISTEN connection
> separately. From any other DataSource it reads the URL and credentials through `getJdbcUrl()`,
> `getUrl()` or `getURL()` (and `getUsername()`/`getUser()`, `getPassword()`). Agroal on Quarkus and
> DataSource proxies expose none of these, so `createListenDataSource` refuses them with an
> `IllegalArgumentException`: build a dedicated single-connection pool from the same settings
> (on Quarkus, `quarkus.datasource.*`) and pass it to the subscription builder's
> `listenDataSource(DataSource)`, which accepts any DataSource and closes it with the
> subscription — the same remedy as the crypto forget signal's two-argument constructor.
> `HybridEventSubscription` needs none of this: on a pool that is not HikariCP it borrows its LISTEN
> connection from the pool itself (see the pool budget above).

## Duration properties: what `0` and a negative value mean

Every `Duration` knob accepts ISO-8601 (`PT30S`, `PT720H` — the Quarkus form) or the compact form
(`30s`, `30d` — the Spring/Micronaut form). What a **zero** or a **negative** value does is *not*
uniform across the knobs, because the knobs do not all measure the same kind of thing. The rule
StreamRune froze for 1.0.0 is:

- **A window or interval that can be switched off** treats zero *and* negative as "off". Retention
  windows and the SSE reapers are in this class.
- **A wait that can legitimately be "do not wait"** treats zero as "try once, immediately" and a
  **negative value as a configuration error**. `streamrune.lock-timeout` is in this class.
- **A period that has no meaningful "off"** rejects zero and negative outright. Lease TTLs and sweep
  intervals are in this class — a zero-length lease or a zero-period sweep is not a weaker setting,
  it is an unrunnable one.

A negative duration is never a valid StreamRune value anywhere: it either means "disabled" (where
zero already says that more clearly) or it is rejected. Nothing interprets it as a magnitude.

**The millisecond floor.** For the knobs whose zero is a *sentinel* — `streamrune.lock-timeout`
("do not wait"), the single-active-consumer lease TTL, `streamrune.event-store.statement-timeout`
("no bound"; programmatically `PostgresEventStoreFactory.statementTimeout`), and the programmatic
`MultiProjectionRunner.Builder.stopTimeout` ("wait forever", via `Thread.join(0)`) — a **positive
value shorter than one millisecond is rejected on the same terms as a negative one**. Every wait
bound in these paths is a whole number of milliseconds (PostgreSQL's `lock_timeout` and
`statement_timeout` take an integer count of them; `Lock#tryLock` and `Thread#join` are called with
them), so a shorter value truncates to `0` and is then read as the *sentinel* rather than as "very
short": `SET lock_timeout = '0ms'` means **no timeout at all** to PostgreSQL, so a configured
`500us` bound became an unbounded wait, while the in-process locker read the same value as "try
once", and `Thread.join(0)` on shutdown means **wait for the projection thread forever** — the
tightest configured stop bound became an unbounded shutdown hang. StreamRune rejects rather than
rounding up, for the same reason it does not clamp a negative value: substituting a number the
operator did not write hides the mistake instead of reporting it. Spring accepts `500us`/`500ns`;
Quarkus and Micronaut accept `PT0.0005S` — all of the property-bound knobs now fail the boot with a
message naming the floor, and the two programmatic knobs (`PostgresEventStoreFactory.statementTimeout`
and `MultiProjectionRunner.Builder.stopTimeout`) throw `IllegalArgumentException` at construction /
`build()` for the same reason.

| Property | Default | `0` | Negative |
|---|---|---|---|
| `streamrune.lock-timeout` | `5s` | **Do not wait**: a single non-blocking attempt; a lock held elsewhere fails the command immediately (retryable `LockException`) | **Rejected at boot** on all three integrations (`IllegalStateException` from `StreamRuneConfigValidator`), and by both `AggregateLocker` implementations (`IllegalArgumentException`) if wired programmatically. A **positive value below `1ms`** is rejected the same way — see the millisecond floor above |
| `streamrune.event-store.statement-timeout` | `30s` | **No bound of the store's own**: event-store statements run as long as the session's `statement_timeout` (your role, pool or server setting) allows | **Rejected at boot** on all three integrations (`IllegalStateException` naming the property). A **positive value below `1ms`** is rejected the same way — see the millisecond floor above — and so is a value above `2147483647ms` (about 24.8 days), the largest `statement_timeout` PostgreSQL accepts |
| `streamrune.circuit-breaker-cooldown` | `30s` | Valid: an OPEN circuit probes on the very next command | Rejected — `CircuitBreakerCommandInterceptor` throws `IllegalArgumentException` at construction, so the app fails to start |
| `streamrune.subscription.single-active-consumer.lease-ttl` | `15s` | Rejected — `LeaseBasedLeadership` requires a TTL of at least `1ms`, so the app fails to start (whenever leadership is actually wired: `single-active-consumer.enabled=true`, the default, with a `DataSource` present). A **positive value below `1ms`** is rejected the same way: it truncated to a lease that was already expired when written, so every replica took over on every poll | Rejected, same path |
| `streamrune.saga.compensation-retry-interval` | `60s` | Rejected — `SagaCompensationRetrySweeper.build()` requires a positive interval (a zero period is a tight spin loop). Reached whenever sagas are enabled and a `SagaRunner` bean exists — the sweeper is built even with `streamrune.saga.compensation-retry-enabled=false` (sampling-only mode, so the saga backlog gauges stay live), and the value is validated either way. The one exception: with the knob off **and** no `StreamRuneMetrics` wired, no sweeper is built at all, so the value is never read | Rejected, same path |
| `streamrune.saga.compensation-retry-give-up-after` | `1h` | Rejected — same builder, same reason, same gate (built even in sampling-only mode, with the same no-metrics exception). Note it additionally must stay strictly **below** `streamrune.inbox.retention-max-age` (a `WARNING`, not a boot failure, emitted only while compensation retry is enabled) | Rejected, same path |
| `streamrune.sse.timeout` | `5m` | Disables the emitter timeout, leaving the keepalive as the sole dead-client reaper | Same as `0` |
| `streamrune.sse.keep-alive-interval` | `30s` | Disables the keepalive, leaving the timeout as the sole reaper | Same as `0` |
| `streamrune.inbox.retention-max-age` | `7d` | Disables inbox pruning — keys are never swept, so they can never go stale; this also normalizes every consumer of the knob (compensation sweeper, `SagaTimeoutRunner`, `SagaDeadLetterReplayer`, the boot validator) to guard-inert | Same as `0` |
| `streamrune.dead-letter.retention-max-age` | `30d` | Disables command-DLQ pruning | Same as `0` |
| `streamrune.outbox.retention-max-age` | `7d` | Disables DELIVERED outbox pruning | Same as `0` |
| `streamrune.outbox.skipped-retention-max-age` | `30d` | Disables SKIPPED outbox pruning | Same as `0` |
| `streamrune.saga.dead-letter-retention-max-age` | `7d` | **Carve-out — see below** | Same as `0` |

> **The retention carve-out: `streamrune.saga.dead-letter-retention-max-age`.** Setting it to zero or
> negative does disable pruning, exactly like its sibling retention knobs — but "never pruned" means
> **unbounded** retention, and this is the one retention window that carries an upper-bound invariant
> as well as a lower one: it **must not exceed** `streamrune.inbox.retention-max-age`, because a
> dead-letter that faulted mid-compensation stays replayable for its whole window and its replay
> re-derives the original command-inbox dedup keys. Outliving those keys turns a replay into a
> re-execution of an already-succeeded compensation (double refund). So with sagas
> enabled and a `SagaDeadLetterStore` bean present (the default), zero/negative violates the
> invariant and `SagaRetentionValidator` throws `IllegalStateException` at startup: **every replica
> fails to boot** rather than merely skipping a sweep. This asymmetry with the other retention knobs
> is deliberate and was kept at the 1.0.0 freeze — relaxing it would trade a loud boot failure for a
> silent correctness hazard. To retain saga dead-letters longer, raise **both**
> `streamrune.saga.dead-letter-retention-max-age` and `streamrune.inbox.retention-max-age` together,
> or set the inbox knob to zero/negative first (the validator reads that as "the inbox never prunes,
> so nothing can outlive it").

## Operations runbook: faulted sagas

A saga is marked `FAULTED` when an orchestrator pure-logic method (`isStartEvent`, `correlate`, `extractSagaId`, `evolve`, `handle`, `compensate`) throws — see [Failure model](advanced/saga.md#failure-model-poison-events-and-faulted). This is a deterministic bug, not a transient failure: it does not resolve itself on retry, so it needs an operator to detect, inspect, fix, and replay (or discard). The same drain also releases correlated events the runner **held** for a saga that could not consume them yet (see `streamrune.saga.event_held`).

### Detect

- **Alert on the `streamrune.saga.faulted` metric.** Every fault increments this counter, tagged with `saga.type`.
- **Alert on the `streamrune.saga.faulted_backlog` gauge** for the *standing* population, which the counter cannot show. It counts the dead-letter entries the retention guard is protecting right now — entries whose saga row is `FAULTED` or shielded, named entries whose saga has no row, and null-saga entries resolved to a target — i.e. the recovery handles waiting for a drain or a discard. The counter fires once, at fault time; if the alert on it is missed (or the fix takes longer than the alert's retention), the gauge is what still shows the work outstanding. It falls back to zero on its own as each entry is replayed or discarded. Sampled once per `SagaDeadLetterRetentionSweeper` cycle, so it needs dead-letter retention enabled.
- **Watch `streamrune.saga.event_held`** (by `hold.reason`) and `streamrune.saga.skipped_while_faulted`: correlated events recorded instead of processed because their saga was genesis-pending, already had a backlog, had no row yet, or was `FAULTED`. Each affected saga needs a `replayAll` once its blocker is gone.
- **Enumerate faulted sagas** with `SagaStore.findByStatus(sagaType, SagaStatus.FAULTED, limit)`, ordered newest-first.
- **Timeout-path and give-up FAULTED sagas have NO quarantine entry.** If `compensate` itself throws during `SagaTimeoutRunner` processing, there is no triggering *event* to quarantine (only an internal timeout signal) — the saga is marked `FAULTED` directly, with nothing in `SagaDeadLetterStore`. The same holds for a `compensate` that throws on a sweeper or timeout-runner resume (a resume triggered by a live correlated event is quarantined like any other poison event) and for the automatic give-up and stale-key faults of the compensation sweeper and the timeout runner. `findByStatus` is the only way to discover these, and `streamrune.saga.faulted_rows` (per saga type) is the gauge that counts them; `SagaDeadLetterStore.findAll`/`findBySaga` will not show them. Such a row reads `status = 'FAULTED'`, `pre_fault_status = 'COMPENSATING'` with no dead-letter entry; [Resume a give-up fault](#resume-a-give-up-fault) recovers it.
- **A forward fault whose entry was discarded has no quarantine entry either.** It reads `status = 'FAULTED'` with `pre_fault_status` `NULL`, `STARTED` or `RUNNING` and nothing in `SagaDeadLetterStore`; `findByStatus` and `streamrune.saga.faulted_rows` still show it, and the discard that left it logged a `WARNING`. [Compensate a stranded forward fault](#compensate-a-stranded-forward-fault) recovers it.

### Inspect

- For event-path faults, look up the quarantine entry: `SagaDeadLetterStore.findBySaga(sagaId)` (or `findAll(limit)` across all sagas). Each entry carries metadata only — saga id/type, event offset/type, exception class name and message, fault timestamps (`faultedAt`, the immutable `firstFaultedAt`), the immutable first-replay anchor, and for a null-saga entry the resolved `targetSagaId` — never the event payload.
- **The payload is deliberately not duplicated.** To inspect the actual event data, read the event store at the entry's `eventOffset` — `event_stream` remains the single crypto-governed, shreddable source of truth. Post-crypto-shred, that read correctly comes back with the `@Encrypted` fields redacted. `EVENT_NOT_FOUND` from the replayer means something else: the event store could not read the offset at all.
- **The saga row records its recovery facts** — the replayer and the runner read them rather than inferring anything from the row's shape, and so can you:

  | `saga_state` column | Meaning |
  |---|---|
  | `genesis_applied` | `TRUE` once the start event's forward step committed; `FALSE` from the saga's first write until then (genesis-pending) |
  | `pre_fault_status` | The status a fault replaced — what a replay resumes from. `NULL` unless `status = 'FAULTED'`, and `NULL` on a genesis-pending fault |
  | `dead_letter_pending` | The shield: the saga owns at least one dead-letter entry (named, or as the resolved target of a null-saga entry). Live correlated events are held while it is `TRUE` (a `COMPENSATING` saga's live events resume its episode instead) |
  | `last_applied_offset` | Highest global offset ever applied; a live redelivery at or below it is skipped |
  | `last_replayed_offset` | The offset the last replay feed applied; a rerun of exactly that feed is skipped |

- For timeout-path and give-up faults (no quarantine entry), inspect application logs around the fault time and the saga's persisted state (`SagaStore.load`) to diagnose what `compensate` failed on, or why the episode never completed (the sweeper's give-up `WARN` names the horizon or the stale key age).

### Fix and deploy

Faults are deterministic orchestrator bugs — diagnose the root cause in `evolve`/`handle`/`compensate`/`correlate`/`extractSagaId`, fix it, and deploy before attempting replay (or, for a give-up fault, before resuming it). A give-up fault caused by a downstream outage rather than a bug needs no deploy — only the outage resolved. Replaying against the same buggy code just re-quarantines the same entry.

### Replay

Once a fix has shipped, use `SagaDeadLetterReplayer` to drain the saga's quarantined events. It is not auto-configured — build one per saga type, against the same `SagaRunner` the subscription drives:

```java
SagaDeadLetterReplayer replayer = SagaDeadLetterReplayer.builder()
    .sagaDeadLetterStore(sagaDeadLetterStore)
    .sagaStore(sagaStore)
    .eventStore(eventStore)
    .sagaRunner(runner)          // the same SagaRunner<S> wired to the subscription
    .inboxRetentionMaxAge(inboxRetention) // streamrune.inbox.retention-max-age; arms both STALE_* guards
    .metrics(metrics)            // optional; defaults to StreamRuneMetrics.NOOP
    .build();

// the normal path — drain every entry of the saga, oldest offset first:
List<SagaDeadLetterReplayer.ReplayOutcome> outcomes = replayer.replayAll(sagaId);
// one entry (refused while an older entry of the saga is pending):
SagaDeadLetterReplayer.ReplayOutcome outcome = replayer.replay(sagaId, eventOffset);
// abandon an entry instead of replaying it:
boolean removed = replayer.discard(sagaId, eventOffset);
```

`replay(sagaId, eventOffset, true)` and `replayAll(sagaId, true)` lift the two key-age refusals (`STALE_*` below) — nothing else. `force` is your per-saga acknowledgement of at-least-once, and it is passed to the step executor too, so the executor's own event-path key-age guard resumes a stale compensation episode instead of refusing it (a `WARNING` names the saga; `streamrune.saga.forced_stale_resume` counts it). Without `inboxRetentionMaxAge` the replayer cannot reason about key age and both refusals are inert; pass your deployment's raw `streamrune.inbox.retention-max-age` (zero/negative — inbox pruning disabled — also normalizes to inert).

| Outcome | Meaning | Entry and saga row afterwards |
|---|---|---|
| `REPLAYED` | The event was fed and consumed — or it was moot: the saga is already terminal, the event was already applied, or a null-saga entry no longer routes to any saga. | Entry discarded. |
| `STILL_POISON` | The feed re-recorded the fault (the same or another exception, including a router that still throws), or a resumed compensation made no durable progress so the row is still `FAULTED`. | Entry kept (refreshed when the fault was re-recorded); the saga the fault was recorded against is `FAULTED` — a null-saga entry whose router still throws names no saga, so no row is faulted. Fix and drain again. |
| `ENTRY_NOT_FOUND` | No entry of this replayer's saga type exists at `(sagaId, eventOffset)`. | N/A. |
| `EVENT_NOT_FOUND` | The event store can no longer read the entry's offset; nothing was fed. | Entry kept; row unchanged, and its shield keeps holding live correlated events. `discard(sagaId, eventOffset)` is the remedy — if it leaves a forward-faulted saga with no entry, see [Compensate a stranded forward fault](#compensate-a-stranded-forward-fault). |
| `STALE_COMPENSATION_BLOCKED` | The saga faulted mid-compensation and its episode claim (`episode_claimed_at`, which every write that enters `COMPENSATING` stamps — a row in an episode without it is refused outright with `SagaUnstampedCompensationEpisodeException`, a saga-store contract violation) is older than `inboxRetentionMaxAge`: a succeeded compensation's inbox dedup key may already be pruned, so a resume could re-execute it (double refund). | Entry kept; row unchanged. Reconcile which compensations ran, then force: the forced resume re-dispatches every compensation of the episode under its original key, and one whose key was already pruned **executes again** — the at-least-once `force` acknowledges. A crash mid-resume leaves the row `FAULTED` (a plain rerun is refused again); a forced rerun dedups the compensations the crashed attempt executed and runs only the rest. |
| `STALE_REDRIVE_BLOCKED` | The entry's first replay attempt (`first_replay_started_at` — stamped once, just before the entry's first feed, and never moved) is older than `inboxRetentionMaxAge`: that attempt's forward command keys may already be pruned, so a re-drive could re-execute a committed forward command (duplicate charge). A first-ever replay stamps the anchor and is never refused. | Entry kept; row unchanged. Verify which forward commands executed, then force: `replay(sagaId, eventOffset, true)` for that entry, or `replayAll(sagaId, true)` — which lifts both key-age refusals for **every** entry of the drain, so reconcile all of them first. |
| `TARGET_PENDING` | A null-saga entry now routes to a saga that is `FAULTED`, genesis-pending, or has an older entry pending. The resolved target is recorded on the entry (`target_saga_id`) and the target saga is shielded before any refusal, anchor or feed. | Entry kept — never pruned while resolved. `replayAll(targetSagaId)` folds it in at its causal position. |
| `SAGA_ROW_PENDING` | Deferred, not attempted: a correlated entry whose saga has no row, or whose genesis is not applied (and which is not mid-compensation). Counted by `streamrune.saga.replay_deferred`, never by `streamrune.saga.replayed`. | Entry kept. `replayAll` feeds deferred entries once a start entry lands in the same pass; entries still deferred afterwards mean the start event is still poison, was discarded, or its live delivery has not completed (a WARN says so) — fix or replay the start, or discard them. |
| `OLDER_ENTRY_PENDING` | Single `replay` refused: an older entry of the same saga (named, or null-saga resolved to it) is pending and must be applied first. `force` does not override ordering. | Entry kept. Use `replayAll(sagaId)`, or discard the older entry deliberately. |

**The drain.** `replayAll(sagaId)` takes the saga's named entries plus every null-saga entry resolved to it and feeds them oldest offset first; a null-saga entry at the same offset as a named one is discarded first (the named entry is the record). `SAGA_ROW_PENDING` entries are stepped over and retried after a start entry lands; **any other non-`REPLAYED` outcome stops the drain** — nothing is ever fed ahead of a blocker, and the WARN names the blocking offset and how many entries stay quarantined behind it. A drain adds no write of its own when it stops: `EVENT_NOT_FOUND` and the `STALE_*` refusals leave the row unchanged, and a `STILL_POISON` row is `FAULTED` by its own fresh fault. The shield is cleared only by the dead-letter store's own conditional clear, and only when no named or resolved entry of the saga is left — after a drain, a single replay, or a `discard`.

**The replayer never un-faults.** A `FAULTED` row is cleared only by the saga executor's own successful write as part of the feed, and a still-poison feed re-records its fault through the runner.

Lookup, `replayAll` and discard are **saga-type-scoped**: an entry owned by a different saga type under a colliding `SagaId` is invisible to this replayer (`ENTRY_NOT_FOUND`) and can never be fed through the wrong orchestrator or deleted by it — drain it through its own type's replayer.

**Crash convergence.** Every durable write the replayer makes is a single statement, and the remedy for a crash or infrastructure failure between any two of them is simply to **rerun the same `replay`/`replayAll`**: a feed that already committed is recognised as applied (an applied genesis for a start entry, the row's `last_replayed_offset` for a correlated one) and its entry is discarded as moot, and forward commands the first attempt already dispatched dedup on their deterministic keys in the command inbox.

**Quiescence assumption.** A drain is an operator action and assumes it is the only recovery actor on that saga while it runs: nothing coordinates two drains of the same saga, or a `replay`/`discard` of one of its entries running alongside a drain — run one at a time per saga. (Live delivery is kept out of the way by the shield, which holds live correlated events for a saga that owns entries; a `COMPENSATING` saga's live events resume its episode instead.) Each step classifies its outcome by re-reading the saga row after the feed, so an interleaved writer can make one call mis-report its outcome; it cannot lose an event — an entry is discarded only once its feed committed or proved moot — and rerunning `replayAll` afterwards converges.

**Null-saga entries can accumulate duplicate rows.** When `sagaId` could never be identified (e.g. `correlate`/`extractSagaId` itself threw), the PostgreSQL store's `saga_id` column is `NULL`, and SQL's standard `NULL`-distinct semantics mean the idempotent-upsert-on-publish behavior does **not** apply across repeated failed replay attempts at the same offset — each failed re-quarantine can insert an additional row rather than updating one (see `PostgresSagaDeadLetterStoreTest.publish_nullSagaEntry_sameOffsetTwice_insertsTwoRows_discardRemovesBoth`, which pins this as an accepted, documented divergence from the in-memory test double). Each new row is born carrying the set's oldest first-fault and first-replay instants and its newest resolved target, so a re-quarantine does not reset the evidence the key-age guards and retention read. `discard(null, eventOffset)` removes **all** matching null-saga rows at once (`IS NOT DISTINCT FROM` semantics); the retention sweeper (below) independently clears stray duplicates of an entry that was never resolved to a target.

### Resume a give-up fault

A compensation episode the compensation-retry sweeper or the timeout runner gave up on — past the give-up horizon, over a stale episode, or when `compensate` threw on their resume — is `FAULTED` with `pre_fault_status = COMPENSATING` and has **no dead-letter entry**, so `replayAll` has nothing to feed. Do not repair the row in the database: resume it with the same replayer:

```java
// fresh episode — resumes under the episode's own keys; committed compensations dedup:
SagaDeadLetterReplayer.ResumeOutcome outcome = replayer.resumeFaulted(sagaId);
// stale episode — after reconciling which compensations actually executed downstream:
replayer.resumeFaulted(sagaId, true);
```

The resume runs through the step executor's own compensation resume, the one the sweeper uses: no dead-letter entry is fabricated and no un-fault is written — only the episode's terminal write (`COMPENSATED`, or `FAILED` on a permanent compensation rejection) clears the fault.

| Outcome | Meaning | Saga row afterwards / what to do |
|---|---|---|
| `RESUMED` | The episode ran to its terminal write. | `COMPENSATED` or `FAILED`. Verify downstream. A live correlated event that arrived during the resume (e.g. one a dispatched compensation emitted) was held behind the fault: the replayer then logs a `WARNING` naming `replayAll(sagaId)`, which discards such entries as moot and clears the shield. |
| `STILL_POISON` | `compensate` still throws, or the terminal write cannot convert the saga state. | When `compensate` threw, re-faulted in place with no entry written; when the terminal write cannot convert the state, unchanged — nothing is written. Fix, deploy, resume again. |
| `NO_PROGRESS` | A compensation failed transiently or was refused admission, or the terminal write lost a race to a concurrent writer; the resume wrote nothing durable. | Still `FAULTED` — the sweeper never re-drives a `FAULTED` row. Resume again once the cause has cleared; a rerun dedups every compensation that already executed. |
| `STALE_COMPENSATION_BLOCKED` | The episode's claim (`episode_claimed_at`) is older than the inbox retention window; nothing was dispatched. Refused by the replayer's `inboxRetentionMaxAge` or, without it, by the step executor's key-age guard. | Unchanged. Reconcile, then `resumeFaulted(sagaId, true)`: the forced resume re-dispatches the episode at-least-once — a compensation whose key was pruned **executes again** (`WARNING`, `streamrune.saga.forced_stale_resume`). |
| `SAGA_NOT_FOUND` | No saga row of this replayer's type. | — |
| `NOT_FAULTED` | The row is live, `COMPENSATING` (the sweeper re-drives it) or terminal. | Unchanged. |
| `FORWARD_FAULT` | A forward-step fault (`pre_fault_status` is not `COMPENSATING`); its poison event is a dead-letter entry until someone discards it. | Unchanged. Use `replayAll(sagaId)`; once its entry was discarded and nothing is left to drain, [`compensateFaulted(sagaId)`](#compensate-a-stranded-forward-fault). |
| `ENTRIES_PENDING` | Dead-letter entries (named, or resolved null-saga entries) are pending for the saga, e.g. a live event held behind the fault. | Unchanged. Use `replayAll(sagaId)`: its first feed resumes the same episode under the same keys. |

`force` lifts only `STALE_COMPENSATION_BLOCKED`. Every outcome is logged and counted by `streamrune.saga.resume_faulted`. **Crash convergence:** if the process dies after some compensations committed and before the terminal write, the row stays `FAULTED` over the same episode (a stale one is still refused without `force` — the crash forges no license); rerun the same call — the committed compensations dedup on the keys the crashed attempt wrote and only the rest run.

### Compensate a stranded forward fault

Discarding the poison entry of a forward fault — the remedy for `EVENT_NOT_FOUND`, or for a business process cancelled out-of-band — leaves the row `FAULTED` (`pre_fault_status` `NULL`, `STARTED` or `RUNNING`) with no entry, and **nothing advances it**: `replayAll` has nothing to feed, `resumeFaulted` answers `FORWARD_FAULT`, and `FAULTED` time is outside every automatic driver — `SagaTimeoutRunner` and the compensation-retry sweeper both skip a `FAULTED` row, so the saga's `timeout()` never compensates it while it is faulted. Its forward side effects stay in place. The discard logs a `WARNING` naming the remedy, and `streamrune.saga.faulted_rows` keeps counting the row. Do not repair it in the database: compensate it with the same replayer:

```java
SagaDeadLetterReplayer.CompensateOutcome outcome = replayer.compensateFaulted(sagaId);
```

It is the claim the timeout runner would have made, taken from the faulted row: a compare-and-swap to `COMPENSATING` that stamps a fresh episode, then `compensate()` over the persisted state under the new episode's keys — no forward command runs again, and there is no key-age refusal and no `force`. If the saga can instead proceed without the discarded event, wait for a later correlated event to be held behind the fault and drain it with `replayAll(sagaId)`, whose forward write clears the fault. A saga that faulted before its genesis was applied has only `compensateFaulted`.

| Outcome | Meaning | Saga row afterwards / what to do |
|---|---|---|
| `TERMINAL` | Claimed and compensated. | `COMPENSATED` or `FAILED`. Verify downstream; a `WARNING` names `replayAll(sagaId)` if a live event was held behind the fault while the call ran. |
| `COMPENSATING` | Claimed; a compensation failed transiently or was refused admission, or the terminal write lost a race. | A claimed `COMPENSATING` episode: the sweeper, the timeout runner and correlated events re-drive it under the same keys. Nothing to do. |
| `POISON` | `compensate` threw on the new episode — or the claim could not convert the saga state, and nothing was written. | `FAULTED` inside the new episode with no entry — the give-up shape. Fix, deploy, then [`resumeFaulted(sagaId)`](#resume-a-give-up-fault). |
| `CLAIM_LOST` | A concurrent writer moved the row first; nothing was dispatched. | Re-read the row before deciding again. |
| `SAGA_NOT_FOUND` | No saga row of this replayer's type. | — |
| `NOT_FAULTED` | The row is live, `COMPENSATING` or terminal. | Unchanged. |
| `COMPENSATION_FAULT` | The row faulted inside a compensation episode; a fresh claim would re-execute its committed compensations. | Unchanged. Use `resumeFaulted(sagaId)`. |
| `ENTRIES_PENDING` | Dead-letter entries are pending for the saga. | Unchanged. `replayAll(sagaId)`, or `discard` each entry deliberately, then compensate. |

Every outcome is logged and counted by `streamrune.saga.compensate_faulted`. **Crash convergence:** before the claim commits nothing is written — rerun the call. After it commits the row is an ordinary claimed `COMPENSATING` episode (a rerun answers `NOT_FAULTED` and dispatches nothing), and its automatic re-drivers finish it under the same keys — the compensations the crashed call executed dedup and only the rest run.

### Verify (or discard)

After a `REPLAYED` (or `RESUMED`) outcome, confirm the saga progressed as expected (`SagaStore.load`, downstream projections/read models). If an entry should be abandoned instead of replayed (e.g. the business process was cancelled out-of-band, or its event can no longer be read), call **`SagaDeadLetterReplayer.discard(sagaId, eventOffset)`** — no replay attempt required. Discard through the replayer, not `SagaDeadLetterStore.discard` directly: the replayer also clears the saga's shield once its backlog is empty, whereas a bare store delete leaves the shield set and the saga's live correlated events held. Discarding the last entry of a `FAULTED` saga leaves it with nothing to drain, so the replayer then logs a `WARNING` naming its remedy — [Compensate a stranded forward fault](#compensate-a-stranded-forward-fault) for a forward fault, [Resume a give-up fault](#resume-a-give-up-fault) for a fault inside a compensation episode.

**A long-faulted saga comes back already past its deadline.** `FAULTED` time is not subtracted from `timeout()`, which is absolute from the saga's start: a saga that a replay moves back to `RUNNING` after `created_at + timeout()` is compensated by `SagaTimeoutRunner` on its next poll. Replay within the window, or expect that compensation.

### Retention and GDPR

Quarantine metadata, command-inbox rows, and command dead-letter queue entries are all subject to storage-limitation retention:

| Property | Default | Sweeper |
|---|---|---|
| `streamrune.saga.dead-letter-retention-max-age` | 7 days | `SagaDeadLetterRetentionSweeper` — deletes `saga_dead_letters` rows whose **`first_faulted_at`** (the immutable first-fault instant, kept by every re-quarantine) is older than the window, so repeated failed replays cannot extend an entry's life. **Never prunes a recovery handle:** an entry whose owning saga row is `FAULTED` or carries the dead-letter shield (`dead_letter_pending`), a named entry whose saga has no row, or a null-saga entry the replayer has resolved to a target (`target_saga_id`) — each is the only record of an event its saga never consumed, so it stays until a drain feeds it or an operator discards it, and `streamrune.saga.faulted_backlog` counts it meanwhile. A null-saga entry that was never resolved stays prunable. **Removing a saga row does not release its entries** — whether an operator deletes it by hand or application code calls `SagaStore.delete(...)` (for example, a cleanup of terminal sagas): the entries become row-less named entries, retained forever and counted in `faulted_backlog`. Discard them through `SagaDeadLetterReplayer.discard(sagaId, eventOffset)` before or after removing the row. **Must not exceed** `streamrune.inbox.retention-max-age`: a mid-compensation dead-letter that outlives its inbox dedup keys re-executes an already-succeeded compensation on replay (double refund), so the boot-time `SagaRetentionValidator` fails fast on a violating config, and a `SagaDeadLetterReplayer` built with `inboxRetentionMaxAge` refuses (`STALE_COMPENSATION_BLOCKED`) to resume a mid-compensation entry whose episode claim (`episode_claimed_at`, which every write that enters `COMPENSATING` stamps; a row in an episode without it is refused with `SagaUnstampedCompensationEpisodeException`, never measured from `updated_at`) is older than the inbox window |
| `streamrune.inbox.retention-max-age` | 7 days | `InboxRetentionSweeper` — deletes processed command-inbox rows older than the window; **must** exceed your actual broker redelivery/subscription-replay horizon, or a pruned key can no longer prevent a genuine duplicate dispatch |
| `streamrune.dead-letter.retention-max-age` | 30 days | `DeadLetterRetentionSweeper` — deletes `dead_letter_queue` rows older than the window. A command DLQ entry stores the full command payload (`@Encrypted` fields encrypted, per the crypto-shredding mapper); this bounds how long it sits queryable while still giving operators time to notice, investigate, and replay or discard it. **Independent of `streamrune.dead-letter.enabled`** — that switch turns off only the automatic retry runner; the bus keeps dead-lettering and this sweeper keeps pruning. Zero or negative is what turns pruning off |

All three accept ISO-8601 (Quarkus, e.g. `PT720H`) or the framework's compact duration form (Spring/Micronaut, e.g. `30d`); zero or negative disables pruning for that sweeper — **except** `streamrune.saga.dead-letter-retention-max-age`, where "disabled" means unbounded retention, which (with sagas enabled and a `SagaDeadLetterStore` bean present, the default) violates the "must not exceed inbox retention" invariant in the row above and makes `SagaRetentionValidator.validateDeadLetterRetention` throw `IllegalStateException` at startup — every replica fails to boot rather than merely skipping pruning. To retain dead-letter entries longer, raise **both** `streamrune.saga.dead-letter-retention-max-age` and `streamrune.inbox.retention-max-age` together (or set the inbox property itself to zero/negative first, which the validator treats as "never prunes, so nothing can outlive it").

The saga dead-letter and inbox sweepers are auto-configured only for the framework's **own** PostgreSQL-backed store beans (the concrete `PostgresSagaDeadLetterStore` / `PostgresCommandInbox`). If your application supplies its own `SagaDeadLetterStore` or `CommandInbox` bean, the framework registers no sweeper for it — retention for a store you own is yours to enforce (build the corresponding sweeper yourself and start/stop it with your own lifecycle). On Quarkus the saga producer logs a `WARN` naming this case, so an absent sweeper is never silent.

The command dead-letter sweeper follows the `DeadLetterQueue` bean instead. No integration creates a `DeadLetterQueue` for you — you declare the bean (typically `new PostgresDeadLetterQueue(dataSource)`) — and the framework registers a `DeadLetterRetentionSweeper` for whichever queue bean exists, so retention applies to a queue you supplied too. It does **not** depend on `streamrune.dead-letter.enabled`: that switch controls only the automatic `DeadLetterRetryRunner` (no `dead-letter-retry` relay component while it is `false`), whereas the bus dead-letters into the queue bean regardless. A deployment that turns automatic replays off and retries from an admin screen therefore still has its entries pruned after `streamrune.dead-letter.retention-max-age`; to keep them for ever, set that property to zero or negative (the sweeper logs at startup that retention is disabled), or declare your own `DeadLetterRetentionSweeper` bean, which replaces the framework's.

> **Saga compensation dwell invariant:** keep `streamrune.saga.compensation-retry-give-up-after` (default 1h) strictly **below** `streamrune.inbox.retention-max-age` (default 7d). A `COMPENSATING` episode that dwells past the inbox window loses its succeeded-compensation dedup keys to the sweeper, so a later resume re-executes an already-succeeded compensation (double refund / double stock release). With `streamrune.saga.compensation-retry-enabled=true` (the default — the setting this bound belongs to) the auto-configs validate this at startup and log a loud `WARNING` (with the exact values) if it is violated. Both dwell bounds (the sweeper's give-up horizon and `SagaTimeoutRunner.maxCompensatingDwell`) are measured from the episode's durable claim instant (`episode_claimed_at`, fault-cycle-immutable), never the refreshable `updated_at`, so a fault→replay cycle cannot reset the clock and re-open the dwell window. If you disable the compensation-retry sweeper but keep a `SagaTimeoutRunner`, set that runner's `maxCompensatingDwell` below the inbox window so the timeout path still caps dwell. A re-drive refused admission wholesale (open circuit breaker, closing bus, interceptor veto) is not counted as an attempt, so the give-up horizon never fires on it: a saga whose re-drives keep being refused stays `COMPENSATING` until one is admitted or the key-age guard below faults it.
>
> **Runtime key-age guard:** the boot-time validation above only `WARN`s, and no in-process bound helps across a *deployment-wide outage* longer than the inbox window. The automatic re-drive paths therefore carry the same key-age guard as `SagaDeadLetterReplayer`'s `STALE_COMPENSATION_BLOCKED`, on the same `episode_claimed_at` anchor: the auto-wired sweeper FAULTs (without dispatching) any `COMPENSATING` episode older than `streamrune.inbox.retention-max-age` instead of re-driving it — even on its guaranteed first post-restart attempt — and a `SagaTimeoutRunner` does the same *before* its resume dispatch when its `Builder.inboxRetentionMaxAge` is set (application-declared runners: wire your deployment's inbox window there yourself, alongside `maxCompensatingDwell`). Such a FAULT means "this episode's dedup keys are unprovably fresh": reconcile which compensations actually executed before any forced resume. The event-path resume (a correlated or start event redelivered to a `COMPENSATING` saga) carries the same guard, armed on every `SagaRunner` bean by the integrations from the same property: it quarantines the event and FAULTs the saga instead of dispatching. A forced dead-letter replay is the one path that passes it — see `STALE_COMPENSATION_BLOCKED` under [Replay](#replay). A **zero/negative** `streamrune.inbox.retention-max-age` (inbox pruning disabled — keys never swept, never stale) normalizes to guard-inert in *every* consumer of the knob — sweeper, timeout runner, `SagaDeadLetterReplayer`, and the boot validator alike — so wire the raw deployment value everywhere without special-casing the disabled-pruning configuration.

`SagaDeadLetterEntry.errorMessage` may contain orchestrator exception text. Orchestrator authors are expected to avoid embedding raw event field values (potential PII) in exception messages, since this column is not itself subject to crypto-shredding — the framework cannot enforce this, only document it as an authoring expectation.

Crypto-shredding covers events, saga state, and the `@Encrypted` components of dead-lettered commands (the rest of a `dead_letter_queue` row — ids, user, error message — is plaintext until the row is deleted), and building the event store fails fast if a registered event or state type has an `@Encrypted` field but no `CryptoEngine` is configured. See the "GDPR crypto-shredding coverage and hardening" section in `docs/QUICKSTART.md` for the full detail (baggage allowlisting, post-forget snapshot behavior, the resurrection-race fix, and `CachedCryptoEngine`'s mandatory TTL).

## Operations runbook: incomplete GDPR erasures

`ForgetSubjectService.forget(subjectId, requester)` runs synchronously and has no durable job behind it: a process that dies, or a step that fails, between the engine's tombstone, the key deletion, the audit rows and the purges leaves the erasure part-done. **The remedy is always the same: call `forget(subjectId, requester)` again for that subject.** Every step is idempotent and the re-call completes the shred, the purges and the query-cache eviction from wherever the previous call stopped; a part-done erasure never makes the subject readable again, because every engine records the tombstone before, or with, the key deletion. The [GDPR guide](advanced/gdpr-erasure.md#when-a-forget-does-not-finish-call-it-again) lists what each stopping point leaves behind.

### Detect

1. **At the call site.** Record each erasure request in your own store before calling `forget(...)`, and mark it done only when the call returned a `ForgetResult` with `fullyErased() == true`. A call that threw (an exception from `deleteKey`, or an `Error` carrying the crypto-shred-completed note), returned `fullyErased() == false`, or never returned (the process died) leaves the request open. This is the only signal for a process that died inside `deleteKey` before any audit row was written.
2. **Metrics and logs.** `streamrune.gdpr.purge_failed` (page on any nonzero value) and the WARN lines `GDPR forget ... failed for subject-hash=...`.
3. **Audit log.** With an audit store configured, each forget writes a `GDPR_FORGET` row with `outcome = 'SUCCESS'` and no `error_message` once the key is deleted (the shred row), then one `SUCCESS` row `purged read model '<name>'` (or a `FAILURE` row) per purger. `aggregate_id` holds the subject's hash, `SubjectId.of(id).redacted()`, never the raw id. An erasure is incomplete when its latest shred row is missing, or is not followed by a `SUCCESS` row for every registered purger:

```sql
-- Subjects whose latest shred row lacks a SUCCESS row for some purger.
-- :purger_count is the number of SubjectDataPurgers the ForgetSubjectService was built with.
WITH shred AS (
  SELECT aggregate_id AS subject_hash, max(id) AS shred_row
  FROM audit_log
  WHERE command_type = 'GDPR_FORGET' AND outcome = 'SUCCESS' AND error_message IS NULL
  GROUP BY aggregate_id)
SELECT s.subject_hash
FROM shred s
LEFT JOIN audit_log p
  ON p.aggregate_id = s.subject_hash AND p.id > s.shred_row
 AND p.command_type = 'GDPR_FORGET' AND p.outcome = 'SUCCESS'
 AND p.error_message LIKE 'purged read model %'
GROUP BY s.subject_hash
HAVING count(DISTINCT p.error_message) < :purger_count;

-- Subjects with a FAILURE row (deleteKey, a purge or the query-cache eviction failed) and no
-- shred row after it.
SELECT DISTINCT f.aggregate_id AS subject_hash
FROM audit_log f
WHERE f.command_type = 'GDPR_FORGET' AND f.outcome = 'FAILURE'
  AND NOT EXISTS (
    SELECT 1 FROM audit_log s
    WHERE s.aggregate_id = f.aggregate_id AND s.id > f.id
      AND s.command_type = 'GDPR_FORGET' AND s.outcome = 'SUCCESS' AND s.error_message IS NULL);
```

The audit log holds hashes, so map a hit back to a subject through your own request record (step 1), or compute `SubjectId.of(candidate).redacted()` for the ids you hold. The queries read the latest shred row per subject, so they cannot see a forget that died before writing any row for a subject an earlier erasure already covered (a subject forgotten, reinstated, then forgotten again): the earlier, completed erasure is the latest row they find. Step 1 covers that case.

### Fix and re-run

Fix the cause first (the key store, the audit store, the read model a purger writes to), then call `forget(subjectId, requester)` again with the same subject id. Confirm the result's `fullyErased()` is `true` and that the audit query above no longer lists the subject. Re-calling an erasure that had in fact completed is harmless: it repeats every step and adds one more set of rows to the append-only audit log.

### Query cache on other instances

The re-call, like the original call, evicts the `CachingQueryBus` of the instance it runs on only. Other instances keep their cached `@Cacheable` answers until the entries' TTL expires; see [Cached query answers are evicted](advanced/gdpr-erasure.md#cached-query-answers-are-evicted) for how to bound that.

## Operations runbook: projection dead letters

**Scope: `CONTINUOUS` and `SCHEDULED` projections only.** `INLINE` projections (see
[Execution modes](concepts.md#execution-modes)) have no dead-letter mechanism, no checkpoint, and
no replay path — a throwing `process()` is logged and swallowed by `InlineProjectionInterceptor`,
and the triggering command still reports success. If an INLINE projection needs the recovery this
runbook describes, register the same projection logic under CONTINUOUS or SCHEDULED as well.

When a projection batch fails under the DLQ error strategy, the runner classifies the error via `ProjectionErrorClassifier` (default, walking the whole cause chain: infra failures — `SQLException`, a bare `CryptoOperationException`, and `EventStoreException`/`IOException`/`UncheckedIOException` frames — are `TRANSIENT`; a `SubjectForgottenException`, a deterministic frame such as `EventDeserializationException`/`UnknownEventTypeException`/`CryptoMappingException`, and everything else are `POISON` — see `ProjectionErrorClassifier.DEFAULT` for the precedence). A `TRANSIENT` error is retried with capped-exponential backoff and is **not** dead-lettered if it recovers; a `POISON` error (or a `TRANSIENT` error that exhausts the retry bound) is dead-lettered. Since this wave, **both** `ContinuousProjectionRunner` and `ScheduledProjectionRunner` dead-letter and **continue** past the failed range rather than halting — `ContinuousProjectionRunner` previously halted the whole projection (`state=ERROR`) on the first DLQ failure; that behavior is gone. Either way the failed `[fromOffset, toOffset]` range is captured in the `ProjectionDeadLetterStore` (`projection_dead_letters` table) for an operator to inspect and replay once the cause is fixed, leaving a hole in the read model behind the checkpoint in the meantime.

### Detect

- **Alert on `streamrune.projections.failed`.** Every projection processing failure increments this counter, tagged with `projection.name`. A run of `TRANSIENT` failures that keeps recovering is not itself a problem (no dead-letter, the projection keeps making progress); alert on a sustained rate, or on this metric combined with a growing dead-letter backlog. A `ProjectionCheckpointSaveException` retry (a projection on `nonAtomicAtLeastOnce()` whose checkpoint store is unavailable) counts on `streamrune.projections.failed` and shows as growing lag; it is never dead-lettered. On a continuous runner's live path each such retry logs the runner's WARN and also the subscription poll loop's `Poll loop '…' failed (N consecutive failure(s)); retrying in … ms` ERROR with a stack trace — the projection itself is healthy; the offset store is not.
- **Enumerate dead letters** with `ProjectionDeadLetterStore.read(projectionName, limit)` (one projection) or `readAll(limit)` (across all projections) — both return **oldest first** (`failedAt` ascending, tie-broken by `fromOffset` ascending), so a backlog larger than `limit` still surfaces its oldest (earliest-blocking) entries first instead of stranding them behind the newest. Each `ProjectionDeadLetterEntry` carries the projection name, the failed `fromOffset`/`toOffset`, `batchSize`, the exception class name (`errorType`) and message (`errorMessage`), `attempts`, and `failedAt` — metadata only, never the event payload.

### Inspect

To see the actual data, re-read the event store over the entry's `[fromOffset, toOffset]` range — `event_stream` remains the single crypto-governed source of truth. A range whose events can no longer be read comes back empty; replaying such an entry discards it without processing, which is the correct outcome. Crypto-shredded events are still read — with their `@Encrypted` fields redacted — and a replay feeds them like any other.

### Fix and deploy

A dead-lettered batch failed deterministically (a projection bug or poison data), so replaying it against unchanged code just re-fails. Diagnose the root cause in `Projection.process(...)`, fix it, and deploy before replaying.

### Replay

Once a fix has shipped, drive recovery with `ProjectionDeadLetterReplayer` (no builder — a direct constructor takes the event store and the dead-letter store). Replay is explicit and operator-driven; there is no background loop.

```java
ProjectionDeadLetterReplayer replayer =
    new ProjectionDeadLetterReplayer(eventStore, projectionDeadLetterStore);

// Replay up to maxEntries dead letters for one projection, oldest range first.
ProjectionDeadLetterReplayer.ReplayResult result =
    replayer.replay(ProjectionName.of("order_summary"), orderSummaryProjection, 50);

int recovered = result.replayed();   // entries re-processed (or empty) and discarded
int stillDead = result.failed();     // entries that failed again and were kept
int fenced = result.fenced();        // entries the projection applied nothing from, kept
```

Each entry is re-read from the global stream and fed to the projection through `Projection.processDeadLetterReplay`; on success the entry is discarded. A still-failing entry is kept and logged, and the remaining entries are still attempted, so one poisoned batch does not block recovery of the others. A feed that returns normally but reports it applied **nothing** keeps the entry too and counts it as `fenced`: a self-fencing projection such as `WindowedProjection`, whose offset fence has already moved past the hole, reads the never-accumulated range as done — replay such an entry from a fresh process before the live runner advances again, or discard it deliberately. A `Projection` decorator must forward `processDeadLetterReplay` to its delegate (the shipped decorators do), or a wrapped self-fencing projection reports "applied" and the replayer discards the range's only record. `Projection.process(List)` **must be idempotent** — replay is an out-of-order patch behind the projection's checkpoint (the checkpoint is never moved), and later offsets in the stream were already applied when the batch was skipped.

Replay is at-least-once and runs outside the checkpoint transaction for every delivery mode, possibly concurrently with the live runner and with an older event after a newer one — `process` must tolerate both. A `BaseProjection` writes the replayed range through the repository it was constructed with, autocommit, even when it is registered `TRANSACTIONAL_LOCAL`. Under `nonAtomicAtLeastOnce()` a dead-letter entry can only come from a projection failure, never from a checkpoint-save failure.

Because a dead-lettered range no longer halts the projection, there is nothing to restart — the runner is already past the range and continuing live; replaying just clears the queued entry and backfills the hole sooner.

## Multi-replica deployment

Running more than one instance of the same application is fully supported, and the framework **enforces** single-active-consumer per projection/subscription name via a persisted, DB-clocked lease (`LeaseBasedLeadership`) instead of only documenting it as a requirement.

> **Read-side scales horizontally; the write-side global append does not.** Adding instances scales
> reads, projections, and *per-aggregate* command concurrency — but **not** the global append rate.
> Plan write capacity against the single write lane below before sizing a write-heavy cluster.

### Write capacity: one global write lane

Every append — to *any* aggregate or stream — reserves its global offsets from the single row of
`global_offset_sequence` and holds that row's lock until the append transaction commits or rolls
back (see the `PostgresEventStore` class javadoc). That lock is what makes the global offsets
**gap-free and commit-ordered**: a rolled-back append releases its reservation instead of leaving a
hole, so a committed offset N means every offset below N is committed. The readers depend on it — a
projection or subscription checkpoint is a single offset, and `readGlobalStream` never skips a
committed event and never waits on an unrelated transaction elsewhere in the database. The price is
that every append's commit path (reserve → batch insert → outbox → audit → commit) is globally
serialized: **one write lane per event store**.

Capacity implications an operator **must** plan for:

- **Sustained global write throughput is bounded by the serial commit rate of that lane**, not by
  appender concurrency or aggregate count. The `PostgresEventStore` javadoc gives the design
  estimate as "a few hundred to low-thousands of append transactions/sec on healthy PostgreSQL with
  `synchronous_commit=on`". That is an order of magnitude to plan around, **not a measurement**: the
  framework project has not published a measured capacity envelope. Everything inside the lane
  lengthens it — larger or multi-event appends, outbox rows, same-transaction audit rows, commit
  latency — while `@Encrypted` serialization runs before the connection is even acquired, so KMS or
  Vault latency stays outside it.
- **Adding app instances or threads does not raise the append ceiling.** Horizontal scale-out raises
  read/projection throughput and per-aggregate command concurrency, but every instance's appends
  still queue behind the same counter-row lock. Provision for the serial append rate, not `N ×`
  per-instance throughput.
- **Contention surfaces as command failures, not just latency.** Size `lock_timeout` /
  `statement_timeout` with the serial append path in mind: under sustained write load appends queue
  on the counter row, and once a queued append exceeds `lock_timeout` or the event store's own
  bound (`streamrune.event-store.statement-timeout`, `30s` by default) it fails and is surfaced as
  an `EventStoreException` command failure. Set these timeouts deliberately so
  append contention degrades predictably (bounded, observable command failures) rather than as
  unbounded latency — but do not set them so aggressively that normal queueing under peak load trips
  them.
- **Measure your own envelope before you size a write-heavy deployment.** Neither harness in the
  repository is a capacity figure: `PostgresPerformanceTest`
  (`streamrune-eventstore/streamrune-postgres`) drives concurrent appends and commands against a
  Testcontainers PostgreSQL and prints the rate it observed on whatever machine ran it, asserting only
  a floor; the JMH suite (`streamrune-runtime/src/jmh`, `./gradlew :streamrune-runtime:jmh`) runs
  against `InMemoryEventStore` and is explicitly not a proxy for `PostgresEventStore`. Load-test
  against your own PostgreSQL, payload sizes, encryption, outbox and projection load — the checklist
  under [Async command admission control](#async-command-admission-control)
  lists what to record.

**The scaling lever is partitioning, not replicas.** If a workload needs more global append
throughput than one lane delivers, that is an architectural constraint of the gap-free design, not a
tuning knob: split the workload by bounded context or tenant across separate event stores — separate
databases, or separate schemas selected by the DataSource's `currentSchema` (the baseline creates
every object unqualified in the connection's current schema, counter included), each with its own
lane. There is no global order across partitions; correlate cross-partition flows through the
events' correlation and causation ids rather than a merged offset.

### Async command admission control

The global counter serialization above is a **deliberate design trade-off**, not a bug — it is what
makes the commit-ordered global stream possible, and no in-process admission control changes it or
substitutes for it. What the framework does bound is the intake of `VirtualThreadCommandBus.executeAsync`: spawning one
virtual thread per call with no limit would let a submission rate that outpaced the serial append
ceiling above queue pending futures and command state in memory without limit — a slow degradation
into an OOM rather than a fast, visible backpressure signal.

`Builder.maxInFlightAsyncCommands(int)` (default **10,000** — a generic safety-valve number, not a
measured throughput figure for any deployment) bounds how many async commands may be admitted
and not yet complete at once. Once exhausted, a further `executeAsync` call returns a future failed
with `CommandBusOverloadedException` **immediately** — nothing is attempted, the caller's thread is
never blocked waiting for a slot — and increments `streamrune.commands.async.rejected`. The
synchronous `execute` path is deliberately **not** gated by this budget: it is already bounded by the
caller's own threads. `close()` continues to drain in-flight commands (sync and async alike) exactly
as it does without the budget; the async budget only governs *admission*, not shutdown. A command frees its slot
just before its future completes, normally or exceptionally, so a caller that resubmits as soon as it
observes the previous result (or from a stage chained on the future) is never refused by its own
finished command; cancelling a future frees nothing early, because the command runs on until it
finishes.

This is an **admission-control safety valve**, not a capacity plan. Treat the default as a starting
point to override, not a target to reach: size `maxInFlightAsyncCommands` from **your own measured**
capacity profile, using a checklist like the one below — this guide does not, and cannot, publish a
number for your deployment:

- [ ] **Hardware and PostgreSQL/storage/replication configuration** the profile was measured against
      (CPU, memory, disk class, `synchronous_commit`, replica count and topology).
- [ ] **Connection pool size** and pool-wait time under the target load — the async budget bounds
      pending command *state*, not active connections; the pool bounds those separately, and both
      must be sized together.
- [ ] **Representative payload size and event count per command** — a capacity number measured with
      trivial payloads does not transfer to a workload with large `@Encrypted` fields or multi-event
      aggregates.
- [ ] **Concurrency and app-instance count** actually exercised during measurement, and whether the
      measured ceiling scales with instances (per the single write lane above, it should not, for
      the global append rate specifically).
- [ ] **Outbox, audit, and crypto enabled or disabled** during measurement — each adds work to the
      same commit path and changes the achievable rate.
- [ ] **p95/p99 command latency, throughput, and memory** at the target `maxInFlightAsyncCommands`,
      and headroom observed before `streamrune.commands.async.rejected` starts climbing.
- [ ] **Projection/outbox lag** under the measured load — an admission budget sized only against the
      append ceiling can still starve downstream consumers if they fall behind faster than commands
      are admitted.
- [ ] **Recovery behavior**: how the system drains `streamrune.commands.in_flight` and any
      `streamrune.commands.async.rejected` backlog once a load spike or an incident ends.

None of these numbers are invented here; run your own load test against your own deployment and
record the result alongside the configured budget. Alert on a sustained nonzero
`streamrune.commands.async.rejected` rate — it means submissions are outpacing the configured budget
(or the shared append path downstream of it) — and watch `streamrune.commands.in_flight` (a live
gauge) alongside it: pinned near the budget with rejections climbing means the budget is the active
bottleneck, while a low gauge with occasional rejections means bursts are short but sharp enough to
still exceed it momentarily.

Expose the knob via `streamrune.max-in-flight-async-commands` in all three integrations (Spring,
Quarkus, Micronaut — same property name and prefix as the other command-bus settings above it,
`streamrune.lock-timeout` / `streamrune.stripe-count`).

### Single-active-consumer leadership

`ContinuousProjectionRunner`/`MultiProjectionRunner` and `ScheduledProjectionRunner` gate all reading, processing, and offset advancement on holding a **live lease** for their projection/subscription name. Leadership is a persisted, **DB-clocked lease** (`LeaseBasedLeadership`, backed by the `subscription_leases` table) — not a PostgreSQL advisory lock. Acquire and the renew heartbeat are each a single atomic statement over that table, using the *database's* clock rather than the JVM's, so no connection is held across calls and no session-stable route to Postgres is required. Only the leader processes; every other replica of that name sits in `ProjectionState.STANDBY`, periodically retrying acquisition.

- **A STANDBY replica is still visible to `/health`.** Every projection runner registers with `SubscriptionHealthContributor` for its whole life, not only while it leads, so a replica that has never won the lease still appears in the health view. Because lag is computed against the **shared** `projection_offset` checkpoint, the entry answers "is this subscription making progress *anywhere*?": while a healthy leader advances the checkpoint every standby reports `UP`, and if **no** replica can lead (a lease table revoked at runtime, or a leader that stood down terminally) the shared lag crosses the lag threshold and every replica reports the subscription `DEGRADED` with the growing lag in its detail. It is not `DOWN`: from the lag alone such a freeze cannot be told apart from a leader still replaying a large backlog, which must not take the replicas out of readiness (see [Health checks](#health-checks)). Alert on the `streamrune.subscriptions.lag` gauge to catch it. Before this, continuous projections registered only while `LIVE`, so an all-replicas-standby freeze reported `UP` forever with no entry for the projection at all.
- **The lease TTL is the explicit failover bound.** `streamrune.subscription.single-active-consumer.lease-ttl` (default **15s**) is how long a granted lease stays live before another replica may take it over: a standby wins the name **at most `lease-ttl` after** a dead leader stops renewing — full stop, regardless of *how* the leader died (clean shutdown, crash, or a silent network partition all converge on the same bound). This replaces the retired advisory-lock design's failover story, where latency depended on the failure mode and, for a silent partition, on the PostgreSQL server's own TCP-keepalive reaping interval (up to hours on stock settings) — there is no server-side tuning knob to reach for any more. The renew heartbeat runs at `lease-ttl / 3` (a live leader renews with ample margin before its own lease could expire).
- **No dedicated connection, no advisory locks in the leadership path.** Every lease operation (`tryAcquire`, renew, `resign`) borrows a connection from the normal pool for the duration of one statement and returns it immediately — leadership no longer holds a connection out of `maximumPoolSize` for the process lifetime, so the old **+1 pool-headroom-per-replica** sizing rule no longer applies. `pg_advisory_lock`/`pg_try_advisory_lock` do not appear anywhere in this path.
- **Epoch fencing at the write — split-brain is impossible, not just detected.** Each successful acquisition (across all replicas) is stamped with a strictly increasing `epoch`, a fencing token. The leader threads its held epoch into every `AtomicBatchProcessor.executeAtomically` call; under the same `FOR UPDATE`-locked `projection_offset` checkpoint-row transaction that already guards the offset advance, `JdbcProjectionRepository` also rejects (rolls back) any commit whose epoch is below the epoch already stamped on that row — the signature of a superseded leader resuming after a pause. The rejection happens **before** the updater writes a single read-model row, so a stale leader's write is refused by the database itself, not merely raced and cleaned up after the fact. This is proven by two acceptance ITs: `TwoRunnerLeadershipFailoverIT` (kill the leader; the standby takes over at `epoch + 1`; the old leader's in-flight commit is rejected) and `LeaseLeadershipPgBouncerTxModeIT` (the same proof, driven through a real PgBouncer container in transaction mode with `pool_size=1` — the exact configuration that split-brained the retired advisory-lock design). See the [PgBouncer warning](#postgresql-setup) above.
- **The fence cannot be left unarmed — an unfenceable multi-replica projection does not boot.** "Split-brain is impossible" is a property of the *pairing*, not of leadership alone: the epoch is only a guarantee when the `AtomicBatchProcessor` receiving it actually rejects a superseded epoch. `ContinuousProjectionRunner`, `MultiProjectionRunner` and `ScheduledProjectionRunner` therefore refuse at `build()` — every replica, at startup, before any traffic — when leadership is anything other than `SubscriptionLeadership.NOOP` and the processor's `supportsFencing()` is `false`, with a message naming both the leadership and the processor class. Every runner takes an explicit `atomicProcessor(...)`; while leadership is on, the three integrations hand each runner they assemble the single `AtomicBatchProcessor` bean — the `JdbcProjectionRepository` (`supportsFencing() == true`) — and fail at startup, naming the fix, when there is none (an application `ProjectionRepository` bean that is not a processor suppresses the framework's) or more than one. The guard therefore bites a hand-built runner given `AtomicBatchProcessor.nonAtomicAtLeastOnce()` or another non-fencing processor beside real leadership. If exactly one instance runs each projection, set `streamrune.subscription.single-active-consumer.enabled=false` — `NOOP` is epoch `0`, explicitly unfenced, and builds fine. `nonAtomicAtLeastOnce()` additionally **refuses** any commit carrying a non-zero epoch, and the default `stampFencingEpoch` throws instead of silently no-opping, so no third-party processor can disarm the fence by omission.
- **The mode is declared, checked at startup, and executed as declared.** Every registration carries a `ProjectionDeliveryMode`; `TRANSACTIONAL_LOCAL`/`EXTERNAL_EFFECT` need a fencing-capable processor, a projection that writes through the handed repository and — when the projection exposes its write target — a processor that *is* that store; `AT_LEAST_ONCE_IDEMPOTENT` is handed no transaction-scoped repository at all. The integrations wire the `JdbcProjectionRepository` bean into a runner only when a registration's mode or the leadership fence demands it — a `DataSource` selects nothing. A refusal is an `IllegalArgumentException` at `build()`/`run()` — a startup failure in the integrations; through the `StreamRune` facade, whose `startProjections()` runs each `ContinuousProjectionRunner` on a thread of its own, it is logged at ERROR when that thread starts and the projection does not run. It is never a warning the projection runs past; see [Delivery modes](concepts.md#delivery-modes--what-a-projection-promises) for the checks and the recipes. For a transactional registration this distinguishes two failure windows that are easy to conflate: a crash BEFORE the transaction commits rolls back BOTH the read-model write and the checkpoint together (the atomic-commit mechanism above), so the batch is simply redelivered and reprocessed from the last committed checkpoint — never assume the write survived just because the projection ran. An ACK/notification LOST AFTER a successful commit is a different, and safe, case: the checkpoint IS advanced, so a redelivery-driven reprocessing attempt re-reads from that already-advanced checkpoint and, where relevant, is additionally rejected by the epoch fence above — it is the checkpoint commit itself, not a downstream acknowledgement, that this framework treats as the durable fact. A crash while the `COMMIT` itself is in flight is decided by PostgreSQL atomically — both the read-model rows and the checkpoint are durable, or neither is; the rerun either starts after the checkpoint or re-reads the range. Nothing in the framework can observe or test the in-flight state. For an `AT_LEAST_ONCE_IDEMPOTENT` registration the read-model write is not part of that transaction: a crash after `process` returned and before the checkpoint committed, or a checkpoint transaction that dies after the projection wrote (a lost connection, a failed `COMMIT`), redelivers a batch the projection already applied — the case its idempotency promise exists for. The epoch fence and the overlap guard run before `process`, so a superseded leader's at-least-once projection writes nothing.
- **The fence is armed at takeover — and a leader that cannot arm it does not process.** Every leadership acquisition durably stamps the new epoch on the projection's checkpoint row (`AtomicBatchProcessor.stampFencingEpoch`) as the very next step after the acquire — before any read or processing — so the superseded leader's writes are rejected from the *moment of takeover*, not only after the new leader's first committed advance (previously the fence stayed inert across the whole catch-up, or a full idle cron period). A failed stamp (a DB blip) fails **closed**: the continuous runner logs a `WARN` and stands by one poll interval before retrying acquire+stamp; the scheduled runner logs a `WARN` and skips that tick. Processing without the stamp would re-open the exact window the stamp closes — and with the checkpoint row unreachable, processing could not have committed anyway. **Consequence worth alerting on: a PERMANENT stamp failure loops the lease-holding runner in that WARN-and-retry standby cycle indefinitely — it does not go terminally `DOWN`.** The runner cannot distinguish a permanent failure from a transient one (the stamp is a plain checkpoint-row write; a revoked table grant looks identical to a blip), and standing down terminally on a transient would trade a self-healing stall for an operator-page outage, so looping is the intended behavior. The stall is still observable: the shared-checkpoint lag rises and is visible through the standby health entry (see the STANDBY bullet above), and each cycle emits the `WARN` ("could not stamp takeover epoch"). Alert on that repeated `WARN` and on projection lag — do not wait for a `DOWN` health transition that this failure mode never produces (lag crossing the lag threshold turns the shared entry `DEGRADED`, never `DOWN`).
- **Defense-in-depth: the overlap and monotonic guards.** `executeAtomically` retains its two pre-existing checkpoint-row checks alongside the epoch fence: it rejects a batch whose **first** offset is at or before the committed checkpoint (the overlap guard — a stale/failed-over writer that read at an older checkpoint and fetched a range extending past a newer leader's advance) and rejects a **non-advancing** offset save (the monotonic guard — a fully-behind redelivery or bare regression). All three checks share the same `FOR UPDATE` lock and roll back the whole transaction (read-model write **and** offset advance) together, so a non-idempotent projection (count++/balance/list-append) is never double-applied. The epoch fence is what specifically stops a *superseded* leader; the overlap/monotonic guards catch redeliveries and same-epoch races the epoch fence alone doesn't address.
- **`PgAdvisoryLocker` (aggregate command locking) is a separate, unaffected subsystem.** The per-aggregate pessimistic lock a command handler holds while it runs is a different mechanism entirely and still uses PostgreSQL advisory locks as before — only subscription/projection *leadership* moved off advisory locks onto the lease.
- **The command DLQ retry runner is leadership-gated too.** `DeadLetterRetryRunner` shares the same `SubscriptionLeadership` (auto-wired into the runner bean in all three integrations): only the leader replica polls and re-dispatches dead-lettered commands. Without this, and without a shared `CommandInbox`, two replicas could each claim (`FOR UPDATE SKIP LOCKED` releases the row before re-execution) and re-run the same dead-lettered command, emitting duplicate domain events. Leadership alone is not enough, and the runner says so: `DeadLetterRetryRunner.Builder.build()` refuses real leadership paired with a `CommandBus` that has no `CommandInbox` (`supportsIdempotentExecution() == false`), because the gate makes one replica *poll* but cannot stop a stale leader (resumed after a pause, past its lease TTL) from re-executing an entry of a batch it already fetched — only the inbox dedups that. All three integrations configure an inbox-backed bus when a `DataSource` bean exists. With `single-active-consumer.enabled=false` it falls back to `NOOP` (always leader) like everything else, so if you disable leadership, either run exactly one instance or configure a `CommandInbox` so DLQ replays are idempotent.
- **Saga subscriptions are not leadership-gated — and do not need to be.** `SagaRunner` is an `EventListener` over whatever subscription you give it, and neither `PollingEventSubscription` nor `HybridEventSubscription` takes a `SubscriptionLeadership`: a saga runner wired on every replica runs on every replica. Its correctness does not rest on a single consumer — the saga row is CAS-protected, every saga command is dispatched through the shared command inbox, and the command bus answers a duplicate keyed dispatch from the inbox even when it overlaps the original (it looks the key up again under the aggregate lock, and once more when a keyed decider rejects) — so concurrent runners cost duplicate deliveries that dedup (CAS conflicts, batch retries, inbox hits), not double side effects. A lease could not carry that guarantee anyway: a leader paused past its TTL resumes as a second consumer, which is exactly the duplicate those mechanisms absorb. To run one active consumer per saga type for efficiency, choose it by deployment topology. The one requirement is that every replica dispatching saga commands shares the same `command_inbox` (the integrations wire `PostgresCommandInbox` whenever a `DataSource` is present; a bus without an inbox refuses keyed dispatch). See [saga.md — Concurrency](advanced/saga.md#concurrency).
- **Disabling it.** Set `streamrune.subscription.single-active-consumer.enabled=false` to fall back to `SubscriptionLeadership.NOOP` — an always-leader lease at **epoch 0 (unfenced)**. This is today's single-writer-assumed behavior: no lease traffic, no epoch fencing (a `fencingEpoch` of `0` always passes the write-fence check). Only do this if you already guarantee exactly one instance runs each projection/subscription name by deployment topology.

All three integrations arm the fence whenever they enable leadership. The `LeaseBasedLeadership` bean stays conditional on a `DataSource`; the fencing processor — the `JdbcProjectionRepository` bean — is wired into every runner they assemble because leadership is on (it must honour the epoch), not because the `DataSource` exists. An out-of-the-box Spring, Quarkus or Micronaut application with a `DataSource` is therefore fenced, not merely leadership-gated, and one that leaves leadership on without exactly one `AtomicBatchProcessor` bean fails at startup instead of running unfenced. With leadership disabled, a runner whose registrations are all `AT_LEAST_ONCE_IDEMPOTENT` runs on `AtomicBatchProcessor.nonAtomicAtLeastOnce()`, and its INFO line says so.

```properties
# Spring / Micronaut (default: enabled=true, lease-ttl=15s, whenever a DataSource bean is present;
# the JdbcProjectionRepository bean exists on the same condition and is wired into a runner because leadership is on)
streamrune.subscription.single-active-consumer.enabled=true
streamrune.subscription.single-active-consumer.lease-ttl=15s
```

```properties
# Quarkus (same property names; Duration parses the ISO-8601 literal)
streamrune.subscription.single-active-consumer.enabled=true
streamrune.subscription.single-active-consumer.lease-ttl=PT15S
```

### Offset reset / rebuild path

Because the monotonic guard rejects any backward `saveOffset`, an intentional checkpoint rewind — replaying a projection from the beginning after a read-model schema change — must **not** go through `saveOffset`. Use `OffsetStore.reset(projectionName)` instead: it bypasses the guard by design (that is the whole point of a reset) and rewinds the checkpoint to `GlobalOffset.initial()`.

```java
offsetStore.reset(ProjectionName.of("order_summary"));
// Then restart (or let the runner naturally re-read) the projection — it replays from offset 0.
```

During the replay the projection's lag is the whole event history, on every replica, until the active consumer catches up: its health detail reads `DEGRADED` and `streamrune.subscriptions.lag` jumps to the size of the store. That is expected and leaves the health check `UP`, so the replicas stay in the readiness probe; silence lag alerts for the rebuilt projection for the length of the replay, and watch the gauge fall to confirm it is progressing.

### Switching a projection's mode

The mode is configuration, checked at startup; nothing in the database records it, so the framework
cannot see what an earlier mode left behind. When you change a projection's `deliveryMode`:

1. **Stop the runner** that drives the projection (every replica).
2. **`AT_LEAST_ONCE_IDEMPOTENT` → `TRANSACTIONAL_LOCAL` on a non-idempotent read model:** rebuild it
   before the first run under the new mode. A redelivery in the at-least-once era may have applied a
   batch twice, and the transaction does not undo history. Reset the checkpoint
   (`OffsetStore.reset(name)`, above), truncate the `<name>_view` table, and discard the projection's
   dead-letter entries (`ProjectionDeadLetterStore.read(name, limit)`, then `discard(name,
   fromOffset)` for each); the replay from offset 0 then rebuilds the model inside checkpoint
   transactions. An idempotent model needs no rebuild.
3. **`TRANSACTIONAL_LOCAL` → `AT_LEAST_ONCE_IDEMPOTENT`:** nothing to migrate — the read model keeps
   its history — but from now on the runner hands the projection no transaction-scoped repository and
   its writes commit on their own, so a redelivery applies a batch again. Make `process` idempotent
   first.
4. **Either direction, when the switch also changes where the projection writes:** rebuild in the
   new store whatever the model's idempotency. A `BaseProjection` that moves onto the processor's
   repository to become `TRANSACTIONAL_LOCAL` writes to a `<name>_view` table that is empty while
   the checkpoint is already advanced, and the same holds for a move from `TRANSACTIONAL_LOCAL` to
   at-least-once writes into another store. Reset the checkpoint, discard the projection's
   dead-letter entries and replay from offset 0, as in step 2; "needs no rebuild" and "nothing to
   migrate" above hold only while the projection keeps writing to the same store.
5. **Redeploy**, and confirm the INFO line and the gauge below report the new mode.

### Verifying a projection's mode at runtime

- **The INFO line** each executing runner logs once per registration when it starts:
  `Projection '<name>' delivery mode <MODE> on <processor> — <verdict>`. The processor is the class
  name, or `nonAtomicAtLeastOnce`; the verdict says whether the write target was verified, only
  declared, or at-least-once. INLINE registrations log their own line at discovery.
- **The gauge** `streamrune.projections.delivery_mode{projection.name, delivery.mode}` (value 1).
- **The checkpoint row:** `SELECT projection_name, last_offset, epoch FROM projection_offset` — an
  `epoch` above `0` means a fenced leader stamped the row, at takeover or on a commit.

### Operations runbook: outbox FAILED entries

An outbox entry reaches `FAILED` when the transport rejected it for its own sake (an `ENTRY` classification) `retryPolicy.maxAttempts()` times. It stays unresolved until an operator replays or skips it: on a `STRICT_PER_AGGREGATE` channel (the default) it blocks every later entry of its aggregate; on an `AVAILABILITY_FIRST` channel its successors are delivered past it — see [Ordering modes](advanced/outbox.md#ordering-modes).

1. **Detect.** `blockage_age_seconds` or `blocked_aggregates` alert, or `delivery_failed` incremented, or the ERROR line.
2. **Enumerate.** `OutboxStore.findByStatus(FAILED, limit)` — oldest `seq` first; read `aggregate_type`, `aggregate_id`, `last_error`, `attempts`, `processed_at`. Read-only; safe while relays poll. **Never use `loadPending` to look** — it claims.
3. **Diagnose.** `last_error` says why the transport rejected the entry (`ENTRY` kind: unroutable, too large, schema rejected, HTTP 4xx). Transport outages never reach `FAILED`.
4. **Fix and replay.** Fix the cause (topic/exchange, consumer schema, endpoint). `OutboxFailedReplayer.replay(id)` for one entry, `replayFailed(max)` oldest-first for many. In strict mode the entry is delivered before its successors; the aggregate resumes in order. Verify: the row reaches `DELIVERED`, `blocked_aggregates` drops.
5. **Or skip.** When the entry must never be delivered: `OutboxFailedReplayer.skip(id, operator, reason)`. The row becomes `SKIPPED` with the audit columns; if it was the blocking head, the aggregate's successors are claimable on the next poll. For many unresolvable rows at once — typically an `AVAILABILITY_FIRST` channel after a consumer change — `skipFailed(max, operator, reason)` skips the oldest `max` with one shared reason; on a strict channel prefer `skip(id)` and look at each head, because every bulk-skipped head drops one aggregate's change.

   **Contract — a skip is not a delivery.** The downstream did not receive the skipped change and will receive the successors as if it had. If the consumer's state depends on that change (inventory, payment, lifecycle, cache), emit a domain compensating or corrective event through the normal command path so the correction is itself ordered and delivered, or reconcile the consumer out of band. Record the ticket in `reason`.
6. **Never** `DELETE` a `FAILED` row by hand. `OutboxStore.delete` refuses one, but a direct SQL `DELETE` releases the aggregate with no record and the framework cannot stop it. Retention will not remove a `FAILED` row for you.
7. **Mode switch checklist.** Before restarting a channel from `AVAILABILITY_FIRST` to `STRICT_PER_AGGREGATE`: `findByStatus(FAILED, …)` must be empty, or every listed entry must be replayed or skipped first; make sure the mapper sets `streamId` on every entry (the strict store will otherwise fail appends with `OutboxOrderingViolationException`). A strict relay that still finds `FAILED` rows with no `aggregate_id` (and no `aggregate_type`) WARNs once at its first poll and keeps running — resolve them with `replay`/`skip` like any other. Before the reverse switch: understand that unresolved `FAILED` heads stop blocking and their successors go out immediately.
8. **Retention.** `SKIPPED` rows are kept for `streamrune.outbox.skipped-retention-max-age` (default 30 d; keep it at least as long as your audit requires). `DELIVERED` rows are kept for `streamrune.outbox.retention-max-age` (7 d). `FAILED` is never pruned.

## Backup, restore and disaster recovery

All of StreamRune's durable state lives in PostgreSQL — apart from key material that the Vault, AWS
KMS or filesystem crypto backends keep outside it — so its recovery story is PostgreSQL's. **The
framework project has not performed a backup/restore drill against a production deployment.** What
follows states what the schema and the runtime require of a restore; it sets no recovery times.
RPO and RTO are parameters of your deployment: choose them, then prove them with the drill checklist
at the end of this section.

**Baseline: a base backup plus continuous WAL archiving (point-in-time recovery).** Per-table dumps
taken at different moments are not a substitute, because the StreamRune tables are only meaningful
relative to one another.

**Restore every StreamRune table — and the crypto key store — to one point in time.** The pairs below
carry invariants that a restore from mixed points breaks:

- `event_stream` and `global_offset_sequence`: a counter behind `max(event_stream.global_offset)`
  makes every append collide. `SchemaValidator` refuses to start on a missing or behind counter row
  and prints the statement that re-seeds it.
- `event_stream` and `command_inbox`: an inbox row records the offsets its command appended and is
  returned to a client retry instead of re-running the command, so an inbox restored ahead of the
  events answers retries with results whose events no longer exist.
- `event_stream` and `projection_offset`: readers select only offsets above their checkpoint, so a
  checkpoint restored ahead of the stream silently skips the events later appended at the offsets in
  between. Read models kept in the same database belong to the same point. Subscription health
  reports such a checkpoint as `DEGRADED` with lag 0 and logs one `WARN` naming the subscription,
  its checkpoint and the stream head, until appends carry the head past the checkpoint again (by
  then the events in between have been skipped for good).
- `projection_offset` and `subscription_leases`: the checkpoint row carries the last leader's fencing
  epoch; a lease table restored behind it hands out a lower epoch and the projection stops with
  `ProjectionEpochRegressionException`, whose message names the two recoveries.
- `saga_state` and `saga_dead_letters`: a saga's shield (`dead_letter_pending`) exists to protect its
  entries, and `last_applied_offset` refers to offsets in `event_stream`.
- `outbox_events` and `event_audit_log`: written in the append transaction itself, like the inbox
  row; `dead_letter_queue` and `audit_log` record the commands behind those events.
- The crypto tables `encryption_keys`, `forgotten_subjects` and `erased_key_generations` (or the
  external key store), with the erasure caveat below.

**Erased subjects must stay erased.** A restore rolls the crypto state back with everything else:
keys in the postgres backend's `encryption_keys` come back for every subject erased after the
restore point, and the erasure tombstones written after it disappear (`forgotten_subjects`, or the
filesystem backend's tombstone markers when its key directory is restored from a snapshot too). The
evidence of an erasure must therefore survive the restore independently: keep an erasure record
outside the restored database, and **before the restored system takes traffic** re-apply
`CryptoEngine.deleteKey(subjectId)` for every subject erased after the restore point. Then confirm
that an erased subject's `@Encrypted` fields read back `[REDACTED]`.

**Key custody follows your threat model.** The postgres backend stores each subject's AES-256 key as
plaintext bytes in `encryption_keys` — there is no envelope encryption — so a backup holding that
table *and* `event_stream` holds keys and ciphertext together and must be protected like the
plaintext data; the Vault and AWS KMS backends keep key material out of the database. See
*Threat Model & Key Custody* in `streamrune-crypto/streamrune-postgres-crypto/README.md`.

**Restarting after a restore.**

1. **Stop every replica before restoring.** A process that keeps running holds leadership epochs,
   outbox claims and in-flight commands that describe the database as it was before the restore.
2. Restore to one point, and re-apply the post-restore-point erasures (above).
3. **Start one replica first.** `PostgresEventStoreFactory.create()` validates the schema, the
   counter row included, before anything runs; then scale out.
4. **Expect the outbox to deliver again.** Every outbox row that was `PENDING` or `IN_PROGRESS` at the
   restore point is delivered again — including rows the live system had already delivered after that
   point — and restored `IN_PROGRESS` claims go back to `PENDING` once their claim lease expires.
   Delivery after a restore is at-least-once: downstream consumers must deduplicate on the
   `X-Outbox-Entry-Id` header every shipped publisher sends, and anything they received for commits
   *after* the restore point refers to events that no longer exist in the store — reconcile it.
   `SKIPPED` rows are terminal and are not redelivered after a restore; `FAILED` rows restored as
   `FAILED` stay blocking on a strict channel until resolved.
5. **Rebuild or reconcile read models kept outside the restored database.** Projections resume from
   the restored checkpoints, but an external read model still carries the effects of events after the
   restore point; rewind it with `OffsetStore.reset(projectionName)` (see
   [Offset reset / rebuild path](#offset-reset--rebuild-path)) or reconcile it.
6. The command dead-letter queue and the saga dead-letter store hold exactly what they held at the
   restore point; work them off as usual (`streamrune.dlq.pending`, `streamrune.saga.faulted_backlog`).

**Drill checklist.** Run it against a production-sized copy and record the results next to your
targets — this guide publishes no numbers for you:

- [ ] Target RPO and RTO written down.
- [ ] Base backup + WAL replay to a chosen point completes; measured restore time and data-loss
      window recorded against the targets.
- [ ] Every StreamRune table and the crypto key store came back from that same point.
- [ ] Erasures after the restore point re-applied from the external record before traffic; a sample
      erased subject reads back `[REDACTED]`.
- [ ] The first replica boots (schema validation passes) before the rest start.
- [ ] Outbox redelivery observed and deduplicated downstream; `streamrune.outbox.pending` and
      `streamrune.outbox.in_flight` drain.
- [ ] Projection lag (`streamrune.subscriptions.lag`) returns to normal; external read models rebuilt
      or reconciled.
- [ ] The dead-letter backlogs match the restore point and are worked off.

## Security checklist

- **Connection security:** always use TLS: `jdbc:postgresql://host:5432/db?sslmode=require`.
- **Least privilege:** dedicated PostgreSQL user with `GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA streamrune TO app_user` and `GRANT USAGE ON ALL SEQUENCES IN SCHEMA streamrune TO app_user` (the `BIGSERIAL` columns of `audit_log`, `event_audit_log` and `outbox_events` draw from sequences). Both grants cover only objects that exist when they run, so apply them after the schema is migrated. `DELETE` is not optional — the retention sweepers (inbox, outbox, saga dead-letters, command DLQ), saga dead-letter discard and saga deletion, projection dead-letter and projection read-model cleanup, the GDPR snapshot purge (`snapshot_store`), and crypto key deletion (`encryption_keys`, `forgotten_subjects`) all issue `DELETE FROM`. If you must further restrict it, revoke `DELETE` only on the append-only `event_stream` and `audit_log` tables (see below), never globally. **These DML-only grants are insufficient under the default configuration**: `streamrune.event-store.schema.auto-initialize=true` (the default) runs Flyway at startup, which additionally needs `CREATE` on the schema and `ALTER`/`CREATE INDEX` on its tables. Either grant those too, or set `streamrune.event-store.schema.auto-initialize=false` and apply `classpath:db/streamrune-migration` (and `classpath:db/crypto-migration` if applicable) out-of-band with a separate, DDL-capable role — see the Flyway section above.
- **Key management for crypto:** store encryption keys in AWS KMS, HashiCorp Vault, or equivalent. Never commit keys to source control. With Vault, give the engine's token the policy listed in the [Vault module README](../../streamrune-crypto/streamrune-vault-crypto/README.md#vault-token-policy): a forget sets `deletion_allowed` on the subject's transit key and then deletes it, so the token needs `update` on `<mount>/keys/+/config` and `delete` on `<mount>/keys/+`.
- **Ingress idempotency:** any mutating endpoint a client may retry should execute its command under a server-owned, scoped `IdempotencyKey` — see [Command idempotency](advanced/idempotency.md).
- **Client-supplied aggregate ids:** build them with `AggregateId.of` in every id extractor. It refuses an id longer than 255 characters or one containing a control character (C0, DEL, C1) with `IllegalArgumentException` before any interceptor runs, so the id never reaches the `aggregate_id` columns beside the registered `aggregate_type` (`event_stream`, `audit_log`, `dead_letter_queue`); map that exception to `400`. The 255 matches the `VARCHAR(255)` `aggregate_id` columns, so a longer id is a client error rather than a failed SQL insert. It is counted in UTF-16 code units (`String.length()`), so a character outside the Basic Multilingual Plane, such as an emoji, counts as two; every id `of` accepts fits the columns. A stream id built from an id longer than 255 characters is refused too, before any interceptor runs. If your extractor adds a prefix to the client's value, bound the client's value at its own ingress so the result stays within 255; two aggregate types that share an id value need no prefix. Bind request fields as `String`, not as `AggregateId`, because Jackson builds the record through its lenient decode constructor. Use that constructor yourself only for ids derived from stored data, such as a saga's state — see [Getting started](getting-started.md).
- **Authorization identity:** `RequestContext.userId` — the identity every `CommandAuthorizationPolicy` / `@RequireRole` / `Authorization.requireOwner` check trusts and the audit records — fails closed. With an `AuthenticatedUserResolver` (Spring Security on the classpath, Quarkus Security, Micronaut Security enabled, or your own bean) it is the **authenticated principal** and the `X-User-Id` header is ignored; with `streamrune.security.trust-user-id-header=true` it is the `X-User-Id` header (trusted-gateway mode); with neither, every request is **anonymous** and the header is ignored. Startup is refused when a command bus runs an authorization interceptor that reads that identity (the one registered for a `CommandAuthorizationPolicy` or `UserRoleResolver` bean, or one you hand to a bus you build yourself) in the anonymous mode, and the mode is logged once at startup — check that line before going live. Set the flag **only** when a gateway authenticates every caller, overwrites `X-User-Id` with the authenticated id and strips any client-supplied value, and the service is reachable only through it; otherwise any client can act as any user. The `X-User-Role` header is read in that mode only — it becomes baggage `role`, recorded in every event's metadata — so the gateway must own it too (forward the caller's role or strip it); in the other modes it is ignored and there is no `role`. This is the behaviour of all three integrations (Quarkus checks the startup rule in a Quarkus REST application, Micronaut in every application that has not replaced `StreamRuneContextFilter`). See [Authorization](advanced/authorization.md#where-the-identity-comes-from-security).
- **Server-Sent Events (`GET /api/sse/{aggregateType}/{aggregateId}`):** disabled by default. It streams an aggregate's **decrypted** domain events by its two-part stream id, so enabling it (`streamrune.sse.enabled=true`) without access control exposes any tenant's events (IDOR/PII). When enabled, provide an `SseAuthorizer` bean that checks the authenticated principal against the requested `StreamId`; if you enable SSE without one the framework installs a fail-closed deny-all authorizer (every stream returns `403`).
- **Audit log protection:** add a PostgreSQL row security policy or revoke `DELETE` privilege on `audit_log` to enforce immutability.
- **Query cache authorization:** `streamrune.query-cache.enabled=true` makes a `@Cacheable` query's cache HIT skip the handler entirely — including any authorization the handler performs internally. Unlike SSE above, the framework does **not** fail closed here (there is no way to know whether a handler is meant to be caller-independent): with no `QueryAuthorizer` bean, caching runs with no authorization opinion of its own and a revoked caller keeps receiving a cached answer until TTL/invalidation. If ANY `@Cacheable` query's result depends on the caller's access, define a `QueryAuthorizer` bean (auto-detected in all three integrations) — see [query-cache.md](advanced/query-cache.md#authorization--a-cache-hit-skips-the-handler).
- **Outbox endpoint security:** webhook endpoint should verify `X-Outbox-Entry-Id` header and require HTTPS.
- **Connection string secrets:** use environment variables for `DB_PASSWORD` — never hardcode in `application.properties`. All frameworks support `${DB_PASSWORD}` interpolation.
