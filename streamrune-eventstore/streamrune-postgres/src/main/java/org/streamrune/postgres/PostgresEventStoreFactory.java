package org.streamrune.postgres;

import java.time.Duration;
import java.util.List;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * Factory for {@link PostgresEventStore}: checks the server version, applies and validates the
 * schema, and builds the store on the application's {@link DataSource}.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * var factory = new PostgresEventStoreFactory(dataSource, typeRegistry);
 * EventStore store = factory.create(); // applies + validates the schema
 * }</pre>
 *
 * <p><b>The DataSource is used as given.</b> The store borrows one connection from {@code
 * dataSource} per operation and returns it when the operation ends. The factory builds no
 * connection pool of its own and changes none of the DataSource's settings, so the application's
 * pool (HikariCP, Agroal, or any other) is the only one to size, and its settings — prepared
 * statement threshold, auto-commit, timeouts — are the ones in force. Hand the factory a pooled
 * DataSource: a plain driver DataSource such as {@code PGSimpleDataSource} opens a new physical
 * connection for every operation. The factory owns nothing that needs closing; closing the
 * DataSource stays with the application that created it.
 *
 * <p><b>Statement bound.</b> {@link #statementTimeout(Duration)} (30 seconds by default) bounds
 * every statement the store runs, per transaction or per statement, never on the session — see
 * {@link PostgresEventStore.Builder#statementTimeout(Duration)}.
 *
 * <p><b>final by design.</b> Like every other concrete adapter in this package, this factory is not
 * an extension point: configuration goes through its fluent setters, and behaviour that varies per
 * deployment (crypto engine, upcasters, snapshot migrations, outbox/inbox/audit stores) is injected
 * rather than overridden. Compose or implement {@link EventStoreFactory} directly instead of
 * subclassing.
 */
public final class PostgresEventStoreFactory implements EventStoreFactory {

  private static final Logger log = LoggerFactory.getLogger(PostgresEventStoreFactory.class);

  /**
   * Classpath location of the event-store migration series. Deliberately NOT Flyway's default
   * {@code classpath:db/migration} — that belongs to the consuming application; see {@link
   * #initializeSchema()}.
   */
  static final String EVENT_STORE_MIGRATION_LOCATION = "classpath:db/streamrune-migration";

  /**
   * Flyway history table for the event-store series. Deliberately NOT Flyway's default {@code
   * flyway_schema_history}, so the framework's series and the application's never interleave in one
   * table; the crypto series is namespaced the same way.
   */
  static final String EVENT_STORE_HISTORY_TABLE = "flyway_schema_history_streamrune";

  private final DataSource dataSource;
  private final EventTypeRegistry typeRegistry;
  private CryptoEngine cryptoEngine;
  private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
  private List<EventUpcaster> upcasters;
  private List<SnapshotMigration> snapshotMigrations;
  private PostgresEventAuditStore eventAuditStore;
  private PostgresOutboxStore outboxStore;
  private OutboxEventMapper outboxEventMapper;
  private PostgresCommandInbox commandInbox;
  private boolean autoInitializeSchema = true;
  private boolean validateSchema = true;
  private Duration statementTimeout = StatementTimeout.DEFAULT;

  /**
   * Creates a new factory.
   *
   * @param dataSource the application's JDBC {@code DataSource}, used as given (required)
   * @param typeRegistry event/state type registry used by {@link PostgresEventStore} (required)
   * @throws IllegalArgumentException if either argument is {@code null}
   */
  public PostgresEventStoreFactory(DataSource dataSource, EventTypeRegistry typeRegistry) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    if (typeRegistry == null) {
      throw new IllegalArgumentException("typeRegistry is required");
    }
    this.dataSource = dataSource;
    this.typeRegistry = typeRegistry;
  }

  /**
   * Initializes the database schema required for the event store using the Flyway migrations
   * shipped at {@link #EVENT_STORE_MIGRATION_LOCATION}, recorded in {@link
   * #EVENT_STORE_HISTORY_TABLE}.
   *
   * <p><b>Why neither is the Flyway default.</b> The series used to live at {@code
   * classpath:db/migration} on {@code flyway_schema_history}, which is exactly where a consuming
   * application puts its OWN migrations (Spring Boot's documented default), and this module
   * declares {@code api(flyway-core)} so Boot's Flyway auto-configuration activates transitively
   * whether or not the application asked for it. Sharing the location merged two independent series
   * into one version namespace — an application {@code V1__} and the framework's {@code V001__} are
   * the same {@code MigrationVersion}, so the run aborted with {@code Found more than one migration
   * with version 1} — and an application that numbered around the collision had its own DDL
   * executed by THIS method, under the framework DataSource and the least-privilege credentials
   * {@code docs/guide/production.md} prescribes. Namespacing both is the same fix the crypto series
   * (below) already carries for the same reason.
   *
   * <p>Refuses a server older than PostgreSQL 17 ({@link UnsupportedServerVersionException}) before
   * the first migration, as {@link #create()} does: an application that provisions the schema with
   * this method and builds the store without the factory never reaches {@code create()}'s check.
   *
   * @return this factory for method chaining
   * @throws UnsupportedServerVersionException if the server is older than PostgreSQL 17
   */
  public PostgresEventStoreFactory initializeSchema() {
    PostgresServerVersion.requireSupported(dataSource);
    migrateSchema();
    return this;
  }

  /**
   * The migrations of {@link #initializeSchema()}, after the caller checked the server version.
   *
   * <p>Both series are resolved before either is applied ({@link ShippedMigrationSeries} looks each
   * script up by name, so this also works in a GraalVM native image): a script missing from the
   * class path is refused with nothing written, instead of leaving the event-store series applied
   * and the crypto series not.
   */
  private void migrateSchema() {
    Flyway flyway =
        ShippedMigrationSeries.EVENT_STORE
            .configure(dataSource)
            .table(EVENT_STORE_HISTORY_TABLE)
            // Baseline 0 — apply the WHOLE series, but tolerate a schema that is already non-empty
            // because it holds the APPLICATION's own tables. With the default history
            // table "no history + non-empty schema" could only mean StreamRune's own tables were
            // there; with a namespaced history it also means "the application migrated first", and
            // Flyway refuses that outright unless baselining is allowed. Baselining at 0 skips
            // nothing: every framework migration is greater than 0, so the series still applies in
            // full. It is inert once the history table exists, so it changes nothing on any later
            // run.
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load();
    Flyway cryptoFlyway = null;
    // When the configured crypto engine needs the shared crypto schema
    // (a Postgres-backed key store, or a durable JDBC tombstone store for KMS/Vault), auto-apply
    // the opt-in crypto migration too — on its OWN Flyway history table so that series (which
    // starts at V001, like the eventstore one) can never collide with it — so erasure works out of
    // the box through the shipped auto-config path, not only after the operator manually adds the
    // location.
    if (cryptoEngine != null && !cryptoEngine.requiredCryptoTables().isEmpty()) {
      // The crypto tables can already exist while
      // flyway_schema_history_crypto does not — the application provisioned them itself (the
      // README's copy-the-DDL route), or a tombstone-only KMS/Vault deployment created
      // forgotten_subjects out of band, or an operator pre-created a custom-named key table. The
      // crypto baseline therefore creates each of its tables only when absent (CREATE TABLE IF NOT
      // EXISTS), so ONE configuration fits every case: baseline "0" (tolerate a non-empty schema,
      // skip nothing) and apply the series. Missing tables are created, present ones are left as
      // they are, and SchemaValidator.validateCryptoTables at create() surfaces any column a
      // pre-provisioned table lacks. No classification of the schema happens here, so no
      // misclassification can record a wrong baseline.
      //
      // Every crypto migration after the baseline must likewise be idempotent and
      // presence-tolerant (IF EXISTS / IF NOT EXISTS): it runs against pre-provisioned schemas that
      // may already be fully current.
      cryptoFlyway =
          ShippedMigrationSeries.CRYPTO
              .configure(dataSource)
              .table("flyway_schema_history_crypto")
              .baselineOnMigrate(true)
              .baselineVersion("0")
              .load();
    }
    flyway.migrate();
    if (cryptoFlyway != null) {
      cryptoFlyway.migrate();
    }
  }

  /**
   * Enables or disables automatic schema initialization. When enabled (default), the schema is
   * automatically created when create() is called.
   *
   * <p>The migration runs on the DataSource as given. Behind a transaction-mode connection pooler
   * (PgBouncer) switch it off and apply the shipped series from a job that connects straight to
   * PostgreSQL: Flyway holds one server connection in its lock transaction and needs a second one
   * for the migration, so a pooler with a single server connection can never complete it, and
   * replicas starting together can starve the one holding the lock (see {@code
   * docs/guide/production.md}).
   *
   * @param autoInitialize true to auto-initialize, false to skip
   * @return this factory for method chaining
   */
  public PostgresEventStoreFactory autoInitializeSchema(boolean autoInitialize) {
    this.autoInitializeSchema = autoInitialize;
    return this;
  }

  /**
   * Enables or disables schema validation after migration. When enabled (default), the schema is
   * validated before creating the EventStore.
   *
   * @param validate true to validate schema, false to skip
   * @return this factory for method chaining
   */
  public PostgresEventStoreFactory validateSchema(boolean validate) {
    this.validateSchema = validate;
    return this;
  }

  /**
   * Sets the bound on every statement the created event store runs. Default is 30 seconds: a
   * statement blocked on a lock or running away is cancelled by the server after this long and the
   * operation fails with an {@code EventStoreException}, instead of holding its connection
   * indefinitely. Pass {@link Duration#ZERO} to set no bound of the store's own, so the session's
   * {@code statement_timeout} (from the application's pool or database role) applies unchanged.
   *
   * <p>The bound is applied per transaction ({@code SET LOCAL statement_timeout}) or per statement
   * (the JDBC query timeout), never on the session, so the connection goes back to the
   * application's pool exactly as it was borrowed — see {@link
   * PostgresEventStore.Builder#statementTimeout(Duration)}.
   *
   * <p>A positive value shorter than one millisecond is rejected rather than accepted: the bound is
   * a whole number of milliseconds, such a value truncates to {@code 0}, and {@code 0} is
   * PostgreSQL's spelling of "no timeout at all" — asking for the tightest bound would remove it.
   *
   * @param timeout the per-statement bound; {@link Duration#ZERO} sets none
   * @return this factory for method chaining
   * @throws IllegalArgumentException if timeout is null, negative, positive and shorter than one
   *     millisecond, or longer than 2147483647 ms (the largest {@code statement_timeout})
   */
  public PostgresEventStoreFactory statementTimeout(Duration timeout) {
    StatementTimeout.of(timeout);
    this.statementTimeout = timeout;
    return this;
  }

  /** Sets the crypto engine for encryption (optional). */
  public PostgresEventStoreFactory cryptoEngine(CryptoEngine cryptoEngine) {
    this.cryptoEngine = cryptoEngine;
    return this;
  }

  /**
   * Sets the {@link StreamRuneMetrics} collector threaded into the event store's {@code
   * CryptoShreddingModule}, so a mass crypto-redaction on the replay/projection decrypt path emits
   * {@code streamrune.crypto.subject_redacted} / systemic-failure signals instead of being silent.
   * Optional; defaults to {@link StreamRuneMetrics#NOOP} (null → NOOP).
   *
   * @param metrics the metrics collector, or {@code null} for {@link StreamRuneMetrics#NOOP}
   * @return this factory for method chaining
   */
  public PostgresEventStoreFactory metrics(StreamRuneMetrics metrics) {
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
    return this;
  }

  /** Sets the event upcasters for schema evolution (optional). */
  public PostgresEventStoreFactory upcasters(List<EventUpcaster> upcasters) {
    this.upcasters = upcasters;
    return this;
  }

  /**
   * Registers snapshot migrations applied on load (optional). This is the registration point for
   * the {@link SnapshotMigration} SPI: at startup, register the migrations that upgrade old
   * snapshot schema versions to the current one. On load, when a stored snapshot's schema version
   * differs from the expected version, a complete chain among these steps migrates the snapshot in
   * place (only later events replay); a gapped or absent chain discards the snapshot and replays
   * from events. Validated at {@link #create()} (no duplicate {@code fromVersion}; {@code toVersion
   * > fromVersion}).
   *
   * @param snapshotMigrations the migration steps, in any order (may be {@code null} or empty)
   * @return this factory for method chaining
   */
  public PostgresEventStoreFactory snapshotMigrations(List<SnapshotMigration> snapshotMigrations) {
    this.snapshotMigrations = snapshotMigrations;
    return this;
  }

  /** Sets the event audit store for same-transaction audit writes (optional). */
  public PostgresEventStoreFactory eventAuditStore(PostgresEventAuditStore eventAuditStore) {
    this.eventAuditStore = eventAuditStore;
    return this;
  }

  /** Sets the outbox store for dual-write async message publishing (optional). */
  public PostgresEventStoreFactory outboxStore(PostgresOutboxStore outboxStore) {
    this.outboxStore = outboxStore;
    return this;
  }

  /** Sets the outbox event mapper for message serialization (optional). */
  public PostgresEventStoreFactory outboxEventMapper(OutboxEventMapper outboxEventMapper) {
    this.outboxEventMapper = outboxEventMapper;
    return this;
  }

  /** Sets the command inbox for idempotent appends via {@code appendWithKey} (optional). */
  public PostgresEventStoreFactory commandInbox(PostgresCommandInbox commandInbox) {
    this.commandInbox = commandInbox;
    return this;
  }

  /**
   * Creates the event store: validates the crypto configuration, checks the server version,
   * migrates and validates the schema (each unless switched off), and builds the store on the
   * DataSource as given, with the configured statement bound.
   *
   * @throws IllegalStateException if no crypto engine is set and a registered event or state type
   *     carries {@code @Encrypted} — checked before the database is touched
   * @throws UnsupportedOperationException if no crypto engine is set and the registry does not
   *     implement {@link EventTypeRegistry#registeredTypes()}
   * @throws UnsupportedServerVersionException if the PostgreSQL server is older than {@link
   *     PostgresServerVersion#MINIMUM_MAJOR_VERSION} — checked before anything is migrated, and not
   *     switchable with {@link #autoInitializeSchema(boolean)} or {@link #validateSchema(boolean)}
   * @throws SchemaValidationException if schema validation finds an ERROR-severity issue
   */
  @Override
  public EventStore create() {
    // Fail fast, before touching the database, if any registered event/state type has an
    // @Encrypted field but no CryptoEngine was configured — otherwise Jackson would silently
    // serialize that field as plaintext (CryptoShreddingModule is only registered on the
    // ObjectMapper when cryptoEngine != null). The same check guards PostgresEventStore.create(...)
    // and PostgresEventStore.builder(); with an engine it still runs, to warn about a
    // property-level Jackson serializer override that would bypass encryption.
    PostgresEventStore.validateCryptoConfig(typeRegistry, cryptoEngine);

    // PostgreSQL 17 or newer. Refused before the first migration — an unsupported server is
    // never written to — and independent of the schema flags below, which govern the schema, not
    // the product. Every integration reaches this through the factory.
    PostgresServerVersion.requireSupported(dataSource);

    if (autoInitializeSchema) {
      migrateSchema();
    }

    if (validateSchema) {
      SchemaValidationResult result = SchemaValidator.validate(dataSource);
      // Surface WARNING-severity issues that isValid() intentionally tolerates (e.g. "schema
      // version ahead of expected", "Flyway history table not found"). These are non-fatal but
      // operationally significant — silently discarding them hides real drift, so log each one.
      for (SchemaIssue issue : result.issues()) {
        if (issue.severity() == SchemaIssue.Severity.WARNING) {
          log.warn("Schema validation warning [{}]: {}", issue.table(), issue.message());
        }
      }
      if (!result.isValid()) {
        throw new SchemaValidationException(result);
      }
      // Also validate the crypto-schema tables the active engine requires, with an
      // actionable "apply classpath:db/crypto-migration" message — so a self-managed schema missing
      // that migration fails fast here instead of on the first @Encrypted operation / GDPR forget.
      if (cryptoEngine != null) {
        SchemaValidationResult cryptoResult =
            SchemaValidator.validateCryptoTables(dataSource, cryptoEngine.requiredCryptoTables());
        if (!cryptoResult.isValid()) {
          throw new SchemaValidationException(cryptoResult);
        }
      }
    }

    return PostgresEventStore.builder()
        .dataSource(dataSource)
        .typeRegistry(typeRegistry)
        .cryptoEngine(cryptoEngine)
        .metrics(metrics)
        .upcasters(upcasters)
        .migrations(snapshotMigrations)
        .eventAuditStore(eventAuditStore)
        .outboxStore(outboxStore)
        .outboxEventMapper(outboxEventMapper)
        .commandInbox(commandInbox)
        .statementTimeout(statementTimeout)
        .buildAfterCryptoCheck();
  }

  /**
   * Creates a new {@link JdbcProjectionRepository} on the DataSource as given. Tables are
   * automatically created on first access.
   *
   * <p><b>Crypto-aware, mirroring the auto-configs.</b> The repository's {@link
   * com.fasterxml.jackson.databind.ObjectMapper} is built from THIS factory's own {@link
   * #cryptoEngine(CryptoEngine) cryptoEngine} and {@link #metrics(StreamRuneMetrics) metrics} via
   * {@link JdbcProjectionRepository#createObjectMapper(CryptoEngine, StreamRuneMetrics)} — the same
   * way the factory-equivalent Spring/Quarkus/Micronaut projection-repository beans and the
   * saga/DLQ stores build theirs. So when a {@link CryptoEngine} is configured, an
   * {@code @Encrypted} read-model field is ciphertext at rest and within {@code forget(subjectId)}
   * scope. Previously this method returned the crypto-BLIND 1-arg repository even though the
   * factory held a configured engine, so an {@code @Encrypted} read model built through the factory
   * was written to the {@code data} column as plaintext PII and GDPR-forget never reached it. With
   * no {@code cryptoEngine} the mapper is crypto-blind exactly as before.
   *
   * @return configured JdbcProjectionRepository, crypto-aware iff this factory has a {@link
   *     CryptoEngine}
   */
  public ProjectionRepository createProjectionRepository() {
    return new JdbcProjectionRepository(
        dataSource, JdbcProjectionRepository.createObjectMapper(cryptoEngine, metrics));
  }
}
