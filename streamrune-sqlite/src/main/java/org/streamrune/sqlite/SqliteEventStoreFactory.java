package org.streamrune.sqlite;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SnapshotMigration;
import org.streamrune.core.SnapshotMigrationChain;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.core.upcasting.UpcasterChain;
import org.streamrune.runtime.GuardedTracing;

/**
 * Factory for {@link SqliteEventStore}: applies and validates the schema on an open {@link
 * SqliteDatabase} and builds the store over it.
 *
 * <pre>{@code
 * SqliteDatabase database = SqliteDatabase.open(dataSource);
 * EventStore store = new SqliteEventStoreFactory(database, typeRegistry).create();
 * }</pre>
 *
 * <p>The setters mirror {@code PostgresEventStoreFactory}'s, with two differences. There is no
 * statement timeout: SQLite has none, and a write waits for the write lock for at most the
 * connection's {@code busy_timeout}. And {@link #clock(Clock)} sets the clock that stamps {@code
 * created_at}, which on Postgres the database's clock stamps.
 *
 * <p>The outbox, the command inbox and the event audit store are this backend's own stores ({@link
 * SqliteOutboxStore}, {@link SqliteCommandInbox}, {@link SqliteEventAuditStore}), built over the
 * same database file: the store writes their rows on its own append connection, inside its append
 * transaction, so a store of another type could not join it, and one over another file would keep
 * rows the relay, the inbox or the audit queries never read.
 *
 * <p>The database handle is used as given and not closed by the factory or the stores: close it
 * after every store built over it has stopped.
 *
 * <p><b>final by design.</b> Configuration goes through the fluent setters; compose or implement
 * {@link EventStoreFactory} instead of subclassing.
 */
public final class SqliteEventStoreFactory implements EventStoreFactory {

  private final SqliteDatabase database;
  private final EventTypeRegistry typeRegistry;
  private CryptoEngine cryptoEngine;
  private StreamRuneMetrics metrics = StreamRuneMetrics.NOOP;
  private List<EventUpcaster> upcasters = List.of();
  private List<SnapshotMigration> snapshotMigrations;
  private boolean autoInitializeSchema = true;
  private boolean validateSchema = true;
  private StreamRuneTracing tracing = StreamRuneTracing.NOOP;
  private Clock clock = Clock.systemUTC();
  private SqliteOutboxStore outboxStore;
  private OutboxEventMapper outboxEventMapper;
  private SqliteCommandInbox commandInbox;
  private SqliteEventAuditStore eventAuditStore;

  /**
   * Creates a new factory.
   *
   * @param database the open database the store reads and writes (required)
   * @param typeRegistry event/state type registry used by the store (required)
   * @throws IllegalArgumentException if either argument is {@code null}
   */
  public SqliteEventStoreFactory(SqliteDatabase database, EventTypeRegistry typeRegistry) {
    if (database == null) {
      throw new IllegalArgumentException("database is required");
    }
    if (typeRegistry == null) {
      throw new IllegalArgumentException("typeRegistry is required");
    }
    this.database = database;
    this.typeRegistry = typeRegistry;
  }

  /**
   * Enables or disables applying the event-store schema series ({@link
   * SqliteSchemaSeries#EVENT_STORE}, and {@link SqliteSchemaSeries#CRYPTO} when the crypto engine
   * needs crypto tables) when {@link #create()} runs. Enabled by default.
   *
   * @param autoInitialize true to apply the series, false to skip
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory autoInitializeSchema(boolean autoInitialize) {
    this.autoInitializeSchema = autoInitialize;
    return this;
  }

  /**
   * Enables or disables validating the schema before the store is built. Enabled by default.
   *
   * @param validate true to validate, false to skip
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory validateSchema(boolean validate) {
    this.validateSchema = validate;
    return this;
  }

  /**
   * The tracing the store starts its spans through when its {@link TracingArea#EVENT_STORE} area is
   * on. Optional; defaults to {@link StreamRuneTracing#NOOP}, which traces nothing.
   *
   * @param tracing the tracing; not {@code null}
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory tracing(StreamRuneTracing tracing) {
    this.tracing = Objects.requireNonNull(tracing, "tracing is required");
    return this;
  }

  /**
   * Sets the crypto engine of {@code @Encrypted} fields. Optional only when no registered event or
   * state type carries {@code @Encrypted}; {@link #create()} refuses such a type without one.
   *
   * @param cryptoEngine the engine, or {@code null} for none
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory cryptoEngine(CryptoEngine cryptoEngine) {
    this.cryptoEngine = cryptoEngine;
    return this;
  }

  /**
   * Sets the metrics collector threaded into the store's crypto module and its snapshot-discard
   * counter. Optional; defaults to {@link StreamRuneMetrics#NOOP}.
   *
   * @param metrics the metrics collector; not {@code null}
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory metrics(StreamRuneMetrics metrics) {
    this.metrics = Objects.requireNonNull(metrics, "metrics is required");
    return this;
  }

  /**
   * Sets the event upcasters for schema evolution. Optional; the default is none.
   *
   * @param upcasters the upcasters, empty for none; not {@code null}
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory upcasters(List<EventUpcaster> upcasters) {
    this.upcasters = Objects.requireNonNull(upcasters, "upcasters is required");
    return this;
  }

  /**
   * Registers snapshot migrations applied on load. Validated at {@link #create()} (no duplicate
   * {@code fromVersion}; {@code toVersion > fromVersion}).
   *
   * @param snapshotMigrations the migration steps, in any order (may be {@code null} or empty)
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory snapshotMigrations(List<SnapshotMigration> snapshotMigrations) {
    this.snapshotMigrations = snapshotMigrations;
    return this;
  }

  /**
   * Sets the clock that stamps {@code created_at} of event and snapshot rows. Optional; defaults to
   * {@link Clock#systemUTC()}.
   *
   * @param clock the clock; not {@code null}
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory clock(Clock clock) {
    this.clock = Objects.requireNonNull(clock, "clock is required");
    return this;
  }

  /**
   * Sets the outbox the store writes the entries of each append to, in the append's transaction.
   * Optional; set together with {@link #outboxEventMapper}, or neither.
   *
   * @param outboxStore the outbox, over the same database file, or {@code null} for none
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory outboxStore(SqliteOutboxStore outboxStore) {
    this.outboxStore = outboxStore;
    return this;
  }

  /**
   * Sets the mapper that derives the outbox entries of each event of an {@code append(StreamId,
   * List, Version)} and an {@code appendWithKey}. Optional; set together with {@link #outboxStore},
   * or neither. {@link SqliteEventStore#append(org.streamrune.core.types.StreamId, List,
   * org.streamrune.core.types.Version, List)} writes only the entries given to it; to write those
   * alone, set a mapper that returns no entries.
   *
   * @param outboxEventMapper the mapper, or {@code null} for none
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory outboxEventMapper(OutboxEventMapper outboxEventMapper) {
    this.outboxEventMapper = outboxEventMapper;
    return this;
  }

  /**
   * Sets the command inbox {@code appendWithKey} claims its key in, in the append's transaction.
   * Optional; without one {@code appendWithKey} throws {@link UnsupportedOperationException}.
   *
   * @param commandInbox the inbox, over the same database file, or {@code null} for none
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory commandInbox(SqliteCommandInbox commandInbox) {
    this.commandInbox = commandInbox;
    return this;
  }

  /**
   * Sets the event audit store the store writes one row per appended event to, in the append's
   * transaction. Optional; without one no event audit row is written.
   *
   * @param eventAuditStore the event audit store, over the same database file, or {@code null} for
   *     none
   * @return this factory for method chaining
   */
  public SqliteEventStoreFactory eventAuditStore(SqliteEventAuditStore eventAuditStore) {
    this.eventAuditStore = eventAuditStore;
    return this;
  }

  /**
   * Creates a new event store: refuses an {@code @Encrypted} type without a crypto engine before it
   * touches the database, applies the schema series and validates them (each unless switched off),
   * and builds the store. Every call builds a new store over the same database.
   *
   * @throws IllegalStateException if exactly one of the outbox store and the outbox mapper is set,
   *     if no crypto engine is set and a registered event or state type carries {@code @Encrypted},
   *     or if schema validation finds an error (a missing table or column, a missing crypto table
   *     the engine needs)
   * @throws UnsupportedOperationException if no crypto engine is set and the registry does not
   *     implement {@link EventTypeRegistry#registeredTypes()}
   * @throws IllegalArgumentException if the snapshot migrations are inconsistent, or the outbox
   *     store, the command inbox or the event audit store is built over another database file
   */
  @Override
  public EventStore create() {
    // Configuration errors, refused before the database is touched.
    if ((outboxStore == null) != (outboxEventMapper == null)) {
      throw new IllegalStateException(
          "outboxStore and outboxEventMapper must be set together (or neither). To write only"
              + " the entries passed to append(..., outboxEntries), set a mapper that returns no"
              + " entries.");
    }
    if (outboxStore != null) {
      requireSameFile("outboxStore", outboxStore.file());
    }
    if (commandInbox != null) {
      requireSameFile("commandInbox", commandInbox.file());
    }
    if (eventAuditStore != null) {
      requireSameFile("eventAuditStore", eventAuditStore.file());
    }
    SqliteEventStore.validateCryptoConfig(typeRegistry, cryptoEngine);
    // Validated before anything is migrated: a bad migration set is a configuration error.
    SnapshotMigrationChain migrationChain = SnapshotMigrationChain.of(snapshotMigrations);
    UpcasterChain upcasterChain = new UpcasterChain(upcasters);
    Set<String> cryptoTables =
        cryptoEngine == null ? Set.of() : cryptoEngine.requiredCryptoTables();

    if (autoInitializeSchema) {
      database.migrate(SqliteSchemaSeries.EVENT_STORE);
      if (!cryptoTables.isEmpty()) {
        database.migrate(SqliteSchemaSeries.CRYPTO);
      }
    }
    if (validateSchema) {
      database.validate(SqliteSchemaSeries.EVENT_STORE);
      requireCryptoTables(cryptoTables);
    }

    return new SqliteEventStore(
        database,
        SqliteEventStore.createObjectMapper(cryptoEngine, metrics),
        typeRegistry,
        upcasterChain,
        migrationChain,
        metrics,
        GuardedTracing.of(tracing, metrics),
        clock,
        outboxStore,
        outboxEventMapper,
        commandInbox,
        eventAuditStore);
  }

  /**
   * Refuses a store over another file than the event store's: its rows would be written into the
   * event store's file, inside the append, and read from the other one.
   */
  private void requireSameFile(String role, Path file) {
    if (!file.equals(database.file())) {
      throw new IllegalArgumentException(
          role
              + " is built over "
              + file
              + ", but the event store over "
              + database.file()
              + ": the store writes it inside its append transaction, so both must be built over"
              + " the same database file.");
    }
  }

  /**
   * Refuses a database without a table the crypto engine needs, naming each, so a missing crypto
   * series fails here instead of on the first {@code @Encrypted} operation or GDPR forget.
   */
  private void requireCryptoTables(Set<String> required) {
    if (required.isEmpty()) {
      return;
    }
    List<String> missing = new ArrayList<>();
    try (Connection conn = database.dataSource().getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT 1 FROM pragma_table_list WHERE schema = 'main' AND name = ?"
                    + " COLLATE NOCASE")) {
      for (String table : new TreeSet<>(required)) {
        ps.setString(1, table);
        try (var rs = ps.executeQuery()) {
          if (!rs.next()) {
            missing.add(table);
          }
        }
      }
    } catch (SQLException e) {
      throw new EventStoreException("Cannot check the crypto tables of " + database, e);
    }
    if (!missing.isEmpty()) {
      throw new IllegalStateException(
          "Schema validation failed: the crypto engine needs the table(s) "
              + missing
              + ", which "
              + database
              + " does not have. Apply the SQLite crypto schema series"
              + " (SqliteSchemaSeries.CRYPTO, whose scripts streamrune-sqlite-crypto ships), or"
              + " create the tables before the event store is built.");
    }
  }
}
