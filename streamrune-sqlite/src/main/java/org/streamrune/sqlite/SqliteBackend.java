package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.LogSanitizer;

/**
 * The SQLite backend, selected by {@code streamrune.backend=sqlite}, for a small service that runs
 * as one instance on one database file.
 *
 * <p>{@link #open(BackendContext)} opens one {@link SqliteDatabase} on the context's {@code
 * DataSource}, and every store of the bundle is built on that one handle, so all of them work on
 * one file under one instance guard. The saga store and the saga dead-letter store in particular
 * must share a file, because the dead-letter store writes its entry and the shield on the saga's
 * {@code saga_state} row in one transaction; the bundle gives no way to build them on different
 * handles. What each accessor builds:
 *
 * <ul>
 *   <li>{@link BackendStores#eventStoreFactory eventStoreFactory} returns a {@link
 *       SqliteEventStoreFactory} configured with the context's schema switches ({@link
 *       BackendContext#migrateSchema()} as {@code autoInitializeSchema}, {@link
 *       BackendContext#validateSchema()} as {@code validateSchema}), its tracing (guarded and
 *       counted in its metrics), metrics, clock and crypto engine, and the options' upcasters,
 *       snapshot migrations and outbox, inbox and audit stores. The factory is memoized per equal
 *       options, and every {@link SqliteEventStoreFactory#create() create()} on it builds a new
 *       {@link SqliteEventStore}, migrating and validating the schema again, as on Postgres.
 *   <li>{@link BackendStores#sagaStore() sagaStore} is a {@link SqliteSagaStore} whose mapper is
 *       {@link SqliteSagaStore#createObjectMapper(org.streamrune.core.crypto.CryptoEngine,
 *       org.streamrune.core.StreamRuneMetrics)}, and {@link BackendStores#defaultReadModelStore()
 *       defaultReadModelStore} a {@link SqliteReadModelStore} named {@code sqlite} whose mapper is
 *       {@link SqliteProjectionRepository#createObjectMapper(
 *       org.streamrune.core.crypto.CryptoEngine, org.streamrune.core.StreamRuneMetrics)}: a saga
 *       keeps its timestamps numeric, a read model writes ISO-8601 text, and both take the
 *       context's crypto engine, {@code null} for a crypto-blind mapper. The read-model store
 *       applies and validates the read-model series as {@link
 *       BackendContext#migrateReadModelSchema()} and {@link BackendContext#validateSchema()} say,
 *       when it is first asked for.
 *   <li>{@link BackendStores#leadership leadership} is {@link SubscriptionLeadership#NOOP} for
 *       every lease duration, before and after {@link BackendStores#close()}: the instance guard
 *       already keeps a second instance off the file, and NOOP holds nothing, so closing it is a
 *       no-op and a leadership asked for after {@code close()} is as closed as it can be.
 *   <li>Every other store is the SQLite class built on the handle alone, with the context's clock
 *       where the class takes one, for example {@link SqliteOffsetStore}, and {@link
 *       BackendStores#outboxStore outboxStore} a {@link SqliteOutboxStore} with the options' claim
 *       lease and ordering mode.
 * </ul>
 *
 * <p>The event store writes the outbox, the command inbox and the event audit store inside its own
 * append transaction, so {@code eventStoreFactory} accepts only {@link SqliteOutboxStore}, {@link
 * SqliteCommandInbox} and {@link SqliteEventAuditStore}, wherever they were built: from this bundle
 * or by the application. Any other store is refused with an {@link IllegalArgumentException} naming
 * its role, the store's backend (or its class when no backend owns it) and {@code sqlite}. A SQLite
 * store built over another database file is refused by the factory's {@code create()}. An outbox
 * store without an outbox event mapper, or a mapper without a store, configures no outbox, as on
 * Postgres.
 *
 * <p>SQLite has no statement timeout (a write waits for the write lock at most the connection's
 * {@code busy_timeout}), so the options' statement timeout is ignored, whatever its value; no other
 * option is one this backend cannot honour.
 *
 * <p>Opening the bundle borrows one connection to check the setup ({@link
 * SqliteDatabase#open(DataSource)}) and takes the instance guard of the file; it runs no schema
 * SQL. Only {@code defaultReadModelStore()}, the event store factory's {@code create()} and the
 * stores' own operations reach the tables. {@link BackendStores#close()} closes the handle, which
 * releases the bundle's reference to the guard, and is idempotent. None of the stores holds
 * anything to close of its own. A store already built keeps the closed handle, and must not be used
 * after {@code close()}; building the event store or the default read-model store on a closed
 * bundle is refused with an {@link IllegalStateException}. The backend is stateless and
 * thread-safe; so is the bundle.
 */
public final class SqliteBackend implements StreamRuneBackend {

  /** The backend's name: the {@code streamrune.backend} value and the default store's name. */
  static final String NAME = "sqlite";

  /** The product name the SQLite driver reports. */
  private static final String PRODUCT_NAME = "SQLite";

  /** Creates the backend; it holds no state. */
  public SqliteBackend() {}

  @Override
  public String name() {
    return NAME;
  }

  /**
   * Refuses a {@code DataSource} that is not a SQLite database StreamRune can run on, in one
   * connection checkout: the product, then every check {@link SqliteDatabase#open(DataSource)} runs
   * before it takes the guard (a database file rather than an in-memory one, no attached database,
   * the SQLite library version, the connection settings, autocommit connections). It reads only and
   * takes no instance guard, so it can run at startup before the bundle is opened; whether another
   * process holds the guard is found when the bundle opens.
   *
   * @param dataSource the application's {@code DataSource}
   * @throws IllegalArgumentException if {@code dataSource} is {@code null}
   * @throws IllegalStateException if the database is not SQLite (the message names the {@code
   *     sqlite} backend and the database's product and version), or a check of {@link
   *     SqliteDatabase#open(DataSource)} refuses it
   * @throws EventStoreException if no connection can be borrowed or read
   */
  @Override
  public void verify(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    try (Connection connection = dataSource.getConnection()) {
      DatabaseMetaData meta = connection.getMetaData();
      String product = meta.getDatabaseProductName();
      if (!PRODUCT_NAME.equalsIgnoreCase(product)) {
        throw new IllegalStateException(
            "The '"
                + NAME
                + "' backend (streamrune.backend=sqlite) needs a SQLite database, but the"
                + " DataSource connects to "
                + LogSanitizer.sanitizeForLog(product)
                + " "
                + LogSanitizer.sanitizeForLog(meta.getDatabaseProductVersion())
                + ". Point the DataSource at a SQLite database file, or select the backend of this"
                + " database.");
      }
      SqliteDatabase.checkSetup(connection);
    } catch (SQLException e) {
      throw new EventStoreException(
          "Cannot verify the SQLite database: " + LogSanitizer.sanitizeFreeText(e.getMessage()), e);
    }
  }

  /**
   * Opens the store bundle: opens one {@link SqliteDatabase} on the context's {@code DataSource},
   * which checks the setup and takes the instance guard, and runs no schema SQL.
   *
   * @param context the {@code DataSource} and shared settings
   * @return the bundle; closing it closes the handle
   * @throws IllegalArgumentException if {@code context} is {@code null}
   * @throws IllegalStateException if {@link SqliteDatabase#open(DataSource)} refuses the database
   * @throws SqliteGuardException if another process holds the guard, or it cannot be taken
   * @throws EventStoreException if no connection can be borrowed or read
   */
  @Override
  public BackendStores open(BackendContext context) {
    if (context == null) {
      throw new IllegalArgumentException("context is required");
    }
    return new SqliteBackendStores(context, SqliteDatabase.open(context.dataSource()));
  }
}
