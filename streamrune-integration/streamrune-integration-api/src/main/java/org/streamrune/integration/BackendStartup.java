package org.streamrune.integration;

import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.Internal;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.core.types.LogSanitizer;

/**
 * The startup checks of the selected storage backend that the Spring, Quarkus and Micronaut
 * integrations share, in one place so the three refuse or warn alike: opening the backend's stores
 * ({@link #open}), the statement timeout SQLite ignores ({@link #statementTimeoutIgnored}) and the
 * key-store engine of the other backend ({@link #requireKeyStoreEngineOfBackend}). The leadership
 * default of SQLite lives in {@link SingleActiveConsumerSetting}, which every reader of that
 * property goes through.
 *
 * @hidden
 */
@Internal
public final class BackendStartup {

  /** The statement-timeout property of the event store, {@value}. */
  public static final String STATEMENT_TIMEOUT_PROPERTY =
      "streamrune.event-store.statement-timeout";

  /** The switch of the PostgreSQL key-store engine, {@value}. */
  public static final String POSTGRES_KEY_STORE_PROPERTY = "streamrune.crypto.postgres.enabled";

  /** The switch of the SQLite key-store engine, {@value}. */
  public static final String SQLITE_KEY_STORE_PROPERTY = "streamrune.crypto.sqlite.enabled";

  private static final Logger LOG = LoggerFactory.getLogger(BackendStartup.class);

  private static final String POSTGRES = "postgres";
  private static final String SQLITE = "sqlite";

  private BackendStartup() {}

  /**
   * Opens the backend's stores on the context's {@code DataSource}, as each integration's {@code
   * BackendStores} bean does.
   *
   * <ul>
   *   <li><b>Verify first, unless the backend is {@code postgres}.</b> A backend whose {@code open}
   *       touches the database is verified before it: {@code verify} names the backend and the
   *       database the {@code DataSource} connects to when they disagree (for example {@code
   *       streamrune.backend=sqlite} on a PostgreSQL {@code DataSource}), and checks the connection
   *       setup. The Postgres backend's {@code open} touches nothing, and its stores read the
   *       server's version when they first reach the database; verifying it here would add a
   *       connection borrow to every Postgres application at a point where none happens, so its
   *       checks stay where they are.
   *   <li><b>{@code verify} passing does not mean the application may start.</b> It takes no
   *       instance guard: only {@code open} finds that another process holds the SQLite file, and
   *       it runs right after, in this call.
   *   <li><b>One name.</b> The default read-model store is named after {@code backend.name()} by
   *       some beans and after {@link BackendStores#backendName()} by others, so a bundle that
   *       reports another name is closed and refused.
   * </ul>
   *
   * @param backend the selected backend
   * @param context the {@code DataSource} and shared settings
   * @return the opened stores
   * @throws NullPointerException if an argument is {@code null}
   * @throws IllegalStateException if the bundle reports a name other than the backend's, or as
   *     {@code verify} and {@code open} document
   */
  public static BackendStores open(StreamRuneBackend backend, BackendContext context) {
    Objects.requireNonNull(backend, "backend is required");
    Objects.requireNonNull(context, "context is required");
    if (!POSTGRES.equals(backend.name())) {
      backend.verify(context.dataSource());
    }
    BackendStores stores = backend.open(context);
    if (!backend.name().equals(stores.backendName())) {
      String reported = stores.backendName();
      stores.close();
      throw new IllegalStateException(
          "The '"
              + backend.name()
              + "' backend opened stores that report the backend name '"
              + reported
              + "'; the default read-model store must have one name");
    }
    return stores;
  }

  /**
   * Logs one WARN when the statement timeout is configured for a backend that cannot honour it:
   * SQLite has no statement timeout (the driver's query timeout only shortens {@code busy_timeout}
   * for a while and does not stop a statement that is computing), so the value is ignored there and
   * waiting for the write lock stays bounded by the connection's {@code busy_timeout}.
   *
   * @param backend the parsed backend name
   * @param configured the property's value as the application configured it, or {@code null} when
   *     it is not set (its default applies, and nothing is logged)
   * @return the WARN text when one was logged
   */
  public static Optional<String> statementTimeoutIgnored(String backend, String configured) {
    if (!SQLITE.equals(backend) || configured == null) {
      return Optional.empty();
    }
    String message =
        STATEMENT_TIMEOUT_PROPERTY
            + "="
            + LogSanitizer.sanitizeForLog(configured)
            + " is ignored on SQLite: SQLite has no statement timeout. Waiting for the write lock"
            + " stays bounded by the connection's busy_timeout; remove the property.";
    LOG.warn(message);
    return Optional.of(message);
  }

  /**
   * Refuses a key-store engine of another backend: the backend's own key-store engine keeps its
   * keys in that backend's database, so {@value #POSTGRES_KEY_STORE_PROPERTY} needs {@code
   * streamrune.backend=postgres} and {@value #SQLITE_KEY_STORE_PROPERTY} needs {@code sqlite}.
   *
   * @param backend the parsed backend name
   * @param postgresKeyStore whether {@value #POSTGRES_KEY_STORE_PROPERTY} is on
   * @param sqliteKeyStore whether {@value #SQLITE_KEY_STORE_PROPERTY} is on
   * @throws IllegalStateException if an enabled key-store engine is not the backend's
   */
  public static void requireKeyStoreEngineOfBackend(
      String backend, boolean postgresKeyStore, boolean sqliteKeyStore) {
    if (postgresKeyStore && !POSTGRES.equals(backend)) {
      throw otherBackendsEngine(POSTGRES_KEY_STORE_PROPERTY, "PostgreSQL", POSTGRES, backend);
    }
    if (sqliteKeyStore && !SQLITE.equals(backend)) {
      throw otherBackendsEngine(SQLITE_KEY_STORE_PROPERTY, "SQLite", SQLITE, backend);
    }
  }

  private static IllegalStateException otherBackendsEngine(
      String property, String database, String engineBackend, String backend) {
    return new IllegalStateException(
        property
            + "=true enables the "
            + database
            + " key-store engine, but "
            + BackendSelection.PROPERTY
            + "="
            + backend
            + ": the key-store engine keeps its keys in the selected backend's database. Set "
            + "streamrune.crypto."
            + backend
            + ".enabled=true instead, or select "
            + BackendSelection.PROPERTY
            + "="
            + engineBackend
            + ".");
  }
}
