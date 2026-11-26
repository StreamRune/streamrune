package org.streamrune.postgres;

import javax.sql.DataSource;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.StreamRuneBackend;

/**
 * The PostgreSQL backend, selected by {@code streamrune.backend=postgres} (the default).
 *
 * <p>Its {@link BackendStores} build exactly what the Spring, Quarkus and Micronaut integrations
 * built before the backend existed, with the same arguments:
 *
 * <ul>
 *   <li>{@link BackendStores#eventStoreFactory eventStoreFactory} returns a {@link
 *       PostgresEventStoreFactory} configured with the context's schema switches ({@link
 *       BackendContext#migrateSchema()} as {@code autoInitializeSchema}, {@link
 *       BackendContext#validateSchema()} as {@code validateSchema}), its tracing (guarded and
 *       counted in its metrics), metrics and crypto engine, and the options' upcasters, snapshot
 *       migrations, statement timeout and outbox, inbox and audit stores. The factory is memoized
 *       per equal options, and every {@link PostgresEventStoreFactory#create() create()} on it
 *       builds a new {@link PostgresEventStore}, checking the server version and migrating and
 *       validating the schema again, as {@code PostgresEventStoreFactory} always did.
 *   <li>{@link BackendStores#sagaStore() sagaStore} is a {@link PostgresSagaStore} whose mapper is
 *       {@link PostgresSagaStore#createObjectMapper(org.streamrune.core.crypto.CryptoEngine,
 *       org.streamrune.core.StreamRuneMetrics)}, and {@link BackendStores#defaultReadModelStore()
 *       defaultReadModelStore} a {@link PostgresReadModelStore} named {@code postgres} whose mapper
 *       is {@link JdbcProjectionRepository#createObjectMapper(
 *       org.streamrune.core.crypto.CryptoEngine, org.streamrune.core.StreamRuneMetrics)}: the two
 *       mappers differ (a saga keeps its timestamps numeric, a read model writes ISO-8601 text),
 *       and both take the context's crypto engine, {@code null} for a crypto-blind mapper.
 *   <li>{@link BackendStores#leadership leadership} is a {@link LeaseBasedLeadership}, which starts
 *       its renew heartbeat when built and is closed by {@link BackendStores#close()}.
 *   <li>Every other store is the Postgres class built from the {@code DataSource} alone, for
 *       example {@link PostgresOffsetStore}, and {@link BackendStores#outboxStore outboxStore} a
 *       {@link PostgresOutboxStore} with the options' claim lease and ordering mode.
 * </ul>
 *
 * <p>The event store writes the outbox, the command inbox and the event audit store on its own
 * append connection, so {@code eventStoreFactory} accepts only {@link PostgresOutboxStore}, {@link
 * PostgresCommandInbox} and {@link PostgresEventAuditStore}, wherever they were built: from this
 * bundle or by the application. Any other store is refused with an {@link IllegalArgumentException}
 * naming its role, the store's backend (or its class when no backend owns it) and {@code postgres}.
 * An outbox store without an outbox event mapper, or a mapper without a store, configures no
 * outbox, as the integrations always did; reporting the half pair is the integration's.
 *
 * <p>A statement timeout PostgreSQL cannot apply (negative, positive and shorter than one
 * millisecond, or longer than {@code statement_timeout} holds) is refused as a {@link
 * org.streamrune.core.backend.BackendOptionException} whose {@code option()} is {@code
 * "statementTimeout"}, with the message and cause {@link
 * PostgresEventStoreFactory#statementTimeout(java.time.Duration)} reports.
 *
 * <p>Opening the bundle runs no SQL; only {@code defaultReadModelStore()} (which migrates and
 * validates the read-model series), the event store factory's {@code create()} and the stores' own
 * operations reach the database. {@link BackendStores#close()} closes every leadership the bundle
 * built and is idempotent; a leadership asked for after {@code close()} is handed back closed, so
 * it never renews. The backend is stateless and thread-safe; so is the bundle.
 */
public final class PostgresBackend implements StreamRuneBackend {

  /** The backend's name: the {@code streamrune.backend} value and the default store's name. */
  static final String NAME = "postgres";

  /** Creates the backend; it holds no state. */
  public PostgresBackend() {}

  @Override
  public String name() {
    return NAME;
  }

  /**
   * Refuses a {@code DataSource} that is not PostgreSQL {@value
   * PostgresServerVersion#MINIMUM_MAJOR_VERSION} or newer, in one connection checkout and without a
   * query.
   *
   * @param dataSource the application's {@code DataSource}
   * @throws IllegalArgumentException if {@code dataSource} is {@code null}
   * @throws IllegalStateException if the database is not PostgreSQL; the message names the {@code
   *     postgres} backend and the database's product and version
   * @throws UnsupportedServerVersionException if the server is older than PostgreSQL {@value
   *     PostgresServerVersion#MINIMUM_MAJOR_VERSION}, as {@link
   *     PostgresServerVersion#requireSupported(DataSource)} reports it
   * @throws org.streamrune.core.EventStoreException if the database cannot be reached
   */
  @Override
  public void verify(DataSource dataSource) {
    if (dataSource == null) {
      throw new IllegalArgumentException("dataSource is required");
    }
    PostgresServerVersion.requirePostgresSupported(dataSource);
  }

  /**
   * Opens the store bundle over the context's {@code DataSource}, running no SQL.
   *
   * @param context the {@code DataSource} and shared settings
   * @return the bundle
   * @throws IllegalArgumentException if {@code context} is {@code null}
   */
  @Override
  public BackendStores open(BackendContext context) {
    if (context == null) {
      throw new IllegalArgumentException("context is required");
    }
    return new PostgresBackendStores(context);
  }
}
