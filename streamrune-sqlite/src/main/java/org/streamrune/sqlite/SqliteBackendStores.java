package org.streamrune.sqlite;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.CommandAuditQuery;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendStores;
import org.streamrune.core.backend.EventStoreOptions;
import org.streamrune.core.backend.OutboxOptions;
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.runtime.GuardedTracing;

/**
 * The stores {@link SqliteBackend} opened on one {@link SqliteDatabase}; see {@link SqliteBackend}
 * for what each accessor builds. Every store is built on its accessor's first call, on the one
 * handle the bundle holds, and memoized; the constructor only keeps the context and the handle.
 * After {@link #close()} every store accessor refuses.
 */
final class SqliteBackendStores implements BackendStores {

  /**
   * The packages of the other backends' stores, so the foreign-store refusal can name the backend a
   * store belongs to. A store from any other package is named by its class.
   */
  private static final Map<String, String> BACKEND_PACKAGES =
      Map.of("org.streamrune.postgres", "postgres");

  private final BackendContext context;
  private final SqliteDatabase database;
  private final Clock clock;

  private final Memo<SqliteOffsetStore> offsetStore;
  private final Memo<SqliteReadModelStore> defaultReadModelStore;
  private final Memo<SqliteSagaStore> sagaStore;
  private final Memo<SqliteSagaDeadLetterStore> sagaDeadLetterStore;
  private final Memo<SqliteCommandInbox> commandInbox;
  private final Memo<SqliteDeadLetterQueue> deadLetterQueue;
  private final Memo<SqliteProjectionDeadLetterStore> projectionDeadLetterStore;
  private final Memo<SqliteAuditStore> auditStore;
  private final Memo<SqliteEventAuditStore> eventAuditStore;
  private final Memo<SqliteCommandAuditQuery> commandAuditQuery;
  private final Memo<SqliteEventAuditQuery> eventAuditQuery;
  private final Memo<SqliteComplianceReportQuery> complianceReportQuery;

  private final Map<EventStoreOptions, SqliteEventStoreFactory> eventStoreFactories =
      new ConcurrentHashMap<>();
  private final Map<OutboxOptions, SqliteOutboxStore> outboxStores = new ConcurrentHashMap<>();

  private volatile boolean closed;

  SqliteBackendStores(BackendContext context, SqliteDatabase database) {
    this.context = context;
    this.database = database;
    this.clock = context.clock();
    this.offsetStore = new Memo<>(() -> new SqliteOffsetStore(database, clock));
    this.defaultReadModelStore =
        new Memo<>(
            () ->
                SqliteReadModelStore.create(
                    SqliteBackend.NAME,
                    database,
                    SqliteProjectionRepository.createObjectMapper(
                        context.cryptoEngine(), context.metrics()),
                    context.migrateReadModelSchema(),
                    context.validateSchema(),
                    clock));
    this.sagaStore =
        new Memo<>(
            () ->
                new SqliteSagaStore(
                    database,
                    SqliteSagaStore.createObjectMapper(context.cryptoEngine(), context.metrics()),
                    clock));
    // On the saga store's handle: its shield and hold write the saga store's rows.
    this.sagaDeadLetterStore = new Memo<>(() -> new SqliteSagaDeadLetterStore(database));
    this.commandInbox = new Memo<>(() -> new SqliteCommandInbox(database, clock));
    this.deadLetterQueue = new Memo<>(() -> new SqliteDeadLetterQueue(database, clock));
    this.projectionDeadLetterStore =
        new Memo<>(() -> new SqliteProjectionDeadLetterStore(database));
    this.auditStore = new Memo<>(() -> new SqliteAuditStore(database));
    this.eventAuditStore = new Memo<>(() -> new SqliteEventAuditStore(database));
    this.commandAuditQuery = new Memo<>(() -> new SqliteCommandAuditQuery(database));
    this.eventAuditQuery = new Memo<>(() -> new SqliteEventAuditQuery(database));
    this.complianceReportQuery = new Memo<>(() -> new SqliteComplianceReportQuery(database));
  }

  @Override
  public String backendName() {
    return SqliteBackend.NAME;
  }

  @Override
  public EventStoreFactory eventStoreFactory(EventStoreOptions options) {
    if (options == null) {
      throw new IllegalArgumentException("options is required");
    }
    requireOpen();
    // A refused store throws out of the mapping function, so nothing is memoized for it.
    return eventStoreFactories.computeIfAbsent(options, this::configure);
  }

  /**
   * Configures the factory as the Postgres bundle configures its own. Runs no SQL; the database is
   * reached only by the factory's {@code create()}. The statement timeout is not read: SQLite has
   * none.
   */
  private SqliteEventStoreFactory configure(EventStoreOptions options) {
    SqliteOutboxStore outbox = narrow(options.outboxStore(), SqliteOutboxStore.class, "outbox");
    SqliteCommandInbox inbox =
        narrow(options.commandInbox(), SqliteCommandInbox.class, "command inbox");
    SqliteEventAuditStore audit =
        narrow(options.eventAuditStore(), SqliteEventAuditStore.class, "event audit");

    var factory =
        new SqliteEventStoreFactory(database, options.typeRegistry())
            .autoInitializeSchema(context.migrateSchema())
            .validateSchema(context.validateSchema())
            // The tracing meets the metrics here, as in the Postgres bundle: its guard counts its
            // failures there.
            .tracing(GuardedTracing.of(context.tracing(), context.metrics()))
            .metrics(context.metrics())
            .clock(clock)
            .cryptoEngine(context.cryptoEngine())
            .upcasters(options.upcasters())
            .snapshotMigrations(options.snapshotMigrations())
            .eventAuditStore(audit)
            .commandInbox(inbox);
    if (outbox != null && options.outboxEventMapper() != null) {
      factory.outboxStore(outbox).outboxEventMapper(options.outboxEventMapper());
    }
    return factory;
  }

  /**
   * {@code store} as this backend's own type, or the foreign-store refusal: the event store writes
   * it inside its own append transaction, which only a SQLite store can join.
   */
  private static <T> T narrow(Object store, Class<T> own, String role) {
    if (store == null || own.isInstance(store)) {
      return own.cast(store);
    }
    throw new IllegalArgumentException(
        role
            + " store of backend '"
            + ownerOf(store)
            + "' cannot share the '"
            + SqliteBackend.NAME
            + "' event store's transaction; build it from this backend's BackendStores");
  }

  /** The backend a store belongs to, or its class's simple name (its name, if anonymous). */
  private static String ownerOf(Object store) {
    Class<?> type = store.getClass();
    String pkg = type.getPackageName();
    for (Map.Entry<String, String> backend : BACKEND_PACKAGES.entrySet()) {
      if (pkg.equals(backend.getKey()) || pkg.startsWith(backend.getKey() + ".")) {
        return backend.getValue();
      }
    }
    return type.getSimpleName().isEmpty() ? type.getName() : type.getSimpleName();
  }

  @Override
  public OffsetStore offsetStore() {
    return offsetStore.get();
  }

  @Override
  public ReadModelStore defaultReadModelStore() {
    return defaultReadModelStore.get();
  }

  @Override
  public SagaStore sagaStore() {
    return sagaStore.get();
  }

  @Override
  public SagaDeadLetterStore sagaDeadLetterStore() {
    return sagaDeadLetterStore.get();
  }

  @Override
  public OutboxStore outboxStore(OutboxOptions options) {
    if (options == null) {
      throw new IllegalArgumentException("options is required");
    }
    requireOpen();
    return outboxStores.computeIfAbsent(
        options, o -> new SqliteOutboxStore(database, o.claimLease(), o.orderingMode(), clock));
  }

  @Override
  public CommandInbox commandInbox() {
    return commandInbox.get();
  }

  @Override
  public DeadLetterQueue deadLetterQueue() {
    return deadLetterQueue.get();
  }

  @Override
  public ProjectionDeadLetterStore projectionDeadLetterStore() {
    return projectionDeadLetterStore.get();
  }

  @Override
  public AuditStore auditStore() {
    return auditStore.get();
  }

  @Override
  public EventAuditStore eventAuditStore() {
    return eventAuditStore.get();
  }

  @Override
  public CommandAuditQuery commandAuditQuery() {
    return commandAuditQuery.get();
  }

  @Override
  public EventAuditQuery eventAuditQuery() {
    return eventAuditQuery.get();
  }

  @Override
  public ComplianceReportQuery complianceReportQuery() {
    return complianceReportQuery.get();
  }

  @Override
  public SnapshotPurger snapshotPurger(
      Function<SubjectId, ? extends Collection<StreamId>> streamsForSubject) {
    if (streamsForSubject == null) {
      throw new IllegalArgumentException("streamsForSubject is required");
    }
    requireOpen();
    return new SqliteSnapshotStorePurger(database, streamsForSubject, clock);
  }

  /**
   * {@link SubscriptionLeadership#NOOP} for every positive duration, before and after {@link
   * #close()}: the instance guard keeps a second instance off the file, and NOOP holds nothing and
   * writes nothing, so after {@code close()} it is handed back as {@link BackendStores} documents
   * (already closed), not refused like the stores.
   */
  @Override
  public SubscriptionLeadership leadership(Duration leaseTtl) {
    if (leaseTtl == null || leaseTtl.isZero() || leaseTtl.isNegative()) {
      throw new IllegalArgumentException("leaseTtl must be a positive duration");
    }
    return SubscriptionLeadership.NOOP;
  }

  /**
   * Closes the bundle's handle, which releases its reference to the instance guard. Idempotent: the
   * handle releases its reference once. No store of the bundle is an {@link AutoCloseable}, and the
   * NOOP leadership holds nothing, so the handle is all there is to close.
   *
   * @throws SqliteGuardException if the lock file's descriptor cannot be closed
   */
  @Override
  public void close() {
    closed = true;
    database.close();
  }

  /**
   * Refuses every store after {@link #close()}: the guard is released, so a store handed out now
   * would write without it. A check, then an act: an accessor racing {@code close()} may still hand
   * out a store just after the release.
   */
  private void requireOpen() {
    if (closed) {
      throw new IllegalStateException(
          "The '"
              + SqliteBackend.NAME
              + "' backend's stores on "
              + database.file()
              + " are closed: a store handed out after close() would write without the instance"
              + " guard. Use the stores only before the bundle is closed.");
    }
  }

  /**
   * A value built on the first {@link #get()} and returned by every later one while the bundle is
   * open; refused, built or not, once it is closed.
   */
  private final class Memo<T> {
    private final Supplier<T> factory;
    private volatile T value;

    Memo(Supplier<T> factory) {
      this.factory = factory;
    }

    T get() {
      requireOpen();
      T current = value;
      if (current == null) {
        synchronized (this) {
          current = value;
          if (current == null) {
            current = factory.get();
            value = current;
          }
        }
      }
      return current;
    }
  }
}
