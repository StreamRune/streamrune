package org.streamrune.postgres;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.CommandAuditQuery;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.audit.EventAuditStore;
import org.streamrune.core.backend.BackendContext;
import org.streamrune.core.backend.BackendOptionException;
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
 * The stores {@link PostgresBackend} opened over one {@code DataSource}; see {@link
 * PostgresBackend} for what each accessor builds. Every store is built on its accessor's first call
 * and memoized; the constructor only keeps the context.
 */
final class PostgresBackendStores implements BackendStores {

  /**
   * The packages of the other backends' stores, so the foreign-store refusal can name the backend a
   * store belongs to. A store from any other package is named by its class.
   */
  private static final Map<String, String> BACKEND_PACKAGES =
      Map.of("org.streamrune.sqlite", "sqlite");

  private final BackendContext context;
  private final DataSource dataSource;

  private final Memo<PostgresOffsetStore> offsetStore;
  private final Memo<PostgresReadModelStore> defaultReadModelStore;
  private final Memo<PostgresSagaStore> sagaStore;
  private final Memo<PostgresSagaDeadLetterStore> sagaDeadLetterStore;
  private final Memo<PostgresCommandInbox> commandInbox;
  private final Memo<PostgresDeadLetterQueue> deadLetterQueue;
  private final Memo<PostgresProjectionDeadLetterStore> projectionDeadLetterStore;
  private final Memo<PostgresAuditStore> auditStore;
  private final Memo<PostgresEventAuditStore> eventAuditStore;
  private final Memo<PostgresCommandAuditQuery> commandAuditQuery;
  private final Memo<PostgresEventAuditQuery> eventAuditQuery;
  private final Memo<PostgresComplianceReportQuery> complianceReportQuery;

  private final Map<EventStoreOptions, PostgresEventStoreFactory> eventStoreFactories =
      new ConcurrentHashMap<>();
  private final Map<OutboxOptions, PostgresOutboxStore> outboxStores = new ConcurrentHashMap<>();
  private final Map<Duration, LeaseBasedLeadership> leaderships = new ConcurrentHashMap<>();

  private volatile boolean closed;

  PostgresBackendStores(BackendContext context) {
    this.context = context;
    this.dataSource = context.dataSource();
    this.offsetStore = new Memo<>(() -> new PostgresOffsetStore(dataSource));
    this.defaultReadModelStore =
        new Memo<>(
            () ->
                PostgresReadModelStore.create(
                    PostgresBackend.NAME,
                    dataSource,
                    JdbcProjectionRepository.createObjectMapper(
                        context.cryptoEngine(), context.metrics()),
                    context.migrateReadModelSchema(),
                    context.validateSchema()));
    this.sagaStore =
        new Memo<>(
            () ->
                new PostgresSagaStore(
                    dataSource,
                    PostgresSagaStore.createObjectMapper(
                        context.cryptoEngine(), context.metrics())));
    this.sagaDeadLetterStore = new Memo<>(() -> new PostgresSagaDeadLetterStore(dataSource));
    this.commandInbox = new Memo<>(() -> new PostgresCommandInbox(dataSource));
    this.deadLetterQueue = new Memo<>(() -> new PostgresDeadLetterQueue(dataSource));
    this.projectionDeadLetterStore =
        new Memo<>(() -> new PostgresProjectionDeadLetterStore(dataSource));
    this.auditStore = new Memo<>(() -> new PostgresAuditStore(dataSource));
    this.eventAuditStore = new Memo<>(() -> new PostgresEventAuditStore(dataSource));
    this.commandAuditQuery = new Memo<>(() -> new PostgresCommandAuditQuery(dataSource));
    this.eventAuditQuery = new Memo<>(() -> new PostgresEventAuditQuery(dataSource));
    this.complianceReportQuery = new Memo<>(() -> new PostgresComplianceReportQuery(dataSource));
  }

  @Override
  public String backendName() {
    return PostgresBackend.NAME;
  }

  @Override
  public EventStoreFactory eventStoreFactory(EventStoreOptions options) {
    if (options == null) {
      throw new IllegalArgumentException("options is required");
    }
    // A refused option throws out of the mapping function, so nothing is memoized for it.
    return eventStoreFactories.computeIfAbsent(options, this::configure);
  }

  /**
   * Configures the factory the way the integrations did: the same setters, in the same cases. Runs
   * no SQL; the database is reached only by the factory's {@code create()}.
   */
  private PostgresEventStoreFactory configure(EventStoreOptions options) {
    PostgresOutboxStore outbox = narrow(options.outboxStore(), PostgresOutboxStore.class, "outbox");
    PostgresCommandInbox inbox =
        narrow(options.commandInbox(), PostgresCommandInbox.class, "command inbox");
    PostgresEventAuditStore audit =
        narrow(options.eventAuditStore(), PostgresEventAuditStore.class, "event audit");

    var factory =
        new PostgresEventStoreFactory(dataSource, options.typeRegistry())
            .autoInitializeSchema(context.migrateSchema())
            .validateSchema(context.validateSchema())
            // The tracing meets the metrics here, as in the integrations: its guard counts its
            // failures there.
            .tracing(GuardedTracing.of(context.tracing(), context.metrics()))
            .metrics(context.metrics());
    if (options.statementTimeout() != null) {
      try {
        factory.statementTimeout(options.statementTimeout());
      } catch (IllegalArgumentException e) {
        throw new BackendOptionException("statementTimeout", e.getMessage(), e);
      }
    }
    if (context.cryptoEngine() != null) {
      factory.cryptoEngine(context.cryptoEngine());
    }
    if (!options.upcasters().isEmpty()) {
      factory.upcasters(options.upcasters());
    }
    if (!options.snapshotMigrations().isEmpty()) {
      factory.snapshotMigrations(options.snapshotMigrations());
    }
    if (audit != null) {
      factory.eventAuditStore(audit);
    }
    if (outbox != null && options.outboxEventMapper() != null) {
      factory.outboxStore(outbox).outboxEventMapper(options.outboxEventMapper());
    }
    if (inbox != null) {
      factory.commandInbox(inbox);
    }
    return factory;
  }

  /**
   * {@code store} as this backend's own type, or the R1 refusal: the event store writes it on its
   * own append connection, which only a Postgres store can join.
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
            + PostgresBackend.NAME
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
    return outboxStores.computeIfAbsent(
        options, o -> new PostgresOutboxStore(dataSource, o.claimLease(), o.orderingMode()));
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
    return new PostgresSnapshotStorePurger(dataSource, streamsForSubject);
  }

  @Override
  public SubscriptionLeadership leadership(Duration leaseTtl) {
    if (leaseTtl == null) {
      // LeaseBasedLeadership's own message for a missing TTL; the map takes no null key.
      throw new IllegalArgumentException("leaseTtl must be a positive duration");
    }
    LeaseBasedLeadership leadership =
        leaderships.computeIfAbsent(leaseTtl, ttl -> new LeaseBasedLeadership(dataSource, ttl));
    if (closed) {
      // Built after close(): hand it back closed, like every other leadership of this bundle, so
      // no heartbeat outlives the bundle. LeaseBasedLeadership.close() is idempotent.
      leadership.close();
    }
    return leadership;
  }

  /**
   * Closes every leadership the bundle built, the only {@link AutoCloseable} among its stores.
   * Idempotent: {@link LeaseBasedLeadership#close()} closes once and resigns its leases, logging a
   * resign the database refuses instead of throwing.
   */
  @Override
  public void close() {
    closed = true;
    leaderships.values().forEach(LeaseBasedLeadership::close);
  }

  /** A value built on the first {@link #get()} and returned by every later one. */
  private static final class Memo<T> {
    private final Supplier<T> factory;
    private volatile T value;

    Memo(Supplier<T> factory) {
      this.factory = factory;
    }

    T get() {
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
