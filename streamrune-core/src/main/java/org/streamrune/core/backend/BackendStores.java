package org.streamrune.core.backend;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.CommandAuditQuery;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.audit.EventAuditStore;
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

/**
 * The stores one {@link StreamRuneBackend} opened over one {@code DataSource}: one explicit
 * accessor per storage port.
 *
 * <p>What every implementation guarantees:
 *
 * <ul>
 *   <li><b>Lazy.</b> Opening the bundle touches no table. A store is built, and reaches the
 *       database (schema migration and validation included), only when its accessor is first
 *       called, so a store the application never asks for never touches the database. The event
 *       store is one step later still: {@link #eventStoreFactory(EventStoreOptions)} touches no
 *       table, and only the factory's {@link EventStoreFactory#create()} does.
 *   <li><b>Memoized.</b> Repeated calls return the same instance: an accessor without parameters
 *       always returns the instance it built first, and {@link
 *       #eventStoreFactory(EventStoreOptions)}, {@link #outboxStore(OutboxOptions)} and {@link
 *       #leadership(Duration)} return the instance they built for an equal argument. {@link
 *       #snapshotPurger(Function)} is the one exception: it builds a new purger on every call.
 *   <li><b>Complete.</b> Every accessor is supported. An accessor may refuse only where its own
 *       documentation says so.
 * </ul>
 *
 * <p>The interface holds no state; the memoization is each implementation's. The bundle has no
 * aggregate locker and no crypto ports. Memoization covers the factory, not what it creates: every
 * {@link EventStoreFactory#create()} of the factory builds a new event store and migrates and
 * validates the schema again, as the context says.
 *
 * <p>The bundle owns what it memoized: {@link #close()} closes every {@link AutoCloseable} it built
 * and memoized, the {@link SubscriptionLeadership} included, and releases what the bundle itself
 * holds, for example a backend's guard on its database file. A consumer never closes a memoized
 * instance itself, since every later call of the accessor would hand back the closed one. It does
 * not close the {@code DataSource}, which belongs to the application. A leadership requested after
 * {@code close()} is returned already closed.
 */
public interface BackendStores extends AutoCloseable {

  /**
   * The name of the backend that opened this bundle ({@link StreamRuneBackend#name()}).
   *
   * @return the backend's name; never blank
   */
  String backendName();

  /**
   * The factory of the event store configured with {@code options}.
   *
   * <p>The call validates the options against this backend and narrows the outbox, command inbox
   * and event audit store to the backend's own types, without touching the database, so a
   * misconfiguration fails when the factory is configured even if {@link
   * EventStoreFactory#create()} never runs. Only {@code create()} touches the database: it migrates
   * and validates the schema (as {@link BackendContext#migrateSchema()} and {@link
   * BackendContext#validateSchema()} say) and builds the store, with {@link
   * BackendContext#cryptoEngine()}.
   *
   * @param options what the event store is configured with; not {@code null}
   * @return the event store factory; the same instance for equal options
   * @throws BackendOptionException if an option value is one this backend cannot honour, for
   *     example a statement timeout it cannot apply; {@link BackendOptionException#option()} names
   *     the {@link EventStoreOptions} component
   * @throws IllegalArgumentException (not a {@code BackendOptionException}) if {@code outboxStore},
   *     {@code commandInbox} or {@code eventAuditStore} is a store that is not this backend's own
   *     type, since the event store writes it in the transaction of its own append. The message
   *     names the role, the store's backend (or its class when no backend owns it) and this
   *     backend, and advises building the store from this backend's {@code BackendStores}
   */
  EventStoreFactory eventStoreFactory(EventStoreOptions options);

  /**
   * The event store's subscription checkpoints.
   *
   * @return the offset store
   */
  OffsetStore offsetStore();

  /**
   * The backend's default read-model store, the one a projection without an explicit store routes
   * to. Its {@link ReadModelStore#name()} is {@link #backendName()}, never {@link
   * ReadModelStore#DEFAULT}.
   *
   * @return the default read-model store
   */
  ReadModelStore defaultReadModelStore();

  /**
   * The saga store.
   *
   * @return the saga store
   */
  SagaStore sagaStore();

  /**
   * The dead letters of sagas.
   *
   * @return the saga dead-letter store
   */
  SagaDeadLetterStore sagaDeadLetterStore();

  /**
   * The transactional outbox built with {@code options}.
   *
   * @param options the claim lease and the ordering mode; not {@code null}
   * @return the outbox store; the same instance for equal options
   */
  OutboxStore outboxStore(OutboxOptions options);

  /**
   * The command inbox an idempotent append claims its key in.
   *
   * @return the command inbox
   */
  CommandInbox commandInbox();

  /**
   * The dead-letter queue of failed commands.
   *
   * @return the dead-letter queue
   */
  DeadLetterQueue deadLetterQueue();

  /**
   * The dead letters of projections.
   *
   * @return the projection dead-letter store
   */
  ProjectionDeadLetterStore projectionDeadLetterStore();

  /**
   * The command audit store.
   *
   * @return the audit store
   */
  AuditStore auditStore();

  /**
   * The event audit store the event store writes audit rows to in its append transaction.
   *
   * @return the event audit store
   */
  EventAuditStore eventAuditStore();

  /**
   * Queries over the command audit trail.
   *
   * @return the command audit query
   */
  CommandAuditQuery commandAuditQuery();

  /**
   * Queries over the event audit trail.
   *
   * @return the event audit query
   */
  EventAuditQuery eventAuditQuery();

  /**
   * The compliance report over the audit trails.
   *
   * @return the compliance report query
   */
  ComplianceReportQuery complianceReportQuery();

  /**
   * The purger that deletes a data subject's aggregate snapshots.
   *
   * @param streamsForSubject maps a subject to every stream whose snapshot may carry its data; not
   *     {@code null}
   * @return a new snapshot purger on every call; the bundle does not memoize it
   */
  SnapshotPurger snapshotPurger(
      Function<SubjectId, ? extends Collection<StreamId>> streamsForSubject);

  /**
   * Subscription leadership through leases of {@code leaseTtl}.
   *
   * @param leaseTtl how long a lease lasts without renewal; positive
   * @return the leadership; the same instance for an equal duration, closed by {@link #close()} and
   *     never by its consumers
   */
  SubscriptionLeadership leadership(Duration leaseTtl);

  /**
   * Closes every {@link AutoCloseable} the bundle built and memoized, the leadership included, and
   * releases what the bundle itself holds. Does nothing by default. Throws no checked exception, so
   * a try-with-resources over the bundle needs no handler.
   */
  @Override
  default void close() {}
}
