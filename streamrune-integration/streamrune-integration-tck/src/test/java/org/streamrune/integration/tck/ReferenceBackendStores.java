package org.streamrune.integration.tck;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.CommandAuditQuery;
import org.streamrune.core.audit.ComplianceReportQuery;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.audit.EventAuditStore;
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
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The stores {@link ReferenceApplication} opens on its database, named {@code postgres} as the
 * default backend: the test kit's in-memory ones, memoized as a backend memoizes them. It holds
 * only the stores the contract reads; every other accessor refuses.
 */
final class ReferenceBackendStores implements BackendStores {

  private final OffsetStore offsetStore = new InMemoryOffsetStore();
  private final ReadModelStore defaultReadModelStore;
  private final Map<OutboxOptions, OutboxStore> outboxStores = new ConcurrentHashMap<>();

  ReferenceBackendStores() {
    InMemoryProjectionRepository repository = new InMemoryProjectionRepository();
    defaultReadModelStore = ReadModelStore.of("postgres", repository, repository, repository);
  }

  @Override
  public String backendName() {
    return "postgres";
  }

  @Override
  public OffsetStore offsetStore() {
    return offsetStore;
  }

  @Override
  public ReadModelStore defaultReadModelStore() {
    return defaultReadModelStore;
  }

  @Override
  public OutboxStore outboxStore(OutboxOptions options) {
    return outboxStores.computeIfAbsent(
        options, o -> new InMemoryOutboxStore(Clock.systemUTC(), o.claimLease(), o.orderingMode()));
  }

  @Override
  public EventStoreFactory eventStoreFactory(EventStoreOptions options) {
    throw refused("an event store factory");
  }

  @Override
  public SagaStore sagaStore() {
    throw refused("a saga store");
  }

  @Override
  public SagaDeadLetterStore sagaDeadLetterStore() {
    throw refused("a saga dead-letter store");
  }

  @Override
  public CommandInbox commandInbox() {
    throw refused("a command inbox");
  }

  @Override
  public DeadLetterQueue deadLetterQueue() {
    throw refused("a dead-letter queue");
  }

  @Override
  public ProjectionDeadLetterStore projectionDeadLetterStore() {
    throw refused("a projection dead-letter store");
  }

  @Override
  public AuditStore auditStore() {
    throw refused("an audit store");
  }

  @Override
  public EventAuditStore eventAuditStore() {
    throw refused("an event audit store");
  }

  @Override
  public CommandAuditQuery commandAuditQuery() {
    throw refused("a command audit query");
  }

  @Override
  public EventAuditQuery eventAuditQuery() {
    throw refused("an event audit query");
  }

  @Override
  public ComplianceReportQuery complianceReportQuery() {
    throw refused("a compliance report query");
  }

  @Override
  public SnapshotPurger snapshotPurger(
      Function<SubjectId, ? extends Collection<StreamId>> streamsForSubject) {
    throw refused("a snapshot purger");
  }

  @Override
  public SubscriptionLeadership leadership(Duration leaseTtl) {
    throw refused("a leadership");
  }

  private static UnsupportedOperationException refused(String store) {
    return new UnsupportedOperationException(
        "the reference application's backend builds no " + store);
  }
}
