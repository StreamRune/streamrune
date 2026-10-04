package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.opentelemetry.api.OpenTelemetry;
import io.quarkus.security.identity.SecurityIdentity;
import jakarta.enterprise.inject.Instance;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.QueryBus;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.types.EventType;
import org.streamrune.runtime.CachingUserRoleResolver;
import org.streamrune.runtime.DeadLetterRetentionSweeper;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.OutboxRetentionSweeper;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.runtime.VirtualThreadCommandBus;

/** Tests for {@link StreamRuneProducers}. */
class StreamRuneProducersTest {

  @Test
  void eventTypeRegistryProducesDefaultEmpty() {
    var producers = new StreamRuneProducers();
    EventTypeRegistry registry = producers.eventTypeRegistry();
    assertNotNull(registry);
  }

  @Test
  void postgresEventStoreFactoryProduced() {
    var producers = new StreamRuneProducers();
    EventStoreFactory factory =
        producers.postgresEventStoreFactory(
            mock(DataSource.class),
            producers.eventTypeRegistry(),
            TestProperties.defaults(),
            unsatisfied(),
            List.of(),
            List.of(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactoryTakesTheConfiguredStatementTimeout() throws Exception {
    var producers = new StreamRuneProducers();
    EventStoreFactory factory =
        eventStoreFactoryWith(
            producers,
            TestProperties.of(Map.of("streamrune.event-store.statement-timeout", "PT2M")));
    var field = factory.getClass().getDeclaredField("statementTimeout");
    field.setAccessible(true);
    assertEquals(java.time.Duration.ofMinutes(2), field.get(factory));
  }

  @Test
  void postgresEventStoreFactoryRefusesAnInvalidStatementTimeoutNamingTheProperty() {
    var producers = new StreamRuneProducers();
    var properties = TestProperties.of(Map.of("streamrune.event-store.statement-timeout", "-PT1S"));
    var ex =
        assertThrows(
            IllegalStateException.class, () -> eventStoreFactoryWith(producers, properties));
    assertTrue(
        ex.getMessage().startsWith("streamrune.event-store.statement-timeout"), ex.getMessage());
  }

  private static EventStoreFactory eventStoreFactoryWith(
      StreamRuneProducers producers, StreamRuneQuarkusProperties properties) {
    return producers.postgresEventStoreFactory(
        mock(DataSource.class),
        producers.eventTypeRegistry(),
        properties,
        unsatisfied(),
        List.of(),
        List.of(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied(),
        unsatisfied());
  }

  @Test
  void eventStoreFromFactoryProduced() {
    var producers = new StreamRuneProducers();
    EventStore stub = mock(EventStore.class);
    EventStoreFactory factory = () -> stub;
    assertSame(stub, producers.eventStore(factory));
  }

  @Test
  void aggregateLockerProduced() {
    var producers = new StreamRuneProducers();
    AggregateLocker locker = producers.aggregateLocker(TestProperties.defaults());
    assertNotNull(locker);
  }

  @Test
  void jdbcProjectionRepositoryProduced() {
    var producers = new StreamRuneProducers();
    var repository =
        producers.jdbcProjectionRepository(mock(DataSource.class), unsatisfied(), unsatisfied());
    assertNotNull(repository);
    // The repository doubles as the AtomicBatchProcessor wired into projection runners.
    assertInstanceOf(org.streamrune.core.projection.AtomicBatchProcessor.class, repository);
  }

  @Test
  void offsetStoreProduced() {
    var producers = new StreamRuneProducers();
    var offsetStore = producers.offsetStore(mock(DataSource.class));
    assertNotNull(offsetStore);
    assertInstanceOf(org.streamrune.postgres.PostgresOffsetStore.class, offsetStore);
  }

  @Test
  void shouldProduceQueryBus() {
    var producers = new StreamRuneProducers();
    QueryBus bus = producers.simpleQueryBus(unsatisfied());
    assertNotNull(bus);
    assertInstanceOf(org.streamrune.runtime.SimpleQueryBus.class, bus);
  }

  @Test
  void virtualThreadCommandBusProduced() {
    var producers = new StreamRuneProducers();
    VirtualThreadCommandBus bus =
        producers.virtualThreadCommandBus(
            mock(EventStore.class),
            producers.aggregateLocker(TestProperties.defaults()),
            TestProperties.defaults(),
            unsatisfied(), // no application SnapshotPolicy bean
            TestBeanManagers.withInterceptors(),
            unsatisfied(), // the never-read interceptor injection point
            Collections.emptyList(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(bus);
    bus.close();
  }

  @Test
  void virtualThreadCommandBusFiltersNullInterceptorProducts() {
    // Optional interceptor producers are dependent-scoped and may yield null; collecting the
    // beans must not propagate those nulls into the bus.
    var producers = new StreamRuneProducers();
    var withNulls =
        TestBeanManagers.withInterceptors(
            Arrays.asList(mock(CommandInterceptor.class), null, mock(CommandInterceptor.class)));
    VirtualThreadCommandBus bus =
        producers.virtualThreadCommandBus(
            mock(EventStore.class),
            producers.aggregateLocker(TestProperties.defaults()),
            TestProperties.defaults(),
            unsatisfied(), // no application SnapshotPolicy bean
            withNulls,
            unsatisfied(), // the never-read interceptor injection point
            Collections.emptyList(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(bus);
    bus.close();
  }

  /**
   * The redundant {@code asyncCommandBus} producer was removed — {@link VirtualThreadCommandBus}
   * already {@code implements ... AsyncCommandBus}, so its own {@code @DefaultBean} producer
   * supplies the {@link org.streamrune.core.AsyncCommandBus} CDI bean type directly. A second
   * producer of the same type made real Arc resolution of a plain {@code @Inject AsyncCommandBus}
   * injection point ambiguous (see {@code
   * QuarkusBeanWiringTest#asyncCommandBusResolvesUniquelyThroughTheRealProducerSet}, which proves
   * this through a real container). This test only pins the plain-Java-object fact the removed
   * producer used to assert: the bus IS-A AsyncCommandBus.
   */
  @Test
  void virtualThreadCommandBusIsUsableAsAsyncCommandBus() {
    var producers = new StreamRuneProducers();
    VirtualThreadCommandBus bus =
        producers.virtualThreadCommandBus(
            mock(EventStore.class),
            producers.aggregateLocker(TestProperties.defaults()),
            TestProperties.defaults(),
            unsatisfied(), // no application SnapshotPolicy bean
            TestBeanManagers.withInterceptors(),
            unsatisfied(), // the never-read interceptor injection point
            Collections.emptyList(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());

    assertInstanceOf(org.streamrune.core.AsyncCommandBus.class, bus);
    bus.close();
  }

  @Test
  void authorizationInterceptorProduced_whenPolicyPresent() {
    Instance<org.streamrune.core.CommandAuthorizationPolicy> policyInstance =
        satisfied(org.streamrune.core.CommandAuthorizationPolicy.allowAll());

    var producers = new StreamRuneProducers();
    var interceptor = producers.authorizationCommandInterceptor(policyInstance);
    assertNotNull(interceptor);
  }

  @Test
  void authorizationInterceptorNull_whenPolicyAbsent() {
    var producers = new StreamRuneProducers();
    var interceptor = producers.authorizationCommandInterceptor(unsatisfied());
    assertNull(interceptor);
  }

  @Test
  void auditStoreProduced() {
    var producers = new StreamRuneProducers();
    var store = producers.auditStore(mock(javax.sql.DataSource.class));
    assertNotNull(store);
    assertInstanceOf(org.streamrune.postgres.PostgresAuditStore.class, store);
  }

  @Test
  void auditCommandInterceptorProduced_whenStorePresent() {
    Instance<org.streamrune.core.audit.AuditStore> storeInstance =
        satisfied(mock(org.streamrune.core.audit.AuditStore.class));

    var producers = new StreamRuneProducers();
    var interceptor = producers.auditCommandInterceptor(storeInstance);
    assertNotNull(interceptor);
  }

  @Test
  void auditCommandInterceptorNull_whenStoreAbsent() {
    var producers = new StreamRuneProducers();
    var interceptor = producers.auditCommandInterceptor(unsatisfied());
    assertNull(interceptor);
  }

  @Test
  void auditCommandInterceptorNull_whenStoreProductNull() {
    // Dependent-scoped optional producers may yield a null product.
    Instance<org.streamrune.core.audit.AuditStore> storeInstance = satisfied(null);

    var producers = new StreamRuneProducers();
    var interceptor = producers.auditCommandInterceptor(storeInstance);
    assertNull(interceptor);
  }

  @Test
  void quarkusSecurityUserRoleResolverProduced_whenIdentityPresent() {
    Instance<SecurityIdentity> identityInstance = satisfied(mock(SecurityIdentity.class));

    var producers = new StreamRuneProducers();
    var resolver = producers.quarkusSecurityUserRoleResolver(identityInstance);
    assertNotNull(resolver);
    assertInstanceOf(QuarkusSecurityUserRoleResolver.class, resolver);
  }

  @Test
  void quarkusSecurityUserRoleResolverNull_whenIdentityAbsent() {
    var producers = new StreamRuneProducers();
    var resolver = producers.quarkusSecurityUserRoleResolver(unsatisfied());
    assertNull(resolver);
  }

  @Test
  void annotationAuthorizationInterceptorProduced_whenResolverPresent() {
    Instance<UserRoleResolver> resolverInstance = satisfied(mock(UserRoleResolver.class));

    var producers = new StreamRuneProducers();
    var interceptor = producers.annotationAuthorizationInterceptor(resolverInstance);
    assertNotNull(interceptor);
    assertInstanceOf(org.streamrune.runtime.AnnotationAuthorizationInterceptor.class, interceptor);
  }

  @Test
  void annotationAuthorizationInterceptorNull_whenResolverAbsent() {
    var producers = new StreamRuneProducers();
    var interceptor = producers.annotationAuthorizationInterceptor(unsatisfied());
    assertNull(interceptor);
  }

  @Test
  void annotationAuthorizationInterceptorNull_whenResolverProductNull() {
    Instance<UserRoleResolver> resolverInstance = satisfied(null);

    var producers = new StreamRuneProducers();
    var interceptor = producers.annotationAuthorizationInterceptor(resolverInstance);
    assertNull(interceptor);
  }

  @Test
  void postgresEventStoreFactoryWiresEventAuditStore_whenPresent() {
    var producers = new StreamRuneProducers();
    var store = new org.streamrune.postgres.PostgresEventAuditStore(mock(DataSource.class));

    EventStoreFactory factory =
        producers.postgresEventStoreFactory(
            mock(DataSource.class),
            producers.eventTypeRegistry(),
            TestProperties.defaults(),
            unsatisfied(),
            List.of(),
            List.of(),
            satisfied(store),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactoryWiresCryptoEngine_whenPresent() {
    var producers = new StreamRuneProducers();
    var cryptoEngine = mock(CryptoEngine.class);

    EventStoreFactory factory =
        producers.postgresEventStoreFactory(
            mock(DataSource.class),
            producers.eventTypeRegistry(),
            TestProperties.defaults(),
            satisfied(cryptoEngine),
            List.of(),
            List.of(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactory_wiresSnapshotMigrations_whenPresent() throws Exception {
    var producers = new StreamRuneProducers();
    var migration = mock(org.streamrune.core.SnapshotMigration.class);

    EventStoreFactory factory =
        producers.postgresEventStoreFactory(
            mock(DataSource.class),
            producers.eventTypeRegistry(),
            TestProperties.defaults(),
            unsatisfied(),
            List.of(),
            List.of(migration),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    var field = factory.getClass().getDeclaredField("snapshotMigrations");
    field.setAccessible(true);
    assertEquals(List.of(migration), field.get(factory));
  }

  @Test
  @SuppressWarnings("unchecked")
  void postgresEventStoreFactoryThrows_whenCryptoEngineAmbiguous() {
    var producers = new StreamRuneProducers();
    Instance<CryptoEngine> ambiguous = mock(Instance.class);
    when(ambiguous.isAmbiguous()).thenReturn(true);

    assertThrows(
        IllegalStateException.class,
        () ->
            producers.postgresEventStoreFactory(
                mock(DataSource.class),
                producers.eventTypeRegistry(),
                TestProperties.defaults(),
                ambiguous,
                List.of(),
                List.of(),
                unsatisfied(),
                unsatisfied(),
                unsatisfied(),
                unsatisfied(),
                unsatisfied(),
                unsatisfied()));
  }

  @Test
  void postgresEventStoreFactoryWiresOutbox_whenBothPresent() {
    var producers = new StreamRuneProducers();
    var mapper = mock(org.streamrune.core.outbox.OutboxEventMapper.class);
    var outboxStore = mock(org.streamrune.postgres.PostgresOutboxStore.class);

    EventStoreFactory factory =
        producers.postgresEventStoreFactory(
            mock(DataSource.class),
            producers.eventTypeRegistry(),
            TestProperties.defaults(),
            unsatisfied(),
            List.of(),
            List.of(),
            unsatisfied(),
            satisfied(mapper),
            satisfied(outboxStore),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactory_withMapperOnly_doesNotWireOutbox_andReturnsFactory() {
    // Exactly one of {OutboxEventMapper, PostgresOutboxStore} present: WARN path.
    // Outbox is not wired, but the factory must still be returned (no exception).
    var producers = new StreamRuneProducers();
    var mapper = mock(org.streamrune.core.outbox.OutboxEventMapper.class);

    EventStoreFactory factory =
        producers.postgresEventStoreFactory(
            mock(DataSource.class),
            producers.eventTypeRegistry(),
            TestProperties.defaults(),
            unsatisfied(),
            List.of(),
            List.of(),
            unsatisfied(),
            satisfied(mapper),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactory_withStoreOnly_doesNotWireOutbox_andReturnsFactory() {
    // Exactly one of {OutboxEventMapper, PostgresOutboxStore} present: WARN path.
    // Outbox is not wired, but the factory must still be returned (no exception).
    var producers = new StreamRuneProducers();
    var outboxStore = mock(org.streamrune.postgres.PostgresOutboxStore.class);

    EventStoreFactory factory =
        producers.postgresEventStoreFactory(
            mock(DataSource.class),
            producers.eventTypeRegistry(),
            TestProperties.defaults(),
            unsatisfied(),
            List.of(),
            List.of(),
            unsatisfied(),
            unsatisfied(),
            satisfied(outboxStore),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(factory);
  }

  @Test
  void outboxRetentionSweeperNull_whenOutboxDisabled() {
    var producers = new StreamRuneProducers();
    // outbox.enabled=false by default
    var sweeper =
        producers.outboxRetentionSweeper(
            satisfied(mock(org.streamrune.postgres.PostgresOutboxStore.class)),
            TestProperties.defaults(),
            unsatisfied(),
            unsatisfied());
    assertNull(sweeper);
  }

  @Test
  void outboxRetentionSweeperNull_whenStoreAbsent() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    var sweeper =
        producers.outboxRetentionSweeper(unsatisfied(), props, unsatisfied(), unsatisfied());
    assertNull(sweeper);
  }

  @Test
  void outboxRetentionSweeperProduced_whenEnabledAndStorePresent() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    var outboxStore = mock(org.streamrune.postgres.PostgresOutboxStore.class);
    var sweeper =
        producers.outboxRetentionSweeper(
            satisfied(outboxStore), props, unsatisfied(), unsatisfied());
    assertNotNull(sweeper);
    assertInstanceOf(OutboxRetentionSweeper.class, sweeper);
    sweeper.close();
  }

  @Test
  void outboxRetentionSweeperUsesConfiguredRetentionMaxAge() {
    var producers = new StreamRuneProducers();
    var props =
        TestProperties.of(
            Map.of(
                "streamrune.outbox.enabled", "true",
                "streamrune.outbox.retention-max-age", "PT336H"));
    var outboxStore = mock(org.streamrune.postgres.PostgresOutboxStore.class);
    var sweeper =
        producers.outboxRetentionSweeper(
            satisfied(outboxStore), props, unsatisfied(), unsatisfied());
    assertNotNull(sweeper);
    sweeper.close();
  }

  @Test
  void inboxRetentionSweeperNull_whenCommandInboxAbsent() {
    var producers = new StreamRuneProducers();
    var sweeper =
        producers.inboxRetentionSweeper(
            unsatisfied(), TestProperties.defaults(), unsatisfied(), unsatisfied());
    assertNull(sweeper);
  }

  @Test
  void inboxRetentionSweeperProduced_whenCommandInboxPresent() {
    var producers = new StreamRuneProducers();
    var inbox = mock(org.streamrune.postgres.PostgresCommandInbox.class);
    var sweeper =
        producers.inboxRetentionSweeper(
            satisfied(inbox), TestProperties.defaults(), unsatisfied(), unsatisfied());
    assertNotNull(sweeper);
    assertInstanceOf(InboxRetentionSweeper.class, sweeper);
    sweeper.close();
  }

  @Test
  void inboxRetentionSweeperUsesConfiguredRetentionMaxAge() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.inbox.retention-max-age", "PT336H"));
    var inbox = mock(org.streamrune.postgres.PostgresCommandInbox.class);
    var sweeper =
        producers.inboxRetentionSweeper(satisfied(inbox), props, unsatisfied(), unsatisfied());
    assertNotNull(sweeper);
    sweeper.close();
  }

  @Test
  void sagaDeadLetterRetentionSweeperNull_whenSagaDisabled() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.saga.enabled", "false"));
    var store = mock(org.streamrune.postgres.PostgresSagaDeadLetterStore.class);
    var sweeper =
        producers.sagaDeadLetterRetentionSweeper(
            satisfied(store), props, unsatisfied(), unsatisfied());
    assertNull(sweeper);
  }

  @Test
  void sagaDeadLetterRetentionSweeperNull_whenStoreAbsent() {
    var producers = new StreamRuneProducers();
    var sweeper =
        producers.sagaDeadLetterRetentionSweeper(
            unsatisfied(), TestProperties.defaults(), unsatisfied(), unsatisfied());
    assertNull(sweeper);
  }

  @Test
  void sagaDeadLetterRetentionSweeperProduced_whenEnabledAndStorePresent() {
    var producers = new StreamRuneProducers();
    var store = mock(org.streamrune.postgres.PostgresSagaDeadLetterStore.class);
    var sweeper =
        producers.sagaDeadLetterRetentionSweeper(
            satisfied(store), TestProperties.defaults(), unsatisfied(), unsatisfied());
    assertNotNull(sweeper);
    assertInstanceOf(SagaDeadLetterRetentionSweeper.class, sweeper);
    sweeper.close();
  }

  @Test
  void sagaDeadLetterRetentionSweeperUsesConfiguredRetentionMaxAge() {
    var producers = new StreamRuneProducers();
    var props =
        TestProperties.of(Map.of("streamrune.saga.dead-letter-retention-max-age", "PT1440H"));
    var store = mock(org.streamrune.postgres.PostgresSagaDeadLetterStore.class);
    var sweeper =
        producers.sagaDeadLetterRetentionSweeper(
            satisfied(store), props, unsatisfied(), unsatisfied());
    assertNotNull(sweeper);
    sweeper.close();
  }

  @Test
  void retentionSweepers_wireMetricsBean_whenPresent() {
    var producers = new StreamRuneProducers();
    var metrics = mock(StreamRuneMetrics.class);
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));

    var outboxStore = mock(org.streamrune.postgres.PostgresOutboxStore.class);
    var outboxSweeper =
        producers.outboxRetentionSweeper(
            satisfied(outboxStore), props, satisfied(metrics), unsatisfied());
    assertNotNull(outboxSweeper);
    outboxSweeper.close();

    var inbox = mock(org.streamrune.postgres.PostgresCommandInbox.class);
    var inboxSweeper =
        producers.inboxRetentionSweeper(satisfied(inbox), props, satisfied(metrics), unsatisfied());
    assertNotNull(inboxSweeper);
    inboxSweeper.close();

    var dlqStore = mock(org.streamrune.postgres.PostgresSagaDeadLetterStore.class);
    var sagaSweeper =
        producers.sagaDeadLetterRetentionSweeper(
            satisfied(dlqStore), props, satisfied(metrics), unsatisfied());
    assertNotNull(sagaSweeper);
    sagaSweeper.close();

    var dlq = mock(org.streamrune.core.DeadLetterQueue.class);
    var dlqSweeper =
        producers.deadLetterRetentionSweeper(
            satisfied(dlq), props, satisfied(metrics), unsatisfied());
    assertNotNull(dlqSweeper);
    dlqSweeper.close();
  }

  @Test
  void deadLetterRetentionSweeperNull_whenQueueAbsent() {
    var producers = new StreamRuneProducers();
    var sweeper =
        producers.deadLetterRetentionSweeper(
            unsatisfied(), TestProperties.defaults(), unsatisfied(), unsatisfied());
    assertNull(sweeper);
  }

  @Test
  void deadLetterRetentionSweeperProduced_whenAutomaticRetryDisabled() {
    // streamrune.dead-letter.enabled switches the automatic retry runner only. The command bus
    // dead-letters into the queue bean whatever it says, so the retention sweeper that bounds how
    // long those payloads stay queryable must follow the queue bean, not that switch.
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.dead-letter.enabled", "false"));
    var dlq = mock(org.streamrune.core.DeadLetterQueue.class);
    var sweeper =
        producers.deadLetterRetentionSweeper(satisfied(dlq), props, unsatisfied(), unsatisfied());
    assertNotNull(sweeper);
    assertInstanceOf(DeadLetterRetentionSweeper.class, sweeper);
    sweeper.close();
  }

  @Test
  void deadLetterRetentionSweeperRegisteredForRelayHealth_whenAutomaticRetryDisabled() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.dead-letter.enabled", "false"));
    var dlq = mock(org.streamrune.core.DeadLetterQueue.class);
    var health = new org.streamrune.runtime.BackgroundRelayHealthContributor();
    var sweeper =
        producers.deadLetterRetentionSweeper(
            satisfied(dlq), props, unsatisfied(), satisfied(health));
    assertNotNull(sweeper);
    assertTrue(
        health.components().stream()
            .anyMatch(c -> c.name().equals("dead-letter-retention-sweeper")),
        "the sweeper's liveness must be reported through the relay health");
    sweeper.close();
  }

  @Test
  void deadLetterRetentionSweeperProduced_whenQueuePresent() {
    var producers = new StreamRuneProducers();
    var dlq = mock(org.streamrune.core.DeadLetterQueue.class);
    var sweeper =
        producers.deadLetterRetentionSweeper(
            satisfied(dlq), TestProperties.defaults(), unsatisfied(), unsatisfied());
    assertNotNull(sweeper);
    assertInstanceOf(DeadLetterRetentionSweeper.class, sweeper);
    sweeper.close();
  }

  @Test
  void deadLetterRetentionSweeperUsesConfiguredRetentionMaxAge() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.dead-letter.retention-max-age", "PT720H"));
    var dlq = mock(org.streamrune.core.DeadLetterQueue.class);
    var sweeper =
        producers.deadLetterRetentionSweeper(satisfied(dlq), props, unsatisfied(), unsatisfied());
    assertNotNull(sweeper);
    sweeper.close();
  }

  @Test
  void postgresEventAuditStoreProduced_whenEnabled() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.event-audit-enabled", "true"));
    var store = producers.postgresEventAuditStore(mock(DataSource.class), props);
    assertNotNull(store);
  }

  @Test
  void postgresEventAuditStoreNull_whenDisabled() {
    var producers = new StreamRuneProducers();
    var store =
        producers.postgresEventAuditStore(mock(DataSource.class), TestProperties.defaults());
    assertNull(store);
  }

  @Test
  void postgresEventAuditQueryProduced_whenEnabled() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.event-audit-enabled", "true"));
    var query = producers.postgresEventAuditQuery(mock(DataSource.class), props);
    assertNotNull(query);
  }

  @Test
  void postgresEventAuditQueryNull_whenDisabled() {
    var producers = new StreamRuneProducers();
    var query =
        producers.postgresEventAuditQuery(mock(DataSource.class), TestProperties.defaults());
    assertNull(query);
  }

  @Test
  void postgresProjectionDeadLetterStoreProduced_whenEnabled() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.projection-dlq-enabled", "true"));
    var dlq = producers.postgresProjectionDeadLetterStore(mock(DataSource.class), props);
    assertNotNull(dlq);
  }

  @Test
  void postgresProjectionDeadLetterStoreNull_whenDisabled() {
    var producers = new StreamRuneProducers();
    var dlq =
        producers.postgresProjectionDeadLetterStore(
            mock(DataSource.class), TestProperties.defaults());
    assertNull(dlq);
  }

  @Test
  void subscriptionLeadershipProduced_whenEnabledAndDataSourcePresent() {
    var producers = new StreamRuneProducers();
    var leadership =
        producers.subscriptionLeadership(
            satisfied(mock(DataSource.class)), TestProperties.defaults());
    assertNotNull(leadership);
    assertInstanceOf(org.streamrune.postgres.LeaseBasedLeadership.class, leadership);
  }

  @Test
  void subscriptionLeadershipReturnsSharedNoop_whenNoDataSource() {
    var producers = new StreamRuneProducers();
    var leadership = producers.subscriptionLeadership(unsatisfied(), TestProperties.defaults());
    // NOOP, never null: CDI forbids a null @Singleton product, and every injection point must
    // resolve the SAME instance rather than minting a per-Dependent product.
    assertSame(org.streamrune.core.subscription.SubscriptionLeadership.NOOP, leadership);
  }

  @Test
  void subscriptionLeadershipReturnsSharedNoop_whenDisabled() {
    var producers = new StreamRuneProducers();
    var props =
        TestProperties.of(
            Map.of("streamrune.subscription.single-active-consumer.enabled", "false"));
    var leadership = producers.subscriptionLeadership(satisfied(mock(DataSource.class)), props);
    assertSame(org.streamrune.core.subscription.SubscriptionLeadership.NOOP, leadership);
  }

  @Test
  void subscriptionLeadershipProducerIsSingletonScoped() throws NoSuchMethodException {
    // @Singleton (not @Dependent) is what guarantees ProjectionProducer.multiProjectionRunner,
    // ProjectionProducer.scheduledProjectionRunner, and StreamRuneLifecycle.onStart all resolve
    // the ONE shared LeaseBasedLeadership (one renew heartbeat thread) rather than each
    // Instance.get() re-invoking the producer and minting a distinct instance.
    var method =
        StreamRuneProducers.class.getDeclaredMethod(
            "subscriptionLeadership", Instance.class, StreamRuneQuarkusProperties.class);
    assertTrue(
        method.isAnnotationPresent(jakarta.inject.Singleton.class),
        "leadership producer must be @Singleton so a single shared instance is used across all "
            + "injection points and the lifecycle");
    assertFalse(
        method.isAnnotationPresent(jakarta.enterprise.context.Dependent.class),
        "leadership producer must NOT be @Dependent — that mints one instance per injection point");
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> satisfied(T value) {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(false);
    when(inst.isResolvable()).thenReturn(true);
    when(inst.get()).thenReturn(value);
    return inst;
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> unsatisfied() {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(true);
    when(inst.isResolvable()).thenReturn(false);
    return inst;
  }

  /** Plain-reflection field accessor (no Spring test-util dependency in this module). */
  private static Object getField(Object target, String fieldName) {
    try {
      var field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void sseEventPublisherProduced() {
    var producers = new StreamRuneProducers();
    assertNotNull(producers.sseEventPublisher());
  }

  @Test
  void circuitBreakerCommandInterceptorProduced() {
    var producers = new StreamRuneProducers();
    var interceptor = producers.circuitBreakerCommandInterceptor(TestProperties.defaults());
    assertNotNull(interceptor);
  }

  @Test
  void beanValidationInterceptorNull_whenValidationDisabled() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.validation-enabled", "false"));
    var interceptor = producers.beanValidationInterceptor(props, unsatisfied());
    assertNull(interceptor);
  }

  // beanValidationInterceptor_whenEnabled_andValidatorAbsent test removed: needs Hibernate
  // Validator on classpath

  @Test
  @SuppressWarnings("unchecked")
  void micrometerStreamRuneMetricsIsTheNoopSingleton_whenDisabled() {
    // The producer is @Singleton (one instance behind every
    // Instance<StreamRuneMetrics>
    // injection point). CDI forbids a null singleton product, so the switched-off path yields the
    // inert StreamRuneMetrics.NOOP — the subscriptionLeadership pattern — never null.
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.metrics.enabled", "false"));
    Instance registry = mock(Instance.class);
    when(registry.isResolvable()).thenReturn(true);
    var metrics = producers.micrometerStreamRuneMetrics(registry, props);
    assertSame(StreamRuneMetrics.NOOP, metrics, "metrics off → the NOOP singleton, never null");
  }

  @Test
  @SuppressWarnings("unchecked")
  void micrometerStreamRuneMetricsIsTheNoopSingleton_whenRegistryAbsent() {
    var producers = new StreamRuneProducers();
    Instance registry = mock(Instance.class);
    when(registry.isResolvable()).thenReturn(false);
    var metrics = producers.micrometerStreamRuneMetrics(registry, TestProperties.defaults());
    assertSame(
        StreamRuneMetrics.NOOP, metrics, "no MeterRegistry → the NOOP singleton, never null");
  }

  @Test
  void micrometerStreamRuneMetricsUsesConfiguredPrefix() {
    var producers = new StreamRuneProducers();
    var registry = new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
    var props = TestProperties.of(Map.of("streamrune.metrics.prefix", "custom"));

    var metrics = producers.micrometerStreamRuneMetrics(satisfied(registry), props);

    assertInstanceOf(org.streamrune.integration.MicrometerStreamRuneMetrics.class, metrics);
    assertNotNull(registry.find("custom.commands.dispatched").counter());
  }

  @Test
  void micrometerStreamRuneMetricsIsSingletonScoped() throws Exception {
    // Root cause: the producer carried no scope, i.e. @Dependent — every one of the
    // Instance<StreamRuneMetrics> injection points in this class got its OWN
    // MicrometerStreamRuneMetrics, and since Micrometer's register() dedups gauge ids, only the
    // first instance's AtomicLongs backed the gauges; every other consumer wrote an orphaned field.
    // Same class of defect, same fix as subscriptionHealthContributor: @Singleton, keeping
    // @DefaultBean so an application-supplied StreamRuneMetrics still wins.
    var method =
        StreamRuneProducers.class.getMethod(
            "micrometerStreamRuneMetrics", Instance.class, StreamRuneQuarkusProperties.class);
    assertTrue(
        method.isAnnotationPresent(jakarta.inject.Singleton.class),
        "micrometerStreamRuneMetrics must be @Singleton so every consumer shares one instance");
    assertTrue(
        method.isAnnotationPresent(io.quarkus.arc.DefaultBean.class),
        "micrometerStreamRuneMetrics must stay @DefaultBean (an application bean overrides it)");
  }

  @Test
  void openTelemetryCommandInterceptorProduced_whenPresent() {
    var producers = new StreamRuneProducers();
    var interceptor = producers.openTelemetryCommandInterceptor(satisfied(OpenTelemetry.noop()));
    assertNotNull(interceptor);
  }

  @Test
  void openTelemetryCommandInterceptorNull_whenAbsent() {
    var producers = new StreamRuneProducers();
    var interceptor = producers.openTelemetryCommandInterceptor(unsatisfied());
    assertNull(interceptor);
  }

  @Test
  void subscriptionHealthContributorProduced_whenPresent() {
    var producers = new StreamRuneProducers();
    var contributor =
        producers.subscriptionHealthContributor(
            mock(EventStore.class),
            satisfied(mock(org.streamrune.core.projection.OffsetStore.class)),
            unsatisfied(),
            TestProperties.defaults());
    assertNotNull(contributor);
    assertEquals(1_000L, contributor.lagThreshold(), "the default lag threshold is 1000 events");
  }

  @Test
  void subscriptionHealthLagThresholdComesFromItsProperty() {
    var producers = new StreamRuneProducers();
    var contributor =
        producers.subscriptionHealthContributor(
            mock(EventStore.class),
            satisfied(mock(org.streamrune.core.projection.OffsetStore.class)),
            unsatisfied(),
            TestProperties.of(Map.of("streamrune.subscription.health.lag-threshold", "25000")));
    assertEquals(25_000L, contributor.lagThreshold());
  }

  @Test
  void aSubscriptionHealthLagThresholdBelowOneIsRefusedNamingTheProperty() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.subscription.health.lag-threshold", "0"));
    var e =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.subscriptionHealthContributor(
                    mock(EventStore.class), unsatisfied(), unsatisfied(), props));
    assertTrue(
        e.getMessage().contains("streamrune.subscription.health.lag-threshold must be at least 1"),
        e.getMessage());
  }

  @Test
  void subscriptionHealthContributorInert_whenOffsetStoreAbsent() {
    // The producer is @Singleton (single shared instance across the runners and the health
    // check). CDI forbids a null singleton product, so an unsatisfied OffsetStore must yield an
    // inert non-null contributor (empty registration map → overallStatus() UP), mirroring the
    // subscriptionLeadership NOOP pattern — never null.
    var producers = new StreamRuneProducers();
    var contributor =
        producers.subscriptionHealthContributor(
            mock(EventStore.class), unsatisfied(), unsatisfied(), TestProperties.defaults());
    assertNotNull(contributor, "must be a non-null inert contributor, never null (@Singleton)");
    assertEquals(
        org.streamrune.core.subscription.SubscriptionHealth.Status.UP,
        contributor.overallStatus(),
        "inert contributor has no registrations → UP");
  }

  @Test
  void subscriptionHealthContributorInert_whenOffsetStoreProductNull() {
    var producers = new StreamRuneProducers();
    Instance<org.streamrune.core.projection.OffsetStore> offsetStore = satisfied(null);
    var contributor =
        producers.subscriptionHealthContributor(
            mock(EventStore.class), offsetStore, unsatisfied(), TestProperties.defaults());
    assertNotNull(contributor, "null OffsetStore product must still yield a non-null contributor");
  }

  @Test
  void subscriptionHealthContributorIsSingletonScoped() throws Exception {
    // Root cause: an @Dependent producer hands the runner and the health check DIFFERENT
    // instances, so markTerminalError on the runner's instance never reaches /q/health/ready.
    // The producer must be @Singleton (like subscriptionLeadership) so all injection points share
    // one instance.
    var method =
        StreamRuneProducers.class.getMethod(
            "subscriptionHealthContributor",
            EventStore.class,
            Instance.class,
            Instance.class,
            StreamRuneQuarkusProperties.class);
    assertTrue(
        method.isAnnotationPresent(jakarta.inject.Singleton.class),
        "subscriptionHealthContributor must be @Singleton so the runner and the health check share"
            + " one instance");
  }

  @Test
  void sagaStoreProduced_whenEnabled() {
    var producers = new StreamRuneProducers();
    var store =
        producers.sagaStore(
            mock(DataSource.class), TestProperties.defaults(), unsatisfied(), unsatisfied());
    assertNotNull(store);
  }

  @Test
  void sagaStoreNull_whenDisabled() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.saga.enabled", "false"));
    var store = producers.sagaStore(mock(DataSource.class), props, unsatisfied(), unsatisfied());
    assertNull(store);
  }

  @Test
  void sagaStoreWiresCryptoEngine_whenPresent() {
    var producers = new StreamRuneProducers();
    var cryptoEngine = new org.streamrune.test.InMemoryCryptoEngine();

    var store =
        producers.sagaStore(
            mock(DataSource.class),
            TestProperties.defaults(),
            satisfied(cryptoEngine),
            unsatisfied());

    assertNotNull(store);
    var mapper = (com.fasterxml.jackson.databind.ObjectMapper) getField(store, "objectMapper");
    assertTrue(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
  }

  @Test
  void sagaStoreMapperIsCryptoBlind_whenNoCryptoEngine() {
    var producers = new StreamRuneProducers();

    var store =
        producers.sagaStore(
            mock(DataSource.class), TestProperties.defaults(), unsatisfied(), unsatisfied());

    assertNotNull(store);
    var mapper = (com.fasterxml.jackson.databind.ObjectMapper) getField(store, "objectMapper");
    assertFalse(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void sagaStoreThrows_whenCryptoEngineAmbiguous() {
    var producers = new StreamRuneProducers();
    Instance<CryptoEngine> ambiguous = mock(Instance.class);
    when(ambiguous.isAmbiguous()).thenReturn(true);

    assertThrows(
        IllegalStateException.class,
        () ->
            producers.sagaStore(
                mock(DataSource.class), TestProperties.defaults(), ambiguous, unsatisfied()));
  }

  // The projection repository producer must wire the CryptoEngine the same way
  // the saga store producer does, so @Encrypted read-model fields are ciphertext at rest.

  @Test
  void jdbcProjectionRepositoryWiresCryptoEngine_whenPresent() {
    var producers = new StreamRuneProducers();
    var cryptoEngine = new org.streamrune.test.InMemoryCryptoEngine();

    var repo =
        producers.jdbcProjectionRepository(
            mock(DataSource.class), satisfied(cryptoEngine), unsatisfied());

    assertNotNull(repo);
    var mapper = (com.fasterxml.jackson.databind.ObjectMapper) getField(repo, "objectMapper");
    assertTrue(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
  }

  @Test
  void jdbcProjectionRepositoryMapperIsCryptoBlind_whenNoCryptoEngine() {
    var producers = new StreamRuneProducers();

    var repo =
        producers.jdbcProjectionRepository(mock(DataSource.class), unsatisfied(), unsatisfied());

    assertNotNull(repo);
    var mapper = (com.fasterxml.jackson.databind.ObjectMapper) getField(repo, "objectMapper");
    assertFalse(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void jdbcProjectionRepositoryThrows_whenCryptoEngineAmbiguous() {
    var producers = new StreamRuneProducers();
    Instance<CryptoEngine> ambiguous = mock(Instance.class);
    when(ambiguous.isAmbiguous()).thenReturn(true);

    assertThrows(
        IllegalStateException.class,
        () -> producers.jdbcProjectionRepository(mock(DataSource.class), ambiguous, unsatisfied()));
  }

  @Test
  void complianceReportQueryProduced() {
    var producers = new StreamRuneProducers();
    var query = producers.complianceReportQuery(mock(DataSource.class));
    assertNotNull(query);
  }

  @Test
  void deadLetterRetryRunnerNull_whenDisabled() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.dead-letter.enabled", "false"));
    var runner =
        producers.deadLetterRetryRunner(
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            List.of(),
            props);
    assertNull(runner);
  }

  @Test
  void deadLetterRetryRunnerNull_whenDepsAbsent() {
    var producers = new StreamRuneProducers();
    // deadLetterEnabled=true by default
    var runner =
        producers.deadLetterRetryRunner(
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            List.of(),
            TestProperties.defaults());
    assertNull(runner);
  }

  @Test
  void deadLetterRetryRunnerProduced_whenEnabledAndDepsPresent() {
    var producers = new StreamRuneProducers();
    var runner =
        producers.deadLetterRetryRunner(
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)),
            satisfied(mock(org.streamrune.core.CommandBus.class)),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            List.of(),
            TestProperties.defaults());
    assertNotNull(runner);
    // No leadership bean present -> the runner defaults to NOOP (always leader = single-instance).
    assertSame(
        org.streamrune.core.subscription.SubscriptionLeadership.NOOP,
        getField(runner, "leadership"),
        "with no leadership bean the DLQ runner must default to SubscriptionLeadership.NOOP");
    runner.close();
  }

  @Test
  void deadLetterRetryRunnerReceivesLeadership_whenBeanPresent() {
    // Wiring proof: a resolvable SubscriptionLeadership bean must be threaded into the DLQ retry
    // runner so only the leader replica polls/retries. Mirrors how the projection producer threads
    // the same bean into projection runners.
    var producers = new StreamRuneProducers();
    var leadership = org.streamrune.core.subscription.SubscriptionLeadership.NOOP;
    var runner =
        producers.deadLetterRetryRunner(
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)),
            satisfied(mock(org.streamrune.core.CommandBus.class)),
            unsatisfied(),
            satisfied(leadership),
            unsatisfied(),
            unsatisfied(),
            List.of(),
            TestProperties.defaults());
    assertNotNull(runner);
    assertSame(
        leadership,
        getField(runner, "leadership"),
        "the DLQ runner must receive the resolvable SubscriptionLeadership bean");
    runner.close();
  }

  @Test
  void deadLetterRetryRunnerUsesCryptoAwareMapper_whenCryptoEngineBeanPresent() {
    var producers = new StreamRuneProducers();
    var runner =
        producers.deadLetterRetryRunner(
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)),
            satisfied(mock(org.streamrune.core.CommandBus.class)),
            satisfied(new org.streamrune.test.InMemoryCryptoEngine()),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            List.of(),
            TestProperties.defaults());
    assertNotNull(runner, "a CryptoEngine bean must yield a crypto-aware DLQ mapper");
    var mapper = (com.fasterxml.jackson.databind.ObjectMapper) getField(runner, "objectMapper");
    assertTrue(
        mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"),
        "the DLQ mapper must register CryptoShreddingModule when a CryptoEngine bean exists");
    runner.close();
  }

  @Test
  void deadLetterRetryRunnerMapperIsCryptoBlind_whenNoCryptoEngine() {
    var producers = new StreamRuneProducers();
    var runner =
        producers.deadLetterRetryRunner(
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)),
            satisfied(mock(org.streamrune.core.CommandBus.class)),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            List.of(),
            TestProperties.defaults());
    assertNotNull(runner);
    var mapper = (com.fasterxml.jackson.databind.ObjectMapper) getField(runner, "objectMapper");
    assertFalse(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
    runner.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  void deadLetterRetryRunnerRejectsAmbiguousCryptoEngines() {
    var producers = new StreamRuneProducers();
    Instance<CryptoEngine> ambiguous = mock(Instance.class);
    when(ambiguous.isAmbiguous()).thenReturn(true);

    assertThrows(
        IllegalStateException.class,
        () ->
            producers.deadLetterRetryRunner(
                satisfied(mock(org.streamrune.core.DeadLetterQueue.class)),
                satisfied(mock(org.streamrune.core.CommandBus.class)),
                ambiguous,
                unsatisfied(),
                unsatisfied(),
                unsatisfied(),
                List.of(),
                TestProperties.defaults()));
  }

  /**
   * Critical GDPR proof: the DLQ replay mapper the producer wires must serialize a command carrying
   * an {@code @Encrypted} field to CIPHERTEXT when a {@link CryptoEngine} bean is present. Before
   * the fix the producer honored the application's own {@code ObjectMapper} bean (which has no
   * {@code CryptoShreddingModule}) and would have leaked the plaintext PII.
   */
  @Test
  void deadLetterRetryRunnerMapperSerializesEncryptedCommandAsCiphertext() throws Exception {
    var producers = new StreamRuneProducers();
    var runner =
        producers.deadLetterRetryRunner(
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)),
            satisfied(mock(org.streamrune.core.CommandBus.class)),
            satisfied(new org.streamrune.test.InMemoryCryptoEngine()),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            List.of(),
            TestProperties.defaults());
    assertNotNull(runner);
    var mapper = (com.fasterxml.jackson.databind.ObjectMapper) getField(runner, "objectMapper");

    var json = mapper.writeValueAsString(new PiiCommand("agg-1", "dave@example.com"));

    assertFalse(
        json.contains("dave@example.com"),
        "the DLQ mapper must serialize @Encrypted fields as ciphertext, never plaintext PII");
    runner.close();
  }

  /**
   * Fail-fast proof: the command bus producer must reject a registered command type carrying an
   * {@code @Encrypted} field when no {@link CryptoEngine} bean is present, naming the offending
   * command class — otherwise that PII would be dead-lettered as plaintext.
   */
  @Test
  void virtualThreadCommandBusFailsFast_whenEncryptedCommandHasNoCryptoEngine() {
    var producers = new StreamRuneProducers();
    var registrations =
        List.<DeciderRegistration<?, ?, ?>>of(
            new DeciderRegistration<>(
                org.streamrune.core.types.AggregateType.of("pii"),
                PiiCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new PiiDecider()));

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.virtualThreadCommandBus(
                    mock(EventStore.class),
                    producers.aggregateLocker(TestProperties.defaults()),
                    TestProperties.defaults(),
                    unsatisfied(), // no application SnapshotPolicy bean
                    emptyInterceptorInstance(),
                    unsatisfied(), // the never-read interceptor injection point
                    registrations,
                    unsatisfied(),
                    unsatisfied(),
                    unsatisfied(),
                    unsatisfied()));
    assertTrue(ex.getMessage().contains("PiiCommand"), ex.getMessage());
    assertTrue(ex.getMessage().contains("email"), ex.getMessage());
  }

  @Test
  void virtualThreadCommandBus_refusesTwoRegistrationsOfOneCommandType() {
    var producers = new StreamRuneProducers();
    var type = org.streamrune.core.types.AggregateType.of("pii");
    var registrations =
        List.<DeciderRegistration<?, ?, ?>>of(
            new DeciderRegistration<>(
                type,
                PiiCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new PiiDecider()),
            new DeciderRegistration<>(
                type,
                PiiCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new PiiDecider()));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                producers.virtualThreadCommandBus(
                    mock(EventStore.class),
                    producers.aggregateLocker(TestProperties.defaults()),
                    TestProperties.defaults(),
                    unsatisfied(), // no application SnapshotPolicy bean
                    emptyInterceptorInstance(),
                    unsatisfied(), // the never-read interceptor injection point
                    registrations,
                    satisfied(new org.streamrune.test.InMemoryCryptoEngine()),
                    unsatisfied(),
                    unsatisfied(),
                    unsatisfied()));
    assertTrue(
        ex.getMessage().contains("is already registered (aggregate type 'pii')"), ex.getMessage());
  }

  /**
   * Companion to {@link #virtualThreadCommandBusFailsFast_whenEncryptedCommandHasNoCryptoEngine}:
   * the identical wiring WITH a {@link CryptoEngine} bean builds cleanly.
   */
  @Test
  void virtualThreadCommandBusBuilds_whenEncryptedCommandHasCryptoEngine() {
    var producers = new StreamRuneProducers();
    var registrations =
        List.<DeciderRegistration<?, ?, ?>>of(
            new DeciderRegistration<>(
                org.streamrune.core.types.AggregateType.of("pii"),
                PiiCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new PiiDecider()));

    var bus =
        producers.virtualThreadCommandBus(
            mock(EventStore.class),
            producers.aggregateLocker(TestProperties.defaults()),
            TestProperties.defaults(),
            unsatisfied(), // no application SnapshotPolicy bean
            emptyInterceptorInstance(),
            unsatisfied(), // the never-read interceptor injection point
            registrations,
            satisfied(new org.streamrune.test.InMemoryCryptoEngine()),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(bus);
    bus.close();
  }

  @Test
  @SuppressWarnings("unchecked")
  void virtualThreadCommandBusRejectsAmbiguousCryptoEngines() {
    var producers = new StreamRuneProducers();
    Instance<CryptoEngine> ambiguous = mock(Instance.class);
    when(ambiguous.isAmbiguous()).thenReturn(true);

    assertThrows(
        IllegalStateException.class,
        () ->
            producers.virtualThreadCommandBus(
                mock(EventStore.class),
                producers.aggregateLocker(TestProperties.defaults()),
                TestProperties.defaults(),
                unsatisfied(), // no application SnapshotPolicy bean
                emptyInterceptorInstance(),
                unsatisfied(), // the never-read interceptor injection point
                List.of(),
                ambiguous,
                unsatisfied(),
                unsatisfied(),
                unsatisfied()));
  }

  /**
   * The AUTO-CONFIGURED command bus must publish a failed command to a resolvable {@link
   * org.streamrune.core.DeadLetterQueue} THROUGH THE BUS (not written directly), mirroring Spring's
   * {@code
   * DeadLetterWiringAutoConfigurationTest#commandBusPublishesFailedCommandsToDeadLetterQueue}. A
   * decider that throws a non-domain {@link IllegalStateException} exhausts retries; the produced
   * bus must dead-letter the command exactly once AND record the DLQ-published meter — proving both
   * the DeadLetterQueue and StreamRuneMetrics wiring. Before the fix the producer never called
   * {@code .deadLetterQueue()}/{@code .metrics()}, so the entry vanished and the meter NOOPed, so
   * this test fails against the unwired producer.
   */
  @Test
  void commandBusPublishesFailedCommandToDeadLetterQueue() {
    var producers = new StreamRuneProducers();
    var dlq = new org.streamrune.test.InMemoryDeadLetterQueue();
    var metrics = new CountingMetrics();
    var registrations =
        List.<DeciderRegistration<?, ?, ?>>of(
            new DeciderRegistration<>(
                org.streamrune.core.types.AggregateType.of("failing"),
                FailingCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new FailingDecider()));

    var bus =
        producers.virtualThreadCommandBus(
            new org.streamrune.test.InMemoryEventStore(),
            producers.aggregateLocker(TestProperties.defaults()),
            TestProperties.defaults(),
            unsatisfied(), // no application SnapshotPolicy bean
            emptyInterceptorInstance(),
            unsatisfied(), // the never-read interceptor injection point
            registrations,
            unsatisfied(), // no CryptoEngine
            unsatisfied(), // no CommandInbox
            satisfied((org.streamrune.core.DeadLetterQueue) dlq),
            satisfied((StreamRuneMetrics) metrics));

    var command = new FailingCommand("agg-1");
    assertThrows(RuntimeException.class, () -> bus.execute(command));

    assertEquals(
        1, dlq.all().size(), "exactly one DLQ entry must be published through the auto-wired bus");
    // The DLQ persists the FULLY-QUALIFIED command class name (collision-safe across
    // bounded contexts), so the entry's commandType is the FQN, not the simple name.
    assertEquals(FailingCommand.class.getName(), dlq.all().getFirst().commandType());
    assertEquals(
        1,
        metrics.dlqPublished.get(),
        "the command-bus DLQ-published meter must be wired (not NOOP)");
    bus.close();
  }

  record FailingCommand(String id) implements org.streamrune.core.Command {}

  record FailingState() implements org.streamrune.core.AggregateState {}

  record FailingEvent() implements org.streamrune.core.DomainEvent {}

  static class FailingDecider
      implements org.streamrune.core.Decider<FailingCommand, FailingState, FailingEvent> {
    @Override
    public FailingState initialState() {
      return new FailingState();
    }

    @Override
    public java.util.List<FailingEvent> decide(FailingCommand command, FailingState state) {
      throw new IllegalStateException("boom");
    }

    @Override
    public FailingState evolve(FailingState state, FailingEvent event) {
      return state;
    }
  }

  /**
   * Records the DLQ-published meter so a test can prove {@code .metrics()} was wired (not NOOP).
   */
  static final class CountingMetrics implements StreamRuneMetrics {
    final java.util.concurrent.atomic.AtomicInteger dlqPublished =
        new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public void recordDeadLetterPublished(String commandType) {
      dlqPublished.incrementAndGet();
    }
  }

  record PiiCommand(String id, @org.streamrune.core.crypto.Encrypted(subjectId = "id") String email)
      implements org.streamrune.core.Command {}

  record PiiState() implements org.streamrune.core.AggregateState {}

  record PiiEvent() implements org.streamrune.core.DomainEvent {}

  static class PiiDecider implements org.streamrune.core.Decider<PiiCommand, PiiState, PiiEvent> {
    @Override
    public PiiState initialState() {
      return new PiiState();
    }

    @Override
    public java.util.List<PiiEvent> decide(PiiCommand command, PiiState state) {
      return java.util.List.of();
    }

    @Override
    public PiiState evolve(PiiState state, PiiEvent event) {
      return state;
    }
  }

  /**
   * Reproduces the "empty commandTypeRegistry" defect: the auto-configured {@link
   * org.streamrune.runtime.DeadLetterRetryRunner} must resolve a DLQ entry's command type using the
   * SAME {@link org.streamrune.runtime.DeciderRegistration} instances the auto-configured command
   * bus already consumes. A DLQ entry is published directly (bypassing the bus) keyed exactly the
   * way {@code VirtualThreadCommandBus.publishToDeadLetterQueue} keys it — {@code
   * command.getClass().getName()} — then a single on-demand {@code retry(CommandId)} is invoked on
   * the producer's runner. Before the fix, the runner's command-type registry is empty, so the
   * entry is "unknown command type" and only its failed-attempt count is bumped — the decider never
   * runs and the entry survives. After the fix, the registry is populated from the injected {@link
   * org.streamrune.runtime.DeciderRegistration}, so the command resolves, the decider actually
   * executes, and the entry is discarded on success.
   */
  @Test
  void deadLetterRetryRunnerResolvesCommandTypeFromDeciderRegistry() throws Exception {
    var producers = new StreamRuneProducers();
    var executed = new java.util.concurrent.atomic.AtomicBoolean(false);
    var dlq = new org.streamrune.test.InMemoryDeadLetterQueue();
    var objectMapper = new com.fasterxml.jackson.databind.ObjectMapper();
    var eventStore = new org.streamrune.test.InMemoryEventStore();

    var commandBus =
        producers.virtualThreadCommandBus(
            eventStore,
            producers.aggregateLocker(TestProperties.defaults()),
            TestProperties.defaults(),
            unsatisfied(), // no application SnapshotPolicy bean
            emptyInterceptorInstance(),
            unsatisfied(), // the never-read interceptor injection point
            List.of(
                new org.streamrune.runtime.DeciderRegistration<>(
                    org.streamrune.core.types.AggregateType.of("registry_test"),
                    RegistryTestCommand.class,
                    cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                    new RegistryTestDecider(executed))),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied());

    var runner =
        producers.deadLetterRetryRunner(
            satisfied(dlq),
            satisfied(commandBus),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            List.of(
                new org.streamrune.runtime.DeciderRegistration<>(
                    org.streamrune.core.types.AggregateType.of("registry_test"),
                    RegistryTestCommand.class,
                    cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                    new RegistryTestDecider(executed))),
            TestProperties.defaults());
    assertNotNull(runner);

    var commandId = org.streamrune.core.types.CommandId.of("dlq-cmd-1");
    var command = new RegistryTestCommand("agg-1");
    dlq.publish(
        new org.streamrune.core.DeadLetterQueue.DeadLetterPublishRequest(
            objectMapper.writeValueAsString(command),
            // Exactly what VirtualThreadCommandBus.publishToDeadLetterQueue stores:
            // its fully-qualified class name, command.getClass().getName().
            RegistryTestCommand.class.getName(),
            commandId,
            org.streamrune.core.types.StreamId.of(
                org.streamrune.core.types.AggregateType.of("registry_test"),
                org.streamrune.core.types.AggregateId.of("agg-1")),
            "java.lang.IllegalStateException",
            "boom",
            1,
            java.time.Instant.now(),
            null,
            null,
            null,
            null));

    boolean retried = runner.retry(commandId);

    assertTrue(retried);
    assertTrue(
        executed.get(),
        "decider must actually execute — the command type must resolve via the decider registry,"
            + " not be skipped as unknown");
    assertTrue(
        dlq.all().isEmpty(),
        "a successfully-retried entry is discarded, not left behind as unresolvable");

    runner.close();
    commandBus.close();
  }

  private static jakarta.enterprise.inject.spi.BeanManager emptyInterceptorInstance() {
    return TestBeanManagers.withInterceptors();
  }

  // ── the snapshot schema version must be reachable ────────────────────────────────

  /**
   * These producers hardcoded {@code SnapshotPolicy.everyNEvents(n)} — the ONE-ARG overload, which
   * pins {@code snapshotVersion} to 1 — so the documented schema-version bump was unreachable and
   * every registered {@code SnapshotMigration} bean was dead code (the store's migrate/discard
   * branch is only entered on a version mismatch). Observed through the produced bus: the version
   * it passes to {@code EventStore.load(streamId, expectedSnapshotVersion)} IS the effective
   * snapshot schema version.
   */
  @Test
  void applicationSuppliedSnapshotPolicyReachesTheCommandBus() {
    assertEquals(2, snapshotVersionSeenByBus(satisfied(SnapshotPolicy.everyNEvents(100, 2))));
  }

  /** The default side: with no application policy bean the shipped behaviour is unchanged. */
  @Test
  void defaultSnapshotPolicyIsEveryNEventsAtSchemaVersionOne() {
    assertEquals(1, snapshotVersionSeenByBus(unsatisfied()));
  }

  /**
   * A resolvable-but-null product (the dependent-scoped null-product contract this class warns
   * about) must fall back to the default rather than NPE inside the builder.
   */
  @Test
  @SuppressWarnings("unchecked")
  void nullSnapshotPolicyProductFallsBackToTheDefault() {
    Instance<SnapshotPolicy> nullProduct = mock(Instance.class);
    when(nullProduct.isResolvable()).thenReturn(true);
    when(nullProduct.get()).thenReturn(null);
    assertEquals(1, snapshotVersionSeenByBus(nullProduct));
  }

  /**
   * {@code SnapshotPolicy.never()} was equally unreachable — {@code
   * streamrune.snapshot-every-n-events} is validated {@code > 0}, so snapshots could not be
   * switched off either. The bus then passes {@code EventStore.IGNORE_SNAPSHOT}, the store's "never
   * consult a snapshot at all" sentinel — distinct from {@code 0}, "accept whatever is stored,"
   * which would silently rehydrate a snapshot left over from before the policy switch.
   */
  @Test
  void applicationSuppliedNeverPolicyDisablesSnapshotting() {
    // never() must route through EventStore.IGNORE_SNAPSHOT, not the old "skip the
    // version check, use any stored snapshot" sentinel (0) — a snapshot left over from before the
    // policy switched to never() must never be silently rehydrated.
    assertEquals(
        EventStore.IGNORE_SNAPSHOT, snapshotVersionSeenByBus(satisfied(SnapshotPolicy.never())));
  }

  /**
   * Builds the command bus through the real producer with the given {@code SnapshotPolicy}
   * Instance, dispatches a probe command that emits no events, and returns the {@code
   * expectedSnapshotVersion} the bus asked the event store for.
   */
  private static int snapshotVersionSeenByBus(Instance<SnapshotPolicy> snapshotPolicyInstance) {
    // The "not yet observed" marker must differ from EventStore.IGNORE_SNAPSHOT (-1),
    // now a real expected value here, so a wiring failure that never reaches the store cannot
    // coincidentally read as a correct observation.
    var seen = new java.util.concurrent.atomic.AtomicInteger(Integer.MIN_VALUE);
    EventStore store = mock(EventStore.class);
    when(store.load(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyInt()))
        .thenAnswer(
            invocation -> {
              seen.set(invocation.getArgument(1));
              return org.streamrune.core.AggregateHistory.empty();
            });
    var producers = new StreamRuneProducers();
    try (VirtualThreadCommandBus bus =
        producers.virtualThreadCommandBus(
            store,
            producers.aggregateLocker(TestProperties.defaults()),
            TestProperties.defaults(),
            snapshotPolicyInstance,
            emptyInterceptorInstance(),
            unsatisfied(), // the never-read interceptor injection point
            List.of(
                new org.streamrune.runtime.DeciderRegistration<>(
                    org.streamrune.core.types.AggregateType.of("snapshot_probe"),
                    SnapshotProbeCommand.class,
                    cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                    new SnapshotProbeDecider())),
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            unsatisfied())) {
      bus.execute(new SnapshotProbeCommand("agg-snap"));
    }
    return seen.get();
  }

  record SnapshotProbeCommand(String id) implements org.streamrune.core.Command {}

  record SnapshotProbeState() implements org.streamrune.core.AggregateState {}

  /** Emits nothing, so the probe dispatch resolves entirely inside the load path. */
  static final class SnapshotProbeDecider
      implements org.streamrune.core.Decider<
          SnapshotProbeCommand, SnapshotProbeState, org.streamrune.core.DomainEvent> {
    @Override
    public SnapshotProbeState initialState() {
      return new SnapshotProbeState();
    }

    @Override
    public List<org.streamrune.core.DomainEvent> decide(
        SnapshotProbeCommand command, SnapshotProbeState state) {
      return List.of();
    }

    @Override
    public SnapshotProbeState evolve(
        SnapshotProbeState state, org.streamrune.core.DomainEvent event) {
      return state;
    }
  }

  record RegistryTestCommand(String id) implements org.streamrune.core.Command {}

  record RegistryTestState() implements org.streamrune.core.AggregateState {}

  record RegistryTestEvent() implements org.streamrune.core.DomainEvent {}

  static class RegistryTestDecider
      implements org.streamrune.core.Decider<
          RegistryTestCommand, RegistryTestState, RegistryTestEvent> {
    private final java.util.concurrent.atomic.AtomicBoolean executed;

    RegistryTestDecider(java.util.concurrent.atomic.AtomicBoolean executed) {
      this.executed = executed;
    }

    @Override
    public RegistryTestState initialState() {
      return new RegistryTestState();
    }

    @Override
    public java.util.List<RegistryTestEvent> decide(
        RegistryTestCommand command, RegistryTestState state) {
      executed.set(true);
      return java.util.List.of(new RegistryTestEvent());
    }

    @Override
    public RegistryTestState evolve(RegistryTestState state, RegistryTestEvent event) {
      return state;
    }
  }

  @Test
  void outboxPollerNull_whenDisabled() {
    var producers = new StreamRuneProducers();
    // outboxEnabled=false by default
    var poller =
        producers.outboxPoller(
            unsatisfied(), unsatisfied(), TestProperties.defaults(), unsatisfied(), unsatisfied());
    assertNull(poller);
  }

  @Test
  void outboxPollerProduced_whenEnabledAndDepsPresent() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    var store = mock(org.streamrune.core.outbox.OutboxStore.class);
    // An un-stubbed mock returns null from orderingMode(), which OutboxPoller.build() refuses, and
    // Duration.ZERO from claimLease(), which opts the poller out of its lease guard.
    when(store.orderingMode())
        .thenReturn(org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST);
    when(store.claimLease()).thenReturn(java.time.Duration.ofMinutes(4));
    var poller =
        producers.outboxPoller(
            satisfied(store),
            satisfied(mock(org.streamrune.core.outbox.OutboxPublisher.class)),
            props,
            unsatisfied(),
            unsatisfied());
    assertNotNull(poller);
    poller.close();
  }

  /**
   * The observability diagnostics (INFO on the disabled path, WARN on the enabled-but-missing-bean
   * path — see {@link StreamRuneProducers#outboxPoller}) must not change the null-return control
   * flow: an enabled outbox whose OutboxStore bean is absent still yields no poller. (The log
   * messages themselves are verified by reading — capturing them would require pinning an SLF4J
   * backend into this module, which risks the QuarkusUnitTest boot tests' logging.)
   */
  @Test
  void outboxPollerNull_whenEnabledButStoreMissing() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    var poller =
        producers.outboxPoller(
            unsatisfied(),
            satisfied(mock(org.streamrune.core.outbox.OutboxPublisher.class)),
            props,
            unsatisfied(),
            unsatisfied());
    assertNull(poller);
  }

  @Test
  void outboxPollerNull_whenEnabledButPublisherMissing() {
    var producers = new StreamRuneProducers();
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    var poller =
        producers.outboxPoller(
            satisfied(mock(org.streamrune.core.outbox.OutboxStore.class)),
            unsatisfied(),
            props,
            unsatisfied(),
            unsatisfied());
    assertNull(poller);
  }

  @Test
  void upcasterChainNull_whenNoUpcasters() {
    var producers = new StreamRuneProducers();
    var chain = producers.upcasterChain(List.of());
    assertNull(chain);
  }

  @Test
  void upcasterChainProduced_whenUpcastersPresent() {
    var producers = new StreamRuneProducers();
    var upcaster = mock(org.streamrune.core.upcasting.EventUpcaster.class);
    when(upcaster.eventType()).thenReturn(new EventType("TestEvent"));
    when(upcaster.currentVersion()).thenReturn(2);
    var chain = producers.upcasterChain(List.of(upcaster));
    assertNotNull(chain);
  }

  @Test
  void annotationAuthorizationInterceptorUsesRawResolver_notWrappedInCaching() {
    // The resolver must be used AS-IS, never auto-wrapped in CachingUserRoleResolver
    // — a cache hit (keyed only by UserId) would skip the delegate and bypass its fail-closed
    // principal-match check. Mirrors Spring's deliberate non-wrap.
    var raw = mock(UserRoleResolver.class);
    Instance<UserRoleResolver> resolverInstance = satisfied(raw);

    var producers = new StreamRuneProducers();
    var interceptor = producers.annotationAuthorizationInterceptor(resolverInstance);
    assertNotNull(interceptor);
    var wired = getField(interceptor, "resolver");
    assertSame(raw, wired, "the interceptor must use the raw resolver, not a wrapped copy");
    assertFalse(
        wired instanceof CachingUserRoleResolver,
        "the resolver must NOT be auto-wrapped in CachingUserRoleResolver (cross-principal bypass)");
  }

  @Test
  void annotationAuthorizationInterceptorUsesExplicitCachingResolverAsIs() {
    // Opt-in caching: an application that explicitly provides a CachingUserRoleResolver (a
    // pure-function resolver by the app's own choice) gets it used verbatim, not double-wrapped.
    var cachedResolver = new CachingUserRoleResolver(mock(UserRoleResolver.class));
    Instance<UserRoleResolver> resolverInstance = satisfied((UserRoleResolver) cachedResolver);

    var producers = new StreamRuneProducers();
    var interceptor = producers.annotationAuthorizationInterceptor(resolverInstance);
    assertNotNull(interceptor);
    assertSame(cachedResolver, getField(interceptor, "resolver"));
  }

  @Test
  void resolverIsNotCached_soPrincipalMatchAlwaysReRuns() {
    // Behavior proof: the auto-configured resolver must re-invoke the delegate on
    // EVERY call. A CachingUserRoleResolver wrap (keyed only by UserId) would serve one principal's
    // cached authorities to a different caller presenting the same X-User-Id header within the TTL.
    // The ambient authenticated principal is simulated with a mutable ref; the resolver fail-closes
    // unless the requested userId matches it.
    var ambientPrincipal = new java.util.concurrent.atomic.AtomicReference<String>();
    UserRoleResolver ambient =
        userId ->
            userId != null && userId.value().equals(ambientPrincipal.get())
                ? new org.streamrune.core.UserAuthority(
                    java.util.Set.of("ADMIN"), java.util.Set.of())
                : org.streamrune.core.UserAuthority.EMPTY;

    var producers = new StreamRuneProducers();
    var interceptor = producers.annotationAuthorizationInterceptor(satisfied(ambient));
    var resolver = (UserRoleResolver) getField(interceptor, "resolver");

    // Alice legitimately authenticated, header X-User-Id=alice -> receives her roles.
    ambientPrincipal.set("alice");
    assertTrue(
        resolver.resolve(org.streamrune.core.types.UserId.of("alice")).hasRole("ADMIN"),
        "the legitimately-authenticated principal must receive their roles");

    // Bob authenticated, spoofs X-User-Id=alice within the TTL -> must get EMPTY (delegate
    // re-runs).
    ambientPrincipal.set("bob");
    assertFalse(
        resolver.resolve(org.streamrune.core.types.UserId.of("alice")).hasRole("ADMIN"),
        "a CachingUserRoleResolver wrap would serve Alice's cached roles to Bob — cross-principal"
            + " authorization bypass");
  }
}
