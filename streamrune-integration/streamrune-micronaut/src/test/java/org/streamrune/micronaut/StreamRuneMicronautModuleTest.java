package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.QueryBus;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.runtime.AuditingQueryBus;
import org.streamrune.runtime.CacheInvalidator;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.CachingUserRoleResolver;
import org.streamrune.runtime.SimpleQueryBus;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.runtime.gdpr.ExportSubjectDataService;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.streamrune.test.InMemoryCryptoEngine;

/** Tests for {@link StreamRuneMicronautModule}. */
class StreamRuneMicronautModuleTest {

  @Test
  void eventTypeRegistryProvidesDefaultEmpty() {
    var module = new StreamRuneMicronautModule();
    EventTypeRegistry registry = module.eventTypeRegistry();
    assertNotNull(registry);
  }

  @Test
  void aggregateLockerBuiltFromProperties() {
    var module = new StreamRuneMicronautModule();
    AggregateLocker locker = module.aggregateLocker(StreamRuneMicronautProperties.withDefaults());
    assertNotNull(locker);
  }

  @Test
  void virtualThreadCommandBusBuilt() {
    var module = new StreamRuneMicronautModule();
    EventStore eventStore = mock(EventStore.class);
    AggregateLocker locker = module.aggregateLocker(StreamRuneMicronautProperties.withDefaults());

    VirtualThreadCommandBus bus =
        module.virtualThreadCommandBus(
            eventStore,
            locker,
            StreamRuneMicronautProperties.withDefaults(),
            null, // no application SnapshotPolicy bean
            List.of(),
            List.of(),
            List.of(),
            null,
            null,
            null);
    assertNotNull(bus);
    bus.close();
  }

  /**
   * Fail-fast proof: the command bus factory must reject a registered command type carrying an
   * {@code @Encrypted} field when no {@link org.streamrune.core.crypto.CryptoEngine} bean is
   * present, naming the offending command class — otherwise that PII would be dead-lettered as
   * plaintext.
   */
  @Test
  void virtualThreadCommandBusFailsFast_whenEncryptedCommandHasNoCryptoEngine() {
    var module = new StreamRuneMicronautModule();
    EventStore eventStore = mock(EventStore.class);
    AggregateLocker locker = module.aggregateLocker(StreamRuneMicronautProperties.withDefaults());
    var registrations =
        List.<org.streamrune.runtime.DeciderRegistration<?, ?, ?>>of(
            new org.streamrune.runtime.DeciderRegistration<>(
                org.streamrune.core.types.AggregateType.of("pii"),
                PiiCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new PiiDecider()));

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                module.virtualThreadCommandBus(
                    eventStore,
                    locker,
                    StreamRuneMicronautProperties.withDefaults(),
                    null, // no application SnapshotPolicy bean
                    List.of(),
                    registrations,
                    List.of(),
                    null,
                    null,
                    null));
    assertTrue(ex.getMessage().contains("PiiCommand"), ex.getMessage());
    assertTrue(ex.getMessage().contains("email"), ex.getMessage());
  }

  /**
   * Companion to {@link #virtualThreadCommandBusFailsFast_whenEncryptedCommandHasNoCryptoEngine}:
   * the identical wiring WITH a {@link org.streamrune.core.crypto.CryptoEngine} bean builds
   * cleanly.
   */
  @Test
  void virtualThreadCommandBusBuilds_whenEncryptedCommandHasCryptoEngine() {
    var module = new StreamRuneMicronautModule();
    EventStore eventStore = mock(EventStore.class);
    AggregateLocker locker = module.aggregateLocker(StreamRuneMicronautProperties.withDefaults());
    var registrations =
        List.<org.streamrune.runtime.DeciderRegistration<?, ?, ?>>of(
            new org.streamrune.runtime.DeciderRegistration<>(
                org.streamrune.core.types.AggregateType.of("pii"),
                PiiCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new PiiDecider()));

    var bus =
        module.virtualThreadCommandBus(
            eventStore,
            locker,
            StreamRuneMicronautProperties.withDefaults(),
            null, // no application SnapshotPolicy bean
            List.of(),
            registrations,
            List.of(new InMemoryCryptoEngine()),
            null,
            null,
            null);
    assertNotNull(bus);
    bus.close();
  }

  @Test
  void virtualThreadCommandBusRejectsMultipleCryptoEngines() {
    var module = new StreamRuneMicronautModule();
    EventStore eventStore = mock(EventStore.class);
    AggregateLocker locker = module.aggregateLocker(StreamRuneMicronautProperties.withDefaults());
    assertThrows(
        IllegalStateException.class,
        () ->
            module.virtualThreadCommandBus(
                eventStore,
                locker,
                StreamRuneMicronautProperties.withDefaults(),
                null, // no application SnapshotPolicy bean
                List.of(),
                List.of(),
                List.of(new InMemoryCryptoEngine(), new InMemoryCryptoEngine()),
                null,
                null,
                null));
  }

  /**
   * Two registrations of one command type are refused by the factory itself, with the registration
   * rule's message — a second registration never silently replaces the first.
   */
  @Test
  void virtualThreadCommandBus_refusesTwoRegistrationsOfOneCommandType() {
    var module = new StreamRuneMicronautModule();
    var type = org.streamrune.core.types.AggregateType.of("pii");
    var registrations =
        List.<org.streamrune.runtime.DeciderRegistration<?, ?, ?>>of(
            new org.streamrune.runtime.DeciderRegistration<>(
                type,
                PiiCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new PiiDecider()),
            new org.streamrune.runtime.DeciderRegistration<>(
                type,
                PiiCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new PiiDecider()));

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                module.virtualThreadCommandBus(
                    mock(EventStore.class),
                    module.aggregateLocker(StreamRuneMicronautProperties.withDefaults()),
                    StreamRuneMicronautProperties.withDefaults(),
                    null, // no application SnapshotPolicy bean
                    List.of(),
                    registrations,
                    List.of(new InMemoryCryptoEngine()),
                    null,
                    null,
                    null));
    assertTrue(
        ex.getMessage().contains("is already registered (aggregate type 'pii')"), ex.getMessage());
    assertTrue(ex.getMessage().contains(PiiCommand.class.getName()), ex.getMessage());
  }

  /**
   * The AUTO-CONFIGURED command bus must publish a failed command to a resolvable {@link
   * org.streamrune.core.DeadLetterQueue} THROUGH THE BUS (not written directly), mirroring Spring's
   * {@code
   * DeadLetterWiringAutoConfigurationTest#commandBusPublishesFailedCommandsToDeadLetterQueue}. A
   * decider that throws a non-domain {@link IllegalStateException} exhausts retries; the produced
   * bus must dead-letter the command exactly once AND record the DLQ-published meter — proving both
   * the DeadLetterQueue and StreamRuneMetrics wiring. Before the fix the factory never called
   * {@code .deadLetterQueue()}/{@code .metrics()}, so the entry vanished and the meter NOOPed, so
   * this test fails against the unwired factory.
   */
  @Test
  void commandBusPublishesFailedCommandToDeadLetterQueue() {
    var module = new StreamRuneMicronautModule();
    var dlq = new org.streamrune.test.InMemoryDeadLetterQueue();
    var metrics = new CountingMetrics();
    var eventStore = new org.streamrune.test.InMemoryEventStore();
    var locker = module.aggregateLocker(StreamRuneMicronautProperties.withDefaults());
    var registrations =
        List.<org.streamrune.runtime.DeciderRegistration<?, ?, ?>>of(
            new org.streamrune.runtime.DeciderRegistration<>(
                org.streamrune.core.types.AggregateType.of("failing"),
                FailingCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new FailingDecider()));

    var bus =
        module.virtualThreadCommandBus(
            eventStore,
            locker,
            StreamRuneMicronautProperties.withDefaults(),
            null, // no application SnapshotPolicy bean
            List.of(),
            registrations,
            List.of(), // no CryptoEngine
            null, // no CommandInbox
            dlq,
            metrics);

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
    public List<FailingEvent> decide(FailingCommand command, FailingState state) {
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
  static final class CountingMetrics implements org.streamrune.core.StreamRuneMetrics {
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
    public List<PiiEvent> decide(PiiCommand command, PiiState state) {
      return List.of();
    }

    @Override
    public PiiState evolve(PiiState state, PiiEvent event) {
      return state;
    }
  }

  @Test
  void postgresEventStoreFactoryBuilt() {
    var module = new StreamRuneMicronautModule();
    EventStoreFactory factory =
        module.postgresEventStoreFactory(
            mock(DataSource.class),
            module.eventTypeRegistry(),
            StreamRuneMicronautProperties.withDefaults(),
            List.of(),
            List.of(),
            List.of(),
            null,
            null,
            null,
            null,
            null,
            null);
    assertNotNull(factory);
  }

  @Test
  void shouldProduceQueryBus() {
    var module = new StreamRuneMicronautModule();
    QueryBus bus = module.simpleQueryBus(null);
    assertNotNull(bus);
    assertInstanceOf(org.streamrune.runtime.SimpleQueryBus.class, bus);
  }

  @Test
  void eventStoreDelegatesToFactory() {
    var module = new StreamRuneMicronautModule();
    EventStoreFactory factory = mock(EventStoreFactory.class);
    EventStore stub = mock(EventStore.class);
    when(factory.create()).thenReturn(stub);

    EventStore result = module.eventStore(factory);
    assertSame(stub, result);
    verify(factory).create();
  }

  @Test
  void authorizationInterceptorCreated_whenPolicyProvided() {
    var module = new StreamRuneMicronautModule();
    org.streamrune.core.CommandAuthorizationPolicy policy =
        org.streamrune.core.CommandAuthorizationPolicy.allowAll();

    var interceptor = module.authorizationCommandInterceptor(policy);

    assertNotNull(interceptor);
    assertInstanceOf(org.streamrune.runtime.AuthorizationCommandInterceptor.class, interceptor);
  }

  @Test
  void auditStoreCreated_whenDataSourceProvided() {
    var module = new StreamRuneMicronautModule();
    var store = module.auditStore(mock(DataSource.class));
    assertNotNull(store);
    assertInstanceOf(org.streamrune.postgres.PostgresAuditStore.class, store);
  }

  @Test
  void auditCommandInterceptorCreated_whenStoreProvided() {
    var module = new StreamRuneMicronautModule();
    org.streamrune.core.audit.AuditStore store = mock(org.streamrune.core.audit.AuditStore.class);

    var interceptor = module.auditCommandInterceptor(store);

    assertNotNull(interceptor);
    assertInstanceOf(org.streamrune.runtime.AuditCommandInterceptor.class, interceptor);
  }

  @Test
  void micronautSecurityUserRoleResolverCreated_whenSecurityServiceProvided() {
    var module = new StreamRuneMicronautModule();
    io.micronaut.security.utils.SecurityService securityService =
        mock(io.micronaut.security.utils.SecurityService.class);

    var resolver = module.micronautSecurityUserRoleResolver(securityService);

    assertNotNull(resolver);
    assertInstanceOf(MicronautSecurityUserRoleResolver.class, resolver);
  }

  @Test
  void annotationAuthorizationInterceptorCreated_whenResolverProvided() {
    var module = new StreamRuneMicronautModule();
    org.streamrune.core.UserRoleResolver resolver =
        mock(org.streamrune.core.UserRoleResolver.class);

    var interceptor = module.annotationAuthorizationInterceptor(resolver);

    assertNotNull(interceptor);
    assertInstanceOf(org.streamrune.runtime.AnnotationAuthorizationInterceptor.class, interceptor);
  }

  @Test
  void forgetSubjectServiceCreated_withoutAuditStore() {
    var module = new StreamRuneMicronautModule();
    ForgetSubjectService service =
        module.forgetSubjectService(new InMemoryCryptoEngine(), null, List.of(), null, null);
    assertNotNull(service);
  }

  @Test
  void forgetSubjectServiceCreated_withAuditStore() {
    var module = new StreamRuneMicronautModule();
    AuditStore auditStore = mock(AuditStore.class);
    ForgetSubjectService service =
        module.forgetSubjectService(new InMemoryCryptoEngine(), auditStore, List.of(), null, null);
    assertNotNull(service);
    service.forget(SubjectId.of("user-1"), UserId.of("admin"));
    verify(auditStore, Mockito.atLeastOnce()).save(any(AuditEntry.class));
  }

  @Test
  void forgetSubjectService_auditsAReinstateBesideTheForgetItReverses() {
    var module = new StreamRuneMicronautModule();
    AuditStore auditStore = mock(AuditStore.class);
    ForgetSubjectService service =
        module.forgetSubjectService(new InMemoryCryptoEngine(), auditStore, List.of(), null, null);

    service.forget(SubjectId.of("user-1"), UserId.of("dpo"));
    service.reinstate(SubjectId.of("user-1"), UserId.of("dpo"));

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(auditStore, times(2)).save(captor.capture());
    assertEquals(
        List.of("GDPR_FORGET", "GDPR_REINSTATE"),
        captor.getAllValues().stream().map(AuditEntry::commandType).toList());
  }

  @Test
  void exportSubjectDataServiceCreated_withoutAuditStore() {
    var module = new StreamRuneMicronautModule();
    SubjectDataCollector collector = mock(SubjectDataCollector.class);
    ExportSubjectDataService service = module.exportSubjectDataService(List.of(collector), null);
    assertNotNull(service);
  }

  @Test
  void exportSubjectDataServiceCreated_withAuditStore() {
    var module = new StreamRuneMicronautModule();
    SubjectDataCollector collector =
        new SubjectDataCollector() {
          @Override
          public String name() {
            return "profile";
          }

          @Override
          public JsonNode collect(SubjectId subjectId) {
            return new ObjectMapper().createObjectNode();
          }
        };
    AuditStore auditStore = mock(AuditStore.class);
    ExportSubjectDataService service =
        module.exportSubjectDataService(List.of(collector), auditStore);
    assertNotNull(service);
    service.export(SubjectId.of("user-1"), UserId.of("admin"));
    verify(auditStore, Mockito.atLeastOnce()).save(any(AuditEntry.class));
  }

  @Test
  void postgresEventStoreFactoryWiresAuditStore_whenPresent() {
    var module = new StreamRuneMicronautModule();
    var auditStore = new org.streamrune.postgres.PostgresEventAuditStore(mock(DataSource.class));
    EventStoreFactory factory =
        module.postgresEventStoreFactory(
            mock(DataSource.class),
            module.eventTypeRegistry(),
            StreamRuneMicronautProperties.withDefaults(),
            List.of(),
            List.of(),
            List.of(),
            auditStore,
            null,
            null,
            null,
            null,
            null);
    assertNotNull(factory);
  }

  @Test
  void circuitBreakerCommandInterceptorBuilt() {
    var module = new StreamRuneMicronautModule();
    var interceptor =
        module.circuitBreakerCommandInterceptor(StreamRuneMicronautProperties.withDefaults());
    assertNotNull(interceptor);
  }

  @Test
  void sseEventPublisherBuilt() {
    var module = new StreamRuneMicronautModule();
    assertNotNull(module.sseEventPublisher());
  }

  @Test
  void beanValidationInterceptor_buildsDefaultValidator_whenNoBean() {
    var module = new StreamRuneMicronautModule();
    try {
      // With a JSR-380 provider on the classpath the fallback builds a real validator.
      var interceptor = module.beanValidationInterceptor(null);
      assertNotNull(interceptor);
    } catch (jakarta.validation.ValidationException e) {
      // Without a provider the fallback fails loudly instead of silently skipping validation.
      assertNotNull(e.getMessage());
    }
  }

  @Test
  void beanValidationInterceptorBuilt_withValidator() {
    var module = new StreamRuneMicronautModule();
    var interceptor = module.beanValidationInterceptor(mock(jakarta.validation.Validator.class));
    assertNotNull(interceptor);
  }

  @Test
  void cachingQueryBusBuilt() {
    var module = new StreamRuneMicronautModule();
    var bus = module.cachingQueryBus(new SimpleQueryBus(), null, null);
    assertNotNull(bus);
    assertInstanceOf(CachingQueryBus.class, bus);
  }

  @Test
  void cacheInvalidatorBuilt() {
    var module = new StreamRuneMicronautModule();
    var bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    CacheInvalidator invalidator = module.cacheInvalidator(bus);
    assertNotNull(invalidator);
  }

  @Test
  void queryBus_withCachingAndAudit() {
    var module = new StreamRuneMicronautModule();
    var simpleBus = new SimpleQueryBus();
    var cachingBus = CachingQueryBus.builder().delegate(simpleBus).build();
    AuditStore auditStore = mock(AuditStore.class);

    QueryBus result = module.queryBus(simpleBus, cachingBus, auditStore);
    assertInstanceOf(AuditingQueryBus.class, result);
  }

  @Test
  void queryBus_withoutCachingOrAudit() {
    var module = new StreamRuneMicronautModule();
    var simpleBus = new SimpleQueryBus();

    QueryBus result = module.queryBus(simpleBus, null, null);
    assertSame(simpleBus, result);
  }

  @Test
  void queryBus_withAuditOnly() {
    var module = new StreamRuneMicronautModule();
    var simpleBus = new SimpleQueryBus();
    AuditStore auditStore = mock(AuditStore.class);

    QueryBus result = module.queryBus(simpleBus, null, auditStore);
    assertInstanceOf(AuditingQueryBus.class, result);
  }

  @Test
  void queryBus_withCachingOnly() {
    var module = new StreamRuneMicronautModule();
    var simpleBus = new SimpleQueryBus();
    var cachingBus = CachingQueryBus.builder().delegate(simpleBus).build();

    QueryBus result = module.queryBus(simpleBus, cachingBus, null);
    assertSame(cachingBus, result);
  }

  @Test
  void postgresEventAuditStoreBuilt() {
    var module = new StreamRuneMicronautModule();
    var store = module.postgresEventAuditStore(mock(DataSource.class));
    assertNotNull(store);
  }

  @Test
  void postgresEventAuditQueryBuilt() {
    var module = new StreamRuneMicronautModule();
    var query = module.postgresEventAuditQuery(mock(DataSource.class));
    assertNotNull(query);
  }

  @Test
  void postgresProjectionDeadLetterStoreBuilt() {
    var module = new StreamRuneMicronautModule();
    var store = module.postgresProjectionDeadLetterStore(mock(DataSource.class));
    assertNotNull(store);
  }

  @Test
  void postgresProjectionDeadLetterStore_backsOffWhenAUserStoreExists() throws Exception {
    // The default projection DLQ store must declare
    // @Requires(missingBeans = ProjectionDeadLetterStore) so a user-supplied store cleanly REPLACES
    // it. Without it, the user store AND the framework default both register; ProjectionFactory's
    // List<ProjectionDeadLetterStore> collection injection exposes BOTH, and resolveSingleDlq
    // hard-fails boot with "Multiple ProjectionDeadLetterStore" — and its @Primary
    // remediation advice cannot reduce a collection-injected List. @Secondary alone does not filter
    // collection injection. Spring uses @ConditionalOnMissingBean and Quarkus @DefaultBean; this
    // restores that parity on Micronaut.
    var m =
        StreamRuneMicronautModule.class.getDeclaredMethod(
            "postgresProjectionDeadLetterStore", DataSource.class);
    boolean guarded =
        java.util.Arrays.stream(
                m.getAnnotationsByType(io.micronaut.context.annotation.Requires.class))
            .anyMatch(
                r ->
                    java.util.Arrays.asList(r.missingBeans())
                        .contains(org.streamrune.core.projection.ProjectionDeadLetterStore.class));
    assertTrue(
        guarded,
        "the default projection DLQ store must @Requires(missingBeans = ProjectionDeadLetterStore)"
            + " so a user-supplied store replaces it instead of both registering");
  }

  @Test
  void sagaStoreBuilt() {
    var module = new StreamRuneMicronautModule();
    var store = module.sagaStore(mock(DataSource.class), List.of(), null);
    assertNotNull(store);
  }

  @Test
  void sagaStoreWiresCryptoEngine_whenPresent() throws Exception {
    var module = new StreamRuneMicronautModule();
    var crypto = new InMemoryCryptoEngine();

    var store = module.sagaStore(mock(DataSource.class), List.of(crypto), null);

    assertNotNull(store);
    var field = store.getClass().getDeclaredField("objectMapper");
    field.setAccessible(true);
    var mapper = (ObjectMapper) field.get(store);
    assertTrue(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
  }

  @Test
  void sagaStoreMapperIsCryptoBlind_whenNoCryptoEngine() throws Exception {
    var module = new StreamRuneMicronautModule();

    var store = module.sagaStore(mock(DataSource.class), List.of(), null);

    assertNotNull(store);
    var field = store.getClass().getDeclaredField("objectMapper");
    field.setAccessible(true);
    var mapper = (ObjectMapper) field.get(store);
    assertFalse(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
  }

  @Test
  void sagaStoreThrows_whenCryptoEngineAmbiguous() {
    var module = new StreamRuneMicronautModule();
    var crypto1 = new InMemoryCryptoEngine();
    var crypto2 = new InMemoryCryptoEngine();

    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> module.sagaStore(mock(DataSource.class), List.of(crypto1, crypto2), null));
    assertTrue(ex.getMessage().contains("Multiple CryptoEngine beans found"));
  }

  // The projection repository bean must wire the CryptoEngine the same way the
  // saga store bean does, so @Encrypted read-model fields are ciphertext at rest (not plaintext).

  @Test
  void jdbcProjectionRepositoryWiresCryptoEngine_whenPresent() throws Exception {
    var module = new StreamRuneMicronautModule();
    var crypto = new InMemoryCryptoEngine();

    var repo = module.jdbcProjectionRepository(mock(DataSource.class), List.of(crypto), null);

    assertNotNull(repo);
    var field = repo.getClass().getDeclaredField("objectMapper");
    field.setAccessible(true);
    var mapper = (ObjectMapper) field.get(repo);
    assertTrue(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
  }

  @Test
  void jdbcProjectionRepositoryMapperIsCryptoBlind_whenNoCryptoEngine() throws Exception {
    var module = new StreamRuneMicronautModule();

    var repo = module.jdbcProjectionRepository(mock(DataSource.class), List.of(), null);

    assertNotNull(repo);
    var field = repo.getClass().getDeclaredField("objectMapper");
    field.setAccessible(true);
    var mapper = (ObjectMapper) field.get(repo);
    assertFalse(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
  }

  @Test
  void jdbcProjectionRepositoryThrows_whenCryptoEngineAmbiguous() {
    var module = new StreamRuneMicronautModule();
    var crypto1 = new InMemoryCryptoEngine();
    var crypto2 = new InMemoryCryptoEngine();

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                module.jdbcProjectionRepository(
                    mock(DataSource.class), List.of(crypto1, crypto2), null));
    assertTrue(ex.getMessage().contains("Multiple CryptoEngine beans found"));
  }

  @Test
  void complianceReportQueryBuilt() {
    var module = new StreamRuneMicronautModule();
    var query = module.complianceReportQuery(mock(DataSource.class));
    assertNotNull(query);
  }

  @Test
  void commandAuditQueryBuilt() {
    var module = new StreamRuneMicronautModule();
    var query = module.commandAuditQuery(mock(DataSource.class));
    assertNotNull(query);
    assertInstanceOf(org.streamrune.postgres.PostgresCommandAuditQuery.class, query);
  }

  @Test
  void offsetStoreDefaultsToPostgres_whenDataSourceProvided() {
    var module = new StreamRuneMicronautModule();
    var offsetStore = module.offsetStore(mock(DataSource.class));
    assertNotNull(offsetStore);
    assertInstanceOf(org.streamrune.postgres.PostgresOffsetStore.class, offsetStore);
  }

  @Test
  void subscriptionHealthContributorBuilt() {
    var module = new StreamRuneMicronautModule();
    var contributor =
        module.subscriptionHealthContributor(
            mock(EventStore.class),
            mock(org.streamrune.core.projection.OffsetStore.class),
            null,
            2_500L);
    assertNotNull(contributor);
    assertEquals(2_500L, contributor.lagThreshold());
  }

  @Test
  void upcasterChainBuilt() {
    var module = new StreamRuneMicronautModule();
    var upcaster = mock(org.streamrune.core.upcasting.EventUpcaster.class);
    when(upcaster.eventType()).thenReturn(new EventType("TestEvent"));
    when(upcaster.currentVersion()).thenReturn(2);
    var chain = module.upcasterChain(List.of(upcaster));
    assertNotNull(chain);
  }

  @Test
  void annotationAuthorizationInterceptorUsesRawResolver_notWrappedInCaching() {
    // The resolver must be used AS-IS, never auto-wrapped in CachingUserRoleResolver
    // — a cache hit (keyed only by UserId) would skip the delegate and bypass its fail-closed
    // principal-match check. Mirrors Spring's deliberate non-wrap.
    var module = new StreamRuneMicronautModule();
    var raw = mock(UserRoleResolver.class);
    var interceptor = module.annotationAuthorizationInterceptor(raw);
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
    var module = new StreamRuneMicronautModule();
    var cachedResolver = new CachingUserRoleResolver(mock(UserRoleResolver.class));
    var interceptor = module.annotationAuthorizationInterceptor(cachedResolver);
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
    var module = new StreamRuneMicronautModule();
    var ambientPrincipal = new java.util.concurrent.atomic.AtomicReference<String>();
    UserRoleResolver ambient =
        userId ->
            userId != null && userId.value().equals(ambientPrincipal.get())
                ? new org.streamrune.core.UserAuthority(
                    java.util.Set.of("ADMIN"), java.util.Set.of())
                : org.streamrune.core.UserAuthority.EMPTY;

    var interceptor = module.annotationAuthorizationInterceptor(ambient);
    var resolver = (UserRoleResolver) getField(interceptor, "resolver");

    // Alice legitimately authenticated, header X-User-Id=alice -> receives her roles.
    ambientPrincipal.set("alice");
    assertTrue(
        resolver.resolve(UserId.of("alice")).hasRole("ADMIN"),
        "the legitimately-authenticated principal must receive their roles");

    // Bob authenticated, spoofs X-User-Id=alice within the TTL -> must get EMPTY (delegate
    // re-runs).
    ambientPrincipal.set("bob");
    assertFalse(
        resolver.resolve(UserId.of("alice")).hasRole("ADMIN"),
        "a CachingUserRoleResolver wrap would serve Alice's cached roles to Bob — cross-principal"
            + " authorization bypass");
  }

  // micrometerStreamRuneMetrics requires MeterRegistry on test classpath — tested via integration

  @Test
  void openTelemetryCommandInterceptorBuilt() {
    var module = new StreamRuneMicronautModule();
    var interceptor =
        module.openTelemetryCommandInterceptor(io.opentelemetry.api.OpenTelemetry.noop());
    assertNotNull(interceptor);
  }

  @Test
  void deadLetterRetryRunnerBuilt() {
    var module = new StreamRuneMicronautModule();
    var runner =
        module.deadLetterRetryRunner(
            mock(org.streamrune.core.DeadLetterQueue.class),
            mock(org.streamrune.core.CommandBus.class),
            List.of(),
            List.of(),
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults());
    assertNotNull(runner);
    // No leadership bean (null) -> the runner defaults to NOOP (always leader = single-instance).
    var leadership = getField(runner, "leadership");
    assertSame(
        org.streamrune.core.subscription.SubscriptionLeadership.NOOP,
        leadership,
        "with no leadership bean the DLQ runner must default to SubscriptionLeadership.NOOP");
  }

  @Test
  void deadLetterRetryRunnerReceivesLeadership_whenBeanPresent() {
    // Wiring proof: a non-null SubscriptionLeadership bean must be threaded into the DLQ retry
    // runner so only the leader replica polls/retries. Mirrors how the projection factory threads
    // the same bean into projection runners.
    var module = new StreamRuneMicronautModule();
    var leadership = org.streamrune.core.subscription.SubscriptionLeadership.NOOP;
    var runner =
        module.deadLetterRetryRunner(
            mock(org.streamrune.core.DeadLetterQueue.class),
            mock(org.streamrune.core.CommandBus.class),
            List.of(),
            List.of(),
            leadership,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults());
    assertNotNull(runner);
    assertSame(
        leadership,
        getField(runner, "leadership"),
        "the DLQ runner must receive the injected SubscriptionLeadership bean");
  }

  @Test
  void deadLetterRetryRunnerRejectsMultipleCryptoEngines() {
    var module = new StreamRuneMicronautModule();
    var engines =
        List.of(
            (org.streamrune.core.crypto.CryptoEngine)
                new org.streamrune.test.InMemoryCryptoEngine(),
            new org.streamrune.test.InMemoryCryptoEngine());
    assertThrows(
        IllegalStateException.class,
        () ->
            module.deadLetterRetryRunner(
                mock(org.streamrune.core.DeadLetterQueue.class),
                mock(org.streamrune.core.CommandBus.class),
                engines,
                List.of(),
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults()));
  }

  @Test
  void deadLetterRetryRunnerWiresCryptoEngine_whenPresent() throws Exception {
    var module = new StreamRuneMicronautModule();
    var crypto = new org.streamrune.test.InMemoryCryptoEngine();

    var runner =
        module.deadLetterRetryRunner(
            mock(org.streamrune.core.DeadLetterQueue.class),
            mock(org.streamrune.core.CommandBus.class),
            List.of(crypto),
            List.of(),
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults());

    assertNotNull(runner);
    var field = runner.getClass().getDeclaredField("objectMapper");
    field.setAccessible(true);
    var mapper = (ObjectMapper) field.get(runner);
    assertTrue(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
    runner.close();
  }

  @Test
  void deadLetterRetryRunnerMapperIsCryptoBlind_whenNoCryptoEngine() throws Exception {
    var module = new StreamRuneMicronautModule();

    var runner =
        module.deadLetterRetryRunner(
            mock(org.streamrune.core.DeadLetterQueue.class),
            mock(org.streamrune.core.CommandBus.class),
            List.of(),
            List.of(),
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults());

    assertNotNull(runner);
    var field = runner.getClass().getDeclaredField("objectMapper");
    field.setAccessible(true);
    var mapper = (ObjectMapper) field.get(runner);
    assertFalse(mapper.getRegisteredModuleIds().contains("CryptoShreddingModule"));
    runner.close();
  }

  /**
   * Reproduces the "empty commandTypeRegistry" defect: the auto-configured {@link
   * org.streamrune.runtime.DeadLetterRetryRunner} must resolve a DLQ entry's command type using the
   * SAME {@link org.streamrune.runtime.DeciderRegistration} instances the auto-configured command
   * bus already consumes. A DLQ entry is published directly (bypassing the bus) keyed exactly the
   * way {@code VirtualThreadCommandBus.publishToDeadLetterQueue} keys it — {@code
   * command.getClass().getName()} — then a single on-demand {@code retry(CommandId)} is invoked on
   * the factory's runner. Before the fix, the runner's command-type registry is empty, so the entry
   * is "unknown command type" and only its failed-attempt count is bumped — the decider never runs
   * and the entry survives. After the fix, the registry is populated from the injected {@link
   * org.streamrune.runtime.DeciderRegistration}, so the command resolves, the decider actually
   * executes, and the entry is discarded on success.
   */
  @Test
  void deadLetterRetryRunnerResolvesCommandTypeFromDeciderRegistry() throws Exception {
    var module = new StreamRuneMicronautModule();
    var executed = new java.util.concurrent.atomic.AtomicBoolean(false);
    var dlq = new org.streamrune.test.InMemoryDeadLetterQueue();
    var objectMapper = new ObjectMapper();
    var eventStore = new org.streamrune.test.InMemoryEventStore();
    var locker = module.aggregateLocker(StreamRuneMicronautProperties.withDefaults());

    var registrations =
        List.<org.streamrune.runtime.DeciderRegistration<?, ?, ?>>of(
            new org.streamrune.runtime.DeciderRegistration<>(
                org.streamrune.core.types.AggregateType.of("registry_test"),
                RegistryTestCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
                new RegistryTestDecider(executed)));

    VirtualThreadCommandBus commandBus =
        module.virtualThreadCommandBus(
            eventStore,
            locker,
            StreamRuneMicronautProperties.withDefaults(),
            null, // no application SnapshotPolicy bean
            List.of(),
            registrations,
            List.of(),
            null,
            null,
            null);

    var runner =
        module.deadLetterRetryRunner(
            dlq,
            commandBus,
            List.of(),
            registrations,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults());
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
    public List<RegistryTestEvent> decide(RegistryTestCommand command, RegistryTestState state) {
      executed.set(true);
      return List.of(new RegistryTestEvent());
    }

    @Override
    public RegistryTestState evolve(RegistryTestState state, RegistryTestEvent event) {
      return state;
    }
  }

  @Test
  void outboxPollerBuilt() {
    var module = new StreamRuneMicronautModule();
    var store = mock(org.streamrune.core.outbox.OutboxStore.class);
    // An un-stubbed mock returns null from orderingMode(), which OutboxPoller.build() refuses, and
    // Duration.ZERO from claimLease(), which opts the poller out of its lease guard.
    when(store.orderingMode())
        .thenReturn(org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST);
    when(store.claimLease()).thenReturn(java.time.Duration.ofMinutes(4));
    var poller =
        module.outboxPoller(
            store,
            mock(org.streamrune.core.outbox.OutboxPublisher.class),
            StreamRuneMicronautProperties.withDefaults(),
            null,
            null);
    assertNotNull(poller);
  }

  // === parity gap tests: crypto, upcasters, outbox, retention, schema.auto-initialize ===

  @Test
  void postgresEventStoreFactory_wiresCryptoEngine_whenPresent() {
    var module = new StreamRuneMicronautModule();
    var crypto = new InMemoryCryptoEngine();
    var factory =
        module.postgresEventStoreFactory(
            mock(DataSource.class),
            module.eventTypeRegistry(),
            StreamRuneMicronautProperties.withDefaults(),
            List.of(crypto),
            List.of(),
            List.of(),
            null,
            null,
            null,
            null,
            null,
            null);
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactory_throwsOnAmbiguousCryptoEngine() {
    var module = new StreamRuneMicronautModule();
    var crypto1 = new InMemoryCryptoEngine();
    var crypto2 = new InMemoryCryptoEngine();
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                module.postgresEventStoreFactory(
                    mock(DataSource.class),
                    module.eventTypeRegistry(),
                    StreamRuneMicronautProperties.withDefaults(),
                    List.of(crypto1, crypto2),
                    List.of(),
                    List.of(),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null));
    assertTrue(
        ex.getMessage().contains("Multiple CryptoEngine beans found"),
        "exception message must identify the ambiguity");
  }

  @Test
  void postgresEventStoreFactory_wiresUpcasters_whenPresent() {
    var module = new StreamRuneMicronautModule();
    var upcaster = mock(EventUpcaster.class);
    when(upcaster.eventType()).thenReturn(new EventType("TestEvent"));
    when(upcaster.currentVersion()).thenReturn(2);
    var factory =
        module.postgresEventStoreFactory(
            mock(DataSource.class),
            module.eventTypeRegistry(),
            StreamRuneMicronautProperties.withDefaults(),
            List.of(),
            List.of(upcaster),
            List.of(),
            null,
            null,
            null,
            null,
            null,
            null);
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactory_wiresSnapshotMigrations_whenPresent() throws Exception {
    var module = new StreamRuneMicronautModule();
    var migration = mock(org.streamrune.core.SnapshotMigration.class);
    var factory =
        module.postgresEventStoreFactory(
            mock(DataSource.class),
            module.eventTypeRegistry(),
            StreamRuneMicronautProperties.withDefaults(),
            List.of(),
            List.of(),
            List.of(migration),
            null,
            null,
            null,
            null,
            null,
            null);
    var field = factory.getClass().getDeclaredField("snapshotMigrations");
    field.setAccessible(true);
    assertEquals(List.of(migration), field.get(factory));
  }

  @Test
  void postgresEventStoreFactory_wiresOutbox_whenBothMapperAndStorePresent() {
    var module = new StreamRuneMicronautModule();
    var outboxMapper = mock(OutboxEventMapper.class);
    var outboxStore = mock(PostgresOutboxStore.class);
    var factory =
        module.postgresEventStoreFactory(
            mock(DataSource.class),
            module.eventTypeRegistry(),
            StreamRuneMicronautProperties.withDefaults(),
            List.of(),
            List.of(),
            List.of(),
            null,
            outboxMapper,
            outboxStore,
            null,
            null,
            null);
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactory_warnAndSucceed_whenOnlyMapperPresent() {
    var module = new StreamRuneMicronautModule();
    var outboxMapper = mock(OutboxEventMapper.class);
    // mapper-only: should warn but still return a non-null factory (both-or-neither warn path)
    var factory =
        module.postgresEventStoreFactory(
            mock(DataSource.class),
            module.eventTypeRegistry(),
            StreamRuneMicronautProperties.withDefaults(),
            List.of(),
            List.of(),
            List.of(),
            null,
            outboxMapper,
            null,
            null,
            null,
            null);
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactory_warnAndSucceed_whenOnlyStorePresent() {
    var module = new StreamRuneMicronautModule();
    var outboxStore = mock(PostgresOutboxStore.class);
    // store-only: should warn but still return a non-null factory (both-or-neither warn path)
    var factory =
        module.postgresEventStoreFactory(
            mock(DataSource.class),
            module.eventTypeRegistry(),
            StreamRuneMicronautProperties.withDefaults(),
            List.of(),
            List.of(),
            List.of(),
            null,
            null,
            outboxStore,
            null,
            null,
            null);
    assertNotNull(factory);
  }

  @Test
  void postgresEventStoreFactory_appliesTheConfiguredStatementTimeout() throws Exception {
    var module = new StreamRuneMicronautModule();
    var properties = mock(StreamRuneMicronautProperties.class);
    when(properties.eventStoreStatementTimeout()).thenReturn(java.time.Duration.ofMinutes(2));
    assertEquals(
        java.time.Duration.ofMinutes(2),
        statementTimeoutOf(eventStoreFactoryWith(module, properties)));

    // An explicit null keeps the factory's own default.
    when(properties.eventStoreStatementTimeout()).thenReturn(null);
    assertEquals(
        java.time.Duration.ofSeconds(30),
        statementTimeoutOf(eventStoreFactoryWith(module, properties)));
  }

  @Test
  void postgresEventStoreFactory_refusesAnInvalidStatementTimeoutNamingTheProperty() {
    var module = new StreamRuneMicronautModule();
    var properties = mock(StreamRuneMicronautProperties.class);
    when(properties.eventStoreStatementTimeout()).thenReturn(java.time.Duration.ofSeconds(-1));
    var ex =
        assertThrows(IllegalStateException.class, () -> eventStoreFactoryWith(module, properties));
    assertTrue(
        ex.getMessage().startsWith("streamrune.event-store.statement-timeout"), ex.getMessage());
  }

  private static EventStoreFactory eventStoreFactoryWith(
      StreamRuneMicronautModule module, StreamRuneMicronautProperties properties) {
    return module.postgresEventStoreFactory(
        mock(DataSource.class),
        module.eventTypeRegistry(),
        properties,
        List.of(),
        List.of(),
        List.of(),
        null,
        null,
        null,
        null,
        null,
        null);
  }

  private static Object statementTimeoutOf(EventStoreFactory factory) throws Exception {
    var field = factory.getClass().getDeclaredField("statementTimeout");
    field.setAccessible(true);
    return field.get(factory);
  }

  @Test
  void postgresEventStoreFactory_refusesAMapperWithAStoreItCannotWriteInTheAppendTransaction() {
    var module = new StreamRuneMicronautModule();
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                module.postgresEventStoreFactory(
                    mock(DataSource.class),
                    module.eventTypeRegistry(),
                    StreamRuneMicronautProperties.withDefaults(),
                    List.of(),
                    List.of(),
                    List.of(),
                    null,
                    mock(OutboxEventMapper.class),
                    new org.streamrune.test.InMemoryOutboxStore(),
                    null,
                    null,
                    null));
    assertTrue(ex.getMessage().contains("InMemoryOutboxStore"), ex.getMessage());
    assertTrue(ex.getMessage().contains("PostgresOutboxStore"), ex.getMessage());
  }

  @Test
  void postgresEventStoreFactory_warnAndSucceed_whenOnlyAnotherStoreIsPresent() {
    var module = new StreamRuneMicronautModule();
    // No mapper, so nothing is emitted and the store's type does not matter: warn, still build.
    var factory =
        module.postgresEventStoreFactory(
            mock(DataSource.class),
            module.eventTypeRegistry(),
            StreamRuneMicronautProperties.withDefaults(),
            List.of(),
            List.of(),
            List.of(),
            null,
            null,
            new org.streamrune.test.InMemoryOutboxStore(),
            null,
            null,
            null);
    assertNotNull(factory);
  }

  @Test
  void transactionalOutboxStore_bindsAPostgresStoreDeclaredThroughTheInterface() {
    org.streamrune.core.outbox.OutboxStore store = mock(PostgresOutboxStore.class);
    assertSame(
        store,
        StreamRuneMicronautModule.transactionalOutboxStore(mock(OutboxEventMapper.class), store));
    assertNull(StreamRuneMicronautModule.transactionalOutboxStore(null, store));
    assertNull(
        StreamRuneMicronautModule.transactionalOutboxStore(mock(OutboxEventMapper.class), null));
  }

  @Test
  void outboxRetentionSweeperBuilt() {
    var module = new StreamRuneMicronautModule();
    var outboxStore = mock(PostgresOutboxStore.class);
    var sweeper =
        module.outboxRetentionSweeper(
            outboxStore, StreamRuneMicronautProperties.withDefaults(), null, null);
    assertNotNull(sweeper);
  }

  @Test
  void outboxRetentionSweeperWiresMetricsBean_whenPresent() {
    var module = new StreamRuneMicronautModule();
    var outboxStore = mock(PostgresOutboxStore.class);
    var metrics = mock(org.streamrune.core.StreamRuneMetrics.class);
    var sweeper =
        module.outboxRetentionSweeper(
            outboxStore, StreamRuneMicronautProperties.withDefaults(), metrics, null);
    assertNotNull(sweeper);
  }

  @Test
  void inboxRetentionSweeperBuilt() {
    var module = new StreamRuneMicronautModule();
    var commandInbox = mock(org.streamrune.postgres.PostgresCommandInbox.class);
    var sweeper =
        module.inboxRetentionSweeper(
            commandInbox, StreamRuneMicronautProperties.withDefaults(), null, null);
    assertNotNull(sweeper);
  }

  @Test
  void inboxRetentionSweeperWiresMetricsBean_whenPresent() {
    var module = new StreamRuneMicronautModule();
    var commandInbox = mock(org.streamrune.postgres.PostgresCommandInbox.class);
    var metrics = mock(org.streamrune.core.StreamRuneMetrics.class);
    var sweeper =
        module.inboxRetentionSweeper(
            commandInbox, StreamRuneMicronautProperties.withDefaults(), metrics, null);
    assertNotNull(sweeper);
  }

  @Test
  void sagaDeadLetterRetentionSweeperBuilt() {
    var module = new StreamRuneMicronautModule();
    var store = mock(org.streamrune.postgres.PostgresSagaDeadLetterStore.class);
    var sweeper =
        module.sagaDeadLetterRetentionSweeper(
            store, StreamRuneMicronautProperties.withDefaults(), null, null);
    assertNotNull(sweeper);
  }

  @Test
  void sagaDeadLetterRetentionSweeperWiresMetricsBean_whenPresent() {
    var module = new StreamRuneMicronautModule();
    var store = mock(org.streamrune.postgres.PostgresSagaDeadLetterStore.class);
    var metrics = mock(org.streamrune.core.StreamRuneMetrics.class);
    var sweeper =
        module.sagaDeadLetterRetentionSweeper(
            store, StreamRuneMicronautProperties.withDefaults(), metrics, null);
    assertNotNull(sweeper);
  }

  @Test
  void deadLetterRetentionSweeperBuilt() {
    var module = new StreamRuneMicronautModule();
    var dlq = mock(org.streamrune.core.DeadLetterQueue.class);
    var sweeper =
        module.deadLetterRetentionSweeper(
            dlq, StreamRuneMicronautProperties.withDefaults(), null, null);
    assertNotNull(sweeper);
  }

  @Test
  void deadLetterRetentionSweeperWiresMetricsBean_whenPresent() {
    var module = new StreamRuneMicronautModule();
    var dlq = mock(org.streamrune.core.DeadLetterQueue.class);
    var metrics = mock(org.streamrune.core.StreamRuneMetrics.class);
    var sweeper =
        module.deadLetterRetentionSweeper(
            dlq, StreamRuneMicronautProperties.withDefaults(), metrics, null);
    assertNotNull(sweeper);
  }

  @Test
  void autoInitializeSchemaProperty_defaultsTrue() {
    var props = StreamRuneMicronautProperties.withDefaults();
    assertTrue(props.autoInitializeSchema(), "autoInitializeSchema must default to true");
  }

  @Test
  void outboxRetentionMaxAgeProperty_defaults7Days() {
    var props = StreamRuneMicronautProperties.withDefaults();
    assertEquals(
        java.time.Duration.ofDays(7),
        props.outboxRetentionMaxAge(),
        "outboxRetentionMaxAge must default to 7 days");
  }

  @Test
  void inboxRetentionMaxAgeProperty_defaults7Days() {
    var props = StreamRuneMicronautProperties.withDefaults();
    assertEquals(
        java.time.Duration.ofDays(7),
        props.inboxRetentionMaxAge(),
        "inboxRetentionMaxAge must default to 7 days");
  }

  @Test
  void sagaDeadLetterRetentionMaxAgeProperty_defaults7Days() {
    // Defaults to the inbox retention window (7d), not longer — a dead-letter must not
    // outlive the inbox dedup keys its replay re-derives.
    var props = StreamRuneMicronautProperties.withDefaults();
    assertEquals(
        java.time.Duration.ofDays(7),
        props.sagaDeadLetterRetentionMaxAge(),
        "sagaDeadLetterRetentionMaxAge must default to 7 days");
  }

  @Test
  void deadLetterRetentionMaxAgeProperty_defaults30Days() {
    var props = StreamRuneMicronautProperties.withDefaults();
    assertEquals(
        java.time.Duration.ofDays(30),
        props.deadLetterRetentionMaxAge(),
        "deadLetterRetentionMaxAge must default to 30 days");
  }

  /**
   * Reflectively reads a private field — used to assert wiring the runner exposes no getter for.
   */
  private static Object getField(Object target, String fieldName) {
    try {
      var field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
