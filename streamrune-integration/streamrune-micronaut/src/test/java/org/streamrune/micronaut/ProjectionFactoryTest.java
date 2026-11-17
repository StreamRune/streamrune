package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import io.micronaut.context.BeanProvider;
import io.micronaut.context.env.Environment;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.integration.ProjectionRoutes;
import org.streamrune.integration.ProjectionStoreRouting;
import org.streamrune.integration.ReadModelStores;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.ScheduledProjectionRunner;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;

/** Tests for {@link ProjectionFactory}. */
class ProjectionFactoryTest {

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder().registerEvent(Ping.class).build();

  // --- Concrete test projections ---

  @ProjectionConfig(name = "mcont", deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class ContProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "msched",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class SchedProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "minline",
      mode = ProjectionConfig.Mode.INLINE,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class InlineProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  // --- Helpers ---

  private static Environment defaultEnv() {
    Environment env = mock(Environment.class);
    when(env.getProperty(anyString(), eq(Boolean.class))).thenReturn(Optional.empty());
    when(env.getProperty(anyString(), eq(String.class))).thenReturn(Optional.empty());
    return env;
  }

  // --- Tests: MultiProjectionRunner ---

  @Test
  void pollingPropertiesDriveSubscriptionConfig() throws Exception {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);
    var props =
        new StreamRuneMicronautProperties(
            100,
            3,
            Duration.ofMillis(50),
            2.0,
            Duration.ofMillis(123),
            Duration.ofMillis(45),
            Duration.ofSeconds(5),
            1024,
            10_000,
            5,
            Duration.ofSeconds(30),
            false,
            false,
            true,
            Duration.ofDays(30),
            false,
            100,
            Duration.ofSeconds(5),
            true,
            5,
            Duration.ofMinutes(1),
            Duration.ofDays(30),
            true,
            "streamrune",
            true,
            true,
            true,
            Duration.ofDays(7),
            Duration.ofDays(7),
            List.of(),
            Duration.ofDays(30),
            "true",
            10,
            Duration.ofSeconds(1),
            2.0,
            Duration.ofSeconds(15),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            // event-store.statement-timeout
            java.time.Duration.ofSeconds(30),
            // sse.polling-interval
            Duration.ofSeconds(1),
            "auto",
            "required",
            "operator-only",
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            true,
            null);

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new ContProjection()),
            eventStore,
            routes(
                List.of(new ContProjection()),
                noStores(),
                subscriptionCheckpoint(offsetStore),
                null,
                defaultEnv()),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            props,
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    var field = MultiProjectionRunner.class.getDeclaredField("subscriptionConfig");
    field.setAccessible(true);
    var config = (SubscriptionConfig) field.get(runner);
    assertEquals(Duration.ofMillis(123), config.pollingInterval());
    assertEquals(Duration.ofMillis(45), config.pollingJitter());
    runner.close();
  }

  @Test
  void continuousProjectionProducesMultiProjectionRunner() {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new ContProjection()),
            eventStore,
            routes(
                List.of(new ContProjection()),
                noStores(),
                subscriptionCheckpoint(offsetStore),
                null,
                defaultEnv()),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  @Test
  void scheduledProjectionProducesScheduledProjectionRunner() {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    ScheduledProjectionRunner runner =
        factory.scheduledProjectionRunner(
            List.of(new SchedProjection()),
            eventStore,
            routes(
                List.of(new SchedProjection()),
                noStores(),
                subscriptionCheckpoint(offsetStore),
                null,
                defaultEnv()),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  @Test
  void inlineProjectionProducesInlineProjectionInterceptor() {
    var factory = new ProjectionFactory();

    InlineProjectionInterceptor interceptor =
        factory.inlineProjectionInterceptor(
            List.of(new InlineProjection()),
            defaultEnv(),
            null,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(interceptor);
  }

  @Test
  void inlineProjectionDoesNotThrowWhileWarningAboutTheMissingRecoveryPath() {
    // An INLINE projection has no automatic recovery path if process()
    // throws — the command still reports SUCCESS and that command's read-model update is silently
    // and permanently lost. Structurally unreachable to fix within this factory (it needs a
    // streamrune-core ProjectionConfig API change to let one bean register under more than one
    // mode), so the framework surfaces the risk with a boot-time WARN instead. When this was
    // written, this module's test environment bound SLF4J to a NOP logger (confirmed empirically
    // with a throwaway probe test, not committed), so no appender/handler-based capture could
    // observe the actual log call here — unlike the Spring (Logback ListAppender) and Quarkus (JUL
    // Handler under JBoss LogManager) siblings of this same fix, both of which DO assert on the
    // captured message (logback-classic joined this module's test classpath later, for the
    // SseController WARN tests). This asserts what was verifiable then: the warning call does not
    // throw (proving the production path executes cleanly end to end), and the exact message it
    // would have produced.
    var factory = new ProjectionFactory();

    InlineProjectionInterceptor interceptor =
        assertDoesNotThrow(
            () ->
                factory.inlineProjectionInterceptor(
                    List.of(new InlineProjection()),
                    defaultEnv(),
                    null,
                    org.streamrune.core.tracing.StreamRuneTracing.NOOP));
    assertNotNull(interceptor);

    String formatted =
        org.slf4j.helpers.MessageFormatter.format(
                ProjectionFactory.INLINE_NO_RECOVERY_WARNING_TEMPLATE, "minline")
            .getMessage();
    assertTrue(formatted.contains("minline"));
    assertTrue(formatted.contains("INLINE"));
    assertTrue(formatted.contains("recovery"));
    assertTrue(formatted.contains("silently"));
  }

  @Test
  void noMatchingProjectionsThrowsForMultiRunner() {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    // Provide a SCHEDULED projection — should NOT match CONTINUOUS. The bean condition prevents
    // this in a container; calling the factory method directly must fail loudly, not return null.
    assertThrows(
        IllegalStateException.class,
        () ->
            factory.multiProjectionRunner(
                List.of(new SchedProjection()),
                eventStore,
                routes(
                    List.of(new SchedProjection()),
                    noStores(),
                    subscriptionCheckpoint(offsetStore),
                    null,
                    defaultEnv()),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                defaultEnv(),
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  // --- Fixtures: one per delivery-policy row the integration can reach ---

  @ProjectionConfig(name = "tl_plain", deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class TlPlainProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(name = "tl_base", deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class TlBaseProjection extends BaseProjection {
    TlBaseProjection(ProjectionRepository repo) {
      super(repo, "tl_base");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      save("row", "v");
    }
  }

  @ProjectionConfig(
      name = "alo_base",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class AloBaseProjection extends BaseProjection {
    AloBaseProjection(ProjectionRepository repo) {
      super(repo, "alo_base");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      save("row", "v");
    }
  }

  @ProjectionConfig(
      name = "inline_tl",
      mode = ProjectionConfig.Mode.INLINE,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class InlineTlProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "msched_tl",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class SchedTlPlainProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "msched_tl_base",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class SchedTlBaseProjection extends BaseProjection {
    SchedTlBaseProjection(ProjectionRepository repo) {
      super(repo, "msched_tl_base");
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      save("row", "v");
    }
  }

  record Ping(int n) implements org.streamrune.core.DomainEvent {}

  /** Records the delivery-mode gauge per projection name. */
  static final class RecordingMetrics implements StreamRuneMetrics {
    final Map<String, ProjectionDeliveryMode> modes = new ConcurrentHashMap<>();

    @Override
    public void recordProjectionDeliveryMode(
        ProjectionName projectionName, ProjectionDeliveryMode mode) {
      modes.put(projectionName.value(), mode);
    }
  }

  /**
   * A context holding these stores, as the factory collects them ({@code getBeansOfType}, which
   * also sees a store registered as a runtime singleton).
   */
  /** The application's stores, with no default store (no DataSource here). */
  private static ReadModelStores stores(ReadModelStore... stores) {
    return new ReadModelStores(java.util.Optional::empty, () -> List.of(stores));
  }

  private static ReadModelStores noStores() {
    return stores();
  }

  /**
   * The routes bean over these inputs, computed when the runner asks for it — as Micronaut creates
   * the singleton on the runner's {@code get()}.
   */
  private static BeanProvider<ProjectionRoutes> routes(
      List<Projection> projections,
      ReadModelStores stores,
      CheckpointTarget subscriptionCheckpoint,
      SubscriptionLeadership leadership,
      Environment env) {
    return () ->
        new ProjectionFactory()
            .projectionRoutes(projections, env, stores, subscriptionCheckpoint, leadership);
  }

  /** A store whose repository, checkpoints and processor are one in-memory repository. */
  private static ReadModelStore memoryStore(String name, InMemoryProjectionRepository repo) {
    return ReadModelStore.of(name, repo, repo, repo);
  }

  /**
   * The subscription checkpoint over {@code offsets} with the nonatomic processor — what {@code
   * OffsetStore#subscriptionCheckpoint()} yields for any offset store but a {@code
   * PostgresOffsetStore}. Built here, so an offset-store mock needs no stubbing.
   */
  private static CheckpointTarget subscriptionCheckpoint(OffsetStore offsets) {
    return new CheckpointTarget(
        OffsetStore.SUBSCRIPTION_CHECKPOINT, offsets, AtomicBatchProcessor.nonAtomicAtLeastOnce());
  }

  private static MultiProjectionRunner multi(
      List<Projection> projections,
      EventStore eventStore,
      ReadModelStores stores,
      CheckpointTarget subscriptionCheckpoint,
      SubscriptionLeadership leadership) {
    return new ProjectionFactory()
        .multiProjectionRunner(
            projections,
            eventStore,
            routes(projections, stores, subscriptionCheckpoint, leadership, defaultEnv()),
            Collections.emptyList(),
            null,
            leadership,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
  }

  private static MultiProjectionRunner multi(
      List<Projection> projections, ReadModelStores stores, SubscriptionLeadership leadership) {
    return multi(
        projections,
        mock(EventStore.class),
        stores,
        subscriptionCheckpoint(mock(OffsetStore.class)),
        leadership);
  }

  private static ScheduledProjectionRunner scheduled(
      List<Projection> projections, ReadModelStores stores) {
    return new ProjectionFactory()
        .scheduledProjectionRunner(
            projections,
            mock(EventStore.class),
            routes(
                projections,
                stores,
                subscriptionCheckpoint(mock(OffsetStore.class)),
                null,
                defaultEnv()),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
  }

  private static InMemoryEventStore oneEvent() {
    var store = new InMemoryEventStore(TYPES);
    var stream = StreamId.of(AggregateType.of("ping"), AggregateId.of("s"));
    store.append(
        stream,
        List.of(
            EventStoreFixture.event(stream, new Version(1), new EventType("Ping"), new Ping(1))),
        new Version(0));
    return store;
  }

  /** The processor of the pair a registration was routed to. */
  private static Object processorOf(Object runner, String projection) throws Exception {
    return pairPart(runner, projection, "processor");
  }

  /** The checkpoints of the pair a registration was routed to. */
  private static Object checkpointsOf(Object runner, String projection) throws Exception {
    return pairPart(runner, projection, "checkpoints");
  }

  /** The name of the target a registration was routed to. */
  private static String targetNameOf(Object runner, String projection) throws Exception {
    return (String) pairPart(runner, projection, "targetName");
  }

  /**
   * Reads the runner's private per-registration pairs, as MicronautReadModelStoreWiringTest does.
   */
  private static Object pairPart(Object runner, String projection, String part) throws Exception {
    Field field = runner.getClass().getDeclaredField("checkpoints");
    field.setAccessible(true);
    Object pair = ((Map<?, ?>) field.get(runner)).get(ProjectionName.of(projection));
    assertNotNull(pair, "the routed pair of " + projection);
    java.lang.reflect.Method accessor = pair.getClass().getDeclaredMethod(part);
    accessor.setAccessible(true);
    return accessor.invoke(pair);
  }

  // --- Tests: each registration is routed on its own ---

  @Test
  void tlProjection_withoutAStore_throwsNamingTheProjection() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> multi(List.of(new TlPlainProjection()), noStores(), null));
    assertTrue(
        ex.getMessage().contains("projection 'tl_plain' (TRANSACTIONAL_LOCAL)"), ex.getMessage());
    assertTrue(ex.getMessage().contains("no read-model store is transactional"), ex.getMessage());
  }

  @Test
  void tlBaseProjection_overTheStoresRepository_isRoutedToTheStore() throws Exception {
    var repo = new InMemoryProjectionRepository();
    try (var runner =
        multi(List.of(new TlBaseProjection(repo)), stores(memoryStore("memory", repo)), null)) {
      assertNotNull(runner);
      assertSame(repo, processorOf(runner, "tl_base"));
      assertSame(repo, checkpointsOf(runner, "tl_base"));
      assertEquals("memory", targetNameOf(runner, "tl_base"));
    }
  }

  @Test
  void tlPlainProjection_onTheOnlyTransactionalStore_throws_doesNotWriteThrough() {
    var repo = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                multi(List.of(new TlPlainProjection()), stores(memoryStore("memory", repo)), null));
    assertTrue(
        ex.getMessage().contains("does not write through the handed repository"), ex.getMessage());
  }

  @Test
  void tlBaseProjection_overARepositoryOfNoStore_throws() {
    var repoA = new InMemoryProjectionRepository();
    var repoB = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    List.of(new TlBaseProjection(repoB)),
                    stores(memoryStore("memory", repoA)),
                    null));
    assertTrue(
        ex.getMessage().contains("projection 'tl_base' (TRANSACTIONAL_LOCAL)"), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("its writeTarget() belongs to no read-model store"),
        ex.getMessage());
  }

  @Test
  void tlBaseProjection_overAJdkProxyOfTheStoresRepository_isMatchedThroughTheProxyAndBoots()
      throws Exception {
    var repo = new InMemoryProjectionRepository();
    Object proxy =
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {ProjectionRepository.class, AtomicBatchProcessor.class},
            (p, m, a) -> m.invoke(repo, a));
    try (var runner =
        multi(
            List.of(new TlBaseProjection((ProjectionRepository) proxy)),
            stores(
                ReadModelStore.of(
                    "memory", (ProjectionRepository) proxy, repo, (AtomicBatchProcessor) proxy)),
            null)) {
      assertNotNull(runner);
      assertSame(
          proxy,
          processorOf(runner, "tl_base"),
          "the forwarding proxy is the registration's processor");
    }
  }

  @Test
  void aloBaseProjection_overASecondRepository_bootsBesideTl_andCheckpointsInTheSubscription() {
    var repoA = new InMemoryProjectionRepository(); // the store
    var repoB = new InMemoryProjectionRepository(); // the at-least-once projection's own repository
    var subscription = new InMemoryOffsetStore();
    var runner =
        multi(
            List.of(new TlBaseProjection(repoA), new AloBaseProjection(repoB)),
            oneEvent(),
            stores(memoryStore("memory", repoA)),
            subscriptionCheckpoint(subscription),
            null);
    runner.start();
    try (runner) {
      // Each registration runs on a thread of its own, and a batch's read-model write lands before
      // its checkpoint: wait for every outcome, not only the first one.
      Awaitility.await()
          .atMost(Duration.ofSeconds(10))
          .untilAsserted(
              () -> {
                assertEquals(
                    Optional.of("v"),
                    repoB.findById(ProjectionName.of("alo_base"), "row", String.class),
                    "the at-least-once projection wrote to its own repository");
                assertEquals(
                    Optional.of("v"),
                    repoA.findById(ProjectionName.of("tl_base"), "row", String.class),
                    "the TRANSACTIONAL_LOCAL sibling wrote inside its store's transaction");
                assertEquals(
                    GlobalOffset.of(1),
                    repoA.committedOffset(ProjectionName.of("tl_base")),
                    "the TRANSACTIONAL_LOCAL checkpoint is in its store");
                assertEquals(
                    GlobalOffset.of(1),
                    subscription.getLastOffset(ProjectionName.of("alo_base")),
                    "the at-least-once checkpoint is in the subscription checkpoint");
              });
    }
    assertEquals(
        GlobalOffset.initial(),
        repoA.committedOffset(ProjectionName.of("alo_base")),
        "the at-least-once projection has no checkpoint in the store");
    assertEquals(
        GlobalOffset.initial(),
        subscription.getLastOffset(ProjectionName.of("tl_base")),
        "the TRANSACTIONAL_LOCAL projection has no checkpoint in the subscription checkpoint");
    assertFalse(
        repoA.hasReadModels(ProjectionName.of("alo_base")),
        "nothing of the at-least-once projection lands in the store");
    assertFalse(
        repoB.hasReadModels(ProjectionName.of("tl_base")),
        "nothing of the TRANSACTIONAL_LOCAL projection lands in the second repository");
  }

  @Test
  void aloWithNoWriteTarget_isRoutedToTheSubscriptionCheckpoint_evenBesideAStore()
      throws Exception {
    var offsets = new InMemoryOffsetStore();
    try (var runner =
        multi(
            List.of(new ContProjection()),
            mock(EventStore.class),
            stores(memoryStore("memory", new InMemoryProjectionRepository())),
            subscriptionCheckpoint(offsets),
            null)) {
      assertSame(
          AtomicBatchProcessor.nonAtomicAtLeastOnce(),
          processorOf(runner, "mcont"),
          "routing selects the subscription checkpoint; the store's presence selects nothing");
      assertSame(offsets, checkpointsOf(runner, "mcont"));
      assertEquals("subscription_checkpoint", targetNameOf(runner, "mcont"));
    }
  }

  @Test
  void aloOnly_realLeadership_aNonFencingSubscriptionCheckpointThrows() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    List.of(new ContProjection()), noStores(), mock(SubscriptionLeadership.class)));
    assertTrue(
        ex.getMessage().contains("projection 'mcont' (AT_LEAST_ONCE_IDEMPOTENT)"), ex.getMessage());
    assertTrue(ex.getMessage().contains("is routed to 'subscription_checkpoint'"), ex.getMessage());
    assertTrue(ex.getMessage().contains("does not fence its checkpoint"), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("streamrune.subscription.single-active-consumer.enabled=false"),
        ex.getMessage());
  }

  @Test
  void aloOnly_realLeadership_aFencingSubscriptionCheckpointIsUsed() throws Exception {
    var fencing = new InMemoryProjectionRepository();
    try (var runner =
        multi(
            List.of(new ContProjection()),
            mock(EventStore.class),
            noStores(),
            new CheckpointTarget(OffsetStore.SUBSCRIPTION_CHECKPOINT, fencing, fencing),
            mock(SubscriptionLeadership.class))) {
      assertSame(fencing, processorOf(runner, "mcont"));
      assertEquals("subscription_checkpoint", targetNameOf(runner, "mcont"));
    }
  }

  @Test
  void aloBase_overAStoresRepository_realLeadership_isRoutedToTheFencingStore() throws Exception {
    var repo = new InMemoryProjectionRepository();
    try (var runner =
        multi(
            List.of(new AloBaseProjection(repo)),
            stores(memoryStore("memory", repo)),
            mock(SubscriptionLeadership.class))) {
      assertSame(repo, processorOf(runner, "alo_base"));
      assertEquals("memory", targetNameOf(runner, "alo_base"));
    }
  }

  @Test
  void tlPlain_withTwoTransactionalStores_throwsAskingForADeclaration() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    List.of(new TlPlainProjection()),
                    stores(
                        memoryStore("a", new InMemoryProjectionRepository()),
                        memoryStore("b", new InMemoryProjectionRepository())),
                    null));
    assertTrue(
        ex.getMessage().contains("several read-model stores are transactional ('a', 'b')"),
        ex.getMessage());
    assertTrue(ex.getMessage().contains("readModelStore = \"<name>\""), ex.getMessage());
  }

  /**
   * The factory collects every store bean; two under one name are refused, never resolved by
   * picking the first (the same fail-loudly rule as the dead-letter store's {@code
   * resolveSingleDlq}).
   */
  @Test
  void twoStoresWithOneName_throw() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    List.of(new ContProjection()),
                    stores(
                        memoryStore("memory", new InMemoryProjectionRepository()),
                        memoryStore("memory", new InMemoryProjectionRepository())),
                    null));
    assertTrue(
        ex.getMessage().contains("Two read-model stores are named 'memory'"), ex.getMessage());
  }

  @ProjectionConfig(
      name = "declared",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
      readModelStore = "memory")
  static class DeclaredStoreProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @Test
  void aDeclaredStore_routesThere_evenForAnAtLeastOnceProjectionWithNoWriteTarget()
      throws Exception {
    var repo = new InMemoryProjectionRepository();
    try (var runner =
        multi(List.of(new DeclaredStoreProjection()), stores(memoryStore("memory", repo)), null)) {
      assertSame(repo, processorOf(runner, "declared"));
      assertEquals("memory", targetNameOf(runner, "declared"));
    }
  }

  @Test
  void anUnknownDeclaredStore_throwsNamingTheStores() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    List.of(new DeclaredStoreProjection()),
                    stores(memoryStore("postgres", new InMemoryProjectionRepository())),
                    null));
    assertTrue(
        ex.getMessage().contains("declares readModelStore = \"memory\", but no read-model store"),
        ex.getMessage());
    assertTrue(ex.getMessage().contains("'postgres'"), ex.getMessage());
  }

  @Test
  void inlineTl_throwsAtDiscovery() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                new ProjectionFactory()
                    .inlineProjectionInterceptor(
                        List.of(new InlineTlProjection()),
                        defaultEnv(),
                        null,
                        org.streamrune.core.tracing.StreamRuneTracing.NOOP));
    assertEquals(
        "projection 'inline_tl' is INLINE and declares TRANSACTIONAL_LOCAL: INLINE runs post-commit on the"
            + " command thread with no checkpoint and no transaction of its own. Declare"
            + " AT_LEAST_ONCE_IDEMPOTENT, or run it CONTINUOUS or SCHEDULED.",
        ex.getMessage());
  }

  @Test
  void inlineAlo_boots_andRecordsTheGauge() {
    // Captured through the logback ListAppender this module's other log-assertion tests use.
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(ProjectionFactory.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    var metrics = new RecordingMetrics();
    try {
      assertNotNull(
          new ProjectionFactory()
              .inlineProjectionInterceptor(
                  List.of(new InlineProjection()),
                  defaultEnv(),
                  metrics,
                  org.streamrune.core.tracing.StreamRuneTracing.NOOP));
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(null);
    }
    assertEquals(Map.of("minline", ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT), metrics.modes);
    String info = ProjectionStoreRouting.INLINE_INFO.replace("{}", "minline");
    List<String> captured =
        appender.list.stream()
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .toList();
    assertEquals(
        1,
        captured.stream().filter(info::equals).count(),
        () -> "one INFO line " + info + "; captured: " + captured);
  }

  @Test
  void theCoarsePropertyIsNoLongerRead() {
    var repo = new InMemoryProjectionRepository();
    Environment env = mock(Environment.class);
    when(env.getProperty(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(env.getProperty("streamrune.projections.allow-at-least-once", String.class))
        .thenReturn(Optional.of("true"));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new ProjectionFactory()
                    .multiProjectionRunner(
                        List.of(new TlPlainProjection()),
                        mock(EventStore.class),
                        routes(
                            List.of(new TlPlainProjection()),
                            stores(memoryStore("memory", repo)),
                            subscriptionCheckpoint(repo),
                            null,
                            env),
                        Collections.emptyList(),
                        null,
                        null,
                        null,
                        null,
                        StreamRuneMicronautProperties.withDefaults(),
                        env,
                        org.streamrune.core.tracing.StreamRuneTracing.NOOP));
    assertTrue(ex.getMessage().contains("does not write through"), ex.getMessage());
  }

  @Test
  void scheduledTlPlain_throws_andScheduledAloBoots() throws Exception {
    var repo = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                scheduled(
                    List.of(new SchedTlPlainProjection()), stores(memoryStore("memory", repo))));
    assertTrue(ex.getMessage().contains("'msched_tl'"), ex.getMessage());
    assertTrue(ex.getMessage().contains("does not write through"), ex.getMessage());

    try (ScheduledProjectionRunner runner =
        scheduled(List.of(new SchedProjection()), stores(memoryStore("memory", repo)))) {
      assertNotNull(runner);
      assertSame(
          AtomicBatchProcessor.nonAtomicAtLeastOnce(),
          processorOf(runner, "msched"),
          "an at-least-once scheduled projection with no write target is not given the store");
      assertEquals("subscription_checkpoint", targetNameOf(runner, "msched"));
    }
  }

  @Test
  void scheduledTlBase_overTheStoresRepository_isRoutedToTheStore() throws Exception {
    var repo = new InMemoryProjectionRepository();
    try (ScheduledProjectionRunner runner =
        scheduled(List.of(new SchedTlBaseProjection(repo)), stores(memoryStore("memory", repo)))) {
      assertSame(repo, processorOf(runner, "msched_tl_base"));
      assertEquals("memory", targetNameOf(runner, "msched_tl_base"));
    }
  }

  @Test
  void noMatchingProjectionsThrowsForScheduledRunner() {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    // Provide a CONTINUOUS projection — should NOT match SCHEDULED
    assertThrows(
        IllegalStateException.class,
        () ->
            factory.scheduledProjectionRunner(
                List.of(new ContProjection()),
                eventStore,
                routes(
                    List.of(new ContProjection()),
                    noStores(),
                    subscriptionCheckpoint(offsetStore),
                    null,
                    defaultEnv()),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                defaultEnv(),
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  @Test
  void noMatchingProjectionsThrowsForInlineInterceptor() {
    var factory = new ProjectionFactory();

    // Provide a CONTINUOUS projection — should NOT match INLINE
    assertThrows(
        IllegalStateException.class,
        () ->
            factory.inlineProjectionInterceptor(
                List.of(new ContProjection()),
                defaultEnv(),
                null,
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  @Test
  void emptyListThrowsForMultiRunner() {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    assertThrows(
        IllegalStateException.class,
        () ->
            factory.multiProjectionRunner(
                Collections.emptyList(),
                eventStore,
                routes(
                    Collections.emptyList(),
                    noStores(),
                    subscriptionCheckpoint(offsetStore),
                    null,
                    defaultEnv()),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                defaultEnv(),
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  @Test
  void scheduledWithoutCronThrowsIllegalStateException() {
    var factory = new ProjectionFactory();

    @ProjectionConfig(
        name = "mbadscheduled",
        mode = ProjectionConfig.Mode.SCHEDULED,
        deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    class NoCronProjection implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {}
    }

    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    assertThrows(
        IllegalStateException.class,
        () ->
            factory.scheduledProjectionRunner(
                List.of(new NoCronProjection()),
                eventStore,
                routes(
                    List.of(new NoCronProjection()),
                    noStores(),
                    subscriptionCheckpoint(offsetStore),
                    null,
                    defaultEnv()),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                defaultEnv(),
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  @Test
  void duplicateProjectionNameThrowsIllegalStateException() {
    var factory = new ProjectionFactory();

    @ProjectionConfig(
        name = "mcont",
        deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    class DuplicateCont implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {}
    }

    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    // Both are CONTINUOUS with the same name "mcont"
    assertThrows(
        IllegalStateException.class,
        () ->
            factory.multiProjectionRunner(
                List.of(new ContProjection(), new DuplicateCont()),
                eventStore,
                routes(
                    List.of(new ContProjection(), new DuplicateCont()),
                    noStores(),
                    subscriptionCheckpoint(offsetStore),
                    null,
                    defaultEnv()),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                defaultEnv(),
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  @Test
  void disabledProjectionIsSkipped() {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    Environment env = mock(Environment.class);
    when(env.getProperty(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(env.getProperty("streamrune.projections.mcont.enabled", String.class))
        .thenReturn(Optional.of("false"));

    assertThrows(
        IllegalStateException.class,
        () ->
            factory.multiProjectionRunner(
                List.of(new ContProjection()),
                eventStore,
                routes(
                    List.of(new ContProjection()),
                    noStores(),
                    subscriptionCheckpoint(offsetStore),
                    null,
                    env),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                env,
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  /**
   * A non-canonical positive value ({@code on}) must ENABLE the projection — identical to the
   * Spring and Quarkus integrations. Micronaut's typed {@code Boolean} converter already read
   * {@code on} as {@code true}, but Spring/Quarkus did not until all three were routed through the
   * shared {@link org.streamrune.integration.RelaxedBoolean} parser.
   */
  @Test
  void nonCanonicalOnEnablesProjection() {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    Environment env = mock(Environment.class);
    when(env.getProperty(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(env.getProperty("streamrune.projections.mcont.enabled", String.class))
        .thenReturn(Optional.of("on"));

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new ContProjection()),
            eventStore,
            routes(
                List.of(new ContProjection()),
                noStores(),
                subscriptionCheckpoint(offsetStore),
                null,
                env),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            env,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  @Test
  void errorStrategyOverrideFromConfig() {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    // Override to SKIP (default is HALT from @ProjectionConfig)
    Environment env = mock(Environment.class);
    when(env.getProperty(anyString(), eq(Boolean.class))).thenReturn(Optional.empty());
    when(env.getProperty(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(env.getProperty("streamrune.projections.mcont.error-strategy", String.class))
        .thenReturn(Optional.of("SKIP"));

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new ContProjection()),
            eventStore,
            routes(
                List.of(new ContProjection()),
                noStores(),
                subscriptionCheckpoint(offsetStore),
                null,
                env),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            env,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  @Test
  void dlqStrategyWithoutDlqBeanThrowsIllegalStateException() {
    @ProjectionConfig(
        name = "mdlq",
        errorStrategy = ProjectionErrorStrategy.DLQ,
        deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    class DlqProjection implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {}
    }

    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    assertThrows(
        IllegalStateException.class,
        () ->
            factory.multiProjectionRunner(
                List.of(new DlqProjection()),
                eventStore,
                routes(
                    List.of(new DlqProjection()),
                    noStores(),
                    subscriptionCheckpoint(offsetStore),
                    null,
                    defaultEnv()),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                defaultEnv(),
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  // --- multiple ProjectionDeadLetterStore beans must fail loudly, not silently pick one ---

  @ProjectionConfig(
      name = "mdlqmulti",
      errorStrategy = ProjectionErrorStrategy.DLQ,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class MultiDlqProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "sdlqmulti",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      errorStrategy = ProjectionErrorStrategy.DLQ,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class MultiDlqScheduledProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @Test
  void multipleDlqStoresThrowLoudly_multiRunner() {
    var factory = new ProjectionFactory();
    var dlqA = mock(org.streamrune.core.projection.ProjectionDeadLetterStore.class);
    var dlqB = mock(org.streamrune.core.projection.ProjectionDeadLetterStore.class);

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                factory.multiProjectionRunner(
                    List.of(new MultiDlqProjection()),
                    mock(EventStore.class),
                    routes(
                        List.of(new MultiDlqProjection()),
                        noStores(),
                        subscriptionCheckpoint(mock(OffsetStore.class)),
                        null,
                        defaultEnv()),
                    List.of(dlqA, dlqB),
                    null,
                    null,
                    null,
                    null,
                    StreamRuneMicronautProperties.withDefaults(),
                    defaultEnv(),
                    org.streamrune.core.tracing.StreamRuneTracing.NOOP));
    assertTrue(
        e.getMessage().contains("Multiple ProjectionDeadLetterStore"),
        "ambiguous DLQ beans must fail loudly, not silently pick dlqs.get(0)");
  }

  @Test
  void multipleDlqStoresThrowLoudly_scheduledRunner() {
    var factory = new ProjectionFactory();
    var dlqA = mock(org.streamrune.core.projection.ProjectionDeadLetterStore.class);
    var dlqB = mock(org.streamrune.core.projection.ProjectionDeadLetterStore.class);

    IllegalStateException e =
        assertThrows(
            IllegalStateException.class,
            () ->
                factory.scheduledProjectionRunner(
                    List.of(new MultiDlqScheduledProjection()),
                    mock(EventStore.class),
                    routes(
                        List.of(new MultiDlqScheduledProjection()),
                        noStores(),
                        subscriptionCheckpoint(mock(OffsetStore.class)),
                        null,
                        defaultEnv()),
                    List.of(dlqA, dlqB),
                    null,
                    null,
                    null,
                    null,
                    defaultEnv(),
                    org.streamrune.core.tracing.StreamRuneTracing.NOOP));
    assertTrue(e.getMessage().contains("Multiple ProjectionDeadLetterStore"));
  }

  @Test
  void singleDlqStoreIsWired_multiRunner() {
    var factory = new ProjectionFactory();
    var dlq = mock(org.streamrune.core.projection.ProjectionDeadLetterStore.class);

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new MultiDlqProjection()),
            mock(EventStore.class),
            routes(
                List.of(new MultiDlqProjection()),
                noStores(),
                subscriptionCheckpoint(mock(OffsetStore.class)),
                null,
                defaultEnv()),
            List.of(dlq),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    assertNotNull(runner, "a single DLQ store must wire cleanly (size==1 path)");
    runner.close();
  }

  @Test
  void proxySubclassAnnotationDiscovered() {
    // Simulate a Micronaut proxy subclass where the annotation is on the superclass
    class ProxiedCont extends ContProjection {}

    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new ProxiedCont()),
            eventStore,
            routes(
                List.of(new ProxiedCont()),
                noStores(),
                subscriptionCheckpoint(offsetStore),
                null,
                defaultEnv()),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  @Test
  void invalidErrorStrategyConfigThrowsIllegalStateException() {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    Environment env = mock(Environment.class);
    when(env.getProperty(anyString(), eq(Boolean.class))).thenReturn(Optional.empty());
    when(env.getProperty(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(env.getProperty("streamrune.projections.mcont.error-strategy", String.class))
        .thenReturn(Optional.of("BOGUS"));

    assertThrows(
        IllegalStateException.class,
        () ->
            factory.multiProjectionRunner(
                List.of(new ContProjection()),
                eventStore,
                routes(
                    List.of(new ContProjection()),
                    noStores(),
                    subscriptionCheckpoint(offsetStore),
                    null,
                    env),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                env,
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  // --- Tests: metrics wiring ---

  @Test
  void metricsBeanWiredIntoMultiProjectionRunner() throws Exception {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);
    var metrics = mock(org.streamrune.core.StreamRuneMetrics.class);

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new ContProjection()),
            eventStore,
            routes(
                List.of(new ContProjection()),
                noStores(),
                subscriptionCheckpoint(offsetStore),
                null,
                defaultEnv()),
            Collections.emptyList(),
            metrics,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    var field = MultiProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertSame(metrics, field.get(runner));
    runner.close();
  }

  @Test
  void absentMetricsLeavesMultiProjectionRunnerWithNoopMetrics() throws Exception {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new ContProjection()),
            eventStore,
            routes(
                List.of(new ContProjection()),
                noStores(),
                subscriptionCheckpoint(offsetStore),
                null,
                defaultEnv()),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    var field = MultiProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertSame(org.streamrune.core.StreamRuneMetrics.NOOP, field.get(runner));
    runner.close();
  }

  @Test
  void metricsBeanWiredIntoScheduledProjectionRunner() throws Exception {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);
    var metrics = mock(org.streamrune.core.StreamRuneMetrics.class);

    ScheduledProjectionRunner runner =
        factory.scheduledProjectionRunner(
            List.of(new SchedProjection()),
            eventStore,
            routes(
                List.of(new SchedProjection()),
                noStores(),
                subscriptionCheckpoint(offsetStore),
                null,
                defaultEnv()),
            Collections.emptyList(),
            metrics,
            null,
            null,
            null,
            defaultEnv(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    var field = ScheduledProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertSame(metrics, field.get(runner));
    runner.close();
  }
}
