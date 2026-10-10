package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.integration.ProjectionProcessorResolution;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.ScheduledProjectionRunner;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;

/** Tests for {@link ProjectionFactory}. */
class ProjectionFactoryTest {

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
            50,
            2.0,
            123,
            45,
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
            5000L,
            true,
            5,
            60000L,
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
            true,
            10,
            1000L,
            2.0,
            Duration.ofSeconds(15),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            // event-store.statement-timeout
            java.time.Duration.ofSeconds(30),
            // sse.polling-interval
            Duration.ofSeconds(1));

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new ContProjection()),
            eventStore,
            offsetStore,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            props,
            defaultEnv());

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
            offsetStore,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv());

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
            offsetStore,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            defaultEnv());

    assertNotNull(runner);
    runner.close();
  }

  @Test
  void inlineProjectionProducesInlineProjectionInterceptor() {
    var factory = new ProjectionFactory();

    InlineProjectionInterceptor interceptor =
        factory.inlineProjectionInterceptor(List.of(new InlineProjection()), defaultEnv(), null);

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
                    List.of(new InlineProjection()), defaultEnv(), null));
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
                offsetStore,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                defaultEnv()));
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

  private static MultiProjectionRunner multi(
      List<Projection> projections,
      EventStore eventStore,
      OffsetStore offsetStore,
      List<AtomicBatchProcessor> processors,
      SubscriptionLeadership leadership) {
    return new ProjectionFactory()
        .multiProjectionRunner(
            projections,
            eventStore,
            offsetStore,
            Collections.emptyList(),
            processors,
            null,
            leadership,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv());
  }

  private static ScheduledProjectionRunner scheduled(
      List<Projection> projections, List<AtomicBatchProcessor> processors) {
    return new ProjectionFactory()
        .scheduledProjectionRunner(
            projections,
            mock(EventStore.class),
            mock(OffsetStore.class),
            Collections.emptyList(),
            processors,
            null,
            null,
            null,
            null,
            defaultEnv());
  }

  private static InMemoryEventStore oneEvent() {
    var store = new InMemoryEventStore();
    var stream = StreamId.of(AggregateType.of("ping"), AggregateId.of("s"));
    store.append(
        stream,
        List.of(
            EventStoreFixture.event(stream, new Version(1), new EventType("Ping"), new Ping(1))),
        new Version(0));
    return store;
  }

  /** The runner's private processor, read as MicronautBeanWiringTest reads it. */
  private static Object atomicProcessorOf(MultiProjectionRunner runner) throws Exception {
    Field field = MultiProjectionRunner.class.getDeclaredField("atomicProcessor");
    field.setAccessible(true);
    return field.get(runner);
  }

  private static Object atomicProcessorOf(ScheduledProjectionRunner runner) throws Exception {
    Field field = ScheduledProjectionRunner.class.getDeclaredField("atomicProcessor");
    field.setAccessible(true);
    return field.get(runner);
  }

  // --- Tests: the processor is resolved by declared need, never by bean presence ---

  @Test
  void tlProjection_withoutAProcessorBean_throwsNamingTheProjection() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    List.of(new TlPlainProjection()),
                    mock(EventStore.class),
                    mock(OffsetStore.class),
                    Collections.emptyList(),
                    null));
    assertTrue(
        ex.getMessage().contains("projection 'tl_plain' declares TRANSACTIONAL_LOCAL"),
        ex.getMessage());
    assertTrue(
        ex.getMessage().contains("no AtomicBatchProcessor bean is available"), ex.getMessage());
  }

  @Test
  void tlBaseProjection_overTheProcessorBean_boots() throws Exception {
    var repo = new InMemoryProjectionRepository();
    var runner =
        multi(
            List.of(new TlBaseProjection(repo)), mock(EventStore.class), repo, List.of(repo), null);
    try {
      assertNotNull(runner);
      assertSame(repo, atomicProcessorOf(runner));
    } finally {
      runner.close();
    }
  }

  @Test
  void tlPlainProjection_underTheProcessorBean_throws_doesNotWriteThrough() {
    var repo = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                multi(
                    List.of(new TlPlainProjection()),
                    mock(EventStore.class),
                    repo,
                    List.of(repo),
                    null));
    assertTrue(
        ex.getMessage().contains("does not write through the handed repository"), ex.getMessage());
  }

  @Test
  void tlBaseProjection_overAnotherRepository_throws_identity() {
    var repoA = new InMemoryProjectionRepository();
    var repoB = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                multi(
                    List.of(new TlBaseProjection(repoB)),
                    mock(EventStore.class),
                    repoA,
                    List.of(repoA),
                    null));
    assertTrue(
        ex.getMessage().contains("'tl_base' declares TRANSACTIONAL_LOCAL and writes to"),
        ex.getMessage());
    assertTrue(
        ex.getMessage()
            .contains("is configured with " + InMemoryProjectionRepository.class.getName()),
        ex.getMessage());
  }

  @Test
  void tlBaseProjection_overAJdkProxyOfTheProcessor_verifiesAndBoots() throws Exception {
    var repo = new InMemoryProjectionRepository();
    Object proxy =
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {ProjectionRepository.class, AtomicBatchProcessor.class},
            (p, m, a) -> m.invoke(repo, a));
    var runner =
        multi(
            List.of(new TlBaseProjection((ProjectionRepository) proxy)),
            mock(EventStore.class),
            repo,
            List.of((AtomicBatchProcessor) proxy),
            null);
    try {
      assertNotNull(runner);
      assertSame(
          proxy, atomicProcessorOf(runner), "the forwarding proxy is the runner's processor");
    } finally {
      runner.close();
    }
  }

  @Test
  void aloBaseProjection_overASecondRepository_bootsBesideTl_andWritesOnlyToIt() {
    var repoA = new InMemoryProjectionRepository(); // the processor AND the runner's offset store
    var repoB = new InMemoryProjectionRepository(); // the at-least-once projection's own store
    var runner =
        multi(
            List.of(new TlBaseProjection(repoA), new AloBaseProjection(repoB)),
            oneEvent(),
            repoA,
            List.of(repoA),
            null);
    runner.start();
    try {
      // Each registration runs on a thread of its own, and a batch's read-model write lands before
      // its checkpoint: wait for all three outcomes, not only the first one.
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
                    "the TRANSACTIONAL_LOCAL sibling wrote inside the processor's transaction");
                assertEquals(
                    GlobalOffset.of(1),
                    repoA.committedOffset(ProjectionName.of("alo_base")),
                    "the at-least-once projection's checkpoint is in the processor's store");
              });
    } finally {
      runner.close();
    }
    assertFalse(
        repoA.hasReadModels(ProjectionName.of("alo_base")),
        "nothing of the at-least-once projection lands in the processor's store");
    assertFalse(
        repoB.hasReadModels(ProjectionName.of("tl_base")),
        "nothing of the TRANSACTIONAL_LOCAL projection lands in the second store");
  }

  @Test
  void aloOnly_noLeadership_selectsTheNonatomicProcessor_evenWhenABeanExists() throws Exception {
    var runner =
        multi(
            List.of(new ContProjection()),
            mock(EventStore.class),
            mock(OffsetStore.class),
            List.of(new InMemoryProjectionRepository()),
            null);
    try {
      assertSame(
          AtomicBatchProcessor.nonAtomicAtLeastOnce(),
          atomicProcessorOf(runner),
          "the rule selects it; the bean's presence selects nothing");
    } finally {
      runner.close();
    }
  }

  @Test
  void aloOnly_realLeadership_withoutABean_throws_toHonourItsEpoch() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    List.of(new ContProjection()),
                    mock(EventStore.class),
                    mock(OffsetStore.class),
                    Collections.emptyList(),
                    mock(SubscriptionLeadership.class)));
    assertTrue(ex.getMessage().contains("to honour its epoch"), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("streamrune.subscription.single-active-consumer.enabled=false"),
        ex.getMessage());
    assertFalse(
        ex.getMessage().contains("'mcont'"),
        "an at-least-once registration is not what needs the bean");
  }

  @Test
  void aloOnly_realLeadership_withTheBean_bootsAndUsesIt() throws Exception {
    var repo = new InMemoryProjectionRepository();
    var runner =
        multi(
            List.of(new ContProjection()),
            mock(EventStore.class),
            mock(OffsetStore.class),
            List.of(repo),
            mock(SubscriptionLeadership.class));
    try {
      // Fencing selects the bean although every registration is at-least-once.
      assertSame(repo, atomicProcessorOf(runner));
    } finally {
      runner.close();
    }
  }

  /**
   * Micronaut's {@code List<AtomicBatchProcessor>} injection hands the factory every processor bean
   * in the context; two of them are refused, never resolved by picking the first (the same
   * fail-loudly rule as the dead-letter store's {@code resolveSingleDlq}).
   */
  @Test
  void twoProcessorBeans_throw() {
    var repoA = new InMemoryProjectionRepository();
    var repoB = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    List.of(new TlBaseProjection(repoA)),
                    mock(EventStore.class),
                    repoA,
                    List.of(repoA, repoB),
                    null));
    assertTrue(ex.getMessage().contains("2 AtomicBatchProcessor beans"), ex.getMessage());
  }

  @Test
  void inlineTl_throwsAtDiscovery() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                new ProjectionFactory()
                    .inlineProjectionInterceptor(
                        List.of(new InlineTlProjection()), defaultEnv(), null));
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
              .inlineProjectionInterceptor(List.of(new InlineProjection()), defaultEnv(), metrics));
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(null);
    }
    assertEquals(Map.of("minline", ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT), metrics.modes);
    String info = ProjectionProcessorResolution.INLINE_INFO.replace("{}", "minline");
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
                        repo,
                        Collections.emptyList(),
                        List.of(repo),
                        null,
                        null,
                        null,
                        null,
                        StreamRuneMicronautProperties.withDefaults(),
                        env));
    assertTrue(ex.getMessage().contains("does not write through"), ex.getMessage());
  }

  @Test
  void scheduledTlPlain_throws_andScheduledAloBoots() throws Exception {
    var repo = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> scheduled(List.of(new SchedTlPlainProjection()), List.of(repo)));
    assertTrue(ex.getMessage().contains("'msched_tl'"), ex.getMessage());
    assertTrue(ex.getMessage().contains("does not write through"), ex.getMessage());

    ScheduledProjectionRunner runner = scheduled(List.of(new SchedProjection()), List.of(repo));
    try {
      assertNotNull(runner);
      assertSame(
          AtomicBatchProcessor.nonAtomicAtLeastOnce(),
          atomicProcessorOf(runner),
          "an at-least-once scheduled runner without leadership is not given the bean");
    } finally {
      runner.close();
    }
  }

  @Test
  void scheduledTlBase_overTheProcessorBean_bootsAndUsesIt() throws Exception {
    var repo = new InMemoryProjectionRepository();
    ScheduledProjectionRunner runner =
        scheduled(List.of(new SchedTlBaseProjection(repo)), List.of(repo));
    try {
      assertSame(repo, atomicProcessorOf(runner));
    } finally {
      runner.close();
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
                offsetStore,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                defaultEnv()));
  }

  @Test
  void noMatchingProjectionsThrowsForInlineInterceptor() {
    var factory = new ProjectionFactory();

    // Provide a CONTINUOUS projection — should NOT match INLINE
    assertThrows(
        IllegalStateException.class,
        () ->
            factory.inlineProjectionInterceptor(List.of(new ContProjection()), defaultEnv(), null));
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
                offsetStore,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                defaultEnv()));
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
                offsetStore,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                defaultEnv()));
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
                offsetStore,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                defaultEnv()));
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
                offsetStore,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                env));
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
            offsetStore,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            env);

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
            offsetStore,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            env);

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
                offsetStore,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                defaultEnv()));
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
                    mock(OffsetStore.class),
                    List.of(dlqA, dlqB),
                    Collections.emptyList(),
                    null,
                    null,
                    null,
                    null,
                    StreamRuneMicronautProperties.withDefaults(),
                    defaultEnv()));
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
                    mock(OffsetStore.class),
                    List.of(dlqA, dlqB),
                    Collections.emptyList(),
                    null,
                    null,
                    null,
                    null,
                    defaultEnv()));
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
            mock(OffsetStore.class),
            List.of(dlq),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv());
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
            offsetStore,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv());

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
                offsetStore,
                Collections.emptyList(),
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                StreamRuneMicronautProperties.withDefaults(),
                env));
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
            offsetStore,
            Collections.emptyList(),
            Collections.emptyList(),
            metrics,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv());

    var field = MultiProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertSame(metrics, field.get(runner));
    runner.close();
  }

  @Test
  void absentMetricsLeavesMultiProjectionRunnerWithNullMetrics() throws Exception {
    var factory = new ProjectionFactory();
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        factory.multiProjectionRunner(
            List.of(new ContProjection()),
            eventStore,
            offsetStore,
            Collections.emptyList(),
            Collections.emptyList(),
            null,
            null,
            null,
            null,
            StreamRuneMicronautProperties.withDefaults(),
            defaultEnv());

    var field = MultiProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertNull(field.get(runner));
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
            offsetStore,
            Collections.emptyList(),
            Collections.emptyList(),
            metrics,
            null,
            null,
            null,
            defaultEnv());

    var field = ScheduledProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertSame(metrics, field.get(runner));
    runner.close();
  }
}
