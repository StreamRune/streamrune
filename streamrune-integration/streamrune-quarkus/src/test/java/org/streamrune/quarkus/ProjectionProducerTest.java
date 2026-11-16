package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import jakarta.enterprise.inject.Instance;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.awaitility.Awaitility;
import org.eclipse.microprofile.config.Config;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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
import org.streamrune.core.projection.ProjectionDeadLetterStore;
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
import org.streamrune.integration.ProjectionStoreRouting;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.ScheduledProjectionRunner;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;

/** Tests for {@link ProjectionProducer}. */
@SuppressWarnings("unchecked")
class ProjectionProducerTest {

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder().registerEvent(Ping.class).build();

  // --- Concrete test projections ---

  @ProjectionConfig(name = "qcont", deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class ContProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "qsched",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class SchedProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "qinline",
      mode = ProjectionConfig.Mode.INLINE,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class InlineProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  // Two projections sharing a name across different modes.
  @ProjectionConfig(
      name = "dup-across-modes",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class DupContinuousProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "dup-across-modes",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class DupScheduledProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  // --- Helpers ---

  private static Config defaultConfig() {
    Config config = mock(Config.class);
    when(config.getOptionalValue(anyString(), eq(String.class))).thenReturn(Optional.empty());
    return config;
  }

  private static Config configWith(String key, String value) {
    Config config = mock(Config.class);
    when(config.getOptionalValue(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(config.getOptionalValue(key, String.class)).thenReturn(Optional.of(value));
    return config;
  }

  private static Instance<Projection> instanceOf(Projection... projections) {
    Instance<Projection> instance = mock(Instance.class);
    when(instance.spliterator()).thenReturn(List.of(projections).spliterator());
    return instance;
  }

  private static Instance<ProjectionDeadLetterStore> unsatisfiedDlq() {
    Instance<ProjectionDeadLetterStore> dlq = mock(Instance.class);
    when(dlq.isUnsatisfied()).thenReturn(true);
    return dlq;
  }

  private static Instance<ReadModelStore> noStores() {
    Instance<ReadModelStore> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    return instance;
  }

  /**
   * The event store's subscription checkpoint as the integration produces it over an offset store
   * that is not a {@code PostgresOffsetStore}: that store and the nonatomic processor.
   */
  private static CheckpointTarget subscriptionCheckpoint(OffsetStore offsetStore) {
    return new CheckpointTarget(
        "subscription_checkpoint", offsetStore, AtomicBatchProcessor.nonAtomicAtLeastOnce());
  }

  private static Instance<org.streamrune.core.StreamRuneMetrics> noMetrics() {
    Instance<org.streamrune.core.StreamRuneMetrics> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(false);
    return instance;
  }

  private static Instance<org.streamrune.core.StreamRuneMetrics> metricsInstanceOf(
      org.streamrune.core.StreamRuneMetrics metrics) {
    Instance<org.streamrune.core.StreamRuneMetrics> instance = mock(Instance.class);
    when(instance.isResolvable()).thenReturn(true);
    when(instance.get()).thenReturn(metrics);
    return instance;
  }

  private static Instance<org.streamrune.core.subscription.SubscriptionLeadership> noLeadership() {
    Instance<org.streamrune.core.subscription.SubscriptionLeadership> instance =
        mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    return instance;
  }

  private static Instance<org.streamrune.core.projection.ProjectionErrorClassifier> noClassifier() {
    Instance<org.streamrune.core.projection.ProjectionErrorClassifier> instance =
        mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    return instance;
  }

  private static Instance<org.streamrune.runtime.SubscriptionHealthContributor> noHealth() {
    Instance<org.streamrune.runtime.SubscriptionHealthContributor> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    return instance;
  }

  // --- Tests: MultiProjectionRunner ---

  @Test
  void continuousProjectionProducesMultiProjectionRunner() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  /**
   * A duplicate @ProjectionConfig.name across DIFFERENT modes (CONTINUOUS + SCHEDULED) must fail
   * loudly at boot. Before the fix the duplicate check ran after the per-mode filter, so
   * discover(CONTINUOUS) never saw the SCHEDULED duplicate and both booted silently, sharing one
   * subscription_checkpoint checkpoint row.
   */
  @Test
  void crossModeDuplicateNameThrows() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new DupContinuousProjection(), new DupScheduledProjection());
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                producer.multiProjectionRunner(
                    projections,
                    mock(EventStore.class),
                    noStores(),
                    subscriptionCheckpoint(mock(OffsetStore.class)),
                    unsatisfiedDlq(),
                    noMetrics(),
                    noLeadership(),
                    noClassifier(),
                    noHealth(),
                    TestProperties.defaults(),
                    defaultConfig(),
                    org.streamrune.core.tracing.StreamRuneTracing.NOOP));
    assertTrue(ex.getMessage().contains("Duplicate"));
  }

  @Test
  void autoDiscoveryDisabledReturnsNullForMultiRunner() {
    // streamrune.projections.auto-discovery.enabled=false is the kill switch (parity with
    // Spring/Micronaut) — the framework assembles no MultiProjectionRunner, so the user owns runner
    // lifecycle entirely.
    var producer = new ProjectionProducer();
    var runner =
        producer.multiProjectionRunner(
            instanceOf(new ContProjection()),
            mock(EventStore.class),
            noStores(),
            subscriptionCheckpoint(mock(OffsetStore.class)),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            configWith("streamrune.projections.auto-discovery.enabled", "false"),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    assertNull(
        runner, "auto-discovery.enabled=false must suppress the framework MultiProjectionRunner");
  }

  /**
   * Already correct here (RelaxedBoolean-backed since), but nothing pinned every non-canonical
   * spelling — only the literal "false". Companion to the Spring/Micronaut parametrised parity
   * tests for the same property.
   */
  @ParameterizedTest
  @ValueSource(strings = {"true", "on", "yes", "y", "t", "1", "TRUE", "On"})
  void autoDiscoveryNonCanonicalTruthyValuesStayEnabled(String value) {
    var producer = new ProjectionProducer();
    var runner =
        producer.multiProjectionRunner(
            instanceOf(new ContProjection()),
            mock(EventStore.class),
            noStores(),
            subscriptionCheckpoint(mock(OffsetStore.class)),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            configWith("streamrune.projections.auto-discovery.enabled", value),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    assertNotNull(runner, "auto-discovery.enabled=" + value + " must leave the factory enabled");
  }

  /** Every canonical-false spelling must disable the whole factory. */
  @ParameterizedTest
  @ValueSource(strings = {"false", "off", "no", "n", "f", "0", "FALSE", "Off"})
  void autoDiscoveryNonCanonicalFalsyValuesStayDisabled(String value) {
    var producer = new ProjectionProducer();
    var runner =
        producer.multiProjectionRunner(
            instanceOf(new ContProjection()),
            mock(EventStore.class),
            noStores(),
            subscriptionCheckpoint(mock(OffsetStore.class)),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            configWith("streamrune.projections.auto-discovery.enabled", value),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    assertNull(runner, "auto-discovery.enabled=" + value + " must disable the factory");
  }

  @Test
  void autoDiscoveryDisabledReturnsNullForScheduledRunner() {
    var producer = new ProjectionProducer();
    var runner =
        producer.scheduledProjectionRunner(
            instanceOf(new SchedProjection()),
            mock(EventStore.class),
            noStores(),
            subscriptionCheckpoint(mock(OffsetStore.class)),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            configWith("streamrune.projections.auto-discovery.enabled", "false"),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    assertNull(
        runner,
        "auto-discovery.enabled=false must suppress the framework ScheduledProjectionRunner");
  }

  @Test
  void noMatchingProjectionsReturnsNullForMultiRunner() {
    var producer = new ProjectionProducer();
    // Provide a SCHEDULED projection — should NOT match CONTINUOUS
    var projections = instanceOf(new SchedProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNull(runner);
  }

  @Test
  void emptyProjectionInstanceReturnsNullForMultiRunner() {
    var producer = new ProjectionProducer();
    Instance<Projection> projections = mock(Instance.class);
    when(projections.spliterator()).thenReturn(Collections.<Projection>emptyList().spliterator());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNull(runner);
  }

  // --- Tests: ScheduledProjectionRunner ---

  @Test
  void scheduledProjectionProducesScheduledProjectionRunner() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new SchedProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    ScheduledProjectionRunner runner =
        producer.scheduledProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  @Test
  void noMatchingProjectionsReturnsNullForScheduledRunner() {
    var producer = new ProjectionProducer();
    // Provide a CONTINUOUS projection — should NOT match SCHEDULED
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    ScheduledProjectionRunner runner =
        producer.scheduledProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNull(runner);
  }

  @Test
  void scheduledWithoutCronThrowsIllegalStateException() {
    var producer = new ProjectionProducer();

    // Inner class with SCHEDULED but no cron
    @ProjectionConfig(
        name = "badscheduled",
        mode = ProjectionConfig.Mode.SCHEDULED,
        deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    class NoCronProjection implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {}
    }

    var projections = instanceOf(new NoCronProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    assertThrows(
        IllegalStateException.class,
        () ->
            producer.scheduledProjectionRunner(
                projections,
                eventStore,
                noStores(),
                subscriptionCheckpoint(offsetStore),
                unsatisfiedDlq(),
                noMetrics(),
                noLeadership(),
                noClassifier(),
                noHealth(),
                defaultConfig(),
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  // --- Tests: InlineProjectionInterceptor ---

  @Test
  void inlineProjectionProducesInlineProjectionInterceptor() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new InlineProjection());

    InlineProjectionInterceptor interceptor =
        producer.inlineProjectionInterceptor(
            projections,
            defaultConfig(),
            noMetrics(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(interceptor);
  }

  @Test
  void inlineProjectionWarnsAboutTheMissingRecoveryPath() {
    // An INLINE projection has no automatic recovery path if process()
    // throws — the command still reports SUCCESS and that command's read-model update is silently
    // and permanently lost. Structurally unreachable to fix within this producer (it needs a
    // streamrune-core ProjectionConfig API change to let one bean register under more than one
    // mode), so this asserts the boot-time WARN that surfaces the risk instead.
    //
    // Captured via a java.util.logging Handler, not a Logback ListAppender: this module's test
    // environment runs under JBoss LogManager (org.slf4j.impl.Slf4jLogger bridges SLF4J calls into
    // it), confirmed empirically with a throwaway probe test (not committed) that printed
    // LogManager.getLogManager().getClass(). JBoss LogManager IS a java.util.logging provider, so a
    // plain JUL Handler observes exactly what the SLF4J call produced.
    var julLogger = java.util.logging.Logger.getLogger(ProjectionProducer.class.getName());
    var captured = new java.util.ArrayList<String>();
    var handler =
        new java.util.logging.Handler() {
          @Override
          public void publish(java.util.logging.LogRecord record) {
            captured.add(record.getMessage());
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    julLogger.addHandler(handler);
    julLogger.setLevel(java.util.logging.Level.ALL);
    try {
      var producer = new ProjectionProducer();
      var projections = instanceOf(new InlineProjection());
      InlineProjectionInterceptor interceptor =
          producer.inlineProjectionInterceptor(
              projections,
              defaultConfig(),
              noMetrics(),
              org.streamrune.core.tracing.StreamRuneTracing.NOOP);
      assertNotNull(interceptor);
    } finally {
      julLogger.removeHandler(handler);
    }

    assertTrue(
        captured.stream()
            .anyMatch(
                m ->
                    m.contains("qinline")
                        && m.contains("INLINE")
                        && m.contains("recovery")
                        && m.contains("silently")),
        () ->
            "an INLINE projection with no recovery path must be flagged at boot; captured: "
                + captured);
  }

  @Test
  void autoDiscoveryDisabledReturnsNullForInlineInterceptor() {
    // The auto-discovery kill switch must gate INLINE projections too —
    // otherwise INLINE projections keep writing read models the operator believes are disabled,
    // while CONTINUOUS/SCHEDULED correctly return null. Spring gates the whole @Configuration and
    // Micronaut the whole @Factory, so this restores 3-framework parity for the same config.
    var producer = new ProjectionProducer();
    var interceptor =
        producer.inlineProjectionInterceptor(
            instanceOf(new InlineProjection()),
            configWith("streamrune.projections.auto-discovery.enabled", "false"),
            noMetrics(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
    assertNull(
        interceptor,
        "auto-discovery.enabled=false must suppress the framework InlineProjectionInterceptor");
  }

  @Test
  void noMatchingProjectionsReturnsNullForInlineInterceptor() {
    var producer = new ProjectionProducer();
    // Provide a CONTINUOUS projection — should NOT match INLINE
    var projections = instanceOf(new ContProjection());

    InlineProjectionInterceptor interceptor =
        producer.inlineProjectionInterceptor(
            projections,
            defaultConfig(),
            noMetrics(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNull(interceptor);
  }

  // --- Tests: duplicate name ---

  @Test
  void duplicateProjectionNameThrowsIllegalStateException() {
    var producer = new ProjectionProducer();

    @ProjectionConfig(
        name = "qcont",
        deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    class DuplicateCont implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {}
    }

    // Both are CONTINUOUS with the same name "qcont"
    var projections = instanceOf(new ContProjection(), new DuplicateCont());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    assertThrows(
        IllegalStateException.class,
        () ->
            producer.multiProjectionRunner(
                projections,
                eventStore,
                noStores(),
                subscriptionCheckpoint(offsetStore),
                unsatisfiedDlq(),
                noMetrics(),
                noLeadership(),
                noClassifier(),
                noHealth(),
                TestProperties.defaults(),
                defaultConfig(),
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  // --- Tests: per-projection enabled flag ---

  @Test
  void disabledProjectionIsSkipped() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    Config config = mock(Config.class);
    when(config.getOptionalValue(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(config.getOptionalValue("streamrune.projections.qcont.enabled", String.class))
        .thenReturn(Optional.of("false"));

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            config,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNull(runner);
  }

  /**
   * A non-canonical positive value ({@code on}) must ENABLE the projection — identical to the
   * Spring and Micronaut integrations. Before the shared {@link
   * org.streamrune.integration.RelaxedBoolean} parser, Quarkus used {@code
   * Boolean.parseBoolean("on") == false}, silently disabling a projection that Micronaut kept
   * enabled.
   */
  @Test
  void nonCanonicalOnEnablesProjection() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    Config config = mock(Config.class);
    when(config.getOptionalValue(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(config.getOptionalValue("streamrune.projections.qcont.enabled", String.class))
        .thenReturn(Optional.of("on"));

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            config,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  /** A non-canonical negative value ({@code off}) must DISABLE the projection on every runtime. */
  @Test
  void nonCanonicalOffDisablesProjection() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    Config config = mock(Config.class);
    when(config.getOptionalValue(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(config.getOptionalValue("streamrune.projections.qcont.enabled", String.class))
        .thenReturn(Optional.of("off"));

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            config,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNull(runner);
  }

  // --- Tests: error-strategy override ---

  @Test
  void errorStrategyOverrideFromConfig() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    // Override to SKIP (default is HALT from @ProjectionConfig)
    Config config = mock(Config.class);
    when(config.getOptionalValue(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(config.getOptionalValue("streamrune.projections.qcont.error-strategy", String.class))
        .thenReturn(Optional.of("SKIP"));

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            config,
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  // --- Tests: DLQ validation ---

  @Test
  void dlqStrategyWithoutDlqBeanThrowsIllegalStateException() {
    // Create a projection with DLQ error strategy
    @ProjectionConfig(
        name = "qdlq",
        errorStrategy = ProjectionErrorStrategy.DLQ,
        deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    class DlqProjection implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {}
    }

    var producer = new ProjectionProducer();
    var projections = instanceOf(new DlqProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    assertThrows(
        IllegalStateException.class,
        () ->
            producer.multiProjectionRunner(
                projections,
                eventStore,
                noStores(),
                subscriptionCheckpoint(offsetStore),
                unsatisfiedDlq(),
                noMetrics(),
                noLeadership(),
                noClassifier(),
                noHealth(),
                TestProperties.defaults(),
                defaultConfig(),
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  // --- Tests: CDI proxy hierarchy walk ---

  @Test
  void proxySubclassAnnotationDiscovered() {
    // Simulate a CDI proxy subclass
    class ProxiedCont extends ContProjection {}

    var producer = new ProjectionProducer();
    var projections = instanceOf(new ProxiedCont());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  // --- Tests: DLQ store wiring ---

  @Test
  void dlqStrategyWithDlqBeanProducesMultiRunner() {
    @ProjectionConfig(
        name = "qdlqok",
        errorStrategy = ProjectionErrorStrategy.DLQ,
        deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
    class DlqOkProjection implements Projection {
      @Override
      public void process(List<EventEnvelope> batch) {}
    }

    var producer = new ProjectionProducer();
    var projections = instanceOf(new DlqOkProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    ProjectionDeadLetterStore dlq = mock(ProjectionDeadLetterStore.class);
    Instance<ProjectionDeadLetterStore> dlqInstance = mock(Instance.class);
    when(dlqInstance.isUnsatisfied()).thenReturn(false);
    when(dlqInstance.get()).thenReturn(dlq);

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            dlqInstance,
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  @Test
  void scheduledRunnerAcceptsDlqBean() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new SchedProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    ProjectionDeadLetterStore dlq = mock(ProjectionDeadLetterStore.class);
    Instance<ProjectionDeadLetterStore> dlqInstance = mock(Instance.class);
    when(dlqInstance.isUnsatisfied()).thenReturn(false);
    when(dlqInstance.get()).thenReturn(dlq);

    ScheduledProjectionRunner runner =
        producer.scheduledProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            dlqInstance,
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    assertNotNull(runner);
    runner.close();
  }

  // --- Tests: each registration is routed to its store or the subscription checkpoint ---

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
      name = "qsched_tl",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class SchedTlPlainProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "qsched_tl_base",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class SchedTlBaseProjection extends BaseProjection {
    SchedTlBaseProjection(ProjectionRepository repo) {
      super(repo, "qsched_tl_base");
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

  /** An Instance over explicit stores: the producer reads isUnsatisfied() and stream(). */
  private static Instance<ReadModelStore> storesOf(ReadModelStore... stores) {
    Instance<ReadModelStore> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(stores.length == 0);
    when(instance.isAmbiguous()).thenReturn(stores.length > 1);
    when(instance.stream()).thenAnswer(inv -> java.util.Arrays.stream(stores));
    return instance;
  }

  /** A store whose repository, checkpoints and processor are one in-memory repository. */
  private static ReadModelStore memoryStore(String name, InMemoryProjectionRepository repo) {
    return ReadModelStore.of(name, repo, repo, repo);
  }

  /**
   * A dependent-scoped store producer that yielded a null product: the bean exists, its instance is
   * {@code null}.
   */
  private static Instance<ReadModelStore> nullStoreProduct() {
    Instance<ReadModelStore> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(false);
    when(instance.get()).thenReturn(null);
    when(instance.stream()).thenAnswer(inv -> Stream.of((ReadModelStore) null));
    return instance;
  }

  /** Anything but NOOP: resolveOrNull reads isUnsatisfied() then get(). */
  private static Instance<SubscriptionLeadership> realLeadership() {
    Instance<SubscriptionLeadership> instance = mock(Instance.class);
    SubscriptionLeadership leadership = mock(SubscriptionLeadership.class);
    when(instance.isUnsatisfied()).thenReturn(false);
    when(instance.get()).thenReturn(leadership);
    return instance;
  }

  private static InMemoryEventStore oneEvent() {
    var store = new InMemoryEventStore(TYPES);
    store.append(
        StreamId.of(AggregateType.of("s"), AggregateId.of("s")),
        List.of(
            EventStoreFixture.event(
                StreamId.of(AggregateType.of("s"), AggregateId.of("s")),
                new Version(1),
                new EventType("Ping"),
                new Ping(1))),
        new Version(0));
    return store;
  }

  private static MultiProjectionRunner multi(
      Instance<Projection> projections,
      EventStore eventStore,
      Instance<ReadModelStore> stores,
      CheckpointTarget subscriptionCheckpoint,
      Instance<SubscriptionLeadership> leadership) {
    return new ProjectionProducer()
        .multiProjectionRunner(
            projections,
            eventStore,
            stores,
            subscriptionCheckpoint,
            unsatisfiedDlq(),
            noMetrics(),
            leadership,
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
  }

  private static MultiProjectionRunner multi(
      Instance<Projection> projections,
      Instance<ReadModelStore> stores,
      Instance<SubscriptionLeadership> leadership) {
    return multi(
        projections,
        mock(EventStore.class),
        stores,
        subscriptionCheckpoint(mock(OffsetStore.class)),
        leadership);
  }

  private static ScheduledProjectionRunner scheduled(
      Instance<Projection> projections, Instance<ReadModelStore> stores) {
    return new ProjectionProducer()
        .scheduledProjectionRunner(
            projections,
            mock(EventStore.class),
            stores,
            subscriptionCheckpoint(mock(OffsetStore.class)),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);
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

  /** Reads the runner's private per-registration pairs, as QuarkusEpochFenceWiringTest does. */
  private static Object pairPart(Object runner, String projection, String part) throws Exception {
    Field field = runner.getClass().getDeclaredField("checkpoints");
    field.setAccessible(true);
    Object pair = ((Map<?, ?>) field.get(runner)).get(ProjectionName.of(projection));
    assertNotNull(pair, "the routed pair of " + projection);
    java.lang.reflect.Method accessor = pair.getClass().getDeclaredMethod(part);
    accessor.setAccessible(true);
    return accessor.invoke(pair);
  }

  @Test
  void tlProjection_withoutAStore_throwsNamingTheProjection() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> multi(instanceOf(new TlPlainProjection()), storesOf(), noLeadership()));
    assertTrue(
        ex.getMessage().contains("projection 'tl_plain' (TRANSACTIONAL_LOCAL)"), ex.getMessage());
    assertTrue(ex.getMessage().contains("no read-model store is transactional"), ex.getMessage());
  }

  @Test
  void tlBaseProjection_overTheStoresRepository_isRoutedToTheStore() throws Exception {
    var repo = new InMemoryProjectionRepository();
    try (var runner =
        multi(
            instanceOf(new TlBaseProjection(repo)),
            storesOf(memoryStore("memory", repo)),
            noLeadership())) {
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
                multi(
                    instanceOf(new TlPlainProjection()),
                    storesOf(memoryStore("memory", repo)),
                    noLeadership()));
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
                    instanceOf(new TlBaseProjection(repoB)),
                    storesOf(memoryStore("memory", repoA)),
                    noLeadership()));
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
            instanceOf(new TlBaseProjection((ProjectionRepository) proxy)),
            storesOf(
                ReadModelStore.of(
                    "memory", (ProjectionRepository) proxy, repo, (AtomicBatchProcessor) proxy)),
            noLeadership())) {
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
            instanceOf(new TlBaseProjection(repoA), new AloBaseProjection(repoB)),
            oneEvent(),
            storesOf(memoryStore("memory", repoA)),
            subscriptionCheckpoint(subscription),
            noLeadership());
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
    var offsets = mock(OffsetStore.class);
    try (var runner =
        multi(
            instanceOf(new ContProjection()),
            mock(EventStore.class),
            storesOf(memoryStore("memory", new InMemoryProjectionRepository())),
            subscriptionCheckpoint(offsets),
            noLeadership())) {
      assertSame(
          AtomicBatchProcessor.nonAtomicAtLeastOnce(),
          processorOf(runner, "qcont"),
          "routing selects the subscription checkpoint; the store's presence selects nothing");
      assertSame(offsets, checkpointsOf(runner, "qcont"));
      assertEquals("subscription_checkpoint", targetNameOf(runner, "qcont"));
    }
  }

  @Test
  void aloOnly_realLeadership_aNonFencingSubscriptionCheckpointThrows() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> multi(instanceOf(new ContProjection()), storesOf(), realLeadership()));
    assertTrue(
        ex.getMessage().contains("projection 'qcont' (AT_LEAST_ONCE_IDEMPOTENT)"), ex.getMessage());
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
            instanceOf(new ContProjection()),
            mock(EventStore.class),
            storesOf(),
            new CheckpointTarget("subscription_checkpoint", fencing, fencing),
            realLeadership())) {
      assertSame(fencing, processorOf(runner, "qcont"));
      assertEquals("subscription_checkpoint", targetNameOf(runner, "qcont"));
    }
  }

  @Test
  void aloBase_overAStoresRepository_realLeadership_isRoutedToTheFencingStore() throws Exception {
    var repo = new InMemoryProjectionRepository();
    try (var runner =
        multi(
            instanceOf(new AloBaseProjection(repo)),
            storesOf(memoryStore("memory", repo)),
            realLeadership())) {
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
                    instanceOf(new TlPlainProjection()),
                    storesOf(
                        memoryStore("a", new InMemoryProjectionRepository()),
                        memoryStore("b", new InMemoryProjectionRepository())),
                    noLeadership()));
    assertTrue(
        ex.getMessage().contains("several read-model stores are transactional ('a', 'b')"),
        ex.getMessage());
    assertTrue(ex.getMessage().contains("readModelStore = \"<name>\""), ex.getMessage());
  }

  @Test
  void twoStoresWithOneName_throw() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    instanceOf(new ContProjection()),
                    storesOf(
                        memoryStore("memory", new InMemoryProjectionRepository()),
                        memoryStore("memory", new InMemoryProjectionRepository())),
                    noLeadership()));
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
        multi(
            instanceOf(new DeclaredStoreProjection()),
            storesOf(memoryStore("memory", repo)),
            noLeadership())) {
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
                    instanceOf(new DeclaredStoreProjection()),
                    storesOf(memoryStore("postgres", new InMemoryProjectionRepository())),
                    noLeadership()));
    assertTrue(
        ex.getMessage().contains("declares readModelStore = \"memory\", but no read-model store"),
        ex.getMessage());
    assertTrue(ex.getMessage().contains("'postgres'"), ex.getMessage());
  }

  /**
   * A dependent-scoped store producer may yield a null product; it counts as no store. An
   * at-least-once registration needs none and boots on the subscription checkpoint; a
   * TRANSACTIONAL_LOCAL one needs one and is refused naming that no store is defined.
   */
  @Test
  void aNullStoreProduct_countsAsNoStore() throws Exception {
    try (var runner = multi(instanceOf(new ContProjection()), nullStoreProduct(), noLeadership())) {
      assertSame(AtomicBatchProcessor.nonAtomicAtLeastOnce(), processorOf(runner, "qcont"));
    }
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> multi(instanceOf(new TlPlainProjection()), nullStoreProduct(), noLeadership()));
    assertTrue(ex.getMessage().contains("no read-model store is defined"), ex.getMessage());
  }

  @Test
  void inlineTl_throwsAtDiscovery() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                new ProjectionProducer()
                    .inlineProjectionInterceptor(
                        instanceOf(new InlineTlProjection()),
                        defaultConfig(),
                        noMetrics(),
                        org.streamrune.core.tracing.StreamRuneTracing.NOOP));
    assertEquals(
        "projection 'inline_tl' is INLINE and declares TRANSACTIONAL_LOCAL: INLINE runs post-commit on the"
            + " command thread with no checkpoint and no transaction of its own. Declare"
            + " AT_LEAST_ONCE_IDEMPOTENT, or run it CONTINUOUS or SCHEDULED.",
        ex.getMessage());
  }

  @Test
  void inlineAlo_boots_andRecordsTheGauge() {
    // Captured through java.util.logging, as inlineProjectionWarnsAboutTheMissingRecoveryPath does.
    var julLogger = java.util.logging.Logger.getLogger(ProjectionProducer.class.getName());
    var captured = new java.util.concurrent.CopyOnWriteArrayList<String>();
    var handler =
        new java.util.logging.Handler() {
          @Override
          public void publish(java.util.logging.LogRecord record) {
            captured.add(record.getMessage());
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    var metrics = new RecordingMetrics();
    julLogger.addHandler(handler);
    julLogger.setLevel(java.util.logging.Level.ALL);
    try {
      assertNotNull(
          new ProjectionProducer()
              .inlineProjectionInterceptor(
                  instanceOf(new InlineProjection()),
                  defaultConfig(),
                  metricsInstanceOf(metrics),
                  org.streamrune.core.tracing.StreamRuneTracing.NOOP));
    } finally {
      julLogger.removeHandler(handler);
    }
    assertEquals(Map.of("qinline", ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT), metrics.modes);
    String info = ProjectionStoreRouting.INLINE_INFO.replace("{}", "qinline");
    assertEquals(
        1,
        captured.stream().filter(info::equals).count(),
        () -> "one INFO line " + info + "; captured: " + captured);
  }

  @Test
  void theCoarsePropertyIsNoLongerRead() {
    var repo = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                new ProjectionProducer()
                    .multiProjectionRunner(
                        instanceOf(new TlPlainProjection()),
                        mock(EventStore.class),
                        storesOf(memoryStore("memory", repo)),
                        subscriptionCheckpoint(repo),
                        unsatisfiedDlq(),
                        noMetrics(),
                        noLeadership(),
                        noClassifier(),
                        noHealth(),
                        TestProperties.defaults(),
                        configWith("streamrune.projections.allow-at-least-once", "true"),
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
                    instanceOf(new SchedTlPlainProjection()),
                    storesOf(memoryStore("memory", repo))));
    assertTrue(ex.getMessage().contains("'qsched_tl'"), ex.getMessage());
    assertTrue(ex.getMessage().contains("does not write through"), ex.getMessage());

    try (ScheduledProjectionRunner runner =
        scheduled(instanceOf(new SchedProjection()), storesOf(memoryStore("memory", repo)))) {
      assertNotNull(runner);
      assertSame(
          AtomicBatchProcessor.nonAtomicAtLeastOnce(),
          processorOf(runner, "qsched"),
          "an at-least-once scheduled projection with no write target is not given the store");
      assertEquals("subscription_checkpoint", targetNameOf(runner, "qsched"));
    }
  }

  @Test
  void scheduledTlBase_overTheStoresRepository_isRoutedToTheStore() throws Exception {
    var repo = new InMemoryProjectionRepository();
    try (ScheduledProjectionRunner runner =
        scheduled(
            instanceOf(new SchedTlBaseProjection(repo)), storesOf(memoryStore("memory", repo)))) {
      assertSame(repo, processorOf(runner, "qsched_tl_base"));
      assertEquals("memory", targetNameOf(runner, "qsched_tl_base"));
    }
  }

  // --- Tests: polling configuration wiring ---

  @Test
  void pollingPropertiesDriveSubscriptionConfig() throws Exception {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.of(
                Map.of(
                    "streamrune.polling-interval", "PT0.123S",
                    "streamrune.polling-jitter", "PT0.045S")),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    var field = MultiProjectionRunner.class.getDeclaredField("subscriptionConfig");
    field.setAccessible(true);
    var config = (SubscriptionConfig) field.get(runner);
    assertEquals(Duration.ofMillis(123), config.pollingInterval());
    assertEquals(Duration.ofMillis(45), config.pollingJitter());
    runner.close();
  }

  // --- Tests: invalid error-strategy config ---

  @Test
  void invalidErrorStrategyConfigThrowsIllegalStateException() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    Config config = mock(Config.class);
    when(config.getOptionalValue(anyString(), eq(String.class))).thenReturn(Optional.empty());
    when(config.getOptionalValue("streamrune.projections.qcont.error-strategy", String.class))
        .thenReturn(Optional.of("BOGUS"));

    assertThrows(
        IllegalStateException.class,
        () ->
            producer.multiProjectionRunner(
                projections,
                eventStore,
                noStores(),
                subscriptionCheckpoint(offsetStore),
                unsatisfiedDlq(),
                noMetrics(),
                noLeadership(),
                noClassifier(),
                noHealth(),
                TestProperties.defaults(),
                config,
                org.streamrune.core.tracing.StreamRuneTracing.NOOP));
  }

  // --- Tests: metrics wiring ---

  @Test
  void metricsBeanWiredIntoMultiProjectionRunner() throws Exception {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);
    org.streamrune.core.StreamRuneMetrics metrics =
        mock(org.streamrune.core.StreamRuneMetrics.class);

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            metricsInstanceOf(metrics),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    var field = MultiProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertSame(metrics, field.get(runner));
    runner.close();
  }

  @Test
  void absentMetricsLeavesMultiProjectionRunnerWithNoopMetrics() throws Exception {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new ContProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);

    MultiProjectionRunner runner =
        producer.multiProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    var field = MultiProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertSame(org.streamrune.core.StreamRuneMetrics.NOOP, field.get(runner));
    runner.close();
  }

  @Test
  void metricsBeanWiredIntoScheduledProjectionRunner() throws Exception {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new SchedProjection());
    var eventStore = mock(EventStore.class);
    var offsetStore = mock(OffsetStore.class);
    org.streamrune.core.StreamRuneMetrics metrics =
        mock(org.streamrune.core.StreamRuneMetrics.class);

    ScheduledProjectionRunner runner =
        producer.scheduledProjectionRunner(
            projections,
            eventStore,
            noStores(),
            subscriptionCheckpoint(offsetStore),
            unsatisfiedDlq(),
            metricsInstanceOf(metrics),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig(),
            org.streamrune.core.tracing.StreamRuneTracing.NOOP);

    var field = ScheduledProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertSame(metrics, field.get(runner));
    runner.close();
  }
}
