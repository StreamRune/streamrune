package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
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
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
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

/** Tests for {@link ProjectionProducer}. */
@SuppressWarnings("unchecked")
class ProjectionProducerTest {

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

  private static Instance<AtomicBatchProcessor> noAtomicProcessor() {
    Instance<AtomicBatchProcessor> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    return instance;
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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig());

    assertNotNull(runner);
    runner.close();
  }

  /**
   * A duplicate @ProjectionConfig.name across DIFFERENT modes (CONTINUOUS + SCHEDULED) must fail
   * loudly at boot. Before the fix the duplicate check ran after the per-mode filter, so
   * discover(CONTINUOUS) never saw the SCHEDULED duplicate and both booted silently, sharing one
   * projection_offset checkpoint row.
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
                    mock(OffsetStore.class),
                    unsatisfiedDlq(),
                    noAtomicProcessor(),
                    noMetrics(),
                    noLeadership(),
                    noClassifier(),
                    noHealth(),
                    TestProperties.defaults(),
                    defaultConfig()));
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
            mock(OffsetStore.class),
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            configWith("streamrune.projections.auto-discovery.enabled", "false"));
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
            mock(OffsetStore.class),
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            configWith("streamrune.projections.auto-discovery.enabled", value));
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
            mock(OffsetStore.class),
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            configWith("streamrune.projections.auto-discovery.enabled", value));
    assertNull(runner, "auto-discovery.enabled=" + value + " must disable the factory");
  }

  @Test
  void autoDiscoveryDisabledReturnsNullForScheduledRunner() {
    var producer = new ProjectionProducer();
    var runner =
        producer.scheduledProjectionRunner(
            instanceOf(new SchedProjection()),
            mock(EventStore.class),
            mock(OffsetStore.class),
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            configWith("streamrune.projections.auto-discovery.enabled", "false"));
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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig());

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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig());

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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig());

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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig());

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
                offsetStore,
                unsatisfiedDlq(),
                noAtomicProcessor(),
                noMetrics(),
                noLeadership(),
                noClassifier(),
                noHealth(),
                defaultConfig()));
  }

  // --- Tests: InlineProjectionInterceptor ---

  @Test
  void inlineProjectionProducesInlineProjectionInterceptor() {
    var producer = new ProjectionProducer();
    var projections = instanceOf(new InlineProjection());

    InlineProjectionInterceptor interceptor =
        producer.inlineProjectionInterceptor(projections, defaultConfig(), noMetrics());

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
          producer.inlineProjectionInterceptor(projections, defaultConfig(), noMetrics());
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
            noMetrics());
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
        producer.inlineProjectionInterceptor(projections, defaultConfig(), noMetrics());

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
                offsetStore,
                unsatisfiedDlq(),
                noAtomicProcessor(),
                noMetrics(),
                noLeadership(),
                noClassifier(),
                noHealth(),
                TestProperties.defaults(),
                defaultConfig()));
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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            config);

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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            config);

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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            config);

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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            config);

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
                offsetStore,
                unsatisfiedDlq(),
                noAtomicProcessor(),
                noMetrics(),
                noLeadership(),
                noClassifier(),
                noHealth(),
                TestProperties.defaults(),
                defaultConfig()));
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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig());

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
            offsetStore,
            dlqInstance,
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig());

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
            offsetStore,
            dlqInstance,
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig());

    assertNotNull(runner);
    runner.close();
  }

  // --- Tests: the processor is resolved by declared need, never by bean presence ---

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

  /** An Instance over explicit candidates: the producer reads isUnsatisfied() and stream(). */
  private static Instance<AtomicBatchProcessor> processorsOf(AtomicBatchProcessor... processors) {
    Instance<AtomicBatchProcessor> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(processors.length == 0);
    when(instance.isAmbiguous()).thenReturn(processors.length > 1);
    when(instance.stream()).thenAnswer(inv -> java.util.Arrays.stream(processors));
    return instance;
  }

  /**
   * A dependent-scoped producer that yielded a null product: the bean exists, its instance is
   * {@code null}.
   */
  private static Instance<AtomicBatchProcessor> nullProcessorProduct() {
    Instance<AtomicBatchProcessor> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(false);
    when(instance.get()).thenReturn(null);
    when(instance.stream()).thenAnswer(inv -> Stream.of((AtomicBatchProcessor) null));
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
    var store = new InMemoryEventStore();
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
      OffsetStore offsetStore,
      Instance<AtomicBatchProcessor> processors,
      Instance<SubscriptionLeadership> leadership) {
    return new ProjectionProducer()
        .multiProjectionRunner(
            projections,
            eventStore,
            offsetStore,
            unsatisfiedDlq(),
            processors,
            noMetrics(),
            leadership,
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig());
  }

  private static ScheduledProjectionRunner scheduled(
      Instance<Projection> projections, Instance<AtomicBatchProcessor> processors) {
    return new ProjectionProducer()
        .scheduledProjectionRunner(
            projections,
            mock(EventStore.class),
            mock(OffsetStore.class),
            unsatisfiedDlq(),
            processors,
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig());
  }

  /** The runner's private processor, read as QuarkusEpochFenceWiringTest reads it. */
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

  @Test
  void tlProjection_withoutAProcessorBean_throwsNamingTheProjection() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    instanceOf(new TlPlainProjection()),
                    mock(EventStore.class),
                    mock(OffsetStore.class),
                    processorsOf(),
                    noLeadership()));
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
            instanceOf(new TlBaseProjection(repo)),
            mock(EventStore.class),
            repo,
            processorsOf(repo),
            noLeadership());
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
                    instanceOf(new TlPlainProjection()),
                    mock(EventStore.class),
                    repo,
                    processorsOf(repo),
                    noLeadership()));
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
                    instanceOf(new TlBaseProjection(repoB)),
                    mock(EventStore.class),
                    repoA,
                    processorsOf(repoA),
                    noLeadership()));
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
            instanceOf(new TlBaseProjection((ProjectionRepository) proxy)),
            mock(EventStore.class),
            repo,
            processorsOf((AtomicBatchProcessor) proxy),
            noLeadership());
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
            instanceOf(new TlBaseProjection(repoA), new AloBaseProjection(repoB)),
            oneEvent(),
            repoA,
            processorsOf(repoA),
            noLeadership());
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
            instanceOf(new ContProjection()),
            mock(EventStore.class),
            mock(OffsetStore.class),
            processorsOf(new InMemoryProjectionRepository()),
            noLeadership());
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
                    instanceOf(new ContProjection()),
                    mock(EventStore.class),
                    mock(OffsetStore.class),
                    processorsOf(),
                    realLeadership()));
    assertTrue(ex.getMessage().contains("to honour its epoch"), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("streamrune.subscription.single-active-consumer.enabled=false"),
        ex.getMessage());
    assertFalse(
        ex.getMessage().contains("'qcont'"),
        "an at-least-once registration is not what needs the bean");
  }

  @Test
  void aloOnly_realLeadership_withTheBean_bootsAndUsesIt() throws Exception {
    var repo = new InMemoryProjectionRepository();
    var runner =
        multi(
            instanceOf(new ContProjection()),
            mock(EventStore.class),
            mock(OffsetStore.class),
            processorsOf(repo),
            realLeadership());
    try {
      // Fencing selects the bean although every registration is at-least-once.
      assertSame(repo, atomicProcessorOf(runner));
    } finally {
      runner.close();
    }
  }

  @Test
  void twoProcessorBeans_throw() {
    var repoA = new InMemoryProjectionRepository();
    var repoB = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    instanceOf(new TlBaseProjection(repoA)),
                    mock(EventStore.class),
                    repoA,
                    processorsOf(repoA, repoB),
                    noLeadership()));
    assertTrue(ex.getMessage().contains("2 AtomicBatchProcessor beans"), ex.getMessage());
  }

  /**
   * A dependent-scoped processor producer may yield a null product; it counts as no bean. An
   * at-least-once runner without leadership needs none and boots on the nonatomic processor; a
   * TRANSACTIONAL_LOCAL registration needs one and is refused with the no-bean message.
   */
  @Test
  void aNullProcessorProduct_countsAsNoBean() throws Exception {
    var runner =
        multi(
            instanceOf(new ContProjection()),
            mock(EventStore.class),
            mock(OffsetStore.class),
            nullProcessorProduct(),
            noLeadership());
    try {
      assertSame(AtomicBatchProcessor.nonAtomicAtLeastOnce(), atomicProcessorOf(runner));
    } finally {
      runner.close();
    }
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                multi(
                    instanceOf(new TlPlainProjection()),
                    mock(EventStore.class),
                    mock(OffsetStore.class),
                    nullProcessorProduct(),
                    noLeadership()));
    assertTrue(
        ex.getMessage().contains("no AtomicBatchProcessor bean is available"), ex.getMessage());
  }

  @Test
  void inlineTl_throwsAtDiscovery() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                new ProjectionProducer()
                    .inlineProjectionInterceptor(
                        instanceOf(new InlineTlProjection()), defaultConfig(), noMetrics()));
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
                  instanceOf(new InlineProjection()), defaultConfig(), metricsInstanceOf(metrics)));
    } finally {
      julLogger.removeHandler(handler);
    }
    assertEquals(Map.of("qinline", ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT), metrics.modes);
    String info = ProjectionProcessorResolution.INLINE_INFO.replace("{}", "qinline");
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
                        repo,
                        unsatisfiedDlq(),
                        processorsOf(repo),
                        noMetrics(),
                        noLeadership(),
                        noClassifier(),
                        noHealth(),
                        TestProperties.defaults(),
                        configWith("streamrune.projections.allow-at-least-once", "true")));
    assertTrue(ex.getMessage().contains("does not write through"), ex.getMessage());
  }

  @Test
  void scheduledTlPlain_throws_andScheduledAloBoots() throws Exception {
    var repo = new InMemoryProjectionRepository();
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> scheduled(instanceOf(new SchedTlPlainProjection()), processorsOf(repo)));
    assertTrue(ex.getMessage().contains("'qsched_tl'"), ex.getMessage());
    assertTrue(ex.getMessage().contains("does not write through"), ex.getMessage());

    ScheduledProjectionRunner runner =
        scheduled(instanceOf(new SchedProjection()), processorsOf(repo));
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
        scheduled(instanceOf(new SchedTlBaseProjection(repo)), processorsOf(repo));
    try {
      assertSame(repo, atomicProcessorOf(runner));
    } finally {
      runner.close();
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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.of(
                Map.of(
                    "streamrune.polling-interval-ms", "123",
                    "streamrune.polling-jitter-ms", "45")),
            defaultConfig());

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
                offsetStore,
                unsatisfiedDlq(),
                noAtomicProcessor(),
                noMetrics(),
                noLeadership(),
                noClassifier(),
                noHealth(),
                TestProperties.defaults(),
                config));
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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            metricsInstanceOf(metrics),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig());

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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            noMetrics(),
            noLeadership(),
            noClassifier(),
            noHealth(),
            TestProperties.defaults(),
            defaultConfig());

    var field = MultiProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    // The builder leaves metrics null when absent; the runner falls back to NOOP per runner.
    assertNull(field.get(runner));
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
            offsetStore,
            unsatisfiedDlq(),
            noAtomicProcessor(),
            metricsInstanceOf(metrics),
            noLeadership(),
            noClassifier(),
            noHealth(),
            defaultConfig());

    var field = ScheduledProjectionRunner.class.getDeclaredField("metrics");
    field.setAccessible(true);
    assertSame(metrics, field.get(runner));
    runner.close();
  }
}
