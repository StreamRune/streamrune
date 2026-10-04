package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.util.ReflectionTestUtils;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.LockMode;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.Versioned;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.ScheduledProjectionRunner;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;

class ProjectionAutoConfigTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(ProjectionAutoConfig.class))
          .withUserConfiguration(InfraConfig.class);

  @Test
  void continuousProjections_buildsMPR() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              // Null-product: a mode with no projection resolves the runner @Bean to null (a
              // NullBean whose definition type still matches), so assert functional absence via the
              // bean provider rather than doesNotHaveBean.
              assertThat(ctx.getBeanProvider(ScheduledProjectionRunner.class).getIfAvailable())
                  .isNull();
              assertThat(ctx.getBeanProvider(InlineProjectionInterceptor.class).getIfAvailable())
                  .isNull();
            });
  }

  @Test
  void pollingPropertiesAreWiredIntoSubscriptionConfig() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues(
            "streamrune.projections.auto-discovery.enabled=true",
            "streamrune.polling-interval-ms=123",
            "streamrune.polling-jitter-ms=45")
        .run(
            ctx -> {
              var mpr = ctx.getBean(MultiProjectionRunner.class);
              // MultiProjectionRunner does not expose its SubscriptionConfig; read the private
              // field to prove the documented properties actually reach the runner.
              var field = MultiProjectionRunner.class.getDeclaredField("subscriptionConfig");
              field.setAccessible(true);
              var config = (org.streamrune.core.subscription.SubscriptionConfig) field.get(mpr);
              assertThat(config.pollingInterval()).isEqualTo(java.time.Duration.ofMillis(123));
              assertThat(config.pollingJitter()).isEqualTo(java.time.Duration.ofMillis(45));
              assertThat(config.listenNotifyEnabled()).isTrue();
            });
  }

  @Test
  void scheduledProjections_buildsSPR() {
    runner
        .withUserConfiguration(ScheduledBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ScheduledProjectionRunner.class);
              assertThat(ctx.getBeanProvider(MultiProjectionRunner.class).getIfAvailable())
                  .isNull();
            });
  }

  @Test
  void inlineProjections_buildsInterceptor() {
    runner
        .withUserConfiguration(InlineBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(ctx -> assertThat(ctx).hasSingleBean(InlineProjectionInterceptor.class));
  }

  @Test
  void inlineProjections_warnsAboutTheMissingRecoveryPath() throws Exception {
    // An INLINE projection has no automatic recovery path if process()
    // throws — the command still reports SUCCESS and that command's read-model update is silently
    // and permanently lost. Structurally unreachable to fix within this auto-configuration (it
    // needs a streamrune-core ProjectionConfig API change to let one bean register under more than
    // one mode), so this asserts the boot-time WARN that surfaces the risk instead.
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(ProjectionAutoConfig.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      runner
          .withUserConfiguration(InlineBean.class)
          .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
          .run(ctx -> assertThat(ctx).hasSingleBean(InlineProjectionInterceptor.class));
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }

    var warnings =
        appender.list.stream()
            .filter(e -> e.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.WARN))
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .toList();
    assertThat(warnings)
        .as("an INLINE projection with no recovery path must be flagged at boot")
        .anyMatch(
            m ->
                m.contains("inline")
                    && m.contains("INLINE")
                    && m.contains("recovery")
                    && m.contains("silently"));
  }

  @Test
  void mixedModes_buildsAllThree() {
    runner
        .withUserConfiguration(ContinuousBean.class, ScheduledBean.class, InlineBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              assertThat(ctx).hasSingleBean(ScheduledProjectionRunner.class);
              assertThat(ctx).hasSingleBean(InlineProjectionInterceptor.class);
            });
  }

  @Test
  void disabledGlobally_skipsAll() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=false")
        .run(
            ctx -> {
              assertThat(ctx).doesNotHaveBean(MultiProjectionRunner.class);
              assertThat(ctx).doesNotHaveBean(ScheduledProjectionRunner.class);
              assertThat(ctx).doesNotHaveBean(InlineProjectionInterceptor.class);
            });
  }

  /**
   * The auto-discovery KILL SWITCH itself must agree with Quarkus/Micronaut on every canonical-true
   * spelling RelaxedBoolean accepts, not just the literal "true" {@code havingValue} used to
   * require. Before the fix, {@code enabled=on} silently DISABLED the whole auto-config on Spring
   * while leaving it enabled on Quarkus and Micronaut — the opposite of {@link
   * #nonCanonicalOnEnablesProjection()}'s per-projection flag, which already went through
   * RelaxedBoolean.
   */
  @ParameterizedTest
  @ValueSource(strings = {"true", "on", "yes", "y", "t", "1", "TRUE", "On"})
  void autoDiscoveryNonCanonicalTruthyValuesStayEnabled(String value) {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=" + value)
        .run(ctx -> assertThat(ctx).hasSingleBean(MultiProjectionRunner.class));
  }

  /** Every canonical-false spelling must disable the whole auto-config. */
  @ParameterizedTest
  @ValueSource(strings = {"false", "off", "no", "n", "f", "0", "FALSE", "Off"})
  void autoDiscoveryNonCanonicalFalsyValuesStayDisabled(String value) {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=" + value)
        .run(ctx -> assertThat(ctx).doesNotHaveBean(MultiProjectionRunner.class));
  }

  /** Absent (the default) must still mean enabled — matchIfMissing's old contract. */
  @Test
  void autoDiscoveryAbsentPropertyStaysEnabled() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .run(ctx -> assertThat(ctx).hasSingleBean(MultiProjectionRunner.class));
  }

  @Test
  void userMPR_skipsContinuousAutoConfig() {
    runner
        .withUserConfiguration(ContinuousBean.class, UserMPRConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              assertThat(ctx.getBean(MultiProjectionRunner.class)).isSameAs(ctx.getBean("userMPR"));
            });
  }

  @Test
  void duplicateName_throws() {
    runner
        .withUserConfiguration(DuplicateNameConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(ctx -> assertThat(ctx).hasFailed());
  }

  @Test
  void scheduledMissingCron_throws() {
    runner
        .withUserConfiguration(ScheduledMissingCronConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(ctx -> assertThat(ctx).hasFailed());
  }

  @Test
  void propertyDisablesProjection() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues(
            "streamrune.projections.auto-discovery.enabled=true",
            "streamrune.projections.cont.enabled=false")
        .run(
            ctx ->
                assertThat(ctx.getBeanProvider(MultiProjectionRunner.class).getIfAvailable())
                    .isNull());
  }

  /**
   * A non-canonical positive value ({@code on}) must ENABLE the projection — identical to the
   * Quarkus and Micronaut integrations. Before the shared {@link
   * org.streamrune.integration.RelaxedBoolean} parser, Spring used {@code
   * Boolean.parseBoolean("on") == false}, silently disabling a projection that Micronaut kept
   * enabled.
   */
  @Test
  void nonCanonicalOnEnablesProjection() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues(
            "streamrune.projections.auto-discovery.enabled=true",
            "streamrune.projections.cont.enabled=on")
        .run(ctx -> assertThat(ctx).hasSingleBean(MultiProjectionRunner.class));
  }

  /** A non-canonical negative value ({@code off}) must DISABLE the projection on every runtime. */
  @Test
  void nonCanonicalOffDisablesProjection() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues(
            "streamrune.projections.auto-discovery.enabled=true",
            "streamrune.projections.cont.enabled=off")
        .run(
            ctx ->
                assertThat(ctx.getBeanProvider(MultiProjectionRunner.class).getIfAvailable())
                    .isNull());
  }

  @Test
  void errorStrategyOverrideFromProperty() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues(
            "streamrune.projections.auto-discovery.enabled=true",
            "streamrune.projections.cont.error-strategy=SKIP")
        .run(ctx -> assertThat(ctx).hasSingleBean(MultiProjectionRunner.class));
  }

  @Test
  void invalidErrorStrategyProperty_throws() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues(
            "streamrune.projections.auto-discovery.enabled=true",
            "streamrune.projections.cont.error-strategy=BOGUS")
        .run(ctx -> assertThat(ctx).hasFailed());
  }

  @Test
  void metricsBeanIsWiredIntoMultiProjectionRunner() {
    var metrics = mock(org.streamrune.core.StreamRuneMetrics.class);
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withBean(org.streamrune.core.StreamRuneMetrics.class, () -> metrics)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              var mpr = ctx.getBean(MultiProjectionRunner.class);
              var field = MultiProjectionRunner.class.getDeclaredField("metrics");
              field.setAccessible(true);
              assertThat(field.get(mpr)).isSameAs(metrics);
            });
  }

  @Test
  void metricsBeanIsWiredIntoScheduledProjectionRunner() {
    var metrics = mock(org.streamrune.core.StreamRuneMetrics.class);
    runner
        .withUserConfiguration(ScheduledBean.class)
        .withBean(org.streamrune.core.StreamRuneMetrics.class, () -> metrics)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              var spr = ctx.getBean(ScheduledProjectionRunner.class);
              var field = ScheduledProjectionRunner.class.getDeclaredField("metrics");
              field.setAccessible(true);
              assertThat(field.get(spr)).isSameAs(metrics);
            });
  }

  @Test
  void absentMetricsLeavesMultiProjectionRunnerWithNullMetrics() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              var mpr = ctx.getBean(MultiProjectionRunner.class);
              var field = MultiProjectionRunner.class.getDeclaredField("metrics");
              field.setAccessible(true);
              assertThat(field.get(mpr)).isNull();
            });
  }

  /**
   * The mode condition must NOT instantiate projection beans during the BeanFactoryPostProcessor
   * phase (before any BeanPostProcessor is registered). A projection with a field-injected
   * {@code @Autowired} collaborator that is eagerly created in that phase gets a null field (no
   * AutowiredAnnotationBeanPostProcessor active yet) and that broken singleton is cached and reused
   * by the runner. With metadata-only condition evaluation the projection is created later, fully
   * post-processed, so the field is injected.
   */
  @Test
  void continuousProjection_fieldInjectionSurvives_conditionDoesNotEagerlyInstantiate() {
    runner
        .withUserConfiguration(FieldInjectedProjectionConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              var projection = ctx.getBean(FieldInjectedProjection.class);
              assertThat(projection.collaborator)
                  .as(
                      "field-@Autowired collaborator must be injected (projection not created "
                          + "before BeanPostProcessors were registered)")
                  .isNotNull();
            });
  }

  @Test
  void dlqStrategyWithoutDlqBean_throws() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(ProjectionAutoConfig.class))
        .withUserConfiguration(NoDlqInfraConfig.class, DlqProjectionConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(ctx -> assertThat(ctx).hasFailed());
  }

  // === delivery mode: the processor by declared need, the INLINE rule ===

  @Test
  void tlProjection_withoutAProcessorBean_failsNamingTheProjection() {
    runner
        .withUserConfiguration(TlPlainBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("projection 'tl_plain' declares TRANSACTIONAL_LOCAL")
                  .hasStackTraceContaining("no AtomicBatchProcessor bean is available");
            });
  }

  @Test
  void tlBaseProjection_overTheProcessorBean_boots() {
    runner
        .withUserConfiguration(ProcessorRepoConfig.class, TlOverProcessorConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              assertThat(processorOf(ctx.getBean(MultiProjectionRunner.class)))
                  .isSameAs(ctx.getBean(InMemoryProjectionRepository.class));
            });
  }

  @Test
  void tlPlainProjection_underTheProcessorBean_fails_doesNotWriteThrough() {
    runner
        .withUserConfiguration(ProcessorRepoConfig.class, TlPlainBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("projection 'tl_plain' declares TRANSACTIONAL_LOCAL")
                  .hasStackTraceContaining("does not write through the handed repository");
            });
  }

  @Test
  void tlBaseProjection_overAnotherRepository_fails_identity() {
    runner
        .withUserConfiguration(
            ProcessorRepoConfig.class, SecondRepoConfig.class, TlOverBConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining(
                      "'tl_base' declares TRANSACTIONAL_LOCAL and writes to "
                          + RepositoryOnly.class.getName())
                  .hasStackTraceContaining(
                      "is configured with " + InMemoryProjectionRepository.class.getName());
            });
  }

  @Test
  void tlBaseProjection_overAProxiedProcessorBean_verifiesAndBoots() {
    runner
        .withUserConfiguration(ProxiedProcessorConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              Object processor = processorOf(ctx.getBean(MultiProjectionRunner.class));
              assertThat(processor).isSameAs(ctx.getBean("proxiedProcessor"));
              assertThat(Proxy.isProxyClass(processor.getClass()))
                  .as("the runner holds the proxy, not the repository behind it")
                  .isTrue();
            });
  }

  @Test
  void aloBaseProjection_overASecondRepository_bootsBesideTl_andWritesOnlyToIt() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(ProjectionAutoConfig.class))
        .withUserConfiguration(
            EventsConfig.class,
            ProcessorRepoConfig.class,
            SecondRepoConfig.class,
            TlOverProcessorConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var processor = ctx.getBean(InMemoryProjectionRepository.class); // the one processor
              var repoB = ctx.getBean(RepositoryOnly.class);
              // Each registration runs on a thread of its own, and a batch's read-model write lands
              // before its checkpoint: wait for all three outcomes, not only the first one.
              Awaitility.await()
                  .atMost(Duration.ofSeconds(10))
                  .untilAsserted(
                      () -> {
                        assertThat(
                                repoB.delegate.findById(
                                    ProjectionName.of("alo_base"), "row", String.class))
                            .as("the at-least-once projection wrote to its own repository")
                            .contains("v");
                        assertThat(
                                processor.findById(
                                    ProjectionName.of("tl_base"), "row", String.class))
                            .as(
                                "the TRANSACTIONAL_LOCAL sibling wrote inside the processor's"
                                    + " transaction")
                            .contains("v");
                        assertThat(processor.committedOffset(ProjectionName.of("alo_base")))
                            .as(
                                "the at-least-once projection's checkpoint is in the processor's"
                                    + " store")
                            .isEqualTo(GlobalOffset.of(1));
                      });
              assertThat(processor.hasReadModels(ProjectionName.of("alo_base")))
                  .as("nothing of the at-least-once projection lands in the processor's store")
                  .isFalse();
              assertThat(repoB.delegate.hasReadModels(ProjectionName.of("tl_base")))
                  .as("nothing of the TRANSACTIONAL_LOCAL projection lands in the second store")
                  .isFalse();
            });
  }

  @Test
  void aloOnly_leadershipOff_selectsTheNonatomicProcessor_andLogsItByName() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx ->
                assertThat(processorOf(ctx.getBean(MultiProjectionRunner.class)))
                    .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce()));
  }

  @Test
  void aloOnly_leadershipOn_requiresTheBeanForFencing() {
    runner
        .withUserConfiguration(ContinuousBean.class, RealLeadershipConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("single-active-consumer leadership")
                  .hasStackTraceContaining("to honour its epoch");
            });
  }

  @Test
  void aloOnly_leadershipOn_withTheBean_bootsAndUsesIt() {
    runner
        .withUserConfiguration(
            ContinuousBean.class, RealLeadershipConfig.class, ProcessorRepoConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              assertThat(processorOf(ctx.getBean(MultiProjectionRunner.class)))
                  .isInstanceOf(InMemoryProjectionRepository.class)
                  .isSameAs(ctx.getBean(InMemoryProjectionRepository.class));
            });
  }

  @Test
  void twoProcessorBeans_fail() {
    runner
        .withUserConfiguration(TwoProcessorsConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("2 AtomicBatchProcessor beans");
            });
  }

  @Test
  void inlineTl_failsAtDiscovery() {
    runner
        .withUserConfiguration(InlineTlBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining(
                      "projection 'inline_tl' is INLINE and declares TRANSACTIONAL_LOCAL");
            });
  }

  @Test
  void inlineAlo_boots_andRecordsTheGauge() {
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(ProjectionAutoConfig.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      runner
          .withUserConfiguration(InlineBean.class, RecordingMetricsConfig.class)
          .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
          .run(
              ctx -> {
                assertThat(ctx).hasSingleBean(InlineProjectionInterceptor.class);
                assertThat(ctx.getBean(RecordingMetrics.class).modes)
                    .containsExactly(
                        Map.entry("inline", ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT));
              });
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    assertThat(appender.list)
        .filteredOn(e -> e.getLevel() == ch.qos.logback.classic.Level.INFO)
        .extracting(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .as("one INFO line names the INLINE projection and its mode")
        .containsExactly(
            "Projection 'inline' delivery mode AT_LEAST_ONCE_IDEMPOTENT inline — post-commit on"
                + " the command thread, no checkpoint; a failure is logged and the batch is not"
                + " retried");
  }

  @Test
  void theCoarsePropertyIsNoLongerRead() {
    runner
        .withUserConfiguration(ProcessorRepoConfig.class, TlPlainBean.class)
        .withPropertyValues(
            "streamrune.projections.auto-discovery.enabled=true",
            "streamrune.projections.allow-at-least-once=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("does not write through the handed repository");
            });
  }

  @Test
  void scheduledTlPlain_fails_andScheduledAloBoots() {
    runner
        .withUserConfiguration(ProcessorRepoConfig.class, SchedTlPlainBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining(
                      "projection 'sched_tl_plain' declares TRANSACTIONAL_LOCAL")
                  .hasStackTraceContaining("does not write through the handed repository");
            });
    runner
        .withUserConfiguration(ProcessorRepoConfig.class, ScheduledBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ScheduledProjectionRunner.class);
              assertThat(processorOf(ctx.getBean(ScheduledProjectionRunner.class)))
                  .as("an at-least-once-only runner is not handed the bean because it exists")
                  .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
            });
  }

  @Test
  void scheduledTlBase_overTheProcessorBean_bootsAndUsesIt() {
    runner
        .withUserConfiguration(ProcessorRepoConfig.class, SchedTlOverProcessorConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ScheduledProjectionRunner.class);
              assertThat(processorOf(ctx.getBean(ScheduledProjectionRunner.class)))
                  .isSameAs(ctx.getBean(InMemoryProjectionRepository.class));
            });
  }

  /** A runner's processor: not exposed, so read from its private field. */
  private static Object processorOf(Object projectionRunner) {
    return ReflectionTestUtils.getField(projectionRunner, "atomicProcessor");
  }

  // === bean configurations ===

  @Configuration
  static class InfraConfig {
    @Bean
    EventStore eventStore() {
      return mock(EventStore.class);
    }

    @Bean
    OffsetStore offsetStore() {
      return mock(OffsetStore.class);
    }

    @Bean
    ProjectionDeadLetterStore deadLetterStore() {
      return mock(ProjectionDeadLetterStore.class);
    }
  }

  @ProjectionConfig(name = "cont", deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class ContProjection implements Projection {
    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "sched",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class SchedProjection implements Projection {
    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }

  @ProjectionConfig(
      name = "inline",
      mode = ProjectionConfig.Mode.INLINE,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class InlineProjectionImpl implements Projection {
    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }

  @Configuration
  static class ContinuousBean {
    @Bean
    Projection cont() {
      return new ContProjection();
    }
  }

  @Configuration
  static class ScheduledBean {
    @Bean
    Projection sched() {
      return new SchedProjection();
    }
  }

  @Configuration
  static class InlineBean {
    @Bean
    Projection inline() {
      return new InlineProjectionImpl();
    }
  }

  @Configuration
  static class UserMPRConfig {
    @Bean
    MultiProjectionRunner userMPR(EventStore es, OffsetStore os, Projection cont) {
      return MultiProjectionRunner.builder()
          .eventStore(es)
          .offsetStore(os)
          .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
          .register("user-cont", cont, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
          .build();
    }
  }

  @ProjectionConfig(name = "dup", deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class DupA implements Projection {
    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }

  @ProjectionConfig(name = "dup", deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class DupB implements Projection {
    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }

  @Configuration
  static class DuplicateNameConfig {
    @Bean
    Projection a() {
      return new DupA();
    }

    @Bean
    Projection b() {
      return new DupB();
    }
  }

  @ProjectionConfig(
      name = "no-cron",
      mode = ProjectionConfig.Mode.SCHEDULED,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class NoCronProjection implements Projection {
    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }

  @Configuration
  static class ScheduledMissingCronConfig {
    @Bean
    Projection p() {
      return new NoCronProjection();
    }
  }

  @Configuration
  static class NoDlqInfraConfig {
    @Bean
    EventStore eventStore() {
      return mock(EventStore.class);
    }

    @Bean
    OffsetStore offsetStore() {
      return mock(OffsetStore.class);
    }
  }

  @ProjectionConfig(
      name = "dlq-proj",
      errorStrategy = org.streamrune.core.projection.ProjectionErrorStrategy.DLQ,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class DlqProjectionImpl implements Projection {
    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }

  @Configuration
  static class DlqProjectionConfig {
    @Bean
    Projection dlqProj() {
      return new DlqProjectionImpl();
    }
  }

  /**
   * Collaborator that a projection field-injects — proves post-processing ran on the projection.
   */
  static class Collaborator {}

  @ProjectionConfig(
      name = "field-injected",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class FieldInjectedProjection implements Projection {
    @org.springframework.beans.factory.annotation.Autowired Collaborator collaborator;

    @Override
    public void process(java.util.List<org.streamrune.core.EventEnvelope> batch) {}
  }

  @Configuration
  static class FieldInjectedProjectionConfig {
    @Bean
    Collaborator collaborator() {
      return new Collaborator();
    }

    @Bean
    FieldInjectedProjection fieldInjectedProjection() {
      return new FieldInjectedProjection();
    }
  }

  // === delivery-mode fixtures ===
  //
  // The registration names use underscores so the @ProjectionConfig name and the BaseProjection
  // name are the same string: the tests read the processor's checkpoint under the registration name
  // and its read models under the BaseProjection name, and the JDBC name rule would refuse a hyphen
  // in a real application anyway.

  @ProjectionConfig(name = "tl_plain", deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class TlPlainBean implements Projection {
    public TlPlainBean() {}

    @Override
    public void process(List<EventEnvelope> b) {}
  }

  @ProjectionConfig(name = "tl_base", deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class TlBaseBean extends BaseProjection {
    public TlBaseBean(ProjectionRepository repo) {
      super(repo, "tl_base");
    }

    @Override
    public void process(List<EventEnvelope> b) {
      save("row", "v");
    }
  }

  @ProjectionConfig(
      name = "alo_base",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class AloBaseBean extends BaseProjection {
    public AloBaseBean(ProjectionRepository repo) {
      super(repo, "alo_base");
    }

    @Override
    public void process(List<EventEnvelope> b) {
      save("row", "v");
    }
  }

  @ProjectionConfig(
      name = "inline_tl",
      mode = ProjectionConfig.Mode.INLINE,
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class InlineTlBean implements Projection {
    public InlineTlBean() {}

    @Override
    public void process(List<EventEnvelope> b) {}
  }

  @ProjectionConfig(
      name = "sched_tl_plain",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class SchedTlPlainBean implements Projection {
    public SchedTlPlainBean() {}

    @Override
    public void process(List<EventEnvelope> b) {}
  }

  @ProjectionConfig(
      name = "sched_tl_base",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class SchedTlBaseBean extends BaseProjection {
    public SchedTlBaseBean(ProjectionRepository repo) {
      super(repo, "sched_tl_base");
    }

    @Override
    public void process(List<EventEnvelope> b) {
      save("row", "v");
    }
  }

  record Ping(int n) implements org.streamrune.core.DomainEvent {}

  /**
   * ONE face of an InMemoryProjectionRepository, as a separate object. The rule this encodes: every
   * bean OBJECT that implements AtomicBatchProcessor is a candidate for {@code
   * ObjectProvider<AtomicBatchProcessor>.orderedStream()} the moment it is instantiated — Spring's
   * AbstractBeanFactory.isTypeMatch matches an existing singleton by {@code
   * typeToMatch.isInstance(beanInstance)}, never by the @Bean method's declared return type — and
   * discovery ({@code ctx.getBeansOfType(Projection.class)} in ProjectionAutoConfig) instantiates
   * every projection bean, and so its repository, BEFORE the processor is resolved. A second
   * InMemoryProjectionRepository declared as ProjectionRepository would therefore be a second
   * processor candidate and fail the "2 AtomicBatchProcessor beans" check — never the identity
   * check, never a clean boot. A second store that is not a processor must be an object that is not
   * one.
   */
  static final class RepositoryOnly implements ProjectionRepository {
    final InMemoryProjectionRepository delegate = new InMemoryProjectionRepository();

    @Override
    public <T> void save(ProjectionName n, String id, T readModel) {
      delegate.save(n, id, readModel);
    }

    @Override
    public <T> Optional<T> findById(ProjectionName n, String id, Class<T> type) {
      return delegate.findById(n, id, type);
    }

    @Override
    public <T> List<T> findAll(ProjectionName n, Class<T> type) {
      return delegate.findAll(n, type);
    }

    @Override
    public void delete(ProjectionName n, String id) {
      delegate.delete(n, id);
    }

    @Override
    public <T> Page<T> findAll(ProjectionName n, Class<T> type, PageRequest page) {
      return delegate.findAll(n, type, page);
    }

    @Override
    public <T> Optional<Versioned<T>> findById(
        ProjectionName n, String id, Class<T> type, LockMode lock) {
      return delegate.findById(n, id, type, lock);
    }

    @Override
    public <T> void save(ProjectionName n, String id, T readModel, long expectedVersion) {
      delegate.save(n, id, readModel, expectedVersion);
    }
    // writeTargetIdentity() keeps the interface default — this object: a store of its own, which
    // is exactly what the identity check must see.
  }

  /**
   * The OffsetStore face of the processor, as a separate object — the same rule as RepositoryOnly.
   */
  static final class OffsetStoreOnly implements OffsetStore {
    private final InMemoryProjectionRepository delegate;

    OffsetStoreOnly(InMemoryProjectionRepository delegate) {
      this.delegate = delegate;
    }

    @Override
    public GlobalOffset getLastOffset(ProjectionName n) {
      return delegate.getLastOffset(n);
    }

    @Override
    public void saveOffset(ProjectionName n, GlobalOffset o) {
      delegate.saveOffset(n, o);
    }

    @Override
    public void reset(ProjectionName n) {
      delegate.reset(n);
    }
  }

  /** Captures what the INLINE path records; every other entry point keeps the no-op default. */
  static final class RecordingMetrics implements StreamRuneMetrics {
    final Map<String, ProjectionDeliveryMode> modes = new ConcurrentHashMap<>();

    @Override
    public void recordProjectionDeliveryMode(
        ProjectionName projectionName, ProjectionDeliveryMode mode) {
      modes.put(projectionName.value(), mode);
    }
  }

  /** The single AtomicBatchProcessor candidate, declared with its concrete type. */
  @Configuration
  static class ProcessorRepoConfig {
    @Bean
    InMemoryProjectionRepository projectionRepository() {
      return new InMemoryProjectionRepository();
    }
  }

  /**
   * A second repository that is NOT an AtomicBatchProcessor, and an at-least-once projection on it.
   */
  @Configuration
  static class SecondRepoConfig {
    @Bean
    RepositoryOnly repoB() {
      return new RepositoryOnly();
    }

    @Bean
    AloBaseBean aloBase(RepositoryOnly repoB) {
      return new AloBaseBean(repoB);
    }
  }

  @Configuration
  static class TlOverProcessorConfig {
    @Bean
    TlBaseBean tlBase(InMemoryProjectionRepository projectionRepository) {
      return new TlBaseBean(projectionRepository);
    }
  }

  @Configuration
  static class TlOverBConfig {
    @Bean
    TlBaseBean tlBase(RepositoryOnly repoB) {
      return new TlBaseBean(repoB);
    }
  }

  @Configuration
  static class SchedTlOverProcessorConfig {
    @Bean
    SchedTlBaseBean schedTlBase(InMemoryProjectionRepository projectionRepository) {
      return new SchedTlBaseBean(projectionRepository);
    }
  }

  /**
   * Real stores for a runner that must actually deliver: one event in an in-memory event store, and
   * the processor's own checkpoint as the runner's OffsetStore — through an object that is not a
   * processor (returning the processor itself under a second bean name would make it a second
   * candidate, by the RepositoryOnly rule).
   */
  @Configuration
  static class EventsConfig {
    @Bean
    EventStore eventStore() {
      var store = new InMemoryEventStore();
      store.append(
          StreamId.of(AggregateType.of("ping"), AggregateId.of("s")),
          List.of(
              EventStoreFixture.event(
                  StreamId.of(AggregateType.of("ping"), AggregateId.of("s")),
                  new Version(1),
                  new EventType("Ping"),
                  new Ping(1))),
          new Version(0));
      return store;
    }

    @Bean
    OffsetStore offsetStore(InMemoryProjectionRepository projectionRepository) {
      return new OffsetStoreOnly(projectionRepository);
    }

    @Bean
    ProjectionDeadLetterStore deadLetterStore() {
      return mock(ProjectionDeadLetterStore.class);
    }
  }

  /** One bean object, one candidate: a JDK proxy over a repository that is not itself a bean. */
  @Configuration
  static class ProxiedProcessorConfig {
    private final InMemoryProjectionRepository real = new InMemoryProjectionRepository();

    @Bean
    AtomicBatchProcessor proxiedProcessor() {
      var factory = new ProxyFactory(real);
      factory.setInterfaces(ProjectionRepository.class, AtomicBatchProcessor.class);
      return (AtomicBatchProcessor) factory.getProxy();
    }

    @Bean
    TlBaseBean tlBase(AtomicBatchProcessor proxiedProcessor) {
      return new TlBaseBean((ProjectionRepository) proxiedProcessor);
    }
  }

  @Configuration
  static class RealLeadershipConfig {
    @Bean
    SubscriptionLeadership leadership() {
      return mock(SubscriptionLeadership.class);
    }
  }

  /** Two processor candidates, by the RepositoryOnly rule, and a projection that needs one. */
  @Configuration
  static class TwoProcessorsConfig {
    @Bean
    InMemoryProjectionRepository processorA() {
      return new InMemoryProjectionRepository();
    }

    @Bean
    InMemoryProjectionRepository processorB() {
      return new InMemoryProjectionRepository();
    }

    @Bean
    TlBaseBean tlBase() {
      return new TlBaseBean(processorA());
    }
  }

  @Configuration
  static class RecordingMetricsConfig {
    @Bean
    RecordingMetrics metrics() {
      return new RecordingMetrics();
    }
  }
}
