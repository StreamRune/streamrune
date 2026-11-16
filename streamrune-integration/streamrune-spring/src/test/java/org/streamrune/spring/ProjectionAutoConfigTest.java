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
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.LockMode;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.Versioned;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
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
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;

class ProjectionAutoConfigTest {

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder().registerEvent(Ping.class).build();

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
            "streamrune.polling-interval=123ms",
            "streamrune.polling-jitter=PT0.045S")
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
  void absentMetricsLeavesMultiProjectionRunnerWithNoopMetrics() {
    runner
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              var mpr = ctx.getBean(MultiProjectionRunner.class);
              var field = MultiProjectionRunner.class.getDeclaredField("metrics");
              field.setAccessible(true);
              assertThat(field.get(mpr)).isSameAs(org.streamrune.core.StreamRuneMetrics.NOOP);
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

  // === routing: each registration to a store or to the subscription checkpoint; INLINE rule ===

  @Test
  void tlProjection_withoutAStore_failsNamingTheProjection() {
    runner
        .withUserConfiguration(TlPlainBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("projection 'tl_plain' (TRANSACTIONAL_LOCAL)")
                  .hasStackTraceContaining("no read-model store is transactional");
            });
  }

  // ---- A BaseProjection behind a CGLIB proxy ----

  @Test
  void aCglibProxyOverABaseProjection_runsTheFinalProcessOnTheUninitialisedProxy() {
    // Why the refusal below exists: CGLIB cannot override BaseProjection's final two-argument
    // process, so it runs on the proxy instance itself, whose fields were never initialised.
    AloBaseBean proxy = cglibProxyOf(new AloBaseBean(new InMemoryProjectionRepository()));

    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> proxy.process(List.of(), new InMemoryProjectionRepository()))
        .isInstanceOf(NullPointerException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"continuous", "scheduled"})
  void aCglibProxiedBaseProjection_isRefusedAtStartup_naming_theBeanAndTheRemedy(String mode) {
    runner
        .withUserConfiguration(
            "continuous".equals(mode)
                ? CglibProxiedContinuousConfig.class
                : CglibProxiedScheduledConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("Projection bean 'proxiedProjection'")
                  .hasStackTraceContaining("is a CGLIB proxy of BaseProjection subclass")
                  .hasStackTraceContaining("process(List, ProjectionRepository) is final")
                  .hasStackTraceContaining("implement Projection directly");
            });
  }

  @Test
  void aCglibProxiedBaseProjection_inInlineMode_boots_becauseInlineCallsTheOneArgumentProcess() {
    runner
        .withUserConfiguration(CglibProxiedInlineConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(ctx -> assertThat(ctx).hasSingleBean(InlineProjectionInterceptor.class));
  }

  @SuppressWarnings("unchecked")
  private static <T> T cglibProxyOf(T target) {
    ProxyFactory factory = new ProxyFactory(target);
    factory.setProxyTargetClass(true);
    return (T) factory.getProxy();
  }

  @Configuration
  static class CglibProxiedContinuousConfig {
    @Bean
    AloBaseBean proxiedProjection() {
      return cglibProxyOf(new AloBaseBean(new InMemoryProjectionRepository()));
    }
  }

  @Configuration
  static class CglibProxiedScheduledConfig {
    @Bean
    SchedAloBaseBean proxiedProjection() {
      return cglibProxyOf(new SchedAloBaseBean(new InMemoryProjectionRepository()));
    }
  }

  @Configuration
  static class CglibProxiedInlineConfig {
    @Bean
    InlineBaseBean proxiedProjection() {
      return cglibProxyOf(new InlineBaseBean(new InMemoryProjectionRepository()));
    }
  }

  @ProjectionConfig(
      name = "sched_alo_base",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class SchedAloBaseBean extends BaseProjection {
    public SchedAloBaseBean(ProjectionRepository repo) {
      super(repo, "sched_alo_base");
    }

    @Override
    public void process(List<EventEnvelope> b) {
      save("row", "v");
    }
  }

  @ProjectionConfig(
      name = "inline_base",
      mode = ProjectionConfig.Mode.INLINE,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class InlineBaseBean extends BaseProjection {
    public InlineBaseBean(ProjectionRepository repo) {
      super(repo, "inline_base");
    }

    @Override
    public void process(List<EventEnvelope> b) {
      save("row", "v");
    }
  }

  @Test
  void tlBaseProjection_overTheStoresRepository_isRoutedToTheStore() {
    runner
        .withUserConfiguration(MemoryStoreConfig.class, TlOverProcessorConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              var mpr = ctx.getBean(MultiProjectionRunner.class);
              assertThat(processorOf(mpr, "tl_base"))
                  .isSameAs(ctx.getBean(InMemoryProjectionRepository.class));
              assertThat(targetNameOf(mpr, "tl_base")).isEqualTo("memory");
            });
  }

  @Test
  void aDeclaredStore_routesThere_evenForAnAtLeastOnceProjectionWithNoWriteTarget() {
    runner
        .withUserConfiguration(MemoryStoreConfig.class, DeclaredStoreBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              var mpr = ctx.getBean(MultiProjectionRunner.class);
              assertThat(processorOf(mpr, "declared"))
                  .isSameAs(ctx.getBean(InMemoryProjectionRepository.class));
              assertThat(targetNameOf(mpr, "declared")).isEqualTo("memory");
            });
  }

  @Test
  void anUnknownDeclaredStore_failsNamingTheStores() {
    runner
        .withUserConfiguration(DeclaredStoreBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining(
                      "projection 'declared' (AT_LEAST_ONCE_IDEMPOTENT) declares readModelStore ="
                          + " \"memory\", but no read-model store has that name")
                  .hasStackTraceContaining("no read-model store is defined");
            });
  }

  @Test
  void withoutASubscriptionCheckpointBean_theRunnerFailsNamingTheBean() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(ProjectionAutoConfig.class))
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .withBean(ProjectionDeadLetterStore.class, () -> mock(ProjectionDeadLetterStore.class))
        .withUserConfiguration(ContinuousBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("No streamRuneSubscriptionCheckpoint bean");
            });
  }

  @Test
  void twoStoresWithOneName_fail() {
    runner
        .withUserConfiguration(TwoStoresConfig.class, ContinuousBean.class)
        .withPropertyValues(
            "streamrune.projections.auto-discovery.enabled=true", "test.second-store-name=a")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("Two read-model stores are named 'a'");
            });
  }

  @Test
  void tlPlainProjection_onTheOnlyTransactionalStore_fails_doesNotWriteThrough() {
    runner
        .withUserConfiguration(MemoryStoreConfig.class, TlPlainBean.class)
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
  void tlBaseProjection_overARepositoryOfNoStore_fails() {
    runner
        .withUserConfiguration(MemoryStoreConfig.class, SecondRepoConfig.class, TlOverBConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("projection 'tl_base' (TRANSACTIONAL_LOCAL)")
                  .hasStackTraceContaining("its writeTarget() belongs to no read-model store")
                  .hasStackTraceContaining("'memory' (transactional, fences its checkpoint)");
            });
  }

  @Test
  void tlBaseProjection_overAProxiedStoreRepository_isMatchedThroughTheProxyAndBoots() {
    runner
        .withUserConfiguration(ProxiedProcessorConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              Object processor = processorOf(ctx.getBean(MultiProjectionRunner.class), "tl_base");
              assertThat(processor).isSameAs(ctx.getBean("proxiedProcessor"));
              assertThat(Proxy.isProxyClass(processor.getClass()))
                  .as("the runner holds the proxy, not the repository behind it")
                  .isTrue();
            });
  }

  @Test
  void aloBaseProjection_overASecondRepository_bootsBesideTl_andCheckpointsInTheSubscription() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(ProjectionAutoConfig.class))
        .withUserConfiguration(
            EventsConfig.class,
            MemoryStoreConfig.class,
            SecondRepoConfig.class,
            TlOverProcessorConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var processor = ctx.getBean(InMemoryProjectionRepository.class); // the store
              var subscription = ctx.getBean(InMemoryOffsetStore.class);
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
                        assertThat(processor.committedOffset(ProjectionName.of("tl_base")))
                            .as("the TRANSACTIONAL_LOCAL projection checkpoints in its store")
                            .isEqualTo(GlobalOffset.of(1));
                        assertThat(subscription.getLastOffset(ProjectionName.of("alo_base")))
                            .as(
                                "the at-least-once projection, matching no store, checkpoints in"
                                    + " the subscription checkpoint")
                            .isEqualTo(GlobalOffset.of(1));
                      });
              assertThat(processor.committedOffset(ProjectionName.of("alo_base")))
                  .as("the at-least-once projection's checkpoint is not in the store")
                  .isEqualTo(GlobalOffset.initial());
              assertThat(processor.hasReadModels(ProjectionName.of("alo_base")))
                  .as("nothing of the at-least-once projection lands in the processor's store")
                  .isFalse();
              assertThat(repoB.delegate.hasReadModels(ProjectionName.of("tl_base")))
                  .as("nothing of the TRANSACTIONAL_LOCAL projection lands in the second store")
                  .isFalse();
            });
  }

  @Test
  void aloWithNoWriteTarget_isRoutedToTheSubscriptionCheckpoint_evenBesideAStore() {
    runner
        .withUserConfiguration(ContinuousBean.class, MemoryStoreConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              var mpr = ctx.getBean(MultiProjectionRunner.class);
              assertThat(processorOf(mpr, "cont"))
                  .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
              assertThat(targetNameOf(mpr, "cont")).isEqualTo("subscription_checkpoint");
            });
  }

  @Test
  void aloOnly_leadershipOn_aNonFencingSubscriptionCheckpointFails() {
    runner
        .withUserConfiguration(ContinuousBean.class, RealLeadershipConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("is routed to 'subscription_checkpoint'")
                  .hasStackTraceContaining("does not fence its checkpoint")
                  .hasStackTraceContaining("single-active-consumer leadership");
            });
  }

  @Test
  void aloBase_overAStoresRepository_leadershipOn_isRoutedToTheFencingStore() {
    runner
        .withUserConfiguration(
            RealLeadershipConfig.class, MemoryStoreConfig.class, AloOverStoreConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(MultiProjectionRunner.class);
              assertThat(processorOf(ctx.getBean(MultiProjectionRunner.class), "alo_base"))
                  .isSameAs(ctx.getBean(InMemoryProjectionRepository.class));
            });
  }

  @Test
  void tlPlain_withTwoTransactionalStores_failsAskingForADeclaration() {
    runner
        .withUserConfiguration(TwoStoresConfig.class, TlPlainBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("several read-model stores are transactional ('a', 'b')")
                  .hasStackTraceContaining("declare readModelStore");
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
        .withUserConfiguration(MemoryStoreConfig.class, TlPlainBean.class)
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
        .withUserConfiguration(MemoryStoreConfig.class, SchedTlPlainBean.class)
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
        .withUserConfiguration(MemoryStoreConfig.class, ScheduledBean.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ScheduledProjectionRunner.class);
              assertThat(processorOf(ctx.getBean(ScheduledProjectionRunner.class), "sched"))
                  .as("an at-least-once registration matching no store is not routed to it")
                  .isSameAs(AtomicBatchProcessor.nonAtomicAtLeastOnce());
            });
  }

  @Test
  void scheduledTlBase_overTheStoresRepository_isRoutedToTheStore() {
    runner
        .withUserConfiguration(MemoryStoreConfig.class, SchedTlOverProcessorConfig.class)
        .withPropertyValues("streamrune.projections.auto-discovery.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(ScheduledProjectionRunner.class);
              var spr = ctx.getBean(ScheduledProjectionRunner.class);
              assertThat(processorOf(spr, "sched_tl_base"))
                  .isSameAs(ctx.getBean(InMemoryProjectionRepository.class));
              assertThat(targetNameOf(spr, "sched_tl_base")).isEqualTo("memory");
            });
  }

  /** The processor a registration commits through: not exposed, so read from the runner. */
  private static Object processorOf(Object projectionRunner, String projection) {
    return ReflectionTestUtils.invokeMethod(pairOf(projectionRunner, projection), "processor");
  }

  /** The name of the target a registration was routed to. */
  private static String targetNameOf(Object projectionRunner, String projection) {
    return ReflectionTestUtils.invokeMethod(pairOf(projectionRunner, projection), "targetName");
  }

  private static Object pairOf(Object projectionRunner, String projection) {
    var pairs = (Map<?, ?>) ReflectionTestUtils.getField(projectionRunner, "checkpoints");
    return pairs.get(ProjectionName.of(projection));
  }

  // === bean configurations ===

  /**
   * The event store's subscription checkpoint, as {@code StreamRuneAutoConfiguration} registers it
   * without a DataSource: the {@code OffsetStore} bean and the nonatomic processor.
   */
  @Configuration
  static class SubscriptionCheckpointConfig {
    @Bean(StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT)
    CheckpointTarget subscriptionCheckpoint(OffsetStore offsetStore) {
      return new CheckpointTarget(
          "subscription_checkpoint", offsetStore, AtomicBatchProcessor.nonAtomicAtLeastOnce());
    }
  }

  @Configuration
  @org.springframework.context.annotation.Import(SubscriptionCheckpointConfig.class)
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
          .register(
              ProjectionName.of("user-cont"), cont, ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
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
  @org.springframework.context.annotation.Import(SubscriptionCheckpointConfig.class)
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
   * ONE face of an InMemoryProjectionRepository, as a separate object: a repository that belongs to
   * no read-model store and is not itself an offset store, so it is not a second {@code
   * OffsetStore} bean beside the subscription checkpoint's.
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
    public <T> Page<T> findAllBy(
        ProjectionName n,
        Class<T> type,
        org.streamrune.core.projection.FieldEquals filter,
        PageRequest page) {
      return delegate.findAllBy(n, type, filter, page);
    }

    @Override
    public void declareIndex(org.streamrune.core.projection.ProjectionIndex index) {
      delegate.declareIndex(index);
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

  /** Captures what the INLINE path records; every other entry point keeps the no-op default. */
  static final class RecordingMetrics implements StreamRuneMetrics {
    final Map<String, ProjectionDeliveryMode> modes = new ConcurrentHashMap<>();

    @Override
    public void recordProjectionDeliveryMode(
        ProjectionName projectionName, ProjectionDeliveryMode mode) {
      modes.put(projectionName.value(), mode);
    }
  }

  /**
   * One read-model store, {@code memory}: an in-memory repository that is its own checkpoints and
   * its own transactional, fencing processor.
   */
  @Configuration
  static class MemoryStoreConfig {
    @Bean
    InMemoryProjectionRepository projectionRepository() {
      return new InMemoryProjectionRepository();
    }

    @Bean
    ReadModelStore memoryStore(InMemoryProjectionRepository projectionRepository) {
      return ReadModelStore.of(
          "memory", projectionRepository, projectionRepository, projectionRepository);
    }
  }

  /** Two transactional stores, {@code a} and the one {@code test.second-store-name} names. */
  @Configuration
  static class TwoStoresConfig {
    @Bean
    ReadModelStore storeA() {
      var repository = new InMemoryProjectionRepository();
      return ReadModelStore.of("a", repository, repository, repository);
    }

    @Bean
    ReadModelStore storeB(org.springframework.core.env.Environment env) {
      var repository = new InMemoryProjectionRepository();
      return ReadModelStore.of(
          env.getProperty("test.second-store-name", "b"), repository, repository, repository);
    }
  }

  /** An at-least-once projection that names the {@code memory} store and writes nowhere. */
  @ProjectionConfig(
      name = "declared",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
      readModelStore = "memory")
  static class DeclaredStoreBean implements Projection {
    @Override
    public void process(List<EventEnvelope> b) {}
  }

  @Configuration
  static class AloOverStoreConfig {
    @Bean
    AloBaseBean aloBase(InMemoryProjectionRepository projectionRepository) {
      return new AloBaseBean(projectionRepository);
    }
  }

  /** A second repository that belongs to no store, and an at-least-once projection on it. */
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
   * an in-memory offset store of its own as the subscription checkpoint.
   */
  @Configuration
  @org.springframework.context.annotation.Import(SubscriptionCheckpointConfig.class)
  static class EventsConfig {
    @Bean
    EventStore eventStore() {
      var store = new InMemoryEventStore(TYPES);
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
    InMemoryOffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }

    @Bean
    ProjectionDeadLetterStore deadLetterStore() {
      return mock(ProjectionDeadLetterStore.class);
    }
  }

  /**
   * A store whose repository and processor are one JDK proxy over a repository that is not itself a
   * bean: routing matches the projection's write target to the store by {@code
   * writeTargetIdentity()}, which the proxy forwards.
   */
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
    ReadModelStore proxiedStore(AtomicBatchProcessor proxiedProcessor) {
      return ReadModelStore.of(
          "proxied", (ProjectionRepository) proxiedProcessor, real, proxiedProcessor);
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

  @Configuration
  static class RecordingMetricsConfig {
    @Bean
    RecordingMetrics metrics() {
      return new RecordingMetrics();
    }
  }
}
