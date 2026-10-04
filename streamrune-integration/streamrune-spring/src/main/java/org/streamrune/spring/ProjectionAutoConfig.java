package org.streamrune.spring;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorClassifier;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.integration.ProjectionDiscoveryMessages;
import org.streamrune.integration.ProjectionProcessorResolution;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.ScheduledProjectionRunner;

/**
 * Builds {@link MultiProjectionRunner}, {@link ScheduledProjectionRunner}, and {@link
 * InlineProjectionInterceptor} from {@link ProjectionConfig @ProjectionConfig}-annotated {@link
 * Projection} beans.
 *
 * <p>Every registration carries the {@link ProjectionDeliveryMode} its {@code
 * ProjectionConfig.deliveryMode()} declares. Each runner's {@link AtomicBatchProcessor} is chosen
 * by {@link ProjectionProcessorResolution#select} from what its registrations declare and from
 * whether the leadership bean needs its epoch honoured — never from the presence of a {@code
 * DataSource} or of a processor bean: a runner of {@code AT_LEAST_ONCE_IDEMPOTENT} registrations
 * without leadership runs on {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()}. An INLINE
 * registration must declare {@code AT_LEAST_ONCE_IDEMPOTENT}.
 *
 * <p>Disabled when {@code streamrune.projections.auto-discovery.enabled=false} (or any other
 * canonical-false spelling RelaxedBoolean accepts — see {@link AutoDiscoveryEnabledCondition}).
 */
@AutoConfiguration
@EnableConfigurationProperties(StreamRuneProperties.class)
@Conditional(ProjectionAutoConfig.AutoDiscoveryEnabledCondition.class)
public class ProjectionAutoConfig {

  private static final Logger log = LoggerFactory.getLogger(ProjectionAutoConfig.class);

  /**
   * {@code @ConditionalOnProperty(..., havingValue = "true")} required an EXACT {@code "true"}
   * match, so a non-canonical truthy spelling ({@code on}, {@code yes}, {@code 1}) did not match
   * {@code havingValue} and therefore silently DISABLED this whole auto-config — the opposite of
   * the operator's intent, and inconsistent with Quarkus ({@code ProjectionProducer}, already
   * routed through {@link org.streamrune.integration.RelaxedBoolean}) and Micronaut, whose
   * {@code @Requires(notEquals = "false")} kept those same spellings ENABLED. Routes the property
   * through the same {@link org.streamrune.integration.RelaxedBoolean} parser every other
   * StreamRune boolean knob uses, so all three runtimes agree on every spelling. Absent (or blank)
   * defaults to enabled, mirroring the old {@code matchIfMissing = true}.
   */
  static final class AutoDiscoveryEnabledCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
      String value =
          context.getEnvironment().getProperty("streamrune.projections.auto-discovery.enabled");
      return org.streamrune.integration.RelaxedBoolean.parse(value, true);
    }
  }

  @Bean
  @ConditionalOnMissingBean
  public MultiProjectionRunner streamRuneMultiProjectionRunner(
      ApplicationContext ctx,
      Environment env,
      EventStore eventStore,
      OffsetStore offsetStore,
      StreamRuneProperties properties,
      ObjectProvider<AtomicBatchProcessor> atomicProcessorProvider,
      ObjectProvider<ProjectionDeadLetterStore> dlqProvider,
      ObjectProvider<StreamRuneMetrics> metricsProvider,
      ObjectProvider<SubscriptionLeadership> leadershipProvider,
      ObjectProvider<ProjectionErrorClassifier> classifierProvider,
      ObjectProvider<org.streamrune.runtime.SubscriptionHealthContributor> healthProvider) {
    var entries = discover(ctx, env, ProjectionConfig.Mode.CONTINUOUS);
    // No eager @Conditional that instantiates every Projection during the
    // BeanFactoryPostProcessor phase. Instead discover() runs during bean instantiation (all
    // BeanPostProcessors active) and we return null when there is no CONTINUOUS projection —
    // Spring registers no bean, mirroring the Quarkus null-product approach. The name-conditional
    // lifecycle below then finds no bean and does not start anything.
    if (entries.isEmpty()) {
      return null;
    }
    // streamrune.polling-interval-ms / polling-jitter-ms drive the catch-up polling cadence of each
    // projection's event subscription. listenNotifyEnabled=true only takes effect if a
    // HybridEventSubscription is wired via a subscription factory; the default
    // MultiProjectionRunner path builds a PollingEventSubscription, which has no LISTEN/NOTIFY
    // push, so the flag is dormant there.
    var subscriptionConfig =
        new SubscriptionConfig(
            true,
            Duration.ofMillis(properties.pollingIntervalMs()),
            Duration.ofMillis(properties.pollingJitterMs()));
    var builder =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .subscriptionConfig(subscriptionConfig);
    // The processor is chosen by what the registrations DECLARE and by whether the leadership
    // needs its epoch honoured — never by the presence of a DataSource or of the bean. An
    // all-AT_LEAST_ONCE_IDEMPOTENT runner without leadership runs on nonAtomicAtLeastOnce().
    builder.atomicProcessor(
        ProjectionProcessorResolution.select(
            registrationsOf(entries),
            leadershipProvider.getIfAvailable(),
            atomicProcessorProvider.orderedStream().toList(),
            "streamRuneMultiProjectionRunner"));
    // Propagate the metrics collector (e.g. the auto-configured MicrometerStreamRuneMetrics) so
    // projections.* meters are recorded; absent collector leaves projection metrics unchanged.
    metricsProvider.ifAvailable(builder::metrics);
    // Thread the single-active-consumer leadership bean (LeaseBasedLeadership when a
    // DataSource is present and the knob is on; absent otherwise) into every per-projection runner;
    // defaults to SubscriptionLeadership.NOOP (always leader) when no bean exists.
    leadershipProvider.ifAvailable(builder::leadership);
    // Optional user-supplied poison-vs-transient classifier; defaults to
    // ProjectionErrorClassifier.DEFAULT when no bean exists.
    classifierProvider.ifAvailable(builder::classifier);
    // Wire the subscription health contributor so each continuous projection registers its live
    // subscription (health() lists it, overallStatus() reflects it) and delivery outcomes feed the
    // contributor; absent contributor leaves health tracking a no-op.
    healthProvider.ifAvailable(builder::healthContributor);
    var dlq = dlqProvider.getIfAvailable();
    if (dlq != null) {
      builder.deadLetterStore(dlq);
    } else {
      requireNoDlqStrategy(entries);
    }
    for (var e : entries) {
      builder.register(e.name(), e.projection(), e.errorStrategy(), e.deliveryMode());
    }
    return builder.build();
  }

  /**
   * Starts the auto-configured {@link MultiProjectionRunner} on context refresh and stops it on
   * context close, before the DataSource shuts down. Conditional on the auto-configured bean name
   * so user-defined runners (which the user starts and stops themselves) are never started twice.
   */
  @Bean
  @ConditionalOnBean(name = "streamRuneMultiProjectionRunner")
  public RunnerLifecycle streamRuneMultiProjectionRunnerLifecycle(
      ObjectProvider<MultiProjectionRunner> runnerProvider) {
    // streamRuneMultiProjectionRunner returns null when there is no CONTINUOUS projection,
    // which registers a NullBean under that name — @ConditionalOnBean(name=...) still matches it,
    // so resolve via ObjectProvider (which filters NullBeans) and return no lifecycle when absent.
    MultiProjectionRunner runner = runnerProvider.getIfAvailable();
    if (runner == null) {
      return null;
    }
    return new RunnerLifecycle("MultiProjectionRunner", runner::start, runner::stop);
  }

  @Bean
  @ConditionalOnMissingBean
  public ScheduledProjectionRunner streamRuneScheduledProjectionRunner(
      ApplicationContext ctx,
      Environment env,
      EventStore eventStore,
      OffsetStore offsetStore,
      ObjectProvider<AtomicBatchProcessor> atomicProcessorProvider,
      ObjectProvider<ProjectionDeadLetterStore> dlqProvider,
      ObjectProvider<StreamRuneMetrics> metricsProvider,
      ObjectProvider<SubscriptionLeadership> leadershipProvider,
      ObjectProvider<ProjectionErrorClassifier> classifierProvider,
      ObjectProvider<org.streamrune.runtime.SubscriptionHealthContributor> healthProvider) {
    var entries = discover(ctx, env, ProjectionConfig.Mode.SCHEDULED);
    // Return null when there is no SCHEDULED projection (see
    // streamRuneMultiProjectionRunner).
    if (entries.isEmpty()) {
      return null;
    }
    var builder =
        ScheduledProjectionRunner.builder().eventStore(eventStore).offsetStore(offsetStore);
    // The same rule as the CONTINUOUS runner: the processor by declared need, never by bean
    // presence. A TRANSACTIONAL_LOCAL scheduled chunk's read-model write and its checkpoint commit
    // in the processor's one transaction, with the same split-brain guard as CONTINUOUS.
    builder.atomicProcessor(
        ProjectionProcessorResolution.select(
            registrationsOf(entries),
            leadershipProvider.getIfAvailable(),
            atomicProcessorProvider.orderedStream().toList(),
            "streamRuneScheduledProjectionRunner"));
    // Propagate the metrics collector so projections.* meters are recorded for scheduled runs;
    // absent collector leaves projection metrics unchanged.
    metricsProvider.ifAvailable(builder::metrics);
    // Only a leader drains on each tick; NOOP (always leader) when no leadership bean exists.
    leadershipProvider.ifAvailable(builder::leadership);
    // Optional user-supplied poison-vs-transient classifier; DEFAULT when absent.
    classifierProvider.ifAvailable(builder::classifier);
    // Wire the subscription health contributor so each scheduled projection registers itself and
    // feeds its per-chunk outcomes; absent contributor leaves health tracking a no-op.
    healthProvider.ifAvailable(builder::healthContributor);
    var dlq = dlqProvider.getIfAvailable();
    if (dlq != null) {
      builder.deadLetterStore(dlq);
    } else {
      requireNoDlqStrategy(entries);
    }
    for (var e : entries) {
      if (e.cron() == null || e.cron().isBlank()) {
        throw new IllegalStateException(ProjectionDiscoveryMessages.cronRequired(e.name()));
      }
      builder.register(e.name(), e.projection(), e.cron(), e.errorStrategy(), e.deliveryMode());
    }
    return builder.build();
  }

  /**
   * Starts the auto-configured {@link ScheduledProjectionRunner} on context refresh and stops it on
   * context close, before the DataSource shuts down. Conditional on the auto-configured bean name
   * so user-defined runners (which the user starts and stops themselves) are never started twice.
   */
  @Bean
  @ConditionalOnBean(name = "streamRuneScheduledProjectionRunner")
  public RunnerLifecycle streamRuneScheduledProjectionRunnerLifecycle(
      ObjectProvider<ScheduledProjectionRunner> runnerProvider) {
    // See streamRuneMultiProjectionRunnerLifecycle — resolve via ObjectProvider so a
    // null-product (no SCHEDULED projection) NullBean yields no lifecycle instead of a failed
    // injection.
    ScheduledProjectionRunner runner = runnerProvider.getIfAvailable();
    if (runner == null) {
      return null;
    }
    return new RunnerLifecycle("ScheduledProjectionRunner", runner::start, runner::stop);
  }

  /**
   * Runs innermost in the command interceptor chain (see the {@code ORDER_*} constants on {@link
   * StreamRuneAutoConfiguration}): {@code after()} executes in reverse order, so inline projections
   * are applied first once events are committed.
   */
  @Bean
  @org.springframework.core.annotation.Order(StreamRuneAutoConfiguration.ORDER_INLINE_PROJECTION)
  @ConditionalOnMissingBean
  public InlineProjectionInterceptor streamRuneInlineProjectionInterceptor(
      ApplicationContext ctx, Environment env, ObjectProvider<StreamRuneMetrics> metricsProvider) {
    var entries = discover(ctx, env, ProjectionConfig.Mode.INLINE);
    // Return null when there is no INLINE projection (see streamRuneMultiProjectionRunner).
    if (entries.isEmpty()) {
      return null;
    }
    var builder = InlineProjectionInterceptor.builder();
    for (var e : entries) {
      // INLINE has no checkpoint to be transactional with: only AT_LEAST_ONCE_IDEMPOTENT boots.
      ProjectionProcessorResolution.requireInlineMode(e.name(), e.deliveryMode());
      warnNoInlineRecoveryPath(e.name());
      log.info(ProjectionProcessorResolution.INLINE_INFO, LogSanitizer.sanitizeForLog(e.name()));
      metricsProvider.ifAvailable(
          m -> m.recordProjectionDeliveryMode(ProjectionName.of(e.name()), e.deliveryMode()));
      builder.register(e.name(), e.projection());
    }
    return builder.build();
  }

  /**
   * Warns at boot that an INLINE projection has no automatic recovery path.
   *
   * <p>An INLINE projection has no automatic recovery path: if {@code process()} throws, {@code
   * InlineProjectionInterceptor} logs and rethrows, but {@code VirtualThreadCommandBus}'s {@code
   * after()} invoker catches and swallows that (the command result is already decided), so the
   * command still reports SUCCESS while the read-model update for it is silently and permanently
   * lost.
   *
   * <p>The framework's own mitigation — also registering the same projection under {@code
   * CONTINUOUS} or {@code SCHEDULED} so a failed inline update is eventually reapplied — is
   * structurally unreachable through {@code @ProjectionConfig} today: {@code Mode} is single-valued
   * and {@code discover()} rejects a second registration of the same {@code name} under any mode,
   * so one projection bean can never be both INLINE and runner-backed. Closing that gap needs a
   * {@code streamrune-core} change (e.g. {@code ProjectionConfig.modes()} accepting more than one
   * {@link ProjectionConfig.Mode}), out of this auto-configuration's reach.
   *
   * <p>Until then, surface the risk at boot instead of leaving it documented only in {@code
   * InlineProjectionInterceptor}'s javadoc — a WARN (not a hard failure: every current INLINE
   * deployment would trip it, and some inline projections are legitimately cheap/idempotent enough
   * that an operator has already accepted this risk) so it shows up where operators actually look.
   */
  private void warnNoInlineRecoveryPath(String projectionName) {
    log.warn(
        "Projection '{}' is registered in INLINE mode with no automatic recovery path: if its"
            + " process() throws, the triggering command still reports SUCCESS and that command's"
            + " read-model update is silently and permanently lost (see"
            + " InlineProjectionInterceptor's javadoc). Consider whether this projection's failure"
            + " mode is acceptable, or register a companion CONTINUOUS/SCHEDULED projection under"
            + " a different name over the same read model as a recovery path.",
        LogSanitizer.sanitizeForLog(projectionName));
  }

  // === discovery + property override ===

  private static List<DiscoveredEntry> discover(
      ApplicationContext ctx, Environment env, ProjectionConfig.Mode mode) {
    Map<String, Projection> beans = ctx.getBeansOfType(Projection.class);
    var seen = new HashSet<String>();
    var entries = new ArrayList<DiscoveredEntry>();
    for (var entry : beans.entrySet()) {
      Projection bean = entry.getValue();
      ProjectionConfig pc = AnnotationUtils.findAnnotation(bean.getClass(), ProjectionConfig.class);
      if (pc == null) continue;

      String name = pc.name();
      if (!seen.add(name)) {
        throw new IllegalStateException(ProjectionDiscoveryMessages.duplicateName(name));
      }

      String prefix = "streamrune.projections." + name + ".";
      boolean enabled =
          org.streamrune.integration.RelaxedBoolean.parse(
              env.getProperty(prefix + "enabled"), true);
      if (!enabled) continue;

      ProjectionConfig.Mode effectiveMode = pc.mode();
      if (effectiveMode != mode) continue;

      ProjectionErrorStrategy strategy =
          parseStrategy(env.getProperty(prefix + "error-strategy"), pc.errorStrategy(), name);
      String cron = pc.cron();

      entries.add(new DiscoveredEntry(name, bean, cron, strategy, pc.deliveryMode()));
    }
    return entries;
  }

  private static ProjectionErrorStrategy parseStrategy(
      String prop, ProjectionErrorStrategy fallback, String name) {
    if (prop == null || prop.isBlank()) return fallback;
    try {
      return ProjectionErrorStrategy.valueOf(prop.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          ProjectionDiscoveryMessages.invalidErrorStrategy(prop, name), e);
    }
  }

  /** One runner's registrations, as the processor-resolution rule reads them. */
  private static List<ProjectionProcessorResolution.Registration> registrationsOf(
      List<DiscoveredEntry> entries) {
    return entries.stream()
        .map(e -> new ProjectionProcessorResolution.Registration(e.name(), e.deliveryMode()))
        .toList();
  }

  private static void requireNoDlqStrategy(List<DiscoveredEntry> entries) {
    boolean any = entries.stream().anyMatch(e -> e.errorStrategy() == ProjectionErrorStrategy.DLQ);
    if (any) {
      throw new IllegalStateException(
          "DLQ error strategy requested but no ProjectionDeadLetterStore bean is available");
    }
  }

  private record DiscoveredEntry(
      String name,
      Projection projection,
      String cron,
      ProjectionErrorStrategy errorStrategy,
      ProjectionDeliveryMode deliveryMode) {}
}
