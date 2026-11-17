package org.streamrune.spring;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
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
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeadLetterStore;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorClassifier;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.integration.ProjectionDiscoveryMessages;
import org.streamrune.integration.ProjectionRoutes;
import org.streamrune.integration.ProjectionStoreRouting;
import org.streamrune.integration.ReadModelStores;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.ScheduledProjectionRunner;
import org.streamrune.runtime.TracingProjectionDecorator;

/**
 * Builds {@link MultiProjectionRunner}, {@link ScheduledProjectionRunner}, and {@link
 * InlineProjectionInterceptor} from {@link ProjectionConfig @ProjectionConfig}-annotated {@link
 * Projection} beans.
 *
 * <p>Every registration carries the {@link ProjectionDeliveryMode} its {@code
 * ProjectionConfig.deliveryMode()} declares, and is routed on its own by {@link
 * ProjectionStoreRouting#route} — once, for both runners, into the {@link ProjectionRoutes} bean
 * the index declaration reads too — to the {@link CheckpointTarget} it reads its position from and
 * commits through: a {@link ReadModelStore}'s (one of the {@link ReadModelStores} — the
 * application's store beans, and the default {@code postgres} store unless one of them takes its
 * name — named by {@code ProjectionConfig.readModelStore()}, matched by the projection's {@code
 * writeTarget()}, or the one transactional store for a transactional registration with no write
 * target), or, for an {@code AT_LEAST_ONCE_IDEMPOTENT} registration that matches no store, the
 * event store's subscription checkpoint (the {@code streamRuneSubscriptionCheckpoint} bean). The
 * routed processor must carry what the registration needs — {@link
 * AtomicBatchProcessor#transactional()} for a transactional mode, {@link
 * AtomicBatchProcessor#fencesCheckpoint()} under real leadership — or the context does not start.
 * An INLINE registration must declare {@code AT_LEAST_ONCE_IDEMPOTENT}.
 *
 * <p>Disabled when {@code streamrune.projections.auto-discovery.enabled=false} (or any other
 * canonical-false spelling RelaxedBoolean accepts — see {@link AutoDiscoveryEnabledCondition}).
 */
@AutoConfiguration
@EnableConfigurationProperties(StreamRuneProperties.class)
@Conditional(ProjectionAutoConfig.AutoDiscoveryEnabledCondition.class)
@org.springframework.context.annotation.Import(ReadModelStoresConfiguration.class)
public class ProjectionAutoConfig {

  private static final Logger log = LoggerFactory.getLogger(ProjectionAutoConfig.class);

  /**
   * {@code @ConditionalOnProperty(..., havingValue = "true")} required an EXACT {@code "true"}
   * match, so a non-canonical truthy spelling ({@code on}, {@code yes}, {@code 1}) did not match
   * {@code havingValue} and therefore silently DISABLED this whole auto-config — the opposite of
   * the operator's intent, and inconsistent with Quarkus ({@code ProjectionProducer}, already
   * routed through {@code RelaxedBoolean}) and Micronaut, whose {@code @Requires(notEquals =
   * "false")} kept those same spellings ENABLED. Routes the property through the same {@code
   * RelaxedBoolean} parser every other StreamRune boolean knob uses, so all three runtimes agree on
   * every spelling. Absent (or blank) defaults to enabled, mirroring the old {@code matchIfMissing
   * = true}.
   */
  static final class AutoDiscoveryEnabledCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
      String value =
          context.getEnvironment().getProperty("streamrune.projections.auto-discovery.enabled");
      return org.streamrune.integration.RelaxedBoolean.parse(value, true);
    }
  }

  /**
   * Builds the framework's runner from the discovered projections (see the class javadoc).
   *
   * @hidden
   */
  @Bean
  @ConditionalOnMissingBean
  @org.streamrune.core.Internal
  public MultiProjectionRunner streamRuneMultiProjectionRunner(
      ApplicationContext ctx,
      Environment env,
      EventStore eventStore,
      StreamRuneProperties properties,
      ObjectProvider<ProjectionRoutes> routesProvider,
      ObjectProvider<ProjectionDeadLetterStore> dlqProvider,
      ObjectProvider<StreamRuneMetrics> metricsProvider,
      ObjectProvider<SubscriptionLeadership> leadershipProvider,
      ObjectProvider<ProjectionErrorClassifier> classifierProvider,
      ObjectProvider<org.streamrune.runtime.SubscriptionHealthContributor> healthProvider,
      ObjectProvider<StreamRuneTracing> tracingProvider) {
    var entries = discover(ctx, env, ProjectionConfig.Mode.CONTINUOUS);
    // No eager @Conditional that instantiates every Projection during the
    // BeanFactoryPostProcessor phase. Instead discover() runs during bean instantiation (all
    // BeanPostProcessors active) and we return null when there is no CONTINUOUS projection —
    // Spring registers no bean, mirroring the Quarkus null-product approach. The name-conditional
    // lifecycle below then finds no bean and does not start anything.
    if (entries.isEmpty()) {
      return null;
    }
    // streamrune.polling-interval / polling-jitter drive the catch-up polling cadence of each
    // projection's event subscription. listenNotifyEnabled=true only takes effect if a
    // HybridEventSubscription is wired via a subscription factory; the default
    // MultiProjectionRunner path builds a PollingEventSubscription, which has no LISTEN/NOTIFY
    // push, so the flag is dormant there.
    var subscriptionConfig =
        new SubscriptionConfig(true, properties.pollingInterval(), properties.pollingJitter());
    var builder =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .subscriptionConfig(subscriptionConfig)
            // Each projection's batches start projection/{name}/process.
            .tracing(tracingOf(tracingProvider));
    // Two leadership beans with none @Primary fail here, as on Quarkus and Micronaut.
    SubscriptionLeadership leadership = LeadershipBeans.resolve(leadershipProvider);
    // Propagate the metrics collector (e.g. the auto-configured MicrometerStreamRuneMetrics) so
    // projections.* meters are recorded; absent collector leaves projection metrics unchanged.
    metricsProvider.ifAvailable(builder::metrics);
    // Thread the single-active-consumer leadership bean (LeaseBasedLeadership when a
    // DataSource is present and the knob is on; absent otherwise) into every per-projection runner;
    // SubscriptionLeadership.NOOP (always leader) when no bean exists.
    builder.leadership(leadership);
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
    // Each registration is routed on its own: the runner sets no runner-wide offset store or
    // processor, so every registration reads and commits through the pair it was routed to.
    ProjectionRoutes routes = routesProvider.getObject();
    for (var e : entries) {
      builder.register(
          ProjectionName.of(e.name()),
          e.projection(),
          e.errorStrategy(),
          e.deliveryMode(),
          routes.target(e.name()));
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

  /**
   * Builds the framework's runner from the discovered projections (see the class javadoc).
   *
   * @hidden
   */
  @Bean
  @ConditionalOnMissingBean
  @org.streamrune.core.Internal
  public ScheduledProjectionRunner streamRuneScheduledProjectionRunner(
      ApplicationContext ctx,
      Environment env,
      EventStore eventStore,
      ObjectProvider<ProjectionRoutes> routesProvider,
      ObjectProvider<ProjectionDeadLetterStore> dlqProvider,
      ObjectProvider<StreamRuneMetrics> metricsProvider,
      ObjectProvider<SubscriptionLeadership> leadershipProvider,
      ObjectProvider<ProjectionErrorClassifier> classifierProvider,
      ObjectProvider<org.streamrune.runtime.SubscriptionHealthContributor> healthProvider,
      ObjectProvider<StreamRuneTracing> tracingProvider) {
    var entries = discover(ctx, env, ProjectionConfig.Mode.SCHEDULED);
    // Return null when there is no SCHEDULED projection (see
    // streamRuneMultiProjectionRunner).
    if (entries.isEmpty()) {
      return null;
    }
    var builder =
        ScheduledProjectionRunner.builder()
            .eventStore(eventStore)
            .tracing(tracingOf(tracingProvider));
    // The same routing as the CONTINUOUS runner: a TRANSACTIONAL_LOCAL scheduled chunk's read-model
    // write and its checkpoint commit in its store's one transaction, with the same split-brain
    // guard as CONTINUOUS.
    SubscriptionLeadership leadership = LeadershipBeans.resolve(leadershipProvider);
    // Propagate the metrics collector so projections.* meters are recorded for scheduled runs;
    // absent collector leaves projection metrics unchanged.
    metricsProvider.ifAvailable(builder::metrics);
    // Only a leader drains on each tick; NOOP (always leader) when no leadership bean exists.
    builder.leadership(leadership);
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
    }
    ProjectionRoutes routes = routesProvider.getObject();
    for (var e : entries) {
      builder.register(
          ProjectionName.of(e.name()),
          e.projection(),
          e.cron(),
          e.errorStrategy(),
          e.deliveryMode(),
          routes.target(e.name()));
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
   * Runs innermost in the command interceptor chain (see the {@code ORDER_*} constants of {@link
   * org.streamrune.runtime.CommandInterceptorOrdering}): {@code after()} executes in reverse order,
   * so inline projections are applied first once events are committed. Each inline projection is
   * decorated with a {@link TracingProjectionDecorator} over the {@link StreamRuneTracing} bean, so
   * its batch starts {@code projection/{name}/process} under the command's span.
   */
  @Bean
  @org.springframework.core.annotation.Order(
      org.streamrune.runtime.CommandInterceptorOrdering.ORDER_INLINE_PROJECTION)
  @ConditionalOnMissingBean
  public InlineProjectionInterceptor streamRuneInlineProjectionInterceptor(
      ApplicationContext ctx,
      Environment env,
      ObjectProvider<StreamRuneMetrics> metricsProvider,
      ObjectProvider<StreamRuneTracing> tracingProvider) {
    var entries = discover(ctx, env, ProjectionConfig.Mode.INLINE);
    // Return null when there is no INLINE projection (see streamRuneMultiProjectionRunner).
    if (entries.isEmpty()) {
      return null;
    }
    var builder = InlineProjectionInterceptor.builder();
    // The tracing meets the metrics bean here: its guard counts its failures there.
    StreamRuneTracing tracing =
        org.streamrune.runtime.GuardedTracing.of(
            tracingOf(tracingProvider),
            metricsProvider.getIfAvailable(() -> StreamRuneMetrics.NOOP));
    for (var e : entries) {
      // INLINE has no checkpoint to be transactional with: only AT_LEAST_ONCE_IDEMPOTENT boots.
      ProjectionStoreRouting.requireInlineMode(e.name(), e.deliveryMode());
      warnNoInlineRecoveryPath(e.name());
      log.info(ProjectionStoreRouting.INLINE_INFO, LogSanitizer.sanitizeForLog(e.name()));
      metricsProvider.ifAvailable(
          m -> m.recordProjectionDeliveryMode(ProjectionName.of(e.name()), e.deliveryMode()));
      ProjectionName name = ProjectionName.of(e.name());
      builder.register(name, TracingProjectionDecorator.traced(name, e.projection(), tracing));
    }
    return builder.build();
  }

  /** The {@link StreamRuneTracing} bean, or {@link StreamRuneTracing#NOOP} without one. */
  private static StreamRuneTracing tracingOf(ObjectProvider<StreamRuneTracing> tracingProvider) {
    return tracingProvider.getIfAvailable(() -> StreamRuneTracing.NOOP);
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
      if (pc == null) {
        continue;
      }

      String name = pc.name();
      if (!seen.add(name)) {
        throw new IllegalStateException(ProjectionDiscoveryMessages.duplicateName(name));
      }

      String prefix = "streamrune.projections." + name + ".";
      boolean enabled =
          org.streamrune.integration.RelaxedBoolean.parse(
              env.getProperty(prefix + "enabled"), true);
      if (!enabled) {
        continue;
      }

      ProjectionConfig.Mode effectiveMode = pc.mode();
      if (effectiveMode != mode) {
        continue;
      }

      if (mode != ProjectionConfig.Mode.INLINE) {
        requireNotACglibProxiedBaseProjection(entry.getKey(), bean);
      }

      ProjectionErrorStrategy strategy =
          parseStrategy(env.getProperty(prefix + "error-strategy"), pc.errorStrategy(), name);
      String cron = pc.cron();

      entries.add(
          new DiscoveredEntry(name, bean, cron, strategy, pc.deliveryMode(), pc.readModelStore()));
    }
    return entries;
  }

  /**
   * Refuses a {@link BaseProjection} bean that Spring AOP proxies with CGLIB: an aspect applies to
   * the class (a class-level {@code @Transactional}, {@code @Async}, {@code @Validated}, a metrics
   * or tracing aspect, a scoped proxy), so the bean the context holds is a generated subclass that
   * forwards each overridable method to the real projection. {@link BaseProjection#process(List,
   * org.streamrune.core.projection.ProjectionRepository)} is final, so the subclass cannot forward
   * it: a runner's call runs it on the proxy instance itself, whose fields were never initialised,
   * and the first batch fails with a {@link NullPointerException}. An {@code INLINE} projection is
   * not refused: the inline interceptor calls the one-argument {@code process}, which the proxy
   * forwards.
   *
   * <p>The proxy is refused, not unwrapped. Handing the runner the target behind it would run the
   * projection without the advice the application put on it, and nothing would say so.
   */
  private static void requireNotACglibProxiedBaseProjection(String beanName, Projection bean) {
    if (bean instanceof BaseProjection && AopUtils.isCglibProxy(bean)) {
      throw new IllegalStateException(
          "Projection bean '"
              + LogSanitizer.sanitizeForLog(beanName)
              + "' is a CGLIB proxy of BaseProjection subclass "
              + AopUtils.getTargetClass(bean).getName()
              + ": an aspect applies to the class. BaseProjection.process(List,"
              + " ProjectionRepository) is final, so the proxy cannot forward it, and the runner's"
              + " call would run it on the uninitialised proxy and fail on the first batch. Move"
              + " the advice off the projection class (into a bean the projection calls), or"
              + " implement Projection directly, overriding both process methods and"
              + " writesThroughRepository(), so that the proxy forwards every call.");
    }
  }

  private static ProjectionErrorStrategy parseStrategy(
      String prop, ProjectionErrorStrategy fallback, String name) {
    if (prop == null || prop.isBlank()) {
      return fallback;
    }
    try {
      return ProjectionErrorStrategy.valueOf(prop.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          ProjectionDiscoveryMessages.invalidErrorStrategy(prop, name), e);
    }
  }

  /**
   * Routes every registration of both runners — each enabled {@code CONTINUOUS} and {@code
   * SCHEDULED} projection — once, each on its own ({@link ProjectionStoreRouting#route}), over the
   * {@link ReadModelStores} (selected and validated once, so a duplicate store name or write target
   * fails as a store error before any registration is routed). The runners register each
   * registration on its route, and the index declaration declares each {@code ProjectionIndex} on
   * the store its projection is routed to. Lazy: created by the first runner, or by the index
   * declaration when index beans exist; with no registration it touches no store.
   *
   * @param ctx the context the projection beans are discovered in
   * @param env the per-projection property overrides
   * @param stores the read-model stores
   * @param subscriptionCheckpointProvider the event store's subscription checkpoint, rule 4's
   *     target
   * @param leadershipProvider the single-active-consumer leadership, {@code NOOP} when absent
   * @return the routes
   * @throws IllegalStateException if the store set is invalid, a registration cannot be routed or
   *     its routed processor lacks a capability it needs, or registrations exist and no
   *     subscription checkpoint bean does
   * @hidden
   */
  @Bean
  @org.springframework.context.annotation.Lazy
  @org.streamrune.core.Internal
  public ProjectionRoutes streamRuneProjectionRoutes(
      ApplicationContext ctx,
      Environment env,
      ReadModelStores stores,
      @Qualifier(StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT)
          ObjectProvider<CheckpointTarget> subscriptionCheckpointProvider,
      ObjectProvider<SubscriptionLeadership> leadershipProvider) {
    var registrations = new ArrayList<ProjectionStoreRouting.Registration>();
    for (var mode : List.of(ProjectionConfig.Mode.CONTINUOUS, ProjectionConfig.Mode.SCHEDULED)) {
      for (var e : discover(ctx, env, mode)) {
        registrations.add(
            new ProjectionStoreRouting.Registration(
                e.name(), e.deliveryMode(), e.readModelStore(), e.projection()));
      }
    }
    return ProjectionRoutes.route(
        registrations,
        stores,
        () -> requireSubscriptionCheckpoint(subscriptionCheckpointProvider),
        LeadershipBeans.resolve(leadershipProvider));
  }

  private static CheckpointTarget requireSubscriptionCheckpoint(
      ObjectProvider<CheckpointTarget> subscriptionCheckpointProvider) {
    CheckpointTarget subscriptionCheckpoint = subscriptionCheckpointProvider.getIfAvailable();
    if (subscriptionCheckpoint == null) {
      throw new IllegalStateException(
          "No "
              + StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT
              + " bean: the auto-configured projection runners route an at-least-once projection"
              + " that matches no read-model store to the event store's subscription checkpoint."
              + " Define a DataSource bean (the checkpoint is then subscription_checkpoint) or an"
              + " OffsetStore bean, or a CheckpointTarget bean named "
              + StreamRuneAutoConfiguration.SUBSCRIPTION_CHECKPOINT
              + ".");
    }
    return subscriptionCheckpoint;
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
      ProjectionDeliveryMode deliveryMode,
      String readModelStore) {}
}
