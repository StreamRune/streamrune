package org.streamrune.quarkus;

import io.quarkus.arc.DefaultBean;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;
import org.eclipse.microprofile.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventStore;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
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
import org.streamrune.integration.ProjectionStoreRouting;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.ScheduledProjectionRunner;
import org.streamrune.runtime.SubscriptionHealthContributor;
import org.streamrune.runtime.TracingProjectionDecorator;

/**
 * Quarkus CDI producer that discovers {@link ProjectionConfig @ProjectionConfig}-annotated {@link
 * Projection} beans and produces {@link MultiProjectionRunner}, {@link ScheduledProjectionRunner},
 * and {@link InlineProjectionInterceptor}.
 *
 * <p>Per-projection properties at {@code streamrune.projections.<name>.*} override annotation
 * values. A projection can be disabled via {@code streamrune.projections.<name>.enabled=false}.
 *
 * <p>Every registration carries the {@link ProjectionDeliveryMode} its {@code
 * ProjectionConfig.deliveryMode()} declares, and is routed on its own by {@link
 * ProjectionStoreRouting#route} to the {@link CheckpointTarget} it reads its position from and
 * commits through: a {@link ReadModelStore} bean's (named by {@code
 * ProjectionConfig.readModelStore()}, matched by the projection's {@code writeTarget()}, or the one
 * transactional store for a transactional registration with no write target), or, for an {@code
 * AT_LEAST_ONCE_IDEMPOTENT} registration that matches no store, the event store's subscription
 * checkpoint (the {@code CheckpointTarget} bean named {@code streamRuneSubscriptionCheckpoint}).
 * The routed processor must carry what the registration needs — {@link
 * AtomicBatchProcessor#transactional()} for a transactional mode, {@link
 * AtomicBatchProcessor#fencesCheckpoint()} under real leadership — or the runner is not produced
 * and the start fails. The runners get no runner-wide offset store or processor: every registration
 * reads and commits through the pair it was routed to. An INLINE registration must declare {@code
 * AT_LEAST_ONCE_IDEMPOTENT}.
 *
 * <p>All three producers are dependent-scoped optional beans: they return {@code null} when no
 * projection of the relevant mode is registered (CDI permits a null product only for
 * {@code @Dependent} beans). The runners are materialized once, started, and stopped by {@link
 * StreamRuneLifecycle}.
 */
@ApplicationScoped
public class ProjectionProducer {

  private static final Logger log = LoggerFactory.getLogger(ProjectionProducer.class);

  /**
   * Bean name of the framework-assembled continuous runner. {@link StreamRuneLifecycle} injects the
   * runner by THIS name so it starts/stops only the runner the framework assembled from
   * {@code @ProjectionConfig} beans — never a user-supplied {@code MultiProjectionRunner} bean,
   * which the user owns (matching Spring's {@code @ConditionalOnBean(name = ...)} contract).
   */
  static final String FRAMEWORK_MULTI_RUNNER = "streamRuneMultiProjectionRunner";

  /**
   * Bean name of the framework-assembled scheduled runner (see {@link #FRAMEWORK_MULTI_RUNNER}).
   */
  static final String FRAMEWORK_SCHEDULED_RUNNER = "streamRuneScheduledProjectionRunner";

  private static boolean autoDiscoveryEnabled(Config config) {
    return org.streamrune.integration.RelaxedBoolean.parse(
        config
            .getOptionalValue("streamrune.projections.auto-discovery.enabled", String.class)
            .orElse(null),
        true);
  }

  /**
   * Produces a {@link MultiProjectionRunner} from all enabled CONTINUOUS projections. Returns
   * {@code null} when no CONTINUOUS projection is found. Each registration is routed to its {@link
   * CheckpointTarget} (see the class javadoc): a {@code TRANSACTIONAL_LOCAL} / {@code
   * EXTERNAL_EFFECT} registration's writes and its checkpoint commit in its store's one
   * transaction.
   *
   * @throws IllegalStateException if the store set is invalid or a registration cannot be routed
   *     ({@link ProjectionStoreRouting#route})
   */
  @Produces
  @DefaultBean
  @Named(FRAMEWORK_MULTI_RUNNER)
  public MultiProjectionRunner multiProjectionRunner(
      Instance<Projection> projections,
      EventStore eventStore,
      @Any Instance<ReadModelStore> storeInstance,
      @Named(StreamRuneProducers.SUBSCRIPTION_CHECKPOINT) CheckpointTarget subscriptionCheckpoint,
      Instance<ProjectionDeadLetterStore> dlqInstance,
      Instance<StreamRuneMetrics> metricsInstance,
      Instance<SubscriptionLeadership> leadershipInstance,
      Instance<ProjectionErrorClassifier> classifierInstance,
      Instance<SubscriptionHealthContributor> healthInstance,
      StreamRuneQuarkusProperties properties,
      Config config,
      StreamRuneTracing tracing) {

    // streamrune.projections.auto-discovery.enabled=false is the framework kill switch
    // (parity with Spring/Micronaut) — assemble nothing, so the user owns runner lifecycle.
    if (!autoDiscoveryEnabled(config)) {
      return null;
    }

    var entries = discover(projections, config, ProjectionConfig.Mode.CONTINUOUS);
    if (entries.isEmpty()) {
      return null;
    }

    // streamrune.polling-interval / polling-jitter drive the catch-up polling cadence of each
    // projection's event subscription. listenNotifyEnabled=true only takes effect if a
    // HybridEventSubscription is wired via a subscription factory; the default
    // MultiProjectionRunner path builds a PollingEventSubscription, which has no LISTEN/NOTIFY
    // push, so the flag is dormant there. Same wiring as the Spring and Micronaut integrations.
    var subscriptionConfig =
        new SubscriptionConfig(true, properties.pollingInterval(), properties.pollingJitter());

    var builder =
        MultiProjectionRunner.builder()
            .eventStore(eventStore)
            .subscriptionConfig(subscriptionConfig)
            // Each projection's batches start projection/{name}/process.
            .tracing(tracing);

    ProjectionDeadLetterStore dlq = resolveOrNull(dlqInstance);
    if (dlq != null) {
      builder.deadLetterStore(dlq);
    } else {
      requireNoDlqStrategy(entries);
    }

    SubscriptionLeadership leadership = LeadershipInstances.resolve(leadershipInstance);

    // Propagate the metrics collector (e.g. the auto-produced MicrometerStreamRuneMetrics) so
    // projections.* meters are recorded; absent collector leaves projection metrics unchanged.
    StreamRuneMetrics metrics = resolveOrNull(metricsInstance);
    if (metrics != null) {
      builder.metrics(metrics);
    }

    // Thread the single-active-consumer leadership bean (LeaseBasedLeadership when a
    // DataSource is present and the knob is on) into every per-projection runner; NOOP (always
    // leader) when absent.
    builder.leadership(leadership);

    // Optional user-supplied poison-vs-transient classifier; DEFAULT when absent.
    ProjectionErrorClassifier classifier = resolveOrNull(classifierInstance);
    if (classifier != null) {
      builder.classifier(classifier);
    }

    // Wire the subscription health contributor so each continuous projection registers its live
    // subscription and feeds delivery outcomes; absent contributor leaves health tracking a no-op.
    SubscriptionHealthContributor health = resolveOrNull(healthInstance);
    if (health != null) {
      builder.healthContributor(health);
    }

    // Each registration is routed on its own: the runner sets no runner-wide offset store or
    // processor, so every registration reads and commits through the pair it was routed to.
    var routes = route(entries, storeInstance, subscriptionCheckpoint, leadership);
    for (var e : entries) {
      builder.register(
          ProjectionName.of(e.name()),
          e.projection(),
          e.errorStrategy(),
          e.deliveryMode(),
          routes.get(e.name()));
    }

    return builder.build();
  }

  /**
   * Produces a {@link ScheduledProjectionRunner} from all enabled SCHEDULED projections. Returns
   * {@code null} when no SCHEDULED projection is found. Each registration is routed as the
   * continuous runner's are.
   *
   * @throws IllegalStateException if a projection has no cron, the store set is invalid, or a
   *     registration cannot be routed ({@link ProjectionStoreRouting#route})
   */
  @Produces
  @DefaultBean
  @Named(FRAMEWORK_SCHEDULED_RUNNER)
  public ScheduledProjectionRunner scheduledProjectionRunner(
      Instance<Projection> projections,
      EventStore eventStore,
      @Any Instance<ReadModelStore> storeInstance,
      @Named(StreamRuneProducers.SUBSCRIPTION_CHECKPOINT) CheckpointTarget subscriptionCheckpoint,
      Instance<ProjectionDeadLetterStore> dlqInstance,
      Instance<StreamRuneMetrics> metricsInstance,
      Instance<SubscriptionLeadership> leadershipInstance,
      Instance<ProjectionErrorClassifier> classifierInstance,
      Instance<SubscriptionHealthContributor> healthInstance,
      Config config,
      StreamRuneTracing tracing) {

    // Auto-discovery kill switch (parity with Spring/Micronaut) — see multiProjectionRunner.
    if (!autoDiscoveryEnabled(config)) {
      return null;
    }

    var entries = discover(projections, config, ProjectionConfig.Mode.SCHEDULED);
    if (entries.isEmpty()) {
      return null;
    }

    var builder = ScheduledProjectionRunner.builder().eventStore(eventStore).tracing(tracing);

    ProjectionDeadLetterStore dlq = resolveOrNull(dlqInstance);
    if (dlq != null) {
      builder.deadLetterStore(dlq);
    } else {
      requireNoDlqStrategy(entries);
    }

    SubscriptionLeadership leadership = LeadershipInstances.resolve(leadershipInstance);

    // Propagate the metrics collector so projections.* meters are recorded for scheduled runs;
    // absent collector leaves projection metrics unchanged.
    StreamRuneMetrics metrics = resolveOrNull(metricsInstance);
    if (metrics != null) {
      builder.metrics(metrics);
    }

    // Only a leader drains on each tick; NOOP (always leader) when no leadership bean exists.
    builder.leadership(leadership);

    // Optional user-supplied poison-vs-transient classifier; DEFAULT when absent.
    ProjectionErrorClassifier classifier = resolveOrNull(classifierInstance);
    if (classifier != null) {
      builder.classifier(classifier);
    }

    // Wire the subscription health contributor so each scheduled projection registers itself and
    // feeds per-chunk outcomes; absent contributor leaves health tracking a no-op.
    SubscriptionHealthContributor health = resolveOrNull(healthInstance);
    if (health != null) {
      builder.healthContributor(health);
    }

    for (var e : entries) {
      String cron = e.cron();
      if (cron == null || cron.isBlank()) {
        throw new IllegalStateException(ProjectionDiscoveryMessages.cronRequired(e.name()));
      }
    }
    // The same routing as the CONTINUOUS runner: a TRANSACTIONAL_LOCAL scheduled chunk's read-model
    // write and its checkpoint commit in its store's one transaction, with the same split-brain
    // guard as CONTINUOUS.
    var routes = route(entries, storeInstance, subscriptionCheckpoint, leadership);
    for (var e : entries) {
      builder.register(
          ProjectionName.of(e.name()),
          e.projection(),
          e.cron(),
          e.errorStrategy(),
          e.deliveryMode(),
          routes.get(e.name()));
    }

    return builder.build();
  }

  /**
   * Produces an {@link InlineProjectionInterceptor} from all enabled INLINE projections. Returns
   * {@code null} when no INLINE projection is found. Every INLINE projection must declare {@code
   * AT_LEAST_ONCE_IDEMPOTENT}; each one is logged once at INFO and recorded on the {@code
   * delivery_mode} gauge when a metrics bean exists. Each one is decorated with a {@link
   * TracingProjectionDecorator} over the {@link StreamRuneTracing} bean, so its batch starts {@code
   * projection/{name}/process} under the command's span.
   */
  @Produces
  @DefaultBean
  public InlineProjectionInterceptor inlineProjectionInterceptor(
      Instance<Projection> projections,
      Config config,
      Instance<StreamRuneMetrics> metricsInstance,
      StreamRuneTracing tracing) {

    // Honor the auto-discovery kill switch for INLINE too. Without this,
    // INLINE @ProjectionConfig projections keep running (the interceptor is picked up by the
    // command bus) when the operator set auto-discovery.enabled=false — corrupting a read model
    // during a rebuild/migration. Spring gates the whole @Configuration and Micronaut the whole
    // @Factory, so this restores 3-framework parity (CDI permits a null product for this @Dependent
    // producer).
    if (!autoDiscoveryEnabled(config)) {
      return null;
    }

    var entries = discover(projections, config, ProjectionConfig.Mode.INLINE);
    if (entries.isEmpty()) {
      return null;
    }

    StreamRuneMetrics metrics = resolveOrNull(metricsInstance);
    var builder = InlineProjectionInterceptor.builder();
    // The tracing meets the metrics bean here: one guard for the inline projections, which
    // counts its failures there.
    StreamRuneTracing inlineTracing = org.streamrune.runtime.GuardedTracing.of(tracing, metrics);
    for (var e : entries) {
      // INLINE has no checkpoint to be transactional with: only AT_LEAST_ONCE_IDEMPOTENT boots.
      ProjectionStoreRouting.requireInlineMode(e.name(), e.deliveryMode());
      warnNoInlineRecoveryPath(e.name());
      log.info(ProjectionStoreRouting.INLINE_INFO, LogSanitizer.sanitizeForLog(e.name()));
      if (metrics != null) {
        metrics.recordProjectionDeliveryMode(ProjectionName.of(e.name()), e.deliveryMode());
      }
      ProjectionName name = ProjectionName.of(e.name());
      builder.register(
          name, TracingProjectionDecorator.traced(name, e.projection(), inlineTracing));
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
   * {@link ProjectionConfig.Mode}), out of this producer's reach.
   *
   * <p>Until then, surface the risk at boot instead of leaving it documented only in {@code
   * InlineProjectionInterceptor}'s javadoc — a WARN (not a hard failure: every current INLINE
   * deployment would trip it, and some inline projections are legitimately cheap/idempotent enough
   * that an operator has already accepted this risk) so it shows up where operators actually look.
   */
  private static void warnNoInlineRecoveryPath(String projectionName) {
    log.warn(
        "Projection '{}' is registered in INLINE mode with no automatic recovery path: if its"
            + " process() throws, the triggering command still reports SUCCESS and that command's"
            + " read-model update is silently and permanently lost (see"
            + " InlineProjectionInterceptor's javadoc). Consider whether this projection's failure"
            + " mode is acceptable, or register a companion CONTINUOUS/SCHEDULED projection under"
            + " a different name over the same read model as a recovery path.",
        LogSanitizer.sanitizeForLog(projectionName));
  }

  /**
   * Resolves an optional bean: returns {@code null} when no bean exists or when the producer
   * yielded a null product (dependent-scoped optional beans in this module).
   */
  private static <T> T resolveOrNull(Instance<T> instance) {
    return instance.isUnsatisfied() ? null : instance.get();
  }

  /**
   * Every {@link ReadModelStore} bean, whatever its qualifiers, in Arc's priority order. A {@code
   * null} product (a dependent-scoped producer that declined) is no store. The default store is
   * among them only when the application defines none: Arc drops a {@code @DefaultBean} from a
   * resolution that holds another bean of its type.
   */
  private static List<ReadModelStore> storesOf(Instance<ReadModelStore> instance) {
    return instance.isUnsatisfied()
        ? List.of()
        : instance.stream().filter(Objects::nonNull).toList();
  }

  /**
   * Routes one runner's registrations, each on its own ({@link ProjectionStoreRouting#route}),
   * after checking the store set once ({@link ProjectionStoreRouting#validateStores}).
   *
   * @return each registration's target, by projection name
   * @throws IllegalStateException if the store set is invalid, or a registration cannot be routed
   *     or its routed processor lacks a capability it needs
   */
  private static Map<String, CheckpointTarget> route(
      List<DiscoveredEntry> entries,
      Instance<ReadModelStore> storeInstance,
      CheckpointTarget subscriptionCheckpoint,
      SubscriptionLeadership leadership) {
    List<ReadModelStore> stores = storesOf(storeInstance);
    ProjectionStoreRouting.validateStores(stores);
    var routes = new LinkedHashMap<String, CheckpointTarget>();
    for (var e : entries) {
      var registration =
          new ProjectionStoreRouting.Registration(
              e.name(), e.deliveryMode(), e.readModelStore(), e.projection());
      routes.put(
          e.name(),
          ProjectionStoreRouting.route(registration, stores, subscriptionCheckpoint, leadership));
    }
    return routes;
  }

  // === discovery + property override ===

  private static List<DiscoveredEntry> discover(
      Instance<Projection> projections, Config config, ProjectionConfig.Mode mode) {

    var seen = new HashSet<String>();
    var entries = new ArrayList<DiscoveredEntry>();

    StreamSupport.stream(projections.spliterator(), false)
        .forEach(
            bean -> {
              ProjectionConfig pc = findProjectionConfig(bean.getClass());
              if (pc == null) {
                return;
              }

              String name = pc.name();

              // Check for duplicate names across ALL registrations BEFORE filtering by mode,
              // so a cross-mode duplicate (e.g. a CONTINUOUS and a SCHEDULED projection both named
              // "orders") fails loudly at boot instead of booting silently and sharing one
              // subscription_checkpoint checkpoint row — where whichever runner commits later
              // advances
              // the offset past events the other never processed. Matches the Spring/Micronaut
              // order.
              if (!seen.add(name)) {
                throw new IllegalStateException(ProjectionDiscoveryMessages.duplicateName(name));
              }

              if (pc.mode() != mode) {
                return;
              }

              String prefix = "streamrune.projections." + name + ".";
              boolean enabled =
                  org.streamrune.integration.RelaxedBoolean.parse(
                      config.getOptionalValue(prefix + "enabled", String.class).orElse(null), true);
              if (!enabled) {
                return;
              }

              ProjectionErrorStrategy strategy =
                  parseStrategy(
                      config.getOptionalValue(prefix + "error-strategy", String.class).orElse(null),
                      pc.errorStrategy(),
                      name);

              entries.add(
                  new DiscoveredEntry(
                      name, bean, pc.cron(), strategy, pc.deliveryMode(), pc.readModelStore()));
            });

    return entries;
  }

  /**
   * Walks the class hierarchy to find {@link ProjectionConfig}, supporting CDI-proxied beans where
   * the annotation may be on a superclass.
   */
  private static ProjectionConfig findProjectionConfig(Class<?> clazz) {
    while (clazz != null && clazz != Object.class) {
      ProjectionConfig pc = clazz.getAnnotation(ProjectionConfig.class);
      if (pc != null) {
        return pc;
      }
      clazz = clazz.getSuperclass();
    }
    return null;
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
