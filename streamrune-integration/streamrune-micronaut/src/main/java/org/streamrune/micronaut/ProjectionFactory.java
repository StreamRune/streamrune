package org.streamrune.micronaut;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import io.micronaut.context.condition.Condition;
import io.micronaut.context.condition.ConditionContext;
import io.micronaut.context.env.Environment;
import io.micronaut.core.annotation.Nullable;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.streamrune.runtime.SubscriptionHealthContributor;

/**
 * Micronaut factory that discovers {@link ProjectionConfig @ProjectionConfig}-annotated {@link
 * Projection} beans and produces {@link MultiProjectionRunner}, {@link ScheduledProjectionRunner},
 * and {@link InlineProjectionInterceptor}.
 *
 * <p>Per-projection properties at {@code streamrune.projections.<name>.*} override annotation
 * values. A projection can be disabled via {@code streamrune.projections.<name>.enabled=false}.
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
 * canonical-false spelling — see {@link AutoDiscoveryEnabled}).
 */
@Factory
@Requires(condition = ProjectionFactory.AutoDiscoveryEnabled.class)
public class ProjectionFactory {

  private static final Logger log = LoggerFactory.getLogger(ProjectionFactory.class);

  /**
   * Bean name of the framework-assembled continuous runner. {@link StreamRuneLifecycle} injects the
   * runner by THIS name so it starts/stops only the runner the framework assembled from
   * {@code @ProjectionConfig} beans — never a user-supplied {@code MultiProjectionRunner} bean,
   * which the user owns (matching Spring's named-bean ownership contract).
   *
   * <p>Named ownership is only half of Spring's contract: it decides who <em>starts</em> a given
   * instance, not whether the framework's instance is <em>defined</em> at all. Spring pairs it with
   * {@code @ConditionalOnMissingBean} and Quarkus with {@code @DefaultBean}, so a user runner
   * replaces the framework's. Micronaut needs {@code @Requires(missingBeans = ...)} for that — see
   * {@link #multiProjectionRunner}. With both in place the name resolves to the framework's runner
   * when it exists and to nothing when the user replaced it, and the {@code @Nullable} lifecycle
   * parameter simply stays null.
   */
  static final String FRAMEWORK_MULTI_RUNNER = "streamRuneMultiProjectionRunner";

  /**
   * Bean name of the framework-assembled scheduled runner (see {@link #FRAMEWORK_MULTI_RUNNER}).
   */
  static final String FRAMEWORK_SCHEDULED_RUNNER = "streamRuneScheduledProjectionRunner";

  /** Creates the projection factory. */
  public ProjectionFactory() {}

  /**
   * Produces a {@link MultiProjectionRunner} from all enabled CONTINUOUS projections. The bean is
   * only created when at least one enabled CONTINUOUS projection exists (see {@link
   * HasContinuousProjections}) — Micronaut factory methods must not return {@code null}.
   *
   * <p>The runner is given the {@link AtomicBatchProcessor} bean (e.g. the default {@code
   * JdbcProjectionRepository}) when one of its registrations declares {@code TRANSACTIONAL_LOCAL}
   * or {@code EXTERNAL_EFFECT}, or when the leadership needs its epoch honoured — those
   * projections' writes and the offset save then commit in one transaction; otherwise it runs on
   * {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()}.
   *
   * @param projections all {@link Projection} beans in the context
   * @param eventStore the event store
   * @param offsetStore the offset store
   * @param dlqs optional dead-letter stores
   * @param atomicProcessors every {@link AtomicBatchProcessor} bean in the context (none, one, or
   *     more — more than one is refused when the runner needs one)
   * @param metrics optional metrics collector
   * @param leadership optional single-active-consumer leadership
   * @param classifier optional poison-vs-transient classifier
   * @param healthContributor optional subscription health contributor
   * @param properties the StreamRune configuration properties
   * @param env the Micronaut environment for property access
   * @return a new runner
   */
  @Singleton
  @Secondary
  @Named(FRAMEWORK_MULTI_RUNNER)
  @Requires(beans = {EventStore.class, OffsetStore.class})
  @Requires(condition = HasContinuousProjections.class)
  // Back off entirely when the application defines its own
  // MultiProjectionRunner — the same self-excluding pattern the StreamRuneMicronautModule
  // background factories use, and the type-level back-off Spring
  // (@ConditionalOnMissingBean(MultiProjectionRunner.class) on streamRuneMultiProjectionRunner) and
  // Quarkus (@DefaultBean on multiProjectionRunner) have always had. @Secondary + @Named alone was
  // NOT parity: @Secondary only demotes in ambiguous single-injection-point selection and never
  // removes the definition, and the named-bean ownership contract below governs only who STARTS a
  // given instance — it says nothing about two DIFFERENT instances covering the same projections.
  // The user's documented path for a custom subscription (wiring a HybridEventSubscription for
  // LISTEN/NOTIFY — see the comment below, since the default path only builds a
  // PollingEventSubscription) is to define their own runner over the same @ProjectionConfig beans.
  // The framework then still assembled its runner from ALL of them and the lifecycle started it by
  // name, so both processed projection "orders" concurrently IN ONE PROCESS. The single
  // SubscriptionLeadership bean cannot fence them apart (one LeaseBasedLeadership = one holder id,
  // so both are simultaneously "leader" for the same subscription name): with a non-atomic
  // processor every committed event applies TWICE to the read model, silently; with the atomic
  // processor the two subscriptions race the offset CAS every batch, producing persistent
  // offset-commit conflicts misread as lost leadership. Same damage class as
  // two pollers on one table and double-applied inline
  // projections). @Named is KEPT: StreamRuneLifecycle's @Nullable @Named parameter resolves to null
  // when this definition backs off (a user bean carries a different name), so the user still owns
  // their runner's lifecycle exactly as the named-bean ownership contract intends.
  @Requires(missingBeans = MultiProjectionRunner.class)
  @Bean(preDestroy = "close")
  public MultiProjectionRunner multiProjectionRunner(
      List<Projection> projections,
      EventStore eventStore,
      OffsetStore offsetStore,
      List<ProjectionDeadLetterStore> dlqs,
      List<AtomicBatchProcessor> atomicProcessors,
      @Nullable StreamRuneMetrics metrics,
      @Nullable SubscriptionLeadership leadership,
      @Nullable ProjectionErrorClassifier classifier,
      @Nullable SubscriptionHealthContributor healthContributor,
      StreamRuneMicronautProperties properties,
      Environment env) {

    var entries = discover(projections, env, ProjectionConfig.Mode.CONTINUOUS);
    if (entries.isEmpty()) {
      throw new IllegalStateException("No enabled CONTINUOUS projections found");
    }

    // streamrune.polling-interval-ms / polling-jitter-ms drive the catch-up polling cadence of each
    // projection's event subscription. listenNotifyEnabled=true only takes effect if a
    // HybridEventSubscription is wired via a subscription factory; the default
    // MultiProjectionRunner path builds a PollingEventSubscription, which has no LISTEN/NOTIFY
    // push, so the flag is dormant there. Same wiring as the Spring and Quarkus integrations.
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

    // Micronaut collection injection exposes EVERY AtomicBatchProcessor bean. The processor is
    // chosen by what the registrations DECLARE and by whether the leadership needs its epoch
    // honoured — never by the presence of a DataSource or of a bean: the single bean when a
    // registration declares TRANSACTIONAL_LOCAL / EXTERNAL_EFFECT or the leadership must fence (no
    // bean, or more than one, is refused then), and nonAtomicAtLeastOnce() otherwise.
    builder.atomicProcessor(
        ProjectionProcessorResolution.select(
            registrationsOf(entries), leadership, atomicProcessors, FRAMEWORK_MULTI_RUNNER));

    // Propagate the metrics collector (e.g. the auto-configured MicrometerStreamRuneMetrics) so
    // projections.* meters are recorded; absent collector leaves projection metrics unchanged.
    if (metrics != null) {
      builder.metrics(metrics);
    }

    // Thread the single-active-consumer leadership bean (LeaseBasedLeadership when a
    // DataSource is present and the knob is on) into every per-projection runner; NOOP (always
    // leader) when absent.
    if (leadership != null) {
      builder.leadership(leadership);
    }

    // Optional user-supplied poison-vs-transient classifier; DEFAULT when absent.
    if (classifier != null) {
      builder.classifier(classifier);
    }

    // Wire the subscription health contributor so each continuous projection registers its live
    // subscription and feeds delivery outcomes; absent contributor leaves health tracking a no-op.
    if (healthContributor != null) {
      builder.healthContributor(healthContributor);
    }

    ProjectionDeadLetterStore dlq = resolveSingleDlq(dlqs);
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
   * Produces a {@link ScheduledProjectionRunner} from all enabled SCHEDULED projections. The bean
   * is only created when at least one enabled SCHEDULED projection exists (see {@link
   * HasScheduledProjections}).
   *
   * <p>The processor is chosen by the same rule as {@link #multiProjectionRunner}: the bean for a
   * {@code TRANSACTIONAL_LOCAL} / {@code EXTERNAL_EFFECT} registration or a leadership that must
   * fence, {@link AtomicBatchProcessor#nonAtomicAtLeastOnce()} otherwise.
   *
   * @param projections all {@link Projection} beans in the context
   * @param eventStore the event store
   * @param offsetStore the offset store
   * @param dlqs optional dead-letter stores
   * @param atomicProcessors every {@link AtomicBatchProcessor} bean in the context
   * @param metrics optional metrics collector
   * @param leadership optional single-active-consumer leadership
   * @param classifier optional poison-vs-transient classifier
   * @param healthContributor optional subscription health contributor
   * @param env the Micronaut environment for property access
   * @return a new runner
   */
  @Singleton
  @Secondary
  @Named(FRAMEWORK_SCHEDULED_RUNNER)
  @Requires(beans = {EventStore.class, OffsetStore.class})
  @Requires(condition = HasScheduledProjections.class)
  // See {@link #multiProjectionRunner} — a user-supplied runner
  // must REPLACE this default, not run beside it against the same @ProjectionConfig projections.
  @Requires(missingBeans = ScheduledProjectionRunner.class)
  @Bean(preDestroy = "close")
  public ScheduledProjectionRunner scheduledProjectionRunner(
      List<Projection> projections,
      EventStore eventStore,
      OffsetStore offsetStore,
      List<ProjectionDeadLetterStore> dlqs,
      List<AtomicBatchProcessor> atomicProcessors,
      @Nullable StreamRuneMetrics metrics,
      @Nullable SubscriptionLeadership leadership,
      @Nullable ProjectionErrorClassifier classifier,
      @Nullable SubscriptionHealthContributor healthContributor,
      Environment env) {

    var entries = discover(projections, env, ProjectionConfig.Mode.SCHEDULED);
    if (entries.isEmpty()) {
      throw new IllegalStateException("No enabled SCHEDULED projections found");
    }

    var builder =
        ScheduledProjectionRunner.builder().eventStore(eventStore).offsetStore(offsetStore);

    // The same rule as the CONTINUOUS runner: the processor by declared need, never by bean
    // presence. A TRANSACTIONAL_LOCAL scheduled chunk's read-model write and its checkpoint commit
    // in the processor's one transaction, with the same split-brain guard as CONTINUOUS.
    builder.atomicProcessor(
        ProjectionProcessorResolution.select(
            registrationsOf(entries), leadership, atomicProcessors, FRAMEWORK_SCHEDULED_RUNNER));

    // Propagate the metrics collector so projections.* meters are recorded for scheduled runs;
    // absent collector leaves projection metrics unchanged.
    if (metrics != null) {
      builder.metrics(metrics);
    }

    // Only a leader drains on each tick; NOOP (always leader) when no leadership bean exists.
    if (leadership != null) {
      builder.leadership(leadership);
    }

    // Optional user-supplied poison-vs-transient classifier; DEFAULT when absent.
    if (classifier != null) {
      builder.classifier(classifier);
    }

    // Wire the subscription health contributor so each scheduled projection registers itself and
    // feeds per-chunk outcomes; absent contributor leaves health tracking a no-op.
    if (healthContributor != null) {
      builder.healthContributor(healthContributor);
    }

    ProjectionDeadLetterStore dlq = resolveSingleDlq(dlqs);
    if (dlq != null) {
      builder.deadLetterStore(dlq);
    } else {
      requireNoDlqStrategy(entries);
    }

    for (var e : entries) {
      String cron = e.cron();
      if (cron == null || cron.isBlank()) {
        throw new IllegalStateException(ProjectionDiscoveryMessages.cronRequired(e.name()));
      }
      builder.register(e.name(), e.projection(), cron, e.errorStrategy(), e.deliveryMode());
    }

    return builder.build();
  }

  /**
   * Produces an {@link InlineProjectionInterceptor} from all enabled INLINE projections. The bean
   * is only created when at least one enabled INLINE projection exists (see {@link
   * HasInlineProjections}). Every INLINE projection must declare {@code AT_LEAST_ONCE_IDEMPOTENT};
   * each one is logged once at INFO and recorded on the {@code delivery_mode} gauge when a metrics
   * bean exists.
   *
   * @param projections all {@link Projection} beans in the context
   * @param env the Micronaut environment for property access
   * @param metrics optional metrics collector
   * @return a new interceptor
   */
  @Singleton
  @Secondary
  // Back off entirely when the application defines its own
  // InlineProjectionInterceptor. @Secondary only demotes single-candidate resolution; the command
  // bus injects List<CommandInterceptor>, and collection injection INCLUDES @Secondary beans — so
  // without missingBeans a user override JOINED the framework default in the chain and both after()
  // hooks ran, applying every inline projection to each committed event TWICE (double-counted read
  // models, duplicate rows) with no error or log. CommandInterceptorOrdering.sorted() only sorts;
  // it does not dedupe by type. Matches Spring's @ConditionalOnMissingBean and Quarkus's
  // @DefaultBean back-off semantics for the same bean.
  @Requires(missingBeans = InlineProjectionInterceptor.class)
  @Requires(condition = HasInlineProjections.class)
  public InlineProjectionInterceptor inlineProjectionInterceptor(
      List<Projection> projections, Environment env, @Nullable StreamRuneMetrics metrics) {

    var entries = discover(projections, env, ProjectionConfig.Mode.INLINE);
    if (entries.isEmpty()) {
      throw new IllegalStateException("No enabled INLINE projections found");
    }

    var builder = InlineProjectionInterceptor.builder();
    for (var e : entries) {
      // INLINE has no checkpoint to be transactional with: only AT_LEAST_ONCE_IDEMPOTENT boots.
      ProjectionProcessorResolution.requireInlineMode(e.name(), e.deliveryMode());
      warnNoInlineRecoveryPath(e.name());
      log.info(ProjectionProcessorResolution.INLINE_INFO, LogSanitizer.sanitizeForLog(e.name()));
      if (metrics != null) {
        metrics.recordProjectionDeliveryMode(ProjectionName.of(e.name()), e.deliveryMode());
      }
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
   * {@link ProjectionConfig.Mode}), out of this factory's reach.
   *
   * <p>Until then, surface the risk at boot instead of leaving it documented only in {@code
   * InlineProjectionInterceptor}'s javadoc — a WARN (not a hard failure: every current INLINE
   * deployment would trip it, and some inline projections are legitimately cheap/idempotent enough
   * that an operator has already accepted this risk) so it shows up where operators actually look.
   */
  private static void warnNoInlineRecoveryPath(String projectionName) {
    log.warn(INLINE_NO_RECOVERY_WARNING_TEMPLATE, LogSanitizer.sanitizeForLog(projectionName));
  }

  /**
   * Package-private (not a string literal inline in {@link #warnNoInlineRecoveryPath}) so a test
   * can reconstruct the exact formatted message via {@code
   * org.slf4j.helpers.MessageFormatter.format(...)} without depending on a real logging backend —
   * when this was written this module's test environment bound SLF4J to a NOP logger, so no
   * appender/handler-based capture (Logback or JUL, contrast the Spring and Quarkus siblings of
   * this same fix) could observe anything logged here; a real Micronaut deployment always ships a
   * real logging backend, so that was a test-environment limitation, not a production gap.
   */
  static final String INLINE_NO_RECOVERY_WARNING_TEMPLATE =
      "Projection '{}' is registered in INLINE mode with no automatic recovery path: if its"
          + " process() throws, the triggering command still reports SUCCESS and that command's"
          + " read-model update is silently and permanently lost (see"
          + " InlineProjectionInterceptor's javadoc). Consider whether this projection's failure"
          + " mode is acceptable, or register a companion CONTINUOUS/SCHEDULED projection under a"
          + " different name over the same read model as a recovery path.";

  // === conditions ===

  /**
   * Replaces {@code @Requires(property = ..., notEquals = "false")}, which matched literally — so a
   * non-canonical FALSE spelling ({@code off}, {@code no}, {@code 0}) was "not equal to false" and
   * left this whole factory ENABLED, the opposite of the operator's intent and inconsistent with
   * Quarkus's {@code ProjectionProducer} (already on {@link
   * org.streamrune.integration.RelaxedBoolean}) and this same class's OWN per-projection {@code
   * enabled} flag (see {@link ProjectionModeCondition}, below, and {@code discover()}), which
   * already used it. Absent (or blank) defaults to enabled, matching the old {@code notEquals}
   * behavior for a missing property.
   */
  public static final class AutoDiscoveryEnabled implements Condition {
    @Override
    public boolean matches(ConditionContext context) {
      if (!(context.getBeanContext() instanceof ApplicationContext applicationContext)) {
        return true;
      }
      return org.streamrune.integration.RelaxedBoolean.parse(
          applicationContext
              .getProperty("streamrune.projections.auto-discovery.enabled", String.class)
              .orElse(null),
          true);
    }
  }

  /** Matches when at least one enabled CONTINUOUS projection bean exists. */
  public static final class HasContinuousProjections extends ProjectionModeCondition {
    /** Creates the condition. */
    public HasContinuousProjections() {
      // Instantiated by Micronaut for @Requires(condition = ...); the mode is all it carries.
    }

    @Override
    ProjectionConfig.Mode mode() {
      return ProjectionConfig.Mode.CONTINUOUS;
    }
  }

  /** Matches when at least one enabled SCHEDULED projection bean exists. */
  public static final class HasScheduledProjections extends ProjectionModeCondition {
    /** Creates the condition. */
    public HasScheduledProjections() {
      // Instantiated by Micronaut for @Requires(condition = ...); the mode is all it carries.
    }

    @Override
    ProjectionConfig.Mode mode() {
      return ProjectionConfig.Mode.SCHEDULED;
    }
  }

  /** Matches when at least one enabled INLINE projection bean exists. */
  public static final class HasInlineProjections extends ProjectionModeCondition {
    /** Creates the condition. */
    public HasInlineProjections() {
      // Instantiated by Micronaut for @Requires(condition = ...); the mode is all it carries.
    }

    @Override
    ProjectionConfig.Mode mode() {
      return ProjectionConfig.Mode.INLINE;
    }
  }

  /**
   * Condition that matches when at least one enabled {@link ProjectionConfig}-annotated {@link
   * Projection} bean of the given mode exists. Uses the same discovery rules as the factory
   * methods, so condition and bean creation cannot disagree.
   *
   * <p>The mode is a method, not a field: Micronaut instantiates a condition into the bean
   * definition's compile-time annotation metadata, which a GraalVM native image keeps in its image
   * heap, and a {@link ProjectionConfig.Mode} held there failed the native build ("An object of
   * type 'org.streamrune.core.ProjectionConfig$Mode' was found in the image heap"; the enum is
   * initialized at run time). {@code ArchitectureTest} keeps every condition free of instance
   * state.
   */
  abstract static class ProjectionModeCondition implements Condition {

    /** The projection mode this condition looks for; read when {@link #matches} runs. */
    abstract ProjectionConfig.Mode mode();

    @Override
    public boolean matches(ConditionContext context) {
      if (!(context.getBeanContext() instanceof ApplicationContext applicationContext)) {
        return false;
      }
      ProjectionConfig.Mode mode = mode();
      for (Projection projection : applicationContext.getBeansOfType(Projection.class)) {
        ProjectionConfig pc = findProjectionConfig(projection.getClass());
        if (pc == null || pc.mode() != mode) {
          continue;
        }
        boolean enabled =
            org.streamrune.integration.RelaxedBoolean.parse(
                applicationContext
                    .getProperty("streamrune.projections." + pc.name() + ".enabled", String.class)
                    .orElse(null),
                true);
        if (enabled) {
          return true;
        }
      }
      return false;
    }
  }

  // === discovery + property override ===

  private static List<DiscoveredEntry> discover(
      List<Projection> projections, Environment env, ProjectionConfig.Mode mode) {

    var seen = new HashSet<String>();
    var entries = new ArrayList<DiscoveredEntry>();

    for (Projection bean : projections) {
      ProjectionConfig pc = findProjectionConfig(bean.getClass());
      if (pc == null) {
        continue;
      }

      String name = pc.name();

      if (!seen.add(name)) {
        throw new IllegalStateException(ProjectionDiscoveryMessages.duplicateName(name));
      }

      boolean enabled =
          org.streamrune.integration.RelaxedBoolean.parse(
              env.getProperty("streamrune.projections." + name + ".enabled", String.class)
                  .orElse(null),
              true);
      if (!enabled) {
        continue;
      }

      if (pc.mode() != mode) {
        continue;
      }

      ProjectionErrorStrategy strategy =
          parseStrategy(
              env.getProperty("streamrune.projections." + name + ".error-strategy", String.class)
                  .orElse(null),
              pc.errorStrategy(),
              name);

      entries.add(new DiscoveredEntry(name, bean, pc.cron(), strategy, pc.deliveryMode()));
    }

    return entries;
  }

  /**
   * Walks the class hierarchy to find {@link ProjectionConfig}, supporting Micronaut-proxied beans
   * where the annotation may be on a superclass.
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
    if (prop == null || prop.isBlank()) return fallback;
    try {
      return ProjectionErrorStrategy.valueOf(prop.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          ProjectionDiscoveryMessages.invalidErrorStrategy(prop, name), e);
    }
  }

  /**
   * Resolves the single {@link ProjectionDeadLetterStore}, failing loudly on ambiguity. Micronaut
   * collection injection ({@code List<ProjectionDeadLetterStore>}) silently exposes ALL beans, so
   * the prior {@code dlqs.get(0)} would route dead-lettered events to an arbitrary store when
   * several are present. Spring ({@code getIfAvailable} → {@code NoUniqueBeanDefinitionException})
   * and Quarkus ({@code Instance.get()} → ambiguity) both fail loudly here; this restores that
   * parity (mirrors the CryptoEngine ambiguity guard).
   *
   * @return the single store, or {@code null} when none is present
   */
  private static ProjectionDeadLetterStore resolveSingleDlq(List<ProjectionDeadLetterStore> dlqs) {
    if (dlqs.isEmpty()) {
      return null;
    }
    if (dlqs.size() > 1) {
      throw new IllegalStateException(
          "Multiple ProjectionDeadLetterStore beans are present ("
              + dlqs.size()
              + ") but StreamRune cannot decide which to use for projection dead-lettering —"
              + " silently picking the first would route dead-lettered events to an arbitrary"
              + " store. Ensure exactly one ProjectionDeadLetterStore bean is resolvable (remove"
              + " duplicates or qualify one with @Primary).");
    }
    return dlqs.get(0);
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
