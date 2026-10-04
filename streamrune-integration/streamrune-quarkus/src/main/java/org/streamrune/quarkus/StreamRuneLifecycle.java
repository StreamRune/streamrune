package org.streamrune.quarkus;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.Bean;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.inject.Named;
import jakarta.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.runtime.DeadLetterRetentionSweeper;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.InboxRetentionSweeper;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.runtime.OutboxRetentionSweeper;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.runtime.ScheduledProjectionRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Explicit lifecycle for StreamRune background components in Quarkus.
 *
 * <p>On {@link StartupEvent}, materializes and starts every runner whose bean exists: {@link
 * MultiProjectionRunner}, {@link ScheduledProjectionRunner}, {@link OutboxPoller}, and {@link
 * DeadLetterRetryRunner}. On {@link ShutdownEvent}, stops them in reverse order and closes the
 * {@link VirtualThreadCommandBus}.
 *
 * <p>The runner producers are dependent-scoped optional beans ({@link ProjectionProducer}, {@link
 * StreamRuneProducers}): when a feature is disabled or has no registrations, {@link Instance#get()}
 * yields {@code null} and that component is simply skipped. This bean is the single place that
 * materializes and owns those runner instances — do not inject the runner types elsewhere, or a
 * second (unstarted) instance is created.
 */
@ApplicationScoped
public class StreamRuneLifecycle {

  private static final Logger logger = LoggerFactory.getLogger(StreamRuneLifecycle.class);

  private final Instance<MultiProjectionRunner> multiProjectionRunnerInstance;
  private final Instance<ScheduledProjectionRunner> scheduledProjectionRunnerInstance;
  private final Instance<OutboxPoller> outboxPollerInstance;
  private final Instance<OutboxRetentionSweeper> outboxRetentionSweeperInstance;
  private final Instance<InboxRetentionSweeper> inboxRetentionSweeperInstance;
  private final Instance<SagaDeadLetterRetentionSweeper> sagaDeadLetterRetentionSweeperInstance;
  private final Instance<DeadLetterRetentionSweeper> deadLetterRetentionSweeperInstance;
  private final Instance<DeadLetterRetryRunner> deadLetterRetryRunnerInstance;
  private final Instance<org.streamrune.core.subscription.SubscriptionLeadership>
      leadershipInstance;
  private final Instance<VirtualThreadCommandBus> commandBusInstance;
  private final BeanManager beanManager;

  private MultiProjectionRunner multiProjectionRunner;
  private ScheduledProjectionRunner scheduledProjectionRunner;
  private OutboxPoller outboxPoller;
  private OutboxRetentionSweeper outboxRetentionSweeper;
  private InboxRetentionSweeper inboxRetentionSweeper;
  private SagaDeadLetterRetentionSweeper sagaDeadLetterRetentionSweeper;
  private DeadLetterRetentionSweeper deadLetterRetentionSweeper;
  private DeadLetterRetryRunner deadLetterRetryRunner;
  private org.streamrune.core.subscription.SubscriptionLeadership leadership;

  @jakarta.inject.Inject
  public StreamRuneLifecycle(
      // Inject the framework-assembled runners BY NAME so this lifecycle starts/stops only
      // the runner the framework built from @ProjectionConfig beans — a user-supplied (unqualified)
      // MultiProjectionRunner bean is never injected here and is therefore never started/stopped by
      // the framework (the user owns it), matching Spring's named-bean ownership contract. And,
      // as on Spring (@ConditionalOnMissingBean) and Micronaut (@Requires(missingBeans)), the
      // framework runner is not even built then (see below): two runners over the same
      // projections would apply every event twice.
      @jakarta.inject.Named(ProjectionProducer.FRAMEWORK_MULTI_RUNNER)
          Instance<MultiProjectionRunner> multiProjectionRunnerInstance,
      @jakarta.inject.Named(ProjectionProducer.FRAMEWORK_SCHEDULED_RUNNER)
          Instance<ScheduledProjectionRunner> scheduledProjectionRunnerInstance,
      // Inject the remaining framework-assembled runners BY NAME too, so a user-supplied
      // (unqualified) OutboxPoller / DeadLetterRetryRunner / sweeper bean, which the user owns and
      // starts, is never started by the framework. The name alone is not enough on Arc: a
      // @DefaultBean is suppressed only where it would be AMBIGUOUS with another bean, and the
      // user's bean does not carry the framework name, so this injection point still resolves the
      // framework producer. onStart therefore asks the BeanManager first (applicationOwns) and
      // neither creates nor starts the framework runner of a type the application supplies.
      @jakarta.inject.Named(StreamRuneProducers.FRAMEWORK_OUTBOX_POLLER)
          Instance<OutboxPoller> outboxPollerInstance,
      @jakarta.inject.Named(StreamRuneProducers.FRAMEWORK_OUTBOX_RETENTION_SWEEPER)
          Instance<OutboxRetentionSweeper> outboxRetentionSweeperInstance,
      @jakarta.inject.Named(StreamRuneProducers.FRAMEWORK_INBOX_RETENTION_SWEEPER)
          Instance<InboxRetentionSweeper> inboxRetentionSweeperInstance,
      @jakarta.inject.Named(StreamRuneProducers.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER)
          Instance<SagaDeadLetterRetentionSweeper> sagaDeadLetterRetentionSweeperInstance,
      @jakarta.inject.Named(StreamRuneProducers.FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER)
          Instance<DeadLetterRetentionSweeper> deadLetterRetentionSweeperInstance,
      @jakarta.inject.Named(StreamRuneProducers.FRAMEWORK_DEAD_LETTER_RETRY_RUNNER)
          Instance<DeadLetterRetryRunner> deadLetterRetryRunnerInstance,
      Instance<org.streamrune.core.subscription.SubscriptionLeadership> leadershipInstance,
      Instance<VirtualThreadCommandBus> commandBusInstance,
      BeanManager beanManager) {
    this.multiProjectionRunnerInstance = multiProjectionRunnerInstance;
    this.scheduledProjectionRunnerInstance = scheduledProjectionRunnerInstance;
    this.outboxPollerInstance = outboxPollerInstance;
    this.outboxRetentionSweeperInstance = outboxRetentionSweeperInstance;
    this.inboxRetentionSweeperInstance = inboxRetentionSweeperInstance;
    this.sagaDeadLetterRetentionSweeperInstance = sagaDeadLetterRetentionSweeperInstance;
    this.deadLetterRetentionSweeperInstance = deadLetterRetentionSweeperInstance;
    this.deadLetterRetryRunnerInstance = deadLetterRetryRunnerInstance;
    this.leadershipInstance = leadershipInstance;
    this.commandBusInstance = commandBusInstance;
    this.beanManager = beanManager;
  }

  /**
   * Client-proxy constructor — see {@link StreamRuneConfigValidator#StreamRuneConfigValidator()}
   * for the rationale. Never used for a real instance.
   */
  protected StreamRuneLifecycle() {
    this.multiProjectionRunnerInstance = null;
    this.scheduledProjectionRunnerInstance = null;
    this.outboxPollerInstance = null;
    this.outboxRetentionSweeperInstance = null;
    this.inboxRetentionSweeperInstance = null;
    this.sagaDeadLetterRetentionSweeperInstance = null;
    this.deadLetterRetentionSweeperInstance = null;
    this.deadLetterRetryRunnerInstance = null;
    this.leadershipInstance = null;
    this.commandBusInstance = null;
    this.beanManager = null;
  }

  // Strictly greater than the validators' Interceptor.Priority.LIBRARY_BEFORE so
  // CDI/Arc always notifies StreamRuneConfigValidator, StreamRuneAuthorizationValidator and
  // SagaStateCryptoValidator FIRST — without an explicit @Priority their relative order was
  // unspecified, so the runners started below (whose first poll, ResilientPollLoop, is immediate)
  // could persist data before a fail-closed validator ever ran. Mirrors Spring, where
  // SmartInitializingSingleton (the validators) always completes before any SmartLifecycle (the
  // runners) starts.
  //
  // @Priority must sit on the EVENT PARAMETER, not the method — see
  // StreamRuneConfigValidator#validate for why. `public` for the same cross-classloader reason as
  // the constructor above.
  public void onStart(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE + 100) StartupEvent event) {
    multiProjectionRunner =
        frameworkRunnerOrNull(
            multiProjectionRunnerInstance,
            MultiProjectionRunner.class,
            ProjectionProducer.FRAMEWORK_MULTI_RUNNER);
    if (multiProjectionRunner != null) {
      multiProjectionRunner.start();
      logger.info("StreamRune: started MultiProjectionRunner");
    }

    scheduledProjectionRunner =
        frameworkRunnerOrNull(
            scheduledProjectionRunnerInstance,
            ScheduledProjectionRunner.class,
            ProjectionProducer.FRAMEWORK_SCHEDULED_RUNNER);
    if (scheduledProjectionRunner != null) {
      scheduledProjectionRunner.start();
      logger.info("StreamRune: started ScheduledProjectionRunner");
    }

    outboxPoller =
        frameworkRunnerOrNull(
            outboxPollerInstance, OutboxPoller.class, StreamRuneProducers.FRAMEWORK_OUTBOX_POLLER);
    if (outboxPoller != null) {
      outboxPoller.start();
      logger.info("StreamRune: started OutboxPoller");
    }

    outboxRetentionSweeper =
        frameworkRunnerOrNull(
            outboxRetentionSweeperInstance,
            OutboxRetentionSweeper.class,
            StreamRuneProducers.FRAMEWORK_OUTBOX_RETENTION_SWEEPER);
    if (outboxRetentionSweeper != null) {
      outboxRetentionSweeper.start();
      logger.info("StreamRune: started OutboxRetentionSweeper");
    }

    inboxRetentionSweeper =
        frameworkRunnerOrNull(
            inboxRetentionSweeperInstance,
            InboxRetentionSweeper.class,
            StreamRuneProducers.FRAMEWORK_INBOX_RETENTION_SWEEPER);
    if (inboxRetentionSweeper != null) {
      inboxRetentionSweeper.start();
      logger.info("StreamRune: started InboxRetentionSweeper");
    }

    sagaDeadLetterRetentionSweeper =
        frameworkRunnerOrNull(
            sagaDeadLetterRetentionSweeperInstance,
            SagaDeadLetterRetentionSweeper.class,
            StreamRuneProducers.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER);
    if (sagaDeadLetterRetentionSweeper != null) {
      sagaDeadLetterRetentionSweeper.start();
      logger.info("StreamRune: started SagaDeadLetterRetentionSweeper");
    }

    deadLetterRetentionSweeper =
        frameworkRunnerOrNull(
            deadLetterRetentionSweeperInstance,
            DeadLetterRetentionSweeper.class,
            StreamRuneProducers.FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER);
    if (deadLetterRetentionSweeper != null) {
      deadLetterRetentionSweeper.start();
      logger.info("StreamRune: started DeadLetterRetentionSweeper");
    }

    deadLetterRetryRunner =
        frameworkRunnerOrNull(
            deadLetterRetryRunnerInstance,
            DeadLetterRetryRunner.class,
            StreamRuneProducers.FRAMEWORK_DEAD_LETTER_RETRY_RUNNER);
    if (deadLetterRetryRunner != null) {
      deadLetterRetryRunner.start();
      logger.info("StreamRune: started DeadLetterRetryRunner");
    }

    // Materialize the leadership bean (if produced) so this lifecycle owns the single instance and
    // can release its advisory locks + dedicated connection on shutdown, after the runners stop.
    // The runners resolve the same singleton and acquire leadership lazily; nothing to start here.
    leadership = resolveOrNull(leadershipInstance);
  }

  void onStop(@Observes ShutdownEvent event) {
    closeQuietly(deadLetterRetryRunner, "DeadLetterRetryRunner");
    deadLetterRetryRunner = null;
    closeQuietly(deadLetterRetentionSweeper, "DeadLetterRetentionSweeper");
    deadLetterRetentionSweeper = null;
    closeQuietly(sagaDeadLetterRetentionSweeper, "SagaDeadLetterRetentionSweeper");
    sagaDeadLetterRetentionSweeper = null;
    closeQuietly(inboxRetentionSweeper, "InboxRetentionSweeper");
    inboxRetentionSweeper = null;
    closeQuietly(outboxRetentionSweeper, "OutboxRetentionSweeper");
    outboxRetentionSweeper = null;
    closeQuietly(outboxPoller, "OutboxPoller");
    outboxPoller = null;
    closeQuietly(scheduledProjectionRunner, "ScheduledProjectionRunner");
    scheduledProjectionRunner = null;
    closeQuietly(multiProjectionRunner, "MultiProjectionRunner");
    multiProjectionRunner = null;

    // Release advisory locks + the dedicated connection AFTER every runner has stopped, so a runner
    // never loses leadership mid-batch, and BEFORE the DataSource closes.
    closeQuietly(leadership, "SubscriptionLeadership");
    leadership = null;

    if (commandBusInstance.isResolvable()) {
      closeQuietly(commandBusInstance.get(), "VirtualThreadCommandBus");
    }
  }

  /** Returns the started {@link MultiProjectionRunner}, or {@code null} when none is running. */
  public MultiProjectionRunner multiProjectionRunner() {
    return multiProjectionRunner;
  }

  /** Returns the started {@link ScheduledProjectionRunner}, or {@code null} when none. */
  public ScheduledProjectionRunner scheduledProjectionRunner() {
    return scheduledProjectionRunner;
  }

  /** Returns the started {@link OutboxPoller}, or {@code null} when none is running. */
  public OutboxPoller outboxPoller() {
    return outboxPoller;
  }

  /** Returns the started {@link OutboxRetentionSweeper}, or {@code null} when none is running. */
  public OutboxRetentionSweeper outboxRetentionSweeper() {
    return outboxRetentionSweeper;
  }

  /** Returns the started {@link InboxRetentionSweeper}, or {@code null} when none is running. */
  public InboxRetentionSweeper inboxRetentionSweeper() {
    return inboxRetentionSweeper;
  }

  /**
   * Returns the started {@link SagaDeadLetterRetentionSweeper}, or {@code null} when none is
   * running.
   */
  public SagaDeadLetterRetentionSweeper sagaDeadLetterRetentionSweeper() {
    return sagaDeadLetterRetentionSweeper;
  }

  /**
   * Returns the started {@link DeadLetterRetentionSweeper}, or {@code null} when none is running.
   */
  public DeadLetterRetentionSweeper deadLetterRetentionSweeper() {
    return deadLetterRetentionSweeper;
  }

  /** Returns the started {@link DeadLetterRetryRunner}, or {@code null} when none is running. */
  public DeadLetterRetryRunner deadLetterRetryRunner() {
    return deadLetterRetryRunner;
  }

  private static <T> T resolveOrNull(Instance<T> instance) {
    return instance.isUnsatisfied() ? null : instance.get();
  }

  /**
   * The framework runner of {@code type}, or {@code null} when the application supplies its own
   * bean of that type (which the application starts and stops) or the framework produced none.
   * Checked before the named instance is resolved, so the framework producer never runs then.
   */
  private <T> T frameworkRunnerOrNull(
      Instance<T> frameworkInstance, Class<T> type, String frameworkBeanName) {
    if (applicationOwns(type, frameworkBeanName)) {
      logger.info(
          "StreamRune: the application supplies its own {} bean, so the framework neither creates"
              + " nor starts one; the application starts and stops its own",
          type.getSimpleName());
      return null;
    }
    return resolveOrNull(frameworkInstance);
  }

  /** Whether a bean of {@code type} other than the framework's named one exists. */
  private boolean applicationOwns(Class<?> type, String frameworkBeanName) {
    for (Bean<?> bean : beanManager.getBeans(type, Any.Literal.INSTANCE)) {
      boolean framework =
          bean.getQualifiers().stream()
              .anyMatch(q -> q instanceof Named named && frameworkBeanName.equals(named.value()));
      if (!framework) {
        return true;
      }
    }
    return false;
  }

  private static void closeQuietly(AutoCloseable closeable, String name) {
    if (closeable == null) {
      return;
    }
    try {
      closeable.close();
      logger.info("StreamRune: stopped {}", name);
    } catch (Exception e) {
      logger.warn("StreamRune: failed to stop {}", name, e);
    }
  }
}
