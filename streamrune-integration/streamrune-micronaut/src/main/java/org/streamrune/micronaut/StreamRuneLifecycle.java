package org.streamrune.micronaut;

import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
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

/**
 * Starts StreamRune background runners when the Micronaut context starts and closes them on
 * shutdown. Without this, {@link MultiProjectionRunner}, {@link ScheduledProjectionRunner}, {@link
 * OutboxPoller}, {@link OutboxRetentionSweeper}, {@link SagaDeadLetterRetentionSweeper}, {@link
 * DeadLetterRetentionSweeper}, and {@link DeadLetterRetryRunner} beans exist but never process
 * anything.
 *
 * <p>Disable with {@code streamrune.runner-lifecycle-enabled=false} to manage runner lifecycle
 * manually. When disabled, this bean is not created, so it neither starts nor closes the runners;
 * each background bean instead carries a {@code @Bean(preDestroy = "close")} in its factory so
 * Micronaut still releases it (notably the {@code SubscriptionLeadership} renew heartbeat and held
 * leases) on context shutdown — Micronaut does not auto-close {@code @Factory}-produced {@link
 * AutoCloseable} beans without it.
 */
@Singleton
@Requires(property = "streamrune.runner-lifecycle-enabled", notEquals = "false")
public class StreamRuneLifecycle
    implements ApplicationEventListener<StartupEvent>, AutoCloseable, Ordered {

  private static final Logger logger = LoggerFactory.getLogger(StreamRuneLifecycle.class);

  private final MultiProjectionRunner multiProjectionRunner;
  private final ScheduledProjectionRunner scheduledProjectionRunner;
  private final OutboxPoller outboxPoller;
  private final OutboxRetentionSweeper outboxRetentionSweeper;
  private final InboxRetentionSweeper inboxRetentionSweeper;
  private final SagaDeadLetterRetentionSweeper sagaDeadLetterRetentionSweeper;
  private final DeadLetterRetentionSweeper deadLetterRetentionSweeper;
  private final DeadLetterRetryRunner deadLetterRetryRunner;
  private final org.streamrune.core.subscription.SubscriptionLeadership leadership;

  /**
   * Creates the lifecycle manager. All runners are optional — only the ones present in the context
   * are managed.
   *
   * @param multiProjectionRunner continuous projection runner, if any
   * @param scheduledProjectionRunner scheduled projection runner, if any
   * @param outboxPoller outbox poller, if any
   * @param outboxRetentionSweeper outbox retention sweeper, if any
   * @param inboxRetentionSweeper command inbox retention sweeper, if any
   * @param sagaDeadLetterRetentionSweeper saga dead-letter retention sweeper, if any
   * @param deadLetterRetentionSweeper command dead-letter-queue retention sweeper, if any
   * @param deadLetterRetryRunner dead-letter retry runner, if any
   * @param leadership single-active-consumer leadership coordinator, if any; closed after the
   *     runners so a runner never loses leadership mid-batch
   */
  public StreamRuneLifecycle(
      // Inject the framework-assembled runners BY NAME so this lifecycle starts/stops only
      // the runner the framework built from @ProjectionConfig beans — a user-supplied (unqualified)
      // MultiProjectionRunner bean is never injected here and is therefore never started/stopped by
      // the framework (the user owns it), matching Spring's named-bean ownership contract.
      @Nullable @jakarta.inject.Named(ProjectionFactory.FRAMEWORK_MULTI_RUNNER)
          MultiProjectionRunner multiProjectionRunner,
      @Nullable @jakarta.inject.Named(ProjectionFactory.FRAMEWORK_SCHEDULED_RUNNER)
          ScheduledProjectionRunner scheduledProjectionRunner,
      // Inject the remaining framework-assembled runners BY NAME too, so a user-supplied
      // OutboxPoller / DeadLetterRetryRunner / sweeper bean (which outranks the @Secondary
      // framework bean) is never started twice (OutboxPoller.start() deliberately throws on a
      // second start). Extends the named-bean ownership contract to every runner type.
      @Nullable @jakarta.inject.Named(StreamRuneMicronautModule.FRAMEWORK_OUTBOX_POLLER)
          OutboxPoller outboxPoller,
      @Nullable @jakarta.inject.Named(StreamRuneMicronautModule.FRAMEWORK_OUTBOX_RETENTION_SWEEPER)
          OutboxRetentionSweeper outboxRetentionSweeper,
      @Nullable @jakarta.inject.Named(StreamRuneMicronautModule.FRAMEWORK_INBOX_RETENTION_SWEEPER)
          InboxRetentionSweeper inboxRetentionSweeper,
      @Nullable
          @jakarta.inject.Named(
              StreamRuneMicronautModule.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER)
          SagaDeadLetterRetentionSweeper sagaDeadLetterRetentionSweeper,
      @Nullable
          @jakarta.inject.Named(StreamRuneMicronautModule.FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER)
          DeadLetterRetentionSweeper deadLetterRetentionSweeper,
      @Nullable @jakarta.inject.Named(StreamRuneMicronautModule.FRAMEWORK_DEAD_LETTER_RETRY_RUNNER)
          DeadLetterRetryRunner deadLetterRetryRunner,
      @Nullable org.streamrune.core.subscription.SubscriptionLeadership leadership) {
    this.multiProjectionRunner = multiProjectionRunner;
    this.scheduledProjectionRunner = scheduledProjectionRunner;
    this.outboxPoller = outboxPoller;
    this.outboxRetentionSweeper = outboxRetentionSweeper;
    this.inboxRetentionSweeper = inboxRetentionSweeper;
    this.sagaDeadLetterRetentionSweeper = sagaDeadLetterRetentionSweeper;
    this.deadLetterRetentionSweeper = deadLetterRetentionSweeper;
    this.deadLetterRetryRunner = deadLetterRetryRunner;
    this.leadership = leadership;
  }

  // Strictly greater than the validators' order (1000) so Micronaut always notifies
  // StreamRuneConfigValidator, StreamRuneAuthorizationValidator and SagaStateCryptoValidator FIRST
  // (Ordered: LOWER value runs first) — without an explicit order their relative sequence was
  // unspecified, so the runners started below (whose first poll, ResilientPollLoop, is immediate)
  // could persist data before a fail-closed validator ever ran. Mirrors Spring, where
  // SmartInitializingSingleton (the validators) always completes before any SmartLifecycle (the
  // runners) starts.
  @Override
  public int getOrder() {
    return 1100;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    if (multiProjectionRunner != null) {
      multiProjectionRunner.start();
      logger.info("Started MultiProjectionRunner");
    }
    if (scheduledProjectionRunner != null) {
      scheduledProjectionRunner.start();
      logger.info("Started ScheduledProjectionRunner");
    }
    if (outboxPoller != null) {
      outboxPoller.start();
      logger.info("Started OutboxPoller");
    }
    if (outboxRetentionSweeper != null) {
      outboxRetentionSweeper.start();
      logger.info("Started OutboxRetentionSweeper");
    }
    if (inboxRetentionSweeper != null) {
      inboxRetentionSweeper.start();
      logger.info("Started InboxRetentionSweeper");
    }
    if (sagaDeadLetterRetentionSweeper != null) {
      sagaDeadLetterRetentionSweeper.start();
      logger.info("Started SagaDeadLetterRetentionSweeper");
    }
    if (deadLetterRetentionSweeper != null) {
      deadLetterRetentionSweeper.start();
      logger.info("Started DeadLetterRetentionSweeper");
    }
    if (deadLetterRetryRunner != null) {
      deadLetterRetryRunner.start();
      logger.info("Started DeadLetterRetryRunner");
    }
  }

  /** Closes all managed runners in reverse start order. */
  @PreDestroy
  @Override
  public void close() {
    closeQuietly(deadLetterRetryRunner, "DeadLetterRetryRunner");
    closeQuietly(deadLetterRetentionSweeper, "DeadLetterRetentionSweeper");
    closeQuietly(sagaDeadLetterRetentionSweeper, "SagaDeadLetterRetentionSweeper");
    closeQuietly(inboxRetentionSweeper, "InboxRetentionSweeper");
    closeQuietly(outboxRetentionSweeper, "OutboxRetentionSweeper");
    closeQuietly(outboxPoller, "OutboxPoller");
    closeQuietly(scheduledProjectionRunner, "ScheduledProjectionRunner");
    closeQuietly(multiProjectionRunner, "MultiProjectionRunner");
    // Release advisory locks + the dedicated connection AFTER every runner has stopped (so a runner
    // never loses leadership mid-batch) and before the DataSource closes.
    closeQuietly(leadership, "SubscriptionLeadership");
  }

  private void closeQuietly(AutoCloseable runner, String name) {
    if (runner == null) {
      return;
    }
    try {
      runner.close();
      logger.info("Closed {}", name);
    } catch (Exception e) {
      logger.warn("Failed to close {}", name, e);
    }
  }
}
