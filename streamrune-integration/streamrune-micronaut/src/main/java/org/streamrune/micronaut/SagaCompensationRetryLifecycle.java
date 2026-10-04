package org.streamrune.micronaut;

import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.integration.SagaRetentionValidator;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SagaCompensationRetrySweeper;
import org.streamrune.runtime.SagaRunner;

/**
 * Auto-wires a {@link SagaCompensationRetrySweeper} for every application {@link SagaRunner} bean
 * in Micronaut and owns their lifecycle: builds and starts one sweeper per runner on {@link
 * StartupEvent}, stops them all on context shutdown.
 *
 * <p>This is the durable, automatic recovery path for a stuck {@code COMPENSATING} saga that has no
 * {@link org.streamrune.runtime.SagaTimeoutRunner} (empty {@code timeout()}) and receives no
 * further correlated event: the sweeper periodically re-drives such sagas' compensation under the
 * shared episode-scoped idempotency key until it durably completes (or the give-up horizon
 * terminalizes it {@code FAULTED}). Saga runners are application-wired; this bean only collects
 * them as {@code List<SagaRunner<?>>}. Each sweeper is leadership-gated via the shared {@link
 * SubscriptionLeadership} bean (single active per saga type); with none it defaults to
 * always-leader (single instance).
 *
 * <p>Skipped entirely only when sagas are disabled ({@code streamrune.saga.enabled=false}). {@code
 * streamrune.saga.compensation-retry-enabled=false} no longer withholds this bean — the sweepers
 * are the sole emitters of {@code streamrune.saga.compensating} / {@code
 * streamrune.saga.faulted_rows}, so the knob is threaded into each sweeper as sampling-only mode
 * instead: the re-drive stops, the gauges stay live.
 */
@Singleton
@Requires(property = "streamrune.saga.enabled", value = "true", defaultValue = "true")
public class SagaCompensationRetryLifecycle
    implements ApplicationEventListener<StartupEvent>, AutoCloseable, Ordered {

  private static final Logger logger =
      LoggerFactory.getLogger(SagaCompensationRetryLifecycle.class);

  private final List<SagaRunner<?>> sagaRunners;
  private final SagaStore sagaStore;
  private final SubscriptionLeadership leadership;
  private final StreamRuneMetrics metrics;
  private final BackgroundRelayHealthContributor relayHealth;
  private final Duration retryInterval;
  private final Duration giveUpAfter;
  private final Duration inboxRetentionMaxAge;
  private final Duration sagaDeadLetterRetentionMaxAge;
  private final boolean compensationRetryEnabled;
  private final List<SagaCompensationRetrySweeper<?>> sweepers = new ArrayList<>();

  public SagaCompensationRetryLifecycle(
      List<SagaRunner<?>> sagaRunners,
      @Nullable SagaStore sagaStore,
      @Nullable SubscriptionLeadership leadership,
      @Nullable StreamRuneMetrics metrics,
      @Nullable BackgroundRelayHealthContributor relayHealth,
      @Property(name = "streamrune.saga.compensation-retry-interval", defaultValue = "60s")
          Duration retryInterval,
      @Property(name = "streamrune.saga.compensation-retry-give-up-after", defaultValue = "1h")
          Duration giveUpAfter,
      @Property(name = "streamrune.inbox.retention-max-age", defaultValue = "7d")
          Duration inboxRetentionMaxAge,
      @Property(name = "streamrune.saga.dead-letter-retention-max-age", defaultValue = "7d")
          Duration sagaDeadLetterRetentionMaxAge,
      @Property(name = "streamrune.saga.compensation-retry-enabled", defaultValue = "true")
          boolean compensationRetryEnabled) {
    this.sagaRunners = sagaRunners;
    this.sagaStore = sagaStore;
    this.leadership = leadership;
    this.metrics = metrics != null ? metrics : StreamRuneMetrics.NOOP;
    this.relayHealth = relayHealth;
    this.retryInterval = retryInterval;
    this.giveUpAfter = giveUpAfter;
    this.inboxRetentionMaxAge = inboxRetentionMaxAge;
    this.sagaDeadLetterRetentionMaxAge = sagaDeadLetterRetentionMaxAge;
    this.compensationRetryEnabled = compensationRetryEnabled;
  }

  // This is a second runner-starting startup listener left out of the startup-validator
  // ordering fix — its first sweep is immediate (ResilientPollLoop) and can re-drive COMPENSATING
  // sagas
  // before SagaStateCryptoValidator / StreamRuneAuthorizationValidator ever run. Same order
  // (1100) as StreamRuneLifecycle: both are runner starters; neither must run before any
  // validator (order 1000), and their relative order to EACH OTHER does not matter.
  @Override
  public int getOrder() {
    return 1100;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    startSweepers();
  }

  /** Package-private for tests: build and start one sweeper per discovered saga runner. */
  void startSweepers() {
    if (sagaRunners == null || sagaRunners.isEmpty() || sagaStore == null) {
      return;
    }
    // The compensation give-up horizon must be strictly below the inbox retention
    // window, or a stuck COMPENSATING episode can outlive its succeeded-compensation dedup keys and
    // re-execute on resume (double refund). Loud WARN with the exact values on violation.
    // The horizon is applied on the re-drive path only, so in sampling-only mode
    // (compensation-retry-enabled=false) it is inert and this WARN would point the operator at a
    // knob that does nothing there — the dwell bound that matters in that config is
    // SagaTimeoutRunner.Builder.maxCompensatingDwell (docs/guide/advanced/saga.md).
    if (compensationRetryEnabled) {
      SagaRetentionValidator.validate(giveUpAfter, inboxRetentionMaxAge);
    }
    // Fail fast if the saga dead-letter retention window can outlive the inbox window,
    // which would let a replayed mid-compensation entry re-execute an already-succeeded
    // compensation (double refund) after its inbox dedup keys were swept.
    SagaRetentionValidator.validateDeadLetterRetention(
        sagaDeadLetterRetentionMaxAge, inboxRetentionMaxAge);
    // With compensation-retry-enabled=false AND no StreamRuneMetrics wired, a sweeper would
    // be a pure idle vthread plus a content-free ":sampling-only" health component that can only
    // ever read UP — it re-drives nothing (the knob is off) and samples nothing (sampleBacklog()
    // short-circuits on metrics==NOOP before ever touching the saga store). The earlier rationale
    // for building it anyway ("never blinds the gauges the moment metrics get wired in later") is
    // false: metrics is a constructor-time final — wiring it later means restarting this bean (a
    // fresh context), which builds fresh sweepers against whatever metrics is present by then.
    // Skip construction entirely in this combination; a MISSING component on /health means "not
    // configured", the accurate reading here.
    if (!compensationRetryEnabled && metrics == StreamRuneMetrics.NOOP) {
      logger.info(
          "Skipping SagaCompensationRetrySweeper construction for {} saga runner(s):"
              + " compensation-retry-enabled=false and no StreamRuneMetrics is wired, so a"
              + " sweeper would neither re-drive nor sample anything",
          sagaRunners.size());
      return;
    }
    for (SagaRunner<?> runner : sagaRunners) {
      SagaCompensationRetrySweeper<?> sweeper = buildSweeper(runner);
      sweeper.start();
      sweepers.add(sweeper);
      // Register the sweeper's liveness so a dead saga driver surfaces DOWN on /health.
      if (relayHealth != null) {
        relayHealth.registerSagaCompensationRetrySweeper(sweeper);
      }
    }
    // Application-declared SagaTimeoutRunner beans are registered by the
    // ALWAYS-present SagaTimeoutRunnerHealthRegistrar, NOT here — this bean is skipped by the saga
    // kill switch and returns early on empty sagaRunners (and used to be
    // @Requires(notEquals="false") on compensation-retry-enabled), so registering timeout-runner
    // health here would drop it in the no-SagaRunner config.
    if (!sweepers.isEmpty()) {
      // Worded to cover both modes — with compensation-retry-enabled=false these sweepers
      // are sampling-only (backlog gauges, never re-drive), so an unconditional "durable
      // compensation recovery" claim overclaimed when the operator switched that off.
      logger.info(
          "Started {} SagaCompensationRetrySweeper(s) (compensation recovery; sampling-only"
              + " when compensation-retry-enabled=false)",
          sweepers.size());
    }
  }

  private <S extends SagaState> SagaCompensationRetrySweeper<S> buildSweeper(SagaRunner<S> runner) {
    var builder =
        SagaCompensationRetrySweeper.<S>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .retryInterval(retryInterval)
            .giveUpAfter(giveUpAfter)
            // The knob switches the re-drive off; the sweeper keeps sampling.
            .compensationRetryEnabled(compensationRetryEnabled)
            // Key-age guard — an episode that dwelled past the inbox retention window
            // (an outage, or a give-up horizon misconfigured above the window, which the validator
            // only WARNs about) is FAULTed without re-dispatch instead of re-executing a
            // possibly-pruned-key compensation (double refund).
            .inboxRetentionMaxAge(inboxRetentionMaxAge)
            .clock(Clock.systemUTC())
            .metrics(metrics);
    if (leadership != null) {
      builder.leadership(leadership);
    }
    return builder.build();
  }

  @PreDestroy
  @Override
  public void close() {
    for (SagaCompensationRetrySweeper<?> sweeper : sweepers) {
      try {
        sweeper.close();
      } catch (RuntimeException e) {
        logger.warn("Failed to stop a SagaCompensationRetrySweeper", e);
      }
    }
    sweepers.clear();
  }

  /** Package-private for tests: the sweepers currently managed by this lifecycle. */
  List<SagaCompensationRetrySweeper<?>> sweepers() {
    return List.copyOf(sweepers);
  }
}
