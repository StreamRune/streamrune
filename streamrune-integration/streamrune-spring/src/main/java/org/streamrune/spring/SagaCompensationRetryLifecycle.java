package org.streamrune.spring;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
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
 * and manages their lifecycle as a group. On context refresh it builds and starts one sweeper per
 * runner; on close it stops them all.
 *
 * <p>This is the automatic, durable recovery path for a stuck {@code COMPENSATING} saga that has no
 * {@link org.streamrune.runtime.SagaTimeoutRunner} (empty {@code timeout()}) and receives no
 * further correlated event — the sweeper periodically re-drives such sagas' compensation under the
 * shared episode-scoped idempotency key until it durably completes (or the give-up horizon
 * terminalizes it {@code FAULTED}). The saga runners themselves are application-wired; this
 * lifecycle only needs to discover them as beans, mirroring how the dead-letter retry runner is
 * gated on the presence of a {@code DeadLetterQueue} bean.
 *
 * <p>{@code streamrune.saga.compensation-retry-enabled=false} does not withhold this lifecycle any
 * more — it builds the sweepers in sampling-only mode, because {@code streamrune.saga.compensating}
 * / {@code streamrune.saga.faulted_rows} are emitted by the sweepers and by nothing else. Only the
 * saga kill switch ({@code streamrune.saga.enabled=false}) skips them entirely.
 *
 * <p>Each sweeper is leadership-gated via the shared {@link SubscriptionLeadership} bean (single
 * active per saga type), so multi-replica deployments do not all re-drive the same episode; with no
 * leadership bean it defaults to always-leader (single-instance).
 */
public final class SagaCompensationRetryLifecycle implements SmartLifecycle {

  private static final Logger logger =
      LoggerFactory.getLogger(SagaCompensationRetryLifecycle.class);

  private final ObjectProvider<SagaRunner<?>> sagaRunners;
  private final SagaStore sagaStore;
  private final ObjectProvider<SubscriptionLeadership> leadership;
  private final ObjectProvider<StreamRuneMetrics> metrics;
  private final ObjectProvider<BackgroundRelayHealthContributor> relayHealth;
  private final StreamRuneProperties properties;
  private final List<SagaCompensationRetrySweeper<?>> sweepers = new ArrayList<>();
  private volatile boolean running;

  public SagaCompensationRetryLifecycle(
      ObjectProvider<SagaRunner<?>> sagaRunners,
      SagaStore sagaStore,
      ObjectProvider<SubscriptionLeadership> leadership,
      ObjectProvider<StreamRuneMetrics> metrics,
      ObjectProvider<BackgroundRelayHealthContributor> relayHealth,
      StreamRuneProperties properties) {
    this.sagaRunners = sagaRunners;
    this.sagaStore = sagaStore;
    this.leadership = leadership;
    this.metrics = metrics;
    this.relayHealth = relayHealth;
    this.properties = properties;
  }

  @Override
  public void start() {
    if (running) {
      return;
    }
    StreamRuneProperties.Saga saga = properties.saga();
    // Honour the documented saga kill switch here too, matching the Quarkus sibling's
    // startSweepers() guard. The auto-configuration's @ConditionalOnProperty already suppresses
    // this bean, but an application that hand-declares the lifecycle bypasses that gate — and the
    // SagaRetentionValidator calls below would then fail-fast on saga invariants for a feature the
    // operator switched off. The kill switch must skip the sweepers, not half-configure them.
    if (!saga.enabled()) {
      logger.debug(
          "StreamRune: streamrune.saga.enabled=false — skipping SagaCompensationRetrySweeper"
              + " startup");
      return;
    }
    // The compensation give-up horizon must be strictly below the inbox retention
    // window, or a stuck COMPENSATING episode can outlive its succeeded-compensation dedup keys and
    // re-execute on resume (double refund). Loud WARN with the exact values on violation.
    // The horizon is applied on the re-drive path only, so in sampling-only mode
    // (compensation-retry-enabled=false) it is inert and this WARN would point the operator at a
    // knob that does nothing there — the dwell bound that matters in that config is
    // SagaTimeoutRunner.Builder.maxCompensatingDwell (docs/guide/advanced/saga.md).
    if (saga.compensationRetryEnabled()) {
      SagaRetentionValidator.validate(
          saga.compensationRetryGiveUpAfter(), properties.inbox().retentionMaxAge());
    }
    // Fail fast if the saga dead-letter retention window can outlive the inbox window,
    // which would let a replayed mid-compensation entry re-execute an already-succeeded
    // compensation (double refund) after its inbox dedup keys were swept.
    SagaRetentionValidator.validateDeadLetterRetention(
        saga.deadLetterRetentionMaxAge(), properties.inbox().retentionMaxAge());
    StreamRuneMetrics resolvedMetrics = metrics.getIfAvailable(() -> StreamRuneMetrics.NOOP);
    // With compensation-retry-enabled=false AND no StreamRuneMetrics wired, a sweeper would
    // be a pure idle vthread plus a content-free ":sampling-only" health component that can only
    // ever read UP — it re-drives nothing (the knob is off) and samples nothing (sampleBacklog()
    // short-circuits on metrics==NOOP before ever touching the saga store). The earlier rationale
    // for building it anyway ("never blinds the gauges the moment metrics get wired in later") is
    // false: metrics is resolved once here, in start() — wiring it later means restarting this
    // context, which re-resolves it from scratch via a fresh start(). Skip construction entirely
    // in this combination (matching the saga.enabled=false kill switch above: return without
    // marking this lifecycle running — there is nothing here for stop() to stop); a MISSING
    // component on /health means "not configured", the accurate reading here.
    if (!saga.compensationRetryEnabled() && resolvedMetrics == StreamRuneMetrics.NOOP) {
      logger.info(
          "StreamRune: skipping SagaCompensationRetrySweeper construction:"
              + " compensation-retry-enabled=false and no StreamRuneMetrics is wired, so a"
              + " sweeper would neither re-drive nor sample anything");
      return;
    }
    BackgroundRelayHealthContributor health = relayHealth.getIfAvailable();
    sagaRunners
        .orderedStream()
        .forEach(
            runner -> {
              SagaCompensationRetrySweeper<?> sweeper = buildSweeper(runner, saga, resolvedMetrics);
              sweeper.start();
              sweepers.add(sweeper);
              // Register the sweeper's liveness so a dead/wedged saga driver surfaces DOWN
              // on /health instead of silently stranding COMPENSATING sagas (refunds never issued).
              if (health != null) {
                health.registerSagaCompensationRetrySweeper(sweeper);
              }
            });
    // Application-declared SagaTimeoutRunner beans are registered by the
    // ALWAYS-present SagaTimeoutRunnerHealthRegistrar, NOT here — this lifecycle is skipped by the
    // saga kill switch and short-circuits on empty sagaRunners (and used to be withheld
    // by compensation-retry-enabled), so registering timeout-runner health here would drop it in
    // the no-SagaRunner config where the timeout runner is the sole compensation driver.
    if (!sweepers.isEmpty()) {
      // Worded to cover both modes — with compensation-retry-enabled=false these sweepers
      // are sampling-only (backlog gauges, never re-drive), so an unconditional "durable
      // compensation recovery" claim overclaimed when the operator switched that off.
      logger.info(
          "StreamRune: started {} SagaCompensationRetrySweeper(s) (compensation recovery;"
              + " sampling-only when compensation-retry-enabled=false)",
          sweepers.size());
    }
    running = true;
  }

  private <S extends SagaState> SagaCompensationRetrySweeper<S> buildSweeper(
      SagaRunner<S> runner, StreamRuneProperties.Saga saga, StreamRuneMetrics resolvedMetrics) {
    var builder =
        SagaCompensationRetrySweeper.<S>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .retryInterval(saga.compensationRetryInterval())
            .giveUpAfter(saga.compensationRetryGiveUpAfter())
            // The knob switches the re-drive off; the sweeper keeps sampling the saga
            // backlog gauges it is the sole emitter of.
            .compensationRetryEnabled(saga.compensationRetryEnabled())
            // Key-age guard — an episode that dwelled past the inbox retention window
            // (an outage, or a give-up horizon misconfigured above the window, which the validator
            // above only WARNs about) is FAULTed without re-dispatch instead of re-executing a
            // possibly-pruned-key compensation (double refund).
            .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
            .clock(Clock.systemUTC())
            .metrics(resolvedMetrics);
    leadership.ifAvailable(builder::leadership);
    return builder.build();
  }

  @Override
  public void stop() {
    if (!running) {
      return;
    }
    running = false;
    for (SagaCompensationRetrySweeper<?> sweeper : sweepers) {
      try {
        sweeper.close();
      } catch (RuntimeException e) {
        logger.warn("StreamRune: failed to stop a SagaCompensationRetrySweeper", e);
      }
    }
    sweepers.clear();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  /**
   * Runs before the web server, after the runners it depends on — same phase as {@link
   * RunnerLifecycle}.
   */
  @Override
  public int getPhase() {
    return RunnerLifecycle.PHASE;
  }

  /** Package-private for tests: the sweepers currently managed by this lifecycle. */
  List<SagaCompensationRetrySweeper<?>> sweepers() {
    return List.copyOf(sweepers);
  }
}
