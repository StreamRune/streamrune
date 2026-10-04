package org.streamrune.quarkus;

import io.quarkus.arc.All;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;
import java.time.Clock;
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
 * in Quarkus and owns their lifecycle. On {@link StartupEvent} it builds and starts one sweeper per
 * runner; on {@link ShutdownEvent} it stops them all.
 *
 * <p>This is the durable, automatic recovery path for a stuck {@code COMPENSATING} saga that has no
 * {@link org.streamrune.runtime.SagaTimeoutRunner} (empty {@code timeout()}) and receives no
 * further correlated event: the sweeper periodically re-drives such sagas' compensation under the
 * shared episode-scoped idempotency key until it durably completes (or the give-up horizon
 * terminalizes it {@code FAULTED}). Saga runners are application-wired; this bean only discovers
 * them via {@code @All List<SagaRunner<?>>}. Each sweeper is leadership-gated via the shared {@link
 * SubscriptionLeadership} bean (single active per saga type); with none it defaults to
 * always-leader (single instance).
 */
@ApplicationScoped
public class SagaCompensationRetryLifecycle {

  private static final Logger logger =
      LoggerFactory.getLogger(SagaCompensationRetryLifecycle.class);

  private final List<SagaRunner<?>> sagaRunners;
  private final Instance<SagaStore> sagaStoreInstance;
  private final Instance<SubscriptionLeadership> leadershipInstance;
  private final Instance<StreamRuneMetrics> metricsInstance;
  private final Instance<BackgroundRelayHealthContributor> relayHealthInstance;
  private final StreamRuneQuarkusProperties properties;
  private final List<SagaCompensationRetrySweeper<?>> sweepers = new ArrayList<>();

  @jakarta.inject.Inject
  public SagaCompensationRetryLifecycle(
      @All List<SagaRunner<?>> sagaRunners,
      Instance<SagaStore> sagaStoreInstance,
      Instance<SubscriptionLeadership> leadershipInstance,
      Instance<StreamRuneMetrics> metricsInstance,
      Instance<BackgroundRelayHealthContributor> relayHealthInstance,
      StreamRuneQuarkusProperties properties) {
    this.sagaRunners = sagaRunners;
    this.sagaStoreInstance = sagaStoreInstance;
    this.leadershipInstance = leadershipInstance;
    this.metricsInstance = metricsInstance;
    this.relayHealthInstance = relayHealthInstance;
    this.properties = properties;
  }

  /**
   * Client-proxy constructor — see {@code StreamRuneConfigValidator#StreamRuneConfigValidator()}
   * for the rationale. Never used for a real instance.
   */
  protected SagaCompensationRetryLifecycle() {
    this.sagaRunners = null;
    this.sagaStoreInstance = null;
    this.leadershipInstance = null;
    this.metricsInstance = null;
    this.relayHealthInstance = null;
    this.properties = null;
  }

  // This is a second runner-starting startup listener left out of the startup-validator
  // ordering fix — its first sweep is immediate (ResilientPollLoop) and can re-drive COMPENSATING
  // sagas
  // before SagaStateCryptoValidator / StreamRuneAuthorizationValidator ever run. Same
  // LIBRARY_BEFORE + 100 tier as StreamRuneLifecycle.onStart (both are runner starters; neither
  // must run before any validator, and their relative order to EACH OTHER does not matter). See
  // StreamRuneConfigValidator#validate for why @Priority sits on the parameter, not the method, and
  // why this method is `public`.
  public void onStart(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE + 100) StartupEvent event) {
    startSweepers();
  }

  /** Package-private for tests: build and start one sweeper per discovered saga runner. */
  void startSweepers() {
    // Mirror the Micronaut sibling's @Requires(property = "streamrune.saga.enabled") —
    // the documented saga kill switch must skip the sweepers, not half-configure them.
    // compensation-retry-enabled=false does NOT skip them — the sweepers are the sole emitters of
    // streamrune.saga.compensating / faulted_rows, so the knob is threaded into each sweeper as
    // sampling-only mode instead (see buildSweeper).
    if (!properties.saga().enabled()) {
      return;
    }
    if (sagaRunners.isEmpty()) {
      return;
    }
    // isUnsatisfied() alone is NOT enough. StreamRuneProducers.sagaStore is a
    // dependent-scoped @DefaultBean producer that returns NULL when streamrune.saga.enabled=false
    // (the null-product contract StreamRuneProducers' class javadoc warns consumers about), so the
    // bean DEFINITION exists — isUnsatisfied() is false — while get() hands back null. Feeding that
    // null to the sweeper builder threw NullPointerException("sagaStore is required") inside the
    // @Observes StartupEvent observer and aborted Quarkus boot for an app that merely flipped the
    // kill switch while keeping its own SagaRunner beans. Resolve first, then null-check.
    SagaStore sagaStore = sagaStoreInstance.isUnsatisfied() ? null : sagaStoreInstance.get();
    if (sagaStore == null) {
      logger.debug(
          "StreamRune: no SagaStore available — skipping SagaCompensationRetrySweeper startup"
              + " (sagas disabled or no saga store configured)");
      return;
    }
    // The compensation give-up horizon must be strictly below the inbox retention
    // window, or a stuck COMPENSATING episode can outlive its succeeded-compensation dedup keys and
    // re-execute on resume (double refund). Loud WARN with the exact values on violation.
    // The horizon is applied on the re-drive path only, so in sampling-only mode
    // (compensation-retry-enabled=false) it is inert and this WARN would point the operator at a
    // knob that does nothing there — the dwell bound that matters in that config is
    // SagaTimeoutRunner.Builder.maxCompensatingDwell (docs/guide/advanced/saga.md).
    if (properties.saga().compensationRetryEnabled()) {
      SagaRetentionValidator.validate(
          properties.saga().compensationRetryGiveUpAfter(), properties.inbox().retentionMaxAge());
    }
    // Fail fast if the saga dead-letter retention window can outlive the inbox window,
    // which would let a replayed mid-compensation entry re-execute an already-succeeded
    // compensation (double refund) after its inbox dedup keys were swept.
    SagaRetentionValidator.validateDeadLetterRetention(
        properties.saga().deadLetterRetentionMaxAge(), properties.inbox().retentionMaxAge());
    // Same null-product contract for the optional beans below (a producer may return null even when
    // its definition exists), so each resolution is coalesced rather than trusted.
    StreamRuneMetrics resolvedMetrics =
        metricsInstance.isUnsatisfied() ? null : metricsInstance.get();
    StreamRuneMetrics metrics = resolvedMetrics == null ? StreamRuneMetrics.NOOP : resolvedMetrics;
    // With compensation-retry-enabled=false AND no StreamRuneMetrics wired, a sweeper would
    // be a pure idle vthread plus a content-free ":sampling-only" health component that can only
    // ever read UP — it re-drives nothing (the knob is off) and samples nothing (sampleBacklog()
    // short-circuits on metrics==NOOP before ever touching the saga store). The earlier rationale
    // for building it anyway ("never blinds the gauges the moment metrics get wired in later") is
    // false: metrics is resolved fresh here, but this method only ever runs once, at
    // StartupEvent — wiring metrics later means restarting the app (a fresh CDI container), which
    // re-resolves metrics from scratch. Skip construction entirely in this combination; a MISSING
    // component on /health means "not configured", the accurate reading here.
    if (!properties.saga().compensationRetryEnabled() && metrics == StreamRuneMetrics.NOOP) {
      logger.info(
          "StreamRune: skipping SagaCompensationRetrySweeper construction for {} saga runner(s):"
              + " compensation-retry-enabled=false and no StreamRuneMetrics is wired, so a"
              + " sweeper would neither re-drive nor sample anything",
          sagaRunners.size());
      return;
    }
    SubscriptionLeadership leadership =
        leadershipInstance.isUnsatisfied() ? null : leadershipInstance.get();
    BackgroundRelayHealthContributor health =
        relayHealthInstance.isUnsatisfied() ? null : relayHealthInstance.get();
    for (SagaRunner<?> runner : sagaRunners) {
      SagaCompensationRetrySweeper<?> sweeper =
          buildSweeper(runner, sagaStore, leadership, metrics);
      sweeper.start();
      sweepers.add(sweeper);
      // Register the sweeper's liveness so a dead saga driver surfaces DOWN on /health.
      if (health != null) {
        health.registerSagaCompensationRetrySweeper(sweeper);
      }
    }
    // Application-declared SagaTimeoutRunner beans are registered by the
    // ALWAYS-present SagaTimeoutRunnerHealthRegistrar, NOT here — this path is skipped by the saga
    // kill switch and short-circuits on empty sagaRunners (and used to be withheld by
    // compensation-retry-enabled), so registering timeout-runner health here would drop it in the
    // no-SagaRunner config where the timeout runner still matters.
    if (!sweepers.isEmpty()) {
      // Worded to cover both modes — with compensation-retry-enabled=false these sweepers
      // are sampling-only (backlog gauges, never re-drive), so an unconditional "durable
      // compensation recovery" claim overclaimed when the operator switched that off.
      logger.info(
          "StreamRune: started {} SagaCompensationRetrySweeper(s) (compensation recovery;"
              + " sampling-only when compensation-retry-enabled=false)",
          sweepers.size());
    }
  }

  private <S extends SagaState> SagaCompensationRetrySweeper<S> buildSweeper(
      SagaRunner<S> runner,
      SagaStore sagaStore,
      SubscriptionLeadership leadership,
      StreamRuneMetrics metrics) {
    var builder =
        SagaCompensationRetrySweeper.<S>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .retryInterval(properties.saga().compensationRetryInterval())
            .giveUpAfter(properties.saga().compensationRetryGiveUpAfter())
            // The knob switches the re-drive off; the sweeper keeps sampling.
            .compensationRetryEnabled(properties.saga().compensationRetryEnabled())
            // Key-age guard — an episode that dwelled past the inbox retention window
            // (an outage, or a give-up horizon misconfigured above the window, which the validator
            // only WARNs about) is FAULTed without re-dispatch instead of re-executing a
            // possibly-pruned-key compensation (double refund).
            .inboxRetentionMaxAge(properties.inbox().retentionMaxAge())
            .clock(Clock.systemUTC())
            .metrics(metrics);
    if (leadership != null) {
      builder.leadership(leadership);
    }
    return builder.build();
  }

  void onStop(@Observes ShutdownEvent event) {
    stopSweepers();
  }

  /** Package-private for tests: stop every managed sweeper. */
  void stopSweepers() {
    for (SagaCompensationRetrySweeper<?> sweeper : sweepers) {
      try {
        sweeper.close();
      } catch (RuntimeException e) {
        logger.warn("StreamRune: failed to stop a SagaCompensationRetrySweeper", e);
      }
    }
    sweepers.clear();
  }

  /** Package-private for tests: the sweepers currently managed by this lifecycle. */
  List<SagaCompensationRetrySweeper<?>> sweepers() {
    return List.copyOf(sweepers);
  }
}
