package org.streamrune.quarkus;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;

/**
 * Validates StreamRune configuration at startup and throws {@link IllegalStateException} with a
 * clear message when the configuration is invalid.
 */
@ApplicationScoped
public class StreamRuneConfigValidator {
  private final StreamRuneQuarkusProperties properties;
  private final Instance<org.streamrune.core.outbox.OutboxStore> outboxStore;
  private final Instance<org.streamrune.core.saga.SagaDeadLetterStore> sagaDeadLetterStore;

  @jakarta.inject.Inject
  public StreamRuneConfigValidator(
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.outbox.OutboxStore> outboxStore,
      Instance<org.streamrune.core.saga.SagaDeadLetterStore> sagaDeadLetterStore) {
    this.properties = properties;
    this.outboxStore = outboxStore;
    this.sagaDeadLetterStore = sagaDeadLetterStore;
  }

  /**
   * Client-proxy constructor. A normal-scoped bean needs a non-private no-arg constructor for its
   * client proxy; Quarkus's build step adds one by bytecode transformation, but the module's
   * container-level tests ({@code RealArcTestContainer}) run the raw Arc processor without a
   * transformer, so the bean must be proxyable as written. {@code protected} rather than
   * package-private: the proxy is a generated SUBCLASS that may live in a different classloader (a
   * different runtime package), where only protected access reaches a {@code super()} constructor.
   * Never used for a real instance — the injecting constructor above is the one Arc actually calls.
   */
  protected StreamRuneConfigValidator() {
    this.properties = null;
    this.outboxStore = null;
    this.sagaDeadLetterStore = null;
  }

  // Without an explicit @Priority, CDI/Arc observer ordering among the several
  // @Observes StartupEvent methods in this module is UNSPECIFIED, so this fail-closed check could
  // run AFTER StreamRuneLifecycle.onStart had already started the runners — whose first poll
  // (ResilientPollLoop, immediate) can persist data before an invalid configuration is ever
  // reported. LIBRARY_BEFORE puts every validator strictly ahead of StreamRuneLifecycle's own
  // (higher-numbered, later) priority. Mirrors Spring, where SmartInitializingSingleton always
  // completes before any SmartLifecycle starts.
  //
  // @Priority must sit on the EVENT PARAMETER, not the method. Arc's observer-priority
  // resolution (io.quarkus.arc.processor.ObserverInfo.create) reads @Priority from
  // Annotations.getParameterAnnotations(...) on the observed-event parameter only — a method-level
  // @Priority compiles (CDI 4 permits @Priority on a method, for producer/alternative
  // prioritization) but is silently ignored for observer ordering, so this annotation previously
  // had no effect on dispatch order at all. `public` (not package-private), for the same
  // cross-classloader reason as the constructor above: the observer invoker Arc generates is a
  // separate class that may live in a different classloader.
  public void validate(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent event) {
    if (properties.snapshotEveryNEvents() <= 0) {
      throw new IllegalStateException(
          "streamrune.snapshot-every-n-events must be > 0, got: "
              + properties.snapshotEveryNEvents());
    }
    // streamrune.lock-timeout is handed verbatim to whichever AggregateLocker is wired
    // and nothing on the wiring path rejects it (VirtualThreadCommandBus.Builder takes it as-is).
    // A NEGATIVE value therefore used to mean two different things: LocalStripedLocker (the
    // auto-configured default) did tryLock(negativeMillis) — one attempt, no wait — while
    // PgAdvisoryLocker (the documented multi-replica upgrade) substituted a hardcoded 30s default
    // and BLOCKED. Moving to multi-replica silently changed the behaviour of an unchanged config.
    // The frozen rule is "zero means do not wait; a negative duration is never a valid StreamRune
    // value", and the operator must learn it at startup rather than under contention. Identical
    // check and message on Spring and Micronaut.
    //
    // The rule now also covers the (0ms, 1ms) band the negative check let through.
    // Quarkus accepts "PT0.0005S"; the value is positive, so it passed here, and PgAdvisoryLocker
    // turned it into lock_timeout = '0ms' — PostgreSQL's spelling of DISABLED, i.e. wait forever —
    // while LocalStripedLocker on the same value made a single attempt. Delegated to
    // AggregateLocker#requireValidTimeout so the five enforcement points (two lockers, three boot
    // validators) share ONE rule and ONE message and cannot drift apart the way the negative arm's
    // three hand-copied checks could; the IAE is translated to the IllegalStateException this
    // validator contracts to throw.
    java.time.Duration lockTimeout = properties.lockTimeout();
    try {
      org.streamrune.core.AggregateLocker.requireValidTimeout(
          lockTimeout, "streamrune.lock-timeout");
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
    if (properties.outbox().enabled() && !outboxStore.isResolvable()) {
      throw new IllegalStateException(
          "Outbox enabled but no OutboxStore found. "
              + "Add streamrune-kafka-outbox or streamrune-rabbitmq-outbox to your classpath.");
    }
    // The saga dead-letter-retention <= inbox-retention invariant is a
    // REPLAY-safety invariant — it protects SagaDeadLetterReplayer, not the compensation-retry
    // sweeper — so it MUST be validated whenever saga dead-letter replay support is configured (a
    // SagaDeadLetterStore bean is present), independent of
    // streamrune.saga.compensation-retry-enabled (SagaCompensationRetryLifecycle.startSweepers(),
    // the only other place this fires, used to return early when that knob was off; the
    // knob now only switches its sweepers to sampling-only). Validating here keeps the fail-fast
    // independent of startSweepers(), which returns before its own check when no SagaRunner beans
    // exist or no SagaStore resolves, so a deployment with replay support but without those beans
    // still fails fast.
    //
    // But the documented saga kill switch disarms it. With streamrune.saga.enabled=false
    // there is no replayer to protect, so a saga invariant must never fail the boot of an app that
    // does not use sagas — the same config boots on Spring and Micronaut, and this asymmetry made
    // Quarkus refuse a legitimate "sagas off + tuned inbox retention" deployment. Two independent
    // holes are closed here:
    //   1. the kill switch itself (checked first, before any bean lookup), and
    //   2. isResolvable() is NOT a presence proxy for these producers. StreamRuneProducers
    //      .sagaDeadLetterStore is a dependent-scoped @DefaultBean producer that returns NULL when
    //      sagas are disabled (the null-product contract its class javadoc warns about), so the
    //      bean DEFINITION always resolves while get() hands back null. Resolve, then null-check —
    //      the same pattern already applied in SagaCompensationRetryLifecycle.startSweepers().
    if (!properties.saga().enabled()) {
      return;
    }
    org.streamrune.core.saga.SagaDeadLetterStore resolvedDeadLetterStore =
        sagaDeadLetterStore.isResolvable() ? sagaDeadLetterStore.get() : null;
    if (resolvedDeadLetterStore != null) {
      org.streamrune.integration.SagaRetentionValidator.validateDeadLetterRetention(
          properties.saga().deadLetterRetentionMaxAge(), properties.inbox().retentionMaxAge());
    }
  }
}
