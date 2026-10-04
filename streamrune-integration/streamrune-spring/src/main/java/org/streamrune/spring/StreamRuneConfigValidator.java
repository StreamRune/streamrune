package org.streamrune.spring;

import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;

/**
 * Validates StreamRune configuration at startup and throws {@link IllegalStateException} with a
 * clear message when the configuration is invalid.
 */
public class StreamRuneConfigValidator implements SmartInitializingSingleton {
  private final StreamRuneProperties properties;
  private final ApplicationContext ctx;
  private final Environment env;

  public StreamRuneConfigValidator(
      StreamRuneProperties properties, ApplicationContext ctx, Environment env) {
    this.properties = properties;
    this.ctx = ctx;
    this.env = env;
  }

  @Override
  public void afterSingletonsInstantiated() {
    int snapshotN =
        env.getProperty(
            "streamrune.snapshot-every-n-events", Integer.class, properties.snapshotEveryNEvents());
    if (snapshotN <= 0) {
      throw new IllegalStateException(
          "streamrune.snapshot-every-n-events must be > 0, got: " + snapshotN);
    }
    // streamrune.lock-timeout is handed verbatim to whichever AggregateLocker is wired
    // and nothing on the wiring path rejects it (VirtualThreadCommandBus.Builder takes it as-is).
    // A NEGATIVE value therefore used to mean two different things: LocalStripedLocker (the
    // auto-configured default) did tryLock(negativeMillis) — one attempt, no wait — while
    // PgAdvisoryLocker (the documented multi-replica upgrade) substituted a hardcoded 30s default
    // and BLOCKED. Moving to multi-replica silently changed the behaviour of an unchanged config.
    // The frozen rule is "zero means do not wait; a negative duration is never a valid StreamRune
    // value", and the operator must learn it at startup rather than under contention.
    // Read from the bound properties, not the Environment: the bound value is the one that actually
    // reaches the command bus, and Environment#getProperty(String, Class) cannot convert the
    // compact duration form ("5s") that the property binder accepts.
    //
    // The rule now also covers the (0ms, 1ms) band the negative check let through.
    // Spring's DurationStyle accepts "500us"/"500ns"; the value is positive, so it passed here, and
    // PgAdvisoryLocker turned it into lock_timeout = '0ms' — PostgreSQL's spelling of DISABLED,
    // i.e. wait forever — while LocalStripedLocker on the same value made a single attempt.
    // Delegated to AggregateLocker#requireValidTimeout so the five enforcement points (two lockers,
    // three boot validators) share ONE rule and ONE message and cannot drift apart the way the
    // negative arm's three hand-copied checks could; the IAE is translated to the
    // IllegalStateException this validator contracts to throw.
    java.time.Duration lockTimeout = properties.lockTimeout();
    try {
      org.streamrune.core.AggregateLocker.requireValidTimeout(
          lockTimeout, "streamrune.lock-timeout");
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
    boolean outboxEnabled =
        env.getProperty("streamrune.outbox.enabled", Boolean.class, properties.outbox().enabled());
    if (outboxEnabled
        && ctx.getBeansOfType(org.streamrune.core.outbox.OutboxStore.class).isEmpty()) {
      throw new IllegalStateException(
          "Outbox enabled but no OutboxStore found. "
              + "Add streamrune-kafka-outbox or streamrune-rabbitmq-outbox to your classpath.");
    }
    // The saga dead-letter-retention <= inbox-retention invariant is a
    // REPLAY-safety invariant — it protects SagaDeadLetterReplayer, not the compensation-retry
    // sweeper — so it MUST be validated whenever saga dead-letter replay support is configured (a
    // SagaDeadLetterStore bean is present), independent of
    // streamrune.saga.compensation-retry-enabled (the SagaCompensationRetryLifecycle bean, the
    // only other place this fires, used to be @ConditionalOnProperty on that knob; the
    // knob now only switches its sweepers to sampling-only). Validating here keeps the fail-fast
    // independent of that lifecycle bean, which exists only when streamrune.saga.enabled is on and
    // SagaRunner and SagaStore beans are present, so a deployment with replay support but without
    // those beans still fails fast.
    //
    // But the documented saga kill switch disarms it, identically on all three frameworks.
    // Gating on bean PRESENCE alone was only accidentally correct here: the framework's own
    // sagaDeadLetterStore is @ConditionalOnProperty(streamrune.saga.enabled), yet a USER-supplied
    // SagaDeadLetterStore bean (custom schema/store, left wired while sagas are switched off) still
    // satisfies the gate and re-armed a replay invariant for a replayer that can never run.
    boolean sagaEnabled =
        env.getProperty("streamrune.saga.enabled", Boolean.class, properties.saga().enabled());
    if (sagaEnabled
        && !ctx.getBeansOfType(org.streamrune.core.saga.SagaDeadLetterStore.class).isEmpty()) {
      org.streamrune.integration.SagaRetentionValidator.validateDeadLetterRetention(
          properties.saga().deadLetterRetentionMaxAge(), properties.inbox().retentionMaxAge());
    }
  }
}
