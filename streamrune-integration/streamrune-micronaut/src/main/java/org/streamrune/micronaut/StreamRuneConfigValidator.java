package org.streamrune.micronaut;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;

/**
 * Validates StreamRune configuration at startup and throws {@link IllegalStateException} with a
 * clear message when the configuration is invalid.
 *
 * <p>Listens on the context-level {@link StartupEvent} (fires in every context, including headless
 * non-HTTP workers) rather than the HTTP-server-only {@code ServerStartupEvent}, which would skip
 * snapshot/outbox validation entirely in a headless app. Matches the sibling {@link
 * SagaStateCryptoValidator} and {@link StreamRuneAuthorizationValidator}.
 */
@Singleton
public class StreamRuneConfigValidator implements ApplicationEventListener<StartupEvent>, Ordered {
  private final StreamRuneMicronautProperties properties;
  private final ApplicationContext ctx;

  public StreamRuneConfigValidator(
      StreamRuneMicronautProperties properties, ApplicationContext ctx) {
    this.properties = properties;
    this.ctx = ctx;
  }

  // Without Ordered, Micronaut's ordering among the several
  // ApplicationEventListener<StartupEvent> beans in this module is unspecified, so this
  // fail-closed check could run AFTER StreamRuneLifecycle's had already started the runners —
  // whose first poll (ResilientPollLoop, immediate) can persist data before an invalid
  // configuration is ever reported. A value strictly below StreamRuneLifecycle's own puts every
  // validator ahead of it (Ordered: LOWER value runs first). Mirrors Spring, where
  // SmartInitializingSingleton always completes before any SmartLifecycle starts.
  @Override
  public int getOrder() {
    return 1000;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
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
    // check and message on Spring and Quarkus.
    //
    // The rule now also covers the (0ms, 1ms) band the negative check let through.
    // Micronaut accepts "PT0.0005S"; the value is positive, so it passed here, and PgAdvisoryLocker
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
    if (properties.outboxEnabled()) {
      if (!ctx.containsBean(org.streamrune.core.outbox.OutboxStore.class)) {
        throw new IllegalStateException(
            "Outbox enabled but no OutboxStore found. Declare a PostgresOutboxStore bean"
                + " (streamrune-postgres); streamrune-kafka-outbox and streamrune-rabbitmq-outbox"
                + " provide only the OutboxPublisher.");
      }
      // Without a publisher the framework poller is never created, so the entries the event store
      // writes would stay PENDING with nothing relaying them. An application poller brings its own.
      if (!ctx.containsBean(org.streamrune.core.outbox.OutboxPublisher.class)
          && !ctx.containsBean(org.streamrune.runtime.OutboxPoller.class)) {
        throw new IllegalStateException(
            "Outbox enabled but no OutboxPublisher found, so no OutboxPoller can relay the outbox"
                + " entries. Declare an OutboxPublisher bean (streamrune-kafka-outbox,"
                + " streamrune-rabbitmq-outbox or an HttpOutboxPublisher).");
      }
    }
    // The framework's event store binds the outbox store when it is built, and an application
    // without projections first builds it for its first command. Build it here whenever a mapper
    // and a store are declared, so a store it cannot write into fails the boot (see
    // StreamRuneMicronautModule#transactionalOutboxStore) before the runners start, instead of
    // failing the first command.
    if (ctx.containsBean(org.streamrune.core.outbox.OutboxEventMapper.class)
        && ctx.containsBean(org.streamrune.core.outbox.OutboxStore.class)
        && ctx.containsBean(org.streamrune.core.EventStoreFactory.class)) {
      ctx.getBean(org.streamrune.core.EventStoreFactory.class);
    }
    // The saga dead-letter-retention <= inbox-retention invariant is a
    // REPLAY-safety invariant — it protects SagaDeadLetterReplayer, not the compensation-retry
    // sweeper — so it MUST be validated whenever saga dead-letter replay support is configured (a
    // SagaDeadLetterStore bean is present), independent of
    // streamrune.saga.compensation-retry-enabled (the SagaCompensationRetryLifecycle bean, the
    // only other place this fires, used to be @Requires(notEquals="false") on that knob;
    // the knob now only switches its sweepers to sampling-only). Validating here keeps the
    // fail-fast independent of that lifecycle bean, which exists only when streamrune.saga.enabled
    // is on and returns before its own check when no SagaRunner beans or no SagaStore are present,
    // so a deployment with replay support but without those beans still fails fast.
    //
    // But the documented saga kill switch disarms it, identically on all three frameworks.
    // Gating on bean PRESENCE alone was only accidentally correct here: the framework's own
    // sagaDeadLetterStore carries @Requires(property = "streamrune.saga.enabled"), yet a
    // USER-supplied SagaDeadLetterStore bean (custom schema/store, left wired while sagas are
    // switched off) still satisfies the gate and re-armed a replay invariant for a replayer that
    // can never run.
    if (properties.sagaEnabled()
        && ctx.containsBean(org.streamrune.core.saga.SagaDeadLetterStore.class)) {
      org.streamrune.integration.SagaRetentionValidator.validateDeadLetterRetention(
          properties.sagaDeadLetterRetentionMaxAge(), properties.inboxRetentionMaxAge());
    }
  }
}
