package org.streamrune.quarkus;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.interceptor.Interceptor;

/**
 * Validates StreamRune configuration at startup and throws {@link IllegalStateException} with a
 * clear message when the configuration is invalid.
 */
@ApplicationScoped
public class StreamRuneConfigValidator {
  private final StreamRuneQuarkusProperties properties;
  private final Instance<org.streamrune.core.outbox.OutboxStore> outboxStore;
  private final Instance<org.streamrune.core.outbox.OutboxPublisher> outboxPublisher;
  private final BeanManager beanManager;
  private final Instance<org.streamrune.core.saga.SagaDeadLetterStore> sagaDeadLetterStore;
  private final Instance<org.streamrune.runtime.gdpr.ForgetSubjectService> forgetSubjectService;

  @jakarta.inject.Inject
  public StreamRuneConfigValidator(
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.outbox.OutboxStore> outboxStore,
      Instance<org.streamrune.core.outbox.OutboxPublisher> outboxPublisher,
      BeanManager beanManager,
      Instance<org.streamrune.core.saga.SagaDeadLetterStore> sagaDeadLetterStore,
      Instance<org.streamrune.runtime.gdpr.ForgetSubjectService> forgetSubjectService) {
    this.properties = properties;
    this.outboxStore = outboxStore;
    this.outboxPublisher = outboxPublisher;
    this.beanManager = beanManager;
    this.sagaDeadLetterStore = sagaDeadLetterStore;
    this.forgetSubjectService = forgetSubjectService;
  }

  /**
   * A validator that does not build the forget service on the startup event — for tests of the
   * other checks.
   */
  StreamRuneConfigValidator(
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.outbox.OutboxStore> outboxStore,
      Instance<org.streamrune.core.outbox.OutboxPublisher> outboxPublisher,
      BeanManager beanManager,
      Instance<org.streamrune.core.saga.SagaDeadLetterStore> sagaDeadLetterStore) {
    this(properties, outboxStore, outboxPublisher, beanManager, sagaDeadLetterStore, null);
  }

  /**
   * A validator for tests of the checks that do not involve the outbox: it sees no publisher and no
   * poller, so it refuses an enabled outbox.
   */
  StreamRuneConfigValidator(
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.outbox.OutboxStore> outboxStore,
      Instance<org.streamrune.core.saga.SagaDeadLetterStore> sagaDeadLetterStore) {
    this(properties, outboxStore, null, null, sagaDeadLetterStore, null);
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
    this.outboxPublisher = null;
    this.beanManager = null;
    this.sagaDeadLetterStore = null;
    this.forgetSubjectService = null;
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
    // streamrune.subscription.single-active-consumer.enabled is bound as text and parsed where it
    // is read — the leadership producer, which runs only when something injects the leadership,
    // and the saga forward fence. Parsing it here makes an unknown spelling fail every startup,
    // with or without a DataSource, as binding does on Spring and Micronaut.
    try {
      org.streamrune.integration.SingleActiveConsumerSetting.enabled(
          properties.subscription().singleActiveConsumer().enabled());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
    // streamrune.gdpr.snapshot-purge, parsed here for the same reason: the forget service that
    // reads it is a dependent bean built only when injected.
    try {
      org.streamrune.integration.SnapshotPurgeSetting.parse(properties.gdpr().snapshotPurge());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(e.getMessage(), e);
    }
    // The produced ForgetSubjectService refuses to build without a snapshot purger bean (or with
    // one under snapshot-purge=none). Building it here makes that a startup failure: an
    // application may inject the service only into a request-time resource, where the refusal
    // would surface on the first erasure request instead.
    if (forgetSubjectService != null && forgetSubjectService.isResolvable()) {
      forgetSubjectService.get();
    }
    if (properties.outbox().enabled()) {
      if (!outboxStore.isResolvable()) {
        throw new IllegalStateException(
            "Outbox enabled but no OutboxStore found. Declare a PostgresOutboxStore bean"
                + " (streamrune-postgres); streamrune-kafka-outbox and streamrune-rabbitmq-outbox"
                + " provide only the OutboxPublisher.");
      }
      // Without a publisher the framework poller is never produced, so the entries the event
      // store writes would stay PENDING with nothing relaying them. An application poller brings
      // its own.
      boolean publisher = outboxPublisher != null && outboxPublisher.isResolvable();
      boolean applicationPoller =
          beanManager != null
              && StreamRuneLifecycle.applicationOwns(
                  beanManager,
                  org.streamrune.runtime.OutboxPoller.class,
                  StreamRuneProducers.FRAMEWORK_OUTBOX_POLLER);
      if (!publisher && !applicationPoller) {
        throw new IllegalStateException(
            "Outbox enabled but no OutboxPublisher found, so no OutboxPoller can relay the outbox"
                + " entries. Declare an OutboxPublisher bean (streamrune-kafka-outbox,"
                + " streamrune-rabbitmq-outbox or an HttpOutboxPublisher).");
      }
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
