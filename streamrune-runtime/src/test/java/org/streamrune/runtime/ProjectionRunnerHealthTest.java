package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * Verifies FIX 3: the projection runners register their subscription with the {@link
 * SubscriptionHealthContributor} and feed delivery outcomes into it, so a real subscription with a
 * poison listener surfaces as errors/DOWN (not a silent always-UP) and the {@code
 * streamrune.subscriptions.lag} gauge is emitted.
 */
class ProjectionRunnerHealthTest {

  private static final AggregateType TYPE = AggregateType.of("orders");

  private EventStore eventStore;
  private OffsetStore offsetStore;

  @BeforeEach
  void setUp() {
    eventStore = new InMemoryEventStore();
    offsetStore = new InMemoryOffsetStore();
  }

  private static SubscriptionConfig fastConfig() {
    return new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5));
  }

  record TestEvent(String payload) implements DomainEvent {}

  private static EventMetadata metadata(long version) {
    return new EventMetadata(
        IdGenerator.generateEventId(),
        IdGenerator.generateCommandId(),
        null,
        null,
        CorrelationId.of("corr-" + version),
        null,
        null,
        Instant.now());
  }

  private static EventEnvelope envelope(StreamId streamId, long version, String payload) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new TestEvent(payload),
        metadata(version));
  }

  /** Records the latest reported lag per subscription so tests can assert the gauge fired. */
  private static final class RecordingMetrics implements StreamRuneMetrics {
    final ConcurrentHashMap<String, Long> lags = new ConcurrentHashMap<>();

    @Override
    public void recordSubscriptionLag(SubscriptionName subscriptionName, long lag) {
      lags.put(subscriptionName.value(), lag);
    }
  }

  @Test
  void continuousRunner_withPoisonLiveListener_reportsErrorsAndEmitsLagGauge() throws Exception {
    // Start empty so catch-up completes immediately and the runner transitions to live mode, where
    // it registers the subscription with the contributor and wraps the listener in a
    // HealthTrackingEventListener. A poison live event then accrues errors through that wrapper.
    StreamId s = StreamId.of(TYPE, AggregateId.of("orders"));

    var metrics = new RecordingMetrics();
    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100, metrics);

    Projection poison =
        events -> {
          throw new RuntimeException("boom");
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .errorStrategy(ProjectionErrorStrategy.HALT)
            .healthContributor(contributor)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(ProjectionName.of("orders"), poison, AT_LEAST_ONCE_IDEMPOTENT));

    // Wait until the subscription is registered (live mode reached), then feed a poison live event.
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (contributor.health().isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse(contributor.health().isEmpty(), "subscription should be registered in live mode");
    eventStore.append(s, List.of(envelope(s, 1, "poison")), Version.initial());

    // The wrapped live listener throws → recordError fires → health() shows errors, proving
    // overallStatus() is no longer a silent always-UP.
    int errors = 0;
    while (System.nanoTime() < deadline) {
      var healths = contributor.health();
      if (!healths.isEmpty() && healths.getFirst().errorCount() > 0) {
        errors = healths.getFirst().errorCount();
        break;
      }
      Thread.sleep(20);
    }
    // Capture status BEFORE close() — close() unregisters the subscription, after which health() is
    // empty and overallStatus() reverts to UP by contract (no subscriptions == healthy).
    var status = contributor.overallStatus();
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));

    assertTrue(errors > 0, "poison listener should have accrued errors in the health contributor");
    assertTrue(
        metrics.lags.containsKey("orders"),
        "subscriptions.lag gauge should have been emitted for the registered subscription");
    // overallStatus reflects the errors: DEGRADED (errors>0) or DOWN (errors>=threshold), never UP.
    assertNotEquals(
        SubscriptionHealth.Status.UP, status, "status must reflect the poison listener, not be UP");
  }

  @Test
  void continuousRunner_withoutContributor_isNoOpAndDelivers() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("noop-stream"));
    eventStore.append(s, List.of(envelope(s, 1, "ok")), Version.initial());

    var delivered = new AtomicInteger();
    Projection proj = events -> delivered.addAndGet(events.size());

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .build(); // no healthContributor

    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(ProjectionName.of("noop"), proj, AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (delivered.get() == 0 && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertTrue(delivered.get() >= 1, "delivery is unchanged when no contributor is wired");
  }

  @Test
  void continuousRunner_healthySubscription_registersAndUnregistersOnClose() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("healthy"));
    eventStore.append(s, List.of(envelope(s, 1, "ok")), Version.initial());

    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);
    var delivered = new AtomicInteger();
    Projection proj = events -> delivered.addAndGet(events.size());

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .healthContributor(contributor)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(ProjectionName.of("healthy"), proj, AT_LEAST_ONCE_IDEMPOTENT));
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
    while (contributor.health().isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(20);
    }
    assertFalse(contributor.health().isEmpty(), "subscription should be registered while running");

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
    assertTrue(contributor.health().isEmpty(), "subscription should be unregistered after close");
    assertEquals(
        SubscriptionHealth.Status.UP,
        contributor.overallStatus(),
        "a CLEAN stop unregisters, so overallStatus reverts to UP (no subscriptions == healthy)");
  }

  /**
   * A permanent ERROR exit (here: a live subscription that reports it stopped — the same {@code
   * state=ERROR} finally path a HALT-strategy runner takes after exhausting its retry budget) must
   * keep the subscription registered and force its health DOWN, so {@code overallStatus()} — and
   * the /health endpoint that reads it — STAYS DOWN after the runner thread exits. The pre-fix code
   * unregistered on any return, emptying the contributor so overallStatus reverted to UP while the
   * read model was permanently frozen.
   */
  @Test
  void continuousRunner_permanentErrorExit_staysDownAfterThreadExits() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("orders"));
    eventStore.append(s, List.of(envelope(s, 1, "ok")), Version.initial());

    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);
    Projection noop = events -> {};

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .subscriptionFactory((name, listener, readPoisonBound) -> new DeadSubscription())
            .healthContributor(contributor)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(ProjectionName.of("orders"), noop, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (RuntimeException _) {
                    // The runner rethrows after detecting the dead live subscription.
                  }
                });
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "runner thread should have exited on the permanent ERROR");

    assertFalse(
        contributor.health().isEmpty(),
        "a permanently halted projection must remain in the health view, not be unregistered");
    assertEquals(
        SubscriptionHealth.Status.DOWN,
        contributor.health().getFirst().status(),
        "the halted subscription must report DOWN");
    assertEquals(
        SubscriptionHealth.Status.DOWN,
        contributor.overallStatus(),
        "overallStatus must STAY DOWN after the runner thread exits — the read model is frozen");
  }

  /**
   * A {@code java.lang.Error} (AssertionError from user mapping code, NoClassDefFound,
   * StackOverflow) thrown from {@code process()} during CATCH-UP must surface the projection as
   * terminally DOWN — not leave {@code /health} stuck UP forever while the read model is frozen.
   * The old {@code run()} caught only {@code Exception}, so the Error escaped, the finally set
   * STOPPED (never marking terminal), and no health entry was ever registered (catch-up registers
   * no live subscription). an earlier fix hardened only the LIVE poll thread; this is the catch-up
   * gap.
   */
  @Test
  void continuousRunner_errorDuringCatchUp_reportsDownNotStuckUp() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("catchup-error"));
    eventStore.append(s, List.of(envelope(s, 1, "boom")), Version.initial());

    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);
    Projection erroring =
        events -> {
          throw new AssertionError("mapping bug during catch-up");
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .healthContributor(contributor)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    runner.run(
                        ProjectionName.of("catchup-error"), erroring, AT_LEAST_ONCE_IDEMPOTENT);
                  } catch (Throwable _) {
                    // The Error is rethrown AFTER state=ERROR is set so the finally marks terminal.
                  }
                });
    t.join(TimeUnit.SECONDS.toMillis(5));
    assertFalse(t.isAlive(), "runner thread should have exited on the catch-up Error");

    assertEquals(
        ProjectionState.ERROR,
        runner.state(),
        "an Error during catch-up must set state=ERROR, not STOPPED");
    assertEquals(
        SubscriptionHealth.Status.DOWN,
        contributor.overallStatus(),
        "an Error during catch-up must surface as DOWN, not a silent always-UP");
  }

  /** Leadership that is never granted — the runner sits in the STANDBY wait loop forever. */
  private static SubscriptionLeadership neverLeader() {
    return new SubscriptionLeadership() {
      @Override
      public Optional<Lease> tryAcquire(String consumerName) {
        return Optional.empty();
      }

      @Override
      public Optional<Lease> current(String consumerName) {
        return Optional.empty();
      }

      @Override
      public void resign(String consumerName) {}

      @Override
      public void close() {}
    };
  }

  /** Leadership granted until {@code revoked} flips, then permanently withheld. */
  private static final class RevocableLeadership implements SubscriptionLeadership {
    private final java.util.concurrent.atomic.AtomicBoolean revoked =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    @Override
    public Optional<Lease> tryAcquire(String consumerName) {
      return revoked.get() ? Optional.empty() : Optional.of(new Lease(1L));
    }

    @Override
    public Optional<Lease> current(String consumerName) {
      return revoked.get() ? Optional.empty() : Optional.of(new Lease(1L));
    }

    @Override
    public void resign(String consumerName) {}

    @Override
    public void close() {}
  }

  private static Optional<SubscriptionHealth> entryFor(
      SubscriptionHealthContributor contributor, String name) {
    return contributor.health().stream().filter(h -> h.name().equals(name)).findFirst();
  }

  /**
   * Waits until the runner sits in its STANDBY wait loop. That happens on the run thread, so a
   * fixed sleep before asserting on it fails on a slow runner. Only the state is awaited, not the
   * health entry: run() registers the entry before its leadership loop sets STANDBY, so once
   * STANDBY is visible the entry must be too, and the caller's assertion on the entry still names
   * the regression instead of timing out here.
   */
  private static void awaitStandby(ContinuousProjectionRunner runner) {
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> runner.state() == ProjectionState.STANDBY);
  }

  /**
   * A STANDBY replica (leadership never granted) is healthy/expected (multi-replica): it must
   * report UP, not DOWN — but it must still be VISIBLE in the health view, otherwise "the
   * subscription is fine" and "the subscription does not exist here" are indistinguishable.
   */
  @Test
  void continuousRunner_standbyReplica_isRegisteredAndNotDown() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("orders"));
    eventStore.append(s, List.of(envelope(s, 1, "ok")), Version.initial());

    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);
    Projection proj = events -> {};

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .leadership(neverLeader())
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .healthContributor(contributor)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(ProjectionName.of("orders"), proj, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      awaitStandby(runner);

      var entry = entryFor(contributor, "orders");
      assertTrue(
          entry.isPresent(),
          "a STANDBY replica must appear in the health view — an invisible subscription cannot be"
              + " distinguished from a healthy one");
      assertEquals(
          SubscriptionLifecycleState.RUNNING,
          entry.get().state(),
          "STANDBY maps to RUNNING for lifecycle purposes, exactly like ScheduledProjectionRunner");
      assertNotEquals(
          SubscriptionHealth.Status.DOWN,
          contributor.overallStatus(),
          "a STANDBY replica whose shared checkpoint is current must NOT report DOWN");
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }
  }

  /**
   * {@link ContinuousProjectionRunner} registered with the health contributor only INSIDE {@code
   * subscribeToLiveEvents} and unregistered on every non-error return, so a replica that never won
   * the lease registered nothing at all. When NO replica can lead — a terminal {@code
   * standDownTerminally} after a heartbeat Error, or a runtime REVOKE on {@code
   * subscription_leases} that passes the boot-time schema check — every replica sat in STANDBY, the
   * read model froze indefinitely, and every replica's {@code /health} reported UP with no entry
   * for the projection at all. Sibling SCHEDULED projections on the same replicas correctly flipped
   * DOWN, because {@link ScheduledProjectionRunner} registers at {@code start()} and keeps the
   * entry through standby ticks.
   *
   * <p>The contributor's lag is computed against the SHARED {@code projection_offset} checkpoint,
   * so a registered standby answers the right question: "is this subscription making progress
   * ANYWHERE?". A working leader keeps the shared lag low and every standby reports UP; a
   * frozen-everywhere subscription crosses the lag threshold and reads DEGRADED on every replica,
   * with the lag in its detail. Not DOWN: from the lag alone a frozen subscription cannot be told
   * apart from one whose leader is still replaying a large backlog, and a DOWN there would take
   * every replica out of readiness for the whole replay.
   */
  @Test
  void continuousRunner_neverLeadingReplica_reportsSharedCheckpointLagAsDegraded()
      throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("orders"));
    // Five events, nothing checkpointed: shared lag is 5, past the threshold of 2.
    eventStore.append(
        s,
        List.of(
            envelope(s, 1, "a"),
            envelope(s, 2, "b"),
            envelope(s, 3, "c"),
            envelope(s, 4, "d"),
            envelope(s, 5, "e")),
        Version.initial());

    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 2);
    Projection proj = events -> {};

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .leadership(neverLeader())
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .healthContributor(contributor)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(ProjectionName.of("orders"), proj, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      awaitStandby(runner);

      var entry = entryFor(contributor, "orders");
      assertTrue(
          entry.isPresent(),
          "a never-leading replica must still register the projection — otherwise an"
              + " all-replicas-standby freeze is invisible to /health");
      assertEquals(
          SubscriptionHealth.Status.DEGRADED,
          entry.get().status(),
          "the shared checkpoint is 5 events behind the global head with a threshold of 2");
      assertEquals(5, entry.get().lag(), "the shared lag is reported in the detail");
      assertEquals(
          SubscriptionHealth.Status.DEGRADED,
          contributor.overallStatus(),
          "an all-replicas-standby freeze must surface as DEGRADED, not a silent UP");
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }
  }

  /**
   * Losing leadership drops the runner back to STANDBY, which is a healthy state — but the health
   * entry must SURVIVE the drop. Previously the live-subscription unregistration fired on the
   * leadership-loss return, so the replica vanished from the health view the moment it stopped
   * leading. It must also stop reporting the CLOSED live subscription's STOPPED state (which would
   * read as DOWN); the runner's own STANDBY view takes over.
   */
  @Test
  void continuousRunner_leadershipLoss_keepsHealthEntryVisible() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("orders"));
    eventStore.append(s, List.of(envelope(s, 1, "ok")), Version.initial());

    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);
    Projection proj = events -> {};
    var leadership = new RevocableLeadership();

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .healthContributor(contributor)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(ProjectionName.of("orders"), proj, AT_LEAST_ONCE_IDEMPOTENT));
    // Wait for LIVE, then revoke leadership.
    long deadline = System.currentTimeMillis() + 5_000;
    while (runner.state() != ProjectionState.LIVE && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(ProjectionState.LIVE, runner.state(), "runner must reach LIVE before revocation");
    leadership.revoked.set(true);

    deadline = System.currentTimeMillis() + 5_000;
    while (runner.state() != ProjectionState.STANDBY && System.currentTimeMillis() < deadline) {
      Thread.sleep(10);
    }
    assertEquals(ProjectionState.STANDBY, runner.state(), "leadership loss must drop to STANDBY");

    var entry = entryFor(contributor, "orders");
    assertTrue(
        entry.isPresent(), "the health entry must survive a leadership-loss drop to STANDBY");
    assertEquals(
        SubscriptionLifecycleState.RUNNING,
        entry.get().state(),
        "the standby view must replace the CLOSED live subscription, which would report STOPPED");
    assertNotEquals(
        SubscriptionHealth.Status.DOWN,
        contributor.overallStatus(),
        "a healthy standby whose shared checkpoint is current must not report DOWN");

    runner.close();
    t.join(TimeUnit.SECONDS.toMillis(3));
  }

  /**
   * Extends {@link #continuousRunner_leadershipLoss_keepsHealthEntryVisible} with a DLQ'd FINAL
   * catch-up chunk before the first LIVE, so health is already DEGRADED (not UP) at the moment
   * leadership is lost — then re-acquires leadership and asserts DEGRADED still holds. The
   * live-&gt;standby return used to call {@code SubscriptionHealthContributor.register()} (a FRESH
   * entry, errorCount reset to 0) on every non-error return INCLUDING a leadership-loss bounce,
   * silently wiping the catch-up-earned DEGRADED back to UP the instant leadership was lost — with
   * no clean batch ever having run since — even though this replica itself just observed the same
   * read-model hole the departing leader left behind.
   */
  @Test
  void continuousRunner_leadershipLossThenReacquire_degradedSurvivesTheBounce() throws Exception {
    var dlqStore = new org.streamrune.test.InMemoryProjectionDeadLetterStore();
    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);
    var leadership = new RevocableLeadership();
    var name = ProjectionName.of("leadership-bounce");
    StreamId poison = StreamId.of(TYPE, AggregateId.of("leadership-bounce-poison"));
    eventStore.append(poison, List.of(envelope(poison, 1, "poison")), Version.initial());
    Projection proj =
        events -> {
          if (((TestEvent) events.get(0).event()).payload().equals("poison")) {
            throw new RuntimeException("permanent poison"); // DLQ'd during catch-up
          }
        };

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .errorStrategy(ProjectionErrorStrategy.DLQ)
            .deadLetterStore(dlqStore)
            .subscriptionConfig(fastConfig())
            .leadership(leadership)
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .healthContributor(contributor)
            .build();

    Thread t = Thread.ofVirtual().start(() -> runner.run(name, proj, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      long deadline = System.currentTimeMillis() + 5_000;
      while (runner.state() != ProjectionState.LIVE && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(
          ProjectionState.LIVE, runner.state(), "runner must reach LIVE before revocation");
      assertFalse(
          dlqStore.read(name, 10).isEmpty(), "the poison chunk was dead-lettered during catch-up");
      assertEquals(
          SubscriptionHealth.Status.DEGRADED,
          entryFor(contributor, "leadership-bounce").orElseThrow().status(),
          "DEGRADED must already hold at LIVE — the catch-up dead-letter, nothing clean since");

      leadership.revoked.set(true);
      deadline = System.currentTimeMillis() + 5_000;
      while (runner.state() != ProjectionState.STANDBY && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(ProjectionState.STANDBY, runner.state(), "leadership loss must drop to STANDBY");

      leadership.revoked.set(false);
      deadline = System.currentTimeMillis() + 5_000;
      while (runner.state() != ProjectionState.LIVE && System.currentTimeMillis() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(
          ProjectionState.LIVE, runner.state(), "runner must re-acquire and return to LIVE");

      assertEquals(
          SubscriptionHealth.Status.DEGRADED,
          entryFor(contributor, "leadership-bounce").orElseThrow().status(),
          "DEGRADED must survive the leadership bounce — nothing clean ran between the"
              + " catch-up dead-letter and the re-acquire, so this replica has no evidence the"
              + " hole closed");
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  /** A deliberate clean stop still drops the projection out of the health view. */
  @Test
  void continuousRunner_cleanStop_unregistersFromHealth() throws Exception {
    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);
    Projection proj = events -> {};

    var runner =
        ContinuousProjectionRunner.builder()
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .subscriptionConfig(fastConfig())
            .leadership(neverLeader())
            .atomicProcessor(TestFencingProcessor.fencingClaimOnly())
            .healthContributor(contributor)
            .build();

    Thread t =
        Thread.ofVirtual()
            .start(() -> runner.run(ProjectionName.of("orders"), proj, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      awaitStandby(runner);
      assertTrue(entryFor(contributor, "orders").isPresent(), "registered while standing by");
    } finally {
      runner.close();
      t.join(TimeUnit.SECONDS.toMillis(3));
    }

    assertTrue(
        entryFor(contributor, "orders").isEmpty(),
        "a clean stop must unregister — a stopped projection is not a frozen one");
    assertEquals(SubscriptionHealth.Status.UP, contributor.overallStatus());
  }

  /**
   * Verifies that a ScheduledProjectionRunner projection under the HALT strategy that halts (state
   * ERROR after the first failed chunk) reports DOWN — not merely DEGRADED via a below-threshold
   * error count — and STAYS DOWN after the projection thread has exited.
   */
  @Test
  void scheduledRunner_haltStrategy_reportsDownAndStaysDownAfterHalt() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("sched-halt"));
    eventStore.append(s, List.of(envelope(s, 1, "poison")), Version.initial());

    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);
    Projection poison =
        events -> {
          throw new RuntimeException("boom");
        };

    var runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .healthContributor(contributor)
            // HALT now retries the same chunk up to maxTransientRetries before
            // halting;
            // shrink the bound + backoff so the terminal HALT (state ERROR, loop exit) is reached
            // quickly. Fire every second.
            .maxTransientRetries(1)
            .drainRetryBackoff(Duration.ofMillis(2), Duration.ofMillis(10))
            .register(
                "sched-halt",
                poison,
                "* * * * * *",
                ProjectionErrorStrategy.HALT,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
      boolean down = false;
      while (System.nanoTime() < deadline) {
        if (contributor.overallStatus() == SubscriptionHealth.Status.DOWN) {
          down = true;
          break;
        }
        Thread.sleep(50);
      }
      assertTrue(down, "a halted (HALT) scheduled projection must report overallStatus DOWN");

      // The runLoop has broken and its thread is exiting; the terminal DOWN survives.
      Thread.sleep(300);
      assertEquals(
          SubscriptionHealth.Status.DOWN,
          contributor.overallStatus(),
          "overallStatus must STAY DOWN after the scheduled projection thread exits");
    } finally {
      runner.stop();
    }
  }

  /**
   * A live subscription that reports it is not running as soon as it starts, driving the runner's
   * "live subscription died unexpectedly" permanent-ERROR exit — the same {@code state=ERROR}
   * finally path a HALT exhaustion takes, without the impractically long wait for 50 real errors.
   */
  private static final class DeadSubscription implements SubscriptionLifecycle {
    @Override
    public void start() {}

    @Override
    public boolean isRunning() {
      return false;
    }

    @Override
    public void close() {}

    @Override
    public void pause() {}

    @Override
    public void resume() {}

    @Override
    public SubscriptionLifecycleState state() {
      return SubscriptionLifecycleState.RUNNING;
    }
  }

  @Test
  void scheduledRunner_withPoisonProjection_reportsErrors() throws Exception {
    StreamId s = StreamId.of(TYPE, AggregateId.of("sched"));
    eventStore.append(s, List.of(envelope(s, 1, "poison")), Version.initial());

    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore, 100);

    Projection poison =
        events -> {
          throw new RuntimeException("boom");
        };

    var runner =
        ScheduledProjectionRunner.builder()
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .eventStore(eventStore)
            .offsetStore(offsetStore)
            .fetchSize(10)
            .batchSize(10)
            .healthContributor(contributor)
            // Fire every second so the first tick lands within the test window.
            .register(
                "sched",
                poison,
                "* * * * * *",
                ProjectionErrorStrategy.SKIP,
                AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    runner.start();
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(6);
    int errors = 0;
    while (System.nanoTime() < deadline) {
      var healths = contributor.health();
      if (!healths.isEmpty() && healths.getFirst().errorCount() > 0) {
        errors = healths.getFirst().errorCount();
        break;
      }
      Thread.sleep(50);
    }
    runner.stop();

    assertTrue(errors > 0, "scheduled poison projection should accrue errors in the contributor");
    assertTrue(
        contributor.health().isEmpty(), "scheduled projection should be unregistered after stop");
  }
}
