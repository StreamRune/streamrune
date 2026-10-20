package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.runtime.BackgroundRelayHealthContributor.Status;

class BackgroundRelayHealthContributorTest {

  /** Adjustable liveness source so every status branch is exercised deterministically. */
  private static final class FakeSource
      implements BackgroundRelayHealthContributor.RelayStatusSource {
    private final String name;
    boolean started;
    boolean alive;
    int consecutiveFailures;

    FakeSource(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public boolean started() {
      return started;
    }

    @Override
    public boolean alive() {
      return alive;
    }

    @Override
    public int consecutiveFailures() {
      return consecutiveFailures;
    }
  }

  @Test
  void statusMapping() {
    // Not started → UP regardless of alive/failures (a not-started relay is not a problem).
    assertEquals(Status.UP, BackgroundRelayHealthContributor.statusFor(false, false, 0));
    assertEquals(Status.UP, BackgroundRelayHealthContributor.statusFor(false, false, 5));
    // Started + alive + no failures → UP.
    assertEquals(Status.UP, BackgroundRelayHealthContributor.statusFor(true, true, 0));
    // Started + alive + failures → DEGRADED.
    assertEquals(Status.DEGRADED, BackgroundRelayHealthContributor.statusFor(true, true, 1));
    // Started + dead thread → DOWN (even if failures is 0).
    assertEquals(Status.DOWN, BackgroundRelayHealthContributor.statusFor(true, false, 0));
    assertEquals(Status.DOWN, BackgroundRelayHealthContributor.statusFor(true, false, 3));
  }

  @Test
  void emptyContributorIsUp() {
    var contributor = new BackgroundRelayHealthContributor();
    assertEquals(Status.UP, contributor.overallStatus());
    assertTrue(contributor.components().isEmpty());
  }

  @Test
  void nullRegistrationsAreIgnored() {
    var contributor = new BackgroundRelayHealthContributor();
    contributor.registerOutboxPoller(null);
    contributor.registerDlqRetryRunner(null);
    assertTrue(contributor.components().isEmpty());
    assertEquals(Status.UP, contributor.overallStatus());
  }

  @Test
  void overallStatusIsWorstOfComponents() {
    var contributor = new BackgroundRelayHealthContributor();
    var relay = new FakeSource("outbox-relay");
    var dlq = new FakeSource("dead-letter-retry");
    contributor.register(relay);
    contributor.register(dlq);

    // Both healthy.
    relay.started = true;
    relay.alive = true;
    dlq.started = true;
    dlq.alive = true;
    assertEquals(Status.UP, contributor.overallStatus());

    // One degraded (in backoff) → overall DEGRADED.
    relay.consecutiveFailures = 4;
    assertEquals(Status.DEGRADED, contributor.overallStatus());

    // The other's thread dies → overall DOWN dominates.
    dlq.alive = false;
    assertEquals(Status.DOWN, contributor.overallStatus());

    var components = contributor.components();
    assertEquals(2, components.size());
    var dlqHealth =
        components.stream()
            .filter(c -> c.name().equals("dead-letter-retry"))
            .findFirst()
            .orElseThrow();
    assertEquals(Status.DOWN, dlqHealth.status());
    assertTrue(dlqHealth.started());
    assertFalse(dlqHealth.alive());
  }

  @Test
  void reportsThroughRegisteredRealRunners() {
    // Exercises the typed register* adapters against real runner instances (not started → UP).
    var contributor = new BackgroundRelayHealthContributor();
    var poller =
        OutboxPoller.builder().outboxStore(new NoOpOutboxStore()).publisher(entry -> {}).build();
    contributor.registerOutboxPoller(poller);

    assertEquals(1, contributor.components().size());
    var health = contributor.components().getFirst();
    assertEquals("outbox-relay", health.name());
    assertFalse(health.started());
    assertEquals(Status.UP, contributor.overallStatus());
  }

  @Test
  void reportsTheSseEventFeed_downOnceItsPollingThreadHasDied() {
    var store = new org.streamrune.test.InMemoryEventStore();
    var dying = new java.util.concurrent.atomic.AtomicBoolean();
    var failing = new java.util.concurrent.atomic.AtomicBoolean();
    org.streamrune.core.EventStore reads =
        new org.streamrune.core.EventStore() {
          @Override
          public org.streamrune.core.AggregateHistory load(
              org.streamrune.core.types.StreamId streamId) {
            return store.load(streamId);
          }

          @Override
          public AppendResult append(
              org.streamrune.core.types.StreamId streamId,
              java.util.List<org.streamrune.core.EventEnvelope> events,
              org.streamrune.core.types.Version expectedVersion) {
            return store.append(streamId, events, expectedVersion);
          }

          @Override
          public void saveSnapshot(
              org.streamrune.core.types.StreamId streamId,
              org.streamrune.core.types.Version version,
              org.streamrune.core.AggregateState state) {
            store.saveSnapshot(streamId, version, state);
          }

          @Override
          public boolean saveSnapshot(
              org.streamrune.core.types.StreamId streamId,
              org.streamrune.core.types.Version version,
              org.streamrune.core.AggregateState state,
              int snapshotVersion,
              long snapshotPurgeGeneration) {
            throw new UnsupportedOperationException("this test store keeps no snapshots");
          }

          @Override
          public java.util.List<org.streamrune.core.EventEnvelope> readGlobalStream(
              org.streamrune.core.types.GlobalOffset afterOffset, int maxCount) {
            if (dying.get()) {
              throw new NoClassDefFoundError("an event class is missing");
            }
            if (failing.get()) {
              throw new IllegalStateException("the database is away");
            }
            return store.readGlobalStream(afterOffset, maxCount);
          }

          @Override
          public java.util.List<org.streamrune.core.EventEnvelope> readStream(
              org.streamrune.core.types.StreamId streamId,
              org.streamrune.core.types.Version afterVersion,
              int maxCount) {
            return store.readStream(streamId, afterVersion, maxCount);
          }

          @Override
          public org.streamrune.core.types.GlobalOffset lastGlobalOffset() {
            return store.lastGlobalOffset();
          }
        };
    var contributor = new BackgroundRelayHealthContributor();
    contributor.registerSseEventFeed(null);
    assertTrue(contributor.components().isEmpty(), "a null feed is ignored");

    try (var publisher = new SseEventPublisher();
        var feed = new SseEventFeed(reads, publisher, java.time.Duration.ofMillis(20))) {
      // A client is subscribed, so the feed reads events: the reads this test fails.
      publisher.subscribe(
          org.streamrune.core.types.StreamId.of(
              org.streamrune.core.types.AggregateType.of("order"),
              org.streamrune.core.types.AggregateId.of("o-1")),
          envelope -> {});
      contributor.registerSseEventFeed(feed);
      var notStarted = contributor.components().getFirst();
      assertEquals("sse-event-feed", notStarted.name());
      assertFalse(notStarted.started());
      assertEquals(
          Status.UP, notStarted.status(), "a feed that was never started is not unhealthy");

      feed.start();
      assertEquals(Status.UP, contributor.overallStatus());

      failing.set(true);
      org.awaitility.Awaitility.await()
          .atMost(java.time.Duration.ofSeconds(5))
          .until(() -> contributor.overallStatus() == Status.DEGRADED);
      failing.set(false);
      org.awaitility.Awaitility.await()
          .atMost(java.time.Duration.ofSeconds(10))
          .until(() -> contributor.overallStatus() == Status.UP);

      dying.set(true);
      org.awaitility.Awaitility.await()
          .atMost(java.time.Duration.ofSeconds(5))
          .until(() -> contributor.overallStatus() == Status.DOWN);
      var dead = contributor.components().getFirst();
      assertTrue(dead.started());
      assertFalse(dead.alive());

      dying.set(false);
      feed.start();
      assertEquals(Status.UP, contributor.overallStatus(), "a restarted feed is healthy again");
    }
  }

  @Test
  void reportsThroughRegisteredSagaTimeoutRunner() {
    // The saga-driver typed adapter builds a saga-type-named component wired to the driver's
    // liveness accessors, so a dead saga driver surfaces DOWN (mapping proven by statusMapping).
    var contributor = new BackgroundRelayHealthContributor();
    var runner =
        SagaTimeoutRunner.<TinyState>builder()
            .stepLock(new SagaStepLock())
            .decider(new TinyDecider())
            .sagaStore(new org.streamrune.test.InMemorySagaStore())
            .commandBus(new NoOpCommandBus())
            .sagaType(org.streamrune.core.types.SagaType.fromClass(TinyState.class))
            .build();
    contributor.registerSagaTimeoutRunner(runner);
    contributor.registerSagaTimeoutRunner(null); // null is ignored
    contributor.registerSagaCompensationRetrySweeper(null); // null is ignored

    assertEquals(1, contributor.components().size());
    var health = contributor.components().getFirst();
    assertEquals(
        "saga-timeout:" + org.streamrune.core.types.SagaType.fromClass(TinyState.class).value(),
        health.name());
    assertFalse(health.started());
    assertEquals(Status.UP, contributor.overallStatus());
  }

  record TinyState(org.streamrune.core.saga.SagaStatus status)
      implements org.streamrune.core.saga.SagaState {}

  static final class TinyDecider implements org.streamrune.core.saga.SagaDecider<TinyState> {
    @Override
    public Class<TinyState> stateType() {
      return TinyState.class;
    }

    @Override
    public TinyState initialState(org.streamrune.core.saga.SagaId sagaId) {
      return new TinyState(org.streamrune.core.saga.SagaStatus.STARTED);
    }

    @Override
    public TinyState evolve(TinyState state, org.streamrune.core.EventEnvelope event) {
      return state;
    }

    @Override
    public java.util.List<org.streamrune.core.saga.SagaCommand> handle(
        TinyState state, org.streamrune.core.EventEnvelope event) {
      return java.util.List.of();
    }

    @Override
    public java.util.Optional<java.time.Duration> timeout() {
      return java.util.Optional.of(java.time.Duration.ofMinutes(5));
    }
  }

  static final class NoOpCommandBus implements org.streamrune.core.CommandBus {
    @Override
    public boolean supportsIdempotentExecution() {
      return true;
    }

    @Override
    public <C extends org.streamrune.core.Command> CommandResult execute(C command) {
      throw new UnsupportedOperationException("not invoked by a non-started runner");
    }

    @Override
    public <C extends org.streamrune.core.Command> CommandResult execute(
        C command, org.streamrune.core.types.IdempotencyKey key) {
      throw new UnsupportedOperationException("not invoked by a non-started runner");
    }
  }

  /** Minimal OutboxStore stub: never asked to do anything by a non-started poller. */
  private static final class NoOpOutboxStore implements org.streamrune.core.outbox.OutboxStore {
    @Override
    public void save(org.streamrune.core.outbox.OutboxEntry entry) {}

    @Override
    public java.util.List<org.streamrune.core.outbox.OutboxEntry> loadPending(int limit) {
      return java.util.List.of();
    }

    @Override
    public String claimedBy() {
      return "test";
    }

    @Override
    public boolean markDelivered(org.streamrune.core.outbox.OutboxEntryId id, String claimedBy) {
      return false;
    }

    @Override
    public java.util.List<org.streamrune.core.outbox.OutboxEntry> findByStatus(
        org.streamrune.core.outbox.OutboxStatus status, int limit) {
      return java.util.List.of();
    }

    @Override
    public boolean resetFailedToPending(org.streamrune.core.outbox.OutboxEntryId id) {
      return false;
    }

    @Override
    public java.util.Optional<org.streamrune.core.outbox.OutboxEntry> findById(
        org.streamrune.core.outbox.OutboxEntryId id) {
      return java.util.Optional.empty();
    }

    @Override
    public boolean skipFailed(
        org.streamrune.core.outbox.OutboxEntryId id, String skippedBy, String reason) {
      return false;
    }

    @Override
    public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
      // OutboxPoller.build() reads the mode; a stub that enforces nothing claims the mode
      // that promises nothing.
      return org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST;
    }

    @Override
    public boolean markFailed(
        org.streamrune.core.outbox.OutboxEntryId id, int attempts, String error, String claimedBy) {
      return false;
    }

    @Override
    public boolean markRetry(
        org.streamrune.core.outbox.OutboxEntryId id,
        int attempts,
        String error,
        java.time.Duration backoff,
        String claimedBy) {
      return false;
    }

    @Override
    public void delete(org.streamrune.core.outbox.OutboxEntryId id) {}

    @Override
    public java.time.Duration claimLease() {
      return java.time.Duration.ofMinutes(4);
    }
  }

  @Test
  void reportsThroughRegisteredRetentionSweepers() {
    // The four retention sweepers register through the typed adapter under their own
    // names (not started → UP), so a dead sweeper thread is visible where the other relays are.
    var contributor = new BackgroundRelayHealthContributor();
    var noopMetrics = org.streamrune.core.StreamRuneMetrics.NOOP;
    var day = java.time.Duration.ofDays(1);
    var hour = java.time.Duration.ofHours(1);
    var clock = java.time.Clock.systemUTC();
    contributor.registerRetentionSweeper(
        new DeadLetterRetentionSweeper(
            mock(org.streamrune.core.DeadLetterQueue.class), day, hour, clock, noopMetrics));
    contributor.registerRetentionSweeper(
        new InboxRetentionSweeper(
            mock(org.streamrune.core.CommandInbox.class), day, hour, clock, noopMetrics));
    contributor.registerRetentionSweeper(
        new OutboxRetentionSweeper(new NoOpOutboxStore(), day, day, hour, clock, noopMetrics));
    contributor.registerRetentionSweeper(
        new SagaDeadLetterRetentionSweeper(
            mock(org.streamrune.core.saga.SagaDeadLetterStore.class),
            day,
            hour,
            clock,
            noopMetrics));
    contributor.registerRetentionSweeper(null); // ignored, like every other register* method

    assertEquals(
        java.util.List.of(
            "dead-letter-retention-sweeper",
            "inbox-retention-sweeper",
            "outbox-retention-sweeper",
            "saga-dead-letter-retention-sweeper"),
        contributor.components().stream()
            .map(BackgroundRelayHealthContributor.ComponentHealth::name)
            .toList());
    assertTrue(
        contributor.components().stream()
            .noneMatch(BackgroundRelayHealthContributor.ComponentHealth::started));
    assertEquals(Status.UP, contributor.overallStatus());
  }

  /** A forget channel whose listening side the test switches. */
  private static final class FakeForgetSignal implements CryptoForgetSignal {
    volatile boolean subscribed;
    volatile boolean listening;

    @Override
    public void publish(String token) {}

    @Override
    public void subscribe(Consumer<String> onForget, Runnable onListening) {
      subscribed = true;
    }

    @Override
    public boolean isSubscribed() {
      return subscribed;
    }

    @Override
    public boolean isListening() {
      return listening;
    }
  }

  private static CachedCryptoEngine cachedEngine(CryptoForgetSignal signal) {
    return CachedCryptoEngine.builder()
        .delegate(mock(CryptoEngine.class))
        .forgetSignal(signal)
        .build();
  }

  @Test
  void aCryptoForgetChannelReadsDegradedWhileItDoesNotHearAndUpWhileItDoes() {
    var signal = new FakeForgetSignal();
    var contributor = new BackgroundRelayHealthContributor();
    contributor.registerCryptoEngine(cachedEngine(signal));

    var notYet = contributor.components();
    assertEquals(1, notYet.size());
    assertEquals("crypto-forget-listen", notYet.getFirst().name());
    assertEquals(Status.DEGRADED, notYet.getFirst().status());
    assertTrue(notYet.getFirst().started());
    assertFalse(notYet.getFirst().alive());
    assertEquals(Status.DEGRADED, contributor.overallStatus(), "never DOWN");

    signal.listening = true;
    var listening = contributor.components().getFirst();
    assertEquals(Status.UP, listening.status());
    assertTrue(listening.alive());

    signal.listening = false; // the LISTEN connection is lost
    assertEquals(Status.DEGRADED, contributor.components().getFirst().status());

    signal.subscribed = false; // closed
    assertEquals(Status.UP, contributor.components().getFirst().status());
  }

  @Test
  void theSameForgetChannelRegisteredTwiceIsReportedOnce() {
    var signal = new FakeForgetSignal();
    var contributor = new BackgroundRelayHealthContributor();
    contributor.registerCryptoEngine(cachedEngine(signal));
    contributor.registerCryptoForgetSignal(signal);
    contributor.registerCryptoEngine(cachedEngine(signal));

    assertEquals(1, contributor.components().size());
    assertEquals("crypto-forget-listen", contributor.components().getFirst().name());
  }

  @Test
  void differentForgetChannelsAreReportedUnderDistinctNames() {
    var first = new FakeForgetSignal();
    var second = new FakeForgetSignal();
    var third = new FakeForgetSignal();
    third.subscribed = true;
    var contributor = new BackgroundRelayHealthContributor();
    contributor.registerCryptoEngine(cachedEngine(first));
    contributor.registerCryptoEngine(cachedEngine(second));
    contributor.registerCryptoForgetSignal(third);

    var components = contributor.components();
    assertEquals(
        java.util.List.of(
            "crypto-forget-listen", "crypto-forget-listen-2", "crypto-forget-listen-3"),
        components.stream().map(BackgroundRelayHealthContributor.ComponentHealth::name).toList());

    // Each reads its own channel: one that listens does not hide the one that does not.
    second.listening = true;
    var byName = contributor.components();
    assertEquals(Status.DEGRADED, byName.get(0).status());
    assertEquals(Status.UP, byName.get(1).status());
    assertEquals(Status.DEGRADED, byName.get(2).status());
  }

  @Test
  void nothingIsReportedForAnEngineWithoutAListeningForgetChannel() {
    var contributor = new BackgroundRelayHealthContributor();
    contributor.registerCryptoEngine(null);
    contributor.registerCryptoEngine(mock(CryptoEngine.class)); // no plaintext cache
    contributor.registerCryptoEngine(cachedEngine(CryptoForgetSignal.NOOP));
    contributor.registerCryptoForgetSignal(null);
    contributor.registerCryptoForgetSignal(new FakeForgetSignal()); // nobody subscribed
    assertTrue(contributor.components().isEmpty());
    assertEquals(Status.UP, contributor.overallStatus());
  }
}
