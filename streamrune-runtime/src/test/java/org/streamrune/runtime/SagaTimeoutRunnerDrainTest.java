package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStepLockFixture.TIMEOUT;
import static org.streamrune.runtime.SagaStepLockFixture.TYPE;
import static org.streamrune.runtime.SagaStepLockFixture.await;
import static org.streamrune.runtime.SagaStepLockFixture.awaitUntil;
import static org.streamrune.runtime.SagaStepLockFixture.onAnotherThread;
import static org.streamrune.runtime.SagaStepLockFixture.outcomeOf;
import static org.streamrune.runtime.SagaStepLockFixture.park;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Release;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.State;
import org.streamrune.test.ForwardingSagaStore;

/**
 * One poll cycle of {@link SagaTimeoutRunner} queries again at once while each batch comes back
 * full and acted on a saga, and ends on a partial batch, a batch with no progress, its drain
 * budget, a stop, or a lost lease.
 */
@Timeout(60)
class SagaTimeoutRunnerDrainTest {

  private static final String LEASE = "saga:" + TYPE.value();

  private final Logger log = (Logger) LoggerFactory.getLogger(SagaTimeoutRunner.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private Level levelBefore;

  @BeforeEach
  void captureTheLog() {
    levelBefore = log.getLevel();
    log.setLevel(Level.INFO);
    appender.start();
    log.addAppender(appender);
  }

  @AfterEach
  void releaseTheLog() {
    log.detachAppender(appender);
    log.setLevel(levelBefore);
  }

  private List<String> infoLines() {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.INFO)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }

  private static SagaTimeoutRunner.Builder<State> builder(SagaStepLockFixture p, int batchSize) {
    return SagaTimeoutRunner.<State>builder()
        .decider(p.saga)
        .sagaStore(p.sagaStore)
        .commandBus(p.bus)
        .sagaType(TYPE)
        .clock(p.clock)
        .sagaRunner(p.runner)
        .batchSize(batchSize);
  }

  /** Opens the sagas of orders 1..n one second apart, then moves past every deadline. */
  private static void dueSagas(SagaStepLockFixture p, int n) {
    for (int order = 1; order <= n; order++) {
      p.deliver(p.append(new Opened(String.valueOf(order))));
      p.clock.advance(Duration.ofSeconds(1));
    }
    p.clock.advance(TIMEOUT);
    p.sagaStore.timeoutQueryLimits.clear();
  }

  private static long untouched(SagaStepLockFixture p, int n) {
    long count = 0;
    for (int order = 1; order <= n; order++) {
      if (p.row(String.valueOf(order)).episodeVersion() == null) {
        count++;
      }
    }
    return count;
  }

  @Test
  void aFullBatchThatClaimed_isFollowedAtOnceByTheNextBatch() {
    try (var p = new SagaStepLockFixture()) {
      dueSagas(p, 5);

      builder(p, 2).build().pollOnce();

      assertThat(untouched(p, 5)).isZero();
      for (int order = 1; order <= 5; order++) {
        assertThat(p.row(String.valueOf(order)).status()).isEqualTo(SagaStatus.COMPENSATED);
      }
      assertThat(p.sagaStore.timeoutQueryLimits).containsExactly(2, 2, 2);
    }
  }

  @Test
  void aPartialBatch_endsTheCycle() {
    try (var p = new SagaStepLockFixture()) {
      dueSagas(p, 1);

      builder(p, 2).build().pollOnce();

      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.sagaStore.timeoutQueryLimits).containsExactly(2);
    }
  }

  @Test
  void aBatchWhoseSagasAllFailed_endsTheCycle() {
    try (var p = new SagaStepLockFixture()) {
      dueSagas(p, 4);
      var reads = new AtomicLong();
      var queries = new CopyOnWriteArrayList<Integer>();
      SagaStore failingReads =
          new ForwardingSagaStore(p.rows) {
            @Override
            public <S extends SagaState> Optional<LoadedSaga<S>> load(
                SagaId id, SagaType type, Class<S> stateType) {
              reads.incrementAndGet();
              throw new IllegalStateException("the saga store is unreachable");
            }

            @Override
            public List<SagaId> findTimedOut(SagaType type, Instant cutoff, int limit) {
              queries.add(limit);
              return super.findTimedOut(type, cutoff, limit);
            }
          };
      var timeouts =
          SagaTimeoutRunner.<State>builder()
              .decider(p.saga)
              .sagaStore(failingReads)
              .commandBus(p.bus)
              .sagaType(TYPE)
              .clock(p.clock)
              .stepLock(p.lock)
              .batchSize(2)
              .build();

      timeouts.pollOnce();

      assertThat(queries).as("one full batch, no progress: no second query").containsExactly(2);
      assertThat(reads.get()).isEqualTo(2);
      assertThat(untouched(p, 4)).isEqualTo(4);
    }
  }

  @Test
  void aBatchOfSagasTheCycleHasSeen_endsTheCycle() {
    try (var p = new SagaStepLockFixture()) {
      dueSagas(p, 2);
      // Both compensations fail transiently: the episodes stay re-picks past their deadline.
      p.refund.failure = () -> new IllegalStateException("connection reset");
      builder(p, 2).build().pollOnce();
      p.clock.advance(TIMEOUT.plusSeconds(1));
      p.sagaStore.timeoutQueryLimits.clear();
      int releasesBefore = p.storedOrder("1").size();

      builder(p, 2).build().pollOnce();

      assertThat(p.sagaStore.timeoutQueryLimits)
          .as(
              "the re-picks are resumed once; the next batch finds only them, reads past them,"
                  + " finds nothing more, and the cycle ends")
          .containsExactly(2, 2, 6);
      assertThat(p.storedOrder("1")).hasSize(releasesBefore);
      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATING);
    }
  }

  @Test
  void theDrainBudget_endsTheCycle_andIsLoggedOnTheFirstAndEveryTenthCycleThatReachesIt() {
    try (var p = new SagaStepLockFixture()) {
      dueSagas(p, 12);
      var now = new AtomicLong();
      // Every read of the ticker is one whole budget later than the one before.
      LongSupplier ticker = () -> now.getAndAdd(Duration.ofMinutes(10).toNanos());
      var timeouts = builder(p, 1).pollInterval(Duration.ofMinutes(1)).drainTicker(ticker).build();

      timeouts.pollOnce();

      assertThat(p.sagaStore.timeoutQueryLimits).containsExactly(1);
      assertThat(untouched(p, 12)).isEqualTo(11);
      assertThat(infoLines())
          .singleElement()
          .satisfies(line -> assertThat(line).contains("drain budget of 10 poll intervals"));

      for (int cycle = 2; cycle <= 10; cycle++) {
        timeouts.pollOnce();
      }
      assertThat(untouched(p, 12)).isEqualTo(2);
      assertThat(infoLines()).hasSize(2);
      assertThat(infoLines().getLast()).contains("10 consecutive poll cycle(s)");
    }
  }

  @Test
  void aCloseDuringTheDrain_startsNoFurtherBatch() {
    try (var p = new SagaStepLockFixture()) {
      dueSagas(p, 3);
      var timeouts = builder(p, 1).pollInterval(Duration.ofHours(1)).build();
      var atTheFirstCompensation = new CountDownLatch(1);
      var interrupted = new AtomicBoolean();
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Release release && release.orderId().equals("1")) {
              atTheFirstCompensation.countDown();
              long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
              while (!Thread.currentThread().isInterrupted() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
              }
              interrupted.set(Thread.currentThread().isInterrupted());
            }
          };

      timeouts.start();
      await(atTheFirstCompensation);
      timeouts.close();

      assertThat(interrupted).as("close() interrupts the thread acting on the saga").isTrue();
      assertThat(timeouts.isAlive()).isFalse();
      assertThat(timeouts.isStarted()).isFalse();
      assertThat(p.sagaStore.timeoutQueryLimits).containsExactly(1);
      assertThat(untouched(p, 3)).isEqualTo(2);
    }
  }

  @Test
  void aLeaseLostDuringTheDrain_startsNoFurtherBatch() {
    var leadership = new RecordingLeadership();
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isTrue();
      dueSagas(p, 3);
      var timeouts = builder(p, 1).build();
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Release release && release.orderId().equals("1")) {
              leadership.revoke(LEASE);
            }
          };

      timeouts.pollOnce();

      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.sagaStore.timeoutQueryLimits).containsExactly(1);
      assertThat(untouched(p, 3)).isEqualTo(2);
      anchor.close();
    }
  }

  /**
   * Saga 1 was passed over on three polls while its step was in flight, and its step has ended: the
   * next poll would claim it in its page of sagas to wait for, after its last batch. The lease is
   * lost during that last batch, a partial one, so the page is not run.
   */
  @Test
  void aLeaseLostDuringTheLastBatch_skipsThePageOfSagasToWaitFor() {
    var leadership = new RecordingLeadership();
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isTrue();
      var timeouts = builder(p, 10).build();
      var release = new CountDownLatch(1);
      var reached = new CountDownLatch(1);
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Reserve reserve && reserve.orderId().equals("1")) {
              reached.countDown();
              park(release);
            }
          };
      var opened = p.append(new Opened("1"));
      var step = onAnotherThread(() -> p.deliver(opened));
      await(reached);
      p.clock.advance(TIMEOUT.plusSeconds(1));
      for (int poll = 0; poll < SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING; poll++) {
        timeouts.pollOnce();
      }
      assertThat(timeouts.deferralsOf(SagaStepLockFixture.sagaOf("1")))
          .isEqualTo(SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING);
      release.countDown();
      assertThat(outcomeOf(step)).isNull();
      p.deliver(p.append(new Opened("2")));
      p.clock.advance(TIMEOUT.plusSeconds(1));
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Release r && r.orderId().equals("2")) {
              leadership.revoke(LEASE);
            }
          };

      timeouts.pollOnce();

      assertThat(p.row("2").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.row("1").episodeVersion()).as("not claimed after the lease was lost").isNull();
      assertThat(timeouts.claimsDeferredPastTheWait()).isZero();
      anchor.close();
    }
  }

  @Test
  void aSagaWhoseDeadlinePassesDuringTheCycle_isClaimedInIt() {
    try (var p = new SagaStepLockFixture()) {
      dueSagas(p, 2);
      p.deliver(p.append(new Opened("3")));
      // Saga 3's deadline passes while the cycle's first batch dispatches.
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Release release && release.orderId().equals("1")) {
              p.clock.advance(TIMEOUT.plusSeconds(1));
            }
          };

      builder(p, 1).build().pollOnce();

      awaitUntil("saga 3 is claimed", () -> p.row("3").episodeVersion() != null);
      assertThat(p.row("3").status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }
}
