package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStepLockFixture.TYPE;
import static org.streamrune.runtime.SagaStepLockFixture.await;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.LoggerFactory;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.SagaType;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Refund;
import org.streamrune.runtime.SagaStepLockFixture.State;
import org.streamrune.test.ForwardingSagaStore;

/**
 * One cycle of {@link SagaCompensationRetrySweeper} queries again at once while each batch comes
 * back full and ended an episode, and ends on a partial batch, a batch whose every re-drive failed
 * transiently, its drain budget, a stop, or a lost lease.
 */
@Timeout(60)
class SagaCompensationRetrySweeperDrainTest {

  private static final Duration RETRY_INTERVAL = Duration.ofSeconds(30);
  private static final String CONSUMER =
      SagaCompensationRetrySweeper.CONSUMER_NAME_PREFIX + TYPE.value();

  private final Logger log = (Logger) LoggerFactory.getLogger(SagaCompensationRetrySweeper.class);
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

  private List<String> budgetLines() {
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.INFO)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(line -> line.contains("drain budget"))
        .toList();
  }

  /** A store that records the limit of every {@code findCompensating}. */
  private static final class CountingStore extends ForwardingSagaStore {
    final List<Integer> queries = new CopyOnWriteArrayList<>();

    CountingStore(SagaStore delegate) {
      super(delegate);
    }

    @Override
    public List<CompensatingSaga> findCompensating(
        SagaType type, Instant updatedBefore, int limit) {
      queries.add(limit);
      return super.findCompensating(type, updatedBefore, limit);
    }
  }

  /**
   * Orders 1..n end up {@code COMPENSATING}, their refund failing transiently, idle for longer than
   * the retry interval.
   */
  private static void stuckEpisodes(SagaStepLockFixture p, int n) {
    for (int order = 1; order <= n; order++) {
      p.deliver(p.append(new Opened(String.valueOf(order))));
      p.clock.advance(Duration.ofSeconds(1));
    }
    p.refund.failure = () -> new IllegalStateException("connection reset");
    p.timeOut();
    for (int order = 1; order <= n; order++) {
      assertThat(p.row(String.valueOf(order)).status()).isEqualTo(SagaStatus.COMPENSATING);
    }
    p.clock.advance(RETRY_INTERVAL.plusSeconds(1));
  }

  private static SagaCompensationRetrySweeper.Builder<State> sweeper(
      SagaStepLockFixture p, SagaStore store, int batchSize) {
    return SagaCompensationRetrySweeper.<State>builder()
        .sagaRunner(p.runner)
        .sagaStore(store)
        .retryInterval(RETRY_INTERVAL)
        .giveUpAfter(Duration.ofHours(1))
        .clock(p.clock)
        .batchSize(batchSize);
  }

  private static int inStatus(SagaStepLockFixture p, int n, SagaStatus status) {
    int count = 0;
    for (int order = 1; order <= n; order++) {
      if (p.row(String.valueOf(order)).status() == status) {
        count++;
      }
    }
    return count;
  }

  @Test
  void aFullBatchThatEndedEpisodes_isFollowedAtOnceByTheNextBatch() {
    try (var p = new SagaStepLockFixture()) {
      stuckEpisodes(p, 5);
      p.refund.failure = null;
      var store = new CountingStore(p.sagaStore);

      sweeper(p, store, 2).build().sweepOnce();

      assertThat(inStatus(p, 5, SagaStatus.COMPENSATED)).isEqualTo(5);
      assertThat(store.queries).containsExactly(2, 2, 2);
    }
  }

  @Test
  void aPartialBatch_endsTheCycle() {
    try (var p = new SagaStepLockFixture()) {
      stuckEpisodes(p, 1);
      p.refund.failure = null;
      var store = new CountingStore(p.sagaStore);

      sweeper(p, store, 2).build().sweepOnce();

      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(store.queries).containsExactly(2);
    }
  }

  @Test
  void aBatchWhoseEveryReDriveFailedTransiently_endsTheCycle_andNoSagaIsReDrivenTwice() {
    try (var p = new SagaStepLockFixture()) {
      stuckEpisodes(p, 5);
      var store = new CountingStore(p.sagaStore);
      var refunds = new AtomicInteger();
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Refund) {
              refunds.incrementAndGet();
            }
          };

      sweeper(p, store, 2).build().sweepOnce();

      assertThat(store.queries).containsExactly(2);
      assertThat(refunds.get()).isEqualTo(2);
      assertThat(inStatus(p, 5, SagaStatus.COMPENSATING)).isEqualTo(5);
    }
  }

  @Test
  void aSagaLeftCompensatingEarlierInTheCycle_isNotReDrivenAgainInIt() {
    try (var p = new SagaStepLockFixture()) {
      stuckEpisodes(p, 3);
      // Order 1's refund keeps failing, the others' succeed: the first batch ends an episode, and
      // the next query returns order 1 again, the oldest row.
      p.refund.failure = null;
      var refundsOfOrder1 = new AtomicInteger();
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Refund refund && refund.orderId().equals("1")) {
              refundsOfOrder1.incrementAndGet();
              throw new IllegalStateException("connection reset");
            }
          };
      var store = new CountingStore(p.sagaStore);

      sweeper(p, store, 2).build().sweepOnce();

      assertThat(refundsOfOrder1.get()).isEqualTo(1);
      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(p.row("2").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.row("3").status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void theDrainBudget_endsTheCycle_andIsLoggedAtInfo() {
    try (var p = new SagaStepLockFixture()) {
      stuckEpisodes(p, 4);
      p.refund.failure = null;
      var store = new CountingStore(p.sagaStore);
      var now = new AtomicLong();
      var sweeper =
          sweeper(p, store, 1)
              .drainTicker(() -> now.getAndAdd(RETRY_INTERVAL.multipliedBy(10).toNanos()))
              .build();

      sweeper.sweepOnce();

      assertThat(store.queries).containsExactly(1);
      assertThat(inStatus(p, 4, SagaStatus.COMPENSATED)).isEqualTo(1);
      assertThat(budgetLines())
          .singleElement()
          .satisfies(line -> assertThat(line).contains("drain budget of 10 retry intervals"));
    }
  }

  @Test
  void aCloseDuringTheDrain_startsNoFurtherBatch() {
    try (var p = new SagaStepLockFixture()) {
      stuckEpisodes(p, 3);
      p.refund.failure = null;
      var store = new CountingStore(p.sagaStore);
      var sweeper = sweeper(p, store, 1).retryInterval(RETRY_INTERVAL).build();
      var atTheFirstRefund = new CountDownLatch(1);
      var interrupted = new AtomicBoolean();
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Refund refund && refund.orderId().equals("1")) {
              atTheFirstRefund.countDown();
              long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
              while (!Thread.currentThread().isInterrupted() && System.nanoTime() < deadline) {
                Thread.onSpinWait();
              }
              interrupted.set(Thread.currentThread().isInterrupted());
            }
          };

      sweeper.start();
      await(atTheFirstRefund);
      sweeper.close();

      assertThat(interrupted).isTrue();
      assertThat(sweeper.isAlive()).isFalse();
      assertThat(store.queries).containsExactly(1);
      assertThat(p.row("2").status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(p.row("3").status()).isEqualTo(SagaStatus.COMPENSATING);
    }
  }

  @Test
  void aLeaseLostDuringTheDrain_startsNoFurtherBatch() {
    try (var p = new SagaStepLockFixture()) {
      stuckEpisodes(p, 3);
      p.refund.failure = null;
      var leadership = new RecordingLeadership();
      leadership.grant(CONSUMER);
      var store = new CountingStore(p.sagaStore);
      var sweeper = sweeper(p, store, 1).leadership(leadership).build();
      p.bus.beforeEveryCommand =
          command -> {
            if (command instanceof Refund refund && refund.orderId().equals("1")) {
              leadership.revoke(CONSUMER);
            }
          };

      sweeper.sweepOnce();

      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(store.queries).containsExactly(1);
      assertThat(inStatus(p, 3, SagaStatus.COMPENSATING)).isEqualTo(2);
    }
  }
}
