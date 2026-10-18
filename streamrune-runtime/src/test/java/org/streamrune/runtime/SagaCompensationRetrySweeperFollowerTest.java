package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaTimeoutRunnerFollowerTest.statusOf;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.runtime.BackgroundRelayHealthContributor.Status;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.State;

/**
 * A compensation-retry sweeper built from a saga runner that carries a lease follows that lease
 * instead of taking a lease of its own; a sweeper of a runner without one keeps its own lease.
 */
class SagaCompensationRetrySweeperFollowerTest {

  private static final String LEASE = "saga:" + SagaStepLockFixture.TYPE.value();
  private static final String OWN_LEASE =
      "saga-compensation-retry:" + SagaStepLockFixture.TYPE.value();

  private final RecordingLeadership leadership = new RecordingLeadership();
  private final Logger log = (Logger) LoggerFactory.getLogger(SagaCompensationRetrySweeper.class);
  private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
  private Level levelBefore;

  @BeforeEach
  void captureTheSweepersLog() {
    levelBefore = log.getLevel();
    log.setLevel(Level.INFO);
    appender.start();
    log.addAppender(appender);
  }

  @AfterEach
  void releaseTheSweepersLog() {
    log.detachAppender(appender);
    log.setLevel(levelBefore);
  }

  @Test
  void aSweeperOfAGatedRunnerFollowsItsLease_andNeverAcquiresOrResigns() {
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ofHours(1))) {
      var store = new CountingSagaStore(p.sagaStore);
      SagaCompensationRetrySweeper<State> sweeper = sweeper(p, store, leadership);

      sweeper.sweepOnce();
      sweeper.close();

      assertThat(store.findCompensatingCalls).isZero();
      assertThat(leadership.tryAcquires(LEASE)).isZero();
      assertThat(leadership.resigns(LEASE)).isZero();
      assertThat(leadership.tryAcquires(OWN_LEASE)).isZero();
      assertThat(leadership.resigns(OWN_LEASE)).isZero();
    }
  }

  @Test
  void aLeadingFollowerReDrives() {
    leadership.grant(LEASE);
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isTrue();
      compensatingRowDue(p);

      p.sweeper.sweepOnce();

      assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.sweeper.leaseUnanchored()).isFalse();
      anchor.close();
    }
  }

  @Test
  void anUngatedRunnerWithLeadership_anchorsItsOwnLease() {
    leadership.grant(OWN_LEASE);
    try (var p = new SagaStepLockFixture()) {
      var store = new CountingSagaStore(p.sagaStore);
      SagaCompensationRetrySweeper<State> sweeper = sweeper(p, store, leadership);

      sweeper.sweepOnce();
      sweeper.sweepOnce();
      sweeper.close();

      assertThat(leadership.tryAcquires(OWN_LEASE)).isEqualTo(2);
      assertThat(leadership.resigns(OWN_LEASE)).isEqualTo(1);
      assertThat(store.findCompensatingCalls).isEqualTo(2);
      assertThat(leadership.tryAcquires(LEASE)).isZero();
      assertThat(sweeper.leaseUnanchored()).isFalse();
    }
  }

  @Test
  void theGaugesAreSampledOnEveryReplica() {
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      SubscriptionLease.Anchor anchor = p.runner.lease().anchor();
      assertThat(anchor.lead(Duration.ZERO)).isFalse();
      List<Long> backlog = new CopyOnWriteArrayList<>();
      var store = new CountingSagaStore(p.sagaStore);
      SagaCompensationRetrySweeper<State> sweeper =
          SagaCompensationRetrySweeper.<State>builder()
              .sagaRunner(p.runner)
              .sagaStore(store)
              .leadership(leadership)
              .clock(p.clock)
              .metrics(
                  new StreamRuneMetrics() {
                    @Override
                    public void recordSagaCompensatingBacklog(String sagaType, long count) {
                      backlog.add(count);
                    }
                  })
              .build();

      sweeper.sweepOnce();
      sweeper.sweepOnce();

      assertThat(backlog).hasSize(2);
      assertThat(store.findCompensatingCalls).isZero();
      anchor.close();
    }
  }

  @Test
  void withinTheGraceItSaysSoOnceAtInfo() {
    try (var p = new SagaStepLockFixture(leadership, Duration.ofHours(1))) {
      p.sweeper.sweepOnce();
      p.sweeper.sweepOnce();

      assertThat(lines(Level.INFO))
          .singleElement()
          .satisfies(
              line ->
                  assertThat(line)
                      .contains(
                          "compensation retries are run by the process that delivers its events"));
      assertThat(lines(Level.WARN)).isEmpty();
      assertThat(p.sweeper.leaseUnanchored()).isFalse();
    }
  }

  @Test
  void pastTheGraceWithNoAnchorEver_itWarnsOnce_andItsHealthIsDegraded() {
    try (var p = new SagaStepLockFixture(leadership, Duration.ZERO)) {
      var relay = new BackgroundRelayHealthContributor();
      relay.registerSagaCompensationRetrySweeper(p.sweeper);
      p.sweeper.start();
      try {
        p.sweeper.sweepOnce();
        p.sweeper.sweepOnce();

        assertThat(lines(Level.WARN))
            .singleElement()
            .satisfies(line -> assertThat(line).contains("has anchored its lease").contains(LEASE));
        assertThat(p.sweeper.leaseUnanchored()).isTrue();
        assertThat(statusOf(relay, "saga-compensation-retry:" + SagaStepLockFixture.TYPE.value()))
            .isEqualTo(Status.DEGRADED);
      } finally {
        p.sweeper.close();
      }
    }
  }

  @Test
  void anAnchorGraceIsRequired() {
    try (var p = new SagaStepLockFixture()) {
      org.assertj.core.api.Assertions.assertThatThrownBy(
              () -> SagaCompensationRetrySweeper.<State>builder().anchorGrace(null))
          .isInstanceOf(NullPointerException.class)
          .hasMessage("anchorGrace is required");
    }
  }

  // ---- helpers ----

  /** A saga of order 1 left COMPENSATING by a failed release, due for a re-drive. */
  private static void compensatingRowDue(SagaStepLockFixture p) {
    p.deliver(p.append(new Opened("1")));
    p.release.failure = () -> new IllegalStateException("the release service is down");
    p.timeOut();
    assertThat(p.row("1").status()).isEqualTo(SagaStatus.COMPENSATING);
    p.release.failure = null;
    p.clock.advance(Duration.ofMinutes(5));
  }

  private static SagaCompensationRetrySweeper<State> sweeper(
      SagaStepLockFixture p, CountingSagaStore store, RecordingLeadership leadership) {
    return SagaCompensationRetrySweeper.<State>builder()
        .sagaRunner(p.runner)
        .sagaStore(store)
        .leadership(leadership)
        .clock(p.clock)
        .build();
  }

  private List<String> lines(Level level) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == level)
        .map(ILoggingEvent::getFormattedMessage)
        .filter(m -> m.contains("anchored"))
        .toList();
  }

  /** Counts the sweeps' reads of the compensating population. */
  static final class CountingSagaStore extends ForwardingSagaStore {
    volatile int findCompensatingCalls;

    CountingSagaStore(org.streamrune.core.saga.SagaStore delegate) {
      super(delegate);
    }

    @Override
    public List<CompensatingSaga> findCompensating(
        org.streamrune.core.types.SagaType type, java.time.Instant dueBefore, int limit) {
      findCompensatingCalls++;
      return super.findCompensating(type, dueBefore, limit);
    }
  }
}
