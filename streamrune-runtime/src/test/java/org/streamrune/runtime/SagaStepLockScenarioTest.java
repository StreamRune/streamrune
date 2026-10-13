package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.streamrune.runtime.SagaStepLockFixture.TYPE;
import static org.streamrune.runtime.SagaStepLockFixture.await;
import static org.streamrune.runtime.SagaStepLockFixture.awaitUntil;
import static org.streamrune.runtime.SagaStepLockFixture.onAnotherThread;
import static org.streamrune.runtime.SagaStepLockFixture.outcomeOf;
import static org.streamrune.runtime.SagaStepLockFixture.park;
import static org.streamrune.runtime.SagaStepLockFixture.sagaOf;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.runtime.SagaDeadLetterReplayer.CompensateOutcome;
import org.streamrune.runtime.SagaStepLockFixture.Advanced;
import org.streamrune.runtime.SagaStepLockFixture.Completed;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.ProcessDeath;
import org.streamrune.runtime.SagaStepLockFixture.Refund;
import org.streamrune.runtime.SagaStepLockFixture.Released;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.Ship;
import org.streamrune.test.InMemoryEventSubscription;

/**
 * Within one process a compensation claim that has no event of its own waits for a step in flight
 * on the same saga, and two steps of one saga run one after the other. Each case asserts the stored
 * order — a forward command is stored before its saga's compensation, never after it — and the
 * saga's final row.
 *
 * <p>The process is {@link SagaStepLockFixture}: the real runner, timeout runner, executor, bus and
 * stores, sharing the runner's {@link SagaStepLock}. A second path is placed at a chosen point of a
 * step by a hook, on the step's own thread or on a second thread held there by latches. Where a
 * test waits for a second thread to be blocked on the lock it also accepts that thread having run
 * on to its own forward command: without the lock that is what happens, and the assertions on the
 * stored order then fail on the order itself.
 */
class SagaStepLockScenarioTest {

  private static final String ORDER = "1";

  // ---- the timeout runner's claim and a step in flight ----

  @Test
  void aStartStepIsInFlightOnAnotherThread_theTimeoutClaimIsNotMade_andIsMadeOnTheNextPoll() {
    try (var p = new SagaStepLockFixture()) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
      await(stepAtItsCommand);

      p.timeOut();

      assertThat(p.storedOrder(ORDER)).as("nothing is compensated under the step").isEmpty();
      assertThat(p.row(ORDER).status()).as("the row is as the step left it").isEqualTo(RUNNING);
      assertThat(p.row(ORDER).episodeVersion()).isNull();
      assertThat(p.timeoutRunner.deferralsOf(sagaOf(ORDER))).isEqualTo(1);

      letTheStepGo.countDown();
      assertThat(outcomeOf(step)).as("the step's write is not refused").isNull();
      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved");
      assertThat(p.row(ORDER).genesisApplied()).isTrue();

      p.timeoutRunner.pollOnce();

      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.timeoutRunner.deferralsOf(sagaOf(ORDER))).isZero();
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void aStartStepIsInFlightOnTheClaimingThread_theTimeoutClaimIsNotMade() {
    try (var p = new SagaStepLockFixture()) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      // The poll runs inside the step, on the step's own thread: a thread cannot wait for itself.
      p.bus.runOnceBefore(Reserve.class, p::timeOut);

      p.deliver(opened);

      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved");
      assertThat(p.row(ORDER).status()).isEqualTo(RUNNING);
      assertThat(p.row(ORDER).genesisApplied()).isTrue();

      p.timeoutRunner.pollOnce();

      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void aCorrelatedStepIsInFlight_theTimeoutClaimIsNotMade_andCompensatesAStateThatIncludesIt() {
    try (var p = new SagaStepLockFixture()) {
      p.deliver(p.append(new Opened(ORDER)));
      EventEnvelope advanced = p.append(new Advanced(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Ship.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(advanced));
      await(stepAtItsCommand);

      p.timeOut();

      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved");
      assertThat(p.row(ORDER).status()).isEqualTo(RUNNING);

      letTheStepGo.countDown();
      assertThat(outcomeOf(step)).isNull();
      assertThat(p.row(ORDER).lastAppliedOffset()).isEqualTo(advanced.globalOffset().value());

      p.timeoutRunner.pollOnce();

      assertThat(p.storedOrder(ORDER))
          .containsExactly("Reserved", "Shipped", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void theOutageEndsInTheRedeliveryThatStraddlesTheDeadline_theClaimWaitsForThatRedelivery() {
    try (var p = new SagaStepLockFixture()) {
      p.reserve.failure = () -> new IllegalStateException(new SQLException("connection reset"));
      EventEnvelope opened = p.append(new Opened(ORDER));
      assertThatThrownBy(() -> p.deliver(opened)).isInstanceOf(IllegalStateException.class);
      assertThat(p.lock.sagasInFlight()).as("a failed attempt holds nothing").isZero();

      p.reserve.failure = null;
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> redelivery = onAnotherThread(() -> p.deliver(opened));
      await(stepAtItsCommand);

      p.timeOut();
      assertThat(p.storedOrder(ORDER)).isEmpty();

      letTheStepGo.countDown();
      assertThat(outcomeOf(redelivery)).isNull();
      p.timeoutRunner.pollOnce();

      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void theDeadlinePassesBetweenTwoAttemptsOfAnOutage_theClaimIsMade_andTheRedeliveryResumesIt() {
    try (var p = new SagaStepLockFixture()) {
      p.reserve.failure = () -> new IllegalStateException(new SQLException("connection reset"));
      EventEnvelope opened = p.append(new Opened(ORDER));
      assertThatThrownBy(() -> p.deliver(opened)).isInstanceOf(IllegalStateException.class);
      p.reserve.failure = null;

      // No attempt is in flight: the saga is bounded by its deadline, as without the lock.
      p.timeOut();

      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);

      p.deliver(opened);

      assertThat(p.storedOrder(ORDER))
          .as("the redelivery meets a terminal row and dispatches no forward command")
          .containsExactly("Released", "Refunded");
    }
  }

  // ---- two steps of one saga ----

  @Test
  void aSecondDeliveryOfTheEventArrivesUnderAStepWhoseCommandIsRejected_itWaitsForThatStep() {
    try (var p = new SagaStepLockFixture()) {
      p.reserve.failure = () -> new IllegalArgumentException("out of stock");
      EventEnvelope opened = p.append(new Opened(ORDER));
      var twinAtItsCommand = new CountDownLatch(1);
      var letTheTwinGo = new CountDownLatch(1);
      var twin = new AtomicReference<CompletableFuture<Void>>();
      // The first delivery, at its command: a second delivery of the same event starts on another
      // thread. With the lock it waits for the first; without it, it reads the same row and
      // parks before its own command.
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            p.bus.runOnceBefore(
                Reserve.class,
                () -> {
                  twinAtItsCommand.countDown();
                  park(letTheTwinGo);
                });
            twin.set(onAnotherThread(() -> p.deliver(opened)));
            awaitUntil(
                "the second delivery waits for the first, or has reached its own command",
                () ->
                    p.lock.waitingFor(TYPE, sagaOf(ORDER)) == 1
                        || twinAtItsCommand.getCount() == 0);
          });

      p.deliver(opened); // rejected: it claims and compensates

      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      p.reserve.failure = null; // the rejection does not repeat
      letTheTwinGo.countDown();
      assertThat(outcomeOf(twin.get())).as("the second delivery meets a terminal row").isNull();

      assertThat(p.storedOrder(ORDER))
          .as("no forward command is stored after the compensation")
          .containsExactly("Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void aStepArrivesWhileAClaimHoldsTheLock_itWaitsForTheClaim_thenResumesTheStampedEpisode() {
    try (var p = new SagaStepLockFixture()) {
      p.deliver(p.append(new Opened(ORDER)));
      EventEnvelope advanced = p.append(new Advanced(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      var step = new AtomicReference<CompletableFuture<Void>>();
      p.bus.runOnceBefore(
          Ship.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      // The claim has read the row and is about to write: a delivery of the next event starts.
      p.sagaStore.runOnceBeforeTheClaimIsWritten(
          () -> {
            step.set(onAnotherThread(() -> p.deliver(advanced)));
            awaitUntil(
                "the step waits for the claim, or has reached its own command",
                () ->
                    p.lock.waitingFor(TYPE, sagaOf(ORDER)) == 1
                        || stepAtItsCommand.getCount() == 0);
          });

      p.timeOut();

      letTheStepGo.countDown();
      assertThat(outcomeOf(step.get())).as("a resume of the claimed episode returns").isNull();
      assertThat(p.storedOrder(ORDER))
          .as(
              "the step read a stamped row: it dispatched no forward command, and each"
                  + " compensation command is stored once")
          .containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.row(ORDER).lastAppliedOffset())
          .as("a resume does not consume the event")
          .isLessThan(advanced.globalOffset().value());
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void aSecondDeliveryStartsInsideTheFirstOnTheSameThread_itEntersAtOnce_andRunsAsWithoutTheLock() {
    try (var p =
        new SagaStepLockFixture(
            new SagaStepLock(Duration.ofMillis(1)), SagaTimeoutRunner.DEFAULT_CLAIM_WAIT)) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      // A thread cannot wait for itself: with a one-millisecond bound a wait would fail here.
      p.bus.runOnceBefore(Reserve.class, () -> p.deliver(opened));

      assertThatThrownBy(() -> p.deliver(opened))
          .as("the nested delivery wrote first; the outer one loses its write, as without the lock")
          .isInstanceOf(OptimisticLockException.class);

      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved");
      assertThat(p.row(ORDER).genesisApplied()).isTrue();
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  // ---- the event of a compensation command, delivered back to the saga ----

  @Test
  void aCompensationCommandsOwnEventIsDeliveredOnTheSameThread_nothingWaits() {
    try (var p = new SagaStepLockFixture()) {
      p.reserve.failure = () -> new IllegalArgumentException("out of stock");
      var subscription = new InMemoryEventSubscription(p.eventStore, p.runner.asEventListener());
      subscription.start();
      try {
        // The start event is delivered on the appending thread; the rejected command's
        // compensation stores Released and Refunded, which carry the saga's origin and are
        // delivered to the same saga on that same thread.
        assertTimeoutPreemptively(
            Duration.ofSeconds(SagaStepLockFixture.PATIENCE_SECONDS),
            () -> {
              p.append(new Opened(ORDER));
            });
      } finally {
        subscription.close();
      }

      assertThat(subscription.deliveryFailureCount()).isZero();
      assertThat(subscription.lastDeliveredOffset())
          .as("the compensation events were delivered")
          .isEqualTo(3L);
      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void aCompensationCommandsOwnEventIsDeliveredOnAnotherThreadDuringTheStep_itWaitsAndReturns() {
    try (var p = new SagaStepLockFixture()) {
      p.reserve.failure = () -> new IllegalArgumentException("out of stock");
      EventEnvelope opened = p.append(new Opened(ORDER));
      var other = new AtomicReference<CompletableFuture<Void>>();
      var otherReturned = new AtomicBoolean();
      // The step is between its two compensation commands: the event of the first one is
      // delivered to the same saga on another thread.
      p.bus.runOnceBefore(
          Refund.class,
          () -> {
            EventEnvelope released =
                p.eventStore.readGlobalStream(GlobalOffset.initial(), 1_000).stream()
                    .filter(e -> e.event() instanceof Released)
                    .findFirst()
                    .orElseThrow();
            other.set(
                onAnotherThread(
                    () -> {
                      p.deliver(released);
                      otherReturned.set(true);
                    }));
            awaitUntil(
                "the delivery of the compensation's event waits for the step, or has returned",
                () -> p.lock.waitingFor(TYPE, sagaOf(ORDER)) == 1 || otherReturned.get());
          });

      p.deliver(opened);

      assertThat(outcomeOf(other.get())).isNull();
      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  // ---- what a step in flight does not hold up ----

  @Test
  void aStepOfOneSagaIsInFlight_anotherSagasStepAndClaimDoNotWaitForIt() {
    try (var p = new SagaStepLockFixture()) {
      EventEnvelope first = p.append(new Opened("1"));
      EventEnvelope second = p.append(new Opened("2"));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(first));
      await(stepAtItsCommand);

      p.deliver(second);
      p.timeOut();

      assertThat(p.storedOrder("2")).containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.row("2").status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.storedOrder("1")).as("the saga in a step is passed over").isEmpty();
      assertThat(p.lock.sagasInFlight()).as("only the saga in a step has an entry").isEqualTo(1);

      letTheStepGo.countDown();
      assertThat(outcomeOf(step)).isNull();
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void theStepEndsWithAnError_theLockIsReleased_andTheNextPollClaims() {
    try (var p = new SagaStepLockFixture()) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            throw new ProcessDeath();
          });

      assertThatThrownBy(() -> p.deliver(opened)).isInstanceOf(ProcessDeath.class);

      assertThat(p.lock.sagasInFlight()).isZero();
      p.timeOut();
      assertThat(p.timeoutRunner.deferralsOf(sagaOf(ORDER))).isZero();
      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void theProcessDiesInsideAClaimBeforeItIsWritten_nothingIsHeldOrWritten_andTheNextPollClaims() {
    try (var p = new SagaStepLockFixture()) {
      p.deliver(p.append(new Opened(ORDER)));
      p.sagaStore.runOnceBeforeTheClaimIsWritten(
          () -> {
            throw new ProcessDeath();
          });

      assertThatThrownBy(p::timeOut).isInstanceOf(ProcessDeath.class);

      assertThat(p.lock.sagasInFlight()).isZero();
      assertThat(p.row(ORDER).status()).as("the row a dead claim leaves").isEqualTo(RUNNING);
      assertThat(p.row(ORDER).episodeVersion()).isNull();
      p.timeoutRunner.pollOnce();
      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void theClaimIsWrittenAndItsCallFails_nothingIsHeld_andTheEpisodeIsResumedAsAnyClaimedOne() {
    try (var p = new SagaStepLockFixture()) {
      p.deliver(p.append(new Opened(ORDER)));
      p.sagaStore.crashOnceAfter("claimCompensating");

      p.timeOut(); // the failure of one saga is logged, not thrown

      assertThat(p.lock.sagasInFlight()).isZero();
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(p.row(ORDER).episodeVersion()).as("stamped by the claim").isEqualTo(3L);
      assertThat(p.storedOrder(ORDER)).as("nothing dispatched yet").containsExactly("Reserved");

      p.timeOut(); // the re-pick of the claimed episode

      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void aDeliveryIsInterruptedWhileItWaitsForAStep_itFailsWithItsInterruptStatusSet() {
    try (var p = new SagaStepLockFixture()) {
      p.deliver(p.append(new Opened(ORDER)));
      EventEnvelope advanced = p.append(new Advanced(ORDER));
      EventEnvelope again = p.append(new Advanced(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Ship.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(advanced));
      await(stepAtItsCommand);
      Thread[] waiter = new Thread[1];
      var interruptStatusKept = new AtomicBoolean();
      CompletableFuture<Void> waiting =
          onAnotherThread(
              () -> {
                waiter[0] = Thread.currentThread();
                try {
                  p.deliver(again);
                } finally {
                  interruptStatusKept.set(Thread.currentThread().isInterrupted());
                }
              });
      p.awaitWaitingFor(ORDER, 1);

      waiter[0].interrupt();

      assertThat(outcomeOf(waiting))
          .isInstanceOf(SagaStepLockException.class)
          .hasMessageContaining("Interrupted")
          .hasCauseInstanceOf(InterruptedException.class);
      assertThat(interruptStatusKept).as("the interrupt is not swallowed").isTrue();
      assertThat(p.lock.waitingFor(TYPE, sagaOf(ORDER))).isZero();

      letTheStepGo.countDown();
      assertThat(outcomeOf(step)).isNull();
      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved", "Shipped");
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void aDeliveryWaitsPastTheStepBound_itFailsAsARetryLaterFailure_havingDispatchedNothing() {
    try (var p =
        new SagaStepLockFixture(
            new SagaStepLock(Duration.ofMillis(20)), SagaTimeoutRunner.DEFAULT_CLAIM_WAIT)) {
      p.deliver(p.append(new Opened(ORDER)));
      EventEnvelope advanced = p.append(new Advanced(ORDER));
      EventEnvelope again = p.append(new Advanced(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Ship.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(advanced));
      await(stepAtItsCommand);

      assertThatThrownBy(() -> p.deliver(again))
          .isInstanceOf(SagaStepLockException.class)
          .hasMessageContaining(sagaOf(ORDER).value())
          .satisfies(e -> assertThat(SagaCommandDispatch.isRetryLater(e)).isTrue());

      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved");
      letTheStepGo.countDown();
      assertThat(outcomeOf(step)).isNull();

      p.deliver(again); // the redelivery
      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved", "Shipped", "Shipped");
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  // ---- a saga that is passed over more than once ----

  @Test
  void aSagaPassedOverThreePolls_theNextClaimWaitsForTheStep_andIsMadeAheadOfALaterDelivery() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), Duration.ofSeconds(20))) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      EventEnvelope advanced = p.append(new Advanced(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
      await(stepAtItsCommand);

      p.timeOut();
      p.timeoutRunner.pollOnce();
      p.timeoutRunner.pollOnce();
      assertThat(p.timeoutRunner.deferralsOf(sagaOf(ORDER)))
          .isEqualTo(SagaTimeoutRunner.DEFERRALS_BEFORE_WAITING);

      // The fourth poll waits for the step; a delivery of the saga's next event arrives behind it.
      CompletableFuture<Void> waitingPoll = onAnotherThread(p.timeoutRunner::pollOnce);
      p.awaitWaitingFor(ORDER, 1);
      CompletableFuture<Void> laterDelivery = onAnotherThread(() -> p.deliver(advanced));
      p.awaitWaitingFor(ORDER, 2);

      letTheStepGo.countDown();

      assertThat(outcomeOf(step)).isNull();
      assertThat(outcomeOf(waitingPoll)).isNull();
      assertThat(outcomeOf(laterDelivery)).isNull();
      assertThat(p.storedOrder(ORDER))
          .as("the claim is made before the later delivery, which resumes the episode")
          .containsExactly("Reserved", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.timeoutRunner.deferralsOf(sagaOf(ORDER))).isZero();
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void aStepThatDoesNotReturn_theClaimWaitsItsBoundOnEveryPoll_warns_andIsNeverMadeUnderIt() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), Duration.ofMillis(20))) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
      await(stepAtItsCommand);

      List<String> warnings =
          timeoutRunnerWarningsDuring(
              () -> {
                p.timeOut();
                p.timeoutRunner.pollOnce();
                p.timeoutRunner.pollOnce();
                p.timeoutRunner.pollOnce();
                p.timeoutRunner.pollOnce();
              });

      assertThat(p.timeoutRunner.deferralsOf(sagaOf(ORDER))).isEqualTo(5);
      assertThat(warnings)
          .as("the three polls that asked once are not warnings; the two that waited are")
          .hasSize(2)
          .allSatisfy(
              line -> {
                assertThat(line).contains(sagaOf(ORDER).value());
                assertThat(line).contains("does not return");
              });
      assertThat(p.storedOrder(ORDER)).as("the claim is never made under the step").isEmpty();
      assertThat(p.row(ORDER).episodeVersion()).isNull();

      letTheStepGo.countDown();
      assertThat(outcomeOf(step)).isNull();
      p.timeoutRunner.pollOnce();
      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
    }
  }

  @Test
  void thePollThreadIsInterruptedWhileItsClaimWaits_thePollReturns_withItsInterruptStatusSet() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), Duration.ofSeconds(20))) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
      await(stepAtItsCommand);
      p.timeOut();
      p.timeoutRunner.pollOnce();
      p.timeoutRunner.pollOnce();
      var pollThread = new AtomicReference<Thread>();
      var interruptStatusKept = new AtomicBoolean();
      CompletableFuture<Void> waitingPoll =
          onAnotherThread(
              () -> {
                pollThread.set(Thread.currentThread());
                p.timeoutRunner.pollOnce();
                interruptStatusKept.set(Thread.currentThread().isInterrupted());
              });
      p.awaitWaitingFor(ORDER, 1);

      pollThread.get().interrupt(); // what stop() does to the poll thread

      assertThat(outcomeOf(waitingPoll)).as("the poll ends without a failure").isNull();
      assertThat(interruptStatusKept).as("the poll loop sees the interrupt").isTrue();
      assertThat(p.storedOrder(ORDER)).as("nothing was claimed").isEmpty();
      assertThat(p.lock.waitingFor(TYPE, sagaOf(ORDER))).isZero();

      letTheStepGo.countDown();
      assertThat(outcomeOf(step)).isNull();
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void aSagaThatCompletesAfterBeingPassedOver_isForgottenByTheTimeoutRunner() {
    try (var p = new SagaStepLockFixture()) {
      EventEnvelope opened = p.append(new Opened(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
      await(stepAtItsCommand);
      p.timeOut();
      assertThat(p.timeoutRunner.deferralsOf(sagaOf(ORDER))).isEqualTo(1);
      letTheStepGo.countDown();
      assertThat(outcomeOf(step)).isNull();

      p.deliver(p.append(new Completed(ORDER)));
      p.timeoutRunner.pollOnce();

      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved");
      assertThat(p.timeoutRunner.deferralsOf(sagaOf(ORDER))).isZero();
    }
  }

  // ---- every exit of a claim releases the lock ----

  @Test
  void aClaimThatWaitedFindsTheSagaCompletedByThatStep_itWritesNothingAndHoldsNothing() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(SHORT_BOUND), Duration.ofSeconds(20))) {
      p.deliver(p.append(new Opened(ORDER)));
      EventEnvelope completed = p.append(new Completed(ORDER));
      var stepAtItsWrite = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.sagaStore.runOnceBeforeTheEventWrite(
          () -> {
            stepAtItsWrite.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(completed));
      await(stepAtItsWrite);
      p.timeOut();
      p.timeoutRunner.pollOnce();
      p.timeoutRunner.pollOnce();
      CompletableFuture<Void> waitingPoll = onAnotherThread(p.timeoutRunner::pollOnce);
      p.awaitWaitingFor(ORDER, 1);

      letTheStepGo.countDown();

      assertThat(outcomeOf(step)).isNull();
      assertThat(outcomeOf(waitingPoll)).isNull();
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPLETED);
      assertThat(p.storedOrder(ORDER)).as("nothing is compensated").containsExactly("Reserved");
      assertThat(p.lock.sagasInFlight()).as("the claim that found nothing to claim").isZero();
      assertALaterDeliveryIsNotHeldUp(p);
    }
  }

  @Test
  void aClaimThatWaitedFindsAnEpisodeTheStepOpened_itReleasesTheLockBeforeItResumesTheEpisode() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(SHORT_BOUND), Duration.ofSeconds(20))) {
      // The step's command is rejected; it claims, and its refund fails on infrastructure, so it
      // leaves the episode COMPENSATING for the claim that waits behind it.
      p.reserve.failure = () -> new IllegalArgumentException("out of stock");
      p.refund.failure = () -> new IllegalStateException("connection reset");
      EventEnvelope opened = p.append(new Opened(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
      await(stepAtItsCommand);
      p.timeOut();
      p.timeoutRunner.pollOnce();
      p.timeoutRunner.pollOnce();
      var pollThread = new AtomicReference<Thread>();
      List<Integer> heldWhileThePollDispatches = new CopyOnWriteArrayList<>();
      p.bus.beforeEveryCommand =
          command -> {
            if (Thread.currentThread() == pollThread.get()) {
              heldWhileThePollDispatches.add(p.lock.sagasInFlight());
            }
          };
      CompletableFuture<Void> waitingPoll =
          onAnotherThread(
              () -> {
                pollThread.set(Thread.currentThread());
                p.timeoutRunner.pollOnce();
              });
      p.awaitWaitingFor(ORDER, 1);

      letTheStepGo.countDown();

      assertThat(outcomeOf(step)).isNull();
      assertThat(outcomeOf(waitingPoll)).isNull();
      assertThat(heldWhileThePollDispatches)
          .as("the poll resumed the episode, with the lock free")
          .containsExactly(0, 0);
      assertThat(p.storedOrder(ORDER)).containsExactly("Released");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATING);
      assertThat(p.lock.sagasInFlight()).isZero();
      p.refund.failure = null;
      assertALaterDeliveryIsNotHeldUp(p);
      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded");
    }
  }

  @Test
  void aClaimThatWaitedBehindAStepThatCompensatedItself_findsATerminalRowAndWritesNothing() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(SHORT_BOUND), Duration.ofSeconds(20))) {
      p.reserve.failure = () -> new IllegalArgumentException("out of stock");
      EventEnvelope opened = p.append(new Opened(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      CompletableFuture<Void> step = onAnotherThread(() -> p.deliver(opened));
      await(stepAtItsCommand);
      p.timeOut();
      p.timeoutRunner.pollOnce();
      p.timeoutRunner.pollOnce();
      CompletableFuture<Void> waitingPoll = onAnotherThread(p.timeoutRunner::pollOnce);
      p.awaitWaitingFor(ORDER, 1);

      letTheStepGo.countDown();

      assertThat(outcomeOf(step)).isNull();
      assertThat(outcomeOf(waitingPoll)).isNull();
      assertThat(p.storedOrder(ORDER))
          .as("the step's own compensation, once")
          .containsExactly("Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void theReadUnderTheClaimsHoldFails_theLockIsReleased_andTheNextPollClaims() {
    try (var p =
        new SagaStepLockFixture(
            new SagaStepLock(SHORT_BOUND), SagaTimeoutRunner.DEFAULT_CLAIM_WAIT)) {
      p.deliver(p.append(new Opened(ORDER)));
      // The poll reads the row to route it, takes the lock, and reads it again: that read fails.
      p.sagaStore.failTheReadNumber(2);

      p.timeOut(); // the failure of one saga is logged, not thrown

      assertThat(p.lock.sagasInFlight()).isZero();
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.RUNNING);
      assertALaterDeliveryIsNotHeldUp(p);
      p.timeoutRunner.pollOnce();
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void theOperatorsClaimFindsNothingToClaim_theLockIsReleased() {
    try (var p =
        new SagaStepLockFixture(
            new SagaStepLock(SHORT_BOUND), SagaTimeoutRunner.DEFAULT_CLAIM_WAIT)) {
      // No row: the executor answers before it reaches a claim.
      assertThat(p.runner.compensateForwardFault(sagaOf(ORDER)))
          .isEqualTo(StepOutcome.NOT_COMPENSATING);
      assertThat(p.lock.sagasInFlight()).as("no row").isZero();

      // A live row: not this operation's.
      p.deliver(p.append(new Opened(ORDER)));
      assertThat(p.runner.compensateForwardFault(sagaOf(ORDER)))
          .isEqualTo(StepOutcome.NOT_COMPENSATING);
      assertThat(p.lock.sagasInFlight()).as("a live row").isZero();
      assertALaterDeliveryIsNotHeldUp(p);

      // A read that fails under the hold.
      p.sagaStore.failTheReadNumber(1);
      assertThatThrownBy(() -> p.runner.compensateForwardFault(sagaOf(ORDER)))
          .isInstanceOf(IllegalStateException.class);
      assertThat(p.lock.sagasInFlight()).as("a failed read").isZero();
      assertALaterDeliveryIsNotHeldUp(p);
    }
  }

  // ---- a waiting claim against deliveries that keep arriving ----

  @Test
  void theDeliveringThreadGoesStraightOnToTheSagasNextEvent_theClaimThatWaitsIsMadeFirst() {
    try (var p = new SagaStepLockFixture(new SagaStepLock(), Duration.ofSeconds(20))) {
      p.deliver(p.append(new Opened(ORDER)));
      EventEnvelope first = p.append(new Advanced(ORDER));
      EventEnvelope second = p.append(new Advanced(ORDER));
      var stepAtItsCommand = new CountDownLatch(1);
      var letTheStepGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Ship.class,
          () -> {
            stepAtItsCommand.countDown();
            park(letTheStepGo);
          });
      // One batch, as a subscription delivers it: the thread that ends the first step asks for
      // the lock again at once, for the second. It did not wait; the claim did.
      CompletableFuture<Void> batch =
          onAnotherThread(() -> p.runner.asEventListener().onEvents(List.of(first, second)));
      await(stepAtItsCommand);
      p.timeOut();
      p.timeoutRunner.pollOnce();
      p.timeoutRunner.pollOnce();
      CompletableFuture<Void> waitingPoll = onAnotherThread(p.timeoutRunner::pollOnce);
      p.awaitWaitingFor(ORDER, 1);

      letTheStepGo.countDown();

      assertThat(outcomeOf(batch)).isNull();
      assertThat(outcomeOf(waitingPoll)).isNull();
      assertThat(p.storedOrder(ORDER))
          .as(
              "the second event's step runs after the claim and resumes the episode: it dispatches"
                  + " no forward command")
          .containsExactly("Reserved", "Shipped", "Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  // ---- the operator's compensation of a faulted forward step ----

  @Test
  void aReplayFeedIsInFlight_theOperatorsCompensationIsNotMadeUnderIt() {
    try (var p = new SagaStepLockFixture()) {
      // A saga a forward step faulted, with no dead-letter entry left.
      EventEnvelope opened = p.append(new Opened(ORDER));
      p.sagaStore.createFaulted(
          sagaOf(ORDER), TYPE, new SagaStepLockFixture.State(SagaStatus.STARTED, ORDER));
      var feedAtItsCommand = new CountDownLatch(1);
      var letTheFeedGo = new CountDownLatch(1);
      p.bus.runOnceBefore(
          Reserve.class,
          () -> {
            feedAtItsCommand.countDown();
            park(letTheFeedGo);
          });
      // A replay feed of the start event re-drives the forward step.
      CompletableFuture<Void> feed =
          onAnotherThread(() -> p.runner.feedReplay(sagaOf(ORDER), opened, true, false));
      await(feedAtItsCommand);

      assertThat(p.replayer.compensateFaulted(sagaOf(ORDER)))
          .as("nothing is claimed under the feed")
          .isEqualTo(CompensateOutcome.CLAIM_LOST);
      assertThat(p.storedOrder(ORDER)).isEmpty();
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.FAULTED);

      letTheFeedGo.countDown();
      assertThat(outcomeOf(feed)).isNull();
      assertThat(p.storedOrder(ORDER)).containsExactly("Reserved");
      assertThat(p.row(ORDER).status()).as("the feed's write clears the fault").isEqualTo(RUNNING);
      assertThat(p.lock.sagasInFlight()).isZero();
    }
  }

  @Test
  void noStepIsInFlight_theOperatorsCompensationClaims_andReleasesTheLockBeforeItCompensates() {
    try (var p = new SagaStepLockFixture()) {
      p.sagaStore.createFaulted(
          sagaOf(ORDER), TYPE, new SagaStepLockFixture.State(SagaStatus.STARTED, ORDER));
      int[] entriesDuringTheCompensation = {-1};
      p.bus.runOnceBefore(
          SagaStepLockFixture.Release.class,
          () -> entriesDuringTheCompensation[0] = p.lock.sagasInFlight());

      assertThat(p.replayer.compensateFaulted(sagaOf(ORDER))).isEqualTo(CompensateOutcome.TERMINAL);

      assertThat(entriesDuringTheCompensation[0])
          .as("the claim's hold ends with the claim write")
          .isZero();
      assertThat(p.storedOrder(ORDER)).containsExactly("Released", "Refunded");
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  @Test
  void theTimeoutClaimReleasesTheLockBeforeItCompensates() {
    try (var p = new SagaStepLockFixture()) {
      p.deliver(p.append(new Opened(ORDER)));
      int[] entriesDuringTheClaim = {-1};
      int[] entriesDuringTheCompensation = {-1};
      p.sagaStore.runOnceBeforeTheClaimIsWritten(
          () -> entriesDuringTheClaim[0] = p.lock.sagasInFlight());
      p.bus.runOnceBefore(
          SagaStepLockFixture.Release.class,
          () -> entriesDuringTheCompensation[0] = p.lock.sagasInFlight());

      p.timeOut();

      assertThat(entriesDuringTheClaim[0]).as("held around the read and the claim").isEqualTo(1);
      assertThat(entriesDuringTheCompensation[0])
          .as("not held while the compensation is dispatched")
          .isZero();
      assertThat(p.row(ORDER).status()).isEqualTo(SagaStatus.COMPENSATED);
    }
  }

  private static final SagaStatus RUNNING = SagaStatus.RUNNING;

  /** A step bound short enough for a leaked lock to fail a test at once. */
  private static final Duration SHORT_BOUND = Duration.ofSeconds(2);

  /**
   * A delivery of the saga from another thread returns. A lock left held by a thread that has gone
   * on — or has ended — would keep it waiting until the step bound and fail it.
   */
  private static void assertALaterDeliveryIsNotHeldUp(SagaStepLockFixture p) {
    EventEnvelope later = p.append(new Advanced(ORDER));
    assertThat(outcomeOf(onAnotherThread(() -> p.deliver(later))))
        .as("a later delivery of the saga, from another thread")
        .isNull();
  }

  /** The timeout runner's WARN lines written while {@code body} runs. */
  private static List<String> timeoutRunnerWarningsDuring(Runnable body) {
    var logger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(SagaTimeoutRunner.class);
    var appender = new ListAppender<ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      body.run();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }
    return appender.list.stream()
        .filter(e -> e.getLevel() == Level.WARN)
        .map(ILoggingEvent::getFormattedMessage)
        .toList();
  }
}
