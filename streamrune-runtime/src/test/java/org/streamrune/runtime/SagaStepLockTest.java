package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.runtime.SagaStepLockFixture.await;
import static org.streamrune.runtime.SagaStepLockFixture.awaitUntil;
import static org.streamrune.runtime.SagaStepLockFixture.onAnotherThread;
import static org.streamrune.runtime.SagaStepLockFixture.outcomeOf;
import static org.streamrune.runtime.SagaStepLockFixture.park;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.SagaType;

/** The rules of {@link SagaStepLock}, on the lock alone. */
class SagaStepLockTest {

  record OrderState() {}

  record PaymentState() {}

  private static final SagaType ORDERS = SagaType.fromClass(OrderState.class);
  private static final SagaType PAYMENTS = SagaType.fromClass(PaymentState.class);
  private static final SagaId SAGA = SagaId.of("saga-1");
  private static final Duration LONG = Duration.ofSeconds(SagaStepLockFixture.PATIENCE_SECONDS);

  /** A thread that enters a step of the saga and stays in it until {@link #leave()}. */
  private static final class StepInFlight {
    private final CountDownLatch entered = new CountDownLatch(1);
    private final CountDownLatch mayLeave = new CountDownLatch(1);
    private final CompletableFuture<Void> thread;

    StepInFlight(SagaStepLock lock, SagaType type, SagaId saga) {
      thread =
          onAnotherThread(
              () -> {
                try (SagaStepLock.Hold _ = lock.enterStep(type, saga)) {
                  entered.countDown();
                  park(mayLeave);
                }
              });
      await(entered);
    }

    void leave() {
      mayLeave.countDown();
      assertThat(outcomeOf(thread)).isNull();
    }
  }

  @Test
  void anEntryExistsWhileASagaIsInFlight_andIsRemovedWithTheLastThread() {
    var lock = new SagaStepLock();
    assertThat(lock.sagasInFlight()).isZero();

    SagaStepLock.Hold step = lock.enterStep(ORDERS, SAGA);
    assertThat(lock.sagasInFlight()).isEqualTo(1);

    step.close();
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void closingAHoldTwiceReleasesOnce() {
    var lock = new SagaStepLock();
    SagaStepLock.Hold outer = lock.enterStep(ORDERS, SAGA);
    SagaStepLock.Hold claimOfAnotherSaga =
        lock.tryEnterClaim(ORDERS, SagaId.of("saga-2"), Duration.ZERO);

    claimOfAnotherSaga.close();
    claimOfAnotherSaga.close();

    assertThat(lock.sagasInFlight()).as("the step of the first saga is still held").isEqualTo(1);
    outer.close();
    outer.close();
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void aStepEntersAgainOnItsOwnThread_andTheSagaStaysInFlightUntilTheOuterOneLeaves() {
    var lock = new SagaStepLock(Duration.ofMillis(1));
    SagaStepLock.Hold outer = lock.enterStep(ORDERS, SAGA);

    SagaStepLock.Hold nested = lock.enterStep(ORDERS, SAGA);
    nested.close();

    assertThat(lock.sagasInFlight()).isEqualTo(1);
    assertThat(lock.tryEnterClaim(ORDERS, SAGA, Duration.ZERO))
        .as("the outer step is still in flight on this thread")
        .isNull();
    outer.close();
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void aClaimOnAThreadThatIsInsideAStepOfTheSagaIsRefused_withAndWithoutAWait() {
    var lock = new SagaStepLock();
    try (SagaStepLock.Hold _ = lock.enterStep(ORDERS, SAGA)) {
      assertThat(lock.tryEnterClaim(ORDERS, SAGA, Duration.ZERO)).isNull();
      // A thread cannot wait for itself: the wait is not started.
      assertThat(lock.tryEnterClaim(ORDERS, SAGA, LONG)).isNull();
      assertThat(lock.sagasInFlight()).isEqualTo(1);
    }
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void aClaimIsRefusedWhileAnotherThreadIsInAStep_andEntersOnceTheStepHasLeft() {
    var lock = new SagaStepLock();
    var step = new StepInFlight(lock, ORDERS, SAGA);

    assertThat(lock.tryEnterClaim(ORDERS, SAGA, Duration.ZERO)).isNull();
    assertThat(lock.tryEnterClaim(ORDERS, SAGA, Duration.ofMillis(5)))
        .as("a wait that ends with the step still in flight")
        .isNull();
    assertThat(lock.waitingFor(ORDERS, SAGA)).as("a refused claim leaves no waiter").isZero();

    step.leave();
    SagaStepLock.Hold claim = lock.tryEnterClaim(ORDERS, SAGA, Duration.ZERO);
    assertThat(claim).isNotNull();
    claim.close();
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void aClaimThatWaitsEntersWhenTheStepLeaves_aheadOfAStepThatArrivedAfterIt() {
    var lock = new SagaStepLock();
    var step = new StepInFlight(lock, ORDERS, SAGA);
    List<String> order = new CopyOnWriteArrayList<>();
    CompletableFuture<Void> claim =
        onAnotherThread(
            () -> {
              try (SagaStepLock.Hold hold = lock.tryEnterClaim(ORDERS, SAGA, LONG)) {
                order.add(hold == null ? "claim refused" : "claim");
              }
            });
    awaitUntil("the claim waits", () -> lock.waitingFor(ORDERS, SAGA) == 1);
    CompletableFuture<Void> laterStep =
        onAnotherThread(
            () -> {
              try (SagaStepLock.Hold _ = lock.enterStep(ORDERS, SAGA)) {
                order.add("later step");
              }
            });
    awaitUntil("the later step waits behind it", () -> lock.waitingFor(ORDERS, SAGA) == 2);

    step.leave();

    assertThat(outcomeOf(claim)).isNull();
    assertThat(outcomeOf(laterStep)).isNull();
    assertThat(order).containsExactly("claim", "later step");
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void aStepWaitsForAClaim_andEntersWhenTheClaimIsReleased() {
    var lock = new SagaStepLock();
    SagaStepLock.Hold claim = lock.tryEnterClaim(ORDERS, SAGA, Duration.ZERO);
    var entered = new AtomicBoolean();
    CompletableFuture<Void> step =
        onAnotherThread(
            () -> {
              try (SagaStepLock.Hold _ = lock.enterStep(ORDERS, SAGA)) {
                entered.set(true);
              }
            });
    awaitUntil("the step waits for the claim", () -> lock.waitingFor(ORDERS, SAGA) == 1);
    assertThat(entered).isFalse();

    claim.close();

    assertThat(outcomeOf(step)).isNull();
    assertThat(entered).isTrue();
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void aStepThatWaitsPastTheBoundFails_andLeavesNothingBehind() {
    var lock = new SagaStepLock(Duration.ofMillis(10));
    var step = new StepInFlight(lock, ORDERS, SAGA);

    assertThatThrownBy(() -> lock.enterStep(ORDERS, SAGA))
        .isInstanceOf(SagaStepLockException.class)
        .hasMessageContaining("saga-1")
        .hasMessageContaining("PT0.01S")
        .hasNoCause();
    assertThat(lock.waitingFor(ORDERS, SAGA)).isZero();
    assertThat(lock.sagasInFlight()).isEqualTo(1);

    step.leave();
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void aStepInterruptedWhileItWaitsFails_withItsInterruptStatusSet() {
    var lock = new SagaStepLock();
    var step = new StepInFlight(lock, ORDERS, SAGA);
    var waiter = new AtomicReference<Thread>();
    var interruptStatusKept = new AtomicBoolean();
    CompletableFuture<Void> waiting =
        onAnotherThread(
            () -> {
              waiter.set(Thread.currentThread());
              try {
                lock.enterStep(ORDERS, SAGA);
              } finally {
                interruptStatusKept.set(Thread.currentThread().isInterrupted());
              }
            });
    awaitUntil("the second step waits", () -> lock.waitingFor(ORDERS, SAGA) == 1);

    waiter.get().interrupt();

    assertThat(outcomeOf(waiting))
        .isInstanceOf(SagaStepLockException.class)
        .hasCauseInstanceOf(InterruptedException.class);
    assertThat(interruptStatusKept).isTrue();
    step.leave();
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void aClaimInterruptedWhileItWaitsFails_withItsInterruptStatusSet_andHoldsNothing() {
    var lock = new SagaStepLock();
    var step = new StepInFlight(lock, ORDERS, SAGA);
    var waiter = new AtomicReference<Thread>();
    var interruptStatusKept = new AtomicBoolean();
    CompletableFuture<Void> waiting =
        onAnotherThread(
            () -> {
              waiter.set(Thread.currentThread());
              try {
                lock.tryEnterClaim(ORDERS, SAGA, LONG);
              } finally {
                interruptStatusKept.set(Thread.currentThread().isInterrupted());
              }
            });
    awaitUntil("the claim waits", () -> lock.waitingFor(ORDERS, SAGA) == 1);

    waiter.get().interrupt();

    assertThat(outcomeOf(waiting))
        .isInstanceOf(SagaStepLockException.class)
        .hasMessageContaining("nothing was claimed")
        .hasCauseInstanceOf(InterruptedException.class);
    assertThat(interruptStatusKept).isTrue();
    assertThat(lock.waitingFor(ORDERS, SAGA)).isZero();
    step.leave();
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void twoSagasDoNotShareALock_norDoesOneSagaIdUnderTwoSagaTypes() {
    var lock = new SagaStepLock(Duration.ofMillis(1));
    var step = new StepInFlight(lock, ORDERS, SAGA);

    // Neither waits: a one-millisecond bound would fail them if they shared the lock.
    try (SagaStepLock.Hold _ = lock.enterStep(ORDERS, SagaId.of("saga-2"));
        SagaStepLock.Hold _ = lock.enterStep(PAYMENTS, SAGA);
        SagaStepLock.Hold claim =
            lock.tryEnterClaim(PAYMENTS, SagaId.of("saga-2"), Duration.ZERO)) {
      assertThat(claim).isNotNull();
      assertThat(lock.sagasInFlight()).isEqualTo(4);
    }

    assertThat(lock.sagasInFlight()).as("only the first step is left").isEqualTo(1);
    step.leave();
    assertThat(lock.sagasInFlight()).isZero();
  }

  @Test
  void theStepBoundIsRequired_andTheDefaultIsThirtySeconds() {
    assertThatThrownBy(() -> new SagaStepLock(null)).isInstanceOf(NullPointerException.class);
    assertThat(SagaStepLock.DEFAULT_STEP_WAIT).isEqualTo(Duration.ofSeconds(30));
  }
}
