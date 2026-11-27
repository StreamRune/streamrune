package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * The slots a saga runner shares between its steps and its timeout runner's claims: never more
 * holders than the limit read at each acquire, arrival order, interruptible waits, and reentrant
 * holds.
 */
@Timeout(30)
class SagaSlotsTest {

  private static void awaitUntil(String what, BooleanSupplier condition) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("never happened: " + what);
      }
      Thread.onSpinWait();
    }
  }

  /** Takes a slot on a new thread, holds it until {@code release}, and completes when done. */
  private static CompletableFuture<Void> holder(
      SagaSlots slots, CountDownLatch release, List<String> order, String name) {
    CompletableFuture<Void> done = new CompletableFuture<>();
    Thread.ofVirtual()
        .start(
            () -> {
              try (SagaSlots.Slot _ = slots.acquire()) {
                order.add(name);
                release.await();
                done.complete(null);
              } catch (Throwable t) {
                done.completeExceptionally(t);
              }
            });
    return done;
  }

  @Test
  void manyThreadsAtOnce_neverMoreHoldersThanTheLimit() throws Exception {
    var slots = new SagaSlots(() -> 3);
    var holding = new AtomicInteger();
    var most = new AtomicInteger();
    var threads = new java.util.ArrayList<Thread>();
    for (int i = 0; i < 24; i++) {
      threads.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    for (int round = 0; round < 20; round++) {
                      try (SagaSlots.Slot _ = slots.acquire()) {
                        most.accumulateAndGet(holding.incrementAndGet(), Math::max);
                        Thread.sleep(1);
                        holding.decrementAndGet();
                      } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                      }
                    }
                  }));
    }
    for (Thread t : threads) {
      t.join();
    }
    assertThat(most.get()).isEqualTo(3);
    assertThat(slots.inUse()).isZero();
    assertThat(slots.waiting()).isZero();
  }

  @Test
  void theLimitIsReadAtEachAcquire_aRaiseAdmitsAWaiterAtOnce_aCutAdmitsNobodyNew()
      throws Exception {
    var limit = new AtomicInteger(1);
    var slots = new SagaSlots(limit::get);
    var order = new CopyOnWriteArrayList<String>();
    var release = new CountDownLatch(1);
    var a = holder(slots, release, order, "a");
    awaitUntil("a holds", () -> order.contains("a"));
    var b = holder(slots, release, order, "b");
    awaitUntil("b waits", () -> slots.waiting() == 1);

    limit.set(2);
    slots.limitChanged();
    awaitUntil("b is let in while a holds", () -> slots.inUse() == 2);

    limit.set(1);
    slots.limitChanged();
    var c = holder(slots, new CountDownLatch(0), order, "c");
    awaitUntil("c waits", () -> slots.waiting() == 1);
    assertThat(slots.inUse()).as("a cut takes no slot from a holder").isEqualTo(2);

    release.countDown();
    CompletableFuture.allOf(a, b, c).get(20, TimeUnit.SECONDS);
    assertThat(order).containsExactly("a", "b", "c");
    assertThat(slots.inUse()).isZero();
  }

  @Test
  void aLimitBelowOne_countsAsOne() throws Exception {
    var slots = new SagaSlots(() -> 0);
    assertThat(slots.currentLimit()).isEqualTo(1);
    try (SagaSlots.Slot _ = slots.acquire()) {
      assertThat(slots.inUse()).isEqualTo(1);
    }
  }

  @Test
  void waitersAreLetInInArrivalOrder() throws Exception {
    var slots = new SagaSlots(() -> 1);
    var order = new CopyOnWriteArrayList<String>();
    var release = new CountDownLatch(1);
    var first = holder(slots, release, order, "first");
    awaitUntil("first holds", () -> slots.inUse() == 1);
    var futures = new java.util.ArrayList<CompletableFuture<Void>>();
    for (int i = 0; i < 5; i++) {
      int queued = i;
      futures.add(holder(slots, new CountDownLatch(0), order, "w" + i));
      awaitUntil("w" + i + " queues", () -> slots.waiting() == queued + 1);
    }
    release.countDown();
    first.get(20, TimeUnit.SECONDS);
    CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).get(20, TimeUnit.SECONDS);
    assertThat(order).containsExactly("first", "w0", "w1", "w2", "w3", "w4");
  }

  @Test
  void aClaimArrivingDuringAContinuousRunOfSteps_isLetInWithinOneRoundOfThem() throws Exception {
    var slots = new SagaSlots(() -> 2);
    var stop = new AtomicBoolean();
    var stepsAfterTheClaimAsked = new AtomicInteger();
    var claimAsked = new AtomicBoolean();
    var steps = new java.util.ArrayList<Thread>();
    for (int i = 0; i < 4; i++) {
      steps.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    while (!stop.get()) {
                      try (SagaSlots.Slot _ = slots.acquire()) {
                        if (claimAsked.get()) {
                          stepsAfterTheClaimAsked.incrementAndGet();
                        }
                        Thread.sleep(1);
                      } catch (InterruptedException e) {
                        return;
                      }
                    }
                  }));
    }
    awaitUntil("the steps keep both slots busy", () -> slots.waiting() >= 1);
    claimAsked.set(true);
    try (SagaSlots.Slot _ = slots.acquire()) {
      assertThat(stepsAfterTheClaimAsked.get())
          .as("only the steps queued before the claim go ahead of it")
          .isLessThanOrEqualTo(4 + 2);
    } finally {
      stop.set(true);
      for (Thread t : steps) {
        t.join();
      }
    }
  }

  @Test
  void aStepArrivingDuringAContinuousRunOfTimedClaims_isLetInWithinOneRoundOfThem()
      throws Exception {
    var slots = new SagaSlots(() -> 2);
    var stop = new AtomicBoolean();
    var claimsAfterTheStepAsked = new AtomicInteger();
    var stepAsked = new AtomicBoolean();
    var claims = new java.util.ArrayList<Thread>();
    for (int i = 0; i < 4; i++) {
      claims.add(
          Thread.ofVirtual()
              .start(
                  () -> {
                    while (!stop.get()) {
                      try (SagaSlots.Slot slot =
                          slots.tryAcquire(java.time.Duration.ofSeconds(5))) {
                        if (slot == null) {
                          continue;
                        }
                        if (stepAsked.get()) {
                          claimsAfterTheStepAsked.incrementAndGet();
                        }
                        Thread.sleep(1);
                      } catch (InterruptedException e) {
                        return;
                      }
                    }
                  }));
    }
    awaitUntil("the claims keep both slots busy", () -> slots.waiting() >= 1);
    stepAsked.set(true);
    try (SagaSlots.Slot _ = slots.acquire()) {
      assertThat(claimsAfterTheStepAsked.get())
          .as("only the claims queued before the step go ahead of it")
          .isLessThanOrEqualTo(4 + 2);
    } finally {
      stop.set(true);
      for (Thread t : claims) {
        t.join();
      }
    }
  }

  @Test
  void aWaiterThatFailsWithAnythingButAnInterrupt_leavesNoTicketInTheQueue() throws Exception {
    var failOnce = new AtomicBoolean(true);
    var slots =
        new SagaSlots(
            () -> {
              if (failOnce.getAndSet(false)) {
                throw new IllegalStateException("the limit cannot be read");
              }
              return 1;
            });

    assertThatThrownBy(slots::acquire).isInstanceOf(IllegalStateException.class);

    assertThat(slots.waiting()).isZero();
    assertThat(slots.inUse()).isZero();
    assertThat(slots.heldByCurrentThread()).isFalse();
    try (SagaSlots.Slot slot = slots.tryAcquire(java.time.Duration.ofSeconds(5))) {
      assertThat(slot).as("the next acquire is not stuck behind a lost ticket").isNotNull();
    }
    assertThat(slots.inUse()).isZero();
  }

  @Test
  void anInterruptedWaiter_leavesTheQueueHoldingNothing_andTheNextOneGoesAhead() throws Exception {
    var slots = new SagaSlots(() -> 1);
    var order = new CopyOnWriteArrayList<String>();
    var release = new CountDownLatch(1);
    var holding = holder(slots, release, order, "holder");
    awaitUntil("held", () -> slots.inUse() == 1);
    var interrupted = new CompletableFuture<Throwable>();
    Thread waiter =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    slots.acquire().close();
                    interrupted.complete(null);
                  } catch (Throwable t) {
                    interrupted.complete(t);
                  }
                });
    awaitUntil("the waiter queues", () -> slots.waiting() == 1);
    var behind = holder(slots, new CountDownLatch(0), order, "behind");
    awaitUntil("a second waiter queues", () -> slots.waiting() == 2);

    waiter.interrupt();

    assertThat(interrupted.get(20, TimeUnit.SECONDS)).isInstanceOf(InterruptedException.class);
    awaitUntil("the interrupted waiter left the queue", () -> slots.waiting() == 1);
    release.countDown();
    CompletableFuture.allOf(holding, behind).get(20, TimeUnit.SECONDS);
    assertThat(order).containsExactly("holder", "behind");
    assertThat(slots.inUse()).isZero();
  }

  @Test
  void aThreadThatHoldsASlot_entersAgainWithoutASecondOne_andGivesItBackAtTheOutermostRelease()
      throws Exception {
    var slots = new SagaSlots(() -> 1);
    assertThat(slots.heldByCurrentThread()).isFalse();
    SagaSlots.Slot outer = slots.acquire();
    SagaSlots.Slot inner = slots.acquire();
    assertThat(slots.inUse()).isEqualTo(1);
    assertThat(slots.heldByCurrentThread()).isTrue();
    inner.close();
    inner.close(); // a second close does nothing
    assertThat(slots.heldByCurrentThread()).isTrue();
    assertThat(slots.inUse()).isEqualTo(1);
    outer.close();
    assertThat(slots.heldByCurrentThread()).isFalse();
    assertThat(slots.inUse()).isZero();
  }

  @Test
  void aTimedAcquire_givesUpAfterItsBound_holdingNothing_andTheNextInLineGoesAhead()
      throws Exception {
    var slots = new SagaSlots(() -> 1);
    var order = new CopyOnWriteArrayList<String>();
    var release = new CountDownLatch(1);
    var holding = holder(slots, release, order, "holder");
    awaitUntil("held", () -> order.contains("holder"));

    long start = System.nanoTime();
    assertThat(slots.tryAcquire(java.time.Duration.ofMillis(200))).isNull();
    assertThat(System.nanoTime() - start)
        .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(200));
    assertThat(slots.waiting()).isZero();
    assertThat(slots.heldByCurrentThread()).isFalse();

    release.countDown();
    holding.get(20, TimeUnit.SECONDS);
    try (SagaSlots.Slot slot = slots.tryAcquire(java.time.Duration.ofSeconds(5))) {
      assertThat(slot).isNotNull();
      assertThat(slots.inUse()).isEqualTo(1);
    }
    assertThat(slots.inUse()).isZero();
  }

  @Test
  void anInterruptedThread_takesAFreeSlotNobodyWaitsFor_andKeepsItsStatus() throws Exception {
    var slots = new SagaSlots(() -> 1);
    Thread.currentThread().interrupt();
    try (SagaSlots.Slot _ = slots.acquire()) {
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
      assertThat(slots.inUse()).isEqualTo(1);
    } finally {
      Thread.interrupted();
    }
    assertThat(slots.inUse()).isZero();
  }

  @Test
  void anInterruptedThread_thatMustWait_getsInterruptedException() throws Exception {
    var slots = new SagaSlots(() -> 1);
    var release = new CountDownLatch(1);
    var holding = holder(slots, release, new CopyOnWriteArrayList<>(), "holder");
    awaitUntil("held", () -> slots.inUse() == 1);
    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(slots::acquire).isInstanceOf(InterruptedException.class);
    } finally {
      Thread.interrupted();
      release.countDown();
    }
    holding.get(20, TimeUnit.SECONDS);
    assertThat(slots.waiting()).isZero();
  }
}
