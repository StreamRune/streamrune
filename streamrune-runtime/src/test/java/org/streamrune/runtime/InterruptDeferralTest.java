package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class InterruptDeferralTest {

  @AfterEach
  void clearFlag() {
    Thread.interrupted();
  }

  @Test
  void outsideADeferredSection_theInterruptIsDeliveredAtOnce() throws Exception {
    var deferral = new InterruptDeferral();
    var sleeping = new CountDownLatch(1);
    var interrupted = new AtomicBoolean();
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  sleeping.countDown();
                  try {
                    Thread.sleep(60_000);
                  } catch (InterruptedException _) {
                    interrupted.set(true);
                  }
                });
    assertTrue(sleeping.await(5, TimeUnit.SECONDS));

    deferral.interrupt(t);
    t.join(5_000);

    assertTrue(interrupted.get(), "the sleep was cut short");
  }

  @Test
  void insideADeferredSection_theInterruptIsHeldUntilResume() throws Exception {
    var deferral = new InterruptDeferral();
    var deferred = new CountDownLatch(1);
    var interruptRequested = new CountDownLatch(1);
    var flagDuringSection = new AtomicBoolean(true);
    var flagAfterResume = new AtomicBoolean();
    Thread t =
        Thread.ofVirtual()
            .start(
                () -> {
                  boolean stopping = deferral.defer();
                  deferred.countDown();
                  try {
                    interruptRequested.await();
                  } catch (InterruptedException _) {
                    return; // would mean the interrupt was not held back
                  }
                  flagDuringSection.set(stopping || Thread.currentThread().isInterrupted());
                  deferral.resume();
                  flagAfterResume.set(Thread.currentThread().isInterrupted());
                });
    assertTrue(deferred.await(5, TimeUnit.SECONDS));

    deferral.interrupt(t);
    interruptRequested.countDown();
    t.join(5_000);

    assertFalse(flagDuringSection.get(), "no interrupt reaches the deferred section");
    assertTrue(flagAfterResume.get(), "the held interrupt is delivered on resume");
  }

  @Test
  void aFlagAlreadySetWhenTheSectionStarts_isClearedAndReportedAsAStop() {
    var deferral = new InterruptDeferral();
    Thread.currentThread().interrupt();

    boolean stopping = deferral.defer();
    boolean flagInSection = Thread.currentThread().isInterrupted();
    deferral.resume();

    assertTrue(stopping);
    assertFalse(flagInSection, "the section runs with the flag cleared");
    assertTrue(Thread.currentThread().isInterrupted(), "the stop is handed back on resume");
  }

  @Test
  void withoutAStop_resumeLeavesTheFlagClear() {
    var deferral = new InterruptDeferral();

    assertFalse(deferral.defer());
    deferral.resume();

    assertFalse(Thread.currentThread().isInterrupted());
  }

  @Test
  void aStopRequestedFromTheSectionItself_isDeliveredOnResume() {
    var deferral = new InterruptDeferral();

    deferral.defer();
    deferral.interrupt(Thread.currentThread()); // close() called from within the poll thread
    boolean flagInSection = Thread.currentThread().isInterrupted();
    deferral.resume();

    assertFalse(flagInSection);
    assertTrue(Thread.currentThread().isInterrupted());
  }
}
