package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.LockException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;

class LocalStripedLockerTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  @Test
  void acquireAndReleaseLock() throws Exception {
    var locker = new LocalStripedLocker(16);
    var lock =
        locker.acquireLock(StreamId.of(TYPE, AggregateId.of("agg-1")), Duration.ofSeconds(1));
    assertNotNull(lock);
    lock.close();
  }

  @Test
  void sequentialAccessToSameIdWorks() throws Exception {
    var locker = new LocalStripedLocker(16);

    // First acquire and release
    var lock1 =
        locker.acquireLock(StreamId.of(TYPE, AggregateId.of("agg-1")), Duration.ofSeconds(1));
    lock1.close();

    // Second acquire on the same id must succeed. It runs on ANOTHER thread: the stripes are
    // ReentrantLocks, so a re-acquire on this thread would succeed even if close() released
    // nothing, and the test could never catch a missing release.
    var failure = new AtomicReference<Throwable>();
    var thread =
        Thread.ofVirtual()
            .start(
                () -> {
                  try (var lock2 =
                      locker.acquireLock(
                          StreamId.of(TYPE, AggregateId.of("agg-1")), Duration.ofSeconds(1))) {
                    assertNotNull(lock2);
                  } catch (Throwable t) {
                    failure.set(t);
                  }
                });
    assertTrue(thread.join(Duration.ofSeconds(5)), "the second acquire must not hang");
    assertNull(
        failure.get(),
        () -> "a released lock must be acquirable from another thread, got: " + failure.get());
  }

  @Test
  void rejectsInvalidStripeCount() {
    assertThrows(IllegalArgumentException.class, () -> new LocalStripedLocker(0));
    assertThrows(IllegalArgumentException.class, () -> new LocalStripedLocker(-1));
  }

  /**
   * {@code streamrune.lock-timeout} is handed verbatim to whichever {@link
   * org.streamrune.core.AggregateLocker} is wired, and a NEGATIVE value used to mean two different
   * things — this locker did {@code tryLock(negativeMillis)} (one attempt, no wait) while {@code
   * PgAdvisoryLocker} substituted a hardcoded 30-second default and BLOCKED. Moving from the
   * auto-configured default locker to the documented multi-replica upgrade silently changed the
   * behaviour of an unchanged configuration. The frozen rule is "zero means do not wait; a negative
   * duration is never a valid StreamRune value", so both lockers now reject it loudly.
   */
  @Test
  void rejectsNegativeTimeout() {
    var locker = new LocalStripedLocker(16);
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                locker.acquireLock(
                    StreamId.of(TYPE, AggregateId.of("agg-negative")), Duration.ofSeconds(-1)));
    assertTrue(
        ex.getMessage().contains("negative"),
        "message must name the offending contract, got: " + ex.getMessage());
  }

  /**
   * The same divergence one band lower. A positive sub-millisecond timeout reached {@code
   * tryLock(timeout.toMillis(), MILLISECONDS)} here as {@code tryLock(0, ...)} — the ZERO arm's
   * single attempt — while {@code PgAdvisoryLocker} turned the identical value into {@code
   * lock_timeout = '0ms'}, which PostgreSQL reads as DISABLED, i.e. wait forever. Both lockers now
   * reject it through the one shared contract check.
   */
  @Test
  void rejectsPositiveSubMillisecondTimeout() {
    var locker = new LocalStripedLocker(16);
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                locker.acquireLock(
                    StreamId.of(TYPE, AggregateId.of("agg-sub-ms")), Duration.ofNanos(500_000)));
    assertTrue(
        ex.getMessage().contains("at least 1ms"),
        "message must name the millisecond floor, got: " + ex.getMessage());
  }

  /** The floor itself stays a working bound, not a rejected one. */
  @Test
  void oneMillisecondTimeoutIsAccepted() throws Exception {
    var locker = new LocalStripedLocker(16);
    try (var lock =
        locker.acquireLock(StreamId.of(TYPE, AggregateId.of("agg-one-ms")), Duration.ofMillis(1))) {
      assertNotNull(lock);
    }
  }

  /** The other half of the frozen contract: {@link Duration#ZERO} is valid and means "try once". */
  @Test
  void zeroTimeoutAcquiresWhenUncontended() throws Exception {
    var locker = new LocalStripedLocker(16);
    try (var lock =
        locker.acquireLock(StreamId.of(TYPE, AggregateId.of("agg-zero")), Duration.ZERO)) {
      assertNotNull(lock);
    }
  }

  @Test
  void zeroTimeoutDoesNotWaitWhenContended() throws Exception {
    var locker = new LocalStripedLocker(1); // single stripe, guaranteed collision
    try (var _ =
        locker.acquireLock(
            StreamId.of(TYPE, AggregateId.of("agg-zero-contended")), Duration.ofSeconds(5))) {
      var failure = new AtomicReference<Throwable>();
      var thread =
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      locker.acquireLock(
                          StreamId.of(TYPE, AggregateId.of("agg-zero-contended")), Duration.ZERO);
                    } catch (Throwable t) {
                      failure.set(t);
                    }
                  });
      thread.join(TimeUnit.SECONDS.toMillis(5));
      assertInstanceOf(
          LockException.class,
          failure.get(),
          "zero timeout must fail fast, not block or throw IAE");
    }
  }

  @Test
  void timeoutWhenLockHeldByAnotherThread() throws Exception {
    var locker = new LocalStripedLocker(1); // single stripe, guaranteed collision
    var lock =
        locker.acquireLock(StreamId.of(TYPE, AggregateId.of("agg-1")), Duration.ofSeconds(5));

    // Try to acquire same stripe from a different thread with short timeout
    var thread =
        Thread.ofVirtual()
            .start(
                () ->
                    assertThrows(
                        RuntimeException.class,
                        () ->
                            locker.acquireLock(
                                StreamId.of(TYPE, AggregateId.of("agg-1")),
                                Duration.ofMillis(50))));
    thread.join();

    lock.close();
  }

  @Test
  void acquisitionTimeoutThrowsTheUnifiedLockExceptionType() throws Exception {
    // The local striped locker's failure must be catchable as the canonical LockException — the
    // same supertype PgAdvisoryLocker throws — so callers (and the command bus) handle every
    // locker's acquisition failure uniformly. It is still a LockAcquisitionException carrying the
    // id.
    // Contend from a DIFFERENT thread: ReentrantLock is reentrant, so the holder would re-acquire.
    var locker = new LocalStripedLocker(1); // single stripe, guaranteed collision
    var held =
        locker.acquireLock(StreamId.of(TYPE, AggregateId.of("agg-1")), Duration.ofSeconds(5));
    try {
      var thrown = new AtomicReference<Throwable>();
      var thread =
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      locker
                          .acquireLock(
                              StreamId.of(TYPE, AggregateId.of("agg-1")), Duration.ofMillis(50))
                          .close();
                    } catch (Throwable t) {
                      thrown.set(t);
                    }
                  });
      thread.join();
      assertInstanceOf(LockException.class, thrown.get());
      assertInstanceOf(LockAcquisitionException.class, thrown.get());
    } finally {
      held.close();
    }
  }

  @Test
  void differentAggregateIdsCollidingOnOneStripeContendForTheSameLock() throws Exception {
    // Striping intentionally maps multiple aggregate ids onto one lock. With a single stripe
    // every id collides, so a DIFFERENT id must still block while the stripe is held.
    var locker = new LocalStripedLocker(1);
    var lock =
        locker.acquireLock(StreamId.of(TYPE, AggregateId.of("agg-1")), Duration.ofSeconds(5));

    var timedOut = new AtomicBoolean(false);
    var thread =
        Thread.ofVirtual()
            .start(
                () -> {
                  try {
                    locker.acquireLock(
                        StreamId.of(TYPE, AggregateId.of("agg-2")), Duration.ofMillis(50));
                  } catch (LockAcquisitionException _) {
                    timedOut.set(true);
                  }
                });
    thread.join();

    assertTrue(
        timedOut.get(),
        "a different aggregate id hashing to the held stripe must contend, not acquire");
    lock.close();
  }

  @Test
  void concurrentAcquisitionsOnSameIdAreMutuallyExclusive() throws Exception {
    var locker = new LocalStripedLocker(4);
    int threads = 64;
    // Plain int on purpose: increments are only safe when the lock serializes them and
    // establishes happens-before between consecutive holders.
    int[] counter = new int[1];
    var startGate = new CountDownLatch(1);
    var done = new CountDownLatch(threads);

    for (int i = 0; i < threads; i++) {
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  startGate.await();
                  try (var _ =
                      locker.acquireLock(
                          StreamId.of(TYPE, AggregateId.of("agg-contended")),
                          Duration.ofSeconds(10))) {
                    counter[0]++;
                  }
                } catch (Exception _) {
                  // Leave the counter short — the assertion below reports the lost increment.
                } finally {
                  done.countDown();
                }
              });
    }
    startGate.countDown();

    assertTrue(done.await(30, TimeUnit.SECONDS), "all acquisitions must complete");
    assertEquals(threads, counter[0], "the lock must serialize all increments on one aggregate id");
  }

  private static final StreamId PRODUCT_X =
      StreamId.of(AggregateType.of("product"), AggregateId.of("x"));
  private static final StreamId INVENTORY_X =
      StreamId.of(AggregateType.of("inventory"), AggregateId.of("x"));

  /** The locker's own stripe function, over the stream's text form. */
  private static int stripeOf(StreamId streamId, int stripes) {
    int hash = java.util.Objects.hash(streamId.value());
    hash ^= hash >>> 16;
    return Math.floorMod(hash, stripes);
  }

  @Test
  void twoTypesSharingAnIdValue_onDifferentStripes_lockIndependently() throws Exception {
    int stripes =
        java.util.stream.IntStream.rangeClosed(2, 64)
            .filter(n -> stripeOf(PRODUCT_X, n) != stripeOf(INVENTORY_X, n))
            .findFirst()
            .orElseThrow();
    var locker = new LocalStripedLocker(stripes);
    try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        var _ = locker.acquireLock(PRODUCT_X, Duration.ofSeconds(1))) {
      Boolean acquired =
          executor
              .submit(
                  () -> {
                    try (var _ = locker.acquireLock(INVENTORY_X, Duration.ZERO)) {
                      return true;
                    }
                  })
              .get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertTrue(acquired, "inventory:x is not blocked by product:x");
    }
  }

  @Test
  void aContendedAcquisition_failsWithTheTypedStream() throws Exception {
    var locker = new LocalStripedLocker(1);
    try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();
        var _ = locker.acquireLock(PRODUCT_X, Duration.ofSeconds(1))) {
      LockAcquisitionException failure =
          executor
              .submit(
                  () -> {
                    try {
                      locker.acquireLock(PRODUCT_X, Duration.ZERO).close();
                      return null;
                    } catch (LockAcquisitionException e) {
                      return e;
                    }
                  })
              .get(5, java.util.concurrent.TimeUnit.SECONDS);
      assertEquals(PRODUCT_X, failure.streamId());
      assertTrue(failure.toString().contains("streamId=product:x"), failure.toString());
    }
  }
}
