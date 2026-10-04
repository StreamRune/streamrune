package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.LockException;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PgAdvisoryLockerTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void init() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  @Test
  void acquireAndReleaseLock() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);

    AutoCloseable lock =
        locker.acquireLock(TestStreams.stream("aggregate-1"), Duration.ofSeconds(5));
    assertNotNull(lock);

    // Releasing the lock should not throw
    lock.close();
  }

  @Test
  void acquireLockTwiceOnDifferentAggregates() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);

    AutoCloseable lock1 =
        locker.acquireLock(TestStreams.stream("aggregate-1"), Duration.ofSeconds(5));
    AutoCloseable lock2 =
        locker.acquireLock(TestStreams.stream("aggregate-2"), Duration.ofSeconds(5));

    assertNotNull(lock1);
    assertNotNull(lock2);

    lock2.close();
    lock1.close();
  }

  @Test
  void zeroTimeoutAcquiresFreeLockImmediately() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);

    // Duration.ZERO means "try once, do not wait" — a free lock must still be acquired.
    AutoCloseable lock = locker.acquireLock(TestStreams.stream("zero-free"), Duration.ZERO);
    assertNotNull(lock);
    lock.close();
  }

  @Test
  void zeroTimeoutFailsImmediatelyWhenLockIsHeld() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);
    var streamId = TestStreams.stream("zero-contended");

    try (var _ = locker.acquireLock(streamId, Duration.ofSeconds(5))) {
      long start = System.nanoTime();
      var ex =
          assertThrows(
              org.streamrune.core.LockException.class,
              () -> locker.acquireLock(streamId, Duration.ZERO));
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertTrue(
          ex.getMessage().contains("Advisory lock for stream: test:zero-contended is held"),
          ex.getMessage());
      assertTrue(elapsedMs < 1_000, "zero timeout must not wait, took " + elapsedMs + " ms");
    }
  }

  /**
   * The blocking-acquire path: thread A holds the advisory lock in an open transaction; thread B
   * calls {@code acquireLock(id, 2s)} and must block inside PostgreSQL until A releases, then
   * succeed. Asserts ordering — B observes the lock only after A has released it.
   */
  @Test
  void blockingAcquireWaitsThenSucceedsWhenHolderReleases() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);
    var streamId = TestStreams.stream("blocking-handoff");

    var aHasLock = new CountDownLatch(1);
    var bReady = new CountDownLatch(1);
    var releaseTimeNanos = new AtomicReference<Long>();
    var acquireTimeNanos = new AtomicReference<Long>();
    var bAcquired = new AtomicBoolean(false);
    var bError = new AtomicReference<Throwable>();

    // Thread A: acquire and hold the lock until B is parked waiting, then release.
    Thread holder =
        new Thread(
            () -> {
              try {
                AutoCloseable lock = locker.acquireLock(streamId, Duration.ofSeconds(5));
                aHasLock.countDown();
                // Wait until B has started its blocking acquire, then give it a moment to actually
                // block server-side before releasing.
                assertTrue(bReady.await(5, TimeUnit.SECONDS), "B never started");
                Thread.sleep(300);
                releaseTimeNanos.set(System.nanoTime());
                lock.close();
              } catch (Exception e) {
                bError.compareAndSet(null, e);
              }
            },
            "locker-holder-A");

    // Thread B: block on the same lock with a generous timeout; record when it finally acquires.
    Thread waiter =
        new Thread(
            () -> {
              try {
                assertTrue(aHasLock.await(5, TimeUnit.SECONDS), "A never acquired");
                bReady.countDown();
                try (var _ = locker.acquireLock(streamId, Duration.ofSeconds(2))) {
                  acquireTimeNanos.set(System.nanoTime());
                  bAcquired.set(true);
                }
              } catch (Throwable t) {
                bError.compareAndSet(null, t);
              }
            },
            "locker-waiter-B");

    holder.start();
    waiter.start();
    holder.join(10_000);
    waiter.join(10_000);

    assertNull(bError.get(), "no thread should have errored: " + bError.get());
    assertTrue(bAcquired.get(), "B must eventually acquire the lock once A releases");
    assertNotNull(releaseTimeNanos.get(), "A must have recorded its release");
    assertNotNull(acquireTimeNanos.get(), "B must have recorded its acquire");
    // Ordering: B only gets the lock after A releases it (server-side wakeup, not poll).
    assertTrue(
        acquireTimeNanos.get() >= releaseTimeNanos.get(),
        "B acquired before A released — locks were not mutually exclusive");
  }

  /**
   * The {@code lock_timeout} -> SQLSTATE 55P03 -> {@link LockException} path. Thread A holds the
   * lock; thread B calls {@code acquireLock(id, ~200ms)} and must throw a {@link LockException}
   * naming the stream and "timeout". The elapsed time proving the wait was bounded server-side by
   * {@code lock_timeout} (not a client poll loop) is roughly the configured timeout, well under the
   * hold duration.
   */
  @Test
  void acquireThrowsLockExceptionWhenLockTimeoutExpires() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);
    var streamId = TestStreams.stream("lock-timeout-victim");
    var timeout = Duration.ofMillis(200);

    try (var _ = locker.acquireLock(streamId, Duration.ofSeconds(5))) {
      long start = System.nanoTime();
      var ex = assertThrows(LockException.class, () -> locker.acquireLock(streamId, timeout));
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;

      assertTrue(
          ex.getMessage().contains("advisory lock for stream: test:lock-timeout-victim"),
          "message must name the stream: " + ex.getMessage());
      assertTrue(
          ex.getMessage().toLowerCase().contains("timeout"),
          "message must mention the timeout: " + ex.getMessage());
      // Server-side lock_timeout fired at ~200ms; allow slack for scheduling/round-trip but it must
      // be well above 0 (proving it waited) and well below the 5s hold (proving it gave up early).
      assertTrue(
          elapsedMs >= 150,
          "should have waited ~200ms server-side, only waited " + elapsedMs + " ms");
      assertTrue(
          elapsedMs < 2_000,
          "should have given up near the 200ms timeout, waited " + elapsedMs + " ms");
    }
  }

  /**
   * The (0 ms, 1 ms) band. {@code streamrune.lock-timeout=500us} (Spring's DurationStyle accepts
   * {@code us}/{@code ns}; Quarkus and Micronaut accept {@code PT0.0005S}) is positive, so it
   * passed the negative rejection and all three boot validators, and {@code isZero()} is false, so
   * it routed to the blocking arm — where {@code toMillis()} truncated it to {@code SET LOCAL
   * lock_timeout = '0ms'}, which PostgreSQL reads as DISABLED. The operator asked for a 0.5 ms
   * bound and got {@code pg_advisory_xact_lock} blocking server-side FOREVER, pinning one pool
   * connection per waiting command, while {@code LocalStripedLocker} on the identical value made a
   * single attempt — the one-knob/two-behaviours divergence that froze the {@link
   * org.streamrune.core.AggregateLocker} contract to eliminate.
   *
   * <p>The waiter runs on a daemon thread with a watchdog because the pre-fix behaviour is an
   * indefinite hang: without the join timeout this test would never return. Post-fix the value is
   * rejected before any connection is checked out, so the watchdog never fires.
   */
  @Test
  void subMillisecondTimeoutIsRejectedRatherThanTurnedIntoAnUnboundedWait() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);
    var streamId = TestStreams.stream("sub-millisecond-band");
    var subMillisecond = Duration.ofNanos(500_000); // 0.5 ms — "500us" / "PT0.0005S"

    try (var _ = locker.acquireLock(streamId, Duration.ofSeconds(5))) {
      var outcome = new AtomicReference<Throwable>();
      var returned = new CountDownLatch(1);
      Thread waiter =
          new Thread(
              () -> {
                try (var _ = locker.acquireLock(streamId, subMillisecond)) {
                  outcome.set(new AssertionError("acquired a lock another session holds"));
                } catch (Throwable t) {
                  outcome.set(t);
                } finally {
                  returned.countDown();
                }
              },
              "sub-ms-waiter");
      waiter.setDaemon(true); // pre-fix this thread never returns; it must not keep the JVM up
      waiter.start();

      assertTrue(
          returned.await(20, TimeUnit.SECONDS),
          "a positive sub-millisecond lock timeout blocked indefinitely — toMillis() truncated it"
              + " to lock_timeout = '0ms', which PostgreSQL treats as DISABLED");
      assertInstanceOf(
          IllegalArgumentException.class,
          outcome.get(),
          "a sub-millisecond timeout is a configuration error, not a transient acquisition"
              + " failure, so it must not go through the retryable LockException channel; got: "
              + outcome.get());
      assertTrue(
          outcome.get().getMessage().contains("1ms"),
          "message must name the millisecond floor, got: " + outcome.get().getMessage());
    }
  }

  /**
   * The floor itself stays a working value: 1 ms is the smallest bound PostgreSQL's {@code
   * lock_timeout} can express, and it must still bound a contended acquire rather than being read
   * as "disabled".
   */
  @Test
  void oneMillisecondTimeoutStillFailsFastOnAHeldLock() throws Exception {
    var locker = new PgAdvisoryLocker(dataSource);
    var streamId = TestStreams.stream("one-millisecond-floor");

    try (var _ = locker.acquireLock(streamId, Duration.ofSeconds(5))) {
      long start = System.nanoTime();
      var ex =
          assertThrows(
              LockException.class, () -> locker.acquireLock(streamId, Duration.ofMillis(1)));
      long elapsedMs = (System.nanoTime() - start) / 1_000_000;
      assertTrue(ex.getMessage().contains("one-millisecond-floor"));
      assertTrue(elapsedMs < 5_000, "1ms must be a bound, not \"disabled\"; waited " + elapsedMs);
    }
  }

  /**
   * Defence in depth for sub-millisecond timeouts: the contract check on {@code acquireLock} is
   * what a configured value meets, but the SQL emit site refuses to write {@code lock_timeout =
   * '0ms'} on its own account. Clamping there instead would have silently absorbed a future caller
   * that skipped the contract — which is how the (0 ms, 1 ms) band survived the original fix in the
   * first place. It throws before touching the connection, so {@code null} is a legitimate argument
   * here.
   */
  @Test
  void theSqlEmitSiteRefusesToWriteADisabledLockTimeout() {
    var locker = new PgAdvisoryLocker(dataSource);
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> locker.acquireBlocking(null, 42L, Duration.ofNanos(1)));
    assertTrue(
        ex.getMessage().contains("lock_timeout"),
        "message must name what it refused to emit, got: " + ex.getMessage());
  }
}
