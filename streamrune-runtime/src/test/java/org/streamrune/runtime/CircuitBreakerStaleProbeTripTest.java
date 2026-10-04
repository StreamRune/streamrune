package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * A probe designation that the circuit outgrew must not be able to trip it.
 *
 * <p>The silent-probe takeover CASes only {@code probeStartedAtNano}; it never re-validates that
 * {@code state} is still HALF_OPEN, and the designation is never cleared when another thread closes
 * the circuit underneath the taker. {@code onError} then wrote {@code state.set(State.OPEN)}
 * unconditionally, so ONE failure on a thread carrying a stale designation slammed a healthy,
 * already-CLOSED circuit open and rejected every command on the bus for a full cooldown — with the
 * consecutive-failure threshold nowhere near reached.
 *
 * <p>The interleaving is driven with two real threads and latches (the probe designation lives in a
 * per-thread stack, so {@code before()} and its callback must run on the same thread) plus an
 * injected nanosecond clock for the timeout boundaries. No sleeps: every step waits on the latch
 * the previous step counts down.
 */
class CircuitBreakerStaleProbeTripTest {

  private static final AggregateType TYPE = AggregateType.of("agg");

  private static final Duration COOLDOWN = Duration.ofSeconds(10);
  private static final Duration PROBE_TIMEOUT = Duration.ofSeconds(30);
  private static final int THRESHOLD = 5;
  private static final long AWAIT_SECONDS = 10;

  record TestCommand() implements Command {}

  private static CommandContext ctx(CommandBus.CommandResult result) {
    return new CommandContext(
        new TestCommand(),
        "TestCommand",
        CommandId.of("cmd-1"),
        TYPE,
        AggregateId.of("agg-1"),
        result,
        Instant.now());
  }

  private static CommandContext executed() {
    return ctx(
        new CommandBus.CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("agg-1")),
            new Version(1),
            List.of(GlobalOffset.of(1))));
  }

  /** Controllable monotonic nano source, matching CircuitBreakerVetoAccountingTest's. */
  private static final class FakeNanoClock implements LongSupplier {
    private volatile long now;

    FakeNanoClock(long start) {
      this.now = start;
    }

    void set(long value) {
      this.now = value;
    }

    @Override
    public long getAsLong() {
      return now;
    }
  }

  private static void await(CountDownLatch latch, String what) throws InterruptedException {
    assertTrue(latch.await(AWAIT_SECONDS, TimeUnit.SECONDS), "timed out waiting for " + what);
  }

  @Test
  void supersededTakeover_cannotTripACircuitTheOriginalProbeAlreadyClosed() throws Exception {
    long start = 1_000_000_000L;
    var clock = new FakeNanoClock(start);
    var cb =
        new CircuitBreakerCommandInterceptor(
            THRESHOLD,
            COOLDOWN,
            PROBE_TIMEOUT,
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
            clock);

    // ---- open the circuit on the main thread -------------------------------------------------
    for (int i = 0; i < THRESHOLD; i++) {
      cb.onError(ctx(null), new RuntimeException("database down"));
    }
    assertEquals("OPEN", cb.circuitState());

    var probeDesignated = new CountDownLatch(1);
    var releaseProbe = new CountDownLatch(1);
    var probeReported = new CountDownLatch(1);
    var takeoverDesignated = new CountDownLatch(1);
    var releaseTakeover = new CountDownLatch(1);
    var takeoverReported = new CountDownLatch(1);
    var failure = new AtomicReference<Throwable>();

    // ---- P: the designated probe, which hangs on a stuck connection and reports LATE ----------
    Thread probe =
        new Thread(
            () -> {
              try {
                clock.set(start + COOLDOWN.toNanos()); // cooldown elapsed
                assertTrue(cb.before(ctx(null)), "cooldown elapsed — P is admitted as the probe");
                probeDesignated.countDown();
                await(releaseProbe, "P to be released");
                cb.after(executed()); // P finally returns — successfully
                probeReported.countDown();
              } catch (Throwable t) {
                failure.compareAndSet(null, t);
                probeDesignated.countDown();
                probeReported.countDown();
              }
            },
            "probe-P");

    // ---- B: takes over after probeTimeout, then hits ONE transient infrastructure failure -----
    Thread takeover =
        new Thread(
            () -> {
              try {
                assertTrue(
                    cb.before(ctx(null)), "P has been silent past probeTimeout — B takes over");
                takeoverDesignated.countDown();
                await(releaseTakeover, "B to be released");
                cb.onError(ctx(null), new RuntimeException("connection reset by peer"));
                takeoverReported.countDown();
              } catch (Throwable t) {
                failure.compareAndSet(null, t);
                takeoverDesignated.countDown();
                takeoverReported.countDown();
              }
            },
            "takeover-B");

    probe.start();
    await(probeDesignated, "P to be designated");
    assertEquals("HALF_OPEN", cb.circuitState());

    // P has now been silent for longer than probeTimeout.
    clock.set(start + COOLDOWN.toNanos() + PROBE_TIMEOUT.toNanos() + 1);
    takeover.start();
    await(takeoverDesignated, "B to take over the probe slot");

    // P returns successfully NOW, after B took over: the circuit is healthy and CLOSED again.
    releaseProbe.countDown();
    await(probeReported, "P to report its success");
    assertEquals(
        "CLOSED", cb.circuitState(), "P's successful probe closes the circuit (pre-condition)");

    // B's command then hits a single transient failure — one connection reset, far below the
    // threshold of 5 on a circuit that is CLOSED and healthy.
    releaseTakeover.countDown();
    await(takeoverReported, "B to report its failure");

    probe.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
    takeover.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
    if (failure.get() != null) {
      throw new AssertionError("worker thread failed", failure.get());
    }

    assertEquals(
        "CLOSED",
        cb.circuitState(),
        "a superseded probe designation must not reopen a circuit another thread already closed");
    assertTrue(
        cb.before(ctx(null)), "the healthy circuit must keep admitting commands after B's failure");
    cb.after(executed());
  }

  @Test
  void supersededTakeover_stillReopensACircuitThatIsStillHalfOpen() throws Exception {
    // The counterpart guard: narrowing the trip to a CAS must NOT stop a genuine probe failure
    // from reopening the circuit while the HALF_OPEN episode it was designated for is still live.
    long start = 1_000_000_000L;
    var clock = new FakeNanoClock(start);
    var cb =
        new CircuitBreakerCommandInterceptor(
            THRESHOLD,
            COOLDOWN,
            PROBE_TIMEOUT,
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
            clock);

    for (int i = 0; i < THRESHOLD; i++) {
      cb.onError(ctx(null), new RuntimeException("database down"));
    }
    clock.set(start + COOLDOWN.toNanos());
    assertTrue(cb.before(ctx(null)));
    assertEquals("HALF_OPEN", cb.circuitState());

    cb.onError(ctx(null), new RuntimeException("still down"));

    assertEquals("OPEN", cb.circuitState(), "a probe failure still reopens a HALF_OPEN circuit");
  }

  @Test
  void supersededProbeFailure_stillCountsTowardTheThreshold() throws Exception {
    // A failure that the CAS declines to act on is real evidence and must not be discarded: it
    // keeps counting toward the consecutive-failure threshold on the now-CLOSED circuit.
    long start = 1_000_000_000L;
    var clock = new FakeNanoClock(start);
    var cb =
        new CircuitBreakerCommandInterceptor(
            2,
            COOLDOWN,
            PROBE_TIMEOUT,
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
            clock);

    cb.onError(ctx(null), new RuntimeException("down"));
    cb.onError(ctx(null), new RuntimeException("down")); // threshold 2 → OPEN
    assertEquals("OPEN", cb.circuitState());

    var probeDesignated = new CountDownLatch(1);
    var releaseProbe = new CountDownLatch(1);
    var probeReported = new CountDownLatch(1);
    var failure = new AtomicReference<Throwable>();

    Thread probe =
        new Thread(
            () -> {
              try {
                clock.set(start + COOLDOWN.toNanos());
                assertTrue(cb.before(ctx(null)));
                probeDesignated.countDown();
                await(releaseProbe, "P to be released");
                cb.onError(ctx(null), new RuntimeException("connection reset by peer"));
                probeReported.countDown();
              } catch (Throwable t) {
                failure.compareAndSet(null, t);
                probeDesignated.countDown();
                probeReported.countDown();
              }
            },
            "probe-P");

    probe.start();
    await(probeDesignated, "P to be designated");
    // Another thread closes the circuit under P (a non-probe execution admitted while CLOSED is
    // not possible here, so drive it the same way the first test does: B takes over and succeeds).
    clock.set(start + COOLDOWN.toNanos() + PROBE_TIMEOUT.toNanos() + 1);
    assertTrue(cb.before(ctx(null)), "main takes over the stale probe slot");
    cb.after(executed()); // closes the circuit
    assertEquals("CLOSED", cb.circuitState());

    releaseProbe.countDown();
    await(probeReported, "P to report its failure");
    probe.join(TimeUnit.SECONDS.toMillis(AWAIT_SECONDS));
    if (failure.get() != null) {
      throw new AssertionError("worker thread failed", failure.get());
    }
    assertEquals("CLOSED", cb.circuitState(), "one failure, threshold 2 — still closed");

    // ...but it was counted: the very next eligible failure reaches the threshold.
    cb.onError(ctx(null), new RuntimeException("connection reset by peer"));
    assertEquals(
        "OPEN",
        cb.circuitState(),
        "the declined probe failure still counted toward the consecutive-failure threshold");
  }
}
