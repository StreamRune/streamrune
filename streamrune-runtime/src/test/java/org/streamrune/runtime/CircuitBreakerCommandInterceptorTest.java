package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.DomainException;
import org.streamrune.core.ValidationException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;

class CircuitBreakerCommandInterceptorTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  record TestCommand() implements Command {}

  private static final CommandContext CTX =
      new CommandContext(
          new TestCommand(),
          "TestCommand",
          CommandId.of("test-id"),
          TYPE,
          AggregateId.of("agg-1"),
          null,
          Instant.now());

  @Test
  void starts_closed_and_allows_commands() {
    var cb = new CircuitBreakerCommandInterceptor(3, Duration.ofMinutes(5));
    assertTrue(cb.before(CTX));
    assertEquals("CLOSED", cb.circuitState());
  }

  @Test
  void opens_after_failure_threshold() {
    var cb = new CircuitBreakerCommandInterceptor(3, Duration.ofMinutes(5));
    cb.onError(CTX, new RuntimeException("fail"));
    cb.onError(CTX, new RuntimeException("fail"));
    assertEquals("CLOSED", cb.circuitState()); // 2 failures, threshold is 3
    cb.onError(CTX, new RuntimeException("fail"));
    assertEquals("OPEN", cb.circuitState());
  }

  @Test
  void rejects_when_open() {
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ofMinutes(5));
    cb.onError(CTX, new RuntimeException("fail"));
    assertThrows(CircuitBreakerOpenException.class, () -> cb.before(CTX));
  }

  @Test
  void transitions_to_half_open_after_cooldown_and_allows_probe() {
    // cooldown=ZERO means cooldown is always elapsed once the circuit opened
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ZERO);
    cb.onError(CTX, new RuntimeException("fail")); // OPEN
    assertEquals("OPEN", cb.circuitState());
    assertTrue(cb.before(CTX)); // cooldown elapsed → HALF_OPEN, probe through
    assertEquals("HALF_OPEN", cb.circuitState());
  }

  @Test
  void closes_after_probe_success() {
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ZERO);
    cb.onError(CTX, new RuntimeException("fail")); // OPEN
    cb.before(CTX); // probe, HALF_OPEN
    cb.after(CTX); // probe succeeded → CLOSED
    assertEquals("CLOSED", cb.circuitState());
    assertTrue(cb.before(CTX)); // normal commands allowed again
  }

  @Test
  void reopens_after_probe_failure() {
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ZERO);
    cb.onError(CTX, new RuntimeException("fail")); // OPEN
    cb.before(CTX); // probe, HALF_OPEN
    cb.onError(CTX, new RuntimeException("probe fail")); // probe failed → OPEN
    assertEquals("OPEN", cb.circuitState());
  }

  @Test
  void rejects_second_concurrent_request_in_half_open() {
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ZERO);
    cb.onError(CTX, new RuntimeException("fail")); // OPEN
    cb.before(CTX); // first probe through, probeSent=true, HALF_OPEN
    // second request while probe is in flight → rejected
    assertThrows(CircuitBreakerOpenException.class, () -> cb.before(CTX));
  }

  @Test
  void success_resets_consecutive_failure_count() {
    var cb = new CircuitBreakerCommandInterceptor(3, Duration.ofMinutes(5));
    cb.onError(CTX, new RuntimeException("fail")); // 1
    cb.onError(CTX, new RuntimeException("fail")); // 2
    cb.after(CTX); // success → reset to 0
    cb.onError(CTX, new RuntimeException("fail")); // 1
    cb.onError(CTX, new RuntimeException("fail")); // 2 — still below threshold
    assertEquals("CLOSED", cb.circuitState());
  }

  @Test
  void circuit_breaker_open_exception_not_counted_as_failure() {
    var cb = new CircuitBreakerCommandInterceptor(2, Duration.ofMinutes(5));
    cb.onError(CTX, new RuntimeException("real failure")); // 1
    cb.onError(CTX, new CircuitBreakerOpenException("open")); // not counted
    assertEquals("CLOSED", cb.circuitState()); // still only 1 real failure
  }

  @Test
  void client_errors_not_counted_as_failures_by_default() {
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ofMinutes(5));
    cb.onError(CTX, new ValidationException(List.of())); // IllegalArgumentException subtype
    cb.onError(CTX, new AuthorizationException("denied")); // DomainException subtype
    cb.onError(CTX, new DomainException("invariant violated"));
    cb.onError(CTX, new IllegalArgumentException("bad input"));
    assertEquals("CLOSED", cb.circuitState()); // threshold is 1, none counted
    assertTrue(cb.before(CTX)); // traffic still flows
  }

  @Test
  void custom_failure_predicate_controls_what_counts() {
    var cb =
        new CircuitBreakerCommandInterceptor(
            1, Duration.ofMinutes(5), Duration.ofMinutes(5), error -> true);
    cb.onError(CTX, new IllegalArgumentException("counts with custom predicate"));
    assertEquals("OPEN", cb.circuitState());
  }

  @Test
  void probe_failing_with_client_error_closes_circuit() {
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ZERO);
    cb.onError(CTX, new RuntimeException("fail")); // OPEN
    cb.before(CTX); // probe, HALF_OPEN
    // The probe reached the domain and was rejected for client reasons — infrastructure works.
    cb.onError(CTX, new ValidationException(List.of()));
    assertEquals("CLOSED", cb.circuitState());
    assertTrue(cb.before(CTX));
  }

  @Test
  void new_probe_allowed_after_probe_timeout_elapses() throws Exception {
    // probeTimeout=ZERO means any HALF_OPEN request may immediately supersede a silent probe
    var cb =
        new CircuitBreakerCommandInterceptor(
            1,
            Duration.ZERO,
            Duration.ZERO,
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES);
    cb.onError(CTX, new RuntimeException("fail")); // OPEN
    // First probe runs on another thread and never reports back (simulates a hung probe).
    Thread hungProbe = new Thread(() -> assertTrue(cb.before(CTX)));
    hungProbe.start();
    hungProbe.join();
    assertEquals("HALF_OPEN", cb.circuitState());
    // This thread takes over as the new probe instead of being rejected forever.
    assertTrue(cb.before(CTX));
    cb.after(CTX); // new probe succeeded → CLOSED
    assertEquals("CLOSED", cb.circuitState());
  }

  @Test
  void probe_within_timeout_still_rejects_other_requests() throws Exception {
    var cb =
        new CircuitBreakerCommandInterceptor(
            1,
            Duration.ZERO,
            Duration.ofMinutes(5),
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES);
    cb.onError(CTX, new RuntimeException("fail")); // OPEN
    Thread probe = new Thread(() -> assertTrue(cb.before(CTX)));
    probe.start();
    probe.join();
    assertEquals("HALF_OPEN", cb.circuitState());
    // Probe is in flight and well within its timeout — others are rejected.
    assertThrows(CircuitBreakerOpenException.class, () -> cb.before(CTX));
  }

  @Test
  void exactly_one_concurrent_request_wins_the_open_to_half_open_probe_slot() throws Exception {
    // The OPEN→HALF_OPEN transition is a compareAndSet: when many requests race after the
    // cooldown elapsed, exactly one may become the probe — every other racer must be rejected.
    // The long probeTimeout keeps the HALF_OPEN takeover path closed, so the CAS is the only
    // way through and the expected winner count is exactly 1 under any interleaving.
    var cb =
        new CircuitBreakerCommandInterceptor(
            1,
            Duration.ZERO,
            Duration.ofMinutes(5),
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES);
    cb.onError(CTX, new RuntimeException("fail")); // OPEN; zero cooldown → probe allowed
    assertEquals("OPEN", cb.circuitState());

    int threads = 32;
    var startGate = new CountDownLatch(1);
    var done = new CountDownLatch(threads);
    var admitted = new AtomicInteger(0);
    var rejected = new AtomicInteger(0);
    var unexpected = new AtomicReference<Throwable>();
    for (int i = 0; i < threads; i++) {
      Thread.ofVirtual()
          .start(
              () -> {
                try {
                  startGate.await();
                  if (cb.before(CTX)) {
                    admitted.incrementAndGet();
                  }
                } catch (CircuitBreakerOpenException _) {
                  rejected.incrementAndGet();
                } catch (Throwable t) {
                  unexpected.set(t);
                } finally {
                  done.countDown();
                }
              });
    }
    startGate.countDown();

    assertTrue(done.await(10, TimeUnit.SECONDS), "all racing requests must finish");
    assertNull(unexpected.get(), "no racer may fail with anything but CircuitBreakerOpenException");
    assertEquals(1, admitted.get(), "exactly one racing request may become the HALF_OPEN probe");
    assertEquals(threads - 1, rejected.get(), "every other racer is rejected");
    // The probe outcome (after()/onError() on the winner thread) driving HALF_OPEN→CLOSED/OPEN
    // is pinned by the single-threaded probe tests above; this test pins the CAS admission.
    assertEquals("HALF_OPEN", cb.circuitState());
  }

  @Test
  void constructor_validates_probe_timeout_and_predicate() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new CircuitBreakerCommandInterceptor(1, Duration.ZERO, null, error -> true));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new CircuitBreakerCommandInterceptor(
                1, Duration.ZERO, Duration.ofSeconds(-1), error -> true));
    assertThrows(
        IllegalArgumentException.class,
        () -> new CircuitBreakerCommandInterceptor(1, Duration.ZERO, Duration.ZERO, null));
  }

  @Test
  void constructor_validates_nano_time_source() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new CircuitBreakerCommandInterceptor(
                1, Duration.ZERO, Duration.ZERO, error -> true, null));
  }

  // -----------------------------------------------------------------------------------------------
  // Deterministic time-boundary tests. The breaker reads elapsed time from an injected monotonic
  // nano source (kept in nanos for monotonicity, never wall-clock), so the OPEN→HALF_OPEN cooldown
  // and the silent-probe takeover transitions are pinned to the exact nanosecond instead of being
  // exercised only at the Duration.ZERO / 5-minute extremes.
  // -----------------------------------------------------------------------------------------------

  /** Controllable monotonic nano source: tests set the value the breaker reads as "now". */
  private static final class FakeNanoClock implements java.util.function.LongSupplier {
    private long now;

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

  @Test
  void open_stays_rejecting_until_cooldown_elapses_then_admits_exactly_one_probe() {
    long start = 1_000_000_000L;
    long cooldownNanos = Duration.ofSeconds(5).toNanos();
    var clock = new FakeNanoClock(start);
    var cb =
        new CircuitBreakerCommandInterceptor(
            1,
            Duration.ofSeconds(5),
            Duration.ofMinutes(5),
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
            clock);

    cb.onError(CTX, new RuntimeException("fail")); // OPEN, openedAtNano == start
    assertEquals("OPEN", cb.circuitState());

    // One nanosecond before the cooldown elapses: still rejecting, still OPEN.
    clock.set(start + cooldownNanos - 1);
    assertThrows(CircuitBreakerOpenException.class, () -> cb.before(CTX));
    assertEquals("OPEN", cb.circuitState());

    // Exactly at the cooldown boundary (elapsed == cooldown, the check is >=): one probe admitted.
    clock.set(start + cooldownNanos);
    assertTrue(cb.before(CTX), "the cooldown boundary is inclusive — a probe is admitted");
    assertEquals("HALF_OPEN", cb.circuitState());

    // A second request at the same instant must be rejected — exactly one probe in flight.
    assertThrows(CircuitBreakerOpenException.class, () -> cb.before(CTX));
    assertEquals("HALF_OPEN", cb.circuitState());
  }

  @Test
  void silent_probe_is_superseded_exactly_at_the_probe_timeout_boundary() throws Exception {
    long start = 5_000_000_000L;
    long probeTimeoutNanos = Duration.ofSeconds(10).toNanos();
    var clock = new FakeNanoClock(start);
    var cb =
        new CircuitBreakerCommandInterceptor(
            1,
            Duration.ZERO, // zero cooldown: a probe is admitted as soon as the circuit opens
            Duration.ofSeconds(10),
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
            clock);

    cb.onError(CTX, new RuntimeException("fail")); // OPEN; openedAtNano == start

    // First probe runs on another thread (so its isProbe thread-local does not leak here) and never
    // reports back — a hung probe. probeStartedAtNano is stamped at "start".
    Thread hungProbe = new Thread(() -> assertTrue(cb.before(CTX)));
    hungProbe.start();
    hungProbe.join();
    assertEquals("HALF_OPEN", cb.circuitState());

    // One nanosecond before the probe timeout: the silent probe still owns the slot, others wait.
    clock.set(start + probeTimeoutNanos - 1);
    assertThrows(CircuitBreakerOpenException.class, () -> cb.before(CTX));
    assertEquals("HALF_OPEN", cb.circuitState());

    // Exactly at the probe-timeout boundary (elapsed == probeTimeout, the check is >=): this thread
    // takes over as the new probe.
    clock.set(start + probeTimeoutNanos);
    assertTrue(cb.before(CTX), "the probe-timeout boundary is inclusive — takeover is allowed");
    cb.after(CTX); // the new probe succeeds → CLOSED
    assertEquals("CLOSED", cb.circuitState());
  }
}
