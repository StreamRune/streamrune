package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {

  @Test
  void defaultPolicyShouldHaveExpectedValues() {
    var p = RetryPolicy.DEFAULT;
    assertEquals(3, p.maxAttempts());
    assertEquals(Duration.ofMillis(50), p.initialDelay());
    assertEquals(2.0, p.backoffMultiplier());
  }

  @Test
  void shouldRejectZeroMaxAttempts() {
    assertThrows(
        IllegalArgumentException.class, () -> new RetryPolicy(0, Duration.ofMillis(50), 2.0));
  }

  @Test
  void shouldRejectNegativeMaxAttempts() {
    assertThrows(
        IllegalArgumentException.class, () -> new RetryPolicy(-1, Duration.ofMillis(50), 2.0));
  }

  @Test
  void shouldRejectNullInitialDelay() {
    assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(3, null, 2.0));
  }

  @Test
  void shouldRejectNegativeInitialDelay() {
    assertThrows(
        IllegalArgumentException.class, () -> new RetryPolicy(3, Duration.ofMillis(-50), 2.0));
  }

  @Test
  void shouldRejectBackoffMultiplierBelowOne() {
    assertThrows(
        IllegalArgumentException.class, () -> new RetryPolicy(3, Duration.ofMillis(50), 0.5));
  }

  @Test
  void shouldAcceptMultiplierOfExactlyOne() {
    // Disable jitter for deterministic testing
    var p = new RetryPolicy(3, Duration.ofMillis(100), 1.0, false);
    assertEquals(Duration.ofMillis(100), p.delayForAttempt(1));
    assertEquals(Duration.ofMillis(100), p.delayForAttempt(2));
    assertEquals(Duration.ofMillis(100), p.delayForAttempt(3));
  }

  @Test
  void delayForAttemptShouldBeExponential() {
    // Disable jitter for deterministic testing
    var p = new RetryPolicy(5, Duration.ofMillis(100), 2.0, false);
    assertEquals(Duration.ofMillis(100), p.delayForAttempt(1)); // 100 * 2^0
    assertEquals(Duration.ofMillis(200), p.delayForAttempt(2)); // 100 * 2^1
    assertEquals(Duration.ofMillis(400), p.delayForAttempt(3)); // 100 * 2^2
    assertEquals(Duration.ofMillis(800), p.delayForAttempt(4)); // 100 * 2^3
  }

  @Test
  void delayForAttemptShouldRejectZeroAttempt() {
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.DEFAULT.delayForAttempt(0));
  }

  @Test
  void delayForAttemptShouldRejectNegativeAttempt() {
    assertThrows(IllegalArgumentException.class, () -> RetryPolicy.DEFAULT.delayForAttempt(-1));
  }

  @Test
  void jitteredDelayStaysWithinEqualJitterBounds() {
    // Equal jitter: uniform factor in [0.5, 1.5) applied to the base delay.
    var p = new RetryPolicy(5, Duration.ofMillis(100), 2.0, true);
    for (int i = 0; i < 200; i++) {
      long delayNanos = p.delayForAttempt(2).toNanos(); // base = 200ms
      assertTrue(
          delayNanos >= 100_000_000L && delayNanos < 300_000_000L,
          "jittered delay must be in [100ms, 300ms) but was " + delayNanos + "ns");
    }
  }

  @Test
  void subMillisecondInitialDelayIsNotTruncatedToZero() {
    var p = new RetryPolicy(3, Duration.ofNanos(500_000), 2.0, false);
    assertEquals(Duration.ofNanos(500_000), p.delayForAttempt(1));
    assertEquals(Duration.ofNanos(1_000_000), p.delayForAttempt(2));
  }

  @Test
  void pathologicalConfigurationSaturatesInsteadOfOverflowing() {
    var p = new RetryPolicy(Integer.MAX_VALUE, Duration.ofDays(365), 1000.0, true);
    Duration delay = p.delayForAttempt(1_000);
    assertFalse(delay.isNegative());
    assertEquals(Duration.ofNanos(Long.MAX_VALUE), delay);
  }

  // --- SnapshotPolicy ---

  @Test
  void snapshotPolicyEveryNEvents() {
    var p = SnapshotPolicy.everyNEvents(100);
    assertInstanceOf(SnapshotPolicy.EveryNEvents.class, p);
    assertEquals(100, ((SnapshotPolicy.EveryNEvents) p).n());
  }

  @Test
  void snapshotPolicyNever() {
    var p = SnapshotPolicy.never();
    assertInstanceOf(SnapshotPolicy.Never.class, p);
  }

  @Test
  void snapshotPolicyShouldRejectZeroN() {
    assertThrows(IllegalArgumentException.class, () -> SnapshotPolicy.everyNEvents(0));
  }

  @Test
  void snapshotPolicyShouldRejectNegativeN() {
    assertThrows(IllegalArgumentException.class, () -> SnapshotPolicy.everyNEvents(-1));
  }

  // --- Exceptions ---

  @Test
  void domainExceptionShouldCarryMessage() {
    var ex = new DomainException("Cart is closed");
    assertEquals("Cart is closed", ex.getMessage());
    assertInstanceOf(RuntimeException.class, ex);
  }

  @Test
  void optimisticLockExceptionShouldCarryMessage() {
    var ex = new OptimisticLockException("version mismatch");
    assertEquals("version mismatch", ex.getMessage());
    assertInstanceOf(RuntimeException.class, ex);
  }

  @Test
  void optimisticLockExceptionShouldCarryCause() {
    var cause = new RuntimeException("db error");
    var ex = new OptimisticLockException("version mismatch", cause);
    assertEquals("version mismatch", ex.getMessage());
    assertSame(cause, ex.getCause());
  }
}
