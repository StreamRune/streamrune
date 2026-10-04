package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The frozen {@code streamrune.lock-timeout} contract, tested where it is defined rather than once
 * per implementation.
 *
 * <p><b>Why this lives on the interface.</b> The contract is frozen here because one knob meant two
 * things depending on which {@link AggregateLocker} happened to be wired, and it closed the
 * negative arm by hand-copying the same {@code if} into two lockers and three boot validators. The
 * (0 ms, 1 ms) band then survived in all five: a positive sub-millisecond duration passed every
 * check, and {@code toMillis()} turned it into {@code lock_timeout = '0ms'} — PostgreSQL's DISABLED
 * — in {@code PgAdvisoryLocker} while {@code LocalStripedLocker} read it as the single-attempt
 * zero. Five hand-copied checks are five chances to miss a band, so the rule now exists once, here,
 * and every enforcement point delegates to it.
 */
class AggregateLockerTimeoutContractTest {

  @Test
  void positiveTimeoutsOfAtLeastOneMillisecondAreValid() {
    assertDoesNotThrow(() -> AggregateLocker.requireValidTimeout(Duration.ofSeconds(5), "t"));
    assertDoesNotThrow(() -> AggregateLocker.requireValidTimeout(Duration.ofMillis(1), "t"));
  }

  /** ZERO is the documented "do not wait at all" value, not a degenerate timeout. */
  @Test
  void zeroIsValid() {
    assertDoesNotThrow(() -> AggregateLocker.requireValidTimeout(Duration.ZERO, "t"));
  }

  /**
   * Null is each caller's own decision, not this method's: the boot validators see it when the
   * property is unset, and {@code PgAdvisoryLocker} substitutes its documented default before
   * calling. Deciding it here would hide that difference rather than resolve it.
   */
  @Test
  void nullValidatesNothing() {
    assertDoesNotThrow(() -> AggregateLocker.requireValidTimeout(null, "t"));
  }

  @Test
  void negativeIsRejectedAndNamesTheOffendingValue() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AggregateLocker.requireValidTimeout(
                    Duration.ofSeconds(-1), "streamrune.lock-timeout"));
    assertTrue(ex.getMessage().contains("streamrune.lock-timeout"));
    assertTrue(ex.getMessage().contains("must not be negative"));
    assertTrue(ex.getMessage().contains("PT-1S"));
  }

  /**
   * The band this contract is about. 0.5 ms is what {@code streamrune.lock-timeout=500us} (Spring)
   * and {@code PT0.0005S} (Quarkus/Micronaut) bind to.
   */
  @Test
  void positiveSubMillisecondIsRejected() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                AggregateLocker.requireValidTimeout(
                    Duration.ofNanos(500_000), "streamrune.lock-timeout"));
    assertTrue(ex.getMessage().contains("streamrune.lock-timeout"));
    assertTrue(ex.getMessage().contains("at least 1ms"));
  }

  /** One nanosecond is the extreme of the same band and must not be mistaken for ZERO. */
  @Test
  void oneNanosecondIsRejectedRatherThanTreatedAsDoNotWait() {
    assertThrows(
        IllegalArgumentException.class,
        () -> AggregateLocker.requireValidTimeout(Duration.ofNanos(1), "t"));
  }

  /**
   * Rejection, not rounding. Rounding 0.5 ms up to 1 ms would restore agreement between the two
   * lockers, but only by substituting a number the operator did not write — which the negative arm
   * of the same contract explicitly refuses ("not clamped to zero, not substituted with a default")
   * — and only for as long as every future implementation remembered to round the same way.
   */
  @Test
  void theFloorIsNotSilentlySubstituted() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> AggregateLocker.requireValidTimeout(Duration.ofNanos(999_999), "t"));
    assertTrue(
        ex.getMessage().contains("PT0.000999999S"),
        "the operator's own value must be echoed back, got: " + ex.getMessage());
  }
}
