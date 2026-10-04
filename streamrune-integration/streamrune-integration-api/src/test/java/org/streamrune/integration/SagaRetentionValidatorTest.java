package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class SagaRetentionValidatorTest {

  @Test
  void safeWhenGiveUpStrictlyBelowRetention() {
    // Default config (1h give-up, 7d retention) is safe → no warning.
    assertTrue(SagaRetentionValidator.validate(Duration.ofHours(1), Duration.ofDays(7)).isEmpty());
  }

  @Test
  void warnsWhenGiveUpEqualsRetention() {
    var warning = SagaRetentionValidator.validate(Duration.ofDays(7), Duration.ofDays(7));
    assertTrue(warning.isPresent());
    assertTrue(warning.get().contains("compensation-retry-give-up-after"));
    assertTrue(warning.get().contains("retention-max-age"));
  }

  @Test
  void warnsWithExactValuesWhenGiveUpExceedsRetention() {
    // A give-up horizon (10d) longer than inbox retention (7d) lets a COMPENSATING
    // episode outlive its dedup keys → the warning must name both exact values and the double-
    // execution risk.
    var warning =
        SagaRetentionValidator.validate(Duration.ofDays(10), Duration.ofDays(7)).orElseThrow();
    assertTrue(warning.contains("PT240H"), warning); // 10 days
    assertTrue(warning.contains("PT168H"), warning); // 7 days
    assertTrue(warning.contains("FAULTs it without re-dispatch"), warning);
    assertTrue(warning.contains("re-execute an already-succeeded compensation"), warning);
  }

  @Test
  void tolerantOfNulls() {
    assertFalse(SagaRetentionValidator.validate(null, Duration.ofDays(7)).isPresent());
    assertFalse(SagaRetentionValidator.validate(Duration.ofHours(1), null).isPresent());
  }

  @Test
  void noWarnWhenInboxPruningDisabled() {
    // Zero/negative inbox retention means the inbox sweeper is DISABLED — dedup keys
    // are kept forever, so a COMPENSATING episode can never outlive them and NO give-up horizon is
    // unsafe. Pre-fix, validate() compared giveUpAfter (1h) against the raw PT0S and emitted a
    // spurious boot WARN that 1h "must be strictly less than" PT0S — a claim that is both
    // unsatisfiable and pointless. Mirrors validateDeadLetterRetention's existing zero-handling.
    assertFalse(SagaRetentionValidator.validate(Duration.ofHours(1), Duration.ZERO).isPresent());
    assertFalse(
        SagaRetentionValidator.validate(Duration.ofDays(14), Duration.ofDays(-1)).isPresent());
  }

  // Boot-time fail-fast when the saga dead-letter window can outlive the inbox window.

  @Test
  void deadLetterRetention_failsFastWhenExceedingInboxRetention() {
    // 30d dead-letter > 7d inbox: a quarantined mid-compensation entry can outlive its inbox dedup
    // keys, so replaying it would re-execute an already-succeeded compensation (double refund).
    var thrown =
        assertThrows(
            IllegalStateException.class,
            () ->
                SagaRetentionValidator.validateDeadLetterRetention(
                    Duration.ofDays(30), Duration.ofDays(7)));
    assertTrue(thrown.getMessage().contains("dead-letter-retention-max-age"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("inbox.retention-max-age"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("must not exceed"), thrown.getMessage());
  }

  @Test
  void deadLetterRetention_bootsWhenEqualToInboxRetention() {
    // The framework default aligns the two (both 7d): dead-letter == inbox is safe → boots.
    assertDoesNotThrow(
        () ->
            SagaRetentionValidator.validateDeadLetterRetention(
                Duration.ofDays(7), Duration.ofDays(7)));
  }

  @Test
  void deadLetterRetention_bootsWhenBelowInboxRetention() {
    assertDoesNotThrow(
        () ->
            SagaRetentionValidator.validateDeadLetterRetention(
                Duration.ofDays(3), Duration.ofDays(7)));
  }

  @Test
  void deadLetterRetention_failsFastWhenDeadLetterPruningDisabledButInboxBounded() {
    // Zero/negative dead-letter retention disables its sweeper (kept forever) → outlives any finite
    // inbox window → fail fast.
    assertThrows(
        IllegalStateException.class,
        () ->
            SagaRetentionValidator.validateDeadLetterRetention(Duration.ZERO, Duration.ofDays(7)));
  }

  @Test
  void deadLetterRetention_safeWhenInboxPruningDisabled() {
    // Zero/negative inbox retention disables its sweeper (dedup keys kept forever) → no dead-letter
    // can outlive them → always safe, even with a long/unbounded dead-letter window.
    assertDoesNotThrow(
        () ->
            SagaRetentionValidator.validateDeadLetterRetention(Duration.ofDays(30), Duration.ZERO));
    assertDoesNotThrow(
        () -> SagaRetentionValidator.validateDeadLetterRetention(Duration.ZERO, Duration.ZERO));
  }

  @Test
  void deadLetterRetention_tolerantOfNulls() {
    assertDoesNotThrow(
        () -> SagaRetentionValidator.validateDeadLetterRetention(null, Duration.ofDays(7)));
    assertDoesNotThrow(
        () -> SagaRetentionValidator.validateDeadLetterRetention(Duration.ofDays(30), null));
  }
}
