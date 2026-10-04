package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class DeadLetterRetryPolicyTest {

  @Test
  void defaultPolicyHasExpectedValues() {
    var policy = DeadLetterRetryPolicy.DEFAULT;
    assertEquals(5, policy.maxRetries());
    assertEquals(Duration.ofSeconds(30), policy.initialDelay());
    assertEquals(2.0, policy.backoffMultiplier());
    assertFalse(policy.discardAfterMaxRetries());
  }

  @Test
  void rejectsMaxRetriesBelowOne() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeadLetterRetryPolicy(0, Duration.ofSeconds(1), 2.0, false));
  }

  @Test
  void rejectsNullInitialDelay() {
    assertThrows(
        IllegalArgumentException.class, () -> new DeadLetterRetryPolicy(3, null, 2.0, false));
  }

  @Test
  void rejectsNegativeInitialDelay() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeadLetterRetryPolicy(3, Duration.ofSeconds(-1), 2.0, false));
  }

  @Test
  void rejectsBackoffMultiplierBelowOne() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new DeadLetterRetryPolicy(3, Duration.ofSeconds(1), 0.5, false));
  }

  @Test
  void delayForAttemptOneIsNearInitialDelay() {
    var policy = new DeadLetterRetryPolicy(3, Duration.ofSeconds(10), 2.0, false);
    Duration delay = policy.delayForAttempt(1);
    assertTrue(
        delay.toMillis() >= 5000 && delay.toMillis() <= 15000,
        "delay should be between 5s and 15s but was " + delay);
  }

  @Test
  void delayForAttemptIncreasesAcrossAttempts() {
    var policy = new DeadLetterRetryPolicy(5, Duration.ofSeconds(1), 2.0, false);
    long sum1 = 0, sum3 = 0;
    for (int i = 0; i < 100; i++) {
      sum1 += policy.delayForAttempt(1).toMillis();
      sum3 += policy.delayForAttempt(3).toMillis();
    }
    assertTrue(sum3 > sum1, "attempt 3 average should be greater than attempt 1 average");
  }

  @Test
  void delayForAttemptRejectsZero() {
    var policy = DeadLetterRetryPolicy.DEFAULT;
    assertThrows(IllegalArgumentException.class, () -> policy.delayForAttempt(0));
  }
}
