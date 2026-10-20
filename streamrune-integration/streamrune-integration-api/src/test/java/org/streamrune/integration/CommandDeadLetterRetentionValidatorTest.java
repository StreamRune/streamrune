package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class CommandDeadLetterRetentionValidatorTest {

  private static final Duration WEEK = Duration.ofDays(7);

  @Test
  void theDefaultsAreValid() {
    assertDoesNotThrow(() -> CommandDeadLetterRetentionValidator.validate(WEEK, WEEK));
  }

  @Test
  void aShorterDeadLetterWindowIsValid() {
    assertDoesNotThrow(
        () -> CommandDeadLetterRetentionValidator.validate(Duration.ofDays(3), WEEK));
  }

  @Test
  void aDeadLetterWindowLongerThanTheInboxWindowIsRefused() {
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> CommandDeadLetterRetentionValidator.validate(Duration.ofDays(30), WEEK));
    assertTrue(ex.getMessage().contains("streamrune.dead-letter.retention-max-age"));
    assertTrue(ex.getMessage().contains("streamrune.inbox.retention-max-age"));
    assertTrue(ex.getMessage().contains("PT720H"), ex.getMessage());
    assertTrue(ex.getMessage().contains("PT168H"), ex.getMessage());
  }

  @Test
  void anUnboundedDeadLetterWindowBesideAFiniteInboxWindowIsRefused() {
    for (Duration unbounded : new Duration[] {Duration.ZERO, Duration.ofDays(-1)}) {
      var ex =
          assertThrows(
              IllegalStateException.class,
              () -> CommandDeadLetterRetentionValidator.validate(unbounded, WEEK));
      assertTrue(ex.getMessage().contains("pruning disabled"), ex.getMessage());
    }
  }

  @Test
  void aNeverPrunedInboxCanNeverBeOutlived() {
    assertDoesNotThrow(
        () -> CommandDeadLetterRetentionValidator.validate(Duration.ofDays(365), Duration.ZERO));
    assertDoesNotThrow(
        () -> CommandDeadLetterRetentionValidator.validate(Duration.ZERO, Duration.ofSeconds(-1)));
  }

  @Test
  void anUnsetValueIsNotJudged() {
    assertDoesNotThrow(() -> CommandDeadLetterRetentionValidator.validate(null, WEEK));
    assertDoesNotThrow(() -> CommandDeadLetterRetentionValidator.validate(WEEK, null));
  }
}
