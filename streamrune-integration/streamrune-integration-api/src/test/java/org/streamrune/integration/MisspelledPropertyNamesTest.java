package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class MisspelledPropertyNamesTest {

  private static final List<String> DURATIONS =
      List.of("streamrune.polling-interval", "streamrune.lock-timeout");

  @Test
  void everyDurationWithASuffixAndTheMultiplierAreNamed() {
    var names = MisspelledPropertyNames.of(DURATIONS);
    assertEquals(3, names.size());
    assertEquals("streamrune.polling-interval", names.get("streamrune.polling-interval-ms"));
    assertEquals("streamrune.lock-timeout", names.get("streamrune.lock-timeout-ms"));
    assertEquals(
        "streamrune.outbox.retry-backoff-multiplier",
        names.get("streamrune.outbox.retry-multiplier"));
  }

  @Test
  void aConfigurationWithoutThemHasNoProblem() {
    assertTrue(MisspelledPropertyNames.problems(DURATIONS, name -> false).isEmpty());
  }

  @Test
  void everyNameThatIsSetIsNamedWithThePropertyToSetInstead() {
    Set<String> set = Set.of("streamrune.lock-timeout-ms", "streamrune.outbox.retry-multiplier");
    assertEquals(
        List.of(
            "streamrune.lock-timeout-ms is not a StreamRune property; set"
                + " streamrune.lock-timeout, a duration with its unit such as 500ms instead",
            "streamrune.outbox.retry-multiplier is not a StreamRune property; set"
                + " streamrune.outbox.retry-backoff-multiplier instead"),
        MisspelledPropertyNames.problems(DURATIONS, set::contains));
  }
}
