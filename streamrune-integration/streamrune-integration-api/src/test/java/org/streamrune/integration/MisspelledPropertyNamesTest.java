package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;

class MisspelledPropertyNamesTest {

  @Test
  void eachNameMapsToThePropertyToSetInstead() {
    assertEquals(7, MisspelledPropertyNames.all().size());
    assertEquals(
        "streamrune.polling-interval",
        MisspelledPropertyNames.all().get("streamrune.polling-interval-ms"));
    assertEquals(
        "streamrune.outbox.retry-backoff-multiplier",
        MisspelledPropertyNames.all().get("streamrune.outbox.retry-multiplier"));
  }

  @Test
  void aConfigurationWithoutThemIsAccepted() {
    assertDoesNotThrow(() -> MisspelledPropertyNames.refuseIfSet(name -> false));
  }

  @Test
  void everyNameThatIsSetIsRefusedWithThePropertyToSetInstead() {
    Set<String> set =
        Set.of("streamrune.outbox.flush-interval-ms", "streamrune.outbox.retry-multiplier");
    var refused =
        assertThrows(
            IllegalStateException.class, () -> MisspelledPropertyNames.refuseIfSet(set::contains));
    String message = refused.getMessage();
    assertTrue(
        message.contains(
            "streamrune.outbox.flush-interval-ms is not a StreamRune property; set"
                + " streamrune.outbox.flush-interval, a duration with its unit such as 500ms"
                + " instead"),
        message);
    assertTrue(
        message.contains(
            "streamrune.outbox.retry-multiplier is not a StreamRune property; set"
                + " streamrune.outbox.retry-backoff-multiplier instead"),
        message);
  }
}
