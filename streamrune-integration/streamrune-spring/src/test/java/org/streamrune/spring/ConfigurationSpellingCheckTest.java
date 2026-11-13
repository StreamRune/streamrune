package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.mock.env.MockEnvironment;

/**
 * Spring reads a duration without its unit as milliseconds and binds {@code yes}, {@code on} or
 * {@code 1} as {@code true} while a bean condition with {@code havingValue = "true"} does not match
 * them: the check refuses both, and every duration property with a {@code -ms} suffix.
 */
class ConfigurationSpellingCheckTest {

  private static List<String> problems(Map<String, String> properties) {
    MockEnvironment environment = new MockEnvironment();
    properties.forEach(environment::setProperty);
    return ConfigurationSpellingCheck.problems(
        environment, ConfigurationSpellingCheckTest.class.getClassLoader());
  }

  @Test
  void theBoundPropertiesAreNamedAsSpringNamesThem() {
    var bound = ConfigurationSpellingCheck.boundProperties();
    assertEquals(int.class, bound.get("streamrune.snapshot-every-n-events"));
    assertEquals(Duration.class, bound.get("streamrune.outbox.flush-interval"));
    assertEquals(boolean.class, bound.get("streamrune.crypto.cache.enabled"));
    assertEquals(boolean.class, bound.get("streamrune.tracing.event-store.enabled"));
    assertEquals(String.class, bound.get("streamrune.subscription.single-active-consumer.enabled"));
  }

  /** A switch only a condition reads, such as a crypto backend's, is found in the class files. */
  @Test
  void theConditionSwitchesAreReadFromTheAutoConfigurations() {
    var switches = ConfigurationSpellingCheck.conditionSwitches(getClass().getClassLoader());
    for (String key :
        List.of(
            "streamrune.sse.enabled",
            "streamrune.validation-enabled",
            "streamrune.query-cache.enabled",
            "streamrune.crypto.filesystem.enabled",
            "streamrune.crypto.aws.enabled")) {
      assertTrue(switches.contains(key), key + " in " + switches);
    }
  }

  @Test
  void aCorrectlySpelledConfigurationHasNoProblem() {
    assertEquals(
        List.of(),
        problems(
            Map.of(
                "streamrune.outbox.flush-interval", "5000ms",
                "streamrune.inbox.retention-max-age", "0s",
                "streamrune.validation-enabled", "false",
                "streamrune.crypto.filesystem.enabled", "true",
                "streamrune.subscription.single-active-consumer.enabled", "off")));
  }

  @Test
  void aDurationWithoutItsUnitIsRefused() {
    assertEquals(
        List.of(
            "streamrune.lock-timeout=-5: a duration needs its unit, such as 5000ms, 30s or 0s,"
                + " or ISO-8601 such as PT30S"),
        problems(Map.of("streamrune.lock-timeout", "-5")));
  }

  /** {@code yes} binds {@code true} while the interceptor's condition does not match it. */
  @Test
  void aSwitchSpelledOtherThanTrueOrFalseIsRefused() {
    for (String spelling : List.of("yes", "on", "1", "TRUE", "")) {
      assertEquals(
          List.of(
              "streamrune.validation-enabled="
                  + spelling
                  + ": a switch is true or false, in lower case"),
          problems(Map.of("streamrune.validation-enabled", spelling)));
    }
    assertEquals(1, problems(Map.of("streamrune.crypto.vault.enabled", "TRUE")).size());
  }

  @Test
  void aDurationPropertyWithAMillisecondSuffixIsRefused() {
    var problems = problems(Map.of("streamrune.sse.keep-alive-interval-ms", "1000"));
    assertEquals(1, problems.size());
    assertTrue(
        problems
            .getFirst()
            .startsWith(
                "streamrune.sse.keep-alive-interval-ms is not a StreamRune property; set"
                    + " streamrune.sse.keep-alive-interval"),
        problems::getFirst);
  }
}
