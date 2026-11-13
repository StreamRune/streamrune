package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * A {@code streamrune.*} value Micronaut cannot convert refuses the start, naming the key and the
 * value, instead of binding the property's default without a word. The configuration reference
 * states it: a duration without its unit is not portable, and Micronaut refuses to start on it.
 */
class ConfigurationValueValidatorTest {

  /** Starts a context with {@code properties}; returns the bound record, or throws the refusal. */
  private static StreamRuneMicronautProperties start(Map<String, Object> properties) {
    try (ApplicationContext context = ApplicationContext.builder().properties(properties).build()) {
      context.start();
      return context.getBean(StreamRuneMicronautProperties.class);
    }
  }

  private static String refusal(Map<String, Object> properties) {
    RuntimeException refused = assertThrows(RuntimeException.class, () -> start(properties));
    StringBuilder messages = new StringBuilder();
    for (Throwable t = refused; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    return messages.toString();
  }

  private static void assertRefusalNames(String refusal, String... fragments) {
    for (String fragment : fragments) {
      assertTrue(
          refusal.contains(fragment),
          () -> "the refusal must say \"" + fragment + "\"; it says:\n" + refusal);
    }
  }

  @Test
  void aDurationWithoutItsUnitRefusesTheStart() {
    assertRefusalNames(
        refusal(Map.of("streamrune.outbox.flush-interval", "1000")),
        "streamrune.outbox.flush-interval=1000: a duration needs its unit, such as 5000ms");
  }

  /** YAML reads an unquoted number as an Integer: refused the same way. */
  @Test
  void aYamlIntegerForADurationRefusesTheStart() {
    assertRefusalNames(
        refusal(Map.of("streamrune.dead-letter.retry-interval", 60000)),
        "streamrune.dead-letter.retry-interval=60000");
  }

  /**
   * A bare zero is refused too: it would otherwise bind the seven-day default, and the retention
   * sweepers would delete the rows the operator asked to keep.
   */
  @Test
  void aBareZeroRefusesTheStart() {
    for (String key :
        List.of(
            "streamrune.inbox.retention-max-age",
            "streamrune.outbox.retention-max-age",
            "streamrune.dead-letter.retention-max-age",
            "streamrune.saga.dead-letter-retention-max-age",
            "streamrune.lock-timeout",
            "streamrune.event-store.statement-timeout",
            "streamrune.polling-jitter",
            "streamrune.sse.timeout")) {
      assertRefusalNames(refusal(Map.of(key, "0")), key + "=0");
    }
  }

  @Test
  void zeroWithItsUnitBindsZero() {
    var bound =
        start(
            Map.of(
                "streamrune.outbox.retention-max-age", "0s",
                "streamrune.polling-jitter", "0ms",
                "streamrune.lock-timeout", "PT0S"));
    assertEquals(Duration.ZERO, bound.outboxRetentionMaxAge());
    assertEquals(Duration.ZERO, bound.pollingJitter());
    assertEquals(Duration.ZERO, bound.lockTimeout());
  }

  @Test
  void aDurationWithItsUnitBindsAsWritten() {
    assertEquals(
        Duration.ofSeconds(1),
        start(Map.of("streamrune.outbox.flush-interval", "1s")).outboxFlushInterval());
  }

  /** Spring and Quarkus read an empty value as not set: Micronaut binds the default too. */
  @Test
  void anEmptyValueBindsTheDefault() {
    assertEquals(
        Duration.ofSeconds(5),
        start(Map.of("streamrune.outbox.flush-interval", "")).outboxFlushInterval());
  }

  /** The unit in upper case, a space before it, a decimal, an amount above an int: all refused. */
  @Test
  void otherSpellingsMicronautCannotReadRefuseTheStart() {
    for (String value : List.of("1D", "1 d", "1.5d", "2592000000ms", "soon")) {
      assertRefusalNames(
          refusal(Map.of("streamrune.inbox.retention-max-age", value)),
          "streamrune.inbox.retention-max-age=" + value,
          "cannot be read as Duration",
          "a duration needs its unit");
    }
  }

  /** Not durations only: every type the integration binds is converted as its bean converts it. */
  @Test
  void anUnreadableNumberRefusesTheStart() {
    assertRefusalNames(
        refusal(Map.of("streamrune.stripe-count", "many")),
        "streamrune.stripe-count=many",
        "cannot be read as int");
    assertTrue(
        !refusal(Map.of("streamrune.stripe-count", "many")).contains("a duration needs its unit"),
        "a number that is not a duration is not told about units");
    assertRefusalNames(
        refusal(Map.of("streamrune.retry-backoff-multiplier", "two")),
        "streamrune.retry-backoff-multiplier=two",
        "cannot be read as double");
  }

  /** The crypto properties, a record of its own, are read the same way. */
  @Test
  void anUnreadableCryptoValueRefusesTheStart() {
    assertRefusalNames(
        refusal(Map.of("streamrune.crypto.cache.expire-after-write", "60")),
        "streamrune.crypto.cache.expire-after-write=60");
  }

  /**
   * A duration bound through a {@code @Property} parameter rather than a properties record: the
   * saga compensation-retry interval, whose zero the sweeper refuses.
   */
  @Test
  void anUnreadablePropertyParameterRefusesTheStart() {
    assertRefusalNames(
        refusal(Map.of("streamrune.saga.compensation-retry-interval", "0")),
        "streamrune.saga.compensation-retry-interval=0");
    assertRefusalNames(
        refusal(Map.of("streamrune.saga.compensation-retry-give-up-after", "7200")),
        "streamrune.saga.compensation-retry-give-up-after=7200");
  }

  /** A factory method's {@code @Property} parameter, of type {@code long}. */
  @Test
  void anUnreadableFactoryParameterRefusesTheStart() {
    assertRefusalNames(
        refusal(Map.of("streamrune.subscription.health.lag-threshold", "lots")),
        "streamrune.subscription.health.lag-threshold=lots",
        "cannot be read as long");
  }

  /** A value is checked even when the feature that reads it is switched off, as on Spring. */
  @Test
  void aValueOfASwitchedOffFeatureIsCheckedToo() {
    assertRefusalNames(
        refusal(
            Map.of(
                "streamrune.saga.enabled", "false",
                "streamrune.saga.compensation-retry-interval", "soon")),
        "streamrune.saga.compensation-retry-interval=soon");
  }

  /** Every unreadable value is named in one refusal, so one restart shows them all. */
  @Test
  void everyUnreadableValueIsNamedAtOnce() {
    Map<String, Object> properties = new LinkedHashMap<>();
    properties.put("streamrune.lock-timeout", "-5");
    properties.put("streamrune.polling-interval", "5000");
    String refusal = refusal(properties);
    assertRefusalNames(
        refusal,
        "these configuration values",
        "streamrune.lock-timeout=-5",
        "streamrune.polling-interval=5000");
  }

  /**
   * A switch is {@code true} or {@code false}: Micronaut binds {@code FALSE} as {@code false} while
   * the cached crypto engine's condition, which compares the text, keeps the plaintext cache; and
   * binds {@code TRUE} as {@code true} while the Bean Validation interceptor's condition leaves it
   * out. Every other spelling is refused, naming the property and the value.
   */
  @Test
  void aSwitchSpelledOtherThanTrueOrFalseRefusesTheStart() {
    Map<String, String> spellings = new LinkedHashMap<>();
    spellings.put("streamrune.crypto.cache.enabled", "FALSE");
    spellings.put("streamrune.validation-enabled", "TRUE");
    spellings.put("streamrune.saga.enabled", "yes");
    spellings.put("streamrune.outbox.enabled", "1");
    spellings.put("streamrune.tracing.lock.enabled", "off");
    spellings.put("streamrune.metrics.enabled", "");
    spellings.forEach(
        (key, value) ->
            assertRefusalNames(
                refusal(Map.of(key, value)), key + "=" + value + ": a switch is true or false"));
  }

  /** A property only a bean condition reads, bound by no settings record, is held to it too. */
  @Test
  void aSwitchOnlyABeanConditionReadsIsCheckedToo() {
    for (String key :
        List.of(
            "streamrune.sse.enabled",
            "streamrune.runner-lifecycle-enabled",
            "streamrune.crypto.filesystem.enabled")) {
      assertRefusalNames(refusal(Map.of(key, "TRUE")), key + "=TRUE");
    }
  }

  @Test
  void aSwitchSpelledTrueOrFalseBindsAsWritten() {
    var bound =
        start(Map.of("streamrune.validation-enabled", "false", "streamrune.saga.enabled", "true"));
    assertEquals(false, bound.validationEnabled());
    assertEquals(true, bound.sagaEnabled());
  }

  /** Every duration property written with a {@code -ms} suffix is refused, naming the property. */
  @Test
  void aDurationPropertyWithAMillisecondSuffixRefusesTheStart() {
    for (String name :
        List.of(
            "streamrune.polling-interval-ms",
            "streamrune.lock-timeout-ms",
            "streamrune.sse.polling-interval-ms",
            "streamrune.saga.compensation-retry-interval-ms")) {
      String property = name.substring(0, name.length() - "-ms".length());
      assertRefusalNames(
          refusal(Map.of(name, "200")), name + " is not a StreamRune property; set " + property);
    }
  }
}
