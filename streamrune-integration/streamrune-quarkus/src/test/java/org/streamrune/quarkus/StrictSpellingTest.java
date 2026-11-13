package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.smallrye.config.WithConverter;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

/**
 * A duration without its unit and a switch spelled other than {@code true} or {@code false} are
 * refused when the configuration is mapped, naming the property and the value: Quarkus would read
 * {@code 5000} as seconds and {@code TRUE}, {@code 1} or {@code yes} as {@code true}, where Spring
 * and Micronaut read them otherwise.
 */
class StrictSpellingTest {

  private static String refusal(Map<String, String> properties) {
    RuntimeException refused =
        assertThrows(RuntimeException.class, () -> TestProperties.of(properties));
    StringBuilder messages = new StringBuilder();
    for (Throwable t = refused; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append('\n');
    }
    return messages.toString();
  }

  @Test
  void aDurationWithoutItsUnitIsRefused() {
    for (String bare : List.of("5000", "0", "-5")) {
      String refusal = refusal(Map.of("streamrune.outbox.flush-interval", bare));
      assertTrue(refusal.contains("streamrune.outbox.flush-interval"), refusal);
      assertTrue(refusal.contains(bare), refusal);
      assertTrue(refusal.contains("a duration needs its unit"), refusal);
    }
  }

  @Test
  void aDurationWithItsUnitIsReadAsQuarkusReadsIt() {
    var properties =
        TestProperties.of(
            Map.of(
                "streamrune.outbox.flush-interval", "5000ms",
                "streamrune.inbox.retention-max-age", "0s",
                "streamrune.lock-timeout", "PT2S",
                "streamrune.sse.timeout", "7d"));
    assertEquals(Duration.ofSeconds(5), properties.outbox().flushInterval());
    assertEquals(Duration.ZERO, properties.inbox().retentionMaxAge());
    assertEquals(Duration.ofSeconds(2), properties.lockTimeout());
    assertEquals(Duration.ofDays(7), properties.sse().timeout());
  }

  @Test
  void aSwitchSpelledOtherThanTrueOrFalseIsRefused() {
    for (String spelling : List.of("TRUE", "False", "yes", "on", "1")) {
      String refusal = refusal(Map.of("streamrune.validation-enabled", spelling));
      assertTrue(refusal.contains("streamrune.validation-enabled"), refusal);
      assertTrue(refusal.contains("a switch is true or false"), refusal);
    }
    var crypto =
        assertThrows(
            RuntimeException.class,
            () -> TestProperties.crypto(Map.of("streamrune.crypto.cache.enabled", "FALSE")));
    assertTrue(String.valueOf(crypto.getMessage()).contains("streamrune.crypto.cache.enabled"));
  }

  @Test
  void aSwitchSpelledTrueOrFalseIsRead() {
    assertFalse(
        TestProperties.of(Map.of("streamrune.validation-enabled", "false")).validationEnabled());
    assertTrue(TestProperties.of(Map.of("streamrune.outbox.enabled", "true")).outbox().enabled());
  }

  /** Every duration and switch of the two mappings names its strict converter. */
  @Test
  void everyDurationAndSwitchOfTheMappingsNamesItsConverter() {
    List<String> missing = new ArrayList<>();
    check(StreamRuneQuarkusProperties.class, missing);
    check(StreamRuneQuarkusCryptoProperties.class, missing);
    assertEquals(List.of(), missing);
  }

  private static void check(Class<?> mapping, List<String> missing) {
    for (Method method : mapping.getMethods()) {
      if (method.isDefault() || method.getParameterCount() > 0) {
        continue;
      }
      Class<?> type = method.getReturnType();
      boolean optionalDuration =
          type == Optional.class
              && method.getGenericReturnType() instanceof ParameterizedType optional
              && optional.getActualTypeArguments()[0] == Duration.class;
      Class<?> expected =
          type == Duration.class || optionalDuration
              ? StrictDurationConverter.class
              : type == boolean.class ? StrictBooleanConverter.class : null;
      if (expected != null) {
        WithConverter converter = method.getAnnotation(WithConverter.class);
        if (converter == null || converter.value() != expected) {
          missing.add(mapping.getSimpleName() + "." + method.getName());
        }
      } else if (type.isInterface() && type.getName().startsWith("org.streamrune.")) {
        check(type, missing);
      }
    }
  }
}
