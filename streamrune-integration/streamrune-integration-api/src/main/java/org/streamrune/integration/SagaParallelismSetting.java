package org.streamrune.integration;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.streamrune.core.types.LogSanitizer;

/**
 * Parses {@code streamrune.saga.parallelism} and the per-subscription {@code
 * streamrune.saga.subscriptions.<name>.parallelism} values, so the three integrations read the same
 * numbers and refuse the same texts. The integrations bind the properties as text and hand them
 * over here.
 */
public final class SagaParallelismSetting {

  /** The global property name, {@value}. */
  public static final String PROPERTY = "streamrune.saga.parallelism";

  /** The prefix of the per-subscription properties, {@value}. */
  public static final String SUBSCRIPTIONS_PREFIX = "streamrune.saga.subscriptions.";

  /** The suffix of the per-subscription properties, {@value}. */
  public static final String SUBSCRIPTION_SUFFIX = ".parallelism";

  /** The parallelism when nothing is configured, {@value}. */
  public static final int DEFAULT = 4;

  private static final int MIN = 1;
  private static final int MAX = 64;

  private SagaParallelismSetting() {}

  /**
   * The parsed values.
   *
   * @param global the global parallelism
   * @param bySubscription the configured parallelism of each subscription that sets its own
   */
  public record Parsed(int global, Map<String, Integer> bySubscription) {
    /** Copies the map. */
    public Parsed {
      bySubscription = Collections.unmodifiableMap(new LinkedHashMap<>(bySubscription));
    }
  }

  /**
   * Parses the configured values.
   *
   * @param global the text of {@value #PROPERTY}; {@code null} or blank means {@value #DEFAULT}
   * @param bySubscription subscription name to the configured text of its parallelism; a {@code
   *     null} or blank text is refused like any other text that is not a number, because a property
   *     set to nothing is a mistake, not the default
   * @return the parsed values
   * @throws IllegalArgumentException if a value is not a whole number from 1 to 64; the message
   *     names the property, its subscription name sanitized for the log, and the value
   */
  public static Parsed parse(String global, Map<String, String> bySubscription) {
    int parsedGlobal = global == null || global.isBlank() ? DEFAULT : number(PROPERTY, global);
    Map<String, Integer> parsed = new LinkedHashMap<>();
    for (Map.Entry<String, String> entry : bySubscription.entrySet()) {
      String property =
          SUBSCRIPTIONS_PREFIX + LogSanitizer.sanitizeForLog(entry.getKey()) + SUBSCRIPTION_SUFFIX;
      parsed.put(entry.getKey(), number(property, entry.getValue()));
    }
    return new Parsed(parsedGlobal, parsed);
  }

  private static int number(String property, String text) {
    if (text == null) {
      throw refusal(property, text);
    }
    int value;
    try {
      value = Integer.parseInt(text.strip());
    } catch (NumberFormatException notANumber) {
      throw refusal(property, text);
    }
    if (value < MIN || value > MAX) {
      throw refusal(property, text);
    }
    return value;
  }

  private static IllegalArgumentException refusal(String property, String text) {
    return new IllegalArgumentException(
        property
            + " must be a whole number between "
            + MIN
            + " and "
            + MAX
            + "; got: '"
            + text
            + "'");
  }
}
