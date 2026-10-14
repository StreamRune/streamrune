package org.streamrune.integration;

import java.util.Locale;

/**
 * The value of {@code streamrune.saga.forward-fence}: whether the {@code SagaRunner} beans that
 * named no forward fence themselves run with it on, off, or as {@code
 * streamrune.subscription.single-active-consumer.enabled} says.
 *
 * <ul>
 *   <li>{@link #AUTO} (the default) — on when single-active-consumer leadership is enabled, off
 *       when it is not
 *   <li>{@link #ON} — on
 *   <li>{@link #OFF} — off
 * </ul>
 *
 * <p>The three integrations bind the property as a {@code String} and parse it here, so all three
 * read the same spellings. YAML 1.1 reads an unquoted {@code on}/{@code off} as a boolean, and the
 * Spring and Micronaut binders then hand over the text {@code true}/{@code false}; those, and the
 * other boolean spellings {@link RelaxedBoolean} accepts, mean {@link #ON}/{@link #OFF}.
 */
public enum SagaForwardFenceSetting {
  AUTO,
  ON,
  OFF;

  /** The property name, {@value}. */
  public static final String PROPERTY = "streamrune.saga.forward-fence";

  /**
   * Parses a configured value, case-insensitively and ignoring surrounding whitespace.
   *
   * @param value the configured text; {@code null} and blank mean {@link #AUTO}
   * @throws IllegalArgumentException if the value is neither {@code auto} nor a boolean spelling
   */
  public static SagaForwardFenceSetting parse(String value) {
    if (value == null || value.isBlank()) {
      return AUTO;
    }
    String normalized = value.strip().toLowerCase(Locale.ROOT);
    if (normalized.equals("auto")) {
      return AUTO;
    }
    try {
      return RelaxedBoolean.parse(normalized, true) ? ON : OFF;
    } catch (IllegalArgumentException notABoolean) {
      throw new IllegalArgumentException(
          PROPERTY
              + " must be auto, on or off (case-insensitive; true/false and the other boolean"
              + " spellings of on/off are accepted); got: '"
              + value
              + "'");
    }
  }

  /**
   * Whether the forward fence is on under this setting.
   *
   * @param singleActiveConsumerEnabled the bound value of {@code
   *     streamrune.subscription.single-active-consumer.enabled}
   */
  public boolean enabled(boolean singleActiveConsumerEnabled) {
    return switch (this) {
      case AUTO -> singleActiveConsumerEnabled;
      case ON -> true;
      case OFF -> false;
    };
  }
}
