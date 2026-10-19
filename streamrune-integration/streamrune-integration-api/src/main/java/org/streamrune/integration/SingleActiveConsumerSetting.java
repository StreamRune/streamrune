package org.streamrune.integration;

/**
 * The one reading of {@code streamrune.subscription.single-active-consumer.enabled}. Everything
 * that reads the property goes through {@link #enabled(String)}: in each of the three integrations
 * the condition that creates the {@code LeaseBasedLeadership} bean, and the {@code auto} setting of
 * the saga forward fence ({@link SagaForwardFenceSetting#enabled(boolean)}). A spelling therefore
 * cannot switch leadership on in one place and the fence off in another.
 *
 * <p>The integrations bind the property as text and parse it here, with the spellings {@link
 * RelaxedBoolean} accepts. An unknown spelling fails startup with the message of {@link
 * #enabled(String)}, which names the property and the value.
 */
public final class SingleActiveConsumerSetting {

  /** The property name, {@value}. */
  public static final String PROPERTY = "streamrune.subscription.single-active-consumer.enabled";

  private SingleActiveConsumerSetting() {}

  /**
   * Parses a configured value, case-insensitively and ignoring surrounding whitespace.
   *
   * @param value the configured text; {@code null} and blank mean the default, {@code true}
   * @return whether single-active-consumer leadership is on
   * @throws IllegalArgumentException if the value is not a boolean spelling; the message names the
   *     property and the value
   */
  public static boolean enabled(String value) {
    try {
      return RelaxedBoolean.parse(value, true);
    } catch (IllegalArgumentException notABoolean) {
      throw new IllegalArgumentException(
          PROPERTY
              + " must be true or false (case-insensitive; on/off, yes/no, y/n, t/f and 1/0 are"
              + " accepted too); got: '"
              + value
              + "'");
    }
  }
}
