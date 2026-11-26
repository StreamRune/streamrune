package org.streamrune.integration;

/**
 * The one reading of {@code streamrune.subscription.single-active-consumer.enabled}. Everything
 * that reads the property goes through {@link #enabled(String, String)}: in each of the three
 * integrations the condition that creates the leadership bean, and the {@code auto} setting of the
 * saga forward fence ({@link SagaForwardFenceSetting#enabled(boolean)}). A spelling therefore
 * cannot switch leadership on in one place and the fence off in another.
 *
 * <p>The integrations bind the property as text and parse it here, with the spellings {@link
 * RelaxedBoolean} accepts. An unknown spelling fails startup with the message of {@link
 * #enabled(String, String)}, which names the property and the value.
 *
 * <p><b>The default follows the backend.</b> Unset, the property means {@code true} on {@code
 * postgres} and {@code false} on {@code sqlite}: SQLite serves one process, which needs no lease.
 * An explicit {@code true} with {@code streamrune.backend=sqlite} fails startup.
 */
public final class SingleActiveConsumerSetting {

  private static final String POSTGRES = "postgres";
  private static final String SQLITE = "sqlite";

  /** The property name, {@value}. */
  public static final String PROPERTY = "streamrune.subscription.single-active-consumer.enabled";

  private SingleActiveConsumerSetting() {}

  /**
   * Parses a configured value for the selected backend, case-insensitively and ignoring surrounding
   * whitespace.
   *
   * @param value the configured text; {@code null} and blank mean the backend's default: {@code
   *     true} on {@code postgres}, {@code false} on {@code sqlite}
   * @param backend the backend's name, as {@link BackendSelection#parse} returned it
   * @return whether single-active-consumer leadership is on
   * @throws NullPointerException if {@code backend} is {@code null}
   * @throws IllegalArgumentException if the value is not a boolean spelling (the message names the
   *     property and the value), or {@code backend} is not a parsed backend name
   * @throws IllegalStateException if the value is on and the backend is {@code sqlite}, which is
   *     single-instance
   */
  public static boolean enabled(String value, String backend) {
    java.util.Objects.requireNonNull(backend, "backend is required");
    if (!POSTGRES.equals(backend) && !SQLITE.equals(backend)) {
      throw new IllegalArgumentException(
          "backend must be a parsed backend name (postgres, sqlite), got '"
              + backend
              + "': parse "
              + BackendSelection.PROPERTY
              + " with BackendSelection.parse first");
    }
    boolean sqlite = SQLITE.equals(backend);
    boolean enabled = parse(value, !sqlite);
    if (enabled && sqlite) {
      throw new IllegalStateException(
          BackendSelection.PROPERTY
              + "=sqlite: SQLite is single-instance; set "
              + PROPERTY
              + "=false (or leave it unset, which means false on SQLite)");
    }
    return enabled;
  }

  /**
   * Checks only the spelling of a configured value, for a properties type that validates the text
   * when it is bound and does not know the backend yet.
   *
   * @param value the configured text; {@code null} and blank are accepted
   * @throws IllegalArgumentException if the value is not a boolean spelling; the message names the
   *     property and the value
   */
  public static void requireBoolean(String value) {
    parse(value, true);
  }

  private static boolean parse(String value, boolean defaultValue) {
    try {
      return RelaxedBoolean.parse(value, defaultValue);
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
