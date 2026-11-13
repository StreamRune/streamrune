package org.streamrune.integration;

import java.util.List;
import java.util.regex.Pattern;
import org.streamrune.core.Internal;

/**
 * The spelling rules the three integrations hold every {@code streamrune.*} duration and switch to,
 * so that one value means one thing on each of them.
 *
 * <ul>
 *   <li>A duration carries its unit ({@code 5000ms}, {@code 30s}, {@code 0s}) or is ISO-8601
 *       ({@code PT30S}). A number without a unit is refused: Spring would read it as milliseconds,
 *       Quarkus as seconds, and Micronaut cannot read it at all.
 *   <li>A switch is {@code true} or {@code false}, in lower case. Any other spelling is refused:
 *       the frameworks read {@code yes}, {@code on}, {@code 1} or {@code TRUE} differently from one
 *       another, and an integration's bean conditions compare the text itself, so such a value
 *       could switch a property on for the bound settings and off for the beans.
 * </ul>
 *
 * <p>The switches that StreamRune reads as text through {@link RelaxedBoolean} (the per-projection
 * switches, {@code streamrune.subscription.single-active-consumer.enabled}) keep that parser's
 * spellings: their bean conditions and their settings read the same parser.
 *
 * @hidden
 */
@Internal
public final class ConfigurationSpelling {

  /** What a refused duration is told. */
  public static final String DURATION_HINT =
      "a duration needs its unit, such as 5000ms, 30s or 0s, or ISO-8601 such as PT30S";

  /** What a refused switch is told. */
  public static final String SWITCH_HINT = "a switch is true or false, in lower case";

  private static final Pattern BARE_NUMBER = Pattern.compile("[+-]?\\d+");

  private ConfigurationSpelling() {}

  /**
   * Whether a configured duration is a number without a unit.
   *
   * @param raw the configured text, {@code null} when the property is not set
   * @return {@code true} for {@code 5000}, {@code 0} or {@code -5}; {@code false} for a value with
   *     a unit, an ISO-8601 value, a blank value or {@code null}
   */
  public static boolean isBareNumber(String raw) {
    return raw != null && BARE_NUMBER.matcher(raw.strip()).matches();
  }

  /**
   * Whether a configured switch is spelled {@code true} or {@code false}.
   *
   * @param raw the configured text of a property that is set
   * @return {@code true} only for exactly {@code true} or {@code false}
   */
  public static boolean isSwitch(String raw) {
    return "true".equals(raw) || "false".equals(raw);
  }

  /**
   * The refusal line of a duration without its unit.
   *
   * @param key the property
   * @param raw its configured text
   * @return {@code key=raw: } followed by {@link #DURATION_HINT}
   */
  public static String bareNumber(String key, String raw) {
    return key + "=" + raw + ": " + DURATION_HINT;
  }

  /**
   * The refusal line of a switch spelled other than {@code true} or {@code false}.
   *
   * @param key the property
   * @param raw its configured text
   * @return {@code key=raw: } followed by {@link #SWITCH_HINT}
   */
  public static String notASwitch(String key, String raw) {
    return key + "=" + raw + ": " + SWITCH_HINT;
  }

  /**
   * Refuses the start when the configuration breaks a rule.
   *
   * @param problems one line per property, each naming the property and its value
   * @throws IllegalStateException listing every line, when there is one
   */
  public static void refuse(List<String> problems) {
    if (!problems.isEmpty()) {
      throw new IllegalStateException(
          "StreamRune cannot read "
              + (problems.size() == 1 ? "this configuration value" : "these configuration values")
              + ":\n  "
              + String.join("\n  ", problems));
    }
  }
}
