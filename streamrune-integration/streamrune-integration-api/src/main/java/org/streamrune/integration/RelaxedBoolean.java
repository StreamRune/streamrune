package org.streamrune.integration;

import java.util.Locale;
import java.util.Set;

/**
 * Canonical, framework-agnostic parser for the {@code streamrune.*} boolean knobs that must be read
 * by hand from a raw string rather than through a typed configuration binding — chiefly the dynamic
 * per-projection {@code streamrune.projections.<name>.enabled} flag, whose key is not known at
 * compile time and so cannot be a typed record/{@code @ConfigMapping} field.
 *
 * <p><b>Why this exists.</b> Each framework's own relaxed boolean conversion disagrees on
 * non-canonical values: {@code Boolean.parseBoolean} (used previously in the Spring and Quarkus
 * integrations) treats <em>only</em> {@code "true"} as {@code true}, so {@code on}/{@code
 * yes}/{@code 1} all read as {@code false}; Micronaut's typed {@code Boolean} converter reads
 * {@code yes}/{@code y}/{@code on}/{@code true} as {@code true} but {@code 1} as {@code false};
 * SmallRye reads {@code 1}/{@code y}/{@code t} as {@code true}. The same {@code enabled=on}
 * therefore enabled a projection on Micronaut but silently disabled it on Spring/Quarkus. Routing
 * every integration through this one method makes the interpretation identical on all three
 * runtimes.
 *
 * <p><b>Canonical interpretation</b> (case-insensitive, surrounding whitespace ignored):
 *
 * <ul>
 *   <li>{@code true} — {@code true}, {@code on}, {@code yes}, {@code y}, {@code t}, {@code 1}
 *   <li>{@code false} — {@code false}, {@code off}, {@code no}, {@code n}, {@code f}, {@code 0}
 *   <li>{@code null} or blank — the supplied default
 *   <li>anything else — {@link IllegalArgumentException} (fail-fast on a misconfigured knob rather
 *       than silently disabling a projection, mirroring how an invalid {@code error-strategy} is
 *       rejected)
 * </ul>
 */
public final class RelaxedBoolean {

  private static final Set<String> TRUE_VALUES = Set.of("true", "on", "yes", "y", "t", "1");
  private static final Set<String> FALSE_VALUES = Set.of("false", "off", "no", "n", "f", "0");

  private RelaxedBoolean() {}

  /**
   * Parses a raw configuration string into a boolean using the canonical interpretation.
   *
   * @param value the raw configured value (may be {@code null} or blank)
   * @param defaultValue the value to return when {@code value} is {@code null} or blank
   * @return the parsed boolean
   * @throws IllegalArgumentException if {@code value} is non-blank but not a recognized boolean
   */
  public static boolean parse(String value, boolean defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    String normalized = value.strip().toLowerCase(Locale.ROOT);
    if (normalized.isEmpty()) {
      return defaultValue;
    }
    if (TRUE_VALUES.contains(normalized)) {
      return true;
    }
    if (FALSE_VALUES.contains(normalized)) {
      return false;
    }
    throw new IllegalArgumentException(
        "Not a boolean: '"
            + value
            + "'. Expected one of true/false, on/off, yes/no, y/n, t/f, 1/0 (case-insensitive).");
  }
}
