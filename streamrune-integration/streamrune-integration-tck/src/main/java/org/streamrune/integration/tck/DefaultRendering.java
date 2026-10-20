package org.streamrune.integration.tck;

import java.time.Duration;
import java.util.Collection;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The one spelling in which the guides write a configuration default, so that a default stated in
 * prose can be compared with the default in code whatever form the code declares it in ({@code
 * "7d"}, {@code "PT168H"}, {@code Duration.ofDays(7)}):
 *
 * <ul>
 *   <li>a {@link Duration}: the shortest of {@code Nd}, {@code Nh}, {@code Nm}, {@code Ns} and
 *       {@code Nms} that is exact ({@code 7d}, {@code 1m}, {@code 90s}, {@code 500ms}), {@code 0s}
 *       for zero, and the ISO-8601 form for a duration that is not a whole number of milliseconds;
 *   <li>a boolean or a number: its text ({@code true}, {@code 100}, {@code 2.0});
 *   <li>an enum constant: its name in lower case with {@code -} for {@code _};
 *   <li>a string: itself;
 *   <li>a collection: its elements, each rendered, joined with {@code ", "};
 *   <li>no value ({@code null}, an empty {@link Optional}, an empty string or collection): {@link
 *       #NONE}.
 * </ul>
 */
public final class DefaultRendering {

  /** The rendering of a key that has no default. */
  public static final String NONE = "—";

  private DefaultRendering() {}

  /** Renders {@code value} as the guides write it; see the class javadoc. */
  public static String render(Object value) {
    return switch (value) {
      case null -> NONE;
      case Optional<?> optional -> optional.map(DefaultRendering::render).orElse(NONE);
      case Duration duration -> duration(duration);
      case Enum<?> constant -> constant.name().toLowerCase(Locale.ROOT).replace('_', '-');
      case CharSequence text -> text.isEmpty() ? NONE : text.toString();
      case Collection<?> elements ->
          elements.isEmpty()
              ? NONE
              : elements.stream().map(DefaultRendering::render).collect(Collectors.joining(", "));
      default -> String.valueOf(value);
    };
  }

  private static String duration(Duration duration) {
    if (duration.isZero()) {
      return "0s";
    }
    if (duration.isNegative()) {
      return "-" + duration(duration.negated());
    }
    if (duration.getNano() % 1_000_000 != 0) {
      return duration.toString();
    }
    long millis = duration.toMillis();
    long[] sizes = {86_400_000L, 3_600_000L, 60_000L, 1_000L};
    String[] units = {"d", "h", "m", "s"};
    for (int i = 0; i < sizes.length; i++) {
      if (millis % sizes[i] == 0) {
        return millis / sizes[i] + units[i];
      }
    }
    return millis + "ms";
  }
}
