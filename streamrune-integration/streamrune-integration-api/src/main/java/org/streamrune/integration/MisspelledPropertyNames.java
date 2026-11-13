package org.streamrune.integration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import org.streamrune.core.Internal;

/**
 * Names that read like StreamRune properties but that no integration binds, each with the property
 * an application sets instead: every duration property written with a {@code -ms} suffix, which
 * StreamRune binds as a duration with its unit and without the suffix, and the outbox retry
 * multiplier under a shorter name.
 *
 * <p>Quarkus refuses to start on a {@code streamrune.*} name its configuration mapping does not
 * declare. Spring and Micronaut ignore such a name, so an application that sets one of these runs
 * on the default of the property it meant, without a word: their startup validators refuse these
 * names through {@link #problems}, so that the three integrations refuse them alike.
 *
 * @hidden
 */
@Internal
public final class MisspelledPropertyNames {

  /** The names that are not a duration with a suffix, each with the property to set instead. */
  public static final Map<String, String> OTHER_NAMES =
      Map.of("streamrune.outbox.retry-multiplier", "streamrune.outbox.retry-backoff-multiplier");

  private MisspelledPropertyNames() {}

  /**
   * The names an integration refuses, each with the property to set instead.
   *
   * @param durationKeys the duration properties the integration binds
   * @return {@code <key>-ms} for every duration property, and {@link #OTHER_NAMES}
   */
  public static Map<String, String> of(Collection<String> durationKeys) {
    Map<String, String> instead = new LinkedHashMap<>();
    durationKeys.stream().sorted().forEach(key -> instead.put(key + "-ms", key));
    instead.putAll(OTHER_NAMES);
    return instead;
  }

  /**
   * The refusal lines of the names the configuration sets.
   *
   * @param durationKeys the duration properties the integration binds
   * @param isSet whether the application's configuration sets a property, as the integration reads
   *     its configuration (relaxed names and environment variables included)
   * @return one line per name that is set, naming it and the property to set instead
   */
  public static List<String> problems(Collection<String> durationKeys, Predicate<String> isSet) {
    List<String> set = new ArrayList<>();
    of(durationKeys)
        .forEach(
            (name, property) -> {
              if (isSet.test(name)) {
                set.add(
                    name
                        + " is not a StreamRune property; set "
                        + property
                        + (name.endsWith("-ms") ? ", a duration with its unit such as 500ms" : "")
                        + " instead");
              }
            });
    return set;
  }
}
