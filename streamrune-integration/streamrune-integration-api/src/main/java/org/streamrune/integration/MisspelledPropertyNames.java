package org.streamrune.integration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Names that read like StreamRune properties but that no integration binds, each with the property
 * an application sets instead: the interval and delay settings with a {@code -ms} suffix, which
 * StreamRune binds as durations without the suffix, and the outbox retry multiplier under a shorter
 * name.
 *
 * <p>Quarkus refuses to start on a {@code streamrune.*} name its configuration mapping does not
 * declare. Spring and Micronaut ignore such a name, so an application that sets one of these runs
 * on the default of the property it meant, without a word: their startup validators call {@link
 * #refuseIfSet} so that the three integrations refuse these names alike.
 */
public final class MisspelledPropertyNames {

  private static final Map<String, String> INSTEAD = names();

  private MisspelledPropertyNames() {}

  private static Map<String, String> names() {
    Map<String, String> instead = new LinkedHashMap<>();
    for (String duration :
        List.of(
            "streamrune.retry-initial-delay",
            "streamrune.polling-interval",
            "streamrune.polling-jitter",
            "streamrune.dead-letter.retry-interval",
            "streamrune.outbox.flush-interval",
            "streamrune.outbox.retry-initial-delay")) {
      instead.put(duration + "-ms", duration);
    }
    instead.put("streamrune.outbox.retry-multiplier", "streamrune.outbox.retry-backoff-multiplier");
    return Collections.unmodifiableMap(instead);
  }

  /**
   * Each name with the property an application sets instead, in a fixed order.
   *
   * @return the names, unmodifiable
   */
  public static Map<String, String> all() {
    return INSTEAD;
  }

  /**
   * Refuses the start when the configuration sets one of these names.
   *
   * @param isSet whether the application's configuration sets a property, as the integration reads
   *     its configuration (relaxed names and environment variables included)
   * @throws IllegalStateException naming every name that is set and the property to set instead
   */
  public static void refuseIfSet(Predicate<String> isSet) {
    List<String> set = new ArrayList<>();
    INSTEAD.forEach(
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
    if (!set.isEmpty()) {
      throw new IllegalStateException(String.join("\n", set));
    }
  }
}
