package org.streamrune.spring;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import org.springframework.core.env.Environment;
import org.streamrune.integration.ConfigurationSpelling;
import org.streamrune.integration.MisspelledPropertyNames;

/**
 * The spelling rules of {@link ConfigurationSpelling} and the names of {@link
 * MisspelledPropertyNames}, read against the application's configuration as Spring resolves it
 * (relaxed names and environment variables included).
 *
 * <p>Spring binds a duration written without its unit as milliseconds, and a switch spelled {@code
 * yes}, {@code on} or {@code 1} as {@code true} while a bean's {@code ConditionalOnProperty(
 * havingValue = "true")} does not match it. The properties read are the keys of {@link
 * StreamRunePropertyKeys}: every duration and boolean component of {@link StreamRuneProperties} and
 * {@link StreamRuneCryptoProperties}, and every switch a condition of the integration's
 * auto-configurations reads. The check reads only those constants and the {@link Environment}, so
 * it runs the same in a native image.
 */
final class ConfigurationSpellingCheck {

  private ConfigurationSpellingCheck() {}

  /**
   * One line per property the configuration sets in a way the integration refuses, naming the
   * property and the value: misspelled names, durations without their unit, switches spelled other
   * than {@code true} or {@code false}.
   */
  static List<String> problems(Environment environment) {
    List<String> problems =
        new ArrayList<>(
            MisspelledPropertyNames.problems(
                StreamRunePropertyKeys.DURATIONS, environment::containsProperty));
    for (String key : new TreeSet<>(StreamRunePropertyKeys.DURATIONS)) {
      String raw = environment.getProperty(key);
      if (ConfigurationSpelling.isBareNumber(raw)) {
        problems.add(ConfigurationSpelling.bareNumber(key, raw));
      }
    }
    for (String key : new TreeSet<>(StreamRunePropertyKeys.SWITCHES)) {
      if (environment.containsProperty(key)) {
        String raw = String.valueOf(environment.getProperty(key, ""));
        if (!ConfigurationSpelling.isSwitch(raw)) {
          problems.add(ConfigurationSpelling.notASwitch(key, raw));
        }
      }
    }
    return problems;
  }
}
