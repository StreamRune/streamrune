package org.streamrune.micronaut;

import io.micronaut.context.env.Environment;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.exceptions.ConfigurationException;
import io.micronaut.core.naming.NameUtils;
import io.micronaut.core.naming.conventions.StringConvention;
import io.micronaut.core.value.PropertyCatalog;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.integration.SagaParallelismSetting;
import org.streamrune.runtime.SagaParallelismSettings;

/**
 * Reads the saga parallelism properties: the global value from {@link
 * StreamRuneMicronautProperties}, the per-subscription values from the environment. The
 * per-subscription keys are read here, not bound to a type, because their names are the
 * subscription names the application chose, like the projection keys the projection factory looks
 * up. They are read by name, with no type bound by reflection.
 *
 * <p>The names come from the keys as written, in every configuration source Micronaut reads,
 * imported files included: the subscription name is everything between {@code
 * streamrune.saga.subscriptions.} and the last element {@code parallelism}, as written, so {@code
 * streamrune.saga.subscriptions.orders.v2.parallelism} names {@code orders.v2} and {@code
 * streamrune.saga.subscriptions.orderSaga.parallelism} names {@code orderSaga}. Any other key under
 * the prefix fails startup: a misspelt {@code parallelism}, an indexed spelling such as {@code
 * streamrune.saga.subscriptions[orders].parallelism}, or the prefix in another letter case; a value
 * at {@code streamrune.saga.subscriptions} itself names no subscription and is not read. Two names
 * that Micronaut reads as one property ({@code orders} and {@code Orders}, {@code order-saga} and
 * {@code orderSaga}) fail startup too, because Micronaut can give one of them the value of the
 * other.
 *
 * <p>The value of a name is the one Micronaut resolves for its property, so a source of higher
 * precedence wins as for every Micronaut property, and placeholders are resolved by Micronaut; a
 * placeholder it cannot resolve fails startup naming the property.
 *
 * <p>From an environment variable, underscores become dots and the name is lower case, so {@code
 * STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_V2_PARALLELISM} names {@code orders.v2}. A variable sets
 * that lower-case dotted name and, because the value is the one Micronaut resolves, every {@code -}
 * or {@code .} spelling of it that is also written in a file or as a system property; a key of
 * higher precedence in another letter case can take the value from it. A name with a hyphen or an
 * upper-case letter cannot be given by a variable alone, only in a properties or YAML file or as a
 * system property (a variable named with dots, which some platforms allow, is read like a key in a
 * file). A variable with an element of one or two digits, which Micronaut reads as an index, fails
 * startup. Startup fails naming the subscriptions that exist when a name matches none.
 */
final class SagaParallelismProperties {

  private static final String PREFIX = "streamrune.saga.subscriptions";

  private static final String ENVIRONMENT_PREFIX = "STREAMRUNE_SAGA_SUBSCRIPTIONS";

  private static final String ROOT = "streamrune";

  /**
   * The spellings of the first element that Micronaut reads as {@code streamrune}, found by
   * Micronaut's own naming rule; the elements after it are matched in any letter case.
   */
  private static final List<String> ROOTS = rootSpellings();

  private SagaParallelismProperties() {}

  private static List<String> rootSpellings() {
    List<String> roots = new java.util.ArrayList<>();
    for (int mask = 0; mask < 1 << ROOT.length(); mask++) {
      StringBuilder spelling = new StringBuilder();
      for (int i = 0; i < ROOT.length(); i++) {
        char letter = ROOT.charAt(i);
        spelling.append((mask & (1 << i)) != 0 ? Character.toUpperCase(letter) : letter);
      }
      if (NameUtils.hyphenate(spelling.toString(), true).equals(ROOT)) {
        roots.add(spelling.toString());
      }
    }
    return List.copyOf(roots);
  }

  /**
   * The settings the properties say.
   *
   * @throws IllegalArgumentException when a value is not a whole number from 1 to 64, the message
   *     naming the property and the value; when a value holds a placeholder that cannot be
   *     resolved, the message naming the property; when a key under {@code
   *     streamrune.saga.subscriptions} is not {@code <name>.parallelism}, the message naming the
   *     key; or when two names are one property to Micronaut, the message naming both keys
   */
  static SagaParallelismSettings read(Environment environment, String global) {
    SagaParallelismSetting.Parsed parsed =
        SagaParallelismSetting.parse(global, bySubscription(environment));
    return new SagaParallelismSettings(parsed.global(), parsed.bySubscription());
  }

  /** Subscription name to the text of its parallelism, as Micronaut resolves the property. */
  static Map<String, String> bySubscription(Environment environment) {
    Set<String> names = new LinkedHashSet<>(environmentNames(environment));
    Set<String> written = writtenKeys(environment);
    for (String key : written) {
      String name = nameOf(key);
      refuseOneProperty(key, written);
      names.add(name);
    }
    Map<String, String> bySubscription = new LinkedHashMap<>();
    for (String name : names) {
      bySubscription.put(name, value(environment, name));
    }
    return bySubscription;
  }

  /**
   * Every key written under the prefix, in any letter case and indexed spellings included, from
   * Micronaut's merge of every source it reads; a map or list value is given as its entries.
   */
  private static Set<String> writtenKeys(Environment environment) {
    Set<String> written = new LinkedHashSet<>();
    for (String root : ROOTS) {
      heldAt(environment, root, written);
      for (String saga : environment.getPropertyEntries(root, PropertyCatalog.RAW)) {
        if (saga.equalsIgnoreCase("saga")) {
          heldAt(environment, root + "." + saga, written);
          writtenUnder(environment, root + "." + saga)
              .forEach(
                  (key, value) -> {
                    if (underThePrefix(key)) {
                      flatten(key, value, written);
                    }
                  });
        }
      }
    }
    // The prefix itself with a value that is neither a map nor a list names no subscription.
    written.remove(PREFIX);
    return written;
  }

  /**
   * A map or list value a source holds at {@code key}, above the prefix or at it, given as its
   * entries that are under the prefix. The sources are asked one by one: the merged view builds a
   * map out of the keys below a path, which is not a value anyone wrote there.
   */
  private static void heldAt(Environment environment, String key, Set<String> written) {
    for (PropertySource source : environment.getPropertySources()) {
      Object value = source.get(key);
      if (value instanceof Map<?, ?> || value instanceof List<?>) {
        Set<String> held = new LinkedHashSet<>();
        flatten(key, value, held);
        for (String entry : held) {
          if (underThePrefix(entry)) {
            written.add(entry);
          }
        }
      }
    }
  }

  /**
   * Whether Micronaut reads the key under the prefix: the prefix in any letter case, then the end,
   * a dot or an index.
   */
  private static boolean underThePrefix(String key) {
    int length = PREFIX.length();
    return key.regionMatches(true, 0, PREFIX, 0, length)
        && (key.length() == length || key.charAt(length) == '.' || key.charAt(length) == '[');
  }

  /**
   * The names the environment-variable sources set; a variable with an index, or one that is not
   * {@code <name>_PARALLELISM}, fails startup.
   */
  private static Set<String> environmentNames(Environment environment) {
    Set<String> names = new LinkedHashSet<>();
    for (PropertySource source : environment.getPropertySources()) {
      if (source.getConvention() != PropertySource.PropertyConvention.ENVIRONMENT_VARIABLE) {
        continue;
      }
      for (String variable : source) {
        if (!variable.regionMatches(true, 0, ENVIRONMENT_PREFIX, 0, ENVIRONMENT_PREFIX.length())
            || variable.length() == ENVIRONMENT_PREFIX.length()) {
          continue;
        }
        char next = variable.charAt(ENVIRONMENT_PREFIX.length());
        if (next == '[' || (next == '_' && variable.indexOf('[') >= 0)) {
          // Micronaut's environment source writes an element of one or two digits as an index,
          // _2_ as [2], and reads the variable as a list, not as a subscription's property.
          throw new IllegalArgumentException(
              written(asWritten(variable), ENVIRONMENT_PREFIX.length())
                  + " holds an element of one or two digits, which Micronaut reads as an index,"
                  + " not as a subscription's property; such an element cannot be written in a"
                  + " variable, set "
                  + SagaParallelismSetting.SUBSCRIPTIONS_PREFIX
                  + "<name>"
                  + SagaParallelismSetting.SUBSCRIPTION_SUFFIX
                  + " in a file or as a system property");
        }
        if (next == '_') {
          String dotted =
              variable
                  .substring(ENVIRONMENT_PREFIX.length() + 1)
                  .toLowerCase(Locale.ROOT)
                  .replace('_', '.');
          names.add(nameOf(SagaParallelismSetting.SUBSCRIPTIONS_PREFIX + dotted));
        }
      }
    }
    return names;
  }

  /** The variable as it was set: the underscores around an index put back. */
  private static String asWritten(String held) {
    return held.replaceAll("\\[(\\d+)\\](?=.)", "_$1_").replaceAll("\\[(\\d+)\\]$", "_$1");
  }

  /**
   * Every key written under {@code path}, as the configuration holds it, with its value:
   * Micronaut's own merge of every source it reads. When a value there holds a placeholder that
   * cannot be resolved, the keys are listed without their values, so that reading that value names
   * its key.
   */
  private static Map<String, Object> writtenUnder(Environment environment, String path) {
    Map<String, Object> keys = new LinkedHashMap<>();
    try {
      environment
          .getProperties(path, StringConvention.RAW)
          .forEach((key, value) -> keys.put(path + "." + key, value));
    } catch (ConfigurationException unresolvable) {
      for (String element : environment.getPropertyEntries(path, PropertyCatalog.RAW)) {
        String child = path + "." + element;
        Map<String, Object> below = writtenUnder(environment, child);
        if (below.isEmpty()) {
          keys.put(child, valueOrNull(environment, child));
        } else {
          keys.putAll(below);
        }
      }
    }
    return keys;
  }

  private static Object valueOrNull(Environment environment, String key) {
    try {
      return environment.getProperty(key, Object.class).orElse(null);
    } catch (ConfigurationException unresolvable) {
      return null;
    }
  }

  /**
   * The key, or, for a non-empty map or list, each of its entries under the key Micronaut gives it.
   */
  private static void flatten(String key, Object value, Set<String> keys) {
    if (value instanceof Map<?, ?> map && !map.isEmpty()) {
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        flatten(key + "." + entry.getKey(), entry.getValue(), keys);
      }
    } else if (value instanceof List<?> list && !list.isEmpty()) {
      for (int i = 0; i < list.size(); i++) {
        flatten(key + "[" + i + "]", list.get(i), keys);
      }
    } else {
      keys.add(key);
    }
  }

  /**
   * Refuses {@code key} when Micronaut reads it as one property with another written key: the
   * property of a name in Micronaut's own spelling (lower case, hyphens) is read from every key
   * that Micronaut spells the same way.
   */
  private static void refuseOneProperty(String key, Set<String> written) {
    String spelt = NameUtils.hyphenate(key, true);
    if (!spelt.equals(key) && written.contains(spelt)) {
      throw new IllegalArgumentException(
          written(spelt)
              + " and "
              + written(key)
              + " are one property to Micronaut, which can give either the value of the other;"
              + " set only one of them");
    }
  }

  /**
   * The subscription name of a key that starts with {@code streamrune.saga.subscriptions.}: the
   * text between that prefix and the final {@code .parallelism}; refuses any other key.
   */
  private static String nameOf(String key) {
    int start = SagaParallelismSetting.SUBSCRIPTIONS_PREFIX.length();
    if (!key.startsWith(SagaParallelismSetting.SUBSCRIPTIONS_PREFIX)
        || !key.endsWith(SagaParallelismSetting.SUBSCRIPTION_SUFFIX)
        || key.length() - SagaParallelismSetting.SUBSCRIPTION_SUFFIX.length() <= start) {
      throw notAProperty(key);
    }
    return key.substring(start, key.length() - SagaParallelismSetting.SUBSCRIPTION_SUFFIX.length());
  }

  private static IllegalArgumentException notAProperty(String key) {
    return new IllegalArgumentException(
        written(key)
            + " is not a property; set "
            + SagaParallelismSetting.SUBSCRIPTIONS_PREFIX
            + "<name>"
            + SagaParallelismSetting.SUBSCRIPTION_SUFFIX);
  }

  /** The value Micronaut resolves for the name's property; null when it holds none. */
  private static String value(Environment environment, String name) {
    String property =
        SagaParallelismSetting.SUBSCRIPTIONS_PREFIX
            + name
            + SagaParallelismSetting.SUBSCRIPTION_SUFFIX;
    try {
      return environment.getProperty(property, String.class).orElse(null);
    } catch (ConfigurationException unresolvable) {
      throw new IllegalArgumentException(
          written(property)
              + " holds a placeholder Micronaut cannot resolve: "
              + unresolvable.getMessage(),
          unresolvable);
    }
  }

  /** A key under the prefix as written, the part after the prefix sanitized for the log. */
  private static String written(String key) {
    return written(key, PREFIX.length());
  }

  /** {@code text} with the part after its first {@code prefix} characters sanitized for the log. */
  private static String written(String text, int prefix) {
    return text.substring(0, prefix) + LogSanitizer.sanitizeForLog(text.substring(prefix));
  }
}
