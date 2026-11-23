package org.streamrune.quarkus;

import io.smallrye.config.EnvConfigSource;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.common.utils.StringUtil;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.ConfigValue;
import org.eclipse.microprofile.config.spi.ConfigSource;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.integration.SagaParallelismSetting;
import org.streamrune.runtime.SagaParallelismSettings;

/**
 * Reads the saga parallelism properties: the global value from {@link StreamRuneQuarkusProperties},
 * the per-subscription values from the property names of the configuration's sources. The keys are
 * read here by name, because their names are the subscription names the application chose; {@link
 * StreamRuneQuarkusProperties.SagaConfig#subscriptions()} only declares them, so that the
 * configuration accepts them.
 *
 * <p>The subscription name of a key written in a file, as a system property or as a dotted line of
 * a {@code .env} file, or a dotted variable of the process environment, is everything between
 * {@code streamrune.saga.subscriptions.} and the final {@code .parallelism}, as written, with
 * surrounding double quotes removed: {@code streamrune.saga.subscriptions.orders.v2.parallelism}
 * and {@code streamrune.saga.subscriptions."orders.v2".parallelism} both name {@code orders.v2},
 * {@code streamrune.saga.subscriptions.Orders.parallelism} names {@code Orders}, {@code
 * streamrune.saga.subscriptions.shard[0].parallelism} names {@code shard[0]}. Any other key under
 * the prefix, a misspelt {@code parallelism} or {@code streamrune.saga.subscriptions} alone among
 * them, fails startup naming the key as written.
 *
 * <p>A key is found when a configuration source lists it among its property names: properties and
 * YAML files, system properties, environment variables and a {@code .env} file do; so is a name the
 * configuration lists that no source writes as it is, such as one a relocating interceptor adds or
 * a dotted environment entry not written exactly as the key, whose value is the configuration's. A
 * source whose property names are {@code null} lists nothing. A source that answers a lookup
 * without listing its names adds no key; it can still give the value of a key another source lists,
 * because the value of each key is the configuration's own: its profiles, the ordinals of its
 * sources and its expressions apply as to any other property. A key of a profile, {@code
 * %prod.streamrune.saga.subscriptions.orders.parallelism} or the variable {@code
 * _PROD_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM}, counts while that profile is active, as
 * the key without its profile; while it is not, the configuration ignores it, and so does this
 * reader. A variable belongs to a profile as the configuration matches it: the profile's name with
 * every character that is not a letter or digit written as an underscore, in any case, so {@code
 * _PROD_EU_STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_PARALLELISM} belongs to {@code prod-eu}. When one
 * subscription is named by two keys the configuration looks up apart (the quoted and the unquoted
 * form, or a file key and a variable that is not its spelling), the value from the source of higher
 * ordinal wins; two such keys at the same ordinal fail startup naming both.
 *
 * <p>An environment variable names a subscription in its own spelling: the variable's name in lower
 * case with every underscore a dot, a number between underscores one element of the name, so {@code
 * STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_V2_PARALLELISM} names {@code orders.v2}, {@code
 * STREAMRUNE_SAGA_SUBSCRIPTIONS_2024_ORDERS_PARALLELISM} names {@code 2024.orders} and {@code
 * STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_7__PARALLELISM} names {@code orders.7}; a part between
 * double underscores is one quoted name element, so {@code
 * STREAMRUNE_SAGA_SUBSCRIPTIONS__ORDERS_V2__PARALLELISM} names {@code orders.v2} too. Its value is
 * the configuration's value of a key the variable spells ({@code
 * streamrune.saga.subscriptions.orders[7].parallelism} for the third). Beside {@code
 * streamrune.saga.subscriptions.order-saga.parallelism} in a file, {@code
 * STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDER_SAGA_PARALLELISM} names {@code order.saga}, a subscription of
 * its own, and startup fails naming the subscriptions that exist when a name matches none; a
 * variable that spells no {@code <name>.parallelism} fails startup naming the key it spells. A name
 * with a hyphen, an upper-case letter or a bracket therefore cannot be set from an environment
 * variable without a dot; set it in the configuration file, as a system property or as a dotted
 * entry of a {@code .env} file.
 */
final class SagaParallelismProperties {

  /** {@code streamrune.saga.subscriptions}, the prefix without its final dot. */
  private static final String SUBSCRIPTIONS =
      SagaParallelismSetting.SUBSCRIPTIONS_PREFIX.substring(
          0, SagaParallelismSetting.SUBSCRIPTIONS_PREFIX.length() - 1);

  private SagaParallelismProperties() {}

  /**
   * The settings the properties say.
   *
   * @param config the configuration to read the per-subscription keys from
   * @param global the text of {@code streamrune.saga.parallelism}
   * @throws IllegalArgumentException when a value is not a whole number from 1 to 64, the message
   *     naming the property and the value; when a key under {@code streamrune.saga.subscriptions}
   *     is not {@code <name>.parallelism}, the message naming the key; or when two keys set one
   *     subscription at the same ordinal, the message naming both
   */
  static SagaParallelismSettings read(Config config, String global) {
    SagaParallelismSetting.Parsed parsed =
        SagaParallelismSetting.parse(global, bySubscription(config));
    return new SagaParallelismSettings(parsed.global(), parsed.bySubscription());
  }

  /** One per-subscription value: the key looked up, its text, the ordinal of its source. */
  private record Setting(String key, String value, int ordinal) {}

  /** Subscription name to the text of its parallelism. */
  static Map<String, String> bySubscription(Config config) {
    List<String> profiles =
        config instanceof SmallRyeConfig smallRye ? smallRye.getProfiles() : List.of();
    // The key to look up, to the keys that name its subscriptions.
    Map<String, Set<String>> keys = new TreeMap<>();
    // The configuration's own spelling of each environment entry read above.
    Set<String> spelled = new TreeSet<>();
    for (ConfigSource source : config.getConfigSources()) {
      if (fromTheEnvironment(source)) {
        for (String variable : source.getProperties().keySet()) {
          if (variable.indexOf('.') < 0) {
            for (String unprofiled : withoutActiveProfiles(variable, profiles)) {
              if (addVariable(keys, unprofiled)) {
                spelled.add(StringUtil.toLowerCaseAndDotted(unprofiled));
              }
            }
          } else {
            String written = withoutActiveProfile(variable, profiles);
            if (addWritten(keys, written)) {
              spelled.add(StringUtil.toLowerCaseAndDotted(written));
            }
          }
        }
      } else {
        Set<String> names = source.getPropertyNames();
        if (names != null) {
          for (String name : names) {
            addWritten(keys, withoutActiveProfile(name, profiles));
          }
        }
      }
    }
    // A name the configuration lists that no source lists as written: one an interceptor adds, an
    // environment entry with a dot that is not written as the key.
    for (String name : config.getPropertyNames()) {
      if (underTheSubscriptions(name) && !keys.containsKey(name) && !spelled.contains(name)) {
        addWritten(keys, name);
      }
    }
    Map<String, Setting> settings = new LinkedHashMap<>();
    keys.forEach(
        (key, naming) -> {
          ConfigValue value = config.getConfigValue(key);
          for (String name : naming) {
            put(
                settings,
                subscriptionName(name, key),
                new Setting(key, value.getValue(), value.getSourceOrdinal()));
          }
        });
    Map<String, String> values = new LinkedHashMap<>();
    settings.forEach((subscription, setting) -> values.put(subscription, setting.value()));
    return values;
  }

  /** A key as a file, a system property or a dotted line of a {@code .env} file writes it. */
  private static boolean addWritten(Map<String, Set<String>> keys, String key) {
    if (key != null && underTheSubscriptions(key)) {
      keys.computeIfAbsent(key, k -> new TreeSet<>()).add(key);
      return true;
    }
    return false;
  }

  /**
   * An environment variable without its profile: looked up as the configuration names it ({@code
   * …orders[7].parallelism} for {@code …_ORDERS_7__PARALLELISM}), its subscription named with every
   * index written as an element ({@code orders.7}). When the variable is that element form's own
   * spelling ({@code …_ORDERS_2024_PARALLELISM}), the element form is looked up instead, so that a
   * file key of the same name and the variable that sets it are one setting.
   */
  private static boolean addVariable(Map<String, Set<String>> keys, String variable) {
    String named = StringUtil.toLowerCaseAndDotted(variable);
    String readable = readable(named);
    if (!underTheSubscriptions(readable)) {
      return false;
    }
    String key =
        StringUtil.replaceNonAlphanumericByUnderscores(readable).equalsIgnoreCase(variable)
            ? readable
            : named;
    keys.computeIfAbsent(key, k -> new TreeSet<>()).add(readable);
    return true;
  }

  private static boolean fromTheEnvironment(ConfigSource source) {
    return source.getName() != null && source.getName().startsWith(EnvConfigSource.NAME);
  }

  /**
   * {@code key} without its profile prefix ({@code %prod.} or {@code %prod,staging.}) when one of
   * its profiles is active, {@code null} when none is, {@code key} itself when it has no profile.
   */
  private static String withoutActiveProfile(String key, List<String> profiles) {
    int dot = key.indexOf('.');
    if (!key.startsWith("%") || dot < 0) {
      return key;
    }
    for (String profile : key.substring(1, dot).split(",")) {
      if (profiles.contains(profile.trim())) {
        return key.substring(dot + 1);
      }
    }
    return null;
  }

  /**
   * An environment variable without its profile, as the configuration matches a profile to it: the
   * variable itself when it has none; when it starts with an underscore and a letter or digit, the
   * rest after each active profile whose name, every character that is not a letter or digit
   * written as an underscore, follows that underscore in any case and is followed by one ({@code
   * _PROD_EU_} for {@code prod-eu}); nothing when no active profile matches.
   */
  private static List<String> withoutActiveProfiles(String variable, List<String> profiles) {
    if (variable.length() < 2
        || variable.charAt(0) != '_'
        || !StringUtil.isAsciiLetterOrDigit(variable.charAt(1))) {
      return List.of(variable);
    }
    List<String> unprofiled = new ArrayList<>();
    for (String profile : profiles) {
      String prefix = "_" + StringUtil.replaceNonAlphanumericByUnderscores(profile) + "_";
      if (variable.length() > prefix.length()
          && StringUtil.replaceNonAlphanumericByUnderscores(variable.substring(0, prefix.length()))
              .equalsIgnoreCase(prefix)) {
        unprofiled.add(variable.substring(prefix.length()));
      }
    }
    return unprofiled;
  }

  /** The key of an environment variable with every index ({@code [2024]}) written as an element. */
  private static String readable(String key) {
    StringBuilder readable = new StringBuilder(key.length() + 2);
    for (int i = 0; i < key.length(); i++) {
      char c = key.charAt(i);
      if (c == '[') {
        readable.append('.');
      } else if (c == ']') {
        if (i + 1 < key.length() && key.charAt(i + 1) != '.') {
          readable.append('.');
        }
      } else {
        readable.append(c);
      }
    }
    return readable.toString();
  }

  private static boolean underTheSubscriptions(String name) {
    return name.equals(SUBSCRIPTIONS)
        || name.startsWith(SagaParallelismSetting.SUBSCRIPTIONS_PREFIX);
  }

  /**
   * Records {@code setting} for {@code subscription} unless a source of higher ordinal set it;
   * refuses two keys at one ordinal.
   */
  private static void put(Map<String, Setting> settings, String subscription, Setting setting) {
    Setting known = settings.get(subscription);
    if (known == null || setting.ordinal() > known.ordinal()) {
      settings.put(subscription, setting);
    } else if (setting.ordinal() == known.ordinal()) {
      TreeSet<String> both = new TreeSet<>(List.of(known.key(), setting.key()));
      throw new IllegalArgumentException(
          written(both.first())
              + " and "
              + written(both.last())
              + " both set the parallelism of the saga subscription '"
              + LogSanitizer.sanitizeForLog(subscription)
              + "' with the same precedence; keep one of them");
    }
  }

  /**
   * The subscription {@code name}, a key under the prefix, names; refuses a name that is not one,
   * naming {@code key}, the key as the configuration names it.
   */
  private static String subscriptionName(String name, String key) {
    String suffix = SagaParallelismSetting.SUBSCRIPTION_SUFFIX;
    String rest =
        name.equals(SUBSCRIPTIONS)
            ? ""
            : name.substring(SagaParallelismSetting.SUBSCRIPTIONS_PREFIX.length());
    if (!rest.endsWith(suffix) || rest.length() == suffix.length()) {
      throw new IllegalArgumentException(
          written(key)
              + " is not a property; set "
              + SagaParallelismSetting.SUBSCRIPTIONS_PREFIX
              + "<name>"
              + suffix);
    }
    String subscription = rest.substring(0, rest.length() - suffix.length());
    if (subscription.length() >= 2
        && subscription.startsWith("\"")
        && subscription.endsWith("\"")) {
      subscription = subscription.substring(1, subscription.length() - 1);
    }
    return subscription;
  }

  /** A key under the prefix as written, the part after the prefix sanitized for the log. */
  private static String written(String key) {
    return SUBSCRIPTIONS + LogSanitizer.sanitizeForLog(key.substring(SUBSCRIPTIONS.length()));
  }

  /**
   * The configuration of the running application: its {@code Config} bean, which a Quarkus
   * application always has; without one, the configuration of this thread's class loader, which
   * reads system properties and environment variables too.
   */
  static Config applicationConfig(jakarta.enterprise.inject.Instance<Config> config) {
    return config != null && config.isResolvable()
        ? config.get()
        : org.eclipse.microprofile.config.ConfigProvider.getConfig();
  }
}
