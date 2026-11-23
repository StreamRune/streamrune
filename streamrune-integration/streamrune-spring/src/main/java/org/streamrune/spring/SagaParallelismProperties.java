package org.streamrune.spring;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.context.properties.source.ConfigurationProperty;
import org.springframework.boot.context.properties.source.ConfigurationPropertyName;
import org.springframework.boot.context.properties.source.ConfigurationPropertySource;
import org.springframework.boot.context.properties.source.ConfigurationPropertySources;
import org.springframework.boot.context.properties.source.IterableConfigurationPropertySource;
import org.springframework.core.env.Environment;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.integration.SagaParallelismSetting;
import org.streamrune.runtime.SagaParallelismSettings;

/**
 * Reads the saga parallelism properties: the global value from {@link StreamRuneProperties}, the
 * per-subscription values from the environment's property sources. The per-subscription keys are
 * read here, not bound as a component of {@link StreamRuneProperties.Saga}, because their names are
 * the subscription names the application chose, like the projection keys the projection
 * auto-configuration looks up. They are read by name, with no type bound by reflection.
 *
 * <p>The subscription name is every element between {@code streamrune.saga.subscriptions.} and the
 * last element {@code parallelism}, as written: {@code
 * streamrune.saga.subscriptions.orders.v2.parallelism} and {@code
 * streamrune.saga.subscriptions.[orders.v2].parallelism} both name {@code orders.v2}, {@code
 * streamrune.saga.subscriptions.Orders.parallelism} names {@code Orders}. Any other key under the
 * prefix, a misspelt {@code parallelism} among them, fails startup; a value at {@code
 * streamrune.saga.subscriptions} itself names no subscription and is not read. When two property
 * sources set one subscription, the one of higher precedence wins.
 *
 * <p>Spring compares names without case, {@code -} and {@code _}, so two unbracketed names of one
 * property source that differ only there ({@code Orders} and {@code orders}, {@code order-saga} and
 * {@code orderSaga}) are one property to it, and either can be given the value of the other. Such a
 * pair fails startup naming both keys; written in brackets, {@code [Orders]} and {@code [orders]},
 * each name is read exactly.
 *
 * <p>An unbracketed name element keeps only letters, digits, {@code -} and {@code _}: Spring drops
 * any other character, so {@code orders/v2} reads as {@code ordersv2}. A name with any other
 * character is written in brackets, {@code [orders/v2]}. From an environment variable, underscores
 * become dots and the name is lower case, so {@code
 * STREAMRUNE_SAGA_SUBSCRIPTIONS_ORDERS_V2_PARALLELISM} names {@code orders.v2}; a name with a
 * hyphen or an upper-case letter cannot be set from an environment variable, only in a properties
 * or YAML file or as a system property. Startup fails naming the subscriptions that exist when a
 * name matches none.
 */
final class SagaParallelismProperties {

  private static final ConfigurationPropertyName PREFIX =
      ConfigurationPropertyName.of("streamrune.saga.subscriptions");

  private static final String LEAF = "parallelism";

  private SagaParallelismProperties() {}

  /**
   * The settings the properties say.
   *
   * @throws IllegalArgumentException when a value is not a whole number from 1 to 64, the message
   *     naming the property and the value; or when a key under {@code
   *     streamrune.saga.subscriptions} is not {@code <name>.parallelism}, the message naming the
   *     key; or when two keys of one property source are one property to Spring, the message naming
   *     both
   */
  static SagaParallelismSettings read(Environment environment, StreamRuneProperties properties) {
    SagaParallelismSetting.Parsed parsed =
        SagaParallelismSetting.parse(properties.saga().parallelism(), bySubscription(environment));
    return new SagaParallelismSettings(parsed.global(), parsed.bySubscription());
  }

  /**
   * Subscription name to the text of its parallelism, from every enumerable property source; a
   * source of higher precedence wins for one name.
   */
  static Map<String, String> bySubscription(Environment environment) {
    var placeholders = new PropertySourcesPlaceholdersResolver(environment);
    Map<String, String> bySubscription = new LinkedHashMap<>();
    for (ConfigurationPropertySource source : ConfigurationPropertySources.get(environment)) {
      if (!(source instanceof IterableConfigurationPropertySource iterable)) {
        continue;
      }
      // The keys of this source under the prefix; equal to Spring when they differ only in case,
      // '-' or '_' outside brackets.
      Map<ConfigurationPropertyName, ConfigurationPropertyName> inThisSource = new HashMap<>();
      for (ConfigurationPropertyName name : iterable) {
        if (!PREFIX.isAncestorOf(name)) {
          continue;
        }
        String subscription = subscriptionName(name);
        ConfigurationPropertyName same = inThisSource.putIfAbsent(name, name);
        if (same != null && !subscriptionName(same).equals(subscription)) {
          throw oneProperty(same, name);
        }
        if (bySubscription.containsKey(subscription)) {
          continue;
        }
        ConfigurationProperty property = iterable.getConfigurationProperty(name);
        Object value =
            property == null ? null : placeholders.resolvePlaceholders(property.getValue());
        bySubscription.put(subscription, Objects.toString(value, null));
      }
    }
    return bySubscription;
  }

  /** The subscription name of a key under the prefix; refuses a key that is not one. */
  private static String subscriptionName(ConfigurationPropertyName name) {
    int first = PREFIX.getNumberOfElements();
    int last = name.getNumberOfElements() - 1;
    if (last <= first
        || !LEAF.equals(name.getElement(last, ConfigurationPropertyName.Form.ORIGINAL))) {
      throw new IllegalArgumentException(
          written(name)
              + " is not a property; set "
              + SagaParallelismSetting.SUBSCRIPTIONS_PREFIX
              + "<name>"
              + SagaParallelismSetting.SUBSCRIPTION_SUFFIX);
    }
    return elements(name, first, last, false);
  }

  /**
   * Refuses two keys of one source that Spring reads as one property although they name two
   * subscriptions, naming both as written.
   */
  private static IllegalArgumentException oneProperty(
      ConfigurationPropertyName one, ConfigurationPropertyName other) {
    List<String> both = List.copyOf(new TreeSet<>(List.of(written(one), written(other))));
    return new IllegalArgumentException(
        both.getFirst()
            + " and "
            + both.getLast()
            + " are one property to Spring, which can give either the value of the other; write"
            + " each name in brackets or set only one of them");
  }

  /** The key under the prefix as written, the part after the prefix sanitized for the log. */
  private static String written(ConfigurationPropertyName name) {
    return SagaParallelismSetting.SUBSCRIPTIONS_PREFIX
        + LogSanitizer.sanitizeForLog(
            elements(name, PREFIX.getNumberOfElements(), name.getNumberOfElements(), true));
  }

  /**
   * The elements {@code [from, to)} as written, joined with dots; with {@code bracketed}, an
   * element that holds a dot itself is shown in brackets, the way it is written in a key.
   */
  private static String elements(
      ConfigurationPropertyName name, int from, int to, boolean bracketed) {
    StringBuilder joined = new StringBuilder();
    for (int i = from; i < to; i++) {
      if (i > from) {
        joined.append('.');
      }
      String element = name.getElement(i, ConfigurationPropertyName.Form.ORIGINAL);
      joined.append(bracketed && element.indexOf('.') >= 0 ? "[" + element + "]" : element);
    }
    return joined.toString();
  }
}
