package org.streamrune.micronaut;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.annotation.AnnotationValue;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.ConversionError;
import io.micronaut.core.order.Ordered;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.BeanDefinitionReference;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.streamrune.integration.ConfigurationSpelling;
import org.streamrune.integration.MisspelledPropertyNames;

/**
 * Refuses the start when a {@code streamrune.*} property is set to a value StreamRune does not read
 * the way the operator meant, naming the property, the value and the reason:
 *
 * <ul>
 *   <li>a value Micronaut cannot convert to the type the integration binds it as. Micronaut binds a
 *       property that carries a default, a {@code Bindable} or {@code Property} default value, to
 *       that default when the configured value does not convert: {@code
 *       streamrune.inbox.retention-max-age=0} — a duration without its unit — would bind {@code
 *       7d}, and {@code streamrune.stripe-count=many} would bind {@code 1024}, without a word;
 *   <li>a switch spelled other than {@code true} or {@code false} ({@link ConfigurationSpelling}).
 *       Micronaut binds {@code yes}, {@code on} or {@code TRUE} as {@code true}, while a bean's
 *       {@code Requires(property, value)} compares the text itself: {@code
 *       streamrune.validation-enabled=TRUE} would bind {@code true} and leave out the Bean
 *       Validation interceptor, and {@code streamrune.crypto.cache.enabled=FALSE} would bind {@code
 *       false} and keep the decrypted-plaintext cache. An empty switch is refused too, as a
 *       condition reads it as a value;
 *   <li>a name that reads like a StreamRune property but is none ({@link MisspelledPropertyNames}),
 *       such as a duration property with a {@code -ms} suffix.
 * </ul>
 *
 * <p>The check is not a list of keys. It reads every property the integration's beans bind — the
 * components of {@link StreamRuneMicronautProperties} and {@link
 * StreamRuneMicronautCryptoProperties}, every {@code @Property} parameter of a bean or factory
 * method of the integration — and every property a bean of the integration requires to be {@code
 * true} or {@code false}, from the metadata Micronaut generates for them, enabled or not. A set
 * value is converted as the bean would convert it. An empty or blank value that is not a switch
 * counts as not set, as it does on Spring and Quarkus, and binds the default.
 *
 * <p>Ordered ahead of the other startup validators (order {@code 1000}), so their checks read the
 * values the operator wrote, and of the listeners that start the runners (order {@code 1100}), so
 * nothing runs on a default the operator did not ask for.
 */
@Singleton
final class ConfigurationValueValidator implements ApplicationEventListener<StartupEvent>, Ordered {

  private static final String PREFIX = "streamrune.";

  /** The package of the integration's bean definitions, which Micronaut generates beside it. */
  private static final String DEFINITIONS = "org.streamrune.micronaut.";

  private final ApplicationContext context;

  ConfigurationValueValidator(ApplicationContext context) {
    this.context = context;
  }

  @Override
  public int getOrder() {
    return 900;
  }

  @Override
  public void onApplicationEvent(StartupEvent event) {
    ConfigurationSpelling.refuse(problems(context));
  }

  /**
   * Every {@code streamrune.*} property the integration reads that is set to a value it refuses, as
   * one line naming the property, the value and the reason, in key order: misspelled names first.
   *
   * @param context the application context whose configuration and bean definitions are read
   * @return the refusal lines; empty when the configuration is read as written
   */
  static List<String> problems(ApplicationContext context) {
    Map<String, Argument<?>> bound = new TreeMap<>();
    Set<String> switches = new TreeSet<>();
    readDefinitions(context, bound, switches);
    List<String> durations =
        bound.entrySet().stream()
            .filter(entry -> entry.getValue().getType() == Duration.class)
            .map(Map.Entry::getKey)
            .toList();
    List<String> problems =
        new ArrayList<>(MisspelledPropertyNames.problems(durations, context::containsProperty));
    for (String key : switches) {
      if (context.containsProperty(key)) {
        String raw = context.getProperty(key, String.class).orElse("");
        if (!ConfigurationSpelling.isSwitch(raw)) {
          problems.add(ConfigurationSpelling.notASwitch(key, raw));
        }
      }
    }
    bound.forEach(
        (key, argument) -> {
          if (switches.contains(key) || !context.containsProperty(key)) {
            return;
          }
          Optional<String> raw = context.getProperty(key, String.class);
          if (raw.isEmpty() || raw.get().isBlank()) {
            return;
          }
          ArgumentConversionContext<?> conversion = ConversionContext.of(argument);
          if (context.getProperty(key, conversion).isPresent()) {
            return;
          }
          boolean duration = argument.getType() == Duration.class;
          if (duration && ConfigurationSpelling.isBareNumber(raw.get())) {
            problems.add(ConfigurationSpelling.bareNumber(key, raw.get()));
            return;
          }
          String reason =
              conversion
                  .getLastError()
                  .map(ConversionError::getCause)
                  .map(Throwable::getMessage)
                  .orElse("no conversion");
          problems.add(
              key
                  + "="
                  + raw.get()
                  + ": cannot be read as "
                  + argument.getType().getSimpleName()
                  + " ("
                  + reason
                  + ")"
                  + (duration ? "; " + ConfigurationSpelling.DURATION_HINT : ""));
        });
    return problems;
  }

  /**
   * Reads the integration's bean definitions, enabled or not, so that a value is checked whether or
   * not the feature that reads it is switched on — as Spring and Quarkus bind every key of their
   * properties types: into {@code bound} every {@code streamrune.*} property a constructor,
   * injected-method or injected-field argument binds, with the argument; into {@code switches}
   * every boolean one of them and every {@code streamrune.*} property a definition requires to be
   * (or not to be) {@code true} or {@code false}.
   */
  private static void readDefinitions(
      ApplicationContext context, Map<String, Argument<?>> bound, Set<String> switches) {
    for (BeanDefinitionReference<?> reference : context.getBeanDefinitionReferences()) {
      if (!reference.getBeanDefinitionName().startsWith(DEFINITIONS) || !reference.isPresent()) {
        continue;
      }
      BeanDefinition<?> definition;
      try {
        definition = reference.load(context);
      } catch (RuntimeException | LinkageError e) {
        // A definition whose optional dependency is not on the classpath (the AWS SDK, Vault):
        // its bean cannot exist, so nothing binds its properties.
        continue;
      }
      List<Argument<?>> arguments = new ArrayList<>();
      if (definition.getConstructor() != null) {
        arguments.addAll(List.of(definition.getConstructor().getArguments()));
      }
      definition
          .getInjectedMethods()
          .forEach(method -> arguments.addAll(List.of(method.getArguments())));
      definition.getInjectedFields().forEach(field -> arguments.add(field.asArgument()));
      for (Argument<?> argument : arguments) {
        argument
            .getAnnotationMetadata()
            .stringValue(Property.class, "name")
            .filter(name -> name.startsWith(PREFIX))
            .ifPresent(
                name -> {
                  bound.putIfAbsent(name, argument);
                  Class<?> type = argument.getType();
                  if (type == boolean.class || type == Boolean.class) {
                    switches.add(name);
                  }
                });
      }
      for (AnnotationValue<Requires> requires :
          definition.getAnnotationMetadata().getAnnotationValuesByType(Requires.class)) {
        Optional<String> property =
            requires.stringValue("property").filter(name -> name.startsWith(PREFIX));
        boolean comparesASwitch =
            requires.stringValue("value").filter(ConfigurationSpelling::isSwitch).isPresent()
                || requires
                    .stringValue("notEquals")
                    .filter(ConfigurationSpelling::isSwitch)
                    .isPresent();
        if (property.isPresent() && comparesASwitch) {
          switches.add(property.get());
        }
      }
    }
  }
}
