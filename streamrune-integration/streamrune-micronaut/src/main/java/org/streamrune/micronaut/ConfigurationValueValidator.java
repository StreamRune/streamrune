package org.streamrune.micronaut;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.convert.ArgumentConversionContext;
import io.micronaut.core.convert.ConversionContext;
import io.micronaut.core.convert.ConversionError;
import io.micronaut.core.order.Ordered;
import io.micronaut.core.type.Argument;
import io.micronaut.inject.BeanDefinition;
import io.micronaut.inject.BeanDefinitionReference;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

/**
 * Refuses the start when a {@code streamrune.*} property is set to a value that Micronaut cannot
 * convert to the type the integration binds it as, naming the property, the value and the reason.
 *
 * <p>Micronaut binds a property that carries a default, a {@code Bindable} or {@code Property}
 * default value, to that default when the configured value does not convert: {@code
 * streamrune.inbox.retention-max-age=0} — a duration without its unit — binds {@code 7d}, and
 * {@code streamrune.stripe-count=many} binds {@code 1024}, without a word. Spring and Quarkus
 * refuse such a value when they bind it; this check makes Micronaut refuse it too.
 *
 * <p>The check is not a list of keys. It reads every property the integration's beans bind — the
 * components of {@link StreamRuneMicronautProperties} and {@link
 * StreamRuneMicronautCryptoProperties}, and every {@code @Property} parameter of a bean or factory
 * method of the integration, from the metadata Micronaut generates for them — and converts each one
 * that is set as the bean would. A value that is empty or blank counts as not set, as it does on
 * Spring and Quarkus, and binds the default.
 *
 * <p>Ordered ahead of the other startup validators (order {@code 1000}), so their checks read the
 * values the operator wrote, and of the listeners that start the runners (order {@code 1100}), so
 * nothing runs on a default the operator did not ask for.
 */
@Singleton
final class ConfigurationValueValidator implements ApplicationEventListener<StartupEvent>, Ordered {

  private static final String PREFIX = "streamrune.";

  /** The package of the integration's own bean definitions, which Micronaut generates beside it. */
  private static final String DEFINITIONS = "org.streamrune.";

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
    List<String> unreadable = unreadableValues(context);
    if (!unreadable.isEmpty()) {
      throw new IllegalStateException(
          "StreamRune cannot read "
              + (unreadable.size() == 1 ? "this configuration value" : "these configuration values")
              + ":\n  "
              + String.join("\n  ", unreadable)
              + "\nA duration needs its unit, such as 0s, 500ms, 30s or 7d, or ISO-8601 such as"
              + " PT30S.");
    }
  }

  /**
   * Every {@code streamrune.*} property the integration binds that is set to a value Micronaut
   * cannot convert, as {@code key=value: reason}, in key order.
   *
   * @param context the application context whose configuration and bean definitions are read
   * @return the unreadable values; empty when every set value converts
   */
  static List<String> unreadableValues(ApplicationContext context) {
    List<String> unreadable = new ArrayList<>();
    boundProperties(context)
        .forEach(
            (key, argument) -> {
              if (!context.containsProperty(key)) {
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
              String reason =
                  conversion
                      .getLastError()
                      .map(ConversionError::getCause)
                      .map(Throwable::getMessage)
                      .orElse("no conversion to " + argument.getTypeName());
              unreadable.add(
                  key
                      + "="
                      + raw.get()
                      + ": cannot be read as "
                      + argument.getType().getSimpleName()
                      + " ("
                      + reason
                      + ")");
            });
    return unreadable;
  }

  /**
   * The {@code streamrune.*} properties the integration's beans bind, each with the argument it is
   * bound to: the constructor, injected-method and injected-field arguments of every bean
   * definition the integration declares, enabled or not, so that a value is checked whether or not
   * the feature that reads it is switched on — as Spring and Quarkus bind every key of their
   * properties types.
   */
  private static Map<String, Argument<?>> boundProperties(ApplicationContext context) {
    Map<String, Argument<?>> bound = new TreeMap<>();
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
            .ifPresent(name -> bound.putIfAbsent(name, argument));
      }
    }
    return bound;
  }
}
