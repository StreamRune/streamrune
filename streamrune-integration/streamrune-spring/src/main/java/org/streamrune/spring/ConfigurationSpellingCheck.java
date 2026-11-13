package org.streamrune.spring;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.MethodMetadata;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.classreading.SimpleMetadataReaderFactory;
import org.streamrune.integration.ConfigurationSpelling;
import org.streamrune.integration.MisspelledPropertyNames;

/**
 * The spelling rules of {@link ConfigurationSpelling} and the names of {@link
 * MisspelledPropertyNames}, read against the application's configuration as Spring resolves it
 * (relaxed names and environment variables included).
 *
 * <p>Spring binds a duration written without its unit as milliseconds, and a switch spelled {@code
 * yes}, {@code on} or {@code 1} as {@code true} while a bean's {@code ConditionalOnProperty(
 * havingValue = "true")} does not match it. The properties read are not a list of keys: every
 * duration and boolean component of {@link StreamRuneProperties} and {@link
 * StreamRuneCryptoProperties}, and every property a condition of the integration's
 * auto-configurations compares with {@code true} or {@code false}, read from their class files
 * without loading them.
 */
final class ConfigurationSpellingCheck {

  private static final List<Class<?>> PROPERTIES_TYPES =
      List.of(StreamRuneProperties.class, StreamRuneCryptoProperties.class);

  private static final String CONFIGURATIONS = "org.streamrune.spring.";

  private ConfigurationSpellingCheck() {}

  /**
   * One line per property the configuration sets in a way the integration refuses, naming the
   * property and the value: misspelled names, durations without their unit, switches spelled other
   * than {@code true} or {@code false}.
   */
  static List<String> problems(Environment environment, ClassLoader classLoader) {
    Map<String, Class<?>> bound = boundProperties();
    List<String> durations = new ArrayList<>();
    Set<String> switches = new TreeSet<>(conditionSwitches(classLoader));
    bound.forEach(
        (key, type) -> {
          if (type == Duration.class) {
            durations.add(key);
          } else if (type == boolean.class || type == Boolean.class) {
            switches.add(key);
          }
        });
    List<String> problems =
        new ArrayList<>(MisspelledPropertyNames.problems(durations, environment::containsProperty));
    for (String key : durations) {
      String raw = environment.getProperty(key);
      if (ConfigurationSpelling.isBareNumber(raw)) {
        problems.add(ConfigurationSpelling.bareNumber(key, raw));
      }
    }
    for (String key : switches) {
      if (environment.containsProperty(key)) {
        String raw = String.valueOf(environment.getProperty(key, ""));
        if (!ConfigurationSpelling.isSwitch(raw)) {
          problems.add(ConfigurationSpelling.notASwitch(key, raw));
        }
      }
    }
    return problems;
  }

  /** Every property the properties types bind, with its type, as Spring names it. */
  static Map<String, Class<?>> boundProperties() {
    Map<String, Class<?>> bound = new TreeMap<>();
    for (Class<?> type : PROPERTIES_TYPES) {
      collect(type, type.getAnnotation(ConfigurationProperties.class).prefix(), bound);
    }
    return bound;
  }

  private static void collect(Class<?> record, String prefix, Map<String, Class<?>> into) {
    for (RecordComponent component : record.getRecordComponents()) {
      String key = prefix + "." + dashed(component.getName());
      if (component.getType().isRecord()) {
        collect(component.getType(), key, into);
      } else {
        into.put(key, component.getType());
      }
    }
  }

  /** {@code snapshotEveryNEvents} as {@code snapshot-every-n-events}, Spring's canonical form. */
  private static String dashed(String name) {
    StringBuilder dashed = new StringBuilder();
    for (char c : name.toCharArray()) {
      if (Character.isUpperCase(c)) {
        dashed.append('-').append(Character.toLowerCase(c));
      } else {
        dashed.append(c);
      }
    }
    return dashed.toString();
  }

  /**
   * Every property a {@code ConditionalOnProperty} of the integration's auto-configurations, their
   * nested classes and their bean methods compares with {@code true} or {@code false}.
   */
  static Set<String> conditionSwitches(ClassLoader classLoader) {
    Set<String> switches = new TreeSet<>();
    MetadataReaderFactory readers = new SimpleMetadataReaderFactory(classLoader);
    for (String configuration : ImportCandidates.load(AutoConfiguration.class, classLoader)) {
      if (configuration.startsWith(CONFIGURATIONS)) {
        readConditions(readers, configuration, switches);
      }
    }
    return switches;
  }

  private static void readConditions(
      MetadataReaderFactory readers, String className, Set<String> into) {
    AnnotationMetadata type;
    try {
      type = readers.getMetadataReader(className).getAnnotationMetadata();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    addSwitches(type, into);
    for (MethodMetadata method : type.getAnnotatedMethods(ConditionalOnProperty.class.getName())) {
      addSwitches(method, into);
    }
    for (String member : type.getMemberClassNames()) {
      readConditions(readers, member, into);
    }
  }

  private static void addSwitches(AnnotatedTypeMetadata metadata, Set<String> into) {
    metadata.getAnnotations().stream(ConditionalOnProperty.class)
        .forEach(
            condition -> {
              String havingValue = condition.getString("havingValue");
              if (!havingValue.isEmpty() && !ConfigurationSpelling.isSwitch(havingValue)) {
                return;
              }
              String prefix = condition.getString("prefix");
              for (String name : names(condition)) {
                into.add(prefix.isEmpty() ? name : prefix + "." + name);
              }
            });
  }

  private static List<String> names(MergedAnnotation<ConditionalOnProperty> condition) {
    List<String> names = new ArrayList<>(List.of(condition.getStringArray("name")));
    names.addAll(List.of(condition.getStringArray("value")));
    return names;
  }
}
