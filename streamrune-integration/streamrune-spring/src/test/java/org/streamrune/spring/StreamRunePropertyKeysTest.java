package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.MethodMetadata;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.springframework.core.type.classreading.SimpleMetadataReaderFactory;
import org.streamrune.integration.ConfigurationSpelling;

/**
 * The constants of {@link StreamRunePropertyKeys} are the keys the properties types bind and the
 * auto-configurations' conditions read, derived here on the JVM from the record components and from
 * the class files the {@code AutoConfiguration.imports} file names: a component or a condition
 * added without its constant fails this test.
 */
class StreamRunePropertyKeysTest {

  private static final List<Class<?>> PROPERTIES_TYPES =
      List.of(StreamRuneProperties.class, StreamRuneCryptoProperties.class);

  @Test
  void theDurationsAreTheDurationComponentsOfThePropertiesTypes() {
    assertEquals(
        new TreeSet<>(keysOfType(Duration.class)), new TreeSet<>(StreamRunePropertyKeys.DURATIONS));
  }

  @Test
  void theSwitchesAreTheBooleanComponentsAndTheConditionSwitches() {
    Set<String> switches = new TreeSet<>(keysOfType(boolean.class));
    switches.addAll(conditionSwitches());
    assertEquals(switches, new TreeSet<>(StreamRunePropertyKeys.SWITCHES));
  }

  /** The walk finds the switches only a condition reads, such as a crypto backend's. */
  @Test
  void theConditionWalkFindsTheConditionOnlySwitches() {
    Set<String> switches = conditionSwitches();
    for (String key :
        List.of(
            "streamrune.sse.enabled",
            "streamrune.validation-enabled",
            "streamrune.crypto.filesystem.enabled",
            "streamrune.crypto.postgres.enabled",
            "streamrune.crypto.vault.enabled",
            "streamrune.crypto.aws.enabled")) {
      assertTrue(switches.contains(key), key + " in " + switches);
    }
  }

  @Test
  void theRecordWalkNamesKeysAsSpringNamesThem() {
    Map<String, Class<?>> bound = boundProperties();
    assertEquals(int.class, bound.get("streamrune.snapshot-every-n-events"));
    assertEquals(boolean.class, bound.get("streamrune.tracing.event-store.enabled"));
    assertEquals(String.class, bound.get("streamrune.subscription.single-active-consumer.enabled"));
  }

  private static List<String> keysOfType(Class<?> type) {
    return boundProperties().entrySet().stream()
        .filter(entry -> entry.getValue() == type)
        .map(Map.Entry::getKey)
        .toList();
  }

  /** Every property the properties types bind, with its type, as Spring names it. */
  private static Map<String, Class<?>> boundProperties() {
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
   * Every {@code streamrune.*} property a {@code ConditionalOnProperty} of the integration's
   * auto-configurations, their nested classes and their bean methods compares with {@code true} or
   * {@code false}, read from the class files.
   */
  private static Set<String> conditionSwitches() {
    ClassLoader classLoader = StreamRunePropertyKeysTest.class.getClassLoader();
    Set<String> switches = new TreeSet<>();
    MetadataReaderFactory readers = new SimpleMetadataReaderFactory(classLoader);
    for (String configuration : ImportCandidates.load(AutoConfiguration.class, classLoader)) {
      if (configuration.startsWith("org.streamrune.spring.")) {
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
      throw new IllegalStateException(e);
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
                String key = prefix.isEmpty() ? name : prefix + "." + name;
                if (key.startsWith("streamrune.")) {
                  into.add(key);
                }
              }
            });
  }

  private static List<String> names(MergedAnnotation<ConditionalOnProperty> condition) {
    List<String> names = new ArrayList<>(List.of(condition.getStringArray("name")));
    names.addAll(List.of(condition.getStringArray("value")));
    return names;
  }
}
