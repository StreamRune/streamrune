package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.ConfigurationProperties;
import io.micronaut.context.annotation.Property;
import io.micronaut.context.annotation.Requires;
import io.micronaut.core.convert.ConversionService;
import io.micronaut.core.naming.NameUtils;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Executable;
import java.lang.reflect.Parameter;
import java.lang.reflect.RecordComponent;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamrune.integration.tck.ConfigurationDocs;
import org.streamrune.integration.tck.ConfigurationOracle;
import org.streamrune.integration.tck.ConfigurationOracle.KeyNaming;
import org.streamrune.integration.tck.ConfigurationOracle.ReadElsewhere;
import org.streamrune.integration.tck.DefaultRendering;

/**
 * Holds the Micronaut integration's configuration documentation to its code. The oracle is what the
 * integration binds and reads: {@link StreamRuneMicronautProperties} and {@link
 * StreamRuneMicronautCryptoProperties} as a started context binds them, named by their
 * {@code @Property} names, their {@code @ConfigurationProperties} groups or Micronaut's
 * hyphenation; the property of every {@code @Property} injection point and every {@code @Requires}
 * in the module, with the default it declares or implies; and the keys the projection discovery
 * looks up in code. Against it:
 *
 * <ol>
 *   <li>{@code configuration-metadata.json} declares every key, and no other, with the default this
 *       integration has;
 *   <li>the guides or this module's README name every key, and every {@code streamrune.} name they,
 *       the repository's README, the quickstart and the API reference use is a key, a meter or a
 *       span attribute;
 *   <li>every table with a {@code Default} column states the default this integration has.
 * </ol>
 */
class ConfigurationDocLintTest {

  private static final String README = "streamrune-integration/streamrune-micronaut/README.md";

  private static final String METADATA = "META-INF/configuration-metadata.json";

  /** The keys the projection discovery looks up in code; see the Spring test for the defaults. */
  private static final List<ReadElsewhere> LOOKUPS =
      List.of(
          new ReadElsewhere("streamrune.projections.auto-discovery.enabled", "true", "lookup"),
          new ReadElsewhere("streamrune.projections.<name>.enabled", "true", "lookup"),
          new ReadElsewhere("streamrune.projections.<name>.error-strategy", null, "lookup"));

  /**
   * An explicit {@code @Property} name, else the group of a nested {@code @ConfigurationProperties}
   * type, else Micronaut's hyphenation of the component name.
   */
  private static final KeyNaming MICRONAUT =
      (parent, element, name) -> {
        if (element.isAnnotationPresent(Property.class)) {
          return element.getAnnotation(Property.class).name();
        }
        if (element instanceof RecordComponent component
            && component.getType().isAnnotationPresent(ConfigurationProperties.class)) {
          return parent
              + "."
              + component.getType().getAnnotation(ConfigurationProperties.class).value();
        }
        return parent + "." + NameUtils.hyphenate(name);
      };

  private static Map<String, String> oracle;
  private static List<String> conflicts;
  private static ConfigurationDocs docs;

  @BeforeAll
  static void readTheOracleAndCheckItOnKnownKeys() throws Exception {
    StreamRuneMicronautProperties properties;
    ConfigurationOracle.Merged merged;
    try (var context = ApplicationContext.run()) {
      properties = context.getBean(StreamRuneMicronautProperties.class);
      merged = oracleOf(properties, context.getBean(StreamRuneMicronautCryptoProperties.class));
    }
    oracle = merged.keys();
    conflicts = merged.conflicts();

    // The oracle must read a default exactly as Micronaut binds it before it judges any document:
    // an explicit @Property name, a nested @ConfigurationProperties group, an injected default.
    assertEquals(Duration.ofDays(7), properties.deadLetterRetentionMaxAge());
    assertEquals("7d", oracle.get("streamrune.dead-letter.retention-max-age"));
    assertEquals(
        DefaultRendering.render(properties.inboxRetentionMaxAge()),
        oracle.get("streamrune.inbox.retention-max-age"));
    assertEquals("100", oracle.get("streamrune.snapshot-every-n-events"));
    assertEquals("5m", oracle.get("streamrune.crypto.aws.data-key-cache.expire-after-write"));
    assertEquals("1m", oracle.get("streamrune.saga.compensation-retry-interval"));
    assertEquals("1000", oracle.get("streamrune.subscription.health.lag-threshold"));
    assertEquals("false", oracle.get("streamrune.crypto.vault.enabled"));
    assertEquals("true", oracle.get("streamrune.runner-lifecycle-enabled"));

    docs = ConfigurationDocs.read(ConfigurationDocs.repositoryRoot(), README);
  }

  /**
   * The oracle of two bound properties records: their keys, named as Micronaut names them, with the
   * property of every injection point and bean condition in the module and the keys the projection
   * discovery looks up. {@code MicronautIntegrationWiringContractTest} reads the properties beans
   * of a started application with it.
   */
  static ConfigurationOracle.Merged oracleOf(
      StreamRuneMicronautProperties properties, StreamRuneMicronautCryptoProperties crypto)
      throws ClassNotFoundException {
    Map<String, String> bound =
        new TreeMap<>(ConfigurationOracle.ofRecord(properties, "streamrune", MICRONAUT));
    bound.putAll(ConfigurationOracle.ofRecord(crypto, "streamrune.crypto", MICRONAUT));
    List<ReadElsewhere> reads = new ArrayList<>(injectionPointsAndConditions());
    reads.addAll(LOOKUPS);
    return ConfigurationOracle.merge(bound, reads);
  }

  @Test
  void anInjectionPointOrAConditionAndThePropertyItReadsAgreeOnTheDefault() {
    assertEquals(List.of(), conflicts);
  }

  @Test
  void theMetadataDeclaresEveryKey() {
    assertNoProblems(
        "key(s) missing from " + METADATA,
        ConfigurationDocs.missingFrom(metadata().keySet(), oracle));
  }

  @Test
  void theMetadataStatesTheMicronautDefaultOfEveryKeyItDeclares() {
    Map<String, String> metadata = metadata();
    assertTrue(metadata.size() > 50, "the metadata's keys must be found: " + metadata);
    assertNoProblems(
        "entr(y/ies) of " + METADATA + " the code disagrees with",
        ConfigurationDocs.wrongMetadata(metadata, oracle));
  }

  private static Map<String, String> metadata() {
    var json = ConfigurationDocLintTest.class.getClassLoader().getResourceAsStream(METADATA);
    assertTrue(json != null, "the metadata must be on the classpath");
    return ConfigurationDocs.metadataDefaults(json);
  }

  @Test
  void theGuidesOrTheReadmeNameEveryKey() {
    assertNoProblems(
        "key(s) named in no guide (docs/guide) and not in " + README, docs.undocumented(oracle));
  }

  @Test
  void everyNameTheGuidesUseIsAKeyAMeterOrASpanAttribute() {
    assertNoProblems(
        "streamrune.* name(s) in the guides, "
            + README
            + " or "
            + ConfigurationDocs.otherPages(ConfigurationDocs.repositoryRoot())
            + " that Micronaut neither binds nor reads",
        docs.unknown(oracle));
  }

  /**
   * The keys this module reads in code, and those it reads through a constant of {@code
   * streamrune-integration-api}, are keys the oracle knows.
   */
  @Test
  void everyKeyTheModuleReadsInCodeIsInTheOracle() {
    List<String> unknown =
        new ArrayList<>(ConfigurationDocs.unknownInJavaSources(Path.of("src/main/java"), oracle));
    unknown.addAll(
        ConfigurationDocs.unknownInJavaSources(
            ConfigurationDocs.repositoryRoot().resolve(ConfigurationDocs.INTEGRATION_API_SOURCES),
            oracle));
    assertNoProblems(
        "streamrune.* name(s) in the string literals of this module or of"
            + " streamrune-integration-api that the oracle does not know",
        unknown);
  }

  @Test
  void everyDefaultColumnStatesTheMicronautDefault() {
    assertTrue(docs.defaultRowCount() > 30, "the Default tables of the guides must be found");
    assertNoProblems("default(s) stated differently from the code", docs.wrongDefaults(oracle));
  }

  /**
   * The property of every {@code @Property} injection point (a constructor or method parameter, a
   * field) and every {@code @Requires} of a bean or factory method in the module.
   */
  private static List<ReadElsewhere> injectionPointsAndConditions() throws ClassNotFoundException {
    List<ReadElsewhere> reads = new ArrayList<>();
    for (String type : ConfigurationDocs.typesDeclaredIn(Path.of("src/main/java"))) {
      collect(Class.forName(type, false, ConfigurationDocLintTest.class.getClassLoader()), reads);
    }
    assertTrue(reads.size() > 40, "the injection points and conditions must be found: " + reads);
    return reads;
  }

  private static void collect(Class<?> type, List<ReadElsewhere> reads) {
    addConditions(type, type.getName(), reads);
    List<Executable> executables = new ArrayList<>(List.of(type.getDeclaredConstructors()));
    executables.addAll(List.of(type.getDeclaredMethods()));
    for (Executable executable : executables) {
      String where = type.getName() + "#" + executable.getName();
      addConditions(executable, where, reads);
      for (Parameter parameter : executable.getParameters()) {
        addInjected(parameter, parameter.getType(), where, reads);
      }
    }
    for (var field : type.getDeclaredFields()) {
      addInjected(field, field.getType(), type.getName() + "#" + field.getName(), reads);
    }
    for (Class<?> nested : type.getDeclaredClasses()) {
      collect(nested, reads);
    }
  }

  /** An injected property implies its declared default, converted as Micronaut converts it. */
  private static void addInjected(
      AnnotatedElement element, Class<?> type, String where, List<ReadElsewhere> reads) {
    Property property = element.getAnnotation(Property.class);
    if (property == null || property.name().isEmpty()) {
      return;
    }
    String implied =
        property.defaultValue().isEmpty()
            ? null
            : DefaultRendering.render(
                ConversionService.SHARED.convertRequired(property.defaultValue(), boxed(type)));
    reads.add(new ReadElsewhere(property.name(), implied, where));
  }

  /**
   * A condition with a {@code defaultValue} implies it; one that requires {@code true} implies
   * {@code false}, one that requires anything but {@code false} implies {@code true}; any other
   * condition implies no default.
   */
  private static void addConditions(
      AnnotatedElement element, String where, List<ReadElsewhere> reads) {
    for (Requires requires : element.getAnnotationsByType(Requires.class)) {
      if (requires.property().isEmpty()) {
        continue;
      }
      String implied;
      if (!requires.defaultValue().isEmpty()) {
        implied = requires.defaultValue();
      } else if ("true".equals(requires.value())) {
        implied = "false";
      } else if ("false".equals(requires.notEquals())) {
        implied = "true";
      } else {
        implied = null;
      }
      reads.add(new ReadElsewhere(requires.property(), implied, where));
    }
  }

  private static Class<?> boxed(Class<?> type) {
    if (!type.isPrimitive()) {
      return type;
    }
    return switch (type.getName()) {
      case "boolean" -> Boolean.class;
      case "int" -> Integer.class;
      case "long" -> Long.class;
      case "double" -> Double.class;
      default -> throw new IllegalArgumentException("unexpected injected type " + type);
    };
  }

  private static void assertNoProblems(String what, List<String> problems) {
    if (!problems.isEmpty()) {
      fail(problems.size() + " " + what + ":\n  " + String.join("\n  ", problems));
    }
  }
}
