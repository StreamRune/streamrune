package org.streamrune.spring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.AnnotatedElement;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.streamrune.integration.tck.ConfigurationDocs;
import org.streamrune.integration.tck.ConfigurationOracle;
import org.streamrune.integration.tck.ConfigurationOracle.ReadElsewhere;
import org.streamrune.integration.tck.DefaultRendering;

/**
 * Holds the Spring integration's configuration documentation to its code. The oracle is what the
 * integration binds and reads: {@link StreamRuneProperties} and {@link StreamRuneCryptoProperties}
 * bound by Spring's {@link Binder} from an empty source (so a default set by {@code @DefaultValue}
 * or by a compact constructor is the one Spring produces), the property of every {@link
 * ConditionalOnProperty} in the module, and the keys {@code ProjectionAutoConfig} looks up in code.
 * Against it:
 *
 * <ol>
 *   <li>{@code additional-spring-configuration-metadata.json} declares every key, and no other,
 *       with the default this integration has;
 *   <li>the guides or this module's README name every key, and every {@code streamrune.} name they
 *       use is a key, a meter or a span attribute;
 *   <li>every table with a {@code Default} column states the default this integration has.
 * </ol>
 */
class ConfigurationDocLintTest {

  private static final String README = "streamrune-integration/streamrune-spring/README.md";

  private static final String METADATA = "META-INF/additional-spring-configuration-metadata.json";

  /**
   * The keys {@code ProjectionAutoConfig} looks up in the environment by name: they are neither
   * bound nor a bean condition. A projection's own keys have no default of their own beyond
   * "enabled": an absent {@code error-strategy} keeps the one {@code @ProjectionConfig} names.
   */
  private static final List<ReadElsewhere> LOOKUPS =
      List.of(
          new ReadElsewhere("streamrune.projections.auto-discovery.enabled", "true", "lookup"),
          new ReadElsewhere("streamrune.projections.<name>.enabled", "true", "lookup"),
          new ReadElsewhere("streamrune.projections.<name>.error-strategy", null, "lookup"));

  private static Map<String, String> oracle;
  private static List<String> conflicts;
  private static ConfigurationDocs docs;

  @BeforeAll
  static void readTheOracleAndCheckItOnKnownKeys() throws Exception {
    Binder binder = new Binder(new MapConfigurationPropertySource());
    StreamRuneProperties properties =
        binder.bindOrCreate("streamrune", Bindable.of(StreamRuneProperties.class));
    StreamRuneCryptoProperties crypto =
        binder.bindOrCreate("streamrune.crypto", Bindable.of(StreamRuneCryptoProperties.class));
    Map<String, String> bound =
        new TreeMap<>(ConfigurationOracle.ofRecord(properties, "streamrune"));
    bound.putAll(ConfigurationOracle.ofRecord(crypto, "streamrune.crypto"));
    List<ReadElsewhere> reads = new ArrayList<>(conditions());
    reads.addAll(LOOKUPS);
    ConfigurationOracle.Merged merged = ConfigurationOracle.merge(bound, reads);
    oracle = merged.keys();
    conflicts = merged.conflicts();

    // The oracle must read a default exactly as Spring binds it before it judges any document.
    assertEquals(Duration.ofDays(7), properties.deadLetter().retentionMaxAge());
    assertEquals("7d", oracle.get("streamrune.dead-letter.retention-max-age"));
    assertEquals(
        DefaultRendering.render(properties.inbox().retentionMaxAge()),
        oracle.get("streamrune.inbox.retention-max-age"));
    assertEquals("100", oracle.get("streamrune.snapshot-every-n-events"));
    assertEquals("—", oracle.get("streamrune.metadata.baggage-allowlist"));
    assertEquals("1m", oracle.get("streamrune.saga.compensation-retry-interval"));
    assertEquals("5m", oracle.get("streamrune.crypto.aws.data-key-cache.expire-after-write"));
    assertEquals("false", oracle.get("streamrune.crypto.vault.enabled"));
    assertEquals("true", oracle.get("streamrune.dead-letter.enabled"));

    docs = ConfigurationDocs.read(ConfigurationDocs.repositoryRoot(), README);
  }

  @Test
  void aBeanConditionAndThePropertyItReadsAgreeOnTheDefault() {
    assertEquals(List.of(), conflicts);
  }

  @Test
  void theMetadataDeclaresEveryKey() {
    assertNoProblems(
        "key(s) missing from " + METADATA,
        ConfigurationDocs.missingFrom(metadata().keySet(), oracle));
  }

  @Test
  void theMetadataStatesTheSpringDefaultOfEveryKeyItDeclares() {
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
        "streamrune.* name(s) in the guides or " + README + " that Spring neither binds nor reads",
        docs.unknown(oracle));
  }

  @Test
  void everyKeyTheModuleReadsInCodeIsInTheOracle() {
    assertNoProblems(
        "streamrune.* name(s) in this module's string literals that the oracle does not know",
        ConfigurationDocs.unknownInJavaSources(Path.of("src/main/java"), oracle));
  }

  @Test
  void everyDefaultColumnStatesTheSpringDefault() {
    assertTrue(docs.defaultRowCount() > 30, "the Default tables of the guides must be found");
    assertNoProblems("default(s) stated differently from the code", docs.wrongDefaults(oracle));
  }

  /**
   * The property of every {@link ConditionalOnProperty} in the module, with the default it implies.
   */
  private static List<ReadElsewhere> conditions() throws ClassNotFoundException {
    List<ReadElsewhere> reads = new ArrayList<>();
    for (String type : ConfigurationDocs.typesDeclaredIn(Path.of("src/main/java"))) {
      collectConditions(
          Class.forName(type, false, ConfigurationDocLintTest.class.getClassLoader()), reads);
    }
    assertTrue(reads.size() >= 15, "the bean conditions of the module must be found: " + reads);
    return reads;
  }

  private static void collectConditions(Class<?> type, List<ReadElsewhere> reads) {
    addConditions(type, type.getName(), reads);
    for (var method : type.getDeclaredMethods()) {
      addConditions(method, type.getName() + "#" + method.getName(), reads);
    }
    for (Class<?> nested : type.getDeclaredClasses()) {
      collectConditions(nested, reads);
    }
  }

  /**
   * A condition that matches a missing property implies the value it expects, {@code true} unless
   * it names another; one that expects {@code true} and does not match a missing property implies
   * {@code false}; any other condition implies no default.
   */
  private static void addConditions(
      AnnotatedElement element, String where, List<ReadElsewhere> reads) {
    for (ConditionalOnProperty condition :
        element.getAnnotationsByType(ConditionalOnProperty.class)) {
      String prefix =
          condition.prefix().isEmpty() ? "" : condition.prefix().replaceAll("\\.$", "") + ".";
      String expected = condition.havingValue();
      String implied =
          condition.matchIfMissing()
              ? (expected.isEmpty() ? "true" : expected)
              : ("true".equals(expected) ? "false" : null);
      List<String> names = new ArrayList<>(List.of(condition.name()));
      names.addAll(List.of(condition.value()));
      for (String name : names) {
        reads.add(new ReadElsewhere(prefix + name, implied, where));
      }
    }
  }

  private static void assertNoProblems(String what, List<String> problems) {
    if (!problems.isEmpty()) {
      fail(problems.size() + " " + what + ":\n  " + String.join("\n  ", problems));
    }
  }
}
