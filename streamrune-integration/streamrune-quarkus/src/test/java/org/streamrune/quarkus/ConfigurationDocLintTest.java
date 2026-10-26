package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamrune.integration.tck.ConfigurationDocs;
import org.streamrune.integration.tck.ConfigurationOracle;
import org.streamrune.integration.tck.ConfigurationOracle.KeyNaming;
import org.streamrune.integration.tck.ConfigurationOracle.ReadElsewhere;
import org.streamrune.integration.tck.DefaultRendering;

/**
 * Holds the Quarkus integration's configuration documentation to its code. The oracle is what the
 * integration binds and reads: {@link StreamRuneQuarkusProperties} and {@link
 * StreamRuneQuarkusCryptoProperties} built by SmallRye Config with no source ({@link
 * TestProperties}, the machinery Quarkus runs), named by {@code @WithName} or SmallRye's kebab
 * case, and the keys the projection discovery looks up in code. Against it:
 *
 * <ol>
 *   <li>this module's README names every key (Quarkus has no configuration metadata file of its
 *       own);
 *   <li>the guides or the README name every key, and every {@code streamrune.} name they use is a
 *       key, a meter or a span attribute;
 *   <li>every table with a {@code Default} column states the default this integration has.
 * </ol>
 */
class ConfigurationDocLintTest {

  private static final String README = "streamrune-integration/streamrune-quarkus/README.md";

  /** The keys the projection discovery looks up in code; see the Spring test for the defaults. */
  private static final List<ReadElsewhere> LOOKUPS =
      List.of(
          new ReadElsewhere("streamrune.projections.auto-discovery.enabled", "true", "lookup"),
          new ReadElsewhere("streamrune.projections.<name>.enabled", "true", "lookup"),
          new ReadElsewhere("streamrune.projections.<name>.error-strategy", null, "lookup"));

  /** {@code @WithName} where present, else SmallRye's kebab case of the method name. */
  private static final KeyNaming SMALLRYE =
      (parent, element, name) ->
          parent
              + "."
              + (element.isAnnotationPresent(WithName.class)
                  ? element.getAnnotation(WithName.class).value()
                  : ConfigMapping.NamingStrategy.KEBAB_CASE.apply(name));

  private static Map<String, String> oracle;
  private static List<String> conflicts;
  private static ConfigurationDocs docs;

  @BeforeAll
  static void readTheOracleAndCheckItOnKnownKeys() {
    StreamRuneQuarkusProperties properties = TestProperties.defaults();
    StreamRuneQuarkusCryptoProperties crypto = TestProperties.crypto(Map.of());
    ConfigurationOracle.Merged merged = oracleOf(properties, crypto);
    oracle = merged.keys();
    conflicts = merged.conflicts();

    // The oracle must read a default exactly as SmallRye binds it before it judges any document:
    // @WithDefault on a nested interface, @WithName, an Optional without a default.
    assertEquals(Duration.ofDays(7), properties.deadLetter().retentionMaxAge());
    assertEquals("7d", oracle.get("streamrune.dead-letter.retention-max-age"));
    assertEquals(
        DefaultRendering.render(properties.inbox().retentionMaxAge()),
        oracle.get("streamrune.inbox.retention-max-age"));
    assertEquals("100", oracle.get("streamrune.snapshot-every-n-events"));
    assertEquals("1000", oracle.get("streamrune.subscription.health.lag-threshold"));
    assertEquals("—", oracle.get("streamrune.crypto.aws.kms-key-id"));
    assertEquals("5m", oracle.get("streamrune.crypto.aws.data-key-cache.expire-after-write"));
    assertEquals("5m", oracle.get("streamrune.crypto.cache.expire-after-write"));
    assertEquals("false", oracle.get("streamrune.crypto.vault.enabled"));

    docs = ConfigurationDocs.read(ConfigurationDocs.repositoryRoot(), README);
  }

  /**
   * The oracle of two bound mappings: their keys, named as SmallRye names them, with the keys the
   * projection discovery looks up. {@code QuarkusIntegrationWiringContractTest} reads the mappings
   * of a started application with it.
   */
  static ConfigurationOracle.Merged oracleOf(
      StreamRuneQuarkusProperties properties, StreamRuneQuarkusCryptoProperties crypto) {
    Map<String, String> bound =
        new TreeMap<>(
            ConfigurationOracle.ofConfigMapping(
                properties, StreamRuneQuarkusProperties.class, "streamrune", SMALLRYE));
    bound.putAll(
        ConfigurationOracle.ofConfigMapping(
            crypto, StreamRuneQuarkusCryptoProperties.class, "streamrune.crypto", SMALLRYE));
    return ConfigurationOracle.merge(bound, LOOKUPS);
  }

  @Test
  void aLookupAndThePropertyItReadsAgreeOnTheDefault() {
    assertEquals(List.of(), conflicts);
  }

  @Test
  void theReadmeNamesEveryKey() throws Exception {
    Set<String> named = new TreeSet<>();
    Matcher name =
        Pattern.compile("streamrune(?:\\.(?:[a-z0-9]+(?:[-_][a-z0-9]+)*|<[a-z-]+>))+")
            .matcher(Files.readString(ConfigurationDocs.repositoryRoot().resolve(README)));
    while (name.find()) {
      named.add(name.group());
    }
    assertNoProblems(
        "key(s) missing from " + README + ", the Quarkus configuration reference",
        ConfigurationDocs.missingFrom(named, oracle));
  }

  /**
   * The README's {@code application.properties} listing sets every key to its default: bound with
   * the listing as its only source, each mapping reads as it reads with no source at all.
   */
  @Test
  void theReadmeListingSetsEveryKeyToItsDefault() throws Exception {
    Map<String, String> listing = new TreeMap<>();
    List<String> lines = Files.readAllLines(ConfigurationDocs.repositoryRoot().resolve(README));
    int start = lines.indexOf("## Configuration");
    assertTrue(start >= 0, "the README's ## Configuration section must be found");
    int fence = start;
    while (!lines.get(fence).strip().equals("```properties")) {
      fence++;
    }
    for (int i = fence + 1; !lines.get(i).strip().equals("```"); i++) {
      String line = lines.get(i).strip();
      if (!line.isEmpty() && !line.startsWith("#")) {
        listing.put(line.substring(0, line.indexOf('=')), line.substring(line.indexOf('=') + 1));
      }
    }
    Map<String, String> mapped = new TreeMap<>();
    Map<String, String> cryptoMapped = new TreeMap<>();
    listing.forEach(
        (key, value) -> {
          if (key.startsWith("streamrune.crypto.")) {
            cryptoMapped.put(key, value);
          } else if (!key.startsWith("streamrune.projections.")) {
            mapped.put(key, value);
          }
        });
    assertTrue(listing.size() > 50, "the listing must be found: " + listing);
    assertEquals(
        ConfigurationOracle.ofConfigMapping(
            TestProperties.defaults(), StreamRuneQuarkusProperties.class, "streamrune", SMALLRYE),
        ConfigurationOracle.ofConfigMapping(
            TestProperties.of(mapped), StreamRuneQuarkusProperties.class, "streamrune", SMALLRYE));
    assertEquals(
        ConfigurationOracle.ofConfigMapping(
            TestProperties.crypto(Map.of()),
            StreamRuneQuarkusCryptoProperties.class,
            "streamrune.crypto",
            SMALLRYE),
        ConfigurationOracle.ofConfigMapping(
            TestProperties.crypto(cryptoMapped),
            StreamRuneQuarkusCryptoProperties.class,
            "streamrune.crypto",
            SMALLRYE));
  }

  @Test
  void theGuidesOrTheReadmeNameEveryKey() {
    assertNoProblems(
        "key(s) named in no guide (docs/guide) and not in " + README, docs.undocumented(oracle));
  }

  @Test
  void everyNameTheGuidesUseIsAKeyAMeterOrASpanAttribute() {
    assertNoProblems(
        "streamrune.* name(s) in the guides or " + README + " that Quarkus neither binds nor reads",
        docs.unknown(oracle));
  }

  @Test
  void everyKeyTheModuleReadsInCodeIsInTheOracle() {
    assertNoProblems(
        "streamrune.* name(s) in this module's string literals that the oracle does not know",
        ConfigurationDocs.unknownInJavaSources(Path.of("src/main/java"), oracle));
  }

  @Test
  void everyDefaultColumnStatesTheQuarkusDefault() {
    assertTrue(docs.defaultRowCount() > 30, "the Default tables of the guides must be found");
    assertNoProblems("default(s) stated differently from the code", docs.wrongDefaults(oracle));
  }

  private static void assertNoProblems(String what, List<String> problems) {
    if (!problems.isEmpty()) {
      fail(problems.size() + " " + what + ":\n  " + String.join("\n  ", problems));
    }
  }
}
