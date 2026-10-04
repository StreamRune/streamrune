package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.boot.diagnostics.FailureAnalysis;

class StreamRuneFailureAnalyzerTest {

  private final StreamRuneFailureAnalyzer analyzer = new StreamRuneFailureAnalyzer();

  @Test
  void analyzesMissingDataSource() {
    var ex = new NoSuchBeanDefinitionException(javax.sql.DataSource.class);
    FailureAnalysis analysis = analyzer.analyze(null, ex);
    assertThat(analysis).isNotNull();
    assertThat(analysis.getDescription()).contains("DataSource");
    assertThat(analysis.getAction()).contains("spring-boot-starter-jdbc");
  }

  @Test
  void returnsNullForUnrelatedExceptions() {
    var ex = new NoSuchBeanDefinitionException("unrelated");
    FailureAnalysis analysis = analyzer.analyze(null, ex);
    assertThat(analysis).isNull();
  }

  @Test
  void analyzesMissingCryptoEngine_namingTheRealEnableKnobs() {
    // The analyzer used to tell operators to set `streamrune.crypto.provider` — a key
    // NOTHING reads (the backends are enabled per-backend via @ConditionalOnProperty). During a
    // boot-failure outage the remedy text must name the knobs that actually exist.
    var ex = new NoSuchBeanDefinitionException(org.streamrune.core.crypto.CryptoEngine.class);
    FailureAnalysis analysis = analyzer.analyze(null, ex);
    assertThat(analysis).isNotNull();
    assertThat(analysis.getAction())
        .contains("streamrune.crypto.filesystem.enabled")
        .contains("streamrune.crypto.postgres.enabled")
        .contains("streamrune.crypto.vault.enabled")
        .contains("streamrune.crypto.aws.enabled")
        .doesNotContain("streamrune.crypto.provider");
  }

  /**
   * Guard: a {@code FailureAnalyzer}'s remedy text is the operator's lifeline during a boot
   * failure, so every {@code streamrune.*} property key it names MUST exist as a real,
   * bound-or-read knob — a renamed knob must never strand an operator with a dead-end remedy again.
   * The module generates no {@code spring-configuration-metadata.json} (no configuration processor
   * is applied), so the oracle is derived mechanically from the actual binding roots: the {@link
   * StreamRuneProperties} record tree (prefix {@code streamrune}), the {@link
   * StreamRuneCryptoProperties} record tree (prefix {@code streamrune.crypto}, registered by {@code
   * CryptoPropertiesConfiguration}), and every {@code @ConditionalOnProperty} key in the module's
   * sources (the crypto {@code <backend>.enabled} flags exist only there).
   */
  @Test
  void everyStreamRunePropertyKeyNamedByAFailureAnalyzerIsARealKnob() throws Exception {
    Set<String> validKeys = new TreeSet<>();
    collectRecordKeys(StreamRuneProperties.class, "streamrune", validKeys);
    collectRecordKeys(StreamRuneCryptoProperties.class, "streamrune.crypto", validKeys);
    collectConditionalOnPropertyKeys(validKeys);

    // Every streamrune.* key mentioned anywhere in a FailureAnalyzer source (string literals,
    // javadoc and comments alike — anything an analyzer names must be real). The lookbehind
    // excludes package/type references such as org.streamrune.spring.
    Pattern keyShape = Pattern.compile("(?<![\\w.])streamrune\\.[a-z0-9]+(?:[.-][a-z0-9]+)*");
    List<String> unknown = new ArrayList<>();
    List<Path> analyzers = new ArrayList<>();
    Path mainSources = Path.of("src/main/java");
    assertTrue(
        Files.isDirectory(mainSources),
        "guard test must run with the module directory as working dir (Gradle default)");
    try (var sources = Files.walk(mainSources)) {
      sources
          .filter(p -> p.getFileName().toString().endsWith("FailureAnalyzer.java"))
          .forEach(analyzers::add);
    }
    assertThat(analyzers).as("the module's FailureAnalyzer sources must be found").isNotEmpty();
    for (Path analyzerSource : analyzers) {
      Matcher m = keyShape.matcher(Files.readString(analyzerSource));
      while (m.find()) {
        if (!validKeys.contains(m.group())) {
          unknown.add(analyzerSource.getFileName() + " names '" + m.group() + "'");
        }
      }
    }
    assertThat(unknown)
        .as(
            "every streamrune.* key a FailureAnalyzer names must exist as a real knob"
                + " (bound property or @ConditionalOnProperty); known keys: %s",
            validKeys)
        .isEmpty();
  }

  /** Adds every key of a constructor-bound record tree, plus namespace prefixes. */
  private static void collectRecordKeys(Class<?> type, String prefix, Set<String> keys) {
    keys.add(prefix);
    for (var component : type.getRecordComponents()) {
      String key = prefix + "." + kebab(component.getName());
      if (component.getType().isRecord()) {
        collectRecordKeys(component.getType(), key, keys);
      } else {
        keys.add(key);
      }
    }
  }

  /**
   * Spring relaxed-binding canonical form: {@code snapshotEveryNEvents → snapshot-every-n-events}.
   */
  private static String kebab(String camel) {
    return camel
        .replaceAll("([a-z0-9])([A-Z])", "$1-$2")
        .replaceAll("([A-Z])([A-Z][a-z])", "$1-$2")
        .toLowerCase(Locale.ROOT);
  }

  /**
   * Keys readable only through {@code @ConditionalOnProperty} (never bound to a properties bean),
   * scraped from the module's sources — covers both the {@code prefix = "...", name = "..."} and
   * the full-key {@code name = "..."} forms used in this module.
   */
  private static void collectConditionalOnPropertyKeys(Set<String> keys) throws IOException {
    Pattern prefixed =
        Pattern.compile(
            "@ConditionalOnProperty\\(\\s*prefix\\s*=\\s*\"([^\"]+)\"\\s*,\\s*name\\s*=\\s*\"([^\"]+)\"");
    Pattern plain = Pattern.compile("@ConditionalOnProperty\\(\\s*name\\s*=\\s*\"([^\"]+)\"");
    try (var sources = Files.walk(Path.of("src/main/java"))) {
      for (Path source : sources.filter(p -> p.toString().endsWith(".java")).toList()) {
        String text = Files.readString(source);
        Matcher m = prefixed.matcher(text);
        while (m.find()) {
          keys.add(m.group(1) + "." + m.group(2));
        }
        m = plain.matcher(text);
        while (m.find()) {
          keys.add(m.group(1));
        }
      }
    }
  }
}
