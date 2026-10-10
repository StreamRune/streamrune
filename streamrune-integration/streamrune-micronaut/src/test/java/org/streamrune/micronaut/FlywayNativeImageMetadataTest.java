package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Checks the Flyway native-image metadata the Micronaut integration ships against the Flyway on the
 * class path.
 *
 * <p>Schema auto-initialization runs Flyway inside the application, and Flyway reaches parts of
 * itself by name at run time: it copies its configuration extensions field by field and
 * instantiates its log creator from a class name. A native image can do that only for what was
 * registered at build time; without the fields the binary stops at startup with a {@code
 * MissingReflectionRegistrationError}. Flyway's plugins need no entry: native-image registers the
 * providers of a {@code ServiceLoader} lookup on its own.
 *
 * <p>What Flyway needs is read from the class path, so a Flyway upgrade that adds, renames or
 * removes one of those classes fails here instead of in an application's native binary. The
 * registration itself takes effect only in a native image, which the demo's native smoke test
 * builds and starts on an empty database.
 */
class FlywayNativeImageMetadataTest {

  private static final String METADATA =
      "/META-INF/native-image/org.streamrune/streamrune-micronaut-flyway/reachability-metadata.json";
  private static final String PLUGIN_SERVICE_FILE =
      "META-INF/services/org.flywaydb.core.extensibility.Plugin";
  private static final String CONFIGURATION_EXTENSION =
      "org.flywaydb.core.extensibility.ConfigurationExtension";
  private static final String LOG_CREATOR =
      "org.flywaydb.core.internal.logging.slf4j.Slf4jLogCreator";

  /**
   * {@code PluginRegister#getCopy()} copies every configuration extension. An extension that
   * inherits {@code ConfigurationExtension#copy()} is copied reflectively: a new instance from
   * {@code getDeclaredConstructor()}, then {@code MergeUtils.copyModel}, which walks {@code
   * getDeclaredFields()} of the class and its superclasses.
   */
  @Test
  void registersEveryConfigurationExtensionFlywayCopiesFieldByField() throws Exception {
    Map<String, JsonNode> registered = reflectionEntries();
    Set<String> copied = reflectivelyCopiedConfigurationExtensions();

    assertThat(copied)
        .contains(
            "org.flywaydb.database.postgresql.PostgreSQLConfigurationExtension",
            "org.flywaydb.core.api.migration.baseline.BaselineMigrationConfigurationExtension");
    for (String extension : copied) {
      Class<?> type = Class.forName(extension);
      assertThat(type.getSuperclass())
          .as("%s declares all of its fields itself", extension)
          .isEqualTo(Object.class);
      assertThat(type.getDeclaredConstructor().getParameterCount()).isZero();
      assertThat(registered).as("reflection entry for %s", extension).containsKey(extension);
      assertThat(registered.get(extension).path("allDeclaredFields").asBoolean())
          .as("%s is registered with its declared fields", extension)
          .isTrue();
      assertThat(registersNoArgumentConstructor(registered.get(extension)))
          .as("%s is registered with its no-argument constructor", extension)
          .isTrue();
    }
  }

  /**
   * StreamRune tells Flyway to log through SLF4J ({@code ShippedMigrationSeries}), and {@code
   * LogFactory} instantiates the log creator for it from this class name, through {@code
   * getDeclaredConstructor()}.
   */
  @Test
  void registersFlywaysSlf4jLogCreatorWithItsNoArgumentConstructor() throws Exception {
    Map<String, JsonNode> registered = reflectionEntries();
    Class<?> logCreator = Class.forName(LOG_CREATOR);

    assertThat(Class.forName("org.flywaydb.core.api.logging.LogCreator"))
        .isAssignableFrom(logCreator);
    assertThat(logCreator.getDeclaredConstructor().getParameterCount()).isZero();
    assertThat(registered).containsKey(LOG_CREATOR);
    assertThat(registersNoArgumentConstructor(registered.get(LOG_CREATOR))).isTrue();
  }

  /** An entry for a class Flyway renamed or dropped would register nothing. */
  @Test
  void registersNothingButTheCopiedExtensionsAndTheLogCreator() throws Exception {
    Set<String> expected = new LinkedHashSet<>(reflectivelyCopiedConfigurationExtensions());
    expected.add(LOG_CREATOR);
    JsonNode metadata = metadata();

    assertThat(reflectionEntries().keySet()).containsExactlyInAnyOrderElementsOf(expected);
    for (JsonNode entry : metadata.path("reflection")) {
      assertThat(entry.path("condition").isMissingNode())
          .as("Flyway runs at startup, before a condition type is reached: %s", entry)
          .isTrue();
    }
    assertThat(metadata.path("resources").isMissingNode())
        .as("the resources Flyway reads are registered in resource-config.json")
        .isTrue();
  }

  private static boolean registersNoArgumentConstructor(JsonNode entry) {
    for (JsonNode method : entry.path("methods")) {
      if ("<init>".equals(method.path("name").asText())
          && method.path("parameterTypes").isArray()
          && method.path("parameterTypes").isEmpty()) {
        return true;
      }
    }
    return false;
  }

  /** The plugins among those on the class path that inherit the reflective copy. */
  private Set<String> reflectivelyCopiedConfigurationExtensions() throws Exception {
    Class<?> configurationExtension = Class.forName(CONFIGURATION_EXTENSION);
    Set<String> copied = new LinkedHashSet<>();
    for (String plugin : pluginsOnTheClassPath()) {
      Class<?> type = Class.forName(plugin);
      if (configurationExtension.isAssignableFrom(type)
          && type.getMethod("copy").getDeclaringClass() == configurationExtension) {
        copied.add(plugin);
      }
    }
    return copied;
  }

  /** Every provider named by a Flyway plugin service file on the class path. */
  private Set<String> pluginsOnTheClassPath() throws IOException {
    Set<String> plugins = new LinkedHashSet<>();
    for (URL url :
        Collections.list(getClass().getClassLoader().getResources(PLUGIN_SERVICE_FILE))) {
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(url.openStream(), StandardCharsets.UTF_8))) {
        for (String line = reader.readLine(); line != null; line = reader.readLine()) {
          int comment = line.indexOf('#');
          String provider = (comment < 0 ? line : line.substring(0, comment)).strip();
          if (!provider.isEmpty()) {
            plugins.add(provider);
          }
        }
      }
    }
    return plugins;
  }

  private Map<String, JsonNode> reflectionEntries() throws IOException {
    Map<String, JsonNode> byType = new LinkedHashMap<>();
    for (JsonNode entry : metadata().path("reflection")) {
      assertThat(byType.put(entry.path("type").asText(), entry))
          .as("one entry per type: %s", entry)
          .isNull();
    }
    return byType;
  }

  private JsonNode metadata() throws IOException {
    try (InputStream in = getClass().getResourceAsStream(METADATA)) {
      assertThat(in).as(METADATA).isNotNull();
      return new ObjectMapper().readTree(in);
    }
  }
}
