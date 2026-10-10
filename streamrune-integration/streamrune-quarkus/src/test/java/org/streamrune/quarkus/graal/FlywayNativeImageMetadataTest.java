package org.streamrune.quarkus.graal;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Checks the Flyway native-image metadata the Quarkus integration ships against the Flyway on the
 * class path.
 *
 * <p>Schema auto-initialization runs Flyway inside the application, and Flyway reaches parts of
 * itself by name at run time: it finds its plugins with {@code ServiceLoader}, copies its
 * configuration extensions field by field, and instantiates its log creator from a class name. A
 * native image can do each of those only for what was registered at build time, and a Quarkus build
 * registers none of it on its own: it switches GraalVM's {@code ServiceLoader} registration off and
 * does not apply the GraalVM reachability-metadata repository. Without the registration the binary
 * fails at startup inside Flyway.
 *
 * <p>What Flyway needs is read from the class path, so a Flyway upgrade that adds, renames or
 * removes one of those classes fails here instead of in an application's native binary. The
 * registration itself takes effect only in a native image, which the demo's native smoke test
 * builds and starts on an empty database.
 */
class FlywayNativeImageMetadataTest {

  private static final String DIRECTORY =
      "/META-INF/native-image/org.streamrune/streamrune-quarkus-flyway/";
  private static final String PLUGIN_SERVICE_FILE =
      "META-INF/services/org.flywaydb.core.extensibility.Plugin";
  private static final String CONFIGURATION_EXTENSION =
      "org.flywaydb.core.extensibility.ConfigurationExtension";
  private static final String LOG_CREATOR =
      "org.flywaydb.core.internal.logging.slf4j.Slf4jLogCreator";
  private static final String INSERT_ROW_LOCK = "org.flywaydb.core.internal.database.InsertRowLock";

  /**
   * {@code PluginRegister} loads every provider the service files name with {@code
   * ServiceLoader.load(Plugin.class)}, the PostgreSQL database type among them. {@code
   * ServiceLoader} instantiates a provider through its public no-argument constructor, and refuses
   * the whole lookup when a named provider cannot be loaded.
   */
  @Test
  void registersEveryFlywayPluginOnTheClassPathWithItsNoArgumentConstructor() throws Exception {
    Map<String, JsonNode> registered = reflectionEntries();
    Set<String> plugins = pluginsOnTheClassPath();

    assertThat(plugins)
        .contains(
            "org.flywaydb.database.postgresql.PostgreSQLDatabaseType",
            "org.flywaydb.core.internal.resource.CoreResourceTypeProvider");
    for (String plugin : plugins) {
      assertThat(Class.forName(plugin).getConstructor().getParameterCount()).isZero();
      assertThat(registered).as("reflection entry for the plugin %s", plugin).containsKey(plugin);
      assertThat(registersNoArgumentConstructor(registered.get(plugin)))
          .as("%s is registered with its no-argument constructor", plugin)
          .isTrue();
    }
  }

  /** {@code ServiceLoader} reads the provider names from this resource in the image. */
  @Test
  void registersTheServiceFileFlywayReadsItsPluginsFrom() throws Exception {
    JsonNode resources = metadata().path("resources");

    assertThat(resources).hasSize(1);
    assertThat(resources.get(0).path("glob").asText()).isEqualTo(PLUGIN_SERVICE_FILE);
    assertThat(resources.get(0).path("condition").isMissingNode()).isTrue();
    assertThat(Collections.list(getClass().getClassLoader().getResources(PLUGIN_SERVICE_FILE)))
        .as("flyway-core and flyway-database-postgresql each ship the service file")
        .hasSizeGreaterThanOrEqualTo(2);
  }

  /**
   * {@code PluginRegister#getCopy()} copies every configuration extension. An extension that
   * inherits {@code ConfigurationExtension#copy()} is copied reflectively: a new instance from
   * {@code getDeclaredConstructor()}, then {@code MergeUtils.copyModel}, which walks {@code
   * getDeclaredFields()} of the class and its superclasses. A native image answers that with no
   * fields for a class registered without them, and the copy silently loses the extension's
   * settings.
   */
  @Test
  void registersTheFieldsOfEveryConfigurationExtensionFlywayCopiesFieldByField() throws Exception {
    Map<String, JsonNode> registered = reflectionEntries();
    Set<String> copied = reflectivelyCopiedConfigurationExtensions();

    assertThat(copied)
        .contains(
            "org.flywaydb.database.postgresql.PostgreSQLConfigurationExtension",
            "org.flywaydb.core.api.migration.baseline.BaselineMigrationConfigurationExtension");
    for (String extension : copied) {
      assertThat(Class.forName(extension).getSuperclass())
          .as("%s declares all of its fields itself", extension)
          .isEqualTo(Object.class);
      assertThat(registered.get(extension).path("allDeclaredFields").asBoolean())
          .as("%s is registered with its declared fields", extension)
          .isTrue();
    }
    for (Map.Entry<String, JsonNode> entry : registered.entrySet()) {
      if (entry.getValue().path("allDeclaredFields").asBoolean()) {
        assertThat(copied).as("only a copied extension needs its fields").contains(entry.getKey());
      }
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
  void registersNothingButFlywaysPluginsAndLogCreator() throws Exception {
    Set<String> expected = new LinkedHashSet<>(pluginsOnTheClassPath());
    expected.add(LOG_CREATOR);

    assertThat(reflectionEntries().keySet()).containsExactlyInAnyOrderElementsOf(expected);
    for (JsonNode entry : metadata().path("reflection")) {
      assertThat(entry.path("condition").isMissingNode())
          .as("Flyway runs at startup, before a condition type is reached: %s", entry)
          .isTrue();
    }
  }

  /**
   * Quarkus initializes every class at image build time, and {@code InsertRowLock} keeps a {@link
   * Random} in a static field. Native-image refuses a {@code Random} in the image heap, so the
   * build fails unless the class is initialized at run time.
   */
  @Test
  void initializesFlywaysInsertRowLockAtRunTime() throws Exception {
    Properties properties = new Properties();
    try (InputStream in = getClass().getResourceAsStream(DIRECTORY + "native-image.properties")) {
      assertThat(in).as("native-image.properties").isNotNull();
      properties.load(in);
    }

    assertThat(properties.getProperty("Args"))
        .isEqualTo("--initialize-at-run-time=" + INSERT_ROW_LOCK);
    assertThat(Class.forName(INSERT_ROW_LOCK).getDeclaredFields())
        .as("%s keeps a static Random", INSERT_ROW_LOCK)
        .anyMatch(FlywayNativeImageMetadataTest::isStaticRandom);
  }

  private static boolean isStaticRandom(Field field) {
    return Modifier.isStatic(field.getModifiers()) && field.getType() == Random.class;
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
    String resource = DIRECTORY + "reachability-metadata.json";
    try (InputStream in = getClass().getResourceAsStream(resource)) {
      assertThat(in).as(resource).isNotNull();
      return new ObjectMapper().readTree(in);
    }
  }
}
