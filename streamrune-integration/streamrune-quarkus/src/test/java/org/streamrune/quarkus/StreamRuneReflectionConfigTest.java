package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.runtime.annotations.RegisterForReflection;
import java.io.InputStream;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.test.NativeReflectionTypes;
import org.streamrune.test.ShippedMigrationScripts;

class StreamRuneReflectionConfigTest {

  private static final String HIKARI_METADATA_RESOURCE =
      "/META-INF/native-image/org.streamrune/streamrune-quarkus/reachability-metadata.json";

  @Test
  void classHasRegisterForReflectionAnnotation() {
    assertNotNull(StreamRuneReflectionConfig.class.getAnnotation(RegisterForReflection.class));
  }

  @Test
  void annotationRegistersExpectedTargets() {
    var annotation = StreamRuneReflectionConfig.class.getAnnotation(RegisterForReflection.class);
    assertTrue(annotation.targets().length > 0);
  }

  /**
   * The framework builds small HikariCP pools of its own for its dedicated connections
   * (notification subscription and crypto forget signal LISTEN connections, the advisory locker's
   * opt-in dedicated pool) with {@code new HikariDataSource(config)}, which copies the
   * configuration by iterating {@code HikariConfig.class.getDeclaredFields()}. A native image
   * answers that with no fields for a class without reflection metadata, so the copy is silently
   * empty, the pool has no {@code DataSource}, and its first connection throws a {@code
   * NullPointerException} — measured on GraalVM CE 25.0.1: the demo's Quarkus binary refused to
   * start ({@code HikariPool$PoolInitializationException: Failed to initialize pool: null}). Its
   * shutdown then failed on the reflective {@code ConcurrentBag$IConcurrentBagEntry[]}. Spring Boot
   * ships HikariCP hints; Quarkus pools with Agroal and does not, so the integration ships them.
   */
  @Test
  void shipsTheHikariCpReachabilityMetadataTheFrameworkPoolsNeed() throws Exception {
    JsonNode metadata;
    try (InputStream in =
        StreamRuneReflectionConfig.class.getResourceAsStream(HIKARI_METADATA_RESOURCE)) {
      assertThat(in).as(HIKARI_METADATA_RESOURCE).isNotNull();
      metadata = new ObjectMapper().readTree(in);
    }
    Map<String, JsonNode> byType = new LinkedHashMap<>();
    for (JsonNode entry : metadata.path("reflection")) {
      byType.put(entry.path("type").toString(), entry);
    }

    assertThat(
            byType.get("\"com.zaxxer.hikari.HikariConfig\"").path("allDeclaredFields").asBoolean())
        .as("HikariConfig declared fields (copied by new HikariDataSource(config))")
        .isTrue();
    assertThat(byType)
        .containsKeys(
            "\"com.zaxxer.hikari.util.ConcurrentBag$IConcurrentBagEntry[]\"",
            "\"java.sql.Statement[]\"",
            "\"com.zaxxer.hikari.pool.PoolEntry\"",
            "\"com.zaxxer.hikari.pool.PoolBase\"",
            "{\"proxy\":[\"java.sql.Connection\"]}");
    for (JsonNode entry : byType.values()) {
      assertThat(entry.path("condition").path("typeReached").asText())
          .as("every entry applies only when HikariCP is reachable: %s", entry)
          .startsWith("com.zaxxer.hikari.");
    }
    assertThat(Class.forName("com.zaxxer.hikari.HikariConfig").getDeclaredFields())
        .extracting(java.lang.reflect.Field::getName)
        .contains("dataSource");
  }

  /**
   * Schema auto-initialization reads every shipped migration script by name, and Flyway reads its
   * own {@code version.txt} when it starts; a native image holds only the resources registered at
   * build time. Quarkus pools and migrates nothing of the framework's itself, so the integration
   * registers them in the same metadata file. The expected scripts are read from the class path, so
   * a script added to a series is checked without being listed here.
   */
  @Test
  void registersEveryShippedMigrationScriptAndFlywaysVersionFileAsResources() throws Exception {
    JsonNode metadata;
    try (InputStream in =
        StreamRuneReflectionConfig.class.getResourceAsStream(HIKARI_METADATA_RESOURCE)) {
      assertThat(in).as(HIKARI_METADATA_RESOURCE).isNotNull();
      metadata = new ObjectMapper().readTree(in);
    }
    List<java.nio.file.PathMatcher> globs = new java.util.ArrayList<>();
    for (JsonNode entry : metadata.path("resources")) {
      assertThat(entry.path("condition").isMissingNode())
          .as("a resource the factory reads before any type is reached is unconditional: %s", entry)
          .isTrue();
      globs.add(
          java.nio.file.FileSystems.getDefault()
              .getPathMatcher("glob:" + entry.path("glob").asText()));
    }
    ClassLoader classLoader = getClass().getClassLoader();
    List<String> expected =
        new java.util.ArrayList<>(ShippedMigrationScripts.onClassPath(classLoader));
    expected.add("org/flywaydb/core/internal/version.txt");

    for (String name : expected) {
      assertThat(classLoader.getResource(name)).as("%s is on the class path", name).isNotNull();
      assertThat(globs.stream().anyMatch(glob -> glob.matches(java.nio.file.Path.of(name))))
          .as("%s is registered as a native resource", name)
          .isTrue();
    }
    assertThat(expected)
        .contains(
            "db/streamrune-migration/V001__streamrune_baseline.sql",
            "db/crypto-migration/V001__crypto_baseline.sql");
  }

  /**
   * {@link org.streamrune.core.projection.Projection#writesThroughRepository()} resolves {@code
   * process(List, ProjectionRepository)} with {@code getClass().getMethod(...)}; the method is
   * declared by {@code Projection} (the default) or by {@code BaseProjection} (final). A native
   * image finds it only on a type registered for reflection — measured: without these registrations
   * the demo's Quarkus binary failed to build its transactional projection runner ({@code
   * NoSuchMethodException ... process(java.util.List, ...ProjectionRepository)}).
   */
  @Test
  void registersTheProjectionTypesWhoseProcessMethodIsResolvedReflectively() {
    var annotation = StreamRuneReflectionConfig.class.getAnnotation(RegisterForReflection.class);

    assertThat(List.of(annotation.targets()))
        .contains(
            org.streamrune.core.projection.Projection.class,
            org.streamrune.core.projection.BaseProjection.class);
    assertThat(annotation.methods()).isTrue();
  }

  /**
   * Completeness parity check DERIVED FROM CODE: every {@code @JsonValue} value type in {@code
   * org.streamrune.core.types} plus every framework record/enum reachable through the registered
   * DTOs' components must appear in {@code @RegisterForReflection}. Unlike an annotation-presence
   * assertion, this fails the moment a new value type or nested component is left unregistered —
   * exactly how {@code SubjectId}/{@code ProjectionName}/etc. shipped silently broken in native
   * images.
   */
  @Test
  void everyCodeDerivedSerializableTypeIsRegisteredForReflection() {
    var annotation = StreamRuneReflectionConfig.class.getAnnotation(RegisterForReflection.class);
    Set<Class<?>> registeredRoots = new LinkedHashSet<>(List.of(annotation.targets()));

    Set<Class<?>> expected = NativeReflectionTypes.expectedNativeReflectionTypes(registeredRoots);

    List<Class<?>> missing =
        expected.stream()
            .filter(type -> !registeredRoots.contains(type))
            .sorted(Comparator.comparing(Class::getName))
            .toList();

    assertThat(missing)
        .as(
            "framework types reachable/derived from code but NOT registered for native-image"
                + " reflection")
        .isEmpty();
  }
}
