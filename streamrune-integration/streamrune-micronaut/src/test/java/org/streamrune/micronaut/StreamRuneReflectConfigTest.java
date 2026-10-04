package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.streamrune.test.NativeReflectionTypes;

/**
 * Native-image parity check for the Micronaut {@code reflect-config.json}. Micronaut ships the
 * reflection metadata as a resource file (there is no Java registrar to unit-test), so this test
 * parses that file and DERIVES the expected set from code.
 */
class StreamRuneReflectConfigTest {

  private static final String REFLECT_CONFIG =
      "/META-INF/native-image/org.streamrune/streamrune-micronaut/reflect-config.json";

  private static final Pattern NAME_ENTRY = Pattern.compile("\"name\"\\s*:\\s*\"([^\"]+)\"");

  @Test
  void reflectConfigResourceExists() {
    assertThat(getClass().getResource(REFLECT_CONFIG)).as(REFLECT_CONFIG).isNotNull();
  }

  /**
   * Completeness parity check DERIVED FROM CODE: every {@code @JsonValue} value type in {@code
   * org.streamrune.core.types} plus every framework record/enum reachable through the registered
   * DTOs' components must be listed in {@code reflect-config.json}. This fails the moment a new
   * value type or nested component is left unregistered — exactly how {@code SubjectId}/{@code
   * ProjectionName}/etc. shipped silently broken in native images.
   */
  @Test
  void everyCodeDerivedSerializableTypeIsRegisteredForReflection() {
    Set<Class<?>> registeredRoots = registeredTypes();

    Set<Class<?>> expected = NativeReflectionTypes.expectedNativeReflectionTypes(registeredRoots);

    List<Class<?>> missing =
        expected.stream()
            .filter(type -> !registeredRoots.contains(type))
            .sorted(Comparator.comparing(Class::getName))
            .toList();

    assertThat(missing)
        .as(
            "framework types reachable/derived from code but NOT registered in reflect-config.json"
                + " for native-image reflection")
        .isEmpty();
  }

  /**
   * {@link org.streamrune.core.projection.Projection#writesThroughRepository()} resolves {@code
   * process(List, ProjectionRepository)} with {@code getClass().getMethod(...)}; the method is
   * declared by {@code Projection} (the default) or by {@code BaseProjection} (final). A native
   * image finds it only on a type whose methods are registered for reflection — measured on GraalVM
   * CE 25.0.1: without these entries the demo's Micronaut binary refused to start ({@code
   * NoSuchMethodException: ...DiscoverableProductProjection.process(java.util.List,
   * ...ProjectionRepository)} while the lifecycle built the projection runner).
   */
  @Test
  void registersTheProjectionTypesWhoseProcessMethodIsResolvedReflectively() throws IOException {
    Map<String, JsonNode> byName = new LinkedHashMap<>();
    for (JsonNode entry : new ObjectMapper().readTree(readReflectConfig())) {
      byName.put(entry.path("name").asText(), entry);
    }

    for (Class<?> type :
        List.of(
            org.streamrune.core.projection.Projection.class,
            org.streamrune.core.projection.BaseProjection.class)) {
      assertThat(byName)
          .as("reflect-config.json entry for %s", type.getName())
          .containsKey(type.getName());
      assertThat(byName.get(type.getName()).path("allDeclaredMethods").asBoolean())
          .as("%s declared methods registered", type.getName())
          .isTrue();
    }
  }

  /** Parses the {@code "name"} entries of reflect-config.json into loaded classes. */
  private Set<Class<?>> registeredTypes() {
    var types = new LinkedHashSet<Class<?>>();
    Matcher matcher = NAME_ENTRY.matcher(readReflectConfig());
    while (matcher.find()) {
      String className = matcher.group(1);
      try {
        types.add(Class.forName(className, false, getClass().getClassLoader()));
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException(
            "reflect-config.json references unknown class: " + className, e);
      }
    }
    return types;
  }

  private String readReflectConfig() {
    try (InputStream in = getClass().getResourceAsStream(REFLECT_CONFIG)) {
      if (in == null) {
        throw new IllegalStateException("Missing resource " + REFLECT_CONFIG);
      }
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
