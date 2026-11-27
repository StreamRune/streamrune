package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
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
   * {@code streamrune.backend=sqlite}: {@code BackendSelection} creates the SQLite backend and its
   * crypto half by class name through their public no-argument constructors, registered by name
   * (this module has no SQLite dependency); the instance guard's foreign downcalls, which the
   * legacy files cannot express, are in the module's reachability metadata.
   */
  @Test
  void registersTheSqliteBackendConstructorsAndTheGuardsForeignDowncalls() throws Exception {
    var mapper = new com.fasterxml.jackson.databind.ObjectMapper();
    List<String> constructors = new java.util.ArrayList<>();
    for (var entry : mapper.readTree(readReflectConfig())) {
      String name = entry.path("name").asText();
      if (name.startsWith("org.streamrune.sqlite.") || name.startsWith("org.streamrune.crypto.")) {
        assertThat(entry.path("methods").toString())
            .as("constructor only: %s", entry)
            .isEqualTo("[{\"name\":\"<init>\",\"parameterTypes\":[]}]");
        assertThat(Class.forName(name).getConstructor()).isNotNull();
        constructors.add(name);
      }
    }
    assertThat(constructors)
        .containsExactly(
            org.streamrune.integration.BackendSelection.SQLITE_BACKEND_CLASS,
            org.streamrune.integration.BackendSelection.SQLITE_CRYPTO_BACKEND_CLASS);

    String foreign =
        "/META-INF/native-image/org.streamrune/streamrune-micronaut-sqlite/reachability-metadata.json";
    try (InputStream in = getClass().getResourceAsStream(foreign)) {
      assertThat(in).as(foreign).isNotNull();
      assertThat(SqliteForeignDowncalls.of(mapper.readTree(in)))
          .isEqualTo(SqliteForeignDowncalls.EXPECTED);
    }
  }

  /** Parses the top-level {@code "name"} entries of reflect-config.json into loaded classes. */
  private Set<Class<?>> registeredTypes() {
    var types = new LinkedHashSet<Class<?>>();
    Matcher matcher = NAME_ENTRY.matcher(readReflectConfig());
    while (matcher.find()) {
      String className = matcher.group(1);
      if (className.equals("<init>")) {
        continue;
      }
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
