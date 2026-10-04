package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.streamrune.test.ShippedMigrationScripts;

/**
 * Native-image check for the Micronaut {@code resource-config.json}: schema auto-initialization
 * reads every shipped migration script by name, and Flyway reads its own {@code version.txt} when
 * it starts, while a native image holds only the resources registered at build time. The expected
 * scripts are read from the class path, so a script added to a series is checked without being
 * listed here.
 */
class StreamRuneResourceConfigTest {

  private static final String RESOURCE_CONFIG =
      "/META-INF/native-image/org.streamrune/streamrune-micronaut/resource-config.json";

  @Test
  void registersEveryShippedMigrationScriptAndFlywaysVersionFileAsResources() throws Exception {
    JsonNode config;
    try (InputStream in = getClass().getResourceAsStream(RESOURCE_CONFIG)) {
      assertThat(in).as(RESOURCE_CONFIG).isNotNull();
      config = new ObjectMapper().readTree(in);
    }
    List<Pattern> includes = new ArrayList<>();
    for (JsonNode include : config.path("resources").path("includes")) {
      includes.add(Pattern.compile(include.path("pattern").asText()));
    }
    ClassLoader classLoader = getClass().getClassLoader();
    List<String> expected = new ArrayList<>(ShippedMigrationScripts.onClassPath(classLoader));
    expected.add("org/flywaydb/core/internal/version.txt");

    for (String name : expected) {
      assertThat(classLoader.getResource(name)).as("%s is on the class path", name).isNotNull();
      assertThat(includes.stream().anyMatch(include -> include.matcher(name).matches()))
          .as("%s is registered as a native resource", name)
          .isTrue();
    }
    assertThat(expected)
        .contains(
            "db/streamrune-migration/V001__streamrune_baseline.sql",
            "db/crypto-migration/V001__crypto_baseline.sql");
    assertThat(
            includes.stream()
                .anyMatch(include -> include.matcher("db/migration/V1__app.sql").matches()))
        .as("the application's own migrations are the application's to register")
        .isFalse();
  }
}
