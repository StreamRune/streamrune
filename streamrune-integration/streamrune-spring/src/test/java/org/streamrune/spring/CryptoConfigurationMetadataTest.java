package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * The module generates no configuration metadata of its own (no configuration processor), so every
 * crypto knob must be declared in {@code additional-spring-configuration-metadata.json} by hand.
 */
class CryptoConfigurationMetadataTest {

  @Test
  void everyCryptoRecordKeyAndBackendSwitchIsDeclared() throws Exception {
    String json;
    try (var in =
        getClass()
            .getClassLoader()
            .getResourceAsStream("META-INF/additional-spring-configuration-metadata.json")) {
      assertThat(in).as("additional metadata must be on the classpath").isNotNull();
      json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    List<String> expected = new ArrayList<>();
    collect(StreamRuneCryptoProperties.class, "streamrune.crypto", expected);
    for (String backend : List.of("filesystem", "postgres", "vault", "aws")) {
      expected.add("streamrune.crypto." + backend + ".enabled");
    }
    assertThat(expected).isNotEmpty();
    for (String key : expected) {
      assertThat(json).as("metadata declares %s", key).contains("\"" + key + "\"");
    }
  }

  private static void collect(Class<?> type, String prefix, List<String> keys) {
    keys.add(prefix);
    for (var component : type.getRecordComponents()) {
      String key =
          prefix + "." + component.getName().replaceAll("([A-Z])", "-$1").toLowerCase(Locale.ROOT);
      if (component.getType().isRecord()) {
        collect(component.getType(), key, keys);
      } else {
        keys.add(key);
      }
    }
  }
}
