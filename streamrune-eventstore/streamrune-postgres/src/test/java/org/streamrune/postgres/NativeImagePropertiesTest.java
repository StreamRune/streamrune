package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/**
 * The module's {@code native-image.properties}: Flyway builds https URLs while it constructs
 * itself, and a native image refuses that unless the https protocol is enabled, so schema
 * auto-initialization could not run in a native image without this build argument (measured on
 * GraalVM CE 25.0.1 with Flyway 13.9: {@code ExceptionInInitializerError} from Flyway's help-link
 * class, "Accessing a URL protocol that was not enabled").
 */
class NativeImagePropertiesTest {

  private static final String NATIVE_IMAGE_PROPERTIES =
      "META-INF/native-image/org.streamrune/streamrune-postgres/native-image.properties";

  @Test
  void enablesTheHttpsUrlProtocolFlywayNeedsToConstructItself() throws IOException {
    Properties properties = new Properties();
    try (InputStream in =
        PostgresEventStoreFactory.class
            .getClassLoader()
            .getResourceAsStream(NATIVE_IMAGE_PROPERTIES)) {
      assertNotNull(in, NATIVE_IMAGE_PROPERTIES);
      properties.load(in);
    }

    List<String> args = List.of(properties.getProperty("Args", "").trim().split("\\s+"));
    assertTrue(args.contains("--enable-url-protocols=https"), () -> "Args: " + args);
  }
}
