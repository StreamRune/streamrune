package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.flywaydb.core.api.ResourceProvider;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.flywaydb.core.api.resource.LoadableResource;
import org.junit.jupiter.api.Test;

/** Tests for {@link ShippedMigrationSeries} that need no database. */
class ShippedMigrationSeriesTest {

  private static final String EVENT_STORE_SCRIPT = "V001__streamrune_baseline.sql";

  @Test
  void answersFlywaysResourceQueriesFromTheListedScripts() throws IOException {
    ResourceProvider provider =
        ShippedMigrationSeries.EVENT_STORE.configure(null).getResourceProvider();

    LoadableResource script = provider.getResource(EVENT_STORE_SCRIPT);
    assertNotNull(script);
    assertNull(provider.getResource(EVENT_STORE_SCRIPT + ".conf"), "no per-script configuration");
    assertEquals(EVENT_STORE_SCRIPT, script.getFilename());
    assertEquals(EVENT_STORE_SCRIPT, script.getRelativePath());
    assertEquals("db/streamrune-migration/" + EVENT_STORE_SCRIPT, script.getAbsolutePath());
    assertTrue(script.getAbsolutePathOnDisk().endsWith(EVENT_STORE_SCRIPT));

    assertEquals(List.of(script), provider.getResources("V", new String[] {".sql"}));
    assertEquals(List.of(script), provider.getResources("", new String[] {".txt", ".sql"}));
    assertEquals(List.of(), provider.getResources("R", new String[] {".sql"}), "no repeatables");
    assertEquals(List.of(), provider.getResources("V", new String[] {".txt"}));

    String onClassPath;
    try (InputStream in =
        getClass()
            .getClassLoader()
            .getResourceAsStream("db/streamrune-migration/" + EVENT_STORE_SCRIPT)) {
      onClassPath = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    assertEquals(onClassPath, readFully(script), "first read");
    assertEquals(onClassPath, readFully(script), "every read starts from the beginning");
  }

  @Test
  void scriptsAreEqualByPath() {
    LoadableResource first =
        ShippedMigrationSeries.EVENT_STORE
            .configure(null)
            .getResourceProvider()
            .getResource(EVENT_STORE_SCRIPT);
    LoadableResource second =
        ShippedMigrationSeries.EVENT_STORE
            .configure(null)
            .getResourceProvider()
            .getResource(EVENT_STORE_SCRIPT);
    LoadableResource crypto =
        ShippedMigrationSeries.CRYPTO
            .configure(null)
            .getResourceProvider()
            .getResource("V001__crypto_baseline.sql");

    assertEquals(first, second);
    assertEquals(first.hashCode(), second.hashCode());
    assertNotEquals(first, crypto);
    assertNotEquals(first, (Object) EVENT_STORE_SCRIPT);
  }

  /**
   * Flyway's default picks a logging back end by probing the class path, which in a native image
   * depends on what the image registers for reflection; the series names SLF4J instead. {@code
   * LogFactory} maps the name to the log creator the Quarkus and Micronaut integrations register.
   */
  @Test
  void tellsFlywayToLogThroughSlf4j() throws Exception {
    for (ShippedMigrationSeries series :
        List.of(ShippedMigrationSeries.EVENT_STORE, ShippedMigrationSeries.CRYPTO)) {
      assertArrayEquals(new String[] {"slf4j"}, series.configure(null).getLoggers());
    }
    assertTrue(
        org.flywaydb.core.api.logging.LogCreator.class.isAssignableFrom(
            Class.forName("org.flywaydb.core.internal.logging.slf4j.Slf4jLogCreator")));
  }

  @Test
  void declaresNoJavaMigrations() {
    FluentConfiguration configuration = ShippedMigrationSeries.EVENT_STORE.configure(null);

    assertEquals(
        List.of(), List.copyOf(configuration.getJavaMigrationClassProvider().getClasses()));
    assertEquals(
        List.of(ShippedMigrationSeries.EVENT_STORE.location()),
        java.util.Arrays.stream(configuration.getLocations())
            .map(location -> location.getDescriptor())
            .toList());
  }

  @Test
  void fallsBackToItsOwnClassLoaderWithoutAContextClassLoader() {
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(null);
    FluentConfiguration configuration;
    try {
      configuration = ShippedMigrationSeries.EVENT_STORE.configure(null);
    } finally {
      thread.setContextClassLoader(previous);
    }

    assertNotNull(configuration.getResourceProvider().getResource(EVENT_STORE_SCRIPT));
  }

  @Test
  void refusesAScriptThatCannotBeRead() {
    ClassLoader unreadable =
        new ClassLoader(getClass().getClassLoader()) {
          @Override
          public URL getResource(String name) {
            if (!name.startsWith("db/streamrune-migration/")) {
              return super.getResource(name);
            }
            URLStreamHandler failing =
                new URLStreamHandler() {
                  @Override
                  protected URLConnection openConnection(URL url) throws IOException {
                    throw new IOException("disk on fire");
                  }
                };
            try {
              return URL.of(URI.create("resource:/" + name), failing);
            } catch (java.net.MalformedURLException e) {
              throw new IllegalStateException(e);
            }
          }
        };
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    thread.setContextClassLoader(unreadable);
    try {
      var refused =
          assertThrows(
              IllegalStateException.class,
              () -> ShippedMigrationSeries.EVENT_STORE.configure(null));
      assertTrue(refused.getMessage().contains(EVENT_STORE_SCRIPT), refused.getMessage());
      assertSame(IOException.class, refused.getCause().getClass());
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  private static String readFully(LoadableResource resource) throws IOException {
    try (Reader reader = resource.read()) {
      var text = new StringBuilder();
      char[] buffer = new char[8192];
      for (int n; (n = reader.read(buffer)) != -1; ) {
        text.append(buffer, 0, n);
      }
      return text.toString();
    }
  }
}
