package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ShippedMigrationScriptsTest {

  @TempDir Path temp;

  @Test
  void listsTheScriptsOfBothSeriesFromDirectories() throws IOException {
    Path root = temp.resolve("classes");
    write(root, "db/streamrune-migration/V001__baseline.sql");
    write(root, "db/streamrune-migration/V002__more.sql");
    write(root, "db/streamrune-migration/README.txt");
    write(root, "db/streamrune-migration/nested/V9__ignored.sql");
    write(root, "db/crypto-migration/V001__crypto.sql");

    try (var loader = new URLClassLoader(new URL[] {root.toUri().toURL()}, null)) {
      assertEquals(
          List.of(
              "db/crypto-migration/V001__crypto.sql",
              "db/streamrune-migration/V001__baseline.sql",
              "db/streamrune-migration/V002__more.sql"),
          ShippedMigrationScripts.onClassPath(loader));
    }
  }

  @Test
  void listsTheScriptsOfBothSeriesFromJars() throws IOException {
    Path jar = temp.resolve("series.jar");
    try (var out = new JarOutputStream(Files.newOutputStream(jar))) {
      for (String name :
          List.of(
              "db/",
              "db/streamrune-migration/",
              "db/streamrune-migration/V001__baseline.sql",
              "db/streamrune-migration/nested/V9__ignored.sql",
              "db/crypto-migration/",
              "db/crypto-migration/V001__crypto.sql",
              "db/crypto-migration/notes.md")) {
        out.putNextEntry(new JarEntry(name));
        out.closeEntry();
      }
    }

    try (var loader = new URLClassLoader(new URL[] {jar.toUri().toURL()}, null)) {
      assertEquals(
          List.of(
              "db/crypto-migration/V001__crypto.sql", "db/streamrune-migration/V001__baseline.sql"),
          ShippedMigrationScripts.onClassPath(loader));
    }
  }

  @Test
  void refusesASeriesThatIsNotOnTheClassPath() throws IOException {
    Path root = temp.resolve("only-event-store");
    write(root, "db/streamrune-migration/V001__baseline.sql");

    try (var loader = new URLClassLoader(new URL[] {root.toUri().toURL()}, null)) {
      var refused =
          assertThrows(
              IllegalStateException.class, () -> ShippedMigrationScripts.onClassPath(loader));
      assertTrue(refused.getMessage().contains("db/crypto-migration"), refused.getMessage());
    }
  }

  @Test
  void refusesALocationItCannotList() {
    ClassLoader resourceProtocol =
        new ClassLoader(null) {
          @Override
          public Enumeration<URL> getResources(String name) throws IOException {
            URLStreamHandler handler =
                new URLStreamHandler() {
                  @Override
                  protected URLConnection openConnection(URL url) {
                    throw new UnsupportedOperationException();
                  }
                };
            return Collections.enumeration(
                List.of(URL.of(URI.create("resource:/" + name), handler)));
          }
        };

    var refused =
        assertThrows(
            IllegalStateException.class,
            () -> ShippedMigrationScripts.onClassPath(resourceProtocol));
    assertTrue(
        refused.getMessage().contains("unsupported protocol resource"), refused.getMessage());
  }

  @Test
  void reportsAClassPathThatCannotBeRead() {
    ClassLoader failing =
        new ClassLoader(null) {
          @Override
          public Enumeration<URL> getResources(String name) throws IOException {
            throw new IOException("class path unreadable");
          }
        };

    var refused =
        assertThrows(
            java.io.UncheckedIOException.class, () -> ShippedMigrationScripts.onClassPath(failing));
    assertTrue(refused.getMessage().contains("class path unreadable"), refused.getMessage());
  }

  private static void write(Path root, String name) throws IOException {
    Path file = root.resolve(name);
    Files.createDirectories(file.getParent());
    try (OutputStream out = Files.newOutputStream(file)) {
      out.write("SELECT 1;".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
  }
}
