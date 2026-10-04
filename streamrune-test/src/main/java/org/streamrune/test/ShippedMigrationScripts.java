package org.streamrune.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

/**
 * Lists, from the class path, every Flyway migration script StreamRune ships, so the Spring,
 * Quarkus and Micronaut native-image parity tests can check that each one is registered as a native
 * image resource. Schema auto-initialization reads every script by name, and a native image holds
 * only the resources registered at build time.
 *
 * <p>The list is read from the class path ({@code file:} directories and {@code jar:} archives),
 * not written down, so a script added to a series is expected to be registered without anyone
 * remembering to list it here.
 */
public final class ShippedMigrationScripts {

  /** The class-path directories of the two shipped migration series. */
  public static final List<String> DIRECTORIES =
      List.of("db/streamrune-migration", "db/crypto-migration");

  private ShippedMigrationScripts() {}

  /**
   * Every {@code .sql} script directly under each of {@link #DIRECTORIES}, as class-path resource
   * names (for example {@code db/streamrune-migration/V001__streamrune_baseline.sql}), sorted.
   *
   * @param classLoader the class loader whose class path is read
   * @return the resource name of every shipped script
   * @throws IllegalStateException if a series directory is not on the class path or holds no script
   */
  public static List<String> onClassPath(ClassLoader classLoader) {
    var scripts = new TreeSet<String>();
    for (String directory : DIRECTORIES) {
      var found = new TreeSet<String>();
      try {
        for (URL url : Collections.list(classLoader.getResources(directory))) {
          for (String fileName : fileNames(url, directory)) {
            if (fileName.endsWith(".sql")) {
              found.add(directory + "/" + fileName);
            }
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      if (found.isEmpty()) {
        throw new IllegalStateException(
            "no migration script found under " + directory + " on the class path");
      }
      scripts.addAll(found);
    }
    return List.copyOf(scripts);
  }

  private static List<String> fileNames(URL url, String directory) throws IOException {
    return switch (url.getProtocol()) {
      case "file" -> {
        try (Stream<Path> files = Files.list(Path.of(url.toURI()))) {
          yield files.filter(Files::isRegularFile).map(p -> p.getFileName().toString()).toList();
        } catch (URISyntaxException e) {
          throw new IllegalStateException(e);
        }
      }
      case "jar" -> {
        var names = new ArrayList<String>();
        var connection = (JarURLConnection) url.openConnection();
        connection.setUseCaches(false);
        try (JarFile jar = connection.getJarFile()) {
          String prefix = directory + "/";
          jar.stream()
              .map(JarEntry::getName)
              .filter(name -> name.startsWith(prefix) && name.indexOf('/', prefix.length()) < 0)
              .filter(name -> name.length() > prefix.length())
              .forEach(name -> names.add(name.substring(prefix.length())));
        }
        yield names;
      }
      default ->
          throw new IllegalStateException(
              "cannot list " + url + ": unsupported protocol " + url.getProtocol());
    };
  }
}
