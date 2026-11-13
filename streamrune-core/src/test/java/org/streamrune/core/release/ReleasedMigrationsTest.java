package org.streamrune.core.release;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * A Flyway migration a release has shipped never changes: a database that ran it has recorded its
 * checksum and Flyway refuses that database when the file differs. {@code
 * gradle/released-migrations.txt} lists the shipped migrations with their SHA-256; a listed file
 * must keep its checksum, and once the newest entry of {@code CHANGELOG.md} is dated (the commit
 * that is released), every migration of the repository must be listed. A migration added after a
 * release is free to change until the pull request that dates the next entry lists it.
 */
class ReleasedMigrationsTest {

  static final String MANIFEST = "gradle/released-migrations.txt";

  private static final Pattern NEWEST_ENTRY =
      Pattern.compile("(?m)^## \\[([0-9]+\\.[0-9]+\\.[0-9]+[^\\]]*)\\] (?:—|-) (.+)$");

  private static final Pattern LINE = Pattern.compile("([0-9a-f]{64})  (\\S+)");

  @Test
  void theRepositoryKeepsTheChecksumsOfItsReleasedMigrations() throws IOException {
    Path root = RepositoryScripts.repositoryRoot();
    List<String> problems =
        problems(
            RepositoryScripts.read("CHANGELOG.md"),
            RepositoryScripts.read(MANIFEST),
            migrations(root));
    assertThat(problems).isEmpty();
  }

  @Test
  void aChangedReleasedMigrationIsRefused() {
    String manifest = sha("create table a;") + "  db/V001__baseline.sql\n";
    assertThat(
            problems(undated(), manifest, Map.of("db/V001__baseline.sql", sha("create table a;"))))
        .isEmpty();
    assertThat(
            problems(undated(), manifest, Map.of("db/V001__baseline.sql", sha("create table b;"))))
        .singleElement()
        .asString()
        .contains("db/V001__baseline.sql")
        .contains("has changed");
    assertThat(problems(undated(), manifest, Map.of()))
        .singleElement()
        .asString()
        .contains("is gone");
  }

  @Test
  void anUnlistedMigrationIsFreeUntilItsReleaseIsDated() {
    Map<String, String> files = Map.of("db/V001__baseline.sql", sha("create table a;"));

    assertThat(problems(undated(), "# none yet\n", files)).isEmpty();
    assertThat(problems(dated(), "# none yet\n", files))
        .singleElement()
        .asString()
        .contains("db/V001__baseline.sql")
        .contains("1.0.0 is dated");
  }

  @Test
  void theChecksumIsThatOfTheFileWithLfLineEndings() {
    byte[] lf =
        "create table a (\n  id int\n);\n".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    byte[] crlf =
        "\uFEFFcreate table a (\r\n  id int\r\n);\r\n"
            .getBytes(java.nio.charset.StandardCharsets.UTF_8);

    assertThat(checksum(crlf)).isEqualTo(checksum(lf)).isEqualTo(sha(lf));
    assertThat(checksum(lf))
        .isNotEqualTo(
            checksum("create table b;\n".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
  }

  @Test
  void aMalformedManifestLineIsRefused() {
    assertThat(problems(undated(), "abc  db/V001__baseline.sql\n", Map.of()))
        .singleElement()
        .asString()
        .contains("line 1");
  }

  private static String undated() {
    return "# Changelog\n\n## [1.0.0] — unreleased\n\nThe first release.\n";
  }

  private static String dated() {
    return "# Changelog\n\n## [1.0.0] — 2026-12-01\n\nThe first release.\n";
  }

  /**
   * Why the migrations {@code files} (repository-relative path to SHA-256) disagree with {@code
   * manifest} under {@code changelog}.
   */
  static List<String> problems(String changelog, String manifest, Map<String, String> files) {
    List<String> problems = new ArrayList<>();
    Map<String, String> listed = new TreeMap<>();
    String[] lines = manifest.split("\n", -1);
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i].strip();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      Matcher m = LINE.matcher(line);
      if (!m.matches()) {
        problems.add(MANIFEST + " line " + (i + 1) + " is not `<sha256>  <path>`: " + line);
        continue;
      }
      listed.put(m.group(2), m.group(1));
    }
    listed.forEach(
        (path, sum) -> {
          String actual = files.get(path);
          if (actual == null) {
            problems.add("the released migration " + path + " is gone");
          } else if (!actual.equals(sum)) {
            problems.add(
                "the released migration "
                    + path
                    + " has changed; a released migration never changes, add a new one instead");
          }
        });
    Matcher entry = NEWEST_ENTRY.matcher(changelog);
    if (entry.find() && !entry.group(2).strip().equals("unreleased")) {
      files.keySet().stream()
          .filter(path -> !listed.containsKey(path))
          .sorted()
          .forEach(
              path ->
                  problems.add(
                      "the CHANGELOG entry of "
                          + entry.group(1)
                          + " is dated, so "
                          + path
                          + " ships with it: list it in "
                          + MANIFEST
                          + " (shasum -a 256 "
                          + path
                          + ")"));
    }
    return problems;
  }

  /** Every Flyway migration under a {@code src/main/resources/db} tree, with its SHA-256. */
  static Map<String, String> migrations(Path root) throws IOException {
    Map<String, String> files = new TreeMap<>();
    try (Stream<Path> walk = Files.walk(root)) {
      for (Path file :
          walk.filter(Files::isRegularFile)
              .filter(p -> p.getFileName().toString().matches("V[0-9]+__.*\\.sql"))
              .filter(
                  p -> {
                    String path = root.relativize(p).toString().replace('\\', '/');
                    return path.contains("/src/main/resources/db/") && !path.contains("/build/");
                  })
              .toList()) {
        files.put(
            root.relativize(file).toString().replace('\\', '/'),
            checksum(Files.readAllBytes(file)));
      }
    } catch (UncheckedIOException e) {
      throw e.getCause();
    }
    assertThat(files).as("the repository's Flyway migrations").isNotEmpty();
    return files;
  }

  /**
   * The SHA-256 of a migration with LF line endings and no byte-order mark: a checkout with CRLF
   * line endings (Windows, {@code core.autocrlf}) has the checksum of the file in the repository,
   * as Flyway's own checksum, which reads the file line by line, does. On a checkout with LF line
   * endings it is what {@code shasum -a 256} prints.
   */
  static String checksum(byte[] file) {
    String text = new String(file, java.nio.charset.StandardCharsets.UTF_8);
    if (text.startsWith("\uFEFF")) {
      text = text.substring(1);
    }
    return sha(text.replace("\r\n", "\n"));
  }

  private static String sha(String text) {
    return sha(text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  private static String sha(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }
}
