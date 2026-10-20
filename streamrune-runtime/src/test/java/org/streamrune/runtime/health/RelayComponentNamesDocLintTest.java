package org.streamrune.runtime.health;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Holds the health tables of {@code docs/guide/production.md} to the code:
 *
 * <ul>
 *   <li>the {@code | Relay component | Present when |} table lists every name of {@link
 *       RelayComponentNames} once and nothing else, the two per-saga names written with {@code
 *       <saga type>};
 *   <li>the {@code | Framework | Endpoint | Check name | Class |} table states the check name each
 *       integration gives its health check. This module cannot depend on the integrations, so the
 *       names are read from their sources: the name Quarkus's {@code StreamRuneHealthCheck} and
 *       Micronaut's {@code StreamRuneHealthIndicator} build their response with, and for Spring the
 *       bean name of the auto-configured indicator without its {@code HealthIndicator} suffix,
 *       which is how Spring Boot names a health contributor.
 * </ul>
 *
 * <p>The guide and the three sources are declared as inputs of this module's {@code docLintTest}
 * task.
 */
class RelayComponentNamesDocLintTest {

  private static final String SAGA_TYPE = "<saga type>";

  private static final Pattern BACKTICKED = Pattern.compile("`([^`]+)`");

  @Test
  void theRelayComponentTableListsEveryNameOnce() {
    Set<String> names = componentNames();
    List<String> listed = new ArrayList<>();
    for (List<String> row : rows("| Relay component | Present when |")) {
      Matcher name = BACKTICKED.matcher(row.get(0));
      while (name.find()) {
        listed.add(name.group(1));
      }
    }
    List<String> problems = new ArrayList<>();
    Set<String> seen = new TreeSet<>();
    for (String name : listed) {
      if (!seen.add(name)) {
        problems.add("listed twice: " + name);
      }
      if (!names.contains(name)) {
        problems.add("not a name of RelayComponentNames: " + name);
      }
    }
    for (String name : names) {
      if (!seen.contains(name)) {
        problems.add("missing from the table: " + name);
      }
    }
    assertEquals(10, names.size(), "the names of RelayComponentNames: " + names);
    assertNoProblems("problem(s) with the Relay component table of production.md", problems);
  }

  @Test
  void theCheckNameTableStatesTheNameEachIntegrationGivesItsCheck() {
    Map<String, String> documented = new LinkedHashMap<>();
    for (List<String> row : rows("| Framework | Endpoint | Check name | Class |")) {
      documented.put(row.get(0), row.get(2).replace("`", ""));
    }
    Map<String, String> fromCode = new LinkedHashMap<>();
    fromCode.put(
        "Spring Boot Actuator",
        single(
            "streamrune-integration/streamrune-spring/src/main/java/org/streamrune/spring/"
                + "StreamRuneAutoConfiguration.java",
            Pattern.compile("public StreamRuneHealthIndicator (\\w+)HealthIndicator\\(")));
    fromCode.put(
        "Quarkus MicroProfile Health",
        single(
            "streamrune-integration/streamrune-quarkus/src/main/java/org/streamrune/quarkus/"
                + "StreamRuneHealthCheck.java",
            Pattern.compile("HealthCheckResponse\\.named\\(\"([^\"]+)\"\\)")));
    fromCode.put(
        "Micronaut Management",
        single(
            "streamrune-integration/streamrune-micronaut/src/main/java/org/streamrune/micronaut/"
                + "StreamRuneHealthIndicator.java",
            Pattern.compile("HealthResult\\.builder\\(\"([^\"]+)\"\\)")));
    assertEquals(fromCode, documented);
  }

  /**
   * The names of {@link RelayComponentNames}: every public string constant that is a name (not a
   * prefix or a suffix), and the two per-saga names for {@code <saga type>}.
   */
  private static Set<String> componentNames() {
    Set<String> names = new TreeSet<>();
    for (Field field : RelayComponentNames.class.getFields()) {
      boolean part = field.getName().endsWith("_PREFIX") || field.getName().endsWith("_SUFFIX");
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class && !part) {
        try {
          names.add((String) field.get(null));
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    names.add(RelayComponentNames.sagaCompensationRetry(SAGA_TYPE));
    names.add(RelayComponentNames.sagaTimeout(SAGA_TYPE));
    return names;
  }

  /** The cells of the data rows of the production guide's table whose header is {@code header}. */
  private static List<List<String>> rows(String header) {
    List<String> lines = read("docs/guide/production.md");
    int start = lines.indexOf(header);
    assertTrue(start >= 0, "no table with the header " + header);
    List<List<String>> rows = new ArrayList<>();
    for (int i = start + 2; i < lines.size() && lines.get(i).startsWith("|"); i++) {
      List<String> cells = new ArrayList<>();
      String row = lines.get(i);
      for (String cell : row.substring(1, row.length() - 1).split("\\|", -1)) {
        cells.add(cell.strip());
      }
      rows.add(cells);
    }
    assertTrue(!rows.isEmpty(), "the table " + header + " has no rows");
    return rows;
  }

  /** The one value {@code pattern} captures in a source file; two different values fail. */
  private static String single(String relative, Pattern pattern) {
    Set<String> values = new TreeSet<>();
    Matcher matcher = pattern.matcher(String.join("\n", read(relative)));
    while (matcher.find()) {
      values.add(matcher.group(1));
    }
    assertEquals(1, values.size(), relative + " must give its check one name: " + values);
    return values.iterator().next();
  }

  private static List<String> read(String relative) {
    try {
      return Files.readAllLines(root().resolve(relative), StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new IllegalStateException("cannot read " + relative, e);
    }
  }

  private static Path root() {
    Path dir = Path.of("").toAbsolutePath();
    while (dir != null && !Files.exists(dir.resolve("settings.gradle.kts"))) {
      dir = dir.getParent();
    }
    if (dir == null) {
      throw new IllegalStateException(
          "no settings.gradle.kts above " + Path.of("").toAbsolutePath());
    }
    return dir;
  }

  private static void assertNoProblems(String what, List<String> problems) {
    if (!problems.isEmpty()) {
      fail(problems.size() + " " + what + ":\n  " + String.join("\n  ", problems));
    }
  }
}
