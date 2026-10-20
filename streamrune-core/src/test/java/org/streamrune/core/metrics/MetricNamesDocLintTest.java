package org.streamrune.core.metrics;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Holds the meter tables and the meter names of the guides to {@link MetricNames}, the contract
 * dashboards and alerts are written against:
 *
 * <ul>
 *   <li>the {@code ### Metrics} table of {@code docs/guide/production.md} names every meter of
 *       {@code MetricNames} exactly once, and nothing else;
 *   <li>the meter table of {@code streamrune-integration/streamrune-integration-api/README.md}
 *       names meters only;
 *   <li>every meter-shaped name in the guides is a meter: a backticked {@code streamrune.} name
 *       whose last segment has an underscore, a shape no configuration key has (keys are kebab
 *       case). A meter name without an underscore is held to the code by the integrations'
 *       configuration doc-lint, which knows the keys as well as the meters.
 * </ul>
 *
 * <p>The documents are declared as inputs of this module's {@code docLintTest} task.
 */
class MetricNamesDocLintTest {

  private static final Pattern BACKTICKED_NAME =
      Pattern.compile("`(streamrune(?:\\.[a-z0-9_]+)+)[^`]*`");

  @Test
  void theProductionGuideMetricsTableNamesEveryMeterOnce() {
    Set<String> meters = meters();
    List<String> rows =
        firstCells(read("docs/guide/production.md"), "### Metrics", "| Metric name |");
    List<String> problems = new ArrayList<>();
    Set<String> seen = new TreeSet<>();
    for (String row : rows) {
      if (!seen.add(row)) {
        problems.add("listed twice: " + row);
      }
      if (!meters.contains(row)) {
        problems.add("not a meter of MetricNames: " + row);
      }
    }
    for (String meter : meters) {
      if (!seen.contains(meter)) {
        problems.add("missing from the table: " + meter);
      }
    }
    assertTrue(meters.size() > 50, "the meters of MetricNames must be found: " + meters.size());
    assertNoProblems("problem(s) with the ### Metrics table of docs/guide/production.md", problems);
  }

  @Test
  void theIntegrationApiReadmeMeterTableNamesMetersOnly() {
    Set<String> meters = meters();
    List<String> rows =
        firstCells(
            read("streamrune-integration/streamrune-integration-api/README.md"), null, "| Meter |");
    assertTrue(rows.size() >= 10, "the README's meter table must be found: " + rows);
    List<String> problems = new ArrayList<>();
    for (String row : rows) {
      if (!meters.contains(row)) {
        problems.add("not a meter of MetricNames: " + row);
      }
    }
    assertNoProblems("problem(s) with the meter table of the integration-api README", problems);
  }

  @Test
  void everyMeterShapedNameInTheGuidesIsAMeter() throws IOException {
    Set<String> meters = meters();
    List<String> problems = new ArrayList<>();
    int checked = 0;
    try (Stream<Path> guides = Files.walk(root().resolve("docs/guide"))) {
      for (Path guide : guides.filter(p -> p.toString().endsWith(".md")).sorted().toList()) {
        List<String> lines = Files.readAllLines(guide, StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
          Matcher name = BACKTICKED_NAME.matcher(lines.get(i));
          while (name.find()) {
            String found = name.group(1);
            if (found.substring(found.lastIndexOf('.') + 1).contains("_")) {
              checked++;
              if (!meters.contains(found)) {
                problems.add(root().relativize(guide) + ":" + (i + 1) + ": " + found);
              }
            }
          }
        }
      }
    }
    assertTrue(checked > 50, "the meter names of the guides must be found: " + checked);
    assertNoProblems(
        "meter-shaped name(s) in the guides that MetricNames does not define", problems);
  }

  /** The {@code streamrune.} values of the public string constants of {@link MetricNames}. */
  static Set<String> meters() {
    Set<String> meters = new TreeSet<>();
    for (Field field : MetricNames.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        try {
          String value = (String) field.get(null);
          if (value.startsWith("streamrune.")) {
            meters.add(value);
          }
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return meters;
  }

  /**
   * The first cells, backticks removed, of the first table under the heading {@code section} (or
   * anywhere, for {@code null}) whose header starts with {@code headerStart}.
   */
  private static List<String> firstCells(List<String> lines, String section, String headerStart) {
    int start = section == null ? 0 : lines.indexOf(section);
    assertTrue(start >= 0, "heading not found: " + section);
    List<String> cells = new ArrayList<>();
    for (int i = start; i < lines.size(); i++) {
      if (!lines.get(i).startsWith(headerStart)) {
        continue;
      }
      for (int row = i + 2; row < lines.size() && lines.get(row).startsWith("|"); row++) {
        String first = lines.get(row).split("\\|")[1];
        cells.add(first.replace("`", "").strip());
      }
      return cells;
    }
    fail("no table whose header starts with '" + headerStart + "' under " + section);
    return cells;
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
