package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;

/**
 * Holds the {@code Type} column of the meter tables to the kind of meter {@link
 * MicrometerStreamRuneMetrics} registers under each name: the {@code ### Metrics} table of {@code
 * docs/guide/production.md} and the meter table of this module's README. A dashboard reads a
 * counter as a rate and a gauge as a level, so a meter whose kind changes in code changes the table
 * in the same commit.
 *
 * <p>The kinds are read from a registry after every recording entry point of the metrics has been
 * called once, so the meters registered on first use are there too. A table cell is the kind's name
 * ({@code Counter}, {@code Timer}, {@code Gauge}), optionally followed by a note in parentheses.
 * {@link org.streamrune.core.metrics.MetricNames}' own doc-lint holds the names of the tables.
 *
 * <p>The documents are declared as inputs of this module's {@code docLintTest} task.
 */
class MeterTypeDocLintTest {

  private static final String GUIDE = "docs/guide/production.md";
  private static final String README =
      "streamrune-integration/streamrune-integration-api/README.md";

  @Test
  void theProductionGuideStatesTheTypeOfEveryMeter() {
    Map<String, String> rows = typeColumn(read(GUIDE), "### Metrics", "| Metric name | Type |");
    assertTrue(rows.size() > 50, "the ### Metrics table must be found: " + rows.keySet());
    assertNoProblems(GUIDE, wrongTypes(rows, registeredTypes()));
  }

  @Test
  void theReadmeStatesTheTypeOfEveryMeter() {
    Map<String, String> rows = typeColumn(read(README), null, "| Meter | Type |");
    assertTrue(rows.size() >= 10, "the README's meter table must be found: " + rows.keySet());
    assertNoProblems(README, wrongTypes(rows, registeredTypes()));
  }

  @Test
  void aTypeCellIsReadByItsFirstWord() {
    Map<String, String> registered = Map.of("streamrune.a", "Counter", "streamrune.b", "Gauge");

    assertEquals(
        List.of(),
        wrongTypes(
            new LinkedHashMap<>(
                Map.of("streamrune.a", "Counter", "streamrune.b", "Gauge (value 1)")),
            registered));
    assertEquals(
        List.of(
            "streamrune.a: the table says 'Timer', the code registers a Counter",
            "streamrune.c: no meter is registered under this name"),
        wrongTypes(
            new LinkedHashMap<>(Map.of("streamrune.a", "Timer", "streamrune.c", "Gauge")),
            registered));
  }

  /**
   * Every row of {@code rows} (meter name to {@code Type} cell) whose cell does not start with the
   * kind {@code registered} holds for the name, as {@code name: …}.
   */
  static List<String> wrongTypes(Map<String, String> rows, Map<String, String> registered) {
    List<String> problems = new ArrayList<>();
    new TreeMap<>(rows)
        .forEach(
            (name, cell) -> {
              String kind = registered.get(name);
              String stated = cell.strip().split("\\s+")[0];
              if (kind == null) {
                problems.add(name + ": no meter is registered under this name");
              } else if (!kind.equals(stated)) {
                problems.add(
                    name + ": the table says '" + stated + "', the code registers a " + kind);
              }
            });
    return problems;
  }

  /** The kind of every meter {@link MicrometerStreamRuneMetrics} registers, by meter name. */
  static Map<String, String> registeredTypes() {
    var registry = new SimpleMeterRegistry();
    var metrics = MicrometerStreamRuneMetrics.builder().registry(registry).build();
    MicrometerStreamRuneMetricsTest.driveEveryRecordingEntryPoint(metrics);
    Map<String, String> kinds = new TreeMap<>();
    for (Meter meter : registry.getMeters()) {
      String kind = kindOf(meter.getId().getType());
      kinds.merge(meter.getId().getName(), kind, (a, b) -> a.equals(b) ? a : a + " and " + b);
    }
    assertTrue(kinds.size() > 50, "the registered meters must be found: " + kinds.keySet());
    return kinds;
  }

  private static String kindOf(Meter.Type type) {
    return switch (type) {
      case COUNTER -> "Counter";
      case TIMER -> "Timer";
      case GAUGE -> "Gauge";
      case DISTRIBUTION_SUMMARY -> "DistributionSummary";
      case LONG_TASK_TIMER -> "LongTaskTimer";
      case OTHER -> "Meter";
    };
  }

  /**
   * The first two cells, backticks removed, of each row of the first table under the heading {@code
   * section} (or anywhere, for {@code null}) whose header starts with {@code headerStart}.
   */
  private static Map<String, String> typeColumn(
      List<String> lines, String section, String headerStart) {
    int start = section == null ? 0 : lines.indexOf(section);
    assertTrue(start >= 0, "heading not found: " + section);
    Map<String, String> rows = new LinkedHashMap<>();
    for (int i = start; i < lines.size(); i++) {
      if (!lines.get(i).startsWith(headerStart)) {
        continue;
      }
      for (int row = i + 2; row < lines.size() && lines.get(row).startsWith("|"); row++) {
        String[] cells = lines.get(row).split("\\|");
        rows.put(cells[1].replace("`", "").strip(), cells[2].replace("*", "").strip());
      }
      return rows;
    }
    fail("no table whose header starts with '" + headerStart + "' under " + section);
    return rows;
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

  private static void assertNoProblems(String document, List<String> problems) {
    if (!problems.isEmpty()) {
      fail(
          problems.size()
              + " meter(s) whose Type column in "
              + document
              + " is not the kind of meter the code registers:\n  "
              + String.join("\n  ", problems));
    }
  }
}
