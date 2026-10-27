package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Every public type of the test kit (package {@code org.streamrune.test}, the package the module
 * exports) is named in the testing guide, {@code docs/guide/testing.md}, and in the module's
 * README: a type a user cannot find is a type they rewrite by hand. The types are the public
 * top-level types declared in this module's main sources; the guide, the README and the sources are
 * declared as inputs of this module's {@code docLintTest} task.
 *
 * <p>A type is named where it is named as itself, not in passing in a sentence about another type:
 * in the guide, by a heading or by a code span that is its name alone ({@code `SagaFixture`}); in
 * the README, by a heading or by such a code span in the first cell of a row of the type table, the
 * table whose header starts with {@value #README_TABLE}.
 */
class TestKitDocLintTest {

  private static final String GUIDE = "docs/guide/testing.md";
  private static final String README = "streamrune-test/README.md";
  private static final String README_TABLE = "| Fixture |";

  /** A markdown heading: one to six hashes and a space. */
  private static final Pattern HEADING = Pattern.compile("^#{1,6}\\s");

  @Test
  void theTestingGuideNamesEveryPublicType() throws Exception {
    List<String> lines = read(GUIDE);
    assertEveryTypeNamed(GUIDE, type -> namedInGuide(lines, type));
  }

  @Test
  void theReadmeNamesEveryPublicType() throws Exception {
    List<String> lines = read(README);
    assertEveryTypeNamed(README, type -> namedInReadme(lines, type));
  }

  @Test
  void aTypeIsNamedOnlyWhereItIsNamedAsItself() {
    List<String> guide =
        List.of(
            "## SagaFixture",
            "`EventStoreFixture` wraps an InMemoryEventStore.",
            "Advance it with `MutableClock.advance(...)`.");
    assertTrue(namedInGuide(guide, "SagaFixture"), "a heading names it");
    assertTrue(namedInGuide(guide, "EventStoreFixture"), "a code span of its name alone");
    assertFalse(namedInGuide(guide, "InMemoryEventStore"), "a word in another type's sentence");
    assertFalse(namedInGuide(guide, "MutableClock"), "a span that quotes a call");

    List<String> readme =
        List.of(
            "Provides `InMemorySagaStore` and more.",
            "",
            "## InMemoryEventStore",
            "",
            "| Fixture | Purpose |",
            "|---|---|",
            "| `DeciderFixture`, `ProjectionFixture` | Given/when/then, as `SagaFixture` does |",
            "",
            "| `FaultSchedule` | a table of another header |");
    assertTrue(namedInReadme(readme, "InMemoryEventStore"), "a heading names it");
    assertTrue(namedInReadme(readme, "DeciderFixture"), "the first cell of a row of the table");
    assertTrue(namedInReadme(readme, "ProjectionFixture"), "one of several in the first cell");
    assertFalse(namedInReadme(readme, "SagaFixture"), "only in another type's description");
    assertFalse(namedInReadme(readme, "InMemorySagaStore"), "only in the overview");
    assertFalse(namedInReadme(readme, "FaultSchedule"), "a row of another table");
  }

  /** {@code type} has a heading of its own, or a code span of its name alone, in {@code lines}. */
  static boolean namedInGuide(List<String> lines, String type) {
    return lines.stream().anyMatch(line -> heads(line, type) || line.contains("`" + type + "`"));
  }

  /**
   * {@code type} has a heading of its own in {@code lines}, or a code span of its name alone in the
   * first cell of a row of the type table.
   */
  static boolean namedInReadme(List<String> lines, String type) {
    if (lines.stream().anyMatch(line -> heads(line, type))) {
      return true;
    }
    for (int i = 0; i < lines.size(); i++) {
      if (!lines.get(i).startsWith(README_TABLE)) {
        continue;
      }
      for (int row = i + 2; row < lines.size() && lines.get(row).startsWith("|"); row++) {
        if (lines.get(row).split("\\|")[1].contains("`" + type + "`")) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean heads(String line, String type) {
    return HEADING.matcher(line).find()
        && Pattern.compile("\\b" + type + "\\b").matcher(line).find();
  }

  private static void assertEveryTypeNamed(
      String document, java.util.function.Predicate<String> named) throws Exception {
    List<String> types = publicTypes();
    assertTrue(types.size() > 25, "the public types of the kit must be found: " + types);
    List<String> missing = new ArrayList<>();
    for (String type : types) {
      if (!named.test(type)) {
        missing.add(type);
      }
    }
    if (!missing.isEmpty()) {
      fail(
          missing.size()
              + " public type(s) of org.streamrune.test not named as themselves in "
              + document
              + " (a heading, or "
              + (document.equals(README)
                  ? "a code span of the name in the first cell of the type table"
                  : "a code span of the name alone")
              + "): "
              + missing);
    }
  }

  /** The simple names of the public top-level types of the exported package, sorted. */
  private static List<String> publicTypes() throws IOException, ClassNotFoundException {
    List<String> types = new ArrayList<>();
    try (Stream<Path> sources = Files.list(Path.of("src/main/java/org/streamrune/test"))) {
      for (Path source : sources.sorted().toList()) {
        String file = source.getFileName().toString();
        if (!file.endsWith(".java") || file.equals("package-info.java")) {
          continue;
        }
        String name = file.substring(0, file.length() - ".java".length());
        Class<?> type =
            Class.forName(
                "org.streamrune.test." + name, false, TestKitDocLintTest.class.getClassLoader());
        if (Modifier.isPublic(type.getModifiers())) {
          types.add(name);
        }
      }
    }
    return types;
  }

  private static List<String> read(String relative) throws IOException {
    return Files.readAllLines(root().resolve(relative), StandardCharsets.UTF_8);
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
}
