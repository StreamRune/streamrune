package org.streamrune.core.tracing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Holds the span table of the "Tracing" section of {@code docs/guide/production.md} to {@link
 * TraceAttributes}: the span names and the attributes the table names are exactly the ones the
 * class defines, so the published contract and the constants the emitters use cannot drift apart.
 */
class TraceAttributesDocLintTest {

  private static final Pattern CODE_SPAN = Pattern.compile("`([^`]+)`");

  /** Stands for a helper's argument; it becomes {@code {}} in the shape compared. */
  private static final String PLACEHOLDER = "placeholderzz";

  /** The rows of the span table under {@code ### Tracing}, each split into its cells. */
  private static List<String[]> tableRows() {
    List<String> lines = read("docs/guide/production.md");
    int section = lines.indexOf("### Tracing");
    assertFalse(section < 0, "heading not found: ### Tracing");
    for (int i = section; i < lines.size(); i++) {
      if (lines.get(i).startsWith("| Emitter | Span name |")) {
        List<String[]> rows = new ArrayList<>();
        for (int row = i + 2; row < lines.size() && lines.get(row).startsWith("|"); row++) {
          rows.add(lines.get(row).split("\\|"));
        }
        return rows;
      }
    }
    return fail("no table whose header starts with '| Emitter | Span name |' under ### Tracing");
  }

  private static Set<String> codeSpans(String cell) {
    Set<String> spans = new TreeSet<>();
    Matcher span = CODE_SPAN.matcher(cell);
    while (span.find()) {
      spans.add(span.group(1));
    }
    return spans;
  }

  @Test
  void theTracingTableNamesExactlyTheAttributeKeys() {
    Set<String> documented = new TreeSet<>();
    for (String[] cells : tableRows()) {
      documented.addAll(codeSpans(cells[cells.length - 1]));
    }
    assertSameSets("attribute keys", attributeKeys(), documented);
  }

  /**
   * The span-name column holds every span name {@link TraceAttributes} defines: each {@code String}
   * constant that holds a {@code /}, and the shape of each public static {@code ...SpanName}
   * helper, called with a placeholder for every argument. A placeholder in the table ({@code
   * {CommandType}}, {@code {name}}) and in a helper's result compare as {@code {}}.
   */
  @Test
  void theTracingTableNamesExactlyTheSpanNames() throws ReflectiveOperationException {
    Set<String> documented = new TreeSet<>();
    for (String[] cells : tableRows()) {
      for (String name : codeSpans(cells[2])) {
        documented.add(name.replaceAll("\\{[^}]*}", "{}"));
      }
    }
    assertSameSets("span names", spanNames(), documented);
  }

  static Set<String> spanNames() throws ReflectiveOperationException {
    Set<String> names = new TreeSet<>();
    for (String constant : stringConstants()) {
      if (constant.contains("/")) {
        names.add(constant);
      }
    }
    for (Method method : TraceAttributes.class.getMethods()) {
      if (Modifier.isStatic(method.getModifiers())
          && method.getReturnType() == String.class
          && method.getName().endsWith("SpanName")) {
        Object[] args = new Object[method.getParameterCount()];
        for (int i = 0; i < args.length; i++) {
          Class<?> type = method.getParameterTypes()[i];
          args[i] =
              type == String.class
                  ? PLACEHOLDER
                  : type.getMethod("of", String.class).invoke(null, PLACEHOLDER);
        }
        names.add(((String) method.invoke(null, args)).replace(PLACEHOLDER, "{}"));
      }
    }
    assertFalse(names.isEmpty(), "the span names of TraceAttributes must be found");
    return names;
  }

  static Set<String> attributeKeys() {
    Set<String> keys = new TreeSet<>();
    for (String constant : stringConstants()) {
      if (!constant.contains("/")) {
        keys.add(constant);
      }
    }
    assertFalse(keys.isEmpty(), "the attribute keys of TraceAttributes must be found");
    return keys;
  }

  private static List<String> stringConstants() {
    List<String> constants = new ArrayList<>();
    for (Field field : TraceAttributes.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        try {
          constants.add((String) field.get(null));
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return constants;
  }

  @Test
  void everyAttributeKeyCarriesTheReservedPrefix() {
    for (String key : attributeKeys()) {
      if (!key.startsWith("streamrune.")) {
        fail("a TraceAttributes key without the streamrune. prefix: " + key);
      }
    }
  }

  private static void assertSameSets(String what, Set<String> defined, Set<String> documented) {
    if (!documented.equals(defined)) {
      Set<String> missing = new TreeSet<>(defined);
      missing.removeAll(documented);
      Set<String> unknown = new TreeSet<>(documented);
      unknown.removeAll(defined);
      fail(
          "the span table of docs/guide/production.md and TraceAttributes differ in their "
              + what
              + "; missing from the table: "
              + missing
              + ", not defined by TraceAttributes: "
              + unknown);
    }
  }

  private static List<String> read(String relative) {
    try {
      return Files.readAllLines(root().resolve(relative));
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
}
