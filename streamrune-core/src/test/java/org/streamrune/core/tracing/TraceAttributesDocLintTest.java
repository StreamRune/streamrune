package org.streamrune.core.tracing;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Holds the span table of the "Tracing" section of {@code docs/guide/production.md} to {@link
 * TraceAttributes}: the attributes the table names are exactly the attribute keys the class
 * defines, so the published contract and the constants the emitters use cannot drift apart.
 */
class TraceAttributesDocLintTest {

  private static final Pattern CODE_SPAN = Pattern.compile("`([^`]+)`");

  @Test
  void theTracingTableNamesExactlyTheAttributeKeys() {
    Set<String> keys = attributeKeys();
    Set<String> documented = new TreeSet<>();
    List<String> lines = read("docs/guide/production.md");
    int section = lines.indexOf("### Tracing");
    assertFalse(section < 0, "heading not found: ### Tracing");
    int header = -1;
    for (int i = section; i < lines.size(); i++) {
      if (lines.get(i).startsWith("| Emitter |")) {
        header = i;
        break;
      }
    }
    assertFalse(header < 0, "no table whose header starts with '| Emitter |' under ### Tracing");
    for (int row = header + 2; row < lines.size() && lines.get(row).startsWith("|"); row++) {
      String[] cells = lines.get(row).split("\\|");
      Matcher name = CODE_SPAN.matcher(cells[cells.length - 1]);
      while (name.find()) {
        documented.add(name.group(1));
      }
    }
    assertFalse(keys.isEmpty(), "the attribute keys of TraceAttributes must be found");
    if (!documented.equals(keys)) {
      Set<String> missing = new TreeSet<>(keys);
      missing.removeAll(documented);
      Set<String> unknown = new TreeSet<>(documented);
      unknown.removeAll(keys);
      fail(
          "the span table of docs/guide/production.md and TraceAttributes differ; missing from the"
              + " table: "
              + missing
              + ", not a key of TraceAttributes: "
              + unknown);
    }
  }

  static Set<String> attributeKeys() {
    Set<String> keys = new TreeSet<>();
    for (Field field : TraceAttributes.class.getFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        try {
          keys.add((String) field.get(null));
        } catch (IllegalAccessException e) {
          throw new IllegalStateException(e);
        }
      }
    }
    return keys;
  }

  @Test
  void everyAttributeKeyCarriesTheReservedPrefix() {
    for (String key : attributeKeys()) {
      if (!key.startsWith("streamrune.")) {
        fail("a TraceAttributes key without the streamrune. prefix: " + key);
      }
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
