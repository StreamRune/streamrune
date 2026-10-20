package org.streamrune.test;

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
 */
class TestKitDocLintTest {

  private static final String GUIDE = "docs/guide/testing.md";
  private static final String README = "streamrune-test/README.md";

  @Test
  void theTestingGuideNamesEveryPublicType() throws Exception {
    assertEveryTypeNamedIn(GUIDE);
  }

  @Test
  void theReadmeNamesEveryPublicType() throws Exception {
    assertEveryTypeNamedIn(README);
  }

  private static void assertEveryTypeNamedIn(String document) throws Exception {
    String text = Files.readString(root().resolve(document), StandardCharsets.UTF_8);
    List<String> types = publicTypes();
    assertTrue(types.size() > 25, "the public types of the kit must be found: " + types);
    List<String> missing = new ArrayList<>();
    for (String type : types) {
      if (!Pattern.compile("\\b" + type + "\\b").matcher(text).find()) {
        missing.add(type);
      }
    }
    if (!missing.isEmpty()) {
      fail(
          missing.size()
              + " public type(s) of org.streamrune.test not named in "
              + document
              + ": "
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
