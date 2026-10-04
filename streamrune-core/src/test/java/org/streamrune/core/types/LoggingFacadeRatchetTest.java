package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Logging goes through SLF4J and nothing else. This ratchet fails the build when any production
 * source set reaches for the JDK's {@code System.Logger} / {@code System.getLogger} or for {@code
 * java.util.logging}.
 *
 * <p>Why one facade: an adopter configures log levels, appenders and the sanitized-identifier
 * policy ({@link LogSinkPolicyRatchetTest}) in exactly one place. A class that logs through the JDK
 * facade bypasses that configuration (it lands on {@code java.util.logging}'s own console handler
 * unless a bridge is installed), and the log-sink ratchet cannot see what a second facade renders.
 *
 * <p>The scan reuses {@link LogSinkPolicyRatchetTest#productionSources()}, so it covers exactly the
 * tree the log-sink ratchet covers, with the same Gradle input declaration. Comments and string
 * literals are blanked first, so prose that merely mentions a forbidden name does not trip it.
 *
 * <p>{@link #ALLOWED} lists the only files permitted to name a forbidden facade, each with the
 * reason. It is empty: no production source needs one. An entry that no longer matches a violating
 * file fails {@link #theAllowListHoldsNoStaleEntries()}, so the list cannot rot into a blanket
 * exemption. Add an entry only where a platform genuinely requires {@code java.util.logging}.
 */
class LoggingFacadeRatchetTest {

  /** Repository-relative path suffix, mapped to the one-line reason it is allowed. */
  private static final java.util.Map<String, String> ALLOWED = java.util.Map.of();

  private static final List<Pattern> FORBIDDEN =
      List.of(
          Pattern.compile("\\bSystem\\s*\\.\\s*getLogger\\b"),
          Pattern.compile("\\bSystem\\s*\\.\\s*Logger\\b"),
          Pattern.compile("\\bjava\\s*\\.\\s*util\\s*\\.\\s*logging\\b"),
          Pattern.compile("\\brequires\\s+(?:transitive\\s+)?java\\.logging\\b"));

  @Test
  void noProductionSourceLogsThroughSystemLoggerOrJavaUtilLogging() {
    List<String> violations = new ArrayList<>();
    for (Path file : LogSinkPolicyRatchetTest.productionSources()) {
      if (isAllowed(file)) {
        continue;
      }
      violations.addAll(violationsIn(file.toString(), LogSinkPolicyRatchetTest.readSource(file)));
    }
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " use(s) of System.Logger / java.util.logging in production sources. Log through"
              + " SLF4J (org.slf4j.Logger / LoggerFactory):\n  "
              + String.join("\n  ", violations));
    }
  }

  @Test
  void theScannerRecognisesEveryForbiddenForm() {
    for (String bad :
        List.of(
            "private static final System.Logger LOG = System.getLogger(\"x\");",
            "import java.lang.System.Logger;",
            "import java.util.logging.Logger;",
            "java.util.logging.Logger.getLogger(\"x\").info(\"m\");",
            "requires java.logging;",
            "System . getLogger(X.class.getName())")) {
      assertEquals(1, violationsIn("Sample.java", List.of(bad)).size(), "must flag: " + bad);
    }
  }

  @Test
  void theScannerIgnoresCommentsAndStringLiterals() {
    List<String> source =
        List.of(
            "/** Mentions System.Logger and java.util.logging in prose. */",
            "// System.getLogger is not used here",
            "String s = \"java.util.logging\";",
            "String t = \"\"\"",
            "    System.getLogger",
            "    \"\"\";",
            "/* java.util.logging",
            "   System.Logger */",
            "org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(X.class);");
    assertEquals(List.of(), violationsIn("Sample.java", source));
  }

  @Test
  void theAllowListHoldsNoStaleEntries() {
    Set<String> stillViolating = new java.util.TreeSet<>();
    for (Path file : LogSinkPolicyRatchetTest.productionSources()) {
      if (isAllowed(file)
          && !violationsIn(file.toString(), LogSinkPolicyRatchetTest.readSource(file)).isEmpty()) {
        stillViolating.add(allowedKey(file));
      }
    }
    for (String entry : ALLOWED.keySet()) {
      assertTrue(
          stillViolating.contains(entry),
          "allow-list entry no longer names a violating production source; remove it: " + entry);
    }
  }

  private static boolean isAllowed(Path file) {
    return allowedKey(file) != null;
  }

  private static String allowedKey(Path file) {
    String path = file.toString().replace('\\', '/');
    return ALLOWED.keySet().stream().filter(path::endsWith).findFirst().orElse(null);
  }

  private static List<String> violationsIn(String name, List<String> lines) {
    String code = blankCommentsAndLiterals(String.join("\n", lines));
    List<String> out = new ArrayList<>();
    String[] codeLines = code.split("\n", -1);
    for (int i = 0; i < codeLines.length; i++) {
      for (Pattern p : FORBIDDEN) {
        if (p.matcher(codeLines[i]).find()) {
          out.add(Path.of(name).getFileName() + ":" + (i + 1) + " " + codeLines[i].strip());
          break;
        }
      }
    }
    return out;
  }

  /**
   * Replaces comments and string / char / text-block literal contents with spaces, keeping every
   * newline so reported line numbers stay true.
   */
  static String blankCommentsAndLiterals(String s) {
    StringBuilder out = new StringBuilder(s.length());
    int i = 0;
    int n = s.length();
    while (i < n) {
      char c = s.charAt(i);
      if (s.startsWith("//", i)) {
        while (i < n && s.charAt(i) != '\n') {
          out.append(' ');
          i++;
        }
      } else if (s.startsWith("/*", i)) {
        int end = s.indexOf("*/", i + 2);
        end = end < 0 ? n : end + 2;
        blank(out, s, i, end);
        i = end;
      } else if (s.startsWith("\"\"\"", i)) {
        int end = s.indexOf("\"\"\"", i + 3);
        end = end < 0 ? n : end + 3;
        blank(out, s, i, end);
        i = end;
      } else if (c == '"' || c == '\'') {
        int j = i + 1;
        while (j < n && s.charAt(j) != c && s.charAt(j) != '\n') {
          j += s.charAt(j) == '\\' ? 2 : 1;
        }
        int end = Math.min(n, j + 1);
        blank(out, s, i, end);
        i = end;
      } else {
        out.append(c);
        i++;
      }
    }
    return out.toString();
  }

  private static void blank(StringBuilder out, String s, int from, int to) {
    for (int k = from; k < to; k++) {
      out.append(s.charAt(k) == '\n' ? '\n' : ' ');
    }
  }
}
