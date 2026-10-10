package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Keeps the project's public text self-contained: comments, javadoc, test names, log text, build
 * files and documentation must explain themselves instead of pointing at an issue-tracker label
 * that only exists in somebody's private notes.
 *
 * <p><b>What is rejected.</b> Work-item labels (a capital {@code W} or {@code FU} followed by
 * digits, optionally with a numeric suffix), review-finding identifiers (a letter-and-digits prefix
 * such as {@code A17-...}, or {@code SAGA-}, {@code CONC17-}, {@code PROJ17-}, {@code SEC-}, {@code
 * ERR17-}, {@code INTG10-} followed by a finding code), a numbered audit round, and review
 * nicknames. The exact expressions are {@link #FORBIDDEN}. Short review labels (a capital letter,
 * one or two digits and an optional lower-case letter) are rejected where they are used as a label:
 * alone in parentheses, or leading a comment line before a colon ({@link #SHORT_LABEL}). Time
 * constants ({@code T0}, {@code T1}), schema versions ({@code V1}, {@code V2}) and the C0/C1
 * control-character sets keep both forms. So are references to the private review material: the
 * expressions in {@link #PRIVATE_MATERIAL}.
 *
 * <p><b>History wording is rejected too.</b> Public text says what the code does and why, not how
 * it got there: a reviewer named by role and a numbered roadmap stage are rejected everywhere
 * ({@link #HISTORY}), and a reference to the state before or after a fix is rejected everywhere
 * except in test sources ({@link #FIX_HISTORY}), where a regression test may say what it fails
 * against.
 *
 * <p><b>What is scanned.</b> Every {@code .java} (main and test), {@code .md} and build file
 * (Gradle scripts, version catalog, properties, workflows, SQL, shell, XML, JSON) under the
 * repository root, except build output, VCS and tool directories, and the internal working material
 * that is never published ({@link #PRUNED_DIRECTORY_NAMES}, {@link #PRUNED_RELATIVE_PATHS}). {@code
 * CHANGELOG.md} is scanned like everything else.
 *
 * <p><b>The one allow-list.</b> A crash-point table in the javadoc of a test legitimately labels
 * its rows ({@code INV-n} for the invariant a row protects, {@code CP-n} for the crash point it
 * exercises); those labels are scoped to the table and the table spells out what each means. They
 * are tolerated only in the {@code src/test/java} files listed in {@link #CRASH_POINT_TABLE_TESTS},
 * and only for the {@link #CRASH_POINT_LABEL} expression; the list is one file long, and {@link
 * #theAllowListHoldsNoStaleEntries()} fails an entry whose file no longer uses a label, so it can
 * only shrink on its own.
 *
 * <p><b>Why a test.</b> The same labels were removed by hand once and would creep back the next
 * time a comment was written from a work log. A rule that depends on remembering is not a rule.
 *
 * <p>The scanned tree is declared as an input of {@code :streamrune-core:test} in {@code
 * streamrune-core/build.gradle.kts}, so Gradle re-runs this test when a file in another module or a
 * guide changes.
 */
class InternalTrackerIdRatchetTest {

  /**
   * The internal planning directory's name, built from pieces so the published text never names it
   * (the public export rejects any file that does).
   */
  private static final String PLANNING = "super" + "powers";

  /** Directory names pruned wherever they occur: build output, VCS, tooling, internal material. */
  private static final Set<String> PRUNED_DIRECTORY_NAMES =
      Set.of(
          "build",
          "out",
          ".git",
          ".gradle",
          ".kotlin",
          ".worktrees",
          ".idea",
          ".vscode",
          "node_modules",
          "." + PLANNING,
          "scratchpad",
          ".claude");

  /** Repository-relative directories that hold internal material and are never published. */
  private static final Set<String> PRUNED_RELATIVE_PATHS =
      Set.of("docs/" + PLANNING, "scripts/sonar");

  /** Repository-relative files that are internal tooling and are never published. */
  private static final Set<String> EXCLUDED_FILES = Set.of("sonar-project.properties");

  private static final Set<String> SCANNED_EXTENSIONS =
      Set.of(
          "java",
          "md",
          "kts",
          "gradle",
          "toml",
          "properties",
          "yml",
          "yaml",
          "sql",
          "sh",
          "xml",
          "json",
          "txt");

  /**
   * The forbidden shapes. Each alternative is anchored on word boundaries so a longer ordinary word
   * never matches: {@code W3C} and {@code WARN} are fine, a label like the examples in {@link
   * #SAMPLES} is not.
   */
  private static final Pattern FORBIDDEN =
      Pattern.compile(
          "\\b(?:W[0-9]{1,2}(?:-[0-9]+)?|FU[0-9]-[0-9]+|F2-[0-9]|PRE[1]"
              + "|A1[0-9]-[A-Z0-9-]+|SAGA-[A-Z0-9-]+|CONC1[0-9]-[0-9]+|PROJ1[0-9]-[0-9]+"
              + "|SEC-[A-Z-]+[0-9]*|ERR1[0-9]-[0-9]+|INTG1[0-9]-[0-9]+|audit[- ]?1[0-9]"
              + "|GPT-[A]stra|P[0-9] fix)\\b");

  /**
   * A short review label used as a label: alone in parentheses, or leading a comment line (after
   * {@code //}, {@code #} or a javadoc {@code *}) before a colon; {@link #SAMPLES}-style examples
   * are in {@link #theRuleRecognisesEveryShapeItForbids()}. Time constants {@code T0}..{@code T9},
   * schema versions {@code V0}..{@code V9} and the {@code C0}/{@code C1} control sets are allowed.
   */
  private static final Pattern SHORT_LABEL =
      Pattern.compile(
          "\\((?![TV][0-9]\\)|C[01]\\))[A-Z][0-9]{1,2}[a-z]?\\)"
              + "|(?://|#|\\*)\\s*(?![TV][0-9]:|C[01]:)[A-Z][0-9]{1,2}[a-z]?:");

  /**
   * References to the private review material (the static-analysis triage list, owner decisions and
   * external reviews), which a reader of the published code cannot follow. The bracketed space
   * keeps this file from matching its own rule.
   */
  private static final Pattern PRIVATE_MATERIAL =
      Pattern.compile(
          "(?i)\\b(?:sonar[ ]triage|owner[ ]decision|external[ ]review)\\b"
              + "|\\bpre-A1[0-9]\\b|\\bP[0-9] #[0-9]+[a-z]?\\b");

  /**
   * History wording rejected in every scanned file: a reviewer named by role, in any letter case,
   * and a numbered stage of the roadmap. The bracketed letter keeps this file from matching its own
   * rule; an ordinary lower-case "horizon" followed by a number (a time bound) is left alone.
   */
  private static final Pattern HISTORY = Pattern.compile("(?i:\\baud[i]tors?\\b)|\\bHorizon [0-9]");

  /**
   * A reference to the state before or after a fix, in any letter case. Rejected outside test
   * sources ({@link #isTestSource}): main code, build files and documentation describe the
   * behaviour as it is.
   */
  private static final Pattern FIX_HISTORY = Pattern.compile("(?i:\\b(?:pre|post)-fix\\b)");

  /** Crash-point table labels: tolerated only in {@link #CRASH_POINT_TABLE_TESTS}. */
  private static final Pattern CRASH_POINT_LABEL =
      Pattern.compile("\\b(?:INV-[0-9]+|CP-H?[0-9]+)\\b");

  /**
   * Repository-relative {@code src/test/java} files whose javadoc crash-point table may carry
   * {@code INV-n} / {@code CP-n} row labels. Each entry needs a table that explains the labels
   * itself.
   */
  private static final Set<String> CRASH_POINT_TABLE_TESTS =
      Set.of(
          "streamrune-runtime/src/test/java/org/streamrune/runtime/OutboxOrderingCrashPointTest.java");

  /**
   * Built from pieces so this file never contains a literal match for its own rule. Each sample
   * proves one alternative of {@link #FORBIDDEN} / {@link #CRASH_POINT_LABEL} still fires.
   */
  private static final List<String> SAMPLES =
      List.of(
          "W" + "7",
          "W" + "11-3",
          "FU" + "3-2",
          "F" + "2-5",
          "PRE" + "1",
          "A" + "16-ERR-3",
          "SAGA" + "-A9-1",
          "SAGA" + "-W" + "11-1",
          "CONC" + "17-3",
          "PROJ" + "17-3",
          "SEC" + "-AUTHZ-2",
          "ERR" + "17-5",
          "INTG" + "10-1",
          "audit" + "-17",
          "audit " + "16",
          "GPT" + "-Astra",
          "P" + "7 fix");

  @Test
  void noInternalTrackerIdAppearsInPublicText() {
    List<String> violations = new ArrayList<>();
    for (Path file : publicTextFiles()) {
      String relative = relative(file);
      boolean crashPointTableTest = CRASH_POINT_TABLE_TESTS.contains(relative);
      boolean testSource = isTestSource(relative);
      List<String> lines = read(file);
      for (int i = 0; i < lines.size(); i++) {
        String line = lines.get(i);
        if (FORBIDDEN.matcher(line).find()
            || SHORT_LABEL.matcher(line).find()
            || PRIVATE_MATERIAL.matcher(line).find()
            || HISTORY.matcher(line).find()
            || (!testSource && FIX_HISTORY.matcher(line).find())
            || (!crashPointTableTest && CRASH_POINT_LABEL.matcher(line).find())) {
          violations.add(relative + ":" + (i + 1) + ": " + line.strip());
        }
      }
    }
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " line(s) cite an internal tracker id (work item, review finding, audit round or"
              + " spec label) or narrate history (a reviewer, the state before a fix, a roadmap"
              + " stage). Say what the thing is and why, in words, instead of citing its label or"
              + " its past; rename a test whose name carries one. Offenders:\n  "
              + String.join("\n  ", violations));
    }
  }

  /** Whether a repository-relative path is a test source file. */
  private static boolean isTestSource(String relative) {
    return relative.contains("/src/test/");
  }

  @Test
  void theRuleRecognisesEveryShapeItForbids() {
    for (String sample : SAMPLES) {
      assertTrue(
          FORBIDDEN.matcher("see " + sample + " for details").find(),
          "the rule must reject: " + sample);
    }
    for (String label :
        List.of(
            "rejected, not rewritten (" + "D11).",
            "an Error included (" + "D3) rolls back",
            "per (" + "T3a), encrypted",
            "      // " + "H3: loop while any row was deleted",
            "# " + "N3: the oldest supported server",
            "   * " + "R2: queues the action",
            "   * " + "T3a: retention")) {
      assertTrue(SHORT_LABEL.matcher(label).find(), "the rule must reject: " + label);
    }
    for (String reference :
        List.of(
            "(Sonar" + " triage D11)",
            "(owner" + " decision 2026-09-30)",
            "Adapted from the external" + " review's appendix",
            "the pre" + "-A16 behaviour",
            "property test (P" + "2 #9c)")) {
      assertTrue(PRIVATE_MATERIAL.matcher(reference).find(), "the rule must reject: " + reference);
    }
    assertTrue(CRASH_POINT_LABEL.matcher("INV" + "-4 holds").find());
    assertTrue(CRASH_POINT_LABEL.matcher("crash at CP" + "-H2").find());
    for (String history :
        List.of(
            "Rejected the aud" + "itor's proposal",
            "The Aud" + "itor proposed one",
            "what the aud" + "itors saw",
            "out of scope for Horizon" + " 1",
            "on the Horizon" + " 2 backlog")) {
      assertTrue(HISTORY.matcher(history).find(), "the rule must reject: " + history);
    }
    for (String fixHistory :
        List.of(
            "the pre" + "-fix sweeper counted it",
            "Pre" + "-fix this helper converted only throws",
            "Post" + "-fix the value is returned")) {
      assertTrue(FIX_HISTORY.matcher(fixHistory).find(), "the rule must reject: " + fixHistory);
    }
    assertTrue(isTestSource("streamrune-core/src/test/java/org/streamrune/core/SomeTest.java"));
    assertFalse(isTestSource("streamrune-core/src/main/java/org/streamrune/core/Some.java"));
    assertFalse(isTestSource("docs/guide/production.md"));
    assertFalse(isTestSource("build.gradle.kts"));
  }

  @Test
  void theRuleLeavesOrdinaryTextAlone() {
    for (String ordinary :
        List.of(
            "W3C trace context",
            "the WARN level",
            "Quickstart §10, GraalVM Native Image",
            "Reactive Streams §2.13",
            "SHA-256 and AES-256",
            "ISO-8601",
            "HTTP-2",
            "an audit trail",
            "the audit log",
            "round trip",
            "PRE-existing row",
            "the give-up horizon 1 h",
            "inFlightHorizon()",
            "the in-flight horizon 60s",
            "an audited command",
            "a prefix of the batch",
            "the pre-fixed key")) {
      assertFalse(FORBIDDEN.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(CRASH_POINT_LABEL.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(SHORT_LABEL.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(PRIVATE_MATERIAL.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(HISTORY.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(FIX_HISTORY.matcher(ordinary).find(), "must not flag: " + ordinary);
    }
    for (String ordinary :
        List.of(
            "the episode age (T0) is the claim anchor",
            "    // T0: router poison",
            "a BEL (C0) becomes one space",
            "resolves to the current class (V2)",
            "    // V1: the original shape",
            "E2[Event record]",
            "the SonarQube triage of rule S2077",
            "a decision for the owner of the subject",
            "an external reviewer")) {
      assertFalse(SHORT_LABEL.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(PRIVATE_MATERIAL.matcher(ordinary).find(), "must not flag: " + ordinary);
    }
  }

  @Test
  void theScannerActuallySeesTheRepository() {
    // Guards the rule itself: a walk that silently finds nothing would pass forever. The numbers
    // are floors, so ordinary growth never trips them.
    List<Path> files = publicTextFiles();
    long java = files.stream().filter(p -> p.toString().endsWith(".java")).count();
    long markdown = files.stream().filter(p -> p.toString().endsWith(".md")).count();
    long build = files.stream().filter(p -> p.toString().endsWith(".kts")).count();
    assertTrue(java > 500, "expected the main and test sources, found " + java + " java files");
    assertTrue(markdown > 20, "expected the guides and READMEs, found " + markdown);
    assertTrue(build > 5, "expected the Gradle build files, found " + build);
    assertTrue(
        files.stream().anyMatch(p -> relative(p).equals("CHANGELOG.md")),
        "CHANGELOG.md must be scanned");
    assertTrue(
        files.stream().anyMatch(p -> relative(p).contains("/src/test/java/")),
        "test sources must be scanned");
    assertTrue(
        files.stream().noneMatch(p -> relative(p).startsWith("." + PLANNING + "/")),
        "internal working material is never scanned");
  }

  @Test
  void theAllowListHoldsNoStaleEntries() {
    Path root = repositoryRoot();
    for (String entry : CRASH_POINT_TABLE_TESTS) {
      Path file = root.resolve(entry);
      assertTrue(
          entry.contains("/src/test/java/") && entry.endsWith(".java"),
          "only test sources may use the crash-point label exception: " + entry);
      assertTrue(Files.isRegularFile(file), "allow-list entry no longer exists: " + entry);
      assertTrue(
          read(file).stream().anyMatch(l -> CRASH_POINT_LABEL.matcher(l).find()),
          "allow-list entry no longer uses a crash-point label; remove it: " + entry);
    }
    assertEquals(
        CRASH_POINT_TABLE_TESTS.size(),
        new java.util.HashSet<>(CRASH_POINT_TABLE_TESTS).size(),
        "duplicate allow-list entries");
  }

  private static String relative(Path file) {
    return repositoryRoot().relativize(file).toString().replace('\\', '/');
  }

  private static List<String> read(Path file) {
    try {
      return Files.readAllLines(file, StandardCharsets.UTF_8);
    } catch (java.nio.file.NoSuchFileException | java.nio.charset.MalformedInputException _) {
      // Another task of a parallel build replaced the file between listing and reading, or the
      // file is not UTF-8 text.
      return List.of();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Every public text file under the repository root, sorted. Excluded subtrees are pruned rather
   * than walked and filtered, and an entry that vanishes mid-walk is skipped: sibling tasks of a
   * parallel build create and delete files under {@code build/} all the time.
   */
  private static List<Path> publicTextFiles() {
    Path root = repositoryRoot();
    List<Path> files = new ArrayList<>();
    try {
      Files.walkFileTree(
          root,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
              if (dir.equals(root)) {
                return FileVisitResult.CONTINUE;
              }
              String name = dir.getFileName().toString();
              String rel = root.relativize(dir).toString().replace('\\', '/');
              return PRUNED_DIRECTORY_NAMES.contains(name) || PRUNED_RELATIVE_PATHS.contains(rel)
                  ? FileVisitResult.SKIP_SUBTREE
                  : FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              String name = file.getFileName().toString();
              int dot = name.lastIndexOf('.');
              String extension = dot < 0 ? "" : name.substring(dot + 1);
              String rel = root.relativize(file).toString().replace('\\', '/');
              if (attrs.isRegularFile()
                  && SCANNED_EXTENSIONS.contains(extension)
                  && !EXCLUDED_FILES.contains(rel)) {
                files.add(file);
              }
              return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    files.sort(java.util.Comparator.naturalOrder());
    return files;
  }

  private static Path repositoryRoot() {
    Path dir = Path.of("").toAbsolutePath();
    while (dir != null) {
      if (Files.exists(dir.resolve("settings.gradle.kts"))) {
        return dir;
      }
      dir = dir.getParent();
    }
    throw new IllegalStateException(
        "cannot locate the repository root (no settings.gradle.kts above "
            + Path.of("").toAbsolutePath()
            + ")");
  }
}
