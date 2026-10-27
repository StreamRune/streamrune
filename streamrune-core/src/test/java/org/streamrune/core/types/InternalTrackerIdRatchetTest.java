package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Keeps the project's public text self-contained: comments, javadoc, test names, log text, build
 * files and documentation must explain themselves instead of pointing at an issue-tracker label
 * that only exists in somebody's private notes.
 *
 * <p><b>What is rejected.</b> Work-item labels (a capital {@code W} or {@code FU} followed by
 * digits, optionally with a numeric suffix, and in either letter case such a label at the front of
 * an identifier, a test name or a string id), review-finding identifiers (a letter-and-digits
 * prefix such as {@code A17-...}, or {@code SAGA-}, {@code CONC17-}, {@code PROJ17-}, {@code SEC-},
 * {@code ERR17-}, {@code INTG10-} followed by a finding code), a numbered audit round, and review
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
 * against. The same split holds for review-finding labels, rejected everywhere ({@link
 * #REVIEW_ID}), and for narration of an earlier behaviour, rejected outside test sources and code
 * samples ({@link #HISTORY_NARRATION}); work deferred to the project roadmap is rejected everywhere
 * ({@link #ROADMAP}).
 *
 * <p><b>Across a line break.</b> A rule that looks for a phrase of more than one word reads on into
 * the next line of the same kind, so a phrase a reflow broke across two lines is still found, and
 * reported on the line it starts on ({@link #ruleBrokenBy}).
 *
 * <p><b>No space before a period.</b> In prose, a period follows its word directly ({@link
 * #STRAY_SPACE_BEFORE_PERIOD}). Prose means the comments of a {@code .java} file — javadoc and
 * block-comment lines outside {@code <pre>} blocks, a one-line javadoc or block comment, a line
 * comment that is a line of its own — and lines of a {@code .md} file outside fenced code blocks;
 * inline code (a backtick span, an inline javadoc tag) counts as one word, so a code sample is
 * never flagged. The other files, the build files among them, are not checked for it.
 *
 * <p><b>What is scanned.</b> Every {@code .java} (main and test), {@code .md} and build file
 * (Gradle scripts, version catalog, properties, workflows, SQL, shell and batch scripts, XML, JSON,
 * text) under the repository root, and the files without an extension the repository publishes, by
 * name ({@link #SCANNED_NAMES}: the commit template {@code .gitmessage}, {@code LICENSE}, {@code
 * NOTICE}, {@code CODEOWNERS}, the git hook, {@code gradlew}), except build output, VCS and tool
 * directories, and the internal working material that is never published ({@link
 * #PRUNED_DIRECTORY_NAMES}, {@link #PRUNED_RELATIVE_PATHS}). {@code CHANGELOG.md} is scanned like
 * everything else.
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
          "bat",
          "xml",
          "json",
          "txt");

  /**
   * The files without an extension that the repository publishes, scanned by name: the commit
   * template, the ignore list, the license and notice, the code owners, the git hook and the Gradle
   * wrapper script. {@code streamrune-core/build.gradle.kts} declares the same names.
   */
  private static final Set<String> SCANNED_NAMES =
      Set.of(
          ".gitmessage", ".gitignore", "LICENSE", "NOTICE", "CODEOWNERS", "pre-commit", "gradlew");

  /** Whether a file of this name is scanned: one of {@link #SCANNED_EXTENSIONS} or names. */
  static boolean isScannedName(String name) {
    int dot = name.lastIndexOf('.');
    return SCANNED_NAMES.contains(name)
        || (dot >= 0 && SCANNED_EXTENSIONS.contains(name.substring(dot + 1)));
  }

  /**
   * The forbidden shapes. Each alternative is anchored on word boundaries so a longer ordinary word
   * never matches: {@code W3C} and {@code WARN} are fine, a label like the examples in {@link
   * #SAMPLES} is not. A work-item label written in lower case as the prefix of an identifier, a
   * test method name or a string id is rejected too, in either letter case: the letter, one or two
   * digits and a review code before an underscore or a dash ({@code w} then {@code 3fd4_} in the
   * samples), or the letter and digits before a dash and a word ({@code w} then {@code 52-stream}).
   */
  private static final Pattern FORBIDDEN =
      Pattern.compile(
          "\\b(?:W[0-9]{1,2}(?:-[0-9]+)?|FU[0-9]-[0-9]+|F2-[0-9]|PRE[1]"
              + "|A1[0-9]-[A-Z0-9-]+|SAGA-[A-Z0-9-]+|CONC1[0-9]-[0-9]+|PROJ1[0-9]-[0-9]+"
              + "|SEC-[A-Z-]+[0-9]*|ERR1[0-9]-[0-9]+|INTG1[0-9]-[0-9]+|audit[- ]?1[0-9]"
              + "|GPT-[A]stra|P[0-9] fix)\\b"
              + "|(?i:\\bw[0-9]{1,2}[a-z]{1,2}[0-9]{1,2}(?=[_-])|\\bw[0-9]{1,2}-(?=[a-z]))");

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

  /**
   * A review-finding label: {@code FIX} or {@code Minor-} followed by a number, and {@code Fix}
   * followed by a number where it is used as a label, alone in parentheses or leading a comment
   * line before a colon (like {@link #SHORT_LABEL}). Rejected everywhere, test sources included: a
   * regression test names the behaviour it guards, not the finding that asked for it. Case matters,
   * so an ordinary sentence that starts with the verb ("Fix three flaky tests") is left alone.
   */
  private static final Pattern REVIEW_ID =
      Pattern.compile(
          "\\bFIX [0-9]+\\b|\\bMinor-[0-9]+\\b|\\(Fix [0-9]+\\)|(?://|#|\\*)\\s*Fix [0-9]+:");

  /**
   * Narration of an earlier behaviour of the code, in any letter case: the words that describe a
   * change instead of the current state ({@code previously}, {@code used to}, {@code no longer},
   * {@code now also}, {@code until now}, {@code historically}, "this wave" (the unit of the work
   * that changed the code), "before this" followed by fix, change or release, and "before this" or
   * "exactly as before" closing a clause). Rejected outside test sources ({@link #isTestSource})
   * and outside code samples (a fenced block of a guide, a {@code <pre>} block of a javadoc), which
   * quote code as it is written; read on into the next line ({@link #ruleBrokenBy}). Write the
   * state as it is: "cannot" or "is not" for a state that ends at run time, "the recorded result"
   * for a result recorded earlier, "the clock that computes" for a purpose. Left alone: {@code used
   * to} after a form of "be", directly or with one word between ("is used to look up", "is not used
   * to gate", "is also used to compute", "was successfully used to rehydrate"), the passive of
   * "use"; "this wave of" ("replicas that join this wave of appends"), a state at run time; "before
   * this" without the comma ("before this call returns"); and "exactly as before" followed by an
   * article or a determiner ("exactly as before the reinstate"), a state at run time.
   */
  private static final Pattern HISTORY_NARRATION =
      Pattern.compile(
          "(?i)\\b(?:previously|historically|until now|now also|no longer"
              + "|this wave(?! of\\b)"
              + "|before this (?:fix|change|release)"
              + "|(?<!\\b(?:is|are|was|were|be|been|being)(?: [a-z]{1,20})? )used to)\\b"
              + "|\\bbefore this,"
              + "|\\bexactly as before\\b"
              + "(?!\\s+(?:the|a|an|its|this|that|their|any|each|every)\\b)");

  /**
   * Work deferred to the project roadmap ("on the" followed by "roadmap"), in any letter case:
   * public text describes what exists. Rejected everywhere; the bracketed letter keeps this file's
   * own source from matching.
   */
  private static final Pattern ROADMAP = Pattern.compile("(?i)\\bon the road[m]ap\\b");

  /**
   * A space between a word (or a closing parenthesis or bracket) and the period that ends it.
   * Checked on prose only ({@link #classify}); see the class javadoc.
   */
  private static final Pattern STRAY_SPACE_BEFORE_PERIOD =
      Pattern.compile("[A-Za-z0-9)\\]] \\.(?:\\s|$)");

  /** Inline code in markdown, replaced by one word before the prose rules run. */
  private static final Pattern MARKDOWN_CODE_SPAN = Pattern.compile("(`+).+?\\1");

  /** Inline javadoc tags, replaced by one word before the prose rules run. */
  private static final Pattern JAVADOC_INLINE_TAG =
      Pattern.compile("\\{@(?:code|link|linkplain|literal|value)\\b[^}]*\\}");

  /** A javadoc or block-comment line: its first character after the indentation is a star. */
  private static final Pattern COMMENT_LINE = Pattern.compile("^\\s*\\*");

  /**
   * A comment that is a whole line of its own: a line comment, or a javadoc or block comment that
   * opens and closes on the line.
   */
  private static final Pattern ONE_LINE_COMMENT = Pattern.compile("^\\s*(?://|/\\*.*\\*/\\s*$)");

  /** The closing marker of a one-line javadoc or block comment, left out of its prose view. */
  private static final Pattern CLOSING_MARKER = Pattern.compile("\\s*\\*/\\s*$");

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
          "P" + "7 fix",
          "w" + "3fd4_eventNotFoundStop",
          "W" + "3FR1_healthyRow",
          "w" + "52-stream");

  @Test
  void noInternalTrackerIdAppearsInPublicText() {
    List<String> violations = new ArrayList<>();
    Map<String, Integer> perRule = new TreeMap<>();
    for (Path file : publicTextFiles()) {
      String relative = relative(file);
      List<Line> lines = classify(relative, read(file));
      for (int i = 0; i < lines.size(); i++) {
        Line next = i + 1 < lines.size() ? lines.get(i + 1) : null;
        String rule = ruleBrokenBy(relative, lines.get(i), next);
        if (rule != null) {
          violations.add(
              rule + " " + relative + ":" + (i + 1) + ": " + lines.get(i).text().strip());
          perRule.merge(rule, 1, Integer::sum);
        }
      }
    }
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " line(s) "
              + perRule
              + " cite an internal tracker id (work item, review finding, audit round or spec"
              + " label), narrate history (a reviewer, the state before a fix, an earlier"
              + " behaviour, a roadmap) or put a space before a period. Say what the thing is and"
              + " why, in words and in the present tense, instead of citing its label or its past;"
              + " rename a test whose name carries one. Offenders:\n  "
              + String.join("\n  ", violations));
    }
  }

  /**
   * One line of a scanned file: its text, its prose view (the text with inline code replaced by one
   * word, or {@code null} when the line is not prose) and whether it belongs to a code sample.
   */
  record Line(String text, String prose, boolean sample) {}

  /**
   * The rule a line of the repository-relative file breaks, by the name of its pattern, or {@code
   * null}. {@code next} is the line after it, or {@code null} at the end of the file.
   *
   * <p>A rule that matches a phrase of more than one word reads on into the next line when that
   * line is of the same kind (prose, code sample, or neither): a reflowed comment or paragraph
   * breaks a phrase across a line break ("no" at the end of one line, "longer" at the start of the
   * next), and the phrase is still there. Both lines are read without their indentation and their
   * leading comment or quote marker ({@link #unmarked}), joined by one space, and a match belongs
   * to the line it starts on. The label rules, which match a single token or a marker-anchored
   * label, read the line alone.
   */
  private static String ruleBrokenBy(String relative, Line classified, Line next) {
    String line = classified.text();
    String prose = classified.prose();
    boolean testSource = isTestSource(relative);
    Line joinable = next != null && kindOf(next) == kindOf(classified) ? next : null;
    String nextText = joinable == null ? null : joinable.text();
    String nextProse = joinable == null ? null : joinable.prose();
    if (startsHere(FORBIDDEN, line, nextText)) {
      return "FORBIDDEN";
    }
    if (SHORT_LABEL.matcher(line).find()) {
      return "SHORT_LABEL";
    }
    if (REVIEW_ID.matcher(line).find() || startsHere(REVIEW_ID, line, nextText)) {
      return "REVIEW_ID";
    }
    if (startsHere(PRIVATE_MATERIAL, line, nextText)) {
      return "PRIVATE_MATERIAL";
    }
    if (startsHere(HISTORY, line, nextText)) {
      return "HISTORY";
    }
    if (startsHere(ROADMAP, line, nextText)) {
      return "ROADMAP";
    }
    if (!testSource && FIX_HISTORY.matcher(line).find()) {
      return "FIX_HISTORY";
    }
    if (!testSource && !classified.sample() && startsHere(HISTORY_NARRATION, line, nextText)) {
      return "HISTORY_NARRATION";
    }
    if (!CRASH_POINT_TABLE_TESTS.contains(relative) && CRASH_POINT_LABEL.matcher(line).find()) {
      return "CRASH_POINT_LABEL";
    }
    if (prose != null && startsHere(STRAY_SPACE_BEFORE_PERIOD, prose, nextProse)) {
      return "STRAY_SPACE_BEFORE_PERIOD";
    }
    return null;
  }

  /** The kind of a line, for {@link #ruleBrokenBy}: lines of one kind read on into each other. */
  private enum Kind {
    PROSE,
    SAMPLE,
    OTHER
  }

  private static Kind kindOf(Line line) {
    return line.prose() != null ? Kind.PROSE : line.sample() ? Kind.SAMPLE : Kind.OTHER;
  }

  /**
   * The indentation and one leading comment or quote marker of a line: what {@link #unmarked}
   * drops.
   */
  private static final Pattern LEADING_MARKER =
      Pattern.compile("^\\s*(?:/\\*\\*?|\\*+|//+|#+|>+)?\\s*");

  /** {@code text} without its indentation and leading comment or quote marker. */
  static String unmarked(String text) {
    return LEADING_MARKER.matcher(text).replaceFirst("");
  }

  /**
   * Whether {@code pattern} matches the line {@code text} joined with {@code nextText} (both {@link
   * #unmarked}, one space between them; {@code nextText} is {@code null} for no next line) at a
   * position on {@code text}: a match that starts on the next line is that line's.
   */
  static boolean startsHere(Pattern pattern, String text, String nextText) {
    String head = unmarked(text);
    String joined = nextText == null ? head : head + " " + unmarked(nextText);
    Matcher matcher = pattern.matcher(joined);
    return matcher.find() && matcher.start() < head.length();
  }

  /**
   * Classifies each line of a file, index for index. In a {@code .md} file a fenced code block (a
   * line that starts with three backticks or three tildes opens and closes one) is a code sample
   * and every other line is prose; in a {@code .java} file a javadoc or block-comment line is
   * prose, or a code sample inside a {@code <pre>} block, and so is a comment that is a whole line
   * of its own ({@link #ONE_LINE_COMMENT}). Code lines and every line of the other file types are
   * neither: they are scanned whole, string literals and comments alike.
   */
  static List<Line> classify(String relative, List<String> lines) {
    List<Line> classified = new ArrayList<>(lines.size());
    boolean markdown = relative.endsWith(".md");
    boolean java = relative.endsWith(".java");
    boolean inSample = false;
    for (String line : lines) {
      if (markdown) {
        String stripped = line.strip();
        if (stripped.startsWith("```") || stripped.startsWith("~~~")) {
          inSample = !inSample;
          classified.add(new Line(line, null, true));
        } else if (inSample) {
          classified.add(new Line(line, null, true));
        } else {
          classified.add(
              new Line(line, MARKDOWN_CODE_SPAN.matcher(line).replaceAll("code"), false));
        }
      } else if (java && COMMENT_LINE.matcher(line).find()) {
        boolean opens = line.contains("<pre>");
        boolean closes = line.contains("</pre>");
        if (inSample || opens || closes) {
          classified.add(new Line(line, null, true));
        } else {
          classified.add(
              new Line(line, JAVADOC_INLINE_TAG.matcher(line).replaceAll("code"), false));
        }
        if (opens != closes) {
          inSample = opens;
        }
      } else if (java && ONE_LINE_COMMENT.matcher(line).find()) {
        // Inline code is one word here too: a javadoc tag, or a backtick span (a Markdown
        // documentation comment, or a line comment that quotes code that way).
        String text = CLOSING_MARKER.matcher(line).replaceFirst("");
        text = JAVADOC_INLINE_TAG.matcher(text).replaceAll("code");
        classified.add(new Line(line, MARKDOWN_CODE_SPAN.matcher(text).replaceAll("code"), false));
      } else {
        classified.add(new Line(line, null, false));
      }
    }
    return classified;
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
    for (String reviewId :
        List.of(
            "see FI" + "X 3 for details",
            "the Mino" + "r-2 case",
            "a timeout (Fi" + "x 4) is fatal",
            "    // Fi" + "x 2: the claim is released",
            "   * Fi" + "x 7: queues the action",
            "# Fi" + "x 1: the oldest server")) {
      assertTrue(REVIEW_ID.matcher(reviewId).find(), "the rule must reject: " + reviewId);
    }
    for (String narration :
        List.of(
            "previ" + "ously the sweeper counted it",
            "Previ" + "ously this method returned null",
            "the guard us" + "ed to exist only on the bus",
            "It us" + "ed to be key-scoped",
            "a claim the poller no lo" + "nger holds",
            "the rule now al" + "so covers the band",
            "until no" + "w it left this loop",
            "histori" + "cally only a scrape",
            "befo" + "re this fix the lock leaked",
            "before this ch" + "ange it was global",
            "before this wa" + "ve the sweep was manual",
            "before this re" + "lease the key was optional")) {
      assertTrue(HISTORY_NARRATION.matcher(narration).find(), "the rule must reject: " + narration);
    }
    for (String roadmap :
        List.of("Envelope encryption is on the road" + "map", "It is On The Road" + "map.")) {
      assertTrue(ROADMAP.matcher(roadmap).find(), "the rule must reject: " + roadmap);
    }
    for (String stray :
        List.of(
            "the run is done " + ".", "ends in a parenthesis (here) " + ". Next", "[1] " + ".")) {
      assertTrue(STRAY_SPACE_BEFORE_PERIOD.matcher(stray).find(), "the rule must reject: " + stray);
    }
    String strayProse = "The sweep is done " + ".";
    assertEquals(
        "STRAY_SPACE_BEFORE_PERIOD",
        ruleBrokenBy(
            "docs/guide/x.md", classify("docs/guide/x.md", List.of(strayProse)).get(0), null));
    String strayJavadoc = "   * The sweep is done " + ".";
    assertEquals(
        "STRAY_SPACE_BEFORE_PERIOD",
        ruleBrokenBy(
            "a/src/main/java/A.java", classify("A.java", List.of(strayJavadoc)).get(0), null));
    // A one-line javadoc and a line comment are prose too.
    for (String strayComment :
        List.of(
            "  /** The sweep is done " + ". */",
            "  /* The sweep is done " + ". */",
            "    // the sweep is done " + ".",
            "    // the sweep is done " + ". Then the purge runs.")) {
      assertEquals(
          "STRAY_SPACE_BEFORE_PERIOD",
          ruleBrokenBy(
              "a/src/main/java/A.java", classify("A.java", List.of(strayComment)).get(0), null),
          strayComment);
    }
    String narrationInGuide = "The rule now al" + "so covers the band.";
    assertEquals(
        "HISTORY_NARRATION",
        ruleBrokenBy(
            "docs/guide/x.md",
            classify("docs/guide/x.md", List.of(narrationInGuide)).get(0),
            null));
    for (String narration :
        List.of(
            "Since this wa" + "ve, both runners continue",
            "after this wa" + "ve the guard is on",
            "This wa" + "ve adds a role to the view",
            "In this wa" + "ve, the lease is epoch-fenced",
            "With this wa" + "ve the runner reports a role",
            "this wa" + "ve fixes the guard",
            "Befo" + "re this, the guard was a flag",
            "tolerated exactly as befo" + "re.",
            "threads it in exactly as befo" + "re even with zero registrations",
            "and stays exactly as befo" + "re (the erased subject)")) {
      assertTrue(HISTORY_NARRATION.matcher(narration).find(), "the rule must reject: " + narration);
    }
    // A phrase broken across a line break is still one phrase, reported on the line it starts on.
    assertWrapped(
        "HISTORY_NARRATION", "a/src/main/java/A.java", "   * It no", "   * lo" + "nger guesses.");
    assertWrapped(
        "HISTORY_NARRATION",
        "a/src/main/java/A.java",
        "    // a relay whose thread is no",
        "    // lo" + "nger alive died unexpectedly");
    assertWrapped(
        "HISTORY_NARRATION",
        "docs/guide/x.md",
        "(the event store can no",
        "lo" + "nger read the entry's offset)");
    assertWrapped("ROADMAP", "docs/guide/x.md", "Envelope encryption is on the", "road" + "map.");
    assertWrapped(
        "STRAY_SPACE_BEFORE_PERIOD",
        "a/src/main/java/A.java",
        "   * The sweep is done",
        "   * " + ". Then the purge runs.");
  }

  /** No line of {@code lines}, consecutive lines of {@code relative}, breaks a rule. */
  private static void assertNoneFlagged(String relative, List<String> lines) {
    List<Line> classified = classify(relative, lines);
    for (int i = 0; i < classified.size(); i++) {
      Line next = i + 1 < classified.size() ? classified.get(i + 1) : null;
      assertNull(
          ruleBrokenBy(relative, classified.get(i), next),
          "must not flag: " + classified.get(i).text());
    }
  }

  /**
   * {@code first} and {@code second}, consecutive lines of {@code relative}, break {@code rule}.
   */
  private static void assertWrapped(String rule, String relative, String first, String second) {
    List<Line> lines = classify(relative, List.of(first, second));
    assertEquals(rule, ruleBrokenBy(relative, lines.get(0), lines.get(1)), first + " / " + second);
    assertNull(ruleBrokenBy(relative, lines.get(1), null), "reported once, on the first line");
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
            "the pre-fixed key",
            "a fixture of the kit",
            "the minority of replicas",
            "a minor-version bump",
            "nowadays every replica leads",
            "Fix three flaky tests",
            "Fix 3 flaky tests in the suite",
            "FIXME",
            "the key is used to look up the subject",
            "it cannot be used to gate a decision",
            "a snapshot that was successfully used to rehydrate the aggregate",
            "the fence is not used to gate reads",
            "the table is also used to compute lag",
            "replicas that join this wave of appends wait",
            "after this wave of appends the lease renews",
            "a W3C-compliant header",
            "the w3c-baggage header",
            "a w-9 form",
            "the rows aw3fd4_x and xw52-y",
            "the previous offset",
            "the road map of a city",
            "version 1.0.1",
            "e.g. a lease",
            "wait ... and retry",
            "./gradlew build",
            "before this call returns",
            "rebuilds stay working, exactly as before the reinstate",
            "the runner processes exactly as before the gate opens",
            "as before the restore",
            "as it was before the transaction",
            "each wave of concurrent appends",
            "the third wave queues")) {
      assertFalse(FORBIDDEN.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(CRASH_POINT_LABEL.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(SHORT_LABEL.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(PRIVATE_MATERIAL.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(HISTORY.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(FIX_HISTORY.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(REVIEW_ID.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(HISTORY_NARRATION.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(ROADMAP.matcher(ordinary).find(), "must not flag: " + ordinary);
      assertFalse(STRAY_SPACE_BEFORE_PERIOD.matcher(ordinary).find(), "must not flag: " + ordinary);
    }
    // Code samples: the narration rule skips a guide's fenced block and a javadoc's <pre> block,
    // and the stray-period rule reads inline code as one word.
    String narration = "the rule now al" + "so covers the band";
    List<String> guide =
        List.of("Text before.", "```java", "// " + narration, "```", "Version `1.0 " + ".` here.");
    assertNoneFlagged("docs/guide/x.md", guide);
    List<String> javadoc =
        List.of(
            "  /**",
            "   * Use it like this:",
            "   * <pre>{@code",
            "   * // " + narration,
            "   * }</pre>",
            "   * Returns {@code done " + ".} and {@link Foo#bar() bar " + ".} as written.",
            "   */");
    assertNoneFlagged("a/src/main/java/A.java", javadoc);
    // A one-line javadoc and a line comment read inline code as one word, as a javadoc does.
    assertNoneFlagged(
        "a/src/main/java/A.java",
        List.of(
            "  /** Returns {@code done " + ".} as written. */",
            "  /** The lease. */",
            "    // wait ... and retry, then ./gradlew build",
            "    // the cutoff (e.g. a lease) holds"));
    // Reading on into the next line keeps its punctuation, and stops at a code sample.
    assertNoneFlagged(
        "a/src/main/java/A.java",
        List.of("   * The lease is no.", "   * Longer leases renew less."));
    assertNoneFlagged("docs/guide/x.md", List.of("A step is no", "```", "lo" + "nger here", "```"));
    assertNoneFlagged("docs/guide/x.md", List.of("A step is no", "", "lo" + "nger here."));
    // A code line of a java file is not prose; a narration in a test source is allowed.
    String codeLine = "    int x = done " + ". size();";
    assertNoneFlagged("a/src/main/java/A.java", List.of(codeLine));
    String testNarration = "  // Previ" + "ously the poller dropped it.";
    assertNoneFlagged("a/src/test/java/ATest.java", List.of(testNarration));
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
    for (String withoutExtension : List.of(".gitmessage", "NOTICE", "gradle/hooks/pre-commit")) {
      assertTrue(
          files.stream().anyMatch(p -> relative(p).equals(withoutExtension)),
          "a text file without an extension must be scanned: " + withoutExtension);
    }
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
              String rel = root.relativize(file).toString().replace('\\', '/');
              if (attrs.isRegularFile()
                  && isScannedName(file.getFileName().toString())
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
