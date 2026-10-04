package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The enforcement half of the log-sink policy described on {@link LogSanitizer}.
 *
 * <p><b>Why this test exists at all.</b> Two independent log-forging gaps reached this branch, and
 * both had the same cause: sanitizing was a helper a call site had to remember to call, after first
 * judging whether that particular value was attacker-influenced. {@code SagaTimeoutRunner}
 * interpolated a stored {@code sagaId} raw into a WARNING; the three request filters sanitized the
 * {@code X-User-Id} header and left the authenticated principal name raw in the very same
 * statement. Fixing those two sites would have left the third one to be found by the next audit. A
 * rule that depends on remembering is not a rule, so this test is the rule: it reads every
 * production source file in the repository and fails the build when an identifier reaches a log
 * record or a thread name unsanitized.
 *
 * <p><b>Repo-wide, deliberately.</b> The policy is repo-wide, so its inventory lives in one place
 * rather than being copied into eight per-module architecture tests that would drift apart exactly
 * the way the two sanitizer implementations did. {@code streamrune-core} hosts it because the
 * sanitizer lives here. The consequence is intentional: an unsanitized sink added in {@code
 * streamrune-runtime} fails a core test.
 *
 * <p><b>"runs on every build" is a claim about Gradle, and it was false.</b> A test that reads
 * files outside its own module is invisible to up-to-date checking: none of {@code
 * :streamrune-core:test}'s declared inputs mentioned {@code streamrune-runtime}, so with {@code
 * org.gradle.caching=true} and a CI cache that persists between runs, the one build this ratchet
 * exists for — someone adding an unsanitized sink in ANOTHER module — was precisely the build on
 * which it went UP-TO-DATE/FROM-CACHE and never executed. Green, with the violation in the tree.
 * {@code streamrune-core/build.gradle.kts} now declares the scanned tree as an input of the test
 * task, and {@link #everyScannedSourceIsDeclaredAsAGradleInputOfThisTask()} keeps that declaration
 * from drifting away from {@link #productionSources()} — silent under-declaration is the same
 * defect wearing a different hat.
 *
 * <p><b>The two accepted forms.</b> {@code LogSanitizer.sanitizeForLog(x)} for an identifier, and
 * {@code x.redacted()} for a {@link SubjectId}, which is potentially personal data and needs a hash
 * rather than a control-stripped copy. There is deliberately no exemption list: an exemption list
 * is a place to record a trust judgement, and per-site trust judgements are the defect.
 *
 * <p><b>What it cannot see.</b> It is a source-text rule, not a dataflow analysis. It recognises
 * identifiers by the framework's own vocabulary — a lowerCamel name matching one of the id types,
 * or a variable declared with one of those types in the same file, which is what catches {@code
 * UserId principal}. A value laundered through an unrelated {@code String} local, or embedded in an
 * exception message that some other class later logs, is outside its reach; those sinks are covered
 * by review and by keeping exception messages log-safe where the framework logs them itself (see
 * {@code SseEventPublisher.SlowConsumerException}).
 */
class LogSinkPolicyRatchetTest {

  /**
   * System property carrying the path of the file listing every source Gradle declared as an input
   * of this test task. Set by {@code streamrune-core/build.gradle.kts}.
   */
  private static final String DECLARED_SOURCES_PROPERTY = "streamrune.logsink.declared-sources";

  /** Identifier value types whose rendered form must pass through the sanitizer. */
  private static final List<String> ID_TYPES =
      List.of(
          "StreamId",
          "AggregateType",
          "SagaId",
          "AggregateId",
          "CommandId",
          "UserId",
          "CorrelationId",
          "TraceId",
          "EventId",
          "SubjectId",
          "IdempotencyKey");

  /**
   * The lowerCamel spellings of {@link #ID_TYPES}. Matched case-sensitively so a type reference
   * ({@code SagaId.of(...)}) is not mistaken for a value rendering, while an accessor call ({@code
   * entry.commandId()}) and a local ({@code sagaId}) both are.
   */
  private static final Pattern ID_NAMES =
      Pattern.compile(
          "\\b(?:"
              + String.join(
                  "|",
                  ID_TYPES.stream()
                      .map(t -> Character.toLowerCase(t.charAt(0)) + t.substring(1))
                      .toList())
              + ")\\b");

  private static final Pattern DECLARED_WITH_ID_TYPE =
      Pattern.compile("\\b(?:" + String.join("|", ID_TYPES) + ")\\s+(\\w+)\\s*[),;=:]");

  private static final Pattern LOG_CALL =
      Pattern.compile(
          "\\b(?:LOG|log|LOGGER|logger)\\s*\\.\\s*(?:log|trace|debug|info|warn|error)\\s*\\(");

  /** {@code Thread.ofVirtual().name(...)} / {@code new Thread(r, ...)} — a {@code %thread} sink. */
  private static final Pattern THREAD_CONSTRUCTION =
      Pattern.compile("Thread\\.(?:ofVirtual|ofPlatform)\\(\\)|new Thread\\(");

  /** The name-carrying call within a thread construction chain. */
  private static final Pattern THREAD_NAME_CALL = Pattern.compile("\\.name\\(|new Thread\\(");

  private static final Pattern SANITIZE_CALL = Pattern.compile("sanitizeForLog\\s*\\(");
  private static final Pattern REDACTED =
      Pattern.compile("\\b\\w+(?:\\.\\w+\\(\\))*\\.redacted\\(\\)");
  private static final Pattern NULL_COMPARISON =
      Pattern.compile("\\b\\w+\\s*[!=]=\\s*null|null\\s*[!=]=\\s*\\w+");

  @Test
  void everyIdentifierRenderedIntoALogRecordIsSanitized() {
    List<String> violations = new ArrayList<>();
    for (Path file : productionSources()) {
      violations.addAll(scan(file, LOG_CALL, "log record"));
    }
    assertNoViolations(
        violations,
        "identifier rendered into a log record without LogSanitizer.sanitizeForLog(...)");
  }

  @Test
  void everyIdentifierRenderedIntoAThreadNameIsSanitized() {
    // A thread name is rendered by %thread on EVERY line that thread emits, so no argument-level
    // sanitization at the log sites can reach it. SseEventPublisher is the only thread name in the
    // framework built from request input; the rest are registration names, saga types, class names
    // or generated UUIDs, and this test keeps it that way.
    List<String> violations = new ArrayList<>();
    for (Path file : productionSources()) {
      violations.addAll(scanThreadNames(file));
    }
    assertNoViolations(violations, "identifier rendered into a thread name without sanitization");
  }

  @Test
  void theScannerActuallySeesTheProductionTree() {
    // Guards the rule itself: a scanner that silently finds nothing would pass both tests above
    // forever. These counts are floors, not exact expectations, so ordinary growth never trips
    // them.
    List<Path> sources = productionSources();
    assertTrue(sources.size() > 300, "expected the whole production tree, found " + sources.size());
    long sanitized =
        sources.stream()
            .filter(p -> SANITIZE_CALL.matcher(String.join("\n", readSource(p))).find())
            .count();
    assertTrue(
        sanitized > 10, "expected the sanitizer to be in use across modules, found " + sanitized);
  }

  /**
   * Guards the guard's TRIGGER, the way {@link #theScannerActuallySeesTheProductionTree()} guards
   * its reach. The two tests above only mean something on a build that runs them, and Gradle
   * decides that from the task's declared inputs. {@code streamrune-core/build.gradle.kts} declares
   * the scanned tree with a pair of globs; this test proves those globs still describe the same
   * file set {@link #productionSources()} walks, so a module added at a nesting depth they miss
   * fails HERE instead of quietly dropping out of every future scan.
   *
   * <p>Fails rather than skips when the property is absent: "the wiring disappeared and nothing
   * said so" is the defect this whole guard exists to catch, and a skipped assumption is exactly
   * that. Running the class from an IDE therefore shows one red test with this explanation; the
   * other four still work there.
   */
  @Test
  void everyScannedSourceIsDeclaredAsAGradleInputOfThisTask() {
    String manifestPath = System.getProperty(DECLARED_SOURCES_PROPERTY);
    if (manifestPath == null) {
      fail(
          "-D"
              + DECLARED_SOURCES_PROPERTY
              + " is not set, so the declared-inputs check cannot run. Gradle's test task supplies"
              + " it (streamrune-core/build.gradle.kts); run `./gradlew :streamrune-core:test`."
              + " If that wiring was removed, the ratchet stops re-running when another module"
              + " changes — restore it rather than deleting this test.");
    }
    Path root = repositoryRoot();
    Set<String> scanned = new java.util.TreeSet<>();
    for (Path source : productionSources()) {
      scanned.add(root.relativize(source).toString().replace('\\', '/'));
    }
    Set<String> declared = new java.util.TreeSet<>();
    for (String line : readLines(Path.of(manifestPath))) {
      if (!line.isBlank()) {
        declared.add(line.strip());
      }
    }

    Set<String> scannedButNotDeclared = new java.util.TreeSet<>(scanned);
    scannedButNotDeclared.removeAll(declared);
    Set<String> declaredButNotScanned = new java.util.TreeSet<>(declared);
    declaredButNotScanned.removeAll(scanned);
    if (!scannedButNotDeclared.isEmpty() || !declaredButNotScanned.isEmpty()) {
      fail(
          "the log-sink ratchet's Gradle inputs no longer match what it scans"
              + ". Fix the include/exclude globs in"
              + " streamrune-core/build.gradle.kts to mirror productionSources().\n  scanned but"
              + " NOT declared as a task input (these changes would not re-run this test): "
              + scannedButNotDeclared
              + "\n  declared but not scanned (harmless, but the globs have drifted): "
              + declaredButNotScanned);
    }
  }

  private static void assertNoViolations(List<String> violations, String what) {
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " "
              + what
              + ". Wrap the value in LogSanitizer.sanitizeForLog(...), or"
              + " use SubjectId.redacted() when it may be personal data:\n  "
              + String.join("\n  ", violations));
    }
  }

  /**
   * Reports every occurrence of an identifier inside a statement introduced by {@code opener} that
   * is not covered by an accepted form.
   */
  private static List<String> scan(Path file, Pattern opener, String sinkKind) {
    List<String> lines = readSource(file);
    Set<String> idVars = declaredIdVariables(lines);
    List<String> violations = new ArrayList<>();
    int i = 0;
    while (i < lines.size()) {
      String raw = lines.get(i);
      String trimmed = raw.stripLeading();
      if (trimmed.startsWith("*") || trimmed.startsWith("//") || trimmed.startsWith("/*")) {
        i++;
        continue;
      }
      if (!opener.matcher(raw).find()) {
        i++;
        continue;
      }
      int end = statementEnd(lines, i);
      String statement = String.join("\n", lines.subList(i, end + 1));
      String code = stripStringLiterals(statement);
      violations.addAll(subjectIdViolations(file, code, i + 1));
      for (int at : unsanitizedIdOffsets(code, idVars)) {
        violations.add(
            file.getFileName()
                + ":"
                + (i + 1)
                + " ["
                + sinkKind
                + "] "
                + code.substring(Math.max(0, at - 40), Math.min(code.length(), at + 40))
                    .replace("\n", " ")
                    .trim());
      }
      i = end + 1;
    }
    return violations;
  }

  private static List<Integer> unsanitizedIdOffsets(String code, Set<String> idVars) {
    List<int[]> covered = new ArrayList<>();
    Matcher sanitize = SANITIZE_CALL.matcher(code);
    while (sanitize.find()) {
      covered.add(new int[] {sanitize.start(), closingParen(code, sanitize.end() - 1)});
    }
    Matcher redacted = REDACTED.matcher(code);
    while (redacted.find()) {
      covered.add(new int[] {redacted.start(), redacted.end()});
    }
    Matcher nullCheck = NULL_COMPARISON.matcher(code);
    while (nullCheck.find()) {
      // `sagaId == null ? "<null>" : sanitizeForLog(sagaId.value())` — the guard is a comparison,
      // not a rendering.
      covered.add(new int[] {nullCheck.start(), nullCheck.end()});
    }

    Set<Integer> hits = new java.util.TreeSet<>();
    List<Matcher> matchers = new ArrayList<>();
    matchers.add(ID_NAMES.matcher(code));
    if (!idVars.isEmpty()) {
      matchers.add(Pattern.compile("\\b(?:" + String.join("|", idVars) + ")\\b").matcher(code));
    }
    for (Matcher m : matchers) {
      while (m.find()) {
        int at = m.start();
        if (covered.stream().noneMatch(span -> span[0] <= at && at < span[1])) {
          hits.add(at);
        }
      }
    }
    return new ArrayList<>(hits);
  }

  /**
   * A {@link SubjectId} is the one identifier the sanitizer must NOT be used on: it may itself be
   * personal data (an email address, a username), so a control-stripped copy still writes the PII
   * the erasure flow exists to remove. Only {@link SubjectId#redacted()} counts.
   */
  private static List<String> subjectIdViolations(Path file, String code, int line) {
    List<String> out = new ArrayList<>();
    Matcher m = Pattern.compile("\\bsubjectId\\b").matcher(code);
    Matcher redacted = REDACTED.matcher(code);
    List<int[]> ok = new ArrayList<>();
    while (redacted.find()) {
      ok.add(new int[] {redacted.start(), redacted.end()});
    }
    while (m.find()) {
      int at = m.start();
      if (ok.stream().noneMatch(span -> span[0] <= at && at < span[1])) {
        out.add(file.getFileName() + ":" + line + " [log record] subjectId must use redacted()");
      }
    }
    return out;
  }

  /**
   * Reports every identifier rendered into a thread name. Scans only the {@code .name(...)} call of
   * a thread-construction chain — not the {@code .start(lambda)} body that follows it, whose
   * contents are ordinary code and not a name.
   */
  private static List<String> scanThreadNames(Path file) {
    List<String> lines = readSource(file);
    Set<String> idVars = declaredIdVariables(lines);
    List<String> violations = new ArrayList<>();
    for (int i = 0; i < lines.size(); i++) {
      String trimmed = lines.get(i).stripLeading();
      if (trimmed.startsWith("*") || trimmed.startsWith("//") || trimmed.startsWith("/*")) {
        continue;
      }
      if (!THREAD_CONSTRUCTION.matcher(lines.get(i)).find()) {
        continue;
      }
      String chain = String.join("\n", lines.subList(i, Math.min(lines.size(), i + 4)));
      Matcher nameCall = THREAD_NAME_CALL.matcher(chain);
      if (!nameCall.find()) {
        continue;
      }
      int open = chain.indexOf('(', nameCall.start());
      String nameExpression =
          stripStringLiterals(chain.substring(open, closingParen(chain, open) + 1));
      for (int at : unsanitizedIdOffsets(nameExpression, idVars)) {
        violations.add(
            file.getFileName()
                + ":"
                + (i + 1)
                + " [thread name] "
                + nameExpression.replace("\n", " ").trim()
                + " (offset "
                + at
                + ")");
      }
    }
    return violations;
  }

  /** Names of locals, fields and parameters declared with one of {@link #ID_TYPES} in this file. */
  private static Set<String> declaredIdVariables(List<String> lines) {
    Set<String> names = new LinkedHashSet<>();
    Matcher m = DECLARED_WITH_ID_TYPE.matcher(String.join("\n", lines));
    while (m.find()) {
      names.add(Pattern.quote(m.group(1)));
    }
    return names;
  }

  /** Index of the last line of the statement starting at {@code start} (balanced parentheses). */
  private static int statementEnd(List<String> lines, int start) {
    int depth = 0;
    boolean opened = false;
    for (int j = start; j < lines.size() && j < start + 30; j++) {
      for (char c : lines.get(j).toCharArray()) {
        if (c == '(') {
          depth++;
          opened = true;
        } else if (c == ')') {
          depth--;
        }
      }
      if (opened && depth <= 0) {
        return j;
      }
    }
    return start;
  }

  private static int closingParen(String s, int openAt) {
    int depth = 0;
    for (int i = openAt; i < s.length(); i++) {
      if (s.charAt(i) == '(') {
        depth++;
      } else if (s.charAt(i) == ')') {
        depth--;
        if (depth == 0) {
          return i;
        }
      }
    }
    return s.length();
  }

  /** Blanks out string literals so message text never counts as a rendered identifier. */
  private static String stripStringLiterals(String s) {
    StringBuilder out = new StringBuilder(s.length());
    int i = 0;
    while (i < s.length()) {
      char c = s.charAt(i);
      if (c == '"') {
        out.append("\"\"");
        i++;
        while (i < s.length() && s.charAt(i) != '"') {
          i += s.charAt(i) == '\\' ? 2 : 1;
        }
        i++;
      } else if (c == '\'') {
        out.append("''");
        i++;
        while (i < s.length() && s.charAt(i) != '\'') {
          i += s.charAt(i) == '\\' ? 2 : 1;
        }
        i++;
      } else {
        out.append(c);
        i++;
      }
    }
    return out.toString();
  }

  static List<Path> productionSources() {
    return productionSources(repositoryRoot());
  }

  /** Every production source under {@code root}, sorted; see {@link ProductionSourceWalk}. */
  private static List<Path> productionSources(Path root) {
    ProductionSourceWalk walk = new ProductionSourceWalk(root);
    try {
      Files.walkFileTree(root, walk);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return walk.sources();
  }

  /**
   * Collects what the ratchet scans from a tree that other tasks of the same build are rewriting
   * while it is walked.
   *
   * <p>{@code Files.walk} stats every entry it lists and turns one that disappeared in between into
   * an {@code UncheckedIOException(NoSuchFileException)} out of the stream, which failed the whole
   * ratchet. In a parallel build that is not hypothetical, and it did fail that way: the old walk
   * descended into every module's {@code build/} and every stale checkout under {@code .worktrees/}
   * and only filtered them out afterwards, so it stat'ed the directories where sibling tasks create
   * and delete files all the time (spotless's {@code build/spotless-clean} copies, Gradle's
   * in-progress test-result spools) — files it would have thrown away anyway. {@code spotlessApply}
   * also replaces a formatted source by copying the new one over it, and {@code Files.copy} with
   * {@code REPLACE_EXISTING} unlinks the target first, so a REAL source can be missing for a moment
   * too.
   *
   * <p>So the excluded subtrees are pruned instead of walked and filtered (the same exclusions, and
   * the ones {@code streamrune-core/build.gradle.kts} declares), and an entry that vanished after
   * it was listed is skipped — unless it already exists again, in which case it was replaced rather
   * than removed and is walked once more, so it is still scanned. Everything else stays strict: any
   * other {@link IOException}, and a vanished ROOT, fail the test, because a ratchet that quietly
   * scans less than the tree is the failure the floors in {@link
   * #theScannerActuallySeesTheProductionTree()} exist to catch.
   */
  private static class ProductionSourceWalk extends SimpleFileVisitor<Path> {

    /** Pruned at any depth: build output, and the stale nested checkouts of the old codebase. */
    private static final Set<String> PRUNED_ANYWHERE = Set.of("build", ".worktrees");

    /** Pruned directly under the root: repository and Gradle metadata, never production source. */
    private static final Set<String> PRUNED_AT_ROOT = Set.of(".git", ".gradle");

    private final Path root;
    private final Set<Path> sources = new TreeSet<>();

    ProductionSourceWalk(Path root) {
      this.root = root;
    }

    List<Path> sources() {
      return List.copyOf(sources);
    }

    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
      if (dir.equals(root)) {
        return FileVisitResult.CONTINUE;
      }
      String name = dir.getFileName().toString();
      boolean pruned =
          PRUNED_ANYWHERE.contains(name)
              || (root.equals(dir.getParent()) && PRUNED_AT_ROOT.contains(name));
      return pruned ? FileVisitResult.SKIP_SUBTREE : FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      String path = file.toString();
      // Files.isRegularFile follows a symbolic link, as the Files.walk filter it replaces did. A
      // file the walk saw as regular is kept even if it is missing for a moment now (spotlessApply
      // rewrites by delete-then-copy): readSource decides at read time, so it is never skipped.
      if (path.endsWith(".java")
          && path.contains("/src/main/java/")
          && (attrs.isRegularFile() || Files.isRegularFile(file))) {
        sources.add(file);
      }
      return FileVisitResult.CONTINUE;
    }

    /** Listed, then gone before its attributes could be read, or its directory opened. */
    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      return skipIfVanished(file, exc);
    }

    /** A directory whose listing failed part-way through. */
    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      return exc == null ? FileVisitResult.CONTINUE : skipIfVanished(dir, exc);
    }

    private FileVisitResult skipIfVanished(Path path, IOException exc) throws IOException {
      if (!(exc instanceof NoSuchFileException) || path.equals(root)) {
        throw exc;
      }
      if (Files.exists(path, LinkOption.NOFOLLOW_LINKS)) {
        // Replaced, not removed: it exists, so it is scanned. The sorted set absorbs any entry of a
        // part-listed directory that the first pass already collected.
        Files.walkFileTree(path, this);
      }
      return FileVisitResult.CONTINUE;
    }
  }

  /**
   * Walks up from the test's working directory (Gradle sets it to the module directory) to the
   * directory holding {@code settings.gradle.kts}. Fails loudly rather than degrading to a no-op
   * scan — a ratchet that silently stops ratcheting is worse than none.
   */
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
            + "); the log-sink ratchet cannot run");
  }

  private static List<String> readLines(Path p) {
    try {
      return Files.readAllLines(p);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reads a source {@link #productionSources()} listed: it can vanish between being listed and
   * being read for the same reason an entry can vanish mid-walk ({@link ProductionSourceWalk}). One
   * that exists again was replaced and is read strictly; one that is still gone has nothing left to
   * scan.
   */
  static List<String> readSource(Path p) {
    try {
      return Files.readAllLines(p);
    } catch (NoSuchFileException _) {
      return Files.exists(p) ? readLines(p) : List.of();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // ---- The walk itself, against a throwaway tree rather than the repository ----

  @Test
  void theSourceWalkPrunesBuildOutputStaleCheckoutsAndRepositoryMetadata(@TempDir Path root)
      throws IOException {
    Path kept = writeSource(root, "m/src/main/java/p/Kept.java");
    Path nested = writeSource(root, "outer/inner/src/main/java/p/Nested.java");
    writeSource(root, "m/build/generated/src/main/java/p/Generated.java");
    writeSource(root, ".worktrees/stale/m/src/main/java/p/Stale.java");
    writeSource(root, ".git/src/main/java/p/Meta.java");
    writeSource(root, ".gradle/src/main/java/p/Cache.java");
    writeSource(root, "m/src/test/java/p/SomeTest.java");
    writeSource(root, "m/src/main/java/p/notes.txt");

    assertEquals(List.of(kept, nested), productionSources(root));
  }

  @Test
  void theSourceWalkSkipsEntriesThatVanishAfterTheyWereListed(@TempDir Path root)
      throws IOException {
    Path pkg = root.resolve("m/src/main/java/p");
    for (String name : List.of("A", "B", "C", "D")) {
      writeSource(root, "m/src/main/java/p/" + name + ".java");
    }
    writeSource(root, "m/src/main/java/p/sub/E.java");
    writeSource(root, "m/src/main/java/p/sub/deeper/F.java");
    // The first time the walk reaches an entry of pkg, every OTHER entry of pkg (files and the sub
    // tree alike) is deleted. The directory listing the walker has already read still names them,
    // so each one fails its stat with NoSuchFileException: what a sibling task's temporary file
    // did to the old Files.walk, made deterministic.
    List<Path> vanished = new ArrayList<>();
    ProductionSourceWalk walk =
        new ProductionSourceWalk(root) {
          private boolean deleted;

          @Override
          public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
            deleteSiblingsOnce(dir);
            return super.preVisitDirectory(dir, attrs);
          }

          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
            deleteSiblingsOnce(file);
            return super.visitFile(file, attrs);
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            vanished.add(file);
            return super.visitFileFailed(file, exc);
          }

          private void deleteSiblingsOnce(Path entry) {
            if (!deleted && pkg.equals(entry.getParent())) {
              deleted = true;
              deleteEverythingIn(pkg, entry);
            }
          }
        };

    Files.walkFileTree(root, walk);

    assertFalse(
        vanished.isEmpty(),
        "the race was not reproduced: the walker did not list an entry deleted mid-walk, so this"
            + " test no longer exercises the vanished-entry path");
    List<Path> stillThere = sourcesOnAQuietTree(root);
    assertFalse(stillThere.isEmpty(), "the entry that triggered the deletion survives it");
    assertEquals(stillThere, walk.sources());
  }

  @Test
  void theSourceWalkStillScansAnEntryThatWasReplacedRatherThanRemoved(@TempDir Path root)
      throws IOException {
    // spotlessApply's delete-then-copy: the stat failed, but by the time the failure is handled the
    // file is back. It exists, so it is scanned.
    Path replaced = writeSource(root, "m/src/main/java/p/Replaced.java");
    Path replacedDir = writeSource(root, "n/src/main/java/q/Inside.java").getParent();
    ProductionSourceWalk walk = new ProductionSourceWalk(root);

    assertEquals(
        FileVisitResult.CONTINUE,
        walk.visitFileFailed(replaced, new NoSuchFileException(replaced.toString())));
    assertEquals(
        FileVisitResult.CONTINUE,
        walk.postVisitDirectory(replacedDir, new NoSuchFileException(replacedDir.toString())));

    assertEquals(List.of(replaced, replacedDir.resolve("Inside.java")), walk.sources());
  }

  @Test
  void theSourceWalkStaysStrictForEverythingButAVanishedEntry(@TempDir Path root)
      throws IOException {
    Path existing = writeSource(root, "m/src/main/java/p/Unreadable.java");
    Path dir = existing.getParent();
    ProductionSourceWalk walk = new ProductionSourceWalk(root);

    AccessDeniedException denied = new AccessDeniedException(existing.toString());
    assertSame(
        denied,
        assertThrows(AccessDeniedException.class, () -> walk.visitFileFailed(existing, denied)));
    IOException broken = new IOException("directory iteration failed");
    assertSame(broken, assertThrows(IOException.class, () -> walk.postVisitDirectory(dir, broken)));
    assertTrue(walk.sources().isEmpty());

    // A vanished ROOT is not a vanished entry: skipping it would scan nothing and pass.
    Path missingRoot = root.resolve("gone");
    UncheckedIOException rootGone =
        assertThrows(UncheckedIOException.class, () -> productionSources(missingRoot));
    assertTrue(rootGone.getCause() instanceof NoSuchFileException, rootGone.toString());
  }

  @Test
  void aSourceThatVanishesBeforeItIsReadHasNothingLeftToScan(@TempDir Path root)
      throws IOException {
    Path present = writeSource(root, "m/src/main/java/p/Present.java");

    assertEquals(List.of(), readSource(root.resolve("m/src/main/java/p/Gone.java")));
    assertEquals(List.of("class X {}"), readSource(present));
  }

  private static Path writeSource(Path root, String relative) throws IOException {
    Path file = root.resolve(relative);
    Files.createDirectories(file.getParent());
    Files.writeString(file, "class X {}\n");
    return file;
  }

  /** Deletes every entry of {@code dir} except {@code keep}, recursively. */
  private static void deleteEverythingIn(Path dir, Path keep) {
    try (Stream<Path> entries = Files.list(dir)) {
      for (Path entry : entries.filter(e -> !e.equals(keep)).toList()) {
        try (Stream<Path> tree = Files.walk(entry)) {
          for (Path p : tree.sorted(Comparator.reverseOrder()).toList()) {
            Files.delete(p);
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** The {@code src/main/java} sources of a tree nothing else is touching. */
  private static List<Path> sourcesOnAQuietTree(Path root) throws IOException {
    try (Stream<Path> walk = Files.walk(root)) {
      return walk.filter(Files::isRegularFile)
          .filter(p -> p.toString().endsWith(".java"))
          .filter(p -> p.toString().contains("/src/main/java/"))
          .sorted()
          .toList();
    }
  }
}
