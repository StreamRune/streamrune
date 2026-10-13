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
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Keeps the statement "a saga's dispatch caused this event" with the writers it has: the main
 * sources of every module may bind the dispatch cause, build one, create a saga origin or take an
 * origin one hop further only in the files listed here.
 *
 * <p><b>Why.</b> {@code EventMetadata.sagaOrigin} and {@code EventMetadata.causationId} are the
 * framework's statements of which saga's dispatch and which event caused an event, and they are
 * worth only as much as their writers are few. The command bus copies both from {@code
 * StreamRuneContext.CAUSE}, which saga dispatch binds for a saga's own commands and {@code
 * StreamRuneContext.continuing} binds for a listener's reaction to a stored event; a request
 * filter, a dead-letter replay or an integration that bound the cause, or built an origin, from a
 * value it read off the wire would let a client make those statements. The compiler cannot stop
 * that — the cause is a public constant and the origin a public record — so this scan does.
 *
 * <p><b>What is scanned.</b> Every {@code .java} file under any module's {@code src/main/java}.
 * Test sources bind causes and build origins freely. A file is matched as a whole, so a shape that
 * the formatter wrapped over two lines is still found. Each shape is refused as a call and as a
 * method reference ({@code SagaOrigin::dispatchedBy}, {@code SagaOrigin::new}, {@code
 * SagaOrigin::nextHop}, {@code DispatchCause::new}, {@code EventMetadata::new}). {@link
 * #noGuideOrMainSourceNamesTheCausationBaggageKey()} also reads every {@code .md} file.
 *
 * <p><b>The allow-lists.</b> One per forbidden shape ({@link #RULES}), each entry with the text
 * that keeps it listed. {@link #theAllowListsHoldNoStaleEntries()} fails an entry whose file is
 * gone or holds that text no more, so a list only shrinks on its own; adding a writer is a reviewed
 * edit of this file.
 *
 * <p>The scanned tree is declared as an input of {@code :streamrune-core:test} in {@code
 * streamrune-core/build.gradle.kts}, so Gradle re-runs this test when a main source of another
 * module changes.
 */
class SagaProvenanceWriterRatchetTest {

  private static final String CONTEXT =
      "streamrune-core/src/main/java/org/streamrune/core/StreamRuneContext.java";
  private static final String ORIGIN =
      "streamrune-core/src/main/java/org/streamrune/core/saga/SagaOrigin.java";
  private static final String SAGA_DISPATCH =
      "streamrune-runtime/src/main/java/org/streamrune/runtime/SagaCommandDispatch.java";
  private static final String STEP_EXECUTOR =
      "streamrune-runtime/src/main/java/org/streamrune/runtime/SagaStepExecutor.java";
  private static final String BUS =
      "streamrune-runtime/src/main/java/org/streamrune/runtime/VirtualThreadCommandBus.java";
  private static final String BUS_CONTRACT =
      "streamrune-test/src/main/java/org/streamrune/test/CommandBusProvenanceContract.java";
  private static final String SAGA_ENVELOPES =
      "streamrune-test/src/main/java/org/streamrune/test/SagaEnvelopes.java";
  private static final String IN_MEMORY_SUBSCRIPTION =
      "streamrune-test/src/main/java/org/streamrune/test/InMemoryEventSubscription.java";

  /**
   * One forbidden shape and the files that may hold it.
   *
   * @param what the shape in words, for the failure message
   * @param forbidden the shape, rejected in every main source outside {@code allowed}
   * @param allowed repository-relative main sources that may hold the shape, each with the text it
   *     must still contain to stay listed
   */
  private record Rule(String what, Pattern forbidden, Map<String, Pattern> allowed) {}

  private static final Pattern QUALIFIED_CAUSE =
      Pattern.compile("StreamRuneContext\\s*\\.\\s*CAUSE\\b");

  /**
   * A reference to the cause's scoped value: qualified, or through a wildcard static import that
   * would hide every later use. Allowed where it is declared (the declaring class also binds it,
   * unqualified, in {@code continuing} and carries it in {@code wrap}), where saga dispatch binds
   * it, where the bus carries it onto its asynchronous thread, and in two places of the test
   * module: the bus contract, which binds a cause to prove that a bus copies it, and the in-memory
   * subscription, which binds it to no cause around a listener that runs on the appending thread.
   */
  private static final Rule CAUSE_REFERENCE =
      new Rule(
          "references StreamRuneContext.CAUSE",
          Pattern.compile(
              QUALIFIED_CAUSE.pattern()
                  + "|import\\s+static\\s+org\\.streamrune\\.core\\.StreamRuneContext\\s*\\.\\s*\\*"),
          Map.of(
              CONTEXT,
              Pattern.compile("ScopedValue<DispatchCause>\\s+CAUSE\\s*="),
              SAGA_DISPATCH,
              QUALIFIED_CAUSE,
              BUS,
              QUALIFIED_CAUSE,
              BUS_CONTRACT,
              QUALIFIED_CAUSE,
              IN_MEMORY_SUBSCRIPTION,
              QUALIFIED_CAUSE));

  private static final Pattern CAUSE_CONSTRUCTOR_CALL =
      Pattern.compile("\\bnew\\s+(?:StreamRuneContext\\s*\\.\\s*)?DispatchCause\\s*\\(");

  /**
   * The dispatch cause's record header, a construction of it, or a reference to its constructor.
   * The declaring file holds the header and the one construction of {@code continuing} ({@link
   * #theContextBuildsADispatchCauseInContinuingOnly()} counts it); saga dispatch and the bus
   * contract hold one each.
   */
  private static final Rule CAUSE_CONSTRUCTION =
      new Rule(
          "declares or constructs a DispatchCause",
          Pattern.compile("\\bDispatchCause\\s*(?:\\(|::\\s*new\\b)"),
          Map.of(
              CONTEXT,
              Pattern.compile("\\brecord\\s+DispatchCause\\s*\\("),
              SAGA_DISPATCH,
              CAUSE_CONSTRUCTOR_CALL,
              BUS_CONTRACT,
              CAUSE_CONSTRUCTOR_CALL));

  /**
   * A call of the origin's constructor or a reference to it: the record itself is the one caller.
   */
  private static final Rule ORIGIN_CONSTRUCTION =
      new Rule(
          "calls or references the SagaOrigin constructor",
          Pattern.compile(
              "\\bnew\\s+(?:[\\w.]+\\.)?SagaOrigin\\s*\\(|\\bSagaOrigin\\s*::\\s*new\\b"),
          Map.of(ORIGIN, Pattern.compile("\\bnew\\s+SagaOrigin\\s*\\(")));

  private static final Pattern ORIGIN_FACTORY_CALL =
      Pattern.compile("SagaOrigin\\s*\\.\\s*dispatchedBy\\s*\\(");

  /**
   * A call of the origin's factory, a reference to it, or a wildcard static import that would hide
   * one. Allowed in the saga step executor, which names the dispatching saga, and in the test
   * module's helper for hand-built saga-caused envelopes.
   */
  private static final Rule ORIGIN_FACTORY =
      new Rule(
          "calls or references SagaOrigin.dispatchedBy",
          Pattern.compile(
              "SagaOrigin\\s*(?:\\.|::)\\s*dispatchedBy\\b"
                  + "|import\\s+static\\s+org\\.streamrune\\.core\\.saga\\.SagaOrigin\\s*\\.\\s*\\*"),
          Map.of(STEP_EXECUTOR, ORIGIN_FACTORY_CALL, SAGA_ENVELOPES, ORIGIN_FACTORY_CALL));

  private static final Pattern NEXT_HOP_CALL = Pattern.compile("\\.\\s*nextHop\\s*\\(");

  /**
   * A declaration of {@code nextHop}, a call of it or a reference to it. The origin declares it and
   * {@code StreamRuneContext.continuing} is its one caller: a second caller would hand out an
   * origin for an event nobody read from the store.
   */
  private static final Rule ORIGIN_NEXT_HOP =
      new Rule(
          "declares, calls or references nextHop",
          Pattern.compile("(?<!#)\\bnextHop\\s*\\(|::\\s*nextHop\\b"),
          Map.of(
              ORIGIN, Pattern.compile("\\bSagaOrigin\\s+nextHop\\s*\\("), CONTEXT, NEXT_HOP_CALL));

  /**
   * A reference to a constructor of the event metadata. The text of a reference does not say which
   * constructor it resolves to, so every one is refused: a reference to the ten-argument
   * constructor would build metadata with an origin without a call the other checks see. No main
   * source holds one.
   */
  private static final Rule METADATA_CONSTRUCTOR_REFERENCE =
      new Rule(
          "references an EventMetadata constructor",
          Pattern.compile("\\bEventMetadata\\s*::\\s*new\\b"),
          Map.of());

  private static final List<Rule> RULES =
      List.of(
          CAUSE_REFERENCE,
          CAUSE_CONSTRUCTION,
          ORIGIN_CONSTRUCTION,
          ORIGIN_FACTORY,
          ORIGIN_NEXT_HOP,
          METADATA_CONSTRUCTOR_REFERENCE);

  @Test
  void theCauseIsBoundOnlyByItsWriters() {
    assertNoViolations(List.of(CAUSE_REFERENCE, CAUSE_CONSTRUCTION));
  }

  @Test
  void originsAreCreatedOnlyByTheirWriters() {
    assertNoViolations(
        List.of(
            ORIGIN_CONSTRUCTION, ORIGIN_FACTORY, ORIGIN_NEXT_HOP, METADATA_CONSTRUCTOR_REFERENCE));
  }

  /**
   * The declaring file is allow-listed as a whole, so the scan above cannot tell its one legitimate
   * construction from a second one. This counts them: the record header, and the construction in
   * {@code continuing}; and one call of {@code nextHop}, in the same method.
   */
  @Test
  void theContextBuildsADispatchCauseInContinuingOnly() {
    String context = read(repositoryRoot().resolve(CONTEXT));

    assertEquals(1, count(CAUSE_CONSTRUCTOR_CALL, context), "constructions of a DispatchCause");
    assertEquals(1, count(NEXT_HOP_CALL, context), "calls of nextHop");
    Matcher continuing =
        Pattern.compile(
                "public static ScopedValue\\.Carrier continuing\\(EventEnvelope cause\\) \\{.*?\n  }\n",
                Pattern.DOTALL)
            .matcher(context);
    assertTrue(continuing.find(), "StreamRuneContext.continuing(EventEnvelope) is declared");
    assertEquals(1, count(CAUSE_CONSTRUCTOR_CALL, continuing.group()));
    assertEquals(1, count(NEXT_HOP_CALL, continuing.group()));
  }

  private static int count(Pattern pattern, String source) {
    int found = 0;
    Matcher matcher = pattern.matcher(source);
    while (matcher.find()) {
      found++;
    }
    return found;
  }

  // A baggage key by which a client claims to name the causing event of its command, and the name
  // of a constant that would make it a framework key. Built from pieces so that this file names
  // neither.
  private static final String CAUSATION_BAGGAGE_KEY = "streamrune." + "causation" + "-id";
  private static final String CAUSATION_BAGGAGE_CONSTANT = "CAUSATION_ID" + "_BAGGAGE_KEY";

  /**
   * The command bus reads no baggage entry into the causation id, so no guide may tell a reader to
   * send one and no main source may hold the key. The one main source that names it is the bus
   * contract of the test module, which binds an entry under it to prove that it decides nothing.
   */
  @Test
  void noGuideOrMainSourceNamesTheCausationBaggageKey() {
    Path root = repositoryRoot();
    List<String> violations = new ArrayList<>();
    List<Path> files = new ArrayList<>(mainSources());
    List<Path> guides = filesMatching(rel -> rel.endsWith(".md"));
    assertTrue(guides.size() > 20, "expected every guide, found " + guides.size());
    files.addAll(guides);
    for (Path file : files) {
      String relative = relative(file);
      String source = read(file);
      if (source.contains(CAUSATION_BAGGAGE_CONSTANT)) {
        violations.add(relative + ": names " + CAUSATION_BAGGAGE_CONSTANT);
      }
      if (source.contains(CAUSATION_BAGGAGE_KEY) && !relative.equals(BUS_CONTRACT)) {
        violations.add(relative + ": names the baggage key " + CAUSATION_BAGGAGE_KEY);
      }
    }
    assertTrue(
        read(root.resolve(BUS_CONTRACT)).contains(CAUSATION_BAGGAGE_KEY),
        "the bus contract holds the key no more; drop its exemption");
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " file(s) name the causation baggage key. The command bus takes the causation id"
              + " from StreamRuneContext.CAUSE and from nowhere else; a baggage entry under that"
              + " key is ordinary baggage. Offenders:\n  "
              + String.join("\n  ", violations));
    }
  }

  @Test
  void theScanRecognisesEveryShapeItForbids() {
    for (String shape :
        List.of(
            "ScopedValue.where(StreamRuneContext.CAUSE, cause)",
            "StreamRuneContext.CAUSE.isBound();",
            "carrier.where(StreamRuneContext\n        .CAUSE, cause)",
            "org.streamrune.core.StreamRuneContext.CAUSE.get()",
            "import static org.streamrune.core.StreamRuneContext.CAUSE;",
            "import static org.streamrune.core.StreamRuneContext.*;")) {
      assertTrue(CAUSE_REFERENCE.forbidden().matcher(shape).find(), "must reject: " + shape);
    }
    for (String shape :
        List.of(
            "new StreamRuneContext.DispatchCause(null, origin)",
            "new DispatchCause(causationId, origin)",
            "new StreamRuneContext.DispatchCause\n    (causationId, null)",
            "public record DispatchCause(CausationId causationId, SagaOrigin sagaOrigin) {",
            "BiFunction<CausationId, SagaOrigin, Object> make = DispatchCause::new;",
            "make = StreamRuneContext.DispatchCause :: new;")) {
      assertTrue(CAUSE_CONSTRUCTION.forbidden().matcher(shape).find(), "must reject: " + shape);
    }
    for (String shape :
        List.of(
            "new SagaOrigin(type, id, 0)",
            "return new  SagaOrigin\n      (type, id, 3);",
            "new org.streamrune.core.saga.SagaOrigin(type, id, 1)",
            "Maker make = SagaOrigin::new;",
            "make = org.streamrune.core.saga.SagaOrigin\n        ::new;")) {
      assertTrue(ORIGIN_CONSTRUCTION.forbidden().matcher(shape).find(), "must reject: " + shape);
    }
    for (String shape :
        List.of(
            "SagaOrigin.dispatchedBy(type, id)",
            "SagaOrigin\n        .dispatchedBy(type, id)",
            "org.streamrune.core.saga.SagaOrigin.dispatchedBy(type, id)",
            "import static org.streamrune.core.saga.SagaOrigin.dispatchedBy;",
            "import static org.streamrune.core.saga.SagaOrigin.*;",
            "BiFunction<SagaType, SagaId, SagaOrigin> name = SagaOrigin::dispatchedBy;",
            "name = SagaOrigin :: dispatchedBy;")) {
      assertTrue(ORIGIN_FACTORY.forbidden().matcher(shape).find(), "must reject: " + shape);
    }
    for (String shape :
        List.of(
            "origin.nextHop()",
            "metadata.sagaOrigin()\n        .nextHop ()",
            "UnaryOperator<SagaOrigin> further = SagaOrigin::nextHop;",
            "further = origin :: nextHop;",
            "public SagaOrigin nextHop() {")) {
      assertTrue(ORIGIN_NEXT_HOP.forbidden().matcher(shape).find(), "must reject: " + shape);
    }
    for (String shape :
        List.of(
            "TenArguments make = EventMetadata::new;",
            "make = org.streamrune.core.EventMetadata :: new;")) {
      assertTrue(
          METADATA_CONSTRUCTOR_REFERENCE.forbidden().matcher(shape).find(),
          "must reject: " + shape);
    }
    for (String ordinary :
        List.of(
            "StreamRuneContext.DispatchCause cause = StreamRuneContext.cause();",
            "SagaOrigin origin = cause == null ? null : cause.sagaOrigin();",
            "{@link StreamRuneContext#CAUSE}",
            "{@link SagaOrigin#dispatchedBy}",
            "import org.streamrune.core.saga.SagaOrigin;",
            "public record SagaOrigin(SagaType sagaType, SagaId sagaId, int hop) {",
            "public static SagaOrigin dispatchedBy(SagaType sagaType, SagaId sagaId) {",
            "metadata.sagaOrigin()",
            "new SagaOriginReader(type)",
            "SagaOrigin::sagaId",
            "DispatchCause::causationId",
            "EventMetadata::eventId",
            "new EventMetadata(eventId, commandId, null, null, correlationId, null, null, now)",
            "{@link SagaOrigin#nextHop()}",
            "int nextHopCount = 0;",
            "StreamRuneContext.continuing(event).run(task)",
            "StreamRuneContext.CAUSES")) {
      for (Rule rule : RULES) {
        assertFalse(rule.forbidden().matcher(ordinary).find(), "must not flag: " + ordinary);
      }
    }
  }

  @Test
  void theAllowListsHoldNoStaleEntries() {
    Path root = repositoryRoot();
    for (Rule rule : RULES) {
      for (Map.Entry<String, Pattern> allowed : rule.allowed().entrySet()) {
        String entry = allowed.getKey();
        assertTrue(
            entry.contains("/src/main/java/") && entry.endsWith(".java"),
            "the scan covers main sources only: " + entry);
        Path file = root.resolve(entry);
        assertTrue(Files.isRegularFile(file), "allow-list entry is gone: " + entry);
        assertTrue(
            allowed.getValue().matcher(read(file)).find(),
            "allow-list entry holds the shape no more (" + rule.what() + "); remove it: " + entry);
      }
    }
  }

  @Test
  void aMentionInJavadocKeepsNoEntryListed() {
    for (Pattern evidence : CAUSE_REFERENCE.allowed().values()) {
      assertFalse(evidence.matcher(" * carries {@link StreamRuneContext#CAUSE} across").find());
      assertFalse(evidence.matcher(" * the bound {@link #CAUSE} and nothing else").find());
    }
    for (Pattern evidence : ORIGIN_FACTORY.allowed().values()) {
      assertFalse(evidence.matcher(" * see {@link SagaOrigin#dispatchedBy}").find());
    }
    for (Pattern evidence : ORIGIN_NEXT_HOP.allowed().values()) {
      assertFalse(evidence.matcher(" * one hop further ({@link SagaOrigin#nextHop()})").find());
    }
  }

  @Test
  void theScanSeesTheMainSourcesOfEveryModule() {
    // Guards the scan itself: a walk that silently finds nothing would pass forever. The number is
    // a floor, so ordinary growth never trips it.
    List<String> scanned =
        mainSources().stream().map(SagaProvenanceWriterRatchetTest::relative).toList();
    assertTrue(
        scanned.size() > 300, "expected every module's main sources, found " + scanned.size());
    for (Rule rule : RULES) {
      for (String entry : rule.allowed().keySet()) {
        assertTrue(scanned.contains(entry), "the scan must reach " + entry);
      }
    }
    assertTrue(
        scanned.stream().noneMatch(path -> path.contains("/src/test/")),
        "test sources are not scanned");
    assertEquals(scanned.size(), Set.copyOf(scanned).size(), "a file is scanned once");
  }

  private static void assertNoViolations(List<Rule> rules) {
    List<String> violations = new ArrayList<>();
    for (Path file : mainSources()) {
      String relative = relative(file);
      String source = read(file);
      for (Rule rule : rules) {
        if (rule.allowed().containsKey(relative)) {
          continue;
        }
        Matcher matcher = rule.forbidden().matcher(source);
        while (matcher.find()) {
          violations.add(relative + ":" + lineOf(source, matcher.start()) + ": " + rule.what());
        }
      }
    }
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " place(s) outside the allow-list touch saga provenance. Only saga dispatch and"
              + " StreamRuneContext.continuing bind the dispatch cause, only the saga step"
              + " executor names a dispatching saga and only continuing takes an origin one hop"
              + " further; a command bus reads the cause through StreamRuneContext.cause(), and a"
              + " listener that reacts to an event dispatches inside"
              + " StreamRuneContext.continuing(event). Offenders:\n  "
              + String.join("\n  ", violations));
    }
  }

  private static int lineOf(String source, int offset) {
    int line = 1;
    for (int i = 0; i < offset; i++) {
      if (source.charAt(i) == '\n') {
        line++;
      }
    }
    return line;
  }

  private static String relative(Path file) {
    return repositoryRoot().relativize(file).toString().replace('\\', '/');
  }

  private static String read(Path file) {
    try {
      return Files.readString(file, StandardCharsets.UTF_8);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** Directory names pruned wherever they occur: build output, VCS, tooling, nested checkouts. */
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
          "scratchpad",
          ".claude");

  /**
   * Every {@code .java} file under any module's {@code src/main/java}, sorted. Excluded subtrees
   * are pruned rather than walked and filtered, and an entry that vanishes mid-walk is skipped:
   * sibling tasks of a parallel build create and delete files under {@code build/} all the time.
   */
  private static List<Path> mainSources() {
    return filesMatching(rel -> rel.endsWith(".java") && rel.contains("/src/main/java/"));
  }

  /**
   * Every regular file whose repository-relative path {@code wanted} accepts, sorted, with the same
   * pruning as {@link #mainSources()}.
   */
  private static List<Path> filesMatching(java.util.function.Predicate<String> wanted) {
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
              return PRUNED_DIRECTORY_NAMES.contains(dir.getFileName().toString())
                  ? FileVisitResult.SKIP_SUBTREE
                  : FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              String rel = root.relativize(file).toString().replace('\\', '/');
              if (attrs.isRegularFile() && wanted.test(rel)) {
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
