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
 * <p><b>Inside a listed file.</b> An entry admits a whole file, so three counts look inside the
 * listed ones: how often each file that may reference the cause binds it and uses it ({@link
 * #everyFileThatMayReferenceTheCauseBindsAndUsesItAsOftenAsListed()}), and how often the declaring
 * class builds a cause and takes an origin one hop further ({@link
 * #theContextBuildsADispatchCauseInContinuingOnly()}). A second binder written into a listed file
 * changes a count.
 *
 * <p><b>The reader.</b> A saga runner routes an event by the origin the bus wrote, never by the
 * correlation id, which a request chooses. A guide, a README, a CHANGELOG passage or a javadoc
 * sample that showed a routing method reading a correlation id would teach the one router this
 * design removes, so {@link #noDocumentedRoutingHookReadsTheCorrelationId()} reads every {@code
 * .md} file and the javadoc of every main source for it, and {@link
 * #noDocumentNamesARoutingMethodThatDoesNotExist()} for the routing methods that took an envelope.
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

  /** A binding of the cause: {@code where(CAUSE, …)}, qualified or not, wrapped or not. */
  private static final Pattern CAUSE_BINDING =
      Pattern.compile("\\bwhere\\s*\\(\\s*(?:[\\w.]+\\s*\\.\\s*)?CAUSE\\b");

  /** A use of the cause in code, qualified or not; a javadoc link ({@code #CAUSE}) is none. */
  private static final Pattern CAUSE_USE = Pattern.compile("(?<![#\\w])CAUSE\\b");

  /**
   * How often a file that may reference the cause binds it and uses it.
   *
   * @param bindings occurrences of {@link #CAUSE_BINDING}
   * @param uses occurrences of {@link #CAUSE_USE}, the bindings and the declaration included
   */
  private record CauseUses(int bindings, int uses) {}

  /**
   * What each allow-listed file does with the cause today. An allow-list entry admits a whole file,
   * so without these numbers a second binder written into one of the files would pass the scan.
   *
   * <ul>
   *   <li>the context: the declaration, the read in {@code cause()}, the binding in {@code
   *       continuing}, and the read and the two bindings of {@code capturedBindings} (what {@code
   *       wrap} carries);
   *   <li>saga dispatch: its one binding;
   *   <li>the bus: the read and the two bindings of {@code prepareAsyncTask};
   *   <li>the bus contract: the two bindings of its cases;
   *   <li>the in-memory subscription: the check and the two bindings to no cause.
   * </ul>
   */
  private static final Map<String, CauseUses> CAUSE_USES =
      Map.of(
          CONTEXT, new CauseUses(3, 8),
          SAGA_DISPATCH, new CauseUses(1, 1),
          BUS, new CauseUses(2, 4),
          BUS_CONTRACT, new CauseUses(2, 2),
          IN_MEMORY_SUBSCRIPTION, new CauseUses(2, 3));

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

  /**
   * The class object of the dispatch cause. A cause read from JSON, or built by reflection, comes
   * into being without a constructor call the other rules see; both need the class object. No main
   * source holds one: a dispatch cause is never serialised.
   */
  private static final Rule CAUSE_CLASS_OBJECT =
      new Rule(
          "takes the class object of DispatchCause",
          Pattern.compile("\\bDispatchCause\\s*\\.\\s*class\\b"),
          Map.of());

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
          CAUSE_CLASS_OBJECT,
          ORIGIN_CONSTRUCTION,
          ORIGIN_FACTORY,
          ORIGIN_NEXT_HOP,
          METADATA_CONSTRUCTOR_REFERENCE);

  @Test
  void theCauseIsBoundOnlyByItsWriters() {
    assertNoViolations(List.of(CAUSE_REFERENCE, CAUSE_CONSTRUCTION, CAUSE_CLASS_OBJECT));
  }

  /**
   * Inside the files that may reference the cause, every binding and every use is counted: a second
   * binder added to one of them changes a number here, and the number is changed only by a reviewed
   * edit of {@link #CAUSE_USES}.
   */
  @Test
  void everyFileThatMayReferenceTheCauseBindsAndUsesItAsOftenAsListed() {
    assertEquals(
        CAUSE_REFERENCE.allowed().keySet(),
        CAUSE_USES.keySet(),
        "every file that may reference the cause has its bindings and uses counted");
    Path root = repositoryRoot();
    for (Map.Entry<String, CauseUses> listed : CAUSE_USES.entrySet()) {
      String source = read(root.resolve(listed.getKey()));
      assertEquals(
          listed.getValue(),
          new CauseUses(count(CAUSE_BINDING, source), count(CAUSE_USE, source)),
          "bindings and uses of StreamRuneContext.CAUSE in "
              + listed.getKey()
              + ". Only saga dispatch and StreamRuneContext.continuing bind a cause they chose;"
              + " wrap and executeAsync carry the bound one to another thread");
    }
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

  // ---------------------------------------------------------------------------------------------
  // Documentation: no text shows saga routing by a correlation id.
  // ---------------------------------------------------------------------------------------------

  // The routing method that took an event's envelope, and its builder form. Neither exists; the
  // names are built from pieces so that this file holds no call of either.
  private static final String ENVELOPE_ROUTER = "correl" + "ate";
  private static final String ENVELOPE_ROUTER_ON_THE_BUILDER = ENVELOPE_ROUTER + "By";

  /**
   * A routing method of the saga interfaces or of the runner's builder, named as a whole word: the
   * start-event methods, the event-data method, and the two names that took an envelope.
   */
  private static final Pattern ROUTING_METHOD =
      Pattern.compile(
          "\\b(?:"
              + ENVELOPE_ROUTER
              + "|"
              + ENVELOPE_ROUTER_ON_THE_BUILDER
              + "|correlateByEventData|isStartEvent|extractSagaId|startWhen)\\b");

  /** Prose that speaks of a saga, with or without naming a routing method. */
  private static final Pattern SPEAKS_OF_A_SAGA = Pattern.compile("(?i)\\bsagas?\\b");

  /**
   * The metadata fields a request supplies, or supplied before the command bus alone wrote them:
   * the correlation id and the trace id (headers, in every identity mode), the user id (the
   * request's identity), baggage (W3C baggage) and the causation id.
   */
  private static final String REQUEST_FIELDS =
      "(?:correlationId|traceId|causationId|baggage|userId)";

  /**
   * Code that reads one of those fields off an event's metadata or a context: the accessor call.
   */
  private static final Pattern REQUEST_FIELD_READ =
      Pattern.compile("\\b" + REQUEST_FIELDS + "\\s*\\(\\s*\\)");

  /** A saga id built from such a field within one statement. */
  private static final Pattern SAGA_ID_FROM_A_REQUEST_FIELD =
      Pattern.compile(
          "(?:\\bSagaId\\s*\\.\\s*of|\\bnew\\s+SagaId)\\s*\\([^;{}]*?"
              + REQUEST_FIELD_READ.pattern());

  /** An idempotency key built from the correlation id or the trace id within one statement. */
  private static final Pattern KEY_FROM_A_TRACING_FIELD =
      Pattern.compile(
          "\\bIdempotencyKey\\s*\\.\\s*of\\s*\\([^;{}]*?\\b(?:correlationId|traceId)\\s*\\(\\s*\\)");

  /** The correlation id named as an identifier, as prose names a field or an accessor. */
  private static final Pattern CORRELATION_ID_IDENTIFIER = Pattern.compile("\\bcorrelationId\\b");

  /**
   * The correlation id named in a list of the metadata fields, beside one of its siblings: there it
   * is the field's name, not a statement about the field.
   */
  private static final Pattern LISTED_AMONG_THE_METADATA_FIELDS =
      Pattern.compile(
          "\\b(?:eventId|commandId|traceId|spanId|causationId|userId)`?\\s*,\\s*`?correlationId\\b"
              + "|\\bcorrelationId`?\\s*,\\s*`?(?:eventId|commandId|traceId|spanId|causationId|userId)\\b");

  /** A method call, or a method declaration: an identifier followed by an opening parenthesis. */
  private static final Pattern CALL = Pattern.compile("\\b([a-z][A-Za-z0-9_]*)\\s*\\(");

  /** A method declared with a body: {@code name(parameters) {}, the name in group 1. */
  private static final Pattern DECLARATION =
      Pattern.compile("\\b([a-z][A-Za-z0-9_]*)\\s*\\([^()]*\\)\\s*(?:throws\\s+[\\w.,\\s]+)?\\{");

  /** Words that precede a parenthesis without being a method an application declares. */
  private static final Set<String> NOT_A_HELPER =
      Set.of(
          "if",
          "for",
          "while",
          "switch",
          "catch",
          "return",
          "new",
          "synchronized",
          "try",
          "yield",
          "when",
          "isStartEvent",
          "extractSagaId",
          "startWhen",
          "correlateByEventData");

  /**
   * A name of the routing method that took an envelope, written as code: called, in backticks, in a
   * {@code {@code}} or {@code {@link}} tag, or as a member reference.
   */
  private static final Pattern NAMES_AN_ENVELOPE_ROUTER =
      Pattern.compile(
          "\\b"
              + ENVELOPE_ROUTER_ON_THE_BUILDER
              + "\\b|(?<![A-Za-z])"
              + ENVELOPE_ROUTER
              + "\\s*\\(|[`#]"
              + ENVELOPE_ROUTER
              + "\\b|\\{@(?:code|link|linkplain)\\s+"
              + ENVELOPE_ROUTER
              + "\\b");

  /** A javadoc comment together with the declaration it documents, up to its body or its end. */
  private static final Pattern JAVADOC_AND_DECLARATION =
      Pattern.compile("/\\*\\*.*?\\*/[^{;]*", Pattern.DOTALL);

  /**
   * One passage of a document: a fenced code block or a block of prose of a {@code .md} file, or a
   * javadoc comment of a main source with the declaration it documents.
   */
  private record Passage(int line, boolean code, String text) {}

  /**
   * The passages of a {@code .md} file. A fenced block is one code passage. Prose is cut at every
   * blank line, heading, list item and table row, so that two bullets of one list, or two rows of
   * one table, are two passages.
   */
  private static List<Passage> markdownPassages(String markdown) {
    List<Passage> passages = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    int startLine = 1;
    boolean inFence = false;
    String[] lines = markdown.split("\n", -1);
    for (int i = 0; i < lines.length; i++) {
      String line = lines[i];
      String trimmed = line.strip();
      boolean fence = trimmed.startsWith("```");
      if (inFence) {
        if (fence) {
          passages.add(new Passage(startLine, true, current.toString()));
          current.setLength(0);
          inFence = false;
        } else {
          current.append(line).append('\n');
        }
        continue;
      }
      boolean startsABlock =
          trimmed.startsWith("#")
              || trimmed.startsWith("|")
              || trimmed.matches("(?:[-*+]|\\d+\\.)\\s.*");
      if (fence || trimmed.isEmpty() || startsABlock) {
        if (!current.isEmpty()) {
          passages.add(new Passage(startLine, false, current.toString()));
          current.setLength(0);
        }
        if (fence) {
          inFence = true;
          startLine = i + 2;
          continue;
        }
        if (trimmed.isEmpty()) {
          continue;
        }
      }
      if (current.isEmpty()) {
        startLine = i + 1;
      }
      current.append(line).append('\n');
    }
    if (!current.isEmpty()) {
      passages.add(new Passage(startLine, inFence, current.toString()));
    }
    return passages;
  }

  /** The javadoc comments of a main source, each with the declaration that follows it. */
  private static List<Passage> javadocPassages(String source) {
    List<Passage> passages = new ArrayList<>();
    Matcher matcher = JAVADOC_AND_DECLARATION.matcher(source);
    while (matcher.find()) {
      passages.add(new Passage(lineOf(source, matcher.start()), true, matcher.group()));
    }
    return passages;
  }

  /** The names of the methods declared with a body in a code passage. */
  private static Set<String> declaredIn(String code) {
    Set<String> names = new java.util.TreeSet<>();
    Matcher matcher = DECLARATION.matcher(code);
    while (matcher.find()) {
      names.add(matcher.group(1));
    }
    return names;
  }

  /**
   * The methods a document's routing methods call, directly or through one another: every name
   * called in a code passage that names a routing method, and every name called in a code passage
   * that declares one of those. A helper is followed through the fenced blocks of one document.
   */
  private static Set<String> helpersOfTheRoutingMethods(List<Passage> document) {
    Set<String> helpers = new java.util.TreeSet<>();
    boolean grew = true;
    while (grew) {
      grew = false;
      for (Passage passage : document) {
        String text = passage.text();
        if (!passage.code()
            || !(ROUTING_METHOD.matcher(text).find()
                || declaredIn(text).stream().anyMatch(helpers::contains))) {
          continue;
        }
        Matcher call = CALL.matcher(text);
        while (call.find()) {
          if (!NOT_A_HELPER.contains(call.group(1))) {
            grew |= helpers.add(call.group(1));
          }
        }
      }
    }
    return helpers;
  }

  /**
   * The passages of one document that take a routing decision, a saga id or an idempotency key from
   * metadata a request supplies.
   *
   * <p>A code passage — a fenced block, a javadoc comment with its declaration — is one when
   *
   * <ul>
   *   <li>it names a routing method and reads a request-supplied field ({@code correlationId()},
   *       {@code traceId()}, {@code causationId()}, {@code baggage()}, {@code userId()});
   *   <li>it declares a method that a routing method of the same document calls, directly or
   *       through other such methods, and reads one of those fields;
   *   <li>it builds a saga id from one of those fields in one statement, whether or not a routing
   *       method is in sight;
   *   <li>it builds an idempotency key from the correlation id or the trace id in one statement.
   * </ul>
   *
   * <p>A prose passage of a guide is one when it names a routing method, or speaks of a saga, and
   * names the {@code correlationId} identifier other than in a list of the metadata fields.
   *
   * <p><b>What is out of reach,</b> and stays the job of whoever writes and reads a guide ({@link
   * #theDocumentationScanRecognisesRoutingByRequestSuppliedMetadata()} holds one sample of each):
   *
   * <ul>
   *   <li>a listener sample that picks a command's <em>target</em> by the correlation id: any
   *       command constructor can take the value, and the scan cannot tell a target from tracing
   *       data;
   *   <li>a helper declared in another document, or reached through a field or a lambda instead of
   *       a declared method, and a read hidden behind a local variable in another passage;
   *   <li>prose that teaches it in other words ("the correlation identifier"), or with another
   *       field than the correlation id;
   *   <li>javadoc prose: a javadoc comment is read as code only.
   * </ul>
   *
   * <p>It also flags two kinds of harmless text, which are reworded when they occur: a sentence
   * that <em>denies</em> routing by {@code correlationId} with that identifier, and prose about a
   * saga's audit rows that names it.
   */
  private static List<Passage> unsafePassages(List<Passage> document) {
    Set<String> helpers = helpersOfTheRoutingMethods(document);
    List<Passage> unsafe = new ArrayList<>();
    for (Passage passage : document) {
      String text = passage.text();
      boolean hit;
      if (passage.code()) {
        hit =
            SAGA_ID_FROM_A_REQUEST_FIELD.matcher(text).find()
                || KEY_FROM_A_TRACING_FIELD.matcher(text).find()
                || (REQUEST_FIELD_READ.matcher(text).find()
                    && (ROUTING_METHOD.matcher(text).find()
                        || declaredIn(text).stream().anyMatch(helpers::contains)));
      } else {
        hit =
            (ROUTING_METHOD.matcher(text).find() || SPEAKS_OF_A_SAGA.matcher(text).find())
                && CORRELATION_ID_IDENTIFIER
                    .matcher(LISTED_AMONG_THE_METADATA_FIELDS.matcher(text).replaceAll(""))
                    .find();
      }
      if (hit) {
        unsafe.add(passage);
      }
    }
    return unsafe;
  }

  /** Every documented passage: of each {@code .md} file and of each main source's javadoc. */
  private static Map<String, List<Passage>> documentedPassages() {
    Map<String, List<Passage>> passages = new java.util.TreeMap<>();
    List<Path> guides = filesMatching(rel -> rel.endsWith(".md"));
    assertTrue(guides.size() > 20, "expected every guide, found " + guides.size());
    for (Path guide : guides) {
      passages.put(relative(guide), markdownPassages(read(guide)));
    }
    for (Path source : mainSources()) {
      passages.put(relative(source), javadocPassages(read(source)));
    }
    return passages;
  }

  /**
   * No guide, README, CHANGELOG passage or javadoc sample routes a saga, builds a saga id or builds
   * an idempotency key from metadata a request supplies. A client chooses the correlation id and
   * the trace id with a header in every identity mode, so a router copied from such a text lets any
   * caller hand its event to another saga. {@link #unsafePassages} says what is recognised and what
   * is out of reach.
   */
  @Test
  void noDocumentedRoutingHookReadsTheCorrelationId() {
    List<String> violations = new ArrayList<>();
    documentedPassages()
        .forEach(
            (file, passages) -> {
              for (Passage passage : unsafePassages(passages)) {
                violations.add(file + ":" + passage.line());
              }
            });
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " passage(s) take a saga routing decision, a saga id or an idempotency key from"
              + " metadata a request supplies (correlation id, trace id, causation id, baggage,"
              + " user id). A saga runner routes the replies to a saga's own commands by"
              + " EventMetadata.sagaOrigin and every other event by"
              + " correlateByEventData(streamId, event); extractSagaId derives the saga id from"
              + " the start event's own data. Show the id derived from the event's data. Where a"
              + " text states that routing does not read the correlation id, say so in words"
              + " (\"the correlation id\"), in a passage of its own. Offenders:\n  "
              + String.join("\n  ", violations));
    }
  }

  /** A saga id built in code: the factory or the constructor. */
  private static final Pattern BUILDS_A_SAGA_ID =
      Pattern.compile("\\bSagaId\\s*\\.\\s*of\\s*\\(|\\bnew\\s+SagaId\\s*\\(");

  /** A routing method that returns a saga id for an event. */
  private static final Pattern NAMES_A_SAGA_FOR_AN_EVENT =
      Pattern.compile(
          "\\b(?:"
              + ENVELOPE_ROUTER
              + "|"
              + ENVELOPE_ROUTER_ON_THE_BUILDER
              + "|correlateByEventData|extractSagaId)\\b");

  /**
   * Whether a code passage builds a saga id in a routing method without checking that the event's
   * data can be one: it names {@code extractSagaId} or {@code correlateByEventData}, builds a
   * {@code SagaId}, and does not mention {@code SagaId.MAX_LENGTH}. A sample that keeps the check
   * in a helper of its own block passes; one that calls a helper declared elsewhere builds no
   * {@code SagaId} itself and passes too.
   */
  private static boolean buildsAnUncheckedSagaId(Passage passage) {
    String text = passage.text();
    return passage.code()
        && NAMES_A_SAGA_FOR_AN_EVENT.matcher(text).find()
        && BUILDS_A_SAGA_ID.matcher(text).find()
        && !text.contains("MAX_LENGTH");
  }

  /**
   * No sample's routing method builds a saga id from an event's data without the check that the
   * data can be one. The event's data is its writer's: {@code SagaId.of} throws for an id of more
   * than {@code SagaId.MAX_LENGTH} characters, a throw in a routing method is poison, and so a
   * caller who writes events with such an id files one null-saga dead-letter entry per event.
   */
  @Test
  void noDocumentedRoutingMethodBuildsASagaIdItDoesNotCheck() {
    List<String> violations = new ArrayList<>();
    documentedPassages()
        .forEach(
            (file, passages) -> {
              for (Passage passage : passages) {
                if (buildsAnUncheckedSagaId(passage)) {
                  violations.add(file + ":" + passage.line());
                }
              }
            });
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " sample(s) build a SagaId in extractSagaId or correlateByEventData without"
              + " checking the event's data against SagaId.MAX_LENGTH. A routing method must not"
              + " throw on the event's data: return Optional.empty() from correlateByEventData,"
              + " and decline the event in isStartEvent, for an id no SagaId can hold."
              + " Offenders:\n  "
              + String.join("\n  ", violations));
    }
  }

  @Test
  void theUncheckedSagaIdScanRecognisesASampleThatThrowsOnTheEventsData() {
    String unchecked =
        "```java\n    @Override\n"
            + "    public Optional<SagaId> correlateByEventData(StreamId streamId,"
            + " DomainEvent event) {\n"
            + "        return event instanceof OrderCancelled e\n"
            + "            ? Optional.of(SagaId.of(\"order-saga-\" + e.orderId()))\n"
            + "            : Optional.empty();\n    }\n```\n";
    String uncheckedOnTheBuilder =
        "```java\n.extractSagaId(event ->"
            + " SagaId.of(\"order-saga-\" + ((OrderPlaced) event.event()).orderId()))\n```\n";
    for (String sample : List.of(unchecked, uncheckedOnTheBuilder)) {
      assertTrue(
          markdownPassages(sample).stream()
              .anyMatch(SagaProvenanceWriterRatchetTest::buildsAnUncheckedSagaId),
          "must reject: " + sample);
    }
    String checked =
        "```java\n    @Override\n"
            + "    public Optional<SagaId> correlateByEventData(StreamId streamId,"
            + " DomainEvent event) {\n"
            + "        return event instanceof OrderCancelled e ? sagaOf(e.orderId())"
            + " : Optional.empty();\n    }\n\n"
            + "    static Optional<SagaId> sagaOf(String orderId) {\n"
            + "        String id = \"order-saga-\" + orderId;\n"
            + "        return id.length() > SagaId.MAX_LENGTH\n"
            + "            ? Optional.empty() : Optional.of(SagaId.of(id));\n    }\n```\n\n"
            + "```java\n.extractSagaId(event ->"
            + " OrderSaga.sagaOf(((OrderPlaced) event.event()).orderId()).orElseThrow())\n```\n";
    assertTrue(
        markdownPassages(checked).stream()
            .noneMatch(SagaProvenanceWriterRatchetTest::buildsAnUncheckedSagaId),
        "must not flag: " + checked);
  }

  /**
   * No document names the routing method that took an event's envelope, or its builder form: an
   * application routes the events of others through {@code correlateByEventData(streamId, event)},
   * and a sample written against the other shape either does not compile (an {@code @Override}, the
   * builder call) or declares a method the runner never calls.
   */
  @Test
  void noDocumentNamesARoutingMethodThatDoesNotExist() {
    List<String> violations = new ArrayList<>();
    List<Path> files = new ArrayList<>(mainSources());
    files.addAll(filesMatching(rel -> rel.endsWith(".md")));
    for (Path file : files) {
      String source = read(file);
      Matcher matcher = NAMES_AN_ENVELOPE_ROUTER.matcher(source);
      while (matcher.find()) {
        violations.add(relative(file) + ":" + lineOf(source, matcher.start()));
      }
    }
    if (!violations.isEmpty()) {
      fail(
          violations.size()
              + " place(s) name a saga routing method that takes an event's envelope. The method"
              + " an application implements is correlateByEventData(StreamId, DomainEvent) (on the"
              + " builder: correlateByEventData(BiFunction)). Offenders:\n  "
              + String.join("\n  ", violations));
    }
  }

  private static boolean flagsMarkdown(String markdown) {
    return !unsafePassages(markdownPassages(markdown)).isEmpty();
  }

  private static boolean flagsJavadoc(String source) {
    return !unsafePassages(javadocPassages(source)).isEmpty();
  }

  /**
   * The scan's own test data: texts it must reject, texts it must accept, the unsafe texts it
   * cannot recognise, and the harmless texts it rejects all the same. The last two groups are
   * asserted as they are, so that a change of the scan that moves one of them is seen.
   */
  @Test
  void theDocumentationScanRecognisesRoutingByRequestSuppliedMetadata() {
    String router = ENVELOPE_ROUTER;
    String builderRouter = ENVELOPE_ROUTER_ON_THE_BUILDER;
    Map<String, String> rejected = new java.util.LinkedHashMap<>();
    rejected.put(
        "an orchestrator sample that returns the correlation id as the saga id",
        "Intro.\n\n```java\n"
            + "    @Override\n"
            + "    public Optional<SagaId> "
            + router
            + "(EventEnvelope event) {\n"
            + "        String corrId = event.metadata().correlationId().value();\n"
            + "        return corrId.startsWith(\"order-saga-\")\n"
            + "            ? Optional.of(SagaId.of(corrId))\n"
            + "            : Optional.empty();\n"
            + "    }\n```\n");
    rejected.put(
        "the same router written on the builder",
        "```java\nSagaRunner.<State>builder()\n    ."
            + builderRouter
            + "(event -> Optional.of(SagaId.of(event.metadata().correlationId().value())))\n```\n");
    rejected.put(
        "a start router that takes the saga id from the correlation id",
        "```java\n.extractSagaId(event -> SagaId.of(event.metadata().correlationId().value()))\n```\n");
    rejected.put(
        "a guide paragraph that states the rule without naming a method",
        "**Correlation:** non-start events are routed to the correct saga by reading"
            + " `event.metadata().correlationId().value()` — the runner sets it when dispatching.\n");
    rejected.put(
        "a CHANGELOG bullet",
        "- Intro bullet.\n- `SagaDecider<S>` (routing configured on the builder with `startWhen`,"
            + " `extractSagaId`, `"
            + builderRouter
            + "`); correlation rides on\n  `EventMetadata.correlationId`, set to the saga id.\n");
    rejected.put(
        "a helper of the start router, declared in a second fenced block",
        "```java\n@Override\npublic SagaId extractSagaId(EventEnvelope event) {\n"
            + "    return sagaOf(event);\n}\n```\n\n"
            + "```java\nprivate static SagaId sagaOf(EventEnvelope event) {\n"
            + "    return SagaId.of(event.metadata().correlationId().value());\n}\n```\n");
    rejected.put(
        "a helper of a helper, three blocks apart, that reads baggage",
        "```java\npublic SagaId extractSagaId(EventEnvelope event) {\n    return sagaOf(event);\n}\n"
            + "```\n\nText.\n\n```java\nprivate SagaId sagaOf(EventEnvelope event) {\n"
            + "    return new SagaId(keyOf(event));\n}\n```\n\n"
            + "```java\nprivate String keyOf(EventEnvelope event) {\n"
            + "    var meta = event.metadata();\n    return meta.baggage().get(\"saga\");\n}\n```\n");
    rejected.put(
        "a start router by the trace id",
        "```java\n.startWhen(e -> e.event() instanceof OrderPlaced)\n"
            + ".extractSagaId(event -> SagaId.of(event.metadata().traceId().value()))\n```\n");
    rejected.put(
        "a start router by a baggage entry",
        "```java\n.extractSagaId(event -> SagaId.of(event.metadata().baggage().get(\"saga\")))\n```\n");
    rejected.put(
        "a start router by the user id",
        "```java\n.extractSagaId(event -> SagaId.of(\"cart-\" + event.metadata().userId().value()))\n"
            + "```\n");
    rejected.put(
        "prose without a word of routing",
        "The saga a reply belongs to is the one named by the event's `correlationId`: saga dispatch"
            + " sets it to the saga id.\n");
    rejected.put(
        "prose: find the saga",
        "To find the saga of a reply, read `event.metadata().correlationId()`.\n");
    rejected.put(
        "isStartEvent decided by the correlation id through local variables",
        "```java\npublic boolean isStartEvent(EventEnvelope event) {\n"
            + "    var meta = event.metadata();\n    var corr = meta.correlationId();\n"
            + "    return !corr.value().startsWith(\"order-saga-\");\n}\n```\n");
    rejected.put(
        "an idempotency key from the correlation id",
        "```java\ncommandBus.execute(cmd, IdempotencyKey.of(\"capture:\""
            + " + event.metadata().correlationId().value()));\n```\n");
    rejected.put(
        "the event-data method through a map keyed by the bound context's correlation id",
        "```java\npublic Optional<SagaId> correlateByEventData(StreamId streamId, DomainEvent event)"
            + " {\n    return Optional.ofNullable(\n"
            + "        SAGA_BY_CORRELATION.get(StreamRuneContext.capture().correlationId()));\n}\n"
            + "```\n");
    rejected.forEach((what, text) -> assertTrue(flagsMarkdown(text), "must reject: " + what));

    Map<String, String> rejectedJavadoc = new java.util.LinkedHashMap<>();
    rejectedJavadoc.put(
        "a class comment of the interface an application implements",
        "/**\n * <h2>Correlation</h2>\n *\n * <p>Implement {@link #"
            + router
            + "} to extract the saga ID from non-start events — typically by reading {@code\n"
            + " * event.metadata().correlationId().value()}.\n */\npublic interface Saga<S> {");
    rejectedJavadoc.put(
        "a method comment whose declaration is the routing method",
        "  /**\n   * Typical implementation: read {@code event.metadata().correlationId().value()}"
            + " and return\n   * a {@link SagaId}.\n   */\n  Optional<SagaId> "
            + router
            + "(EventEnvelope event);");
    rejectedJavadoc.put(
        "a class comment with a sample and no routing method named",
        "/**\n * Finds the saga of a reply: {@code"
            + " SagaId.of(event.metadata().correlationId().value())}.\n */\n"
            + "public final class SagaLookup {");
    rejectedJavadoc.put(
        "the start router's comment: the user id",
        "  /**\n   * Typical implementation: {@code SagaId.of(event.metadata().userId().value())}.\n"
            + "   */\n  SagaId extractSagaId(EventEnvelope event);");
    rejectedJavadoc.forEach((what, text) -> assertTrue(flagsJavadoc(text), "must reject: " + what));

    // What the guides say instead.
    String routedByData =
        "The runner asks `correlateByEventData(streamId, event)`.\n\n"
            + "A correlation id is tracing data. A client sets it with `X-Correlation-Id`, so never"
            + " route, authorise or deduplicate by it.\n\n```java\n"
            + "    @Override\n"
            + "    public SagaId extractSagaId(EventEnvelope event) {\n"
            + "        return sagaOf(((OrderPlaced) event.event()).orderId()).orElseThrow();\n"
            + "    }\n\n"
            + "    @Override\n"
            + "    public Optional<SagaId> correlateByEventData(StreamId streamId,"
            + " DomainEvent event) {\n"
            + "        return event instanceof OrderCancelled e ? sagaOf(e.orderId())"
            + " : Optional.empty();\n"
            + "    }\n\n"
            + "    private static Optional<SagaId> sagaOf(String orderId) {\n"
            + "        String id = \"order-saga-\" + orderId;\n"
            + "        return id.length() > SagaId.MAX_LENGTH\n"
            + "            ? Optional.empty() : Optional.of(new SagaId(id));\n"
            + "    }\n```\n\n"
            + "```java\n// A listener keys its command by the causing event\n"
            + "commandBus.execute(command,\n"
            + "    IdempotencyKey.of(\"capture:\" + event.metadata().eventId().value()));\n```\n\n"
            + "```java\n// An audit lookup: no routing method, no saga id, no key\n"
            + "audit.findByCorrelationId(ctx.correlationId());\n```\n\n"
            + "| `correlationId` | `X-Correlation-Id`; generated when absent | yes |\n"
            + "| `sagaOrigin` | saga dispatch | no |\n"
            + "- `EventMetadata` on every envelope: `eventId`, `correlationId`, `causationId`.\n"
            + "- A saga is driven by `SagaRunner` as an `EventListener`.\n"
            + "- `EventMetadata` on every envelope: `traceId`, `spanId`, `correlationId`,\n"
            + "  `causationId`, `userId` and `sagaOrigin`, which names the saga whose dispatch\n"
            + "  caused the event.\n";
    assertFalse(flagsMarkdown(routedByData), "must not flag: " + routedByData);
    String derivedFromData =
        "  /**\n   * Derive the saga id from the event's own data, never from its correlation id.\n"
            + "   */\n  SagaId extractSagaId(EventEnvelope event);\n"
            + "  /** The correlation id of the request: {@code ctx.correlationId()}. */\n"
            + "  CorrelationId correlationId();\n"
            + "  /** {@code SagaId.of(\"order-saga-\" + orderId)}; see {@code ctx.userId()}. */\n"
            + "  SagaId sagaOf(String orderId);";
    assertFalse(flagsJavadoc(derivedFromData), "must not flag: " + derivedFromData);

    // Unsafe, and out of the scan's reach.
    Map<String, String> outOfReach = new java.util.LinkedHashMap<>();
    outOfReach.put(
        "a listener that picks its command's target by the correlation id: the value goes into a"
            + " command constructor like any other argument",
        "```java\nEventListener pm = events -> {\n  for (EventEnvelope e : events) {\n"
            + "    commandBus.execute(new CapturePayment(e.metadata().correlationId().value()));\n"
            + "  }\n};\n```\n");
    outOfReach.put(
        "a helper that is not declared in the document",
        "```java\npublic SagaId extractSagaId(EventEnvelope event) {\n"
            + "    return SagaIds.fromMetadata(event);\n}\n```\n");
    outOfReach.put(
        "prose in other words",
        "A reply finds its saga through the correlation identifier of its metadata.\n");
    outOfReach.put(
        "prose with another field",
        "The saga of a cart is the one named by the event's `userId`.\n");
    outOfReach.forEach(
        (what, text) -> assertFalse(flagsMarkdown(text), "recognised after all: " + what));

    // Harmless, and rejected all the same: reword them where they occur.
    Map<String, String> harmlessButRejected = new java.util.LinkedHashMap<>();
    harmlessButRejected.put(
        "a sentence that denies it with the identifier",
        "`correlateByEventData` does not receive `correlationId`.\n");
    harmlessButRejected.put(
        "prose about a saga's audit rows",
        "Audit rows of a saga's commands are found by `correlationId`; routing does not use it.\n");
    harmlessButRejected.forEach(
        (what, text) -> assertTrue(flagsMarkdown(text), "accepted after all: " + what));

    for (String named :
        List.of(
            "." + builderRouter + "(event -> Optional.empty())",
            "`" + builderRouter + "`",
            "Optional<SagaId> " + router + "(EventEnvelope event);",
            "inner." + router + " (event)",
            "`" + router + "`",
            "{@link #" + router + "}",
            "{@link SagaOrchestrator#" + router + "}",
            "{@code " + router + "}")) {
      assertTrue(NAMES_AN_ENVELOPE_ROUTER.matcher(named).find(), "must reject: " + named);
    }
    for (String ordinary :
        List.of(
            "correlateByEventData(streamId, event)",
            "`correlateByEventData`",
            "{@link #correlateByEventData}",
            ".correlateByEventData((streamId, event) -> Optional.empty())",
            "a correlated event",
            "an operator can still " + router + " audit rows to the subject",
            "correlatedEvent(5L)")) {
      assertFalse(NAMES_AN_ENVELOPE_ROUTER.matcher(ordinary).find(), "must not flag: " + ordinary);
    }
  }

  /** The cutting of a guide into passages: fences whole, prose by block, bullet and table row. */
  @Test
  void aGuideIsCutIntoFencedBlocksAndBlocksOfProse() {
    List<Passage> passages =
        markdownPassages(
            "# Title\n\nFirst paragraph,\nsecond line.\n\n```java\ncode();\n\nmore();\n```\n"
                + "- bullet one\n  continued\n- bullet two\n\n| a | b |\n| c | d |\n");
    assertEquals(
        List.of(
            "# Title\n",
            "First paragraph,\nsecond line.\n",
            "code();\n\nmore();\n",
            "- bullet one\n  continued\n",
            "- bullet two\n",
            "| a | b |\n",
            "| c | d |\n"),
        passages.stream().map(Passage::text).toList());
    assertEquals(
        List.of(false, false, true, false, false, false, false),
        passages.stream().map(Passage::code).toList());
    assertEquals(List.of(1, 3, 7, 11, 13, 15, 16), passages.stream().map(Passage::line).toList());
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
            "mapper.readValue(json, StreamRuneContext.DispatchCause.class)",
            "DispatchCause.class.getDeclaredConstructors()",
            "Class<?> type = org.streamrune.core.StreamRuneContext.DispatchCause\n    .class;")) {
      assertTrue(CAUSE_CLASS_OBJECT.forbidden().matcher(shape).find(), "must reject: " + shape);
    }
    for (String binding :
        List.of(
            "ScopedValue.where(StreamRuneContext.CAUSE, cause)",
            "ScopedValue.where(CAUSE, cause)",
            "carrier.where(\n            CAUSE,\n            new DispatchCause(id, origin))",
            "detached.where(org.streamrune.core.StreamRuneContext.CAUSE, null)")) {
      assertEquals(1, count(CAUSE_BINDING, binding), "one binding: " + binding);
      assertEquals(1, count(CAUSE_USE, binding), "one use: " + binding);
    }
    assertEquals(0, count(CAUSE_BINDING, "ScopedValue.where(StreamRuneContext.CURRENT, context)"));
    assertEquals(0, count(CAUSE_BINDING, "ScopedValue.where(SAGA_OWNED, Boolean.FALSE)"));
    assertEquals(2, count(CAUSE_USE, "return CAUSE.isBound() ? CAUSE.get() : null;"));
    assertEquals(
        0, count(CAUSE_USE, " * carries {@link StreamRuneContext#CAUSE} and {@link #CAUSE}"));
    assertEquals(
        0, count(CAUSE_USE, "DispatchCause cause = StreamRuneContext.cause(); // BECAUSE"));
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
            "{@link StreamRuneContext.DispatchCause}",
            "StreamRuneContext.DispatchCause cause = StreamRuneContext.cause();",
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
