package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Every statement in StreamRune's main sources that writes {@code saga_state} is classified here,
 * and every write that can take a saga out of its forward phase goes through {@code
 * PostgresSagaStore}'s one fence-changing write path, which takes the saga's exclusive fence lock
 * ({@link SagaFenceLock}) before the statement. A forward command's append reads the row under the
 * shared form of that lock; a write that skipped the exclusive lock could stamp an episode or end a
 * saga while a forward command of it is committing, and that command would be stored after the
 * compensation began.
 *
 * <p><b>How to classify a new write.</b> The test finds every statement whose SQL text contains
 * {@code INSERT INTO saga_state}, {@code UPDATE saga_state}, {@code DELETE FROM saga_state}, {@code
 * MERGE INTO saga_state} or {@code TRUNCATE saga_state} (comments are ignored; the statement's
 * string literals are joined, so a split literal is found too) and fails for one that is not in
 * {@link #CLASSIFIED}, or whose kind of statement ({@link Verb}) is not the classified one — an
 * insert that gains {@code ON CONFLICT … DO UPDATE} is an upsert and is classified again, as an
 * update of the existing row. Ask of the new statement: can it turn the fence's verdict from
 * "forward" to "refused" — that is, can it leave a row carrying an episode stamp it did not carry,
 * at a terminal status ({@code COMPLETED}, {@code COMPENSATED}, {@code FAILED}), or remove the row?
 *
 * <ul>
 *   <li>Yes, for some bind values: it is {@link Kind#FENCE_CHANGING}. Declare it in {@code
 *       PostgresSagaStore} as a {@code FenceChangingWrite} and run it through {@code
 *       fenceChangingWrite(...)}, passing {@code leavesForwardPhase} true for every call whose bind
 *       values can make that turn (when in doubt, true: the lock then only costs a wait).
 *   <li>No, for every bind value — a plain insert or an insert that does nothing on conflict (the
 *       fence refuses an absent row, so a new row turns nothing to "refused"; an upsert updates the
 *       existing row and is judged as an update), a write of a non-terminal status other than
 *       {@code COMPENSATING} that keeps the stamp as it is, a write of a column the fence does not
 *       read: it is {@link Kind#KEEPS_FORWARD_PHASE}. Add it with the reason, as a plain {@code
 *       String} constant.
 * </ul>
 *
 * <p>An inline SQL string (not a constant) that writes {@code saga_state} is refused outright: name
 * it, then classify it. A delete, a merge and a truncate are always fence-changing.
 *
 * <p><b>A table the text does not name.</b> The classification reads the table from the statement's
 * text. A write that composes its table from a value the code supplies ({@code "UPDATE " + TABLE},
 * {@code String.format("UPDATE %s ...", table)}) is found too and must be listed in {@link
 * #COMPOSED_TARGETS} with the reason its table is never {@code saga_state}; and no literal may name
 * {@code saga_state} alone — the piece such a write would be composed from — outside {@link
 * #LITERAL_TABLE_NAMES}. The limit: the test does not follow a supplied value to where it is
 * computed, so a listed write stays listed if the code feeding it changes; and the scan covers the
 * repository's Java main sources and SQL resources, not SQL built at run time from other input.
 *
 * <p>Each call of {@code fenceChangingWrite(...)} passes {@code leavesForwardPhase} as the literal
 * {@code true} or as {@code leavesForwardPhase(<status>)}, so the one place the lock condition can
 * be wrong is that method, which the behavioural tests of {@code SagaForwardFenceLockingIT} pin for
 * every fence-changing write.
 */
class SagaStateWriteRatchetTest {

  enum Kind {
    FENCE_CHANGING,
    KEEPS_FORWARD_PHASE
  }

  /** The kind of statement a write is; a classification holds for one kind only. */
  enum Verb {
    INSERT,
    UPSERT,
    UPDATE,
    DELETE,
    MERGE,
    TRUNCATE
  }

  private record Classified(Verb verb, Kind kind, String reason) {}

  private static final String SAGA_STORE = "PostgresSagaStore.java";

  /** Every {@code saga_state} write of the main sources, by {@code File.CONSTANT}. */
  private static final Map<String, Classified> CLASSIFIED =
      Map.of(
          "PostgresSagaStore.UPDATE_CAS",
          new Classified(
              Verb.UPDATE,
              Kind.FENCE_CHANGING,
              "update and claimCompensating: COMPENSATING stamps the episode, a terminal status"
                  + " ends the saga"),
          "PostgresSagaStore.APPLY_EVENT",
          new Classified(
              Verb.UPDATE,
              Kind.FENCE_CHANGING,
              "an evolved COMPENSATING stamps the episode, an evolved terminal status ends the"
                  + " saga"),
          "PostgresSagaStore.DELETE_BY_TYPE",
          new Classified(
              Verb.DELETE,
              Kind.FENCE_CHANGING,
              "a forward command of a saga with no row is refused"),
          "PostgresSagaStore.INSERT_NEW",
          new Classified(
              Verb.INSERT,
              Kind.KEEPS_FORWARD_PHASE,
              "an insert: the fence refused the absent row before it"),
          "PostgresSagaStore.INSERT_GENESIS_PENDING",
          new Classified(
              Verb.INSERT,
              Kind.KEEPS_FORWARD_PHASE,
              "an insert: the fence refused the absent row before it"),
          "PostgresSagaStore.INSERT_FAULTED",
          new Classified(
              Verb.INSERT,
              Kind.KEEPS_FORWARD_PHASE,
              "an insert: the fence refused the absent row before it"),
          "PostgresSagaStore.MARK_FAULTED",
          new Classified(
              Verb.UPDATE,
              Kind.KEEPS_FORWARD_PHASE,
              "FAULTED is not terminal and the statement does not touch the stamp"),
          "PostgresSagaStore.SET_DEAD_LETTER_PENDING",
          new Classified(
              Verb.UPDATE, Kind.KEEPS_FORWARD_PHASE, "the shield flag; the fence does not read it"),
          "PostgresSagaDeadLetterStore.CLEAR_SHIELD_IF_DRAINED",
          new Classified(
              Verb.UPDATE, Kind.KEEPS_FORWARD_PHASE, "the shield flag; the fence does not read it"),
          "PostgresSagaDeadLetterStore.ASSERT_SHIELD",
          new Classified(
              Verb.UPDATE,
              Kind.KEEPS_FORWARD_PHASE,
              "the shield flag; the fence does not read it"));

  /**
   * Every write whose table the statement's text does not name, with the reason its table is never
   * {@code saga_state}. Keyed by {@code File.CONSTANT}, or for an unnamed statement by {@code File:
   * <its text from the verb>} with {@code <value>} where the code supplies a value.
   */
  private static final Map<String, String> COMPOSED_TARGETS =
      Map.of(
          "JdbcProjectionRepository.UPSERT",
          "a projection's read-model table: the projection's name plus _view",
          "JdbcProjectionRepository.VERSIONED_UPDATE",
          "a projection's read-model table: the projection's name plus _view",
          "JdbcProjectionRepository.DELETE",
          "a projection's read-model table: the projection's name plus _view",
          "PostgresCryptoEngine.java: DELETE FROM <value> WHERE subject_id = ?",
          "the crypto engine's key table (streamrune.crypto.postgres.table-name, default"
              + " encryption_keys)",
          "PostgresCryptoEngine.java: INSERT INTO <value> (subject_id, key_bytes, key_versio",
          "the crypto engine's key table (streamrune.crypto.postgres.table-name, default"
              + " encryption_keys)");

  /** The files whose literals may name {@code saga_state} alone, with the reason. */
  private static final Map<String, String> LITERAL_TABLE_NAMES =
      Map.of(
          "SchemaValidator.java",
          "the tables the schema check expects to find; it reads the catalogue and writes nothing");

  /** In a statement's shape: a value the code supplies between two literals. */
  private static final char SUPPLIED = '\u0001';

  /**
   * A write whose verb is followed by a value the text does not show: a supplied value (a
   * concatenated constant, variable or call) or a format placeholder ({@code %s}, {@code %1$s},
   * {@code {0}}), after a space or a quote. The verb starts a statement's text, or follows {@code
   * ;}, a parenthesis or a supplied value: {@code FOR UPDATE}, {@code DO UPDATE}, a message such as
   * {@code "saga update"} and a lone word {@code "update"} are not writes.
   */
  private static final Pattern COMPOSED_TARGET =
      Pattern.compile(
          "(?i)(?:^|[;("
              + SUPPLIED
              + "])\\s*(insert\\s+into|update|delete\\s+from|merge\\s+into"
              + "|truncate(?:\\s+table)?(?:\\s+only)?)(?:\\s+(?:\\\\?\")?|\\\\?\")\\s*(?:"
              + SUPPLIED
              + "|%(?:\\d+\\$)?s|\\{\\d*\\})");

  /**
   * A literal that is the name of the {@code saga_state} table and nothing else (quoted, or
   * qualified by a schema, or the qualified tail {@code .saga_state}): a name a statement could be
   * composed from.
   */
  private static final Pattern NAMES_SAGA_STATE_ALONE =
      Pattern.compile(
          "(?i)\\s*(?:(?:\\\\?\")?\\w+(?:\\\\?\")?)?\\.?(?:\\\\?\")?saga_state(?:\\\\?\")?\\s*");

  private static final Pattern SAGA_STATE_WRITE =
      Pattern.compile(
          "(?i)\\b(insert\\s+into|update|delete\\s+from|merge\\s+into"
              + "|truncate(?:\\s+table)?(?:\\s+only)?)\\s+saga_state\\b");

  private static final Pattern UPSERT =
      Pattern.compile("(?is)\\bon\\s+conflict\\b.*\\bdo\\s+update\\b");

  /** The fourth argument of a fenceChangingWrite call: the literal true or the status predicate. */
  private static final Pattern LOCK_CONDITION =
      Pattern.compile("true|leavesForwardPhase\\(\\s*[A-Za-z_]\\w*\\s*\\)");

  /** {@code [modifiers] Type NAME = ...} at the start of a statement. */
  private static final Pattern CONSTANT_DECLARATION =
      Pattern.compile(
          "(?s)^\\s*(?:@\\w+\\s+)*(?:(?:private|public|protected|static|final)\\s+)+"
              + "([\\w.<>]+)\\s+([A-Z][A-Z0-9_]*)\\s*=");

  private static final Set<String> PRUNED_DIRECTORY_NAMES =
      Set.of("build", "out", ".git", ".gradle", ".idea", "node_modules");

  @Test
  void everySagaStateWriteIsClassified() {
    Map<String, Write> found = sagaStateWrites();

    Set<String> unclassified = new TreeSet<>(found.keySet());
    unclassified.removeAll(CLASSIFIED.keySet());
    Set<String> stale = new TreeSet<>(CLASSIFIED.keySet());
    stale.removeAll(found.keySet());
    if (!unclassified.isEmpty() || !stale.isEmpty()) {
      fail(
          "saga_state writes not classified in SagaStateWriteRatchetTest.CLASSIFIED: "
              + unclassified
              + "; classified but not found: "
              + stale
              + ". See the class javadoc for how to classify a write.");
    }
  }

  @Test
  void everyFenceChangingWriteIsAFenceChangingWriteOfTheSagaStore() {
    Map<String, Write> found = sagaStateWrites();
    for (var entry : CLASSIFIED.entrySet()) {
      Write write = found.get(entry.getKey());
      if (write == null) {
        continue; // reported by everySagaStateWriteIsClassified
      }
      Classified classified = entry.getValue();
      assertEquals(
          classified.verb(),
          verbOf(write.sql()),
          entry.getKey()
              + " is not the kind of statement it is classified as: classify it again"
              + " (see the class javadoc)");
      if (Set.of(Verb.DELETE, Verb.MERGE, Verb.TRUNCATE).contains(classified.verb())) {
        assertEquals(
            Kind.FENCE_CHANGING,
            classified.kind(),
            entry.getKey()
                + ": a "
                + classified.verb()
                + " of saga_state is always fence-changing");
      }
      if (classified.kind() == Kind.FENCE_CHANGING) {
        assertEquals(
            SAGA_STORE,
            write.file(),
            entry.getKey() + " changes the fence's verdict: it belongs to the saga store's path");
        assertEquals(
            "FenceChangingWrite",
            write.declaredType(),
            entry.getKey()
                + " changes the fence's verdict: declare it as a FenceChangingWrite so it can only"
                + " run through fenceChangingWrite(...)");
      } else {
        assertEquals(
            "String",
            write.declaredType(),
            entry.getKey() + " is classified as keeping the forward phase: a plain String");
      }
    }
  }

  /**
   * The SQL of a {@code FenceChangingWrite} is read in one method only, and that method takes the
   * exclusive fence lock before it prepares the statement.
   */
  @Test
  void theSagaStoreRunsAFenceChangingWriteOnlyAfterTheExclusiveLock() {
    Path store = only(SAGA_STORE);
    String code = Source.of(read(store)).code();

    Matcher record = Pattern.compile("record\\s+FenceChangingWrite\\s*\\(").matcher(code);
    assertTrue(record.find(), "PostgresSagaStore declares FenceChangingWrite");
    for (Path other : mainJavaSources()) {
      if (!other.equals(store)) {
        String otherCode = Source.of(read(other)).code();
        assertTrue(
            !otherCode.contains("FenceChangingWrite"),
            other + " must not use FenceChangingWrite: the saga store's path is the only one");
      }
    }

    int start = code.indexOf("T fenceChangingWrite(");
    assertTrue(start >= 0, "PostgresSagaStore declares fenceChangingWrite(...)");
    int bodyStart = code.indexOf('{', start);
    int bodyEnd = matchingBrace(code, bodyStart);
    String body = code.substring(bodyStart, bodyEnd);

    List<Integer> sqlReads = indexesOf(code, ".sql()");
    assertTrue(!sqlReads.isEmpty(), "fenceChangingWrite reads the statement");
    for (int at : sqlReads) {
      assertTrue(
          at > bodyStart && at < bodyEnd,
          "the SQL of a FenceChangingWrite is read outside fenceChangingWrite(...) at offset "
              + at);
    }
    int lock = body.indexOf("SagaFenceLock.lockExclusive(");
    int lastSqlRead = body.lastIndexOf(".sql()");
    assertTrue(lock >= 0, "fenceChangingWrite takes the exclusive fence lock");
    assertTrue(
        lock < lastSqlRead, "the lock is taken before the statement that leaves the forward phase");
  }

  /**
   * Every call of {@code fenceChangingWrite(...)} passes the lock condition as the literal {@code
   * true} or as {@code leavesForwardPhase(<status>)}: a constant {@code false} or another
   * expression would leave a fence-changing write unlocked with the declared type still correct.
   */
  @Test
  void everyFenceChangingWriteCallTakesTheLockOnTheStatusPredicateOrAlways() {
    String code = Source.of(read(only(SAGA_STORE))).code();
    int declaration = code.indexOf("T fenceChangingWrite(");
    List<String> conditions = new ArrayList<>();
    for (int at : indexesOf(code, "fenceChangingWrite(")) {
      if (at == declaration + 2) {
        continue;
      }
      List<String> arguments = arguments(code, at + "fenceChangingWrite(".length());
      assertEquals(6, arguments.size(), "fenceChangingWrite takes six arguments: " + arguments);
      conditions.add(arguments.get(3));
    }
    assertEquals(
        3, conditions.size(), "the three fence-changing writes call fenceChangingWrite once each");
    for (String condition : conditions) {
      assertTrue(
          LOCK_CONDITION.matcher(condition).matches(),
          "a fenceChangingWrite call passes the lock condition `"
              + condition
              + "`; pass true or leavesForwardPhase(<status>)");
    }
  }

  /**
   * A write whose table the statement composes from a value the code supplies, or from a format
   * placeholder, cannot be read here: each one is listed in {@link #COMPOSED_TARGETS} with the
   * reason its table is never {@code saga_state}. A new one fails the build until it is listed.
   */
  @Test
  void everyWriteToATableTheTextDoesNotNameIsListed() {
    Map<String, String> found = composedTargetWrites();
    Set<String> unlisted = new TreeSet<>(found.keySet());
    unlisted.removeAll(COMPOSED_TARGETS.keySet());
    Set<String> stale = new TreeSet<>(COMPOSED_TARGETS.keySet());
    stale.removeAll(found.keySet());
    if (!unlisted.isEmpty() || !stale.isEmpty()) {
      fail(
          "writes whose table the statement's text does not name, not listed in"
              + " SagaStateWriteRatchetTest.COMPOSED_TARGETS: "
              + unlisted
              + "; listed but not found: "
              + stale
              + ". Name saga_state in the statement's text and classify it, or list the write with"
              + " the reason its table is never saga_state.");
    }
  }

  /**
   * No literal of the main sources is the bare name of {@code saga_state} — the piece a statement
   * would compose a {@code saga_state} write from — outside the files {@link #LITERAL_TABLE_NAMES}
   * lists.
   */
  @Test
  void noLiteralNamesTheSagaStateTableAlone() {
    List<String> found = new ArrayList<>();
    for (Path file : mainJavaSources()) {
      String name = file.getFileName().toString();
      if (LITERAL_TABLE_NAMES.containsKey(name)) {
        continue;
      }
      for (Statement statement : Source.of(read(file)).statements()) {
        for (String literal : statement.literalList()) {
          if (NAMES_SAGA_STATE_ALONE.matcher(literal).matches()) {
            found.add(name + ": \"" + literal + "\"");
          }
        }
      }
    }
    assertTrue(
        found.isEmpty(),
        "a literal names saga_state alone; write the table's name in the statement's text, where"
            + " the classification reads it: "
            + found);
  }

  // ---------------------------------------------------------------------------------------------

  /** Every write of the main sources whose target the text does not name, by its key. */
  private static Map<String, String> composedTargetWrites() {
    Map<String, String> writes = new TreeMap<>();
    for (Path file : mainJavaSources()) {
      String name = file.getFileName().toString();
      for (Statement statement : Source.of(read(file)).statements()) {
        Matcher write = COMPOSED_TARGET.matcher(statement.shape());
        while (write.find()) {
          String text =
              statement
                  .shape()
                  .substring(
                      write.start(1), Math.min(statement.shape().length(), write.start(1) + 48))
                  .replace(String.valueOf(SUPPLIED), "<value>")
                  .replaceAll("\\s+", " ")
                  .trim();
          Matcher declaration = CONSTANT_DECLARATION.matcher(statement.code());
          String key =
              declaration.find()
                  ? name.replace(".java", "") + "." + declaration.group(2)
                  : name + ": " + text;
          writes.put(key, text);
        }
      }
    }
    return writes;
  }

  private static Verb verbOf(String sql) {
    Matcher write = SAGA_STATE_WRITE.matcher(sql);
    if (!write.find()) {
      throw new IllegalStateException("not a saga_state write: " + sql);
    }
    String head = write.group(1).toLowerCase(java.util.Locale.ROOT);
    if (head.startsWith("insert")) {
      return UPSERT.matcher(sql).find() ? Verb.UPSERT : Verb.INSERT;
    }
    if (head.startsWith("update")) {
      return Verb.UPDATE;
    }
    if (head.startsWith("delete")) {
      return Verb.DELETE;
    }
    return head.startsWith("merge") ? Verb.MERGE : Verb.TRUNCATE;
  }

  /** The top-level arguments of the call whose argument list starts at {@code from}. */
  private static List<String> arguments(String code, int from) {
    List<String> arguments = new ArrayList<>();
    int depth = 0;
    int start = from;
    for (int i = from; i < code.length(); i++) {
      char c = code.charAt(i);
      if (c == '(' || c == '{' || c == '[') {
        depth++;
      } else if ((c == ')' || c == '}' || c == ']') && depth > 0) {
        depth--;
      } else if (c == ')' && depth == 0) {
        arguments.add(code.substring(start, i).trim());
        return arguments;
      } else if (c == ',' && depth == 0) {
        arguments.add(code.substring(start, i).trim());
        start = i + 1;
      }
    }
    throw new IllegalStateException("unbalanced call from offset " + from);
  }

  private record Write(String file, String constant, String declaredType, String sql) {}

  /** Every statement of the main sources whose joined string literals write saga_state. */
  private static Map<String, Write> sagaStateWrites() {
    Map<String, Write> writes = new TreeMap<>();
    List<String> inline = new ArrayList<>();
    for (Path file : mainJavaSources()) {
      String name = file.getFileName().toString();
      for (Statement statement : Source.of(read(file)).statements()) {
        if (!SAGA_STATE_WRITE.matcher(statement.literals()).find()) {
          continue;
        }
        Matcher declaration = CONSTANT_DECLARATION.matcher(statement.code());
        if (!declaration.find()) {
          inline.add(file + ": " + statement.literals());
          continue;
        }
        String key = name.replace(".java", "") + "." + declaration.group(2);
        writes.put(key, new Write(name, key, declaration.group(1), statement.literals()));
      }
    }
    for (Path sql : sqlResources()) {
      String text = read(sql).replaceAll("--[^\n]*", "");
      if (SAGA_STATE_WRITE.matcher(text).find()) {
        inline.add(sql + ": a SQL resource writes saga_state");
      }
    }
    if (!inline.isEmpty()) {
      fail("saga_state written by an unnamed statement; name and classify it: " + inline);
    }
    return writes;
  }

  /**
   * One statement: its code (literals blanked), its literals joined, its shape — the literals
   * joined with a {@link #SUPPLIED} mark wherever code supplies a value between them — and each
   * literal.
   */
  private record Statement(String code, String literals, String shape, List<String> literalList) {}

  /**
   * A Java source split into statements at {@code ;}, {@code {} and {@code }} outside literals and
   * comments; {@link #code()} is the source with comments removed and literal contents blanked.
   */
  private record Source(String code, List<Statement> statements) {

    static Source of(String text) {
      StringBuilder code = new StringBuilder(text.length());
      List<Statement> statements = new ArrayList<>();
      StringBuilder stmtCode = new StringBuilder();
      StringBuilder stmtLiterals = new StringBuilder();
      StringBuilder stmtShape = new StringBuilder();
      List<String> stmtLiteralList = new ArrayList<>();
      int i = 0;
      int n = text.length();
      while (i < n) {
        char c = text.charAt(i);
        if (c == '/' && i + 1 < n && text.charAt(i + 1) == '/') {
          int end = text.indexOf('\n', i);
          i = end < 0 ? n : end;
          continue;
        }
        if (c == '/' && i + 1 < n && text.charAt(i + 1) == '*') {
          int end = text.indexOf("*/", i + 2);
          i = end < 0 ? n : end + 2;
          code.append(' ');
          continue;
        }
        if (c == '"' && text.startsWith("\"\"\"", i)) {
          int end = text.indexOf("\"\"\"", i + 3);
          int stop = end < 0 ? n : end + 3;
          String literal = text.substring(i + 3, Math.max(i + 3, stop - 3));
          stmtLiterals.append(literal);
          stmtShape.append(literal);
          stmtLiteralList.add(literal);
          code.append("\"\"\"\"\"\"");
          stmtCode.append("\"\"");
          i = stop;
          continue;
        }
        if (c == '"' || c == '\'') {
          int j = i + 1;
          while (j < n && text.charAt(j) != c) {
            j += text.charAt(j) == '\\' ? 2 : 1;
          }
          if (c == '"') {
            String literal = text.substring(i + 1, Math.min(j, n));
            stmtLiterals.append(literal);
            stmtShape.append(literal);
            stmtLiteralList.add(literal);
          } else {
            appendCode(stmtShape, c);
          }
          code.append(c).append(c);
          stmtCode.append(c).append(c);
          i = Math.min(j + 1, n);
          continue;
        }
        code.append(c);
        if (c == ';' || c == '{' || c == '}') {
          statements.add(
              new Statement(
                  stmtCode.toString(),
                  stmtLiterals.toString(),
                  stmtShape.toString(),
                  List.copyOf(stmtLiteralList)));
          stmtCode.setLength(0);
          stmtLiterals.setLength(0);
          stmtShape.setLength(0);
          stmtLiteralList.clear();
        } else {
          stmtCode.append(c);
          appendCode(stmtShape, c);
        }
        i++;
      }
      statements.add(
          new Statement(
              stmtCode.toString(),
              stmtLiterals.toString(),
              stmtShape.toString(),
              List.copyOf(stmtLiteralList)));
      return new Source(code.toString(), statements);
    }

    /**
     * Code between literals in a statement's shape: whitespace and {@code +} join two literals into
     * one text; any other code is a value the text does not show, one {@link #SUPPLIED} mark.
     */
    private static void appendCode(StringBuilder shape, char c) {
      if (Character.isWhitespace(c) || c == '+') {
        return;
      }
      if (shape.isEmpty() || shape.charAt(shape.length() - 1) != SUPPLIED) {
        shape.append(SUPPLIED);
      }
    }
  }

  private static int matchingBrace(String code, int open) {
    int depth = 0;
    for (int i = open; i < code.length(); i++) {
      char c = code.charAt(i);
      if (c == '{') {
        depth++;
      } else if (c == '}' && --depth == 0) {
        return i;
      }
    }
    throw new IllegalStateException("unbalanced braces from offset " + open);
  }

  private static List<Integer> indexesOf(String text, String needle) {
    List<Integer> at = new ArrayList<>();
    for (int i = text.indexOf(needle); i >= 0; i = text.indexOf(needle, i + 1)) {
      at.add(i);
    }
    return at;
  }

  private static Path only(String fileName) {
    List<Path> matches =
        mainJavaSources().stream()
            .filter(p -> p.getFileName().toString().equals(fileName))
            .toList();
    assertEquals(1, matches.size(), "exactly one " + fileName + " in the main sources");
    return matches.getFirst();
  }

  private static List<Path> mainJavaSources() {
    return filesMatching(rel -> rel.endsWith(".java") && rel.contains("/src/main/java/"));
  }

  private static List<Path> sqlResources() {
    return filesMatching(rel -> rel.endsWith(".sql") && rel.contains("/src/main/"));
  }

  private static String read(Path file) {
    try {
      return Files.readString(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static List<Path> filesMatching(java.util.function.Predicate<String> wanted) {
    Path root = repositoryRoot();
    List<Path> files = new ArrayList<>();
    try {
      Files.walkFileTree(
          root,
          new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
              return !dir.equals(root)
                      && PRUNED_DIRECTORY_NAMES.contains(dir.getFileName().toString())
                  ? FileVisitResult.SKIP_SUBTREE
                  : FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
              String rel = root.relativize(file).toString().replace('\\', '/');
              if (attrs.isRegularFile() && wanted.test("/" + rel)) {
                files.add(file);
              }
              return FileVisitResult.CONTINUE;
            }
          });
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    files.sort(java.util.Comparator.naturalOrder());
    return files;
  }

  /**
   * The repository's root build: the nearest directory with both a {@code settings.gradle.kts} and
   * the Gradle wrapper. {@code streamrune-eventstore} has a {@code settings.gradle.kts} of its own
   * and no wrapper; stopping there would confine the scan to the event-store modules.
   */
  private static Path repositoryRoot() {
    Path dir = Path.of("").toAbsolutePath();
    while (dir != null) {
      if (Files.exists(dir.resolve("settings.gradle.kts"))
          && Files.exists(dir.resolve("gradlew"))) {
        return dir;
      }
      dir = dir.getParent();
    }
    throw new IllegalStateException("cannot locate the repository root");
  }
}
