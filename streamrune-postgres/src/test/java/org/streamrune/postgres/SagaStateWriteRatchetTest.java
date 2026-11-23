package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * and every write that can take a saga out of its forward phase belongs to its backend's saga
 * store. On Postgres it goes through {@code PostgresSagaStore}'s one fence-changing write path,
 * which takes the saga's exclusive fence lock ({@link SagaFenceLock}) before the statement. A
 * forward command's append reads the row under the shared form of that lock; a write that skipped
 * the exclusive lock could stamp an episode or end a saga while a forward command of it is
 * committing, and that command would be stored after the compensation began. On SQLite it is a
 * statement of {@code SqliteSagaStore}, which runs every write inside {@code BEGIN IMMEDIATE} …
 * {@code COMMIT}: the database-wide write lock it holds is the fence, so the statement needs no
 * fence lock of its own (its behaviour is pinned by the SQLite store's tests, not here). Saga state
 * is written by its backend's saga store only, and its shield by that backend's dead-letter store.
 *
 * <p><b>How to classify a new write.</b> The test finds every statement whose SQL text contains
 * {@code INSERT INTO saga_state}, {@code UPDATE saga_state}, {@code DELETE FROM saga_state}, {@code
 * MERGE INTO saga_state} or {@code TRUNCATE saga_state}, or one of SQLite's forms {@code INSERT OR
 * <action> INTO saga_state} ({@code IGNORE}, {@code REPLACE}, {@code ABORT}, {@code FAIL}, {@code
 * ROLLBACK}), {@code REPLACE INTO saga_state} and {@code UPDATE OR <action> saga_state}, with the
 * table bare, quoted ({@code "saga_state"}, {@code `saga_state`}, {@code [saga_state]}) or
 * qualified by a schema ({@code main.saga_state}, {@code public.saga_state}) (comments are ignored;
 * the statement's string literals are joined, so a split literal is found too) and fails for one
 * that is not in {@link #CLASSIFIED}, or whose kind of statement ({@link Verb}) is not the
 * classified one — an insert that gains {@code ON CONFLICT … DO UPDATE} is an upsert and is
 * classified again, as an update of the existing row. A {@code REPLACE} form ({@code INSERT OR
 * REPLACE}, {@code REPLACE INTO}, {@code UPDATE OR REPLACE}) deletes a conflicting row before it
 * writes, so, like a delete, a merge and a truncate, it is always fence-changing. Ask of the new
 * statement: can it turn the fence's verdict from "forward" to "refused" — that is, can it leave a
 * row carrying an episode stamp it did not carry, at a terminal status ({@code COMPLETED}, {@code
 * COMPENSATED}, {@code FAILED}), or remove the row?
 *
 * <ul>
 *   <li>Yes, for some bind values: it is {@link Kind#FENCE_CHANGING}. On Postgres, declare it in
 *       {@code PostgresSagaStore} as a {@code FenceChangingWrite} and run it through {@code
 *       fenceChangingWrite(...)}, passing {@code leavesForwardPhase} true for every call whose bind
 *       values can make that turn (when in doubt, true: the lock then only costs a wait). On
 *       SQLite, declare it in {@code SqliteSagaStore} as a plain {@code String}.
 *   <li>No, for every bind value — a plain insert or an insert that does nothing on conflict (the
 *       fence refuses an absent row, so a new row turns nothing to "refused"; an upsert updates the
 *       existing row and is judged as an update), a write of a non-terminal status other than
 *       {@code COMPENSATING} that keeps the stamp as it is, a write of a column the fence does not
 *       read: it is {@link Kind#KEEPS_FORWARD_PHASE}. Add it with the reason, as a plain {@code
 *       String} constant.
 * </ul>
 *
 * <p>An inline SQL string (not a constant) that writes {@code saga_state} is refused outright: name
 * it, then classify it. A delete, a merge, a truncate and a replace are always fence-changing. A
 * fence-changing write belongs to the saga store of its module ({@code streamrune-postgres}: {@code
 * PostgresSagaStore}, {@code streamrune-sqlite}: {@code SqliteSagaStore}); a module without a saga
 * store has none.
 *
 * <p><b>It runs on every build that changes what it scans.</b> The scan reads every module's main
 * sources, not only this module's, so it runs in a test task of its own, {@code
 * sagaStateWriteRatchetTest}, whose inputs {@code streamrune-postgres/build.gradle.kts} declares
 * them to be; {@link #everyScannedFileIsDeclaredAsAGradleInputOfTheTestTask} fails when the
 * declaration and the scan differ. The module's {@code test} task leaves the ratchet out, so a
 * main-source edit elsewhere does not re-run the module's whole suite; {@code check} and {@code
 * quickCheck} run the ratchet's task.
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
    TRUNCATE,
    /**
     * SQLite's {@code INSERT OR REPLACE INTO}, {@code REPLACE INTO} and {@code UPDATE OR REPLACE}:
     * a conflicting row is deleted and the new one written, so it can drop a stamp or replace a
     * terminal row.
     */
    REPLACE
  }

  private record Classified(Verb verb, Kind kind, String reason) {}

  private static final String SAGA_STORE = "PostgresSagaStore.java";

  /** The system property naming the manifest of the files Gradle declares as the task's input. */
  private static final String DECLARED_SOURCES_PROPERTY = "streamrune.sagastate.declared-sources";

  /**
   * The SQLite saga store: on SQLite every write holds the database-wide write lock from its {@code
   * BEGIN IMMEDIATE} to its commit, and that lock is the fence, so a fence-changing write there is
   * a plain statement of this store, run in its write transaction, with no fence lock of its own.
   */
  private static final String SQLITE_SAGA_STORE = "SqliteSagaStore.java";

  /**
   * The saga store of each backend module: a fence-changing write of a module belongs to that
   * module's saga store, and a module not listed has none.
   */
  private static final Map<String, String> SAGA_STORES =
      Map.of("streamrune-postgres", SAGA_STORE, "streamrune-sqlite", SQLITE_SAGA_STORE);

  /** Every {@code saga_state} write of the main sources, by {@code File.CONSTANT}. */
  private static final Map<String, Classified> CLASSIFIED =
      Map.ofEntries(
          Map.entry(
              "PostgresSagaStore.UPDATE_CAS",
              new Classified(
                  Verb.UPDATE,
                  Kind.FENCE_CHANGING,
                  "update and claimCompensating: COMPENSATING stamps the episode, a terminal status"
                      + " ends the saga")),
          Map.entry(
              "PostgresSagaStore.APPLY_EVENT",
              new Classified(
                  Verb.UPDATE,
                  Kind.FENCE_CHANGING,
                  "an evolved COMPENSATING stamps the episode, an evolved terminal status ends the"
                      + " saga")),
          Map.entry(
              "PostgresSagaStore.DELETE_BY_TYPE",
              new Classified(
                  Verb.DELETE,
                  Kind.FENCE_CHANGING,
                  "a forward command of a saga with no row is refused")),
          Map.entry(
              "PostgresSagaStore.INSERT_NEW",
              new Classified(
                  Verb.INSERT,
                  Kind.KEEPS_FORWARD_PHASE,
                  "an insert: the fence refused the absent row before it")),
          Map.entry(
              "PostgresSagaStore.INSERT_GENESIS_PENDING",
              new Classified(
                  Verb.INSERT,
                  Kind.KEEPS_FORWARD_PHASE,
                  "an insert: the fence refused the absent row before it")),
          Map.entry(
              "PostgresSagaStore.INSERT_FAULTED",
              new Classified(
                  Verb.INSERT,
                  Kind.KEEPS_FORWARD_PHASE,
                  "an insert: the fence refused the absent row before it")),
          Map.entry(
              "PostgresSagaStore.MARK_FAULTED",
              new Classified(
                  Verb.UPDATE,
                  Kind.KEEPS_FORWARD_PHASE,
                  "FAULTED is not terminal and the statement does not touch the stamp")),
          Map.entry(
              "PostgresSagaStore.SET_DEAD_LETTER_PENDING",
              new Classified(
                  Verb.UPDATE,
                  Kind.KEEPS_FORWARD_PHASE,
                  "the shield flag; the fence does not read it")),
          Map.entry(
              "PostgresSagaDeadLetterStore.CLEAR_SHIELD_IF_DRAINED",
              new Classified(
                  Verb.UPDATE,
                  Kind.KEEPS_FORWARD_PHASE,
                  "the shield flag; the fence does not read it")),
          Map.entry(
              "PostgresSagaDeadLetterStore.ASSERT_SHIELD",
              new Classified(
                  Verb.UPDATE,
                  Kind.KEEPS_FORWARD_PHASE,
                  "the shield flag; the fence does not read it")),
          Map.entry(
              "SqliteSagaStore.UPDATE_CAS",
              new Classified(
                  Verb.UPDATE,
                  Kind.FENCE_CHANGING,
                  "update and claimCompensating, on SQLite: COMPENSATING stamps the episode, a terminal status ends the saga")),
          Map.entry(
              "SqliteSagaStore.APPLY_EVENT",
              new Classified(
                  Verb.UPDATE,
                  Kind.FENCE_CHANGING,
                  "on SQLite: an evolved COMPENSATING stamps the episode, an evolved terminal status ends the saga")),
          Map.entry(
              "SqliteSagaStore.DELETE_BY_TYPE",
              new Classified(
                  Verb.DELETE,
                  Kind.FENCE_CHANGING,
                  "on SQLite: a forward command of a saga with no row is refused")),
          Map.entry(
              "SqliteSagaStore.INSERT_NEW",
              new Classified(
                  Verb.INSERT,
                  Kind.KEEPS_FORWARD_PHASE,
                  "an insert: the fence refused the absent row before it")),
          Map.entry(
              "SqliteSagaStore.INSERT_GENESIS_PENDING",
              new Classified(
                  Verb.INSERT,
                  Kind.KEEPS_FORWARD_PHASE,
                  "an insert: the fence refused the absent row before it")),
          Map.entry(
              "SqliteSagaStore.INSERT_FAULTED",
              new Classified(
                  Verb.INSERT,
                  Kind.KEEPS_FORWARD_PHASE,
                  "an insert: the fence refused the absent row before it")),
          Map.entry(
              "SqliteSagaStore.MARK_FAULTED",
              new Classified(
                  Verb.UPDATE,
                  Kind.KEEPS_FORWARD_PHASE,
                  "FAULTED is not terminal and the statement does not touch the stamp")),
          Map.entry(
              "SqliteSagaStore.SET_DEAD_LETTER_PENDING",
              new Classified(
                  Verb.UPDATE,
                  Kind.KEEPS_FORWARD_PHASE,
                  "the shield flag; the fence does not read it")),
          Map.entry(
              "SqliteSagaDeadLetterStore.CLEAR_SHIELD_IF_DRAINED",
              new Classified(
                  Verb.UPDATE,
                  Kind.KEEPS_FORWARD_PHASE,
                  "the shield flag; the fence does not read it")),
          Map.entry(
              "SqliteSagaDeadLetterStore.ASSERT_SHIELD",
              new Classified(
                  Verb.UPDATE,
                  Kind.KEEPS_FORWARD_PHASE,
                  "the shield flag; the fence does not read it")));

  /** Why a write of {@code PostgresCheckpointFence} never targets {@code saga_state}. */
  private static final String CHECKPOINT_TABLE =
      "a checkpoint table, named only by the CheckpointTable enum: subscription_checkpoint or"
          + " readmodel_checkpoint";

  /**
   * Every write whose table the statement's text does not name, with the reason its table is never
   * {@code saga_state}. Keyed by {@code File.CONSTANT}, or for an unnamed statement by {@code File:
   * VERB #n}, the n-th such write with that verb in the file.
   */
  private static final Map<String, String> COMPOSED_TARGETS =
      Map.of(
          "JdbcProjectionRepository.UPSERT",
          "a projection's read-model table: the projection's name plus _view",
          "JdbcProjectionRepository.VERSIONED_UPDATE",
          "a projection's read-model table: the projection's name plus _view",
          "JdbcProjectionRepository.DELETE",
          "a projection's read-model table: the projection's name plus _view",
          "PostgresCryptoEngine.java: DELETE FROM #1",
          "the crypto engine's key table (streamrune.crypto.postgres.table-name, default"
              + " encryption_keys)",
          "PostgresCryptoEngine.java: INSERT INTO #1",
          "the crypto engine's key table (streamrune.crypto.postgres.table-name, default"
              + " encryption_keys)",
          "PostgresCheckpointFence.SEED",
          CHECKPOINT_TABLE,
          "PostgresCheckpointFence.SAVE_LOCKED",
          CHECKPOINT_TABLE,
          "PostgresCheckpointFence.STAMP",
          CHECKPOINT_TABLE,
          "PostgresCheckpointFence.RESET",
          CHECKPOINT_TABLE,
          "PostgresCheckpointFence.SAVE_UNLOCKED",
          CHECKPOINT_TABLE);

  /** The files whose literals may name {@code saga_state} alone, with the reason. */
  private static final Map<String, String> LITERAL_TABLE_NAMES =
      Map.of(
          "SchemaValidator.java",
          "the tables the schema check expects to find; it reads the catalogue and writes nothing",
          "SqliteSchemaValidator.java",
          "the tables the SQLite schema check expects to find; it reads the catalogue and writes"
              + " nothing");

  /** In a statement's shape: a value the code supplies between two literals. */
  private static final char SUPPLIED = '\u0001';

  /**
   * The verb of a write, as group 1: Postgres' {@code INSERT INTO}, {@code UPDATE [ONLY]}, {@code
   * DELETE FROM [ONLY]}, {@code MERGE INTO} and {@code TRUNCATE [TABLE] [ONLY]}, and SQLite's
   * {@code INSERT OR <action> INTO}, {@code REPLACE INTO} and {@code UPDATE OR <action>}.
   */
  private static final String WRITE_VERB =
      "(insert\\s+or\\s+\\w+\\s+into|insert\\s+into|replace\\s+into|update\\s+or\\s+\\w+"
          + "|update(?:\\s+only)?|delete\\s+from(?:\\s+only)?|merge\\s+into"
          + "|truncate(?:\\s+table)?(?:\\s+only)?)";

  /**
   * An identifier quote around a table or schema name: a double quote (escaped or not in the Java
   * literal), a backtick, a bracket, or a single quote (SQLite takes a string literal where it
   * expects a name: {@code DELETE FROM 'saga_state'} deletes from the table).
   */
  private static final String QUOTE_CHAR = "(?:\\\\?\"|`|\\[|\\]|\\\\?')";

  /** {@link #QUOTE_CHAR}, or none. */
  private static final String QUOTE = QUOTE_CHAR + "?";

  /**
   * A write whose verb is followed by a value the text does not show: a supplied value (a
   * concatenated constant, variable or call) or a format placeholder ({@code %s}, {@code %1$s},
   * {@code {0}}), after a space or a quote, alone or behind a literal schema ({@code UPDATE
   * app.<value>}); {@code ONLY} may follow the verb. The verb starts a statement's text, or follows
   * {@code ;}, an opening or closing parenthesis (a common table expression) or a supplied value:
   * {@code FOR UPDATE}, {@code DO UPDATE}, a message such as {@code "saga update"} and a lone word
   * {@code "update"} are not writes.
   */
  private static final Pattern COMPOSED_TARGET =
      Pattern.compile(
          "(?i)(?:^|[;()"
              + SUPPLIED
              + "])\\s*"
              + WRITE_VERB
              + "(?:\\s+"
              + QUOTE
              + "|"
              + QUOTE_CHAR
              + ")\\s*(?:"
              + QUOTE
              + "\\w+"
              + QUOTE
              + "\\s*\\.\\s*"
              + QUOTE
              + ")?(?:"
              + SUPPLIED
              + "|%(?:\\d+\\$)?s|\\{\\d*\\})");

  /**
   * A literal that is the name of the {@code saga_state} table and nothing else (quoted, or
   * qualified by a schema, or the qualified tail {@code .saga_state}): a name a statement could be
   * composed from.
   */
  private static final Pattern NAMES_SAGA_STATE_ALONE =
      Pattern.compile(
          "(?i)\\s*(?:"
              + QUOTE
              + "\\w+"
              + QUOTE
              + ")?\\s*\\.?\\s*"
              + QUOTE
              + "saga_state"
              + QUOTE
              + "\\s*");

  /**
   * A write of {@code saga_state}: a {@link #WRITE_VERB}, then the table, bare, quoted, or
   * qualified by a schema ({@code main.saga_state}, {@code public.saga_state}, {@code
   * "saga_state"}).
   */
  private static final Pattern SAGA_STATE_WRITE =
      Pattern.compile(
          "(?i)\\b"
              + WRITE_VERB
              + "(?:\\s+|(?="
              + QUOTE_CHAR
              + "))"
              + "(?:"
              + QUOTE
              + "\\w+"
              + QUOTE
              + "\\s*\\.\\s*)?"
              + QUOTE
              + "saga_state"
              + QUOTE
              + "(?!\\w)");

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

  /**
   * The kinds of statement that can remove a row, whatever the bind values: never classified else.
   */
  private static final Set<Verb> ALWAYS_FENCE_CHANGING =
      Set.of(Verb.DELETE, Verb.MERGE, Verb.TRUNCATE, Verb.REPLACE);

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
      if (ALWAYS_FENCE_CHANGING.contains(classified.verb())) {
        assertEquals(
            Kind.FENCE_CHANGING,
            classified.kind(),
            entry.getKey()
                + ": a "
                + classified.verb()
                + " of saga_state is always fence-changing");
      }
      if (classified.kind() == Kind.FENCE_CHANGING) {
        String sagaStore = SAGA_STORES.get(write.module());
        assertTrue(
            sagaStore != null,
            entry.getKey()
                + " changes the fence's verdict but is in module "
                + write.module()
                + ", which has no saga store: it belongs to a backend's saga store "
                + SAGA_STORES);
        assertEquals(
            sagaStore,
            write.file(),
            entry.getKey()
                + " changes the fence's verdict: it belongs to "
                + sagaStore
                + ", the saga store of its module "
                + write.module());
        if (sagaStore.equals(SAGA_STORE)) {
          assertEquals(
              "FenceChangingWrite",
              write.declaredType(),
              entry.getKey()
                  + " changes the fence's verdict: declare it as a FenceChangingWrite so it can"
                  + " only run through fenceChangingWrite(...)");
        } else {
          assertEquals(
              "String",
              write.declaredType(),
              entry.getKey()
                  + ": on SQLite the write lock is the fence, so the write is a plain String of"
                  + " the saga store");
        }
      } else {
        assertEquals(
            "String",
            write.declaredType(),
            entry.getKey() + " is classified as keeping the forward phase: a plain String");
      }
    }
  }

  /**
   * The module a main source belongs to: the directory that holds its {@code src/main}, relative to
   * the repository root.
   */
  private static String moduleOf(Path file) {
    String rel = repositoryRoot().relativize(file).toString().replace('\\', '/');
    int src = rel.indexOf("/src/main/");
    return src < 0 ? "" : rel.substring(0, src);
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
        assertFalse(
            otherCode.contains("FenceChangingWrite"),
            other + " must not use FenceChangingWrite: the saga store's path is the only one");
      }
    }

    int start = code.indexOf("T fenceChangingWrite(");
    assertTrue(start >= 0, "PostgresSagaStore declares fenceChangingWrite(...)");
    int bodyStart = code.indexOf('{', start);
    int bodyEnd = matchingBrace(code, bodyStart);
    String body = code.substring(bodyStart, bodyEnd);

    List<Integer> sqlReads = indexesOf(code, ".sql()");
    assertFalse(sqlReads.isEmpty(), "fenceChangingWrite reads the statement");
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
   * The write grammar finds every form of a {@code saga_state} write, Postgres' and SQLite's, with
   * the table bare, quoted or qualified, and reads its kind of statement; reads and other tables
   * are not writes of it.
   */
  @Test
  void theWriteGrammarFindsEveryFormOfASagaStateWrite() {
    Map<String, Verb> writes = new java.util.LinkedHashMap<>();
    writes.put("INSERT INTO saga_state (x) VALUES (?)", Verb.INSERT);
    writes.put(
        "INSERT INTO saga_state (x) VALUES (?) ON CONFLICT (x) DO UPDATE SET x = 1", Verb.UPSERT);
    writes.put("INSERT OR IGNORE INTO saga_state (x) VALUES (?)", Verb.INSERT);
    writes.put("insert or abort into saga_state (x) values (?)", Verb.INSERT);
    writes.put("INSERT OR REPLACE INTO saga_state (x) VALUES (?)", Verb.REPLACE);
    writes.put("REPLACE INTO saga_state (x) VALUES (?)", Verb.REPLACE);
    writes.put("UPDATE OR REPLACE saga_state SET x = 1", Verb.REPLACE);
    writes.put("UPDATE OR IGNORE saga_state SET x = 1", Verb.UPDATE);
    writes.put("UPDATE saga_state SET x = 1", Verb.UPDATE);
    writes.put("UPDATE ONLY saga_state SET x = 1", Verb.UPDATE);
    writes.put("UPDATE main.saga_state SET x = 1", Verb.UPDATE);
    writes.put("UPDATE public . saga_state SET x = 1", Verb.UPDATE);
    writes.put("UPDATE \"saga_state\" SET x = 1", Verb.UPDATE);
    // As a Java literal keeps it: the quotes escaped.
    writes.put("UPDATE \\\"public\\\".\\\"saga_state\\\" SET x = 1", Verb.UPDATE);
    writes.put("UPDATE`saga_state` SET x = 1", Verb.UPDATE);
    writes.put("UPDATE [saga_state] SET x = 1", Verb.UPDATE);
    writes.put("DELETE FROM 'saga_state' WHERE x = 1", Verb.DELETE);
    writes.put("UPDATE 'main'.'saga_state' SET x = 1", Verb.UPDATE);
    writes.put("UPDATE `main`.[saga_state] SET x = 1", Verb.UPDATE);
    writes.put("DELETE FROM main.saga_state WHERE x = 1", Verb.DELETE);
    writes.put("MERGE INTO saga_state s USING t ON s.x = t.x", Verb.MERGE);
    writes.put("TRUNCATE TABLE saga_state", Verb.TRUNCATE);
    for (var write : writes.entrySet()) {
      assertTrue(SAGA_STATE_WRITE.matcher(write.getKey()).find(), "a write: " + write.getKey());
      assertEquals(write.getValue(), verbOf(write.getKey()), write.getKey());
    }
    for (String notAWrite :
        List.of(
            "SELECT status FROM saga_state WHERE saga_id = ?",
            "SELECT status FROM saga_state WHERE saga_id = ? FOR UPDATE",
            "UPDATE saga_state_archive SET x = 1",
            "UPDATE main.saga_state_archive SET x = 1",
            "INSERT INTO saga_dead_letters (x) VALUES (?) ON CONFLICT (x) DO UPDATE SET x = 1",
            "CREATE TABLE saga_state (x INTEGER)")) {
      assertFalse(SAGA_STATE_WRITE.matcher(notAWrite).find(), "not a write: " + notAWrite);
    }
    assertTrue(
        ALWAYS_FENCE_CHANGING.contains(Verb.REPLACE),
        "a REPLACE deletes the conflicting row: always fence-changing");
  }

  /** The composed-target grammar knows SQLite's write forms too. */
  @Test
  void theComposedTargetGrammarFindsEveryWriteForm() {
    for (String verb :
        List.of(
            "INSERT INTO ",
            "INSERT OR REPLACE INTO ",
            "INSERT OR IGNORE INTO ",
            "REPLACE INTO ",
            "UPDATE OR REPLACE ",
            "UPDATE ",
            "DELETE FROM ",
            "UPDATE main.",
            "UPDATE '",
            "UPDATE `",
            "UPDATE [",
            "UPDATE 'main'.'",
            "UPDATE `main`.`",
            "UPDATE [main].[")) {
      String shape = verb + SUPPLIED + " SET x = 1";
      assertTrue(COMPOSED_TARGET.matcher(shape).find(), "a composed write: " + verb);
    }
  }

  /**
   * A literal is read as the database receives it: escapes decoded and SQL comments removed before
   * any pattern is matched, so a line break written as an escape, a Unicode escape or a comment
   * between verb and table hides no write, no composed target and no bare table name.
   */
  @Test
  void literalsAreNormalisedBeforeTheyAreMatched() {
    for (String literal :
        List.of(
            "UPDATE\\nsaga_state SET x = 1",
            "UPDATE\\tsaga_state SET x = 1",
            "UPDATE\\ssaga_state SET x = 1",
            "UPDATE\\012saga_state SET x = 1",
            "UPDATE \\u0073aga_state SET x = 1",
            "UPDATE \\uuu0073aga_state SET x = 1",
            "UPDATE/* a comment */saga_state SET x = 1",
            "UPDATE -- a comment\\n saga_state SET x = 1",
            "DELETE FROM /* x */ main /* y */ . /* z */ saga_state")) {
      Statement statement = only(Source.of("String A = \"" + literal + "\";").statements());
      assertTrue(SAGA_STATE_WRITE.matcher(statement.literals()).find(), "a write: " + literal);
    }
    Statement split =
        only(Source.of("String A = \"UPDATE /* a \" + \" b */ saga_state\";").statements());
    assertTrue(SAGA_STATE_WRITE.matcher(split.literals()).find(), "a comment split over literals");

    for (String composed :
        List.of(
            "String A = \"UPDATE /* c */ \" + table + \" SET x = 1\";",
            "String A = \"UPDATE -- c\\n\" + table + \" SET x = 1\";",
            "String A = \"UPDATE\\n'\" + table + \"' SET x = 1\";")) {
      Statement statement = only(Source.of(composed).statements());
      assertTrue(COMPOSED_TARGET.matcher(statement.shape()).find(), "composed: " + composed);
    }

    for (String alone :
        List.of(
            "'saga_state'",
            "`saga_state`",
            "[saga_state]",
            "main.'saga_state'",
            "/* c */saga_state",
            "saga\\u005fstate",
            "saga_state -- the table")) {
      Statement statement = only(Source.of("String A = \"" + alone + "\";").statements());
      assertTrue(
          NAMES_SAGA_STATE_ALONE.matcher(statement.literalList().getFirst()).matches(),
          "names the table alone: " + alone);
    }

    // Decoding follows the compiler: an escaped backslash is not the start of an escape.
    assertEquals("a\\u0073", decodeEscapes("a\\\\u0073"));
    assertEquals("a\\qb\"'", decodeEscapes("a\\qb\\\"\\'"));
    assertEquals("ab", decodeEscapes("a\\\nb"), "a text block's line continuation");
    assertEquals("a b", stripSqlComments("a/* x\n y */b"));
    assertEquals("a  \nb", stripSqlComments("a -- x\nb"));
    assertEquals("a  " + SUPPLIED + "b", stripSqlComments("a -- x" + SUPPLIED + "b"));
    // Comment marks inside a SQL string or quoted name are text, not comments.
    assertEquals("SELECT '--'; UPDATE x", stripSqlComments("SELECT '--'; UPDATE x"));
    assertEquals("SELECT 'it''s -- /*'; x", stripSqlComments("SELECT 'it''s -- /*'; x"));
    assertTrue(
        SAGA_STATE_WRITE
            .matcher(
                only(Source.of("String A = \"SELECT '--'; UPDATE saga_state SET x = 1\";")
                        .statements())
                    .literals())
            .find(),
        "a -- inside a SQL string hides no write after it");
  }

  /**
   * {@code SagaFence} of {@code streamrune-sqlite} reads {@code saga_state} inside the append's
   * transaction and writes nothing: the ratchet sees its read and does not classify it as a write.
   */
  @Test
  void theSqliteFenceReadIsAReadAndNotAWrite() {
    Path fence = only("SagaFence.java");
    assertEquals("streamrune-sqlite", moduleOf(fence));
    List<Statement> statements = Source.of(read(fence)).statements();
    assertTrue(
        statements.stream()
            .anyMatch(s -> s.literals().matches("(?is).*\\bfrom\\s+saga_state\\b.*")),
        "SagaFence reads saga_state");
    assertTrue(
        statements.stream().noneMatch(s -> SAGA_STATE_WRITE.matcher(s.literals()).find()),
        "SagaFence writes no saga_state row");
    assertTrue(
        statements.stream().noneMatch(s -> COMPOSED_TARGET.matcher(s.shape()).find()),
        "SagaFence composes no write");
  }

  private static Statement only(List<Statement> statements) {
    List<Statement> withLiterals =
        statements.stream().filter(s -> !s.literalList().isEmpty()).toList();
    assertEquals(1, withLiterals.size(), "one statement with literals: " + statements);
    return withLiterals.getFirst();
  }

  /**
   * The ratchet's task ({@code sagaStateWriteRatchetTest}) re-runs when a file this ratchet scans
   * changes: every scanned file is declared as a Gradle input of the task ({@code
   * streamrune-postgres/build.gradle.kts}, which hands the declared list over as a manifest), and
   * every declared file is scanned. Fails rather than skips when the manifest is missing: the
   * wiring disappearing silently is the defect this guards.
   */
  @Test
  void everyScannedFileIsDeclaredAsAGradleInputOfTheTestTask() {
    String manifestPath = System.getProperty(DECLARED_SOURCES_PROPERTY);
    if (manifestPath == null) {
      fail(
          "-D"
              + DECLARED_SOURCES_PROPERTY
              + " is not set, so the declared-inputs check cannot run. The ratchet's Gradle task"
              + " supplies it (streamrune-postgres/build.gradle.kts); run `./gradlew"
              + " :streamrune-postgres:sagaStateWriteRatchetTest`. If that wiring was removed, the"
              + " ratchet stops re-running when another module changes; restore it rather than"
              + " deleting this test.");
    }
    Path root = repositoryRoot();
    Set<String> scanned = new TreeSet<>();
    for (Path file : mainJavaSources()) {
      scanned.add(root.relativize(file).toString().replace('\\', '/'));
    }
    for (Path file : sqlResources()) {
      scanned.add(root.relativize(file).toString().replace('\\', '/'));
    }
    Set<String> declared = new TreeSet<>();
    for (String line : read(Path.of(manifestPath)).split("\n")) {
      if (!line.isBlank()) {
        declared.add(line.strip());
      }
    }
    Set<String> scannedButNotDeclared = new TreeSet<>(scanned);
    scannedButNotDeclared.removeAll(declared);
    Set<String> declaredButNotScanned = new TreeSet<>(declared);
    declaredButNotScanned.removeAll(scanned);
    assertTrue(
        scannedButNotDeclared.isEmpty() && declaredButNotScanned.isEmpty(),
        "the ratchet's Gradle inputs no longer match what it scans; fix the globs in"
            + " streamrune-postgres/build.gradle.kts.\n  scanned but NOT declared (a change to"
            + " these would not re-run this test): "
            + scannedButNotDeclared
            + "\n  declared but not scanned: "
            + declaredButNotScanned);
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
      Map<String, Integer> unnamedByVerb = new TreeMap<>();
      for (Statement statement : Source.of(read(file)).statements()) {
        Matcher write = COMPOSED_TARGET.matcher(statement.shape());
        while (write.find()) {
          String verb = write.group(1).toUpperCase(java.util.Locale.ROOT).replaceAll("\\s+", " ");
          String text =
              statement
                  .shape()
                  .substring(write.start(1))
                  .replace(String.valueOf(SUPPLIED), "<value>")
                  .replaceAll("\\s+", " ")
                  .trim();
          Matcher declaration = CONSTANT_DECLARATION.matcher(statement.code());
          String key =
              declaration.find()
                  ? name.replace(".java", "") + "." + declaration.group(2)
                  : name + ": " + verb + " #" + unnamedByVerb.merge(verb, 1, Integer::sum);
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
    String head = write.group(1).toLowerCase(java.util.Locale.ROOT).replaceAll("\\s+", " ");
    if (head.startsWith("replace") || head.matches("(insert|update) or replace\\b.*")) {
      return Verb.REPLACE;
    }
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

  private record Write(
      String module, String file, String constant, String declaredType, String sql) {}

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
        writes.put(
            key, new Write(moduleOf(file), name, key, declaration.group(1), statement.literals()));
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
   * literal. Each literal's escapes are decoded when it is read ({@link #decodeEscapes}); the
   * joined text, the shape and each literal have their SQL comments removed ({@link
   * #stripSqlComments}), so that neither an escaped line break between verb and table nor a comment
   * between them hides a write.
   */
  private record Statement(String code, String literals, String shape, List<String> literalList) {

    static Statement of(String code, String literals, String shape, List<String> literalList) {
      return new Statement(
          code,
          stripSqlComments(literals),
          stripSqlComments(shape),
          literalList.stream().map(SagaStateWriteRatchetTest::stripSqlComments).toList());
    }
  }

  /**
   * Replaces each {@code /* ... *}{@code /} comment and each {@code --} comment with a space,
   * outside SQL quotes: a {@code '...'} string or a {@code "..."} identifier is copied as it is (a
   * doubled quote stays inside), so a {@code --} or {@code /*} written inside one does not swallow
   * the text after it. A line comment ends at a line break or at a {@link #SUPPLIED} mark: the
   * value the code supplies there may hold the line break. An unterminated block comment is kept.
   */
  static String stripSqlComments(String sql) {
    StringBuilder out = new StringBuilder(sql.length());
    int i = 0;
    int n = sql.length();
    while (i < n) {
      char c = sql.charAt(i);
      if (c == '\'' || c == '"') {
        int end = i + 1;
        while (end < n && (sql.charAt(end) != c || (end + 1 < n && sql.charAt(end + 1) == c))) {
          end += sql.charAt(end) == c ? 2 : 1;
        }
        end = Math.min(end + 1, n);
        out.append(sql, i, end);
        i = end;
      } else if (sql.startsWith("/*", i) && sql.indexOf("*/", i + 2) >= 0) {
        out.append(' ');
        i = sql.indexOf("*/", i + 2) + 2;
      } else if (sql.startsWith("--", i)) {
        out.append(' ');
        while (i < n && sql.charAt(i) != '\n' && sql.charAt(i) != SUPPLIED) {
          i++;
        }
      } else {
        out.append(c);
        i++;
      }
    }
    return out.toString();
  }

  /**
   * Decodes the escapes of a Java string literal or text block, as the compiler does: Unicode
   * escapes first (a backslash that is not itself escaped, one or more {@code u}, four hex digits),
   * then the character escapes ({@code b t n f r s " ' } and the backslash), octal escapes and a
   * text block's line continuation. An unknown escape is kept as written.
   */
  static String decodeEscapes(String text) {
    StringBuilder unicode = new StringBuilder(text.length());
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == '\\' && i + 1 < text.length() && text.charAt(i + 1) == '\\') {
        // An escaped backslash: neither it nor the one after it starts a Unicode escape.
        unicode.append(c).append(c);
        i++;
        continue;
      }
      if (c == '\\' && i + 1 < text.length() && text.charAt(i + 1) == 'u') {
        int j = i + 1;
        while (j < text.length() && text.charAt(j) == 'u') {
          j++;
        }
        if (j + 4 <= text.length() && text.substring(j, j + 4).matches("[0-9a-fA-F]{4}")) {
          unicode.append((char) Integer.parseInt(text.substring(j, j + 4), 16));
          i = j + 3;
          continue;
        }
      }
      unicode.append(c);
    }
    String raw = unicode.toString();
    StringBuilder out = new StringBuilder(raw.length());
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (c != '\\' || i + 1 >= raw.length()) {
        out.append(c);
        continue;
      }
      char next = raw.charAt(++i);
      switch (next) {
        case 'b' -> out.append('\b');
        case 't' -> out.append('\t');
        case 'n' -> out.append('\n');
        case 'f' -> out.append('\f');
        case 'r' -> out.append('\r');
        case 's' -> out.append(' ');
        case '"', '\'', '\\' -> out.append(next);
        case '\n' -> {} // a text block's line continuation
        case '\r' -> {
          if (i + 1 < raw.length() && raw.charAt(i + 1) == '\n') {
            i++;
          }
        }
        default -> {
          if (next >= '0' && next <= '7') {
            int end = i + 1;
            int max = next <= '3' ? i + 3 : i + 2;
            while (end < Math.min(max, raw.length())
                && raw.charAt(end) >= '0'
                && raw.charAt(end) <= '7') {
              end++;
            }
            out.append((char) Integer.parseInt(raw.substring(i, end), 8));
            i = end - 1;
          } else {
            out.append('\\').append(next);
          }
        }
      }
    }
    return out.toString();
  }

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
          String literal = decodeEscapes(text.substring(i + 3, Math.max(i + 3, stop - 3)));
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
            String literal = decodeEscapes(text.substring(i + 1, Math.min(j, n)));
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
              Statement.of(
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
          Statement.of(
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
   * the Gradle wrapper, so the scan covers every module whatever directory the test runs from.
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
