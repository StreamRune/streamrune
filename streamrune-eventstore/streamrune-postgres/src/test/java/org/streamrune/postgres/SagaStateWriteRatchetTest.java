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
 * {@code INSERT INTO saga_state}, {@code UPDATE saga_state} or {@code DELETE FROM saga_state}
 * (comments are ignored; the statement's string literals are joined, so a split literal is found
 * too) and fails for one that is not in {@link #CLASSIFIED}. Ask of the new statement: can it turn
 * the fence's verdict from "forward" to "refused" — that is, can it leave a row carrying an episode
 * stamp it did not carry, at a terminal status ({@code COMPLETED}, {@code COMPENSATED}, {@code
 * FAILED}), or remove the row?
 *
 * <ul>
 *   <li>Yes, for some bind values: it is {@link Kind#FENCE_CHANGING}. Declare it in {@code
 *       PostgresSagaStore} as a {@code FenceChangingWrite} and run it through {@code
 *       fenceChangingWrite(...)}, passing {@code leavesForwardPhase} true for every call whose bind
 *       values can make that turn (when in doubt, true: the lock then only costs a wait).
 *   <li>No, for every bind value — an insert (the fence refuses an absent row, so a new row turns
 *       nothing to "refused"), a write of a non-terminal status other than {@code COMPENSATING}
 *       that keeps the stamp as it is, a write of a column the fence does not read: it is {@link
 *       Kind#KEEPS_FORWARD_PHASE}. Add it with the reason, as a plain {@code String} constant.
 * </ul>
 *
 * <p>An inline SQL string (not a constant) that writes {@code saga_state} is refused outright: name
 * it, then classify it.
 */
class SagaStateWriteRatchetTest {

  enum Kind {
    FENCE_CHANGING,
    KEEPS_FORWARD_PHASE
  }

  private record Classified(Kind kind, String reason) {}

  private static final String SAGA_STORE = "PostgresSagaStore.java";

  /** Every {@code saga_state} write of the main sources, by {@code File.CONSTANT}. */
  private static final Map<String, Classified> CLASSIFIED =
      Map.of(
          "PostgresSagaStore.UPDATE_CAS",
          new Classified(
              Kind.FENCE_CHANGING,
              "update and claimCompensating: COMPENSATING stamps the episode, a terminal status"
                  + " ends the saga"),
          "PostgresSagaStore.APPLY_EVENT",
          new Classified(
              Kind.FENCE_CHANGING,
              "an evolved COMPENSATING stamps the episode, an evolved terminal status ends the"
                  + " saga"),
          "PostgresSagaStore.DELETE_BY_TYPE",
          new Classified(Kind.FENCE_CHANGING, "a forward command of a saga with no row is refused"),
          "PostgresSagaStore.INSERT_NEW",
          new Classified(
              Kind.KEEPS_FORWARD_PHASE, "an insert: the fence refused the absent row before it"),
          "PostgresSagaStore.INSERT_GENESIS_PENDING",
          new Classified(
              Kind.KEEPS_FORWARD_PHASE, "an insert: the fence refused the absent row before it"),
          "PostgresSagaStore.INSERT_FAULTED",
          new Classified(
              Kind.KEEPS_FORWARD_PHASE, "an insert: the fence refused the absent row before it"),
          "PostgresSagaStore.MARK_FAULTED",
          new Classified(
              Kind.KEEPS_FORWARD_PHASE,
              "FAULTED is not terminal and the statement does not touch the stamp"),
          "PostgresSagaStore.SET_DEAD_LETTER_PENDING",
          new Classified(Kind.KEEPS_FORWARD_PHASE, "the shield flag; the fence does not read it"),
          "PostgresSagaDeadLetterStore.CLEAR_SHIELD_IF_DRAINED",
          new Classified(Kind.KEEPS_FORWARD_PHASE, "the shield flag; the fence does not read it"),
          "PostgresSagaDeadLetterStore.ASSERT_SHIELD",
          new Classified(Kind.KEEPS_FORWARD_PHASE, "the shield flag; the fence does not read it"));

  private static final Pattern SAGA_STATE_WRITE =
      Pattern.compile("(?i)\\b(insert\\s+into|update|delete\\s+from)\\s+saga_state\\b");

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
      if (entry.getValue().kind() == Kind.FENCE_CHANGING) {
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

  // ---------------------------------------------------------------------------------------------

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

  private record Statement(String code, String literals) {}

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
          stmtLiterals.append(text, i + 3, Math.max(i + 3, stop - 3));
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
            stmtLiterals.append(text, i + 1, Math.min(j, n));
          }
          code.append(c).append(c);
          stmtCode.append(c).append(c);
          i = Math.min(j + 1, n);
          continue;
        }
        code.append(c);
        if (c == ';' || c == '{' || c == '}') {
          statements.add(new Statement(stmtCode.toString(), stmtLiterals.toString()));
          stmtCode.setLength(0);
          stmtLiterals.setLength(0);
        } else {
          stmtCode.append(c);
        }
        i++;
      }
      statements.add(new Statement(stmtCode.toString(), stmtLiterals.toString()));
      return new Source(code.toString(), statements);
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

  private static Path repositoryRoot() {
    Path dir = Path.of("").toAbsolutePath();
    while (dir != null) {
      if (Files.exists(dir.resolve("settings.gradle.kts"))) {
        return dir;
      }
      dir = dir.getParent();
    }
    throw new IllegalStateException("cannot locate the repository root");
  }
}
