package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.SagaType;

/** The key of the saga fence lock. */
class SagaFenceLockTest {

  /**
   * The event store and the saga store compute the key independently, also in two versions of an
   * application during a rolling deployment: the hash is a protocol and must not change.
   */
  @Test
  void theKeyIsAFixedFunctionOfTheSagaTypeAndId() {
    assertEquals(0x53524746, SagaFenceLock.CLASS_ID);
    assertEquals(
        1173324710, SagaFenceLock.objectId(SagaType.of("fence.Saga"), SagaId.of("saga-1")));
    assertNotEquals(
        SagaFenceLock.objectId(SagaType.of("fence.Saga"), SagaId.of("saga-1")),
        SagaFenceLock.objectId(SagaType.of("fence.Other"), SagaId.of("saga-1")),
        "the type is part of the key");
  }

  /**
   * The exclusive lock's statement, as the saga store sends it: the isolation pin first, the
   * transaction's idle bound when the row-write bound is set, the lock last — one round trip.
   */
  @Test
  void theExclusiveLockStatementPinsTheIsolationBoundsTheIdleTimeAndLocks() throws Exception {
    SagaType type = SagaType.of("fence.Saga");
    SagaId id = SagaId.of("saga-1");
    String lock = "SELECT pg_advisory_xact_lock(1397901126, 1173324710)";

    Connection bounded = mock(Connection.class);
    Statement boundedStatement = mock(Statement.class);
    when(bounded.createStatement()).thenReturn(boundedStatement);
    SagaFenceLock.lockExclusive(bounded, type, id, StatementTimeout.of(Duration.ofMillis(5000)));
    verify(boundedStatement)
        .execute(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED;"
                + " SET LOCAL idle_in_transaction_session_timeout = 5000; "
                + lock);
    verify(boundedStatement).setQueryTimeout(5);

    Connection unbounded = mock(Connection.class);
    Statement unboundedStatement = mock(Statement.class);
    when(unbounded.createStatement()).thenReturn(unboundedStatement);
    SagaFenceLock.lockExclusive(unbounded, type, id, StatementTimeout.DISABLED);
    verify(unboundedStatement).execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED; " + lock);
    verify(unboundedStatement, never()).setQueryTimeout(org.mockito.ArgumentMatchers.anyInt());
  }

  /**
   * No other main source takes a two-key advisory lock, so nothing else can meet the saga fence's
   * key space; one-key locks ({@code pg_advisory_xact_lock(bigint)}) live in a separate space.
   */
  @Test
  void onlyTheSagaFenceTakesATwoKeyAdvisoryLock() {
    Pattern twoKeyLock =
        Pattern.compile("pg_(?:try_)?advisory_(?:xact_)?lock(?:_shared)?\\s*\\([^(),]*,");
    List<String> others = new ArrayList<>();
    for (Path file : mainSources()) {
      if (file.getFileName().toString().equals("SagaFenceLock.java")) {
        continue;
      }
      if (twoKeyLock.matcher(read(file)).find()) {
        others.add(file.toString());
      }
    }
    assertTrue(others.isEmpty(), "two-key advisory locks outside SagaFenceLock: " + others);
  }

  private static List<Path> mainSources() {
    Path root = Path.of("").toAbsolutePath();
    while (!Files.exists(root.resolve("settings.gradle.kts"))) {
      root = root.getParent();
    }
    try (Stream<Path> walk = Files.walk(root)) {
      return walk.filter(p -> p.toString().replace('\\', '/').contains("/src/main/"))
          .filter(p -> !p.toString().contains("/build/"))
          .filter(p -> p.toString().endsWith(".java") || p.toString().endsWith(".sql"))
          .toList();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String read(Path file) {
    try {
      return Files.readString(file);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
