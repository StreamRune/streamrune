package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The shared forgotten-subject SQL on a SQLite file created from the shipped crypto DDL, with
 * {@link SqliteTestDialect}. Every call runs in a write transaction the test owns, on a connection
 * that fails the test when a helper commits, rolls back, closes it or changes its autoCommit.
 */
class JdbcForgottenSubjectsTest {

  private static final Instant FIRST = Instant.parse("2026-01-02T03:04:05.123456Z");
  private static final Instant LATER = Instant.parse("2026-02-03T04:05:06.654321Z");
  private static final Instant EVIDENCE = Instant.parse("2025-12-24T18:00:00.000001Z");
  private static final String SUBJECT = "a1b2c3";

  @TempDir Path dir;

  private SqliteCryptoTestDatabase db;
  private final List<String> calls = new ArrayList<>();
  private JdbcForgottenSubjects forgotten;

  @BeforeEach
  void setUp() throws Exception {
    db = SqliteCryptoTestDatabase.create(dir);
    forgotten = new JdbcForgottenSubjects(recording(FIRST));
  }

  private JdbcCryptoDialect recording(Instant now) {
    return new JdbcSubjectKeyStoreTest.RecordingDialect(Clock.fixed(now, ZoneOffset.UTC), calls);
  }

  private void run(SqliteCryptoTestDatabase.Work<?> work) throws Exception {
    db.inWrite(work);
  }

  @Test
  void constructorRejectsANullDialect() {
    assertThrows(IllegalArgumentException.class, () -> new JdbcForgottenSubjects(null));
  }

  @Test
  void unknownSubjectIsNotForgottenAndHasNoInstants() throws Exception {
    assertFalse(db.inWriteBoolean(c -> forgotten.isForgotten(c, SUBJECT)));
    assertEquals(Optional.empty(), db.inWriteInstant(c -> forgotten.forgottenAt(c, SUBJECT)));
    assertEquals(Optional.empty(), db.inWriteInstant(c -> forgotten.keyCreatedAt(c, SUBJECT)));
  }

  @Test
  void forgetWithoutEvidence_tombstonesWithTheClockInstantAndNoEvidence() throws Exception {
    run(
        c -> {
          forgotten.forget(c, SUBJECT);
          return null;
        });
    assertEquals(List.of("insertTombstone"), calls);
    assertTrue(db.inWriteBoolean(c -> forgotten.isForgotten(c, SUBJECT)));
    assertEquals(Optional.of(FIRST), db.inWriteInstant(c -> forgotten.forgottenAt(c, SUBJECT)));
    assertEquals(Optional.empty(), db.inWriteInstant(c -> forgotten.keyCreatedAt(c, SUBJECT)));
  }

  @Test
  void forgetWithEvidence_roundTripsThroughReadInstant() throws Exception {
    run(
        c -> {
          forgotten.forget(c, SUBJECT, EVIDENCE);
          return null;
        });
    assertEquals(List.of("insertTombstoneWithEvidence"), calls);
    assertEquals(Optional.of(EVIDENCE), db.inWriteInstant(c -> forgotten.keyCreatedAt(c, SUBJECT)));
    assertEquals(Optional.of(FIRST), db.inWriteInstant(c -> forgotten.forgottenAt(c, SUBJECT)));
  }

  @Test
  void forgetWithNullEvidence_recordsNoEvidence() throws Exception {
    run(
        c -> {
          forgotten.forget(c, SUBJECT, null);
          return null;
        });
    assertEquals(List.of("insertTombstoneWithEvidence"), calls);
    assertTrue(db.inWriteBoolean(c -> forgotten.isForgotten(c, SUBJECT)));
    assertEquals(Optional.empty(), db.inWriteInstant(c -> forgotten.keyCreatedAt(c, SUBJECT)));
  }

  @Test
  void forgetIsFirstWriteWins() throws Exception {
    run(
        c -> {
          forgotten.forget(c, SUBJECT, EVIDENCE);
          return null;
        });
    JdbcForgottenSubjects later = new JdbcForgottenSubjects(recording(LATER));
    run(
        c -> {
          later.forget(c, SUBJECT, LATER);
          later.forget(c, SUBJECT);
          later.forgetForErase(c, SUBJECT);
          return null;
        });
    assertEquals(Optional.of(FIRST), db.inWriteInstant(c -> forgotten.forgottenAt(c, SUBJECT)));
    assertEquals(Optional.of(EVIDENCE), db.inWriteInstant(c -> forgotten.keyCreatedAt(c, SUBJECT)));
    assertEquals(1, db.count("SELECT count(*) FROM forgotten_subjects"));
  }

  @Test
  void aTombstoneWithoutEvidenceNeverGainsSome() throws Exception {
    run(
        c -> {
          forgotten.forget(c, SUBJECT);
          forgotten.forget(c, SUBJECT, EVIDENCE);
          return null;
        });
    assertEquals(Optional.empty(), db.inWriteInstant(c -> forgotten.keyCreatedAt(c, SUBJECT)));
  }

  @Test
  void forgetForErase_tombstonesThroughTheEraseStatement() throws Exception {
    run(
        c -> {
          forgotten.forgetForErase(c, SUBJECT);
          return null;
        });
    assertEquals(List.of("insertEraseTombstone"), calls);
    assertTrue(db.inWriteBoolean(c -> forgotten.isForgotten(c, SUBJECT)));
    assertEquals(Optional.of(FIRST), db.inWriteInstant(c -> forgotten.forgottenAt(c, SUBJECT)));
    assertNull(
        db.selectLong(
            "SELECT key_created_at FROM forgotten_subjects WHERE subject_id = ?", SUBJECT));
  }

  @Test
  void reinstateDeletesTheTombstone() throws Exception {
    run(
        c -> {
          forgotten.forget(c, SUBJECT, EVIDENCE);
          forgotten.forget(c, "other");
          return null;
        });
    run(
        c -> {
          forgotten.reinstate(c, SUBJECT);
          return null;
        });
    assertFalse(db.inWriteBoolean(c -> forgotten.isForgotten(c, SUBJECT)));
    assertEquals(Optional.empty(), db.inWriteInstant(c -> forgotten.keyCreatedAt(c, SUBJECT)));
    assertTrue(db.inWriteBoolean(c -> forgotten.isForgotten(c, "other")));
    // Reinstating a subject without a tombstone changes nothing.
    run(
        c -> {
          forgotten.reinstate(c, SUBJECT);
          return null;
        });
    assertEquals(1, db.count("SELECT count(*) FROM forgotten_subjects"));
  }

  @Test
  void sqlExceptionPropagatesUnwrapped() throws Exception {
    SQLException boom = new SQLException("database is locked");
    JdbcForgottenSubjects failing =
        new JdbcForgottenSubjects(
            new SqliteTestDialect(Clock.fixed(FIRST, ZoneOffset.UTC)) {
              @Override
              public void insertTombstoneWithEvidence(
                  Connection conn, String storedSubjectId, Instant keyCreatedAtOrNull)
                  throws SQLException {
                throw boom;
              }
            });
    assertSame(
        boom,
        assertThrows(
            SQLException.class,
            () ->
                db.inWrite(
                    c -> {
                      failing.forget(c, SUBJECT, EVIDENCE);
                      return null;
                    })));
    assertEquals(0, db.count("SELECT count(*) FROM forgotten_subjects"));
  }

  @Test
  void readInstantFailurePropagatesUnwrapped() throws Exception {
    run(
        c -> {
          forgotten.forget(c, SUBJECT, EVIDENCE);
          return null;
        });
    SQLException boom = new SQLException("malformed timestamp");
    JdbcForgottenSubjects failing =
        new JdbcForgottenSubjects(
            new SqliteTestDialect(Clock.fixed(FIRST, ZoneOffset.UTC)) {
              @Override
              public Optional<Instant> readInstant(java.sql.ResultSet rs, String column)
                  throws SQLException {
                throw boom;
              }
            });
    assertSame(
        boom,
        assertThrows(SQLException.class, () -> db.inWrite(c -> failing.keyCreatedAt(c, SUBJECT))));
  }
}
