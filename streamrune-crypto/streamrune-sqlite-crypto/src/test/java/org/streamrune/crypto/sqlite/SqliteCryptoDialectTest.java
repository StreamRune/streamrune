package org.streamrune.crypto.sqlite;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.sqlite.SqliteDatabase;

/**
 * Runs each statement of {@link SqliteCryptoDialect} on a SQLite file with the shipped crypto
 * series applied: the timestamps it binds are the clock's instant in microseconds, the {@code max}
 * upsert never lowers a generation, a NULL evidence column reads back empty, and a key insert never
 * overwrites an existing key.
 */
class SqliteCryptoDialectTest {

  private static final Instant NOW = Instant.parse("2026-11-26T08:09:10.123456Z");
  private static final long NOW_MICROS = NOW.getEpochSecond() * 1_000_000L + 123_456L;

  private final SqliteCryptoDialect dialect =
      new SqliteCryptoDialect(Clock.fixed(NOW, ZoneOffset.UTC));

  @TempDir Path dir;

  private DataSource dataSource;
  private SqliteDatabase database;

  @BeforeEach
  void open() {
    dataSource = SqliteFiles.dataSource(dir.resolve("dialect.db"));
    database = SqliteFiles.migrated(dataSource);
  }

  @AfterEach
  void close() {
    database.close();
  }

  @Test
  void lockSubject_runsNoStatement() throws Exception {
    try (Connection c = dataSource.getConnection()) {
      dialect.lockSubject(c, "lock-subject");
      assertTrue(c.getAutoCommit(), "no transaction was started");
    }
    assertEquals(List.of(), SqliteFiles.query(dataSource, "SELECT * FROM forgotten_subjects"));
  }

  @Test
  void insertKey_stampsTheClocksMicros_andReturnsFalseOnConflictWithoutOverwriting()
      throws Exception {
    byte[] first = new byte[32];
    byte[] second = new byte[32];
    Arrays.fill(first, (byte) 1);
    Arrays.fill(second, (byte) 2);
    try (Connection c = dataSource.getConnection()) {
      assertTrue(dialect.insertKey(c, "encryption_keys", "insert-key", first, 3));
      assertFalse(dialect.insertKey(c, "encryption_keys", "insert-key", second, 4));
    }
    List<List<Object>> rows =
        SqliteFiles.query(
            dataSource,
            "SELECT subject_id, key_bytes, created_at, key_version FROM encryption_keys");
    assertEquals(1, rows.size());
    assertEquals("insert-key", rows.get(0).get(0));
    assertArrayEquals(first, (byte[]) rows.get(0).get(1), "the first writer's key stays");
    assertEquals(NOW_MICROS, ((Number) rows.get(0).get(2)).longValue());
    assertEquals(3, ((Number) rows.get(0).get(3)).intValue());
  }

  @Test
  void recordErasedGeneration_neverLowersTheRecordedGeneration() throws Exception {
    try (Connection c = dataSource.getConnection()) {
      dialect.recordErasedGeneration(c, "erased-generation", 3);
      assertEquals(3, maxErased());
      dialect.recordErasedGeneration(c, "erased-generation", 1);
      assertEquals(3, maxErased(), "a lower generation never lowers it");
      dialect.recordErasedGeneration(c, "erased-generation", 3);
      assertEquals(3, maxErased(), "a rerun is idempotent");
      dialect.recordErasedGeneration(c, "erased-generation", 5);
      assertEquals(5, maxErased(), "a higher generation raises it");
    }
  }

  @Test
  void insertTombstoneWithEvidence_stampsTheClocksMicros_andKeepsTheFirstRow() throws Exception {
    Instant evidence = Instant.parse("2026-03-04T05:06:07.000008Z");
    try (Connection c = dataSource.getConnection()) {
      dialect.insertTombstoneWithEvidence(c, "evidence", evidence);
      new SqliteCryptoDialect(Clock.fixed(NOW.plusSeconds(60), ZoneOffset.UTC))
          .insertTombstoneWithEvidence(c, "evidence", evidence.plusSeconds(86_400));
      assertEquals(Optional.of(evidence), readInstant(c, "evidence", "key_created_at"));
      assertEquals(Optional.of(NOW), readInstant(c, "evidence", "forgotten_at"));
    }
    assertEquals(
        List.of(List.of("evidence", NOW_MICROS, 1_772_600_767_000_008L)),
        tombstones(),
        "micros since the epoch, first write wins");
  }

  @Test
  void aNullEvidence_readsBackEmpty() throws Exception {
    try (Connection c = dataSource.getConnection()) {
      dialect.insertTombstoneWithEvidence(c, "no-evidence", null);
      assertEquals(Optional.empty(), readInstant(c, "no-evidence", "key_created_at"));
      assertEquals(Optional.of(NOW), readInstant(c, "no-evidence", "forgotten_at"));
    }
  }

  @Test
  void tombstoneInserts_stampTheClock_writeNoEvidence_andLeaveAnExistingTombstoneAsItIs()
      throws Exception {
    Instant evidence = Instant.parse("2026-01-02T03:04:05Z");
    try (Connection c = dataSource.getConnection()) {
      dialect.insertTombstone(c, "plain-tombstone");
      dialect.insertTombstone(c, "plain-tombstone");
      assertEquals(Optional.empty(), readInstant(c, "plain-tombstone", "key_created_at"));
      assertEquals(Optional.of(NOW), readInstant(c, "plain-tombstone", "forgotten_at"));

      dialect.insertEraseTombstone(c, "erase-tombstone");
      dialect.insertEraseTombstone(c, "erase-tombstone");
      assertEquals(Optional.empty(), readInstant(c, "erase-tombstone", "key_created_at"));
      assertEquals(Optional.of(NOW), readInstant(c, "erase-tombstone", "forgotten_at"));

      dialect.insertTombstoneWithEvidence(c, "evidence-first", evidence);
      dialect.insertTombstone(c, "evidence-first");
      dialect.insertEraseTombstone(c, "evidence-first");
      assertEquals(Optional.of(evidence), readInstant(c, "evidence-first", "key_created_at"));
    }
  }

  private int maxErased() throws SQLException {
    List<List<Object>> rows =
        SqliteFiles.query(
            dataSource,
            "SELECT max_erased_version FROM erased_key_generations"
                + " WHERE subject_id = 'erased-generation'");
    assertEquals(1, rows.size());
    return ((Number) rows.get(0).get(0)).intValue();
  }

  private List<List<Object>> tombstones() throws SQLException {
    return SqliteFiles.query(
        dataSource, "SELECT subject_id, forgotten_at, key_created_at FROM forgotten_subjects");
  }

  private Optional<Instant> readInstant(Connection c, String subject, String column)
      throws SQLException {
    try (var ps =
        c.prepareStatement("SELECT " + column + " FROM forgotten_subjects WHERE subject_id = ?")) {
      ps.setString(1, subject);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), "the tombstone exists");
        return dialect.readInstant(rs, column);
      }
    }
  }
}
