package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Runs each statement of {@link PostgresCryptoDialect} on a real PostgreSQL with the shipped crypto
 * baseline applied. Every subject id is unique to its test, so the tests share one database.
 */
@Testcontainers
class PostgresCryptoDialectIntegrationTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static final PostgresCryptoDialect DIALECT = PostgresCryptoDialect.INSTANCE;

  private static PGSimpleDataSource dataSource;

  @BeforeAll
  static void init() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    String resource = "/db/crypto-migration/V001__crypto_baseline.sql";
    String ddl;
    try (var in = PostgresCryptoDialectIntegrationTest.class.getResourceAsStream(resource)) {
      assertNotNull(in, "shipped migration must be on the classpath: " + resource);
      ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(ddl);
    }
  }

  @Test
  @Timeout(value = 30, unit = TimeUnit.SECONDS)
  void lockSubject_blocksASecondTransactionOnTheSameSubject_untilTheFirstEnds() throws Exception {
    try (Connection holder = dataSource.getConnection();
        Connection waiter = dataSource.getConnection()) {
      // Every lock statement of the waiter gives up after 200 ms instead of blocking the test for
      // ever. The timeout is set for the session while autoCommit is still on, so the rollbacks
      // below do not clear it (a SET LOCAL would end with the first rollback). SQLState 55P03
      // (lock_not_available) proves the first lock statement waited on the holder's lock.
      try (var stmt = waiter.createStatement()) {
        stmt.execute("SET lock_timeout = '200ms'");
      }
      holder.setAutoCommit(false);
      waiter.setAutoCommit(false);
      DIALECT.lockSubject(holder, "lock-subject");

      SQLException blocked =
          assertThrows(SQLException.class, () -> DIALECT.lockSubject(waiter, "lock-subject"));
      assertEquals("55P03", blocked.getSQLState(), blocked.getMessage());
      waiter.rollback();

      // Another subject takes another lock: it does not wait on the holder.
      DIALECT.lockSubject(waiter, "other-subject");
      waiter.rollback();

      // The lock is transaction-scoped: once the holder's transaction ends, the waiter gets it.
      holder.commit();
      DIALECT.lockSubject(waiter, "lock-subject");
      waiter.commit();
    }
  }

  @Test
  void insertKey_insertsOnce_andReturnsFalseOnConflictWithoutOverwriting() throws Exception {
    byte[] first = new byte[32];
    byte[] second = new byte[32];
    java.util.Arrays.fill(first, (byte) 1);
    java.util.Arrays.fill(second, (byte) 2);
    try (Connection c = dataSource.getConnection()) {
      assertTrue(DIALECT.insertKey(c, "encryption_keys", "insert-key", first, 3));
      assertFalse(DIALECT.insertKey(c, "encryption_keys", "insert-key", second, 4));
      try (var ps =
          c.prepareStatement(
              "SELECT key_bytes, key_version FROM encryption_keys WHERE subject_id = ?")) {
        ps.setString(1, "insert-key");
        try (var rs = ps.executeQuery()) {
          assertTrue(rs.next());
          assertArrayEquals(first, rs.getBytes("key_bytes"), "the first writer's key stays");
          assertEquals(3, rs.getInt("key_version"));
        }
      }
    }
  }

  @Test
  void recordErasedGeneration_neverLowersTheRecordedGeneration() throws Exception {
    try (Connection c = dataSource.getConnection()) {
      DIALECT.recordErasedGeneration(c, "erased-generation", 3);
      assertEquals(3, maxErased(c, "erased-generation"));
      DIALECT.recordErasedGeneration(c, "erased-generation", 1);
      assertEquals(3, maxErased(c, "erased-generation"), "a lower generation never lowers it");
      DIALECT.recordErasedGeneration(c, "erased-generation", 3);
      assertEquals(3, maxErased(c, "erased-generation"), "a rerun is idempotent");
      DIALECT.recordErasedGeneration(c, "erased-generation", 5);
      assertEquals(5, maxErased(c, "erased-generation"), "a higher generation raises it");
    }
  }

  @Test
  void insertTombstone_roundTripsThroughReadInstant_andKeepsTheFirstEvidence() throws Exception {
    // timestamptz stores microseconds.
    Instant evidence = Instant.parse("2026-03-04T05:06:07.123456Z");
    try (Connection c = dataSource.getConnection()) {
      DIALECT.insertTombstone(c, "evidence", evidence);
      DIALECT.insertTombstone(c, "evidence", evidence.plus(1, ChronoUnit.DAYS));
      assertEquals(Optional.of(evidence), readInstant(c, "evidence", "key_created_at"));
      assertTrue(readInstant(c, "evidence", "forgotten_at").isPresent());
    }
  }

  @Test
  void aNullInstant_readsBackEmpty() throws Exception {
    try (Connection c = dataSource.getConnection()) {
      DIALECT.insertTombstone(c, "no-evidence", null);
      assertEquals(Optional.empty(), readInstant(c, "no-evidence", "key_created_at"));
    }
  }

  @Test
  void insertTombstone_leavesAnExistingTombstoneAsItIs() throws Exception {
    Instant evidence = Instant.parse("2026-01-02T03:04:05Z");
    try (Connection c = dataSource.getConnection()) {
      DIALECT.insertTombstone(c, "plain-tombstone", null);
      DIALECT.insertTombstone(c, "plain-tombstone", null);
      DIALECT.insertTombstone(c, "plain-tombstone", evidence);
      assertEquals(
          Optional.empty(),
          readInstant(c, "plain-tombstone", "key_created_at"),
          "a tombstone without evidence never gains some");

      DIALECT.insertTombstone(c, "evidence-first", evidence);
      DIALECT.insertTombstone(c, "evidence-first", null);
      assertEquals(Optional.of(evidence), readInstant(c, "evidence-first", "key_created_at"));
    }
  }

  private static int maxErased(Connection c, String subject) throws SQLException {
    try (var ps =
        c.prepareStatement(
            "SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?")) {
      ps.setString(1, subject);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getInt("max_erased_version");
      }
    }
  }

  private static Optional<Instant> readInstant(Connection c, String subject, String column)
      throws SQLException {
    try (var ps =
        c.prepareStatement("SELECT " + column + " FROM forgotten_subjects WHERE subject_id = ?")) {
      ps.setString(1, subject);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next(), "the tombstone exists");
        return DIALECT.readInstant(rs, column);
      }
    }
  }
}
