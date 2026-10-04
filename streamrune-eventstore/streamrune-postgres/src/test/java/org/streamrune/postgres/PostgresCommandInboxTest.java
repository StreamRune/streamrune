package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.CommandInbox.InboxResult;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresCommandInboxTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("inbox_test");

  static PGSimpleDataSource dataSource;
  PostgresCommandInbox inbox;

  @BeforeAll
  static void setUpSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .load()
        .migrate();
  }

  @BeforeEach
  void setUp() throws Exception {
    inbox = new PostgresCommandInbox(dataSource);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM command_inbox");
    }
  }

  @Test
  void tryClaimReturnsTrueFirstTimeFalseSecondTime() throws Exception {
    IdempotencyKey key = IdempotencyKey.of("cmd-1");
    StreamId streamId = TestStreams.stream("stream-1");
    Version finalVersion = new Version(1);

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      boolean first = inbox.tryClaim(key, "TestCommand", streamId, finalVersion, conn);
      conn.commit();
      assertTrue(first, "First claim should succeed");
    }

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      boolean second = inbox.tryClaim(key, "TestCommand", streamId, finalVersion, conn);
      conn.commit();
      assertFalse(second, "Second claim for same key should fail");
    }
  }

  @Test
  void findReturnsRowWithBackfilledOffsets() throws Exception {
    IdempotencyKey key = IdempotencyKey.of("cmd-2");
    StreamId streamId = TestStreams.stream("stream-2");
    Version finalVersion = new Version(3);
    List<GlobalOffset> offsets = List.of(GlobalOffset.of(10L), GlobalOffset.of(11L));

    // claim
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      inbox.tryClaim(key, "OrderCommand", streamId, finalVersion, conn);
      inbox.backfillOffsets(key, offsets, conn);
      conn.commit();
    }

    // find
    var result = inbox.find(key);
    assertTrue(result.isPresent());
    InboxResult r = result.get();
    assertEquals(key, r.key());
    assertEquals("OrderCommand", r.commandType());
    assertEquals(streamId, r.streamId());
    assertEquals(finalVersion, r.finalVersion());
    assertEquals(offsets, r.globalOffsets());
    assertNotNull(r.processedAt());
  }

  @Test
  void aKeyClaimedForATypedStream_isFoundBackWithTheTypedPair() throws Exception {
    IdempotencyKey key = IdempotencyKey.of("cmd-typed");
    StreamId product = StreamId.of(AggregateType.of("product"), AggregateId.of("x"));

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      assertTrue(inbox.tryClaim(key, "CreateProduct", product, new Version(1), conn));
      conn.commit();
    }

    InboxResult found = inbox.find(key).orElseThrow();
    assertEquals(product, found.streamId());
    assertEquals("product", found.streamId().aggregateType().value());
    assertEquals("x", found.streamId().aggregateId().value());
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT aggregate_type, aggregate_id FROM command_inbox"
                    + " WHERE idempotency_key = 'cmd-typed'")) {
      assertTrue(rs.next());
      assertEquals("product", rs.getString(1));
      assertEquals("x", rs.getString(2));
    }
  }

  @Test
  void aKeyReusedAcrossTwoTypesSharingAnIdValue_isRefused() throws Exception {
    IdempotencyKey key = IdempotencyKey.of("cmd-shared");
    StreamId product = StreamId.of(AggregateType.of("product"), AggregateId.of("x"));
    StreamId inventory = StreamId.of(AggregateType.of("inventory"), AggregateId.of("x"));

    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      inbox.tryClaim(key, "CreateProduct", product, new Version(1), conn);
      conn.commit();
    }

    InboxResult found = inbox.find(key).orElseThrow();
    found.requireBoundTo("CreateProduct", product);
    var ex =
        assertThrows(
            IllegalArgumentException.class, () -> found.requireBoundTo("CreateProduct", inventory));
    assertTrue(ex.getMessage().contains("product:x"), ex.getMessage());
    assertTrue(ex.getMessage().contains("inventory:x"), ex.getMessage());
  }

  @Test
  void findReturnsEmptyForUnknownKey() {
    assertTrue(inbox.find(IdempotencyKey.of("no-such-key")).isEmpty());
  }

  @Test
  void deleteProcessedBeforePrunesOldRows() throws Exception {
    // Insert 2 old rows and 1 recent row
    Instant past = Instant.parse("2020-01-01T00:00:00Z");
    Instant recent = Instant.now();

    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO command_inbox"
                    + " (idempotency_key, command_type, aggregate_type, aggregate_id, final_version,"
                    + " processed_at) VALUES (?, 'T', 'test', 'stream', 0, ?)")) {
      for (String k : List.of("old-1", "old-2")) {
        ps.setString(1, k);
        ps.setObject(2, java.sql.Timestamp.from(past));
        ps.addBatch();
      }
      ps.executeBatch();
    }
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO command_inbox"
                    + " (idempotency_key, command_type, aggregate_type, aggregate_id, final_version,"
                    + " processed_at) VALUES (?, 'T', 'test', 'stream', 0, ?)")) {
      ps.setString(1, "new-1");
      ps.setObject(2, java.sql.Timestamp.from(recent));
      ps.executeUpdate();
    }

    int deleted = inbox.deleteProcessedBefore(Instant.parse("2021-01-01T00:00:00Z"));

    assertEquals(2, deleted);
    assertTrue(inbox.find(IdempotencyKey.of("old-1")).isEmpty());
    assertTrue(inbox.find(IdempotencyKey.of("old-2")).isEmpty());
    assertTrue(inbox.find(IdempotencyKey.of("new-1")).isPresent());
  }

  /**
   * A chunk that comes back shorter than its LIMIT because a concurrent writer removed one of its
   * candidates inside the DELETE window must not end the sweep while aged rows remain. Every
   * instance runs its own {@code InboxRetentionSweeper} with no cluster lock, so a second sweeper
   * deleting the same oldest rows is the ordinary multi-instance shape; the held single-row DELETE
   * here is its deterministic stand-in. Under the former {@code batch == DELETE_BATCH_SIZE}
   * termination the first chunk deleted 999, the loop stopped, and 500 aged rows outlived the
   * sweep.
   */
  @Test
  void deleteProcessedBefore_keepsDrainingWhenAConcurrentDeleteShortensAChunk() throws Exception {
    Instant old = Instant.parse("2020-01-01T00:00:00Z");
    int aged = 1500;
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "INSERT INTO command_inbox"
                    + " (idempotency_key, command_type, aggregate_type, aggregate_id, final_version,"
                    + " processed_at) VALUES (?, 'T', 'test', 'stream', 0, ?)")) {
      for (int i = 0; i < aged; i++) {
        ps.setString(1, String.format("aged-%04d", i));
        ps.setObject(2, java.sql.Timestamp.from(old.plusMillis(i)));
        ps.addBatch();
      }
      ps.executeBatch();
    }

    int deleted =
        RowLockRaces.sweepWhileAWriterHoldsARow(
            dataSource,
            "DELETE FROM command_inbox WHERE idempotency_key = ?",
            () -> inbox.deleteProcessedBefore(Instant.parse("2021-01-01T00:00:00Z")),
            "aged-0000");

    assertEquals(
        aged - 1, deleted, "every aged row the concurrent writer did not take must be swept");
    assertEquals(0, countInboxRows(), "no aged row may outlive the sweep");
  }

  private static long countInboxRows() throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT count(*) FROM command_inbox");
        var rs = ps.executeQuery()) {
      rs.next();
      return rs.getLong(1);
    }
  }
}
