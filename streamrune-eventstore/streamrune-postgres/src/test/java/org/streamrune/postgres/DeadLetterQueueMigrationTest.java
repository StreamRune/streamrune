package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashSet;
import java.util.Set;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Pins the shape of {@code dead_letter_queue} as the shipped event-store baseline provisions it —
 * Flyway is its only provisioner — and that the command DLQ works end-to-end against it.
 */
@Testcontainers
class DeadLetterQueueMigrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("dlq_migration_test");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void migrateSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();
  }

  @Test
  void migrationAloneCreatesDeadLetterQueueTableWithExpectedColumns() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT column_name, data_type, character_maximum_length, is_nullable "
                    + "FROM information_schema.columns "
                    + "WHERE table_schema = current_schema() AND table_name = 'dead_letter_queue'")) {
      Set<String> columns = new HashSet<>();
      while (rs.next()) {
        columns.add(rs.getString("column_name"));
      }

      assertEquals(
          Set.of(
              "command_id",
              "command_type",
              "payload",
              "aggregate_type",
              "aggregate_id",
              "error_type",
              "error_message",
              "attempts",
              "first_attempt_at",
              "published_at",
              "dlq_attempts",
              "last_attempt_at",
              "correlation_id",
              "user_id",
              "trace_id",
              "idempotency_key"),
          columns,
          "dead_letter_queue must have exactly the full current column set after Flyway alone");
    }
  }

  @Test
  void migrationCreatesCommandIdAsVarchar255PrimaryKey() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT data_type, character_maximum_length FROM information_schema.columns "
                    + "WHERE table_schema = current_schema() AND table_name = 'dead_letter_queue' "
                    + "AND column_name = 'command_id'")) {
      assertTrue(rs.next());
      assertEquals("character varying", rs.getString("data_type"));
      assertEquals(255, rs.getInt("character_maximum_length"));
    }

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT tc.constraint_type FROM information_schema.table_constraints tc "
                    + "JOIN information_schema.key_column_usage kcu "
                    + "  ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema "
                    + "WHERE tc.table_schema = current_schema() AND tc.table_name = 'dead_letter_queue' "
                    + "AND kcu.column_name = 'command_id' AND tc.constraint_type = 'PRIMARY KEY'")) {
      assertTrue(rs.next(), "command_id must be the primary key");
    }
  }

  @Test
  void idempotencyKeyColumnIsNullableAndHoldsAMaximumLengthKey() throws Exception {
    // The column must accept NULL (a command that ran unkeyed stores none) and hold
    // IdempotencyKey's
    // 512-character maximum — a narrower column would fail to dead-letter exactly the commands
    // whose replay most needs the original key.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT data_type, is_nullable, character_maximum_length"
                    + " FROM information_schema.columns WHERE table_schema = current_schema()"
                    + " AND table_name = 'dead_letter_queue' AND column_name = 'idempotency_key'")) {
      assertTrue(rs.next(), "dead_letter_queue.idempotency_key must exist");
      assertEquals("character varying", rs.getString("data_type"));
      assertEquals(
          "YES",
          rs.getString("is_nullable"),
          "unkeyed commands legitimately store NULL, so the column cannot be NOT NULL");
      assertEquals(512, rs.getInt("character_maximum_length"), "matches IdempotencyKey's maximum");
    }

    var dlq = new PostgresDeadLetterQueue(dataSource);
    var maxKey = org.streamrune.core.types.IdempotencyKey.of("k".repeat(512));
    var commandId = org.streamrune.core.types.CommandId.of("mig-probe-maxkey-00000001");
    dlq.publish(
        new org.streamrune.core.DeadLetterQueue.DeadLetterPublishRequest(
            "{\"ok\":true}",
            "MaxKeyProbeCommand",
            commandId,
            null,
            "SimulatedError",
            "probe",
            1,
            java.time.Instant.now(),
            null,
            null,
            null,
            maxKey));
    assertEquals(
        maxKey,
        dlq.find(commandId).orElseThrow().idempotencyKey(),
        "a maximum-length key must round-trip unchanged");
    dlq.discard(commandId);
  }

  @Test
  void publishedAtAndDlqAttemptsAreNotNullWithDefaults() throws Exception {
    // The publish INSERT stamps published_at = NOW() and leaves dlq_attempts to its DEFAULT, so
    // neither is ever NULL: every read maps them straight to the entry's publishedAt/dlqAttempts.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT column_name, is_nullable, column_default FROM information_schema.columns"
                    + " WHERE table_schema = current_schema() AND table_name = 'dead_letter_queue'"
                    + " AND column_name IN ('published_at', 'dlq_attempts')")) {
      var nullable = new java.util.HashMap<String, String>();
      var defaults = new java.util.HashMap<String, String>();
      while (rs.next()) {
        nullable.put(rs.getString("column_name"), rs.getString("is_nullable"));
        defaults.put(rs.getString("column_name"), rs.getString("column_default"));
      }
      assertEquals(java.util.Map.of("published_at", "NO", "dlq_attempts", "NO"), nullable);
      assertEquals("now()", defaults.get("published_at"));
      assertEquals("0", defaults.get("dlq_attempts"));
    }
  }

  @Test
  void migrationCreatesPublishedAtIndex() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT indexname FROM pg_indexes WHERE schemaname = current_schema() "
                    + "AND tablename = 'dead_letter_queue' AND indexname = 'idx_dlq_published_at'")) {
      assertTrue(rs.next(), "idx_dlq_published_at must exist after migration");
    }
  }

  @Test
  void dlqIsFunctionalAgainstTheMigratedSchema() {
    // End-to-end proof: publish() succeeds against the Flyway-provisioned table.
    var dlq = new PostgresDeadLetterQueue(dataSource);
    var req =
        new org.streamrune.core.DeadLetterQueue.DeadLetterPublishRequest(
            "{\"ok\":true}",
            "MigrationProbeCommand",
            org.streamrune.core.types.CommandId.of("mig-probe-0000000000001"),
            TestStreams.stream("mig-probe-stream"),
            "SimulatedError",
            "probe",
            1,
            java.time.Instant.now(),
            null,
            null,
            null,
            null);

    assertDoesNotThrow(() -> dlq.publish(req));
    var entries = dlq.read(10);
    assertTrue(
        entries.stream().anyMatch(e -> e.commandType().equals("MigrationProbeCommand")),
        "published entry should be readable back from the migration-provisioned table");

    dlq.discard(org.streamrune.core.types.CommandId.of("mig-probe-0000000000001"));
  }

  @Test
  void idempotencyKeyRoundTripsThroughMigratedSchema() {
    // The idempotency_key column must persist the caller's original key and read it
    // back, so the retry runner can replay under it. A null key (unkeyed command) must round-trip
    // as
    // null. Exercised against the Flyway-migrated schema.
    var dlq = new PostgresDeadLetterQueue(dataSource);

    var keyed =
        new org.streamrune.core.DeadLetterQueue.DeadLetterPublishRequest(
            "{\"ok\":true}",
            "KeyedProbeCommand",
            org.streamrune.core.types.CommandId.of("mig-probe-keyed-000000001"),
            TestStreams.stream("mig-probe-stream"),
            "SimulatedError",
            "probe",
            1,
            java.time.Instant.now(),
            null,
            null,
            null,
            org.streamrune.core.types.IdempotencyKey.of("client-key-abc"));
    dlq.publish(keyed);

    var readBack =
        dlq.find(org.streamrune.core.types.CommandId.of("mig-probe-keyed-000000001")).orElseThrow();
    assertEquals(
        org.streamrune.core.types.IdempotencyKey.of("client-key-abc"),
        readBack.idempotencyKey(),
        "the originating idempotency key must round-trip through the migrated schema");

    // An unkeyed entry (the command ran without an idempotency key) must store and read back a
    // null key.
    var unkeyed =
        new org.streamrune.core.DeadLetterQueue.DeadLetterPublishRequest(
            "{\"ok\":true}",
            "UnkeyedProbeCommand",
            org.streamrune.core.types.CommandId.of("mig-probe-unkeyed-00000001"),
            null,
            "SimulatedError",
            "probe",
            1,
            java.time.Instant.now(),
            null,
            null,
            null,
            null);
    dlq.publish(unkeyed);
    var unkeyedBack =
        dlq.find(org.streamrune.core.types.CommandId.of("mig-probe-unkeyed-00000001"))
            .orElseThrow();
    assertNull(
        unkeyedBack.idempotencyKey(), "an unkeyed command must store a null idempotency key");

    dlq.discard(org.streamrune.core.types.CommandId.of("mig-probe-keyed-000000001"));
    dlq.discard(org.streamrune.core.types.CommandId.of("mig-probe-unkeyed-00000001"));
  }
}
