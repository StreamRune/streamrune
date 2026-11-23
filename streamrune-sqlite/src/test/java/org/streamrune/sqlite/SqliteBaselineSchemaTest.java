package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.sqlite.SqliteSchemaSeriesTest.execute;
import static org.streamrune.sqlite.SqliteSchemaSeriesTest.query;

import java.nio.file.Path;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The constraints of the event-store baseline, mirrored from the Postgres baseline. */
class SqliteBaselineSchemaTest {

  @TempDir Path dir;

  private SqliteDatabase db;
  private DataSource ds;

  @BeforeEach
  void migrate() {
    db = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("app.db")));
    db.migrate(SqliteSchemaSeries.EVENT_STORE);
    ds = db.dataSource();
  }

  @AfterEach
  void close() {
    db.close();
  }

  @ParameterizedTest
  @ValueSource(strings = {"o", "order", "order_line_2", "abcdefghijklmnopqrstuvwxyz012345"})
  void aggregateTypeAcceptsThePostgresSyntax(String type) {
    assertThatCode(() -> insertEvent(1, type, "a-1", 1)).doesNotThrowAnyException();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "Order",
        "1order",
        "_order",
        "order-line",
        "order line",
        "ordér",
        "order\n",
        "abcdefghijklmnopqrstuvwxyz0123456"
      })
  void aggregateTypeRefusesWhatPostgresRefuses(String type) {
    assertThatThrownBy(() -> insertEvent(1, type, "a-1", 1))
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("CHECK constraint failed");
  }

  @Test
  void theAggregateTypeCheckGuardsEveryTableWithTheColumn() {
    for (String sql :
        List.of(
            "INSERT INTO snapshot_store (aggregate_type, aggregate_id, version, created_at)"
                + " VALUES ('Bad', 'a', 1, 0)",
            "INSERT INTO command_inbox (idempotency_key, command_type, aggregate_type,"
                + " aggregate_id, final_version, processed_at) VALUES ('k', 'c', 'Bad', 'a', 1, 0)",
            "INSERT INTO dead_letter_queue (command_id, command_type, payload, aggregate_type,"
                + " aggregate_id, error_type, attempts, first_attempt_at, published_at)"
                + " VALUES ('c', 'c', '{}', 'Bad', 'a', 'e', 1, 0, 0)",
            "INSERT INTO audit_log (command_type, aggregate_type, occurred_at, outcome)"
                + " VALUES ('c', 'Bad', 0, 'OK')",
            "INSERT INTO event_audit_log (event_id, event_type, aggregate_type, aggregate_id,"
                + " version, command_id, correlation_id, occurred_at)"
                + " VALUES ('e', 't', 'Bad', 'a', 1, 'c', 'r', 0)",
            "INSERT INTO outbox_events (entry_id, payload, payload_type, created_at,"
                + " aggregate_type, aggregate_id) VALUES ('e', '{}', 't', 0, 'Bad', 'a')")) {
      assertThatThrownBy(() -> execute(ds, sql))
          .as(sql)
          .hasMessageContaining("CHECK constraint failed");
    }
  }

  @Test
  void varcharLengthsAreEnforcedAsInPostgres() throws Exception {
    insertEvent(1, "order", "x".repeat(255), 1);
    assertThatThrownBy(() -> insertEvent(2, "order", "x".repeat(256), 1))
        .hasMessageContaining("CHECK constraint failed");
    // Characters, not bytes: 255 two-byte characters fit, as in VARCHAR(255).
    insertEvent(3, "order", "é".repeat(255), 1);
  }

  @Test
  void jsonColumnsRefuseTextThatIsNotJson() {
    assertThatThrownBy(
            () ->
                execute(
                    ds,
                    "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id,"
                        + " version, event_type, payload, metadata, created_at)"
                        + " VALUES (1, 'order', 'a', 1, 't', '{not json', '{}', 0)"))
        .hasMessageContaining("CHECK constraint failed");
    assertThatThrownBy(
            () ->
                execute(
                    ds,
                    "INSERT INTO command_inbox (idempotency_key, command_type, aggregate_type,"
                        + " aggregate_id, final_version, global_offsets, processed_at)"
                        + " VALUES ('k', 'c', 'order', 'a', 1, '{}', 0)"))
        .as("global_offsets must be a JSON array")
        .hasMessageContaining("CHECK constraint failed");
  }

  @Test
  void commandInboxOffsetsDefaultToAnEmptyArray() throws Exception {
    execute(
        ds,
        "INSERT INTO command_inbox (idempotency_key, command_type, aggregate_type, aggregate_id,"
            + " final_version, processed_at) VALUES ('k', 'c', 'order', 'a', 1, 0)");
    assertThat(query(ds, "SELECT global_offsets FROM command_inbox"))
        .containsExactly(List.of("[]"));
  }

  @Test
  void strictTablesRefuseAValueOfTheWrongType() {
    assertThatThrownBy(() -> insertEvent(1, "order", "a", "not a number"))
        .hasMessageContaining("cannot store TEXT value in INTEGER column");
  }

  @Test
  void streamVersionIsUnique() throws Exception {
    insertEvent(1, "order", "a", 1);
    assertThatThrownBy(() -> insertEvent(2, "order", "a", 1))
        .hasMessageContaining("UNIQUE constraint failed");
  }

  @Test
  void theCounterHoldsExactlyOneRow() {
    assertThatThrownBy(
            () -> execute(ds, "INSERT INTO global_offset_counter (id, next_value) VALUES (2, 0)"))
        .hasMessageContaining("CHECK constraint failed");
    assertThatThrownBy(
            () -> execute(ds, "INSERT INTO global_offset_counter (id, next_value) VALUES (1, 5)"))
        .hasMessageContaining("UNIQUE constraint failed");
  }

  @Test
  void snapshotStateIsPresentOrPurgedAsAWhole() throws Exception {
    assertThatThrownBy(
            () ->
                execute(
                    ds,
                    "INSERT INTO snapshot_store (aggregate_type, aggregate_id, version,"
                        + " state_type, created_at) VALUES ('order', 'a', 1, 'T', 0)"))
        .hasMessageContaining("CHECK constraint failed");
    execute(
        ds,
        "INSERT INTO snapshot_store (aggregate_type, aggregate_id, version, created_at)"
            + " VALUES ('order', 'a', 1, 0)");
  }

  @Test
  void streamColumnsAreBothSetOrBothNull() {
    assertThatThrownBy(
            () ->
                execute(
                    ds,
                    "INSERT INTO outbox_events (entry_id, payload, payload_type, created_at,"
                        + " aggregate_type) VALUES ('e', '{}', 't', 0, 'order')"))
        .hasMessageContaining("CHECK constraint failed");
    assertThatThrownBy(
            () ->
                execute(
                    ds,
                    "INSERT INTO dead_letter_queue (command_id, command_type, payload,"
                        + " aggregate_id, error_type, attempts, first_attempt_at, published_at)"
                        + " VALUES ('c', 'c', '{}', 'a', 'e', 1, 0, 0)"))
        .hasMessageContaining("CHECK constraint failed");
  }

  @Test
  void outboxSeqFollowsInsertOrder() throws Exception {
    for (String id : List.of("z", "a", "m")) {
      execute(
          ds,
          "INSERT INTO outbox_events (entry_id, payload, payload_type, created_at)"
              + " VALUES ('"
              + id
              + "', '{}', 't', 0)");
    }
    assertThat(query(ds, "SELECT entry_id, seq, status, attempts FROM outbox_events ORDER BY seq"))
        .containsExactly(
            List.of("z", 1L, "PENDING", 0L),
            List.of("a", 2L, "PENDING", 0L),
            List.of("m", 3L, "PENDING", 0L));
    assertThatThrownBy(
            () ->
                execute(
                    ds,
                    "INSERT INTO outbox_events (entry_id, payload, payload_type, created_at)"
                        + " VALUES ('a', '{}', 't', 0)"))
        .hasMessageContaining("UNIQUE constraint failed: outbox_events.entry_id");
  }

  @Test
  void oneInFlightOutboxEntryPerStream() throws Exception {
    String insert =
        "INSERT INTO outbox_events (entry_id, payload, payload_type, created_at, status,"
            + " aggregate_type, aggregate_id) VALUES ('%s', '{}', 't', 0, 'IN_PROGRESS', 'order',"
            + " 'a')";
    execute(ds, insert.formatted("e1"));
    assertThatThrownBy(() -> execute(ds, insert.formatted("e2")))
        .hasMessageContaining("UNIQUE constraint failed");
  }

  /** {@code NULLS NOT DISTINCT} in Postgres: a keyed command without a stream is stored once. */
  @Test
  void deadLetterIdempotencyKeyTreatsANullStreamAsOneValue() throws Exception {
    String insert =
        "INSERT INTO dead_letter_queue (command_id, command_type, payload, error_type, attempts,"
            + " first_attempt_at, published_at, idempotency_key) VALUES ('%s', 'c', '{}', 'e', 1,"
            + " 0, 0, %s)";
    execute(ds, insert.formatted("c1", "'k'"));
    assertThatThrownBy(() -> execute(ds, insert.formatted("c2", "'k'")))
        .hasMessageContaining("UNIQUE constraint failed");
    // Unkeyed commands never collide.
    execute(ds, insert.formatted("c3", "NULL"), insert.formatted("c4", "NULL"));
    // The same key on a command with a stream is a different command.
    execute(
        ds,
        "INSERT INTO dead_letter_queue (command_id, command_type, payload, aggregate_type,"
            + " aggregate_id, error_type, attempts, first_attempt_at, published_at,"
            + " idempotency_key) VALUES ('c5', 'c', '{}', 'order', 'a', 'e', 1, 0, 0, 'k')");
  }

  @Test
  void sagaEpisodeStampRuleIsEnforced() throws Exception {
    String insert =
        "INSERT INTO saga_state (saga_id, saga_type, status, state_json, created_at, updated_at,"
            + " episode_version, episode_claimed_at, episode_cause_event_id, pre_fault_status)"
            + " VALUES ('%s', 't', '%s', '{}', 0, 0, %s, %s, %s, %s)";
    execute(ds, insert.formatted("ok-running", "RUNNING", "NULL", "NULL", "NULL", "NULL"));
    execute(ds, insert.formatted("ok-comp", "COMPENSATING", "3", "10", "'ev'", "NULL"));
    execute(ds, insert.formatted("ok-faulted", "FAULTED", "NULL", "NULL", "NULL", "'RUNNING'"));
    for (String bad :
        List.of(
            insert.formatted("half-stamp", "RUNNING", "3", "NULL", "NULL", "NULL"),
            insert.formatted("comp-unstamped", "COMPENSATING", "NULL", "NULL", "NULL", "NULL"),
            insert.formatted(
                "fault-unstamped", "FAULTED", "NULL", "NULL", "NULL", "'COMPENSATING'"),
            insert.formatted("cause-unstamped", "RUNNING", "NULL", "NULL", "'ev'", "NULL"))) {
      assertThatThrownBy(() -> execute(ds, bad))
          .as(bad)
          .hasMessageContaining("CHECK constraint failed");
    }
  }

  @Test
  void sagaFlagsAreBooleans() throws Exception {
    assertThatThrownBy(
            () ->
                execute(
                    ds,
                    "INSERT INTO saga_state (saga_id, saga_type, status, state_json, created_at,"
                        + " updated_at, genesis_applied) VALUES ('s', 't', 'RUNNING', '{}', 0, 0,"
                        + " 2)"))
        .hasMessageContaining("CHECK constraint failed");
    execute(
        ds,
        "INSERT INTO saga_state (saga_id, saga_type, status, state_json, created_at, updated_at)"
            + " VALUES ('s', 't', 'RUNNING', '{}', 0, 0)");
    assertThat(
            query(
                ds,
                "SELECT version, genesis_applied, dead_letter_pending FROM saga_state"
                    + " WHERE saga_id = 's'"))
        .containsExactly(List.of(1L, 0L, 0L));
  }

  /** Null saga ids at one offset are distinct, as Postgres' default NULL semantics make them. */
  @Test
  void sagaDeadLetterKeyHasPostgresNullSemantics() throws Exception {
    String insert =
        "INSERT INTO saga_dead_letters (saga_id, saga_type, event_offset, event_type, error_type,"
            + " faulted_at, first_faulted_at) VALUES (%s, 't', 7, 'e', 'x', 0, 0)";
    execute(ds, insert.formatted("NULL"), insert.formatted("NULL"), insert.formatted("'s1'"));
    assertThatThrownBy(() -> execute(ds, insert.formatted("'s1'")))
        .hasMessageContaining("UNIQUE constraint failed");
  }

  @Test
  void timestampsAreEpochMicrosecondsAndHaveNoDatabaseClockDefault() {
    // The application Clock stamps every timestamp: a NOT NULL timestamp the statement omits fails.
    assertThatThrownBy(
            () ->
                execute(
                    ds,
                    "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id,"
                        + " version, event_type, payload, metadata) VALUES (1, 'order', 'a', 1,"
                        + " 't', '{}', '{}')"))
        .hasMessageContaining("NOT NULL constraint failed: event_stream.created_at");
  }

  private void insertEvent(long offset, String type, String id, Object version)
      throws SQLException {
    try (var connection = ds.getConnection();
        var statement =
            connection.prepareStatement(
                "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
                    + " event_type, payload, metadata, created_at)"
                    + " VALUES (?, ?, ?, ?, 'created', '{}', '{}', ?)")) {
      statement.setLong(1, offset);
      statement.setString(2, type);
      statement.setString(3, id);
      statement.setObject(4, version);
      statement.setLong(5, SqliteTimestamps.toMicros(java.time.Instant.EPOCH));
      statement.executeUpdate();
    }
  }
}
