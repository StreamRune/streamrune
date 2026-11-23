package org.streamrune.sqlite;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import javax.sql.DataSource;

/**
 * Checks that a SQLite database holds a schema series: the tables and the columns the runtime reads
 * and writes, the global-offset counter row, and the version recorded in the series' history table.
 * The checks and their messages are those of the Postgres {@code SchemaValidator}.
 *
 * <p>Column types are not checked, as on Postgres: a column is required by name. A table the
 * application provisioned without {@code STRICT} therefore passes, and loses only SQLite's type
 * enforcement. Table and column names match case-insensitively, as SQLite resolves identifiers.
 *
 * <p>Issues come in a fixed order: tables in name order, each table's columns in name order, then
 * the counter row, then the version.
 */
final class SqliteSchemaValidator {

  /** How serious an issue is: an {@link #ERROR} fails validation, a {@link #WARNING} is logged. */
  enum Severity {
    WARNING,
    ERROR
  }

  /** One finding: the table it concerns and what is wrong. */
  record Issue(Severity severity, String table, String message) {}

  private static final String COUNTER_TABLE = "global_offset_counter";
  private static final String EVENT_TABLE = "event_stream";

  /** The triggers that keep event_stream.global_offset at or below the counter (V001). */
  private static final List<String> OFFSET_TRIGGERS =
      List.of("event_stream_offset_from_counter", "event_stream_offset_from_counter_on_update");

  /**
   * The tables of each series and the columns the runtime reads and writes: the Postgres lists,
   * without the lease table (leadership is a no-op on SQLite) and with {@code
   * global_offset_counter} for {@code global_offset_sequence}. The crypto series ships no tables
   * yet.
   */
  private static final Map<SqliteSchemaSeries, SortedMap<String, SortedSet<String>>> REQUIRED =
      Map.of(
          SqliteSchemaSeries.EVENT_STORE,
          sorted(
              Map.ofEntries(
                  Map.entry(
                      EVENT_TABLE,
                      Set.of(
                          "global_offset",
                          "aggregate_type",
                          "aggregate_id",
                          "version",
                          "event_type",
                          "payload",
                          "metadata",
                          "schema_version")),
                  Map.entry(
                      "snapshot_store",
                      Set.of(
                          "aggregate_type",
                          "aggregate_id",
                          "version",
                          "state_type",
                          "state_payload",
                          "snapshot_version",
                          "purge_generation")),
                  Map.entry(
                      "subscription_checkpoint", Set.of("projection_name", "last_offset", "epoch")),
                  // Every append reserves its offsets from this one-row counter, so a schema that
                  // lacks it fails every append; the row itself is checked separately.
                  Map.entry(COUNTER_TABLE, Set.of("id", "next_value")),
                  Map.entry(
                      "saga_state",
                      Set.of(
                          "saga_id",
                          "saga_type",
                          "status",
                          "state_json",
                          "version",
                          "episode_version",
                          "episode_claimed_at",
                          "episode_cause_event_id",
                          "genesis_applied",
                          "pre_fault_status",
                          "dead_letter_pending",
                          "last_applied_offset",
                          "last_replayed_offset")),
                  Map.entry(
                      "outbox_events",
                      Set.of(
                          "entry_id",
                          "payload",
                          "payload_type",
                          "status",
                          "attempts",
                          "seq",
                          "aggregate_type",
                          "aggregate_id",
                          "claimed_at",
                          "claimed_by",
                          "processed_at",
                          "next_retry_at",
                          "skipped_at",
                          "skipped_by",
                          "skip_reason",
                          "trace_id",
                          "span_id")),
                  Map.entry(
                      "audit_log",
                      Set.of(
                          "id",
                          "command_id",
                          "command_type",
                          "aggregate_type",
                          "aggregate_id",
                          "occurred_at",
                          "outcome")),
                  Map.entry(
                      "event_audit_log",
                      Set.of(
                          "id",
                          "event_id",
                          "event_type",
                          "aggregate_type",
                          "aggregate_id",
                          "version",
                          "command_id",
                          "correlation_id")),
                  Map.entry(
                      "projection_dead_letters",
                      Set.of(
                          "projection_name", "from_offset", "to_offset", "error_type", "attempts")),
                  Map.entry(
                      "saga_dead_letters",
                      Set.of(
                          "saga_id",
                          "saga_type",
                          "event_offset",
                          "event_type",
                          "error_type",
                          "faulted_at",
                          "first_faulted_at",
                          "target_saga_id",
                          "first_replay_started_at")),
                  Map.entry(
                      "command_inbox",
                      Set.of(
                          "idempotency_key",
                          "command_type",
                          "aggregate_type",
                          "aggregate_id",
                          "final_version",
                          "processed_at",
                          "global_offsets")),
                  Map.entry(
                      "dead_letter_queue",
                      Set.of(
                          "command_id",
                          "command_type",
                          "payload",
                          "aggregate_type",
                          "aggregate_id",
                          "error_type",
                          "attempts",
                          "first_attempt_at",
                          "published_at",
                          "dlq_attempts",
                          "idempotency_key")))),
          SqliteSchemaSeries.READ_MODEL,
          sorted(Map.of("readmodel_checkpoint", Set.of("projection_name", "last_offset", "epoch"))),
          SqliteSchemaSeries.CRYPTO,
          sorted(Map.of()));

  private SqliteSchemaValidator() {}

  /**
   * Validates {@code series} on the database behind {@code dataSource}.
   *
   * @return every issue found; empty when the series is complete. A database that cannot be read
   *     yields one ERROR for the table {@code <connection>}
   */
  static List<Issue> validate(DataSource dataSource, SqliteSchemaSeries series) {
    List<Issue> issues = new ArrayList<>();
    try (Connection connection = dataSource.getConnection()) {
      for (var entry : REQUIRED.get(series).entrySet()) {
        String table = entry.getKey();
        if (!tableExists(connection, table)) {
          issues.add(new Issue(Severity.ERROR, table, "Required table missing"));
          continue;
        }
        Set<String> actual = columns(connection, table);
        for (String column : entry.getValue()) {
          if (!actual.contains(column)) {
            issues.add(new Issue(Severity.ERROR, table, "Missing column: " + column));
          }
        }
      }
      if (series == SqliteSchemaSeries.EVENT_STORE && tableExists(connection, COUNTER_TABLE)) {
        checkCounterRow(connection, tableExists(connection, EVENT_TABLE), issues);
      }
      if (series == SqliteSchemaSeries.EVENT_STORE && tableExists(connection, EVENT_TABLE)) {
        for (String trigger : OFFSET_TRIGGERS) {
          if (!triggerExists(connection, trigger)) {
            issues.add(
                new Issue(
                    Severity.ERROR,
                    EVENT_TABLE,
                    "Missing trigger: "
                        + trigger
                        + " — it refuses event rows whose global_offset the counter did not hand"
                        + " out; re-create it from db/sqlite-migration/V001__baseline.sql"));
          }
        }
      }
      String history = series.historyTable();
      if (tableExists(connection, history)) {
        checkVersion(connection, history, series.expectedVersion(), issues);
      } else {
        issues.add(
            new Issue(
                Severity.WARNING,
                history,
                "Flyway history table not found — schema may not be managed by Flyway"));
      }
    } catch (SQLException e) {
      issues.add(
          new Issue(
              Severity.ERROR, "<connection>", "Failed to connect to database: " + e.getMessage()));
    }
    return List.copyOf(issues);
  }

  /** The message an invalid schema fails with: every ERROR, one per line, as on Postgres. */
  static String failureMessage(List<Issue> issues) {
    StringBuilder message = new StringBuilder("Schema validation failed:");
    for (Issue issue : issues) {
      if (issue.severity() == Severity.ERROR) {
        message.append("\n  - [").append(issue.table()).append("] ").append(issue.message());
      }
    }
    return message.toString();
  }

  private static boolean triggerExists(Connection connection, String trigger) throws SQLException {
    try (PreparedStatement ps =
        connection.prepareStatement(
            "SELECT 1 FROM sqlite_schema WHERE type = 'trigger' AND name = ? COLLATE NOCASE")) {
      ps.setString(1, trigger);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static boolean tableExists(Connection connection, String table) throws SQLException {
    try (PreparedStatement statement =
        connection.prepareStatement(
            "SELECT 1 FROM pragma_table_list WHERE schema = 'main' AND type = 'table'"
                + " AND name = ? COLLATE NOCASE")) {
      statement.setString(1, table);
      try (ResultSet rs = statement.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static Set<String> columns(Connection connection, String table) throws SQLException {
    Set<String> columns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
    try (PreparedStatement statement =
        connection.prepareStatement("SELECT name FROM pragma_table_info(?, 'main')")) {
      statement.setString(1, table);
      try (ResultSet rs = statement.executeQuery()) {
        while (rs.next()) {
          columns.add(rs.getString(1));
        }
      }
    }
    return columns;
  }

  /**
   * The {@code id=1} counter row must exist, and its {@code next_value} must not be behind {@code
   * max(event_stream.global_offset)}: a missing row fails every append, and a counter behind the
   * events makes every append collide on {@code event_stream}'s primary key. Both values are read
   * in one statement, so they come from one snapshot.
   */
  private static void checkCounterRow(
      Connection connection, boolean eventTableExists, List<Issue> issues) throws SQLException {
    String sql =
        eventTableExists
            ? "SELECT next_value, (SELECT COALESCE(max(global_offset), 0) FROM event_stream)"
                + " FROM global_offset_counter WHERE id = 1"
            : "SELECT next_value FROM global_offset_counter WHERE id = 1";
    long nextValue;
    long maxOffset;
    try (Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(sql)) {
      if (!rs.next()) {
        issues.add(
            new Issue(
                Severity.ERROR,
                COUNTER_TABLE,
                "Missing the id=1 counter row — the event store reserves every global offset from"
                    + " it, so every append will fail at runtime. Re-seed it with: INSERT INTO"
                    + " global_offset_counter (id, next_value) VALUES (1,"
                    + " COALESCE((SELECT max(global_offset) FROM event_stream), 0))"));
        return;
      }
      nextValue = rs.getLong(1);
      if (!eventTableExists) {
        return;
      }
      maxOffset = rs.getLong(2);
    }
    if (nextValue < maxOffset) {
      issues.add(
          new Issue(
              Severity.ERROR,
              COUNTER_TABLE,
              "next_value ("
                  + nextValue
                  + ") is BEHIND max(event_stream.global_offset) ("
                  + maxOffset
                  + ") — the offset counter is desynced and every append would collide on"
                  + " event_stream's primary key with no retry able to resolve it. Re-seed it with:"
                  + " UPDATE global_offset_counter SET next_value = (SELECT"
                  + " COALESCE(max(global_offset), 0) FROM event_stream) WHERE id = 1"));
    }
  }

  /**
   * Compares the highest successfully applied numeric version in {@code history} with {@code
   * expected}: behind is an ERROR, ahead a WARNING.
   */
  private static void checkVersion(
      Connection connection, String history, int expected, List<Issue> issues) throws SQLException {
    try (Statement statement = connection.createStatement();
        ResultSet rs =
            statement.executeQuery(
                "SELECT MAX(CAST(version AS INTEGER)) FROM \""
                    + history
                    + "\" WHERE success = 1 AND version GLOB '[0-9]*'"
                    + " AND version NOT GLOB '*[^0-9]*'")) {
      rs.next();
      int recorded = rs.getInt(1);
      if (recorded > expected) {
        issues.add(
            new Issue(
                Severity.WARNING,
                history,
                "Database schema version ("
                    + recorded
                    + ") is ahead of expected version ("
                    + expected
                    + ")"));
      } else if (recorded < expected) {
        issues.add(
            new Issue(
                Severity.ERROR,
                history,
                "Database schema version ("
                    + recorded
                    + ") is behind expected version ("
                    + expected
                    + ") — run migrations"));
      }
    }
  }

  private static SortedMap<String, SortedSet<String>> sorted(Map<String, Set<String>> tables) {
    SortedMap<String, SortedSet<String>> sorted = new TreeMap<>();
    tables.forEach(
        (table, columns) ->
            sorted.put(table, Collections.unmodifiableSortedSet(new TreeSet<>(columns))));
    return Collections.unmodifiableSortedMap(sorted);
  }
}
