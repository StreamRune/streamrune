package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.sqlite.SqliteSchemaSeriesTest.execute;

import java.nio.file.Path;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.sqlite.SqliteSchemaValidator.Issue;
import org.streamrune.sqlite.SqliteSchemaValidator.Severity;

/** {@link SqliteSchemaValidator} reports what {@code SchemaValidator} reports on Postgres. */
class SqliteSchemaValidatorTest {

  @TempDir Path dir;

  private SqliteDatabase db;
  private DataSource ds;

  @BeforeEach
  void open() {
    db = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("app.db")));
    ds = db.dataSource();
  }

  @AfterEach
  void close() {
    db.close();
  }

  @Test
  void aMigratedEventStoreHasNoIssues() {
    db.migrate(SqliteSchemaSeries.EVENT_STORE);

    assertThat(validate()).isEmpty();
    assertThatCode(() -> db.validate(SqliteSchemaSeries.EVENT_STORE)).doesNotThrowAnyException();
  }

  @Test
  void anEmptyDatabaseReportsEveryRequiredTableAndTheMissingHistory() {
    List<Issue> issues = validate();

    assertThat(issues)
        .filteredOn(issue -> issue.severity() == Severity.ERROR)
        .extracting(Issue::table)
        .containsExactlyElementsOf(SqliteSchemaSeriesTest.BASELINE_TABLES);
    assertThat(issues)
        .filteredOn(issue -> issue.severity() == Severity.ERROR)
        .extracting(Issue::message)
        .containsOnly("Required table missing");
    assertThat(issues)
        .filteredOn(issue -> issue.severity() == Severity.WARNING)
        .containsExactly(
            new Issue(
                Severity.WARNING,
                "flyway_schema_history_streamrune",
                "Flyway history table not found — schema may not be managed by Flyway"));
  }

  @Test
  void eachMissingTableAndColumnIsReported() throws Exception {
    db.migrate(SqliteSchemaSeries.EVENT_STORE);
    execute(
        ds,
        "DROP TABLE projection_dead_letters",
        "ALTER TABLE saga_dead_letters DROP COLUMN first_replay_started_at",
        "ALTER TABLE outbox_events DROP COLUMN span_id",
        "ALTER TABLE outbox_events DROP COLUMN trace_id");

    assertThat(validate())
        .containsExactly(
            new Issue(Severity.ERROR, "outbox_events", "Missing column: span_id"),
            new Issue(Severity.ERROR, "outbox_events", "Missing column: trace_id"),
            new Issue(Severity.ERROR, "projection_dead_letters", "Required table missing"),
            new Issue(
                Severity.ERROR, "saga_dead_letters", "Missing column: first_replay_started_at"));
    assertThatThrownBy(() -> db.validate(SqliteSchemaSeries.EVENT_STORE))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Schema validation failed:"
                + "\n  - [outbox_events] Missing column: span_id"
                + "\n  - [outbox_events] Missing column: trace_id"
                + "\n  - [projection_dead_letters] Required table missing"
                + "\n  - [saga_dead_letters] Missing column: first_replay_started_at");
  }

  @Test
  void namesMatchCaseInsensitivelyAsSqliteIdentifiersDo() throws Exception {
    db.migrate(SqliteSchemaSeries.EVENT_STORE);
    execute(
        ds,
        "DROP TABLE projection_dead_letters",
        "CREATE TABLE PROJECTION_DEAD_LETTERS (PROJECTION_NAME TEXT, FROM_OFFSET INTEGER,"
            + " TO_OFFSET INTEGER, ERROR_TYPE TEXT, ATTEMPTS INTEGER)");

    assertThat(validate()).isEmpty();
  }

  @Test
  void aMissingCounterRowIsAnError() throws Exception {
    db.migrate(SqliteSchemaSeries.EVENT_STORE);
    execute(ds, "DELETE FROM global_offset_counter");

    assertThat(validate())
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.severity()).isEqualTo(Severity.ERROR);
              assertThat(issue.table()).isEqualTo("global_offset_counter");
              assertThat(issue.message())
                  .startsWith("Missing the id=1 counter row")
                  .contains(
                      "INSERT INTO global_offset_counter (id, next_value) VALUES (1,"
                          + " COALESCE((SELECT max(global_offset) FROM event_stream), 0))");
            });
  }

  @Test
  void aCounterBehindTheEventsIsAnError() throws Exception {
    db.migrate(SqliteSchemaSeries.EVENT_STORE);
    execute(
        ds,
        "INSERT INTO event_stream (global_offset, aggregate_type, aggregate_id, version,"
            + " event_type, payload, metadata, created_at) VALUES (5, 'order', 'a', 1, 't', '{}',"
            + " '{}', 0)",
        "UPDATE global_offset_counter SET next_value = 4");

    assertThat(validate())
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.severity()).isEqualTo(Severity.ERROR);
              assertThat(issue.table()).isEqualTo("global_offset_counter");
              assertThat(issue.message())
                  .startsWith("next_value (4) is BEHIND max(event_stream.global_offset) (5)");
            });

    execute(ds, "UPDATE global_offset_counter SET next_value = 5");
    assertThat(validate()).isEmpty();
  }

  @Test
  void theCounterRowIsCheckedWithoutTheEventTable() throws Exception {
    db.migrate(SqliteSchemaSeries.EVENT_STORE);
    execute(ds, "DROP TABLE event_stream");

    assertThat(validate())
        .containsExactly(new Issue(Severity.ERROR, "event_stream", "Required table missing"));

    execute(ds, "DELETE FROM global_offset_counter");
    assertThat(validate()).extracting(Issue::table).contains("global_offset_counter");
  }

  @Test
  void aSchemaBehindTheExpectedVersionIsAnErrorAndAheadAWarning() throws Exception {
    db.migrate(SqliteSchemaSeries.EVENT_STORE);
    execute(ds, "UPDATE flyway_schema_history_streamrune SET success = 0 WHERE version = '001'");

    assertThat(validate())
        .containsExactly(
            new Issue(
                Severity.ERROR,
                "flyway_schema_history_streamrune",
                "Database schema version (0) is behind expected version (1) — run migrations"));

    execute(
        ds,
        "UPDATE flyway_schema_history_streamrune SET success = 1",
        "INSERT INTO flyway_schema_history_streamrune (installed_rank, version, description,"
            + " type, script, checksum, installed_by, execution_time, success)"
            + " VALUES (99, '7', 'later', 'SQL', 'V007__later.sql', 0, '', 0, 1)",
        "INSERT INTO flyway_schema_history_streamrune (installed_rank, version, description,"
            + " type, script, checksum, installed_by, execution_time, success)"
            + " VALUES (100, '9.1', 'dotted', 'SQL', 'V9_1__dotted.sql', 0, '', 0, 1)");
    assertThat(validate())
        .containsExactly(
            new Issue(
                Severity.WARNING,
                "flyway_schema_history_streamrune",
                "Database schema version (7) is ahead of expected version (1)"));
    assertThatCode(() -> db.validate(SqliteSchemaSeries.EVENT_STORE)).doesNotThrowAnyException();
  }

  @Test
  void anUnmanagedButCompleteSchemaOnlyWarns() throws Exception {
    db.migrate(SqliteSchemaSeries.EVENT_STORE);
    execute(ds, "DROP TABLE flyway_schema_history_streamrune");

    assertThat(validate())
        .containsExactly(
            new Issue(
                Severity.WARNING,
                "flyway_schema_history_streamrune",
                "Flyway history table not found — schema may not be managed by Flyway"));
    assertThatCode(() -> db.validate(SqliteSchemaSeries.EVENT_STORE)).doesNotThrowAnyException();
  }

  @Test
  void anEmptySeriesWithoutItsHistoryOnlyWarns() {
    assertThat(SqliteSchemaValidator.validate(ds, SqliteSchemaSeries.CRYPTO))
        .containsExactly(
            new Issue(
                Severity.WARNING,
                "flyway_schema_history_crypto",
                "Flyway history table not found — schema may not be managed by Flyway"));
    assertThatCode(() -> db.validate(SqliteSchemaSeries.CRYPTO)).doesNotThrowAnyException();
  }

  @Test
  void anUnreachableDatabaseIsAConnectionError() {
    DataSource broken = TestDatabases.url("jdbc:sqlite:" + dir.resolve("missing/dir/x.db"));

    assertThat(SqliteSchemaValidator.validate(broken, SqliteSchemaSeries.EVENT_STORE))
        .singleElement()
        .satisfies(
            issue -> {
              assertThat(issue.severity()).isEqualTo(Severity.ERROR);
              assertThat(issue.table()).isEqualTo("<connection>");
              assertThat(issue.message()).startsWith("Failed to connect to database: ");
            });
  }

  @Test
  void theFailureMessageListsOnlyErrors() {
    assertThat(
            SqliteSchemaValidator.failureMessage(
                List.of(
                    new Issue(Severity.WARNING, "w", "ignored"),
                    new Issue(Severity.ERROR, "a", "first"),
                    new Issue(Severity.ERROR, "b", "second"))))
        .isEqualTo("Schema validation failed:\n  - [a] first\n  - [b] second");
  }

  private List<Issue> validate() {
    return SqliteSchemaValidator.validate(ds, SqliteSchemaSeries.EVENT_STORE);
  }
}
