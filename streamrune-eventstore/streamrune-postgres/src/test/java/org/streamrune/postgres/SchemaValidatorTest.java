package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class SchemaValidatorTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("schema_test");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void setUp() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  @Test
  void validSchemaPassesValidation() {
    Flyway.configure()
        .dataSource(dataSource)
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();

    var result = SchemaValidator.validate(dataSource);
    assertTrue(result.isValid(), () -> "Validation failed: " + result.issues());
    // The highest migration on the classpath must equal EXPECTED_VERSION: a fully migrated DB is
    // neither ahead (WARNING) nor behind (ERROR).
    // Pins EXPECTED_VERSION=1 to the single shipped baseline (V001).
    assertEquals(1, SchemaValidator.EXPECTED_VERSION);
    assertTrue(
        result.issues().stream().noneMatch(i -> i.message().contains("version")),
        () -> "A fully migrated DB must have no version mismatch: " + result.issues());
  }

  @Test
  void emptyDatabaseFailsValidation() throws Exception {
    // Use a separate schema to get a clean database
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS empty_schema");
      stmt.execute("SET search_path TO empty_schema");
    }

    var emptyDs = new PGSimpleDataSource();
    emptyDs.setUrl(PG.getJdbcUrl() + "&currentSchema=empty_schema");
    emptyDs.setUser(PG.getUsername());
    emptyDs.setPassword(PG.getPassword());

    var result = SchemaValidator.validate(emptyDs);
    assertFalse(result.isValid());

    // Should report missing tables
    var errors =
        result.issues().stream().filter(i -> i.severity() == SchemaIssue.Severity.ERROR).toList();
    assertFalse(errors.isEmpty());
    assertTrue(
        errors.stream().anyMatch(i -> i.table().equals("event_stream")),
        "Should report missing event_stream table");
  }

  @Test
  void missingColumnReportsError() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS partial_schema");
      stmt.execute("SET search_path TO partial_schema");
      // Create event_stream without schema_version column
      stmt.execute(
          """
          CREATE TABLE event_stream (
              global_offset BIGSERIAL PRIMARY KEY,
              stream_id VARCHAR(255) NOT NULL,
              version BIGINT NOT NULL,
              event_type VARCHAR(255) NOT NULL,
              payload JSONB NOT NULL,
              metadata JSONB NOT NULL,
              created_at TIMESTAMPTZ DEFAULT NOW()
          )""");
    }

    var partialDs = new PGSimpleDataSource();
    partialDs.setUrl(PG.getJdbcUrl() + "&currentSchema=partial_schema");
    partialDs.setUser(PG.getUsername());
    partialDs.setPassword(PG.getPassword());

    var result = SchemaValidator.validate(partialDs);
    assertFalse(result.isValid());

    assertTrue(
        result.issues().stream()
            .anyMatch(
                i -> i.table().equals("event_stream") && i.message().contains("schema_version")),
        "Should report missing schema_version column");
    // A schema that still has stream_id but lacks the stream pair is reported; the extra column is
    // ignored.
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i -> i.table().equals("event_stream") && i.message().contains("aggregate_type")),
        "a pre-typed-stream schema is reported");
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i -> i.table().equals("event_stream") && i.message().contains("aggregate_id")),
        "a pre-typed-stream schema is reported");
    assertTrue(
        result.issues().stream().noneMatch(i -> i.message().contains("stream_id")),
        "an extra column is ignored, never required");
  }

  @Test
  void unresolvableSearchPathFailsClosedInsteadOfMatchingForeignSchemas() {
    // currentSchema pointing at a schema that does not exist: PostgreSQL accepts the search_path
    // (existence is not checked), but current_schema() is NULL. Validation must fail closed — a
    // null schema passed to DatabaseMetaData.getTables would match required tables in ANY schema
    // (e.g. a fully migrated public), reporting a database this connection cannot actually query
    // as valid.
    var brokenDs = new PGSimpleDataSource();
    brokenDs.setUrl(PG.getJdbcUrl() + "&currentSchema=no_such_schema");
    brokenDs.setUser(PG.getUsername());
    brokenDs.setPassword(PG.getPassword());

    var result = SchemaValidator.validate(brokenDs);

    assertFalse(result.isValid());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("<schema>")
                        && i.message().contains("search_path")),
        () -> "Expected fail-closed <schema> error, got: " + result.issues());
  }

  @Test
  void flywayVersionMismatchReportsWarning() throws Exception {
    // After full migration, manually insert a future version into the namespaced history table
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("SET search_path TO public");
      stmt.execute(
          """
          INSERT INTO flyway_schema_history_streamrune
              (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success)
          VALUES (99, '99', 'future migration', 'SQL', 'V099__future.sql', 0, 'test', 0, true)""");
    }

    var result = SchemaValidator.validate(dataSource);
    // Still valid (warning, not error) — but has a version mismatch warning
    assertTrue(result.isValid());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.WARNING
                        && i.message().contains("version")),
        "Should warn about version mismatch");

    // Clean up
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM flyway_schema_history_streamrune WHERE installed_rank = 99");
    }
  }

  @Test
  void schemaVersionBehindExpectedReportsError() throws Exception {
    // A database whose namespaced history is BEHIND EXPECTED_VERSION must fail validation on the
    // version check itself. Staged so that nothing ELSE can fail it: every required table and
    // column is present (built by the shipped migration under a throwaway history table), while
    // flyway_schema_history_streamrune records only Flyway's baseline marker at version 0 — the
    // history of a schema adopted by baselineOnMigrate whose shipped migration was never recorded
    // as applied. REQUIRED_COLUMNS and the offset-counter row check are satisfied, so the
    // behind-version branch of checkFlywayVersion is the ONLY thing that can make this database
    // invalid, and the assertions pin that it is the sole ERROR — deleting that branch turns this
    // red, it cannot pass on an unrelated error.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS behind_version_schema");
    }
    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl() + "&currentSchema=behind_version_schema");
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());

    Flyway.configure()
        .dataSource(ds)
        .schemas("behind_version_schema")
        .locations("classpath:db/streamrune-migration")
        .table("throwaway_history")
        .load()
        .migrate();
    try (var conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE throwaway_history");
    }
    Flyway.configure()
        .dataSource(ds)
        .schemas("behind_version_schema")
        .table("flyway_schema_history_streamrune")
        .baselineVersion("0")
        .load()
        .baseline();

    var result = SchemaValidator.validate(ds);
    var errors =
        result.issues().stream().filter(i -> i.severity() == SchemaIssue.Severity.ERROR).toList();
    assertFalse(
        result.isValid(), () -> "A schema behind EXPECTED_VERSION must fail: " + result.issues());
    assertEquals(
        1, errors.size(), () -> "the behind-version ERROR must be the only one: " + errors);
    assertEquals("flyway_schema_history_streamrune", errors.get(0).table());
    assertTrue(
        errors
            .get(0)
            .message()
            .contains("(0) is behind expected version (" + SchemaValidator.EXPECTED_VERSION + ")"),
        () -> "Should report the schema version is behind expected: " + errors);
  }

  @Test
  void missingNewlyRequiredTableReportsError() throws Exception {
    // A DB that was migrated but is missing one of the REQUIRED_COLUMNS tables (saga_dead_letters,
    // command_inbox, dead_letter_queue) must be flagged.
    // Fully migrate a fresh schema, then drop dead_letter_queue and re-validate.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS missing_dlq_schema");
    }

    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl() + "&currentSchema=missing_dlq_schema");
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());

    Flyway.configure()
        .dataSource(ds)
        .schemas("missing_dlq_schema")
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();

    try (var conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE dead_letter_queue");
    }

    var result = SchemaValidator.validate(ds);
    assertFalse(result.isValid(), () -> "Missing dead_letter_queue must fail: " + result.issues());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("dead_letter_queue")),
        () -> "Should report missing dead_letter_queue table: " + result.issues());
  }

  @Test
  void missingGlobalOffsetSequenceRowReportsError() throws Exception {
    // A present-but-empty global_offset_sequence (e.g. the single row lost in a
    // restore/copy) validated green before this change, then failed 100% of appends at runtime with
    // EventStoreException("global_offset_sequence row missing"). Fully migrate, delete the id=1
    // row,
    // and assert validation now flags it as an ERROR.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS missing_gos_row_schema");
    }
    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl() + "&currentSchema=missing_gos_row_schema");
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());

    Flyway.configure()
        .dataSource(ds)
        .schemas("missing_gos_row_schema")
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();

    try (var conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM global_offset_sequence WHERE id = 1");
    }

    var result = SchemaValidator.validate(ds);
    assertFalse(
        result.isValid(),
        () -> "A present-but-empty global_offset_sequence must fail: " + result.issues());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("global_offset_sequence")
                        && i.message().contains("id=1")),
        () -> "Should report the missing id=1 counter row: " + result.issues());
  }

  @Test
  void missingRuntimeRequiredColumnReportsError() throws Exception {
    // Columns the store SQL hard-depends on are required (here
    // command_inbox.global_offsets, read by PostgresCommandInbox.find). Fully
    // migrate a fresh schema, drop the column, and assert validation flags it.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS missing_col_schema");
    }
    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl() + "&currentSchema=missing_col_schema");
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());

    Flyway.configure()
        .dataSource(ds)
        .schemas("missing_col_schema")
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();

    try (var conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("ALTER TABLE command_inbox DROP COLUMN global_offsets");
    }

    var result = SchemaValidator.validate(ds);
    assertFalse(
        result.isValid(),
        () -> "Missing command_inbox.global_offsets must fail: " + result.issues());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("command_inbox")
                        && i.message().contains("global_offsets")),
        () -> "Should report missing command_inbox.global_offsets column: " + result.issues());
  }

  @Test
  void missingSkipColumnReportsError() throws Exception {
    // Ordering modes: the three audited-skip columns (skipped_at, skipped_by, skip_reason) are
    // read by every outbox SELECT and written by skipFailed, so they are runtime-required. Fully
    // migrate a fresh schema, drop one of them, and assert validation flags it.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS missing_skip_col_schema");
    }
    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl() + "&currentSchema=missing_skip_col_schema");
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());

    Flyway.configure()
        .dataSource(ds)
        .schemas("missing_skip_col_schema")
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();

    try (var conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("ALTER TABLE outbox_events DROP COLUMN skip_reason");
    }

    var result = SchemaValidator.validate(ds);
    assertFalse(
        result.isValid(), () -> "Missing outbox_events.skip_reason must fail: " + result.issues());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("outbox_events")
                        && i.message().contains("skip_reason")),
        () -> "Should report missing outbox_events.skip_reason column: " + result.issues());
  }

  @Test
  void missingSagaDeadLetterTypeColumnReportsError() throws Exception {
    // saga_dead_letters.saga_type scopes every PostgresSagaDeadLetterStore statement to the
    // entry's own saga type; without it every publish and read fails at runtime, so its absence
    // must fail validation instead.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS missing_sdl_type_schema");
    }
    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl() + "&currentSchema=missing_sdl_type_schema");
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());

    Flyway.configure()
        .dataSource(ds)
        .schemas("missing_sdl_type_schema")
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();

    try (var conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("ALTER TABLE saga_dead_letters DROP COLUMN saga_type");
    }

    var result = SchemaValidator.validate(ds);
    assertFalse(
        result.isValid(),
        () -> "Missing saga_dead_letters.saga_type must fail: " + result.issues());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("saga_dead_letters")
                        && i.message().contains("saga_type")),
        () -> "Should report missing saga_dead_letters.saga_type column: " + result.issues());
  }

  @Test
  void factoryTolerantAndValidatorProducesWarningForSchemaAhead() throws Exception {
    // The factory validates by default and must NOT throw on a WARNING-only result (schema version
    // ahead of expected). It logs each WARNING (see PostgresEventStoreFactory.create) rather than
    // silently discarding it. We assert the validator surfaces the WARNING and the factory does not
    // throw on it. Uses its own fully migrated schema so it is independent of test-execution order.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS schema_ahead");
    }

    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl() + "&currentSchema=schema_ahead");
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());

    Flyway.configure()
        .dataSource(ds)
        .schemas("schema_ahead")
        .locations("classpath:db/streamrune-migration")
        .table("flyway_schema_history_streamrune")
        .baselineOnMigrate(false)
        .load()
        .migrate();

    // Insert a version beyond EXPECTED_VERSION so the validator emits a schema-ahead WARNING.
    try (var conn = ds.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          """
          INSERT INTO flyway_schema_history_streamrune
              (installed_rank, version, description, type, script, checksum, installed_by, execution_time, success)
          VALUES (98, '98', 'future migration', 'SQL', 'V098__future.sql', 0, 'test', 0, true)""");
    }

    var result = SchemaValidator.validate(ds);
    assertTrue(result.isValid(), () -> "Schema-ahead is a WARNING, not ERROR: " + result.issues());
    assertTrue(
        result.issues().stream().anyMatch(i -> i.severity() == SchemaIssue.Severity.WARNING),
        () -> "Schema-ahead must produce a WARNING issue the factory logs: " + result.issues());

    var factory =
        new PostgresEventStoreFactory(
            ds,
            new org.streamrune.core.EventTypeRegistry() {
              @Override
              public Class<?> resolveEventType(org.streamrune.core.types.EventType eventType) {
                return Object.class;
              }

              @Override
              public Class<?> resolveStateType(String stateType) {
                return Object.class;
              }

              @Override
              public java.util.Collection<Class<?>> registeredTypes() {
                // No @Encrypted-bearing types are exercised through this registry in this test;
                // report none explicitly rather than inheriting the throwing default.
                return java.util.List.of();
              }
            });
    assertDoesNotThrow(factory::create, "WARNING-only validation must not fail create()");
  }

  @Test
  void factoryValidatesSchemaByDefault() {
    // Factory with valid schema should not throw
    var factory =
        new PostgresEventStoreFactory(
            dataSource,
            new org.streamrune.core.EventTypeRegistry() {
              @Override
              public Class<?> resolveEventType(org.streamrune.core.types.EventType eventType) {
                return Object.class;
              }

              @Override
              public Class<?> resolveStateType(String stateType) {
                return Object.class;
              }

              @Override
              public java.util.Collection<Class<?>> registeredTypes() {
                // No @Encrypted-bearing types are exercised through this registry in this test;
                // report none explicitly rather than inheriting the throwing default.
                return java.util.List.of();
              }
            });
    assertDoesNotThrow(factory::create);
  }

  @Test
  void factorySkipsValidationWhenDisabled() {
    var factory =
        new PostgresEventStoreFactory(
                dataSource,
                new org.streamrune.core.EventTypeRegistry() {
                  @Override
                  public Class<?> resolveEventType(org.streamrune.core.types.EventType eventType) {
                    return Object.class;
                  }

                  @Override
                  public Class<?> resolveStateType(String stateType) {
                    return Object.class;
                  }

                  @Override
                  public java.util.Collection<Class<?>> registeredTypes() {
                    // No @Encrypted-bearing types are exercised through this registry in this
                    // test; report none explicitly rather than inheriting the throwing
                    // default.
                    return java.util.List.of();
                  }
                })
            .validateSchema(false);
    assertDoesNotThrow(factory::create);
  }
}
