package org.streamrune.postgres;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;

/**
 * Validates that the database schema matches the expected StreamRune schema. Checks required
 * tables, critical columns, and Flyway migration version.
 */
public final class SchemaValidator {

  /**
   * The highest event-store migration this build ships: the baseline ({@code V001}) until the first
   * migration after it. A database whose namespaced history is behind this fails validation; one
   * ahead of it only warns.
   */
  static final int EXPECTED_VERSION = 1;

  private static final Map<String, Set<String>> REQUIRED_COLUMNS =
      Map.ofEntries(
          Map.entry(
              "event_stream",
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
                  "snapshot_version")),
          Map.entry("projection_offset", Set.of("projection_name", "last_offset", "epoch")),
          Map.entry(
              "subscription_leases", Set.of("consumer_name", "holder_id", "epoch", "lease_until")),
          // PostgresEventStore reserves every global offset from this single-row
          // counter (RESERVE_OFFSETS), so a self-managed schema that lacks it (or a restore
          // that missed the row) must fail validation, not fail 100% of appends at runtime. Table
          // presence is checked here; the id=1 row is checked separately (present-but-empty also
          // breaks appends).
          Map.entry("global_offset_sequence", Set.of("id", "next_value")),
          Map.entry(
              "saga_state",
              Set.of(
                  "saga_id",
                  "saga_type",
                  "status",
                  "state_json",
                  "version",
                  "episode_version",
                  // episode_claimed_at is read by every saga load SELECT and
                  // anchors the stale-compensation replay guard.
                  "episode_claimed_at",
                  // The five recorded recovery facts, read by every saga
                  // load SELECT and written by the executor / runner / replayer.
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
                  // Runtime-required outbox columns the validator omitted.
                  // processed_at (retention sweep + MARK_DELIVERED/MARK_FAILED) and
                  // next_retry_at (the claim statements / MARK_RETRY backoff scheduling).
                  "processed_at",
                  "next_retry_at",
                  // Ordering modes: the audited skip, read by every outbox SELECT.
                  "skipped_at",
                  "skipped_by",
                  "skip_reason")),
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
              Set.of("projection_name", "from_offset", "to_offset", "error_type", "attempts")),
          Map.entry(
              "saga_dead_letters",
              Set.of(
                  "saga_id",
                  // saga_type is read by every SELECT and scopes the targeted
                  // null-saga reads, the typed discard, the anchor and target writes, the retention
                  // guard and the conditional shield clear to the entry's own type.
                  "saga_type",
                  "event_offset",
                  "event_type",
                  "error_type",
                  "faulted_at",
                  // first_faulted_at is read by every SELECT, written once by the
                  // publish upsert, and anchors the retention sweep.
                  "first_faulted_at",
                  // target_saga_id is read by every SELECT, stamped by the
                  // replayer on TARGET_PENDING, and joined by the retention sweep so a null-saga
                  // entry whose resolved target saga is still FAULTED is never pruned.
                  "target_saga_id",
                  // first_replay_started_at is read by every SELECT, established
                  // once by establishFirstReplayAnchor before an entry's first replay feed, and
                  // anchors the re-drive key-age guard (STALE_REDRIVE_BLOCKED) — a missing column
                  // silently disarms the duplicate-forward-dispatch refusal.
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
                  // global_offsets is read by PostgresCommandInbox.find.
                  "global_offsets")),
          Map.entry(
              "dead_letter_queue",
              Set.of(
                  "command_id",
                  "command_type",
                  "payload",
                  // The stream the command targeted; read by every DLQ read and the replay.
                  "aggregate_type",
                  "aggregate_id",
                  "error_type",
                  "attempts",
                  "first_attempt_at",
                  // Read by every DLQ read; published_at also orders read and readRetryable.
                  "published_at",
                  "dlq_attempts",
                  // idempotency_key drives replay-key correctness.
                  "idempotency_key")));

  /**
   * Runtime-required columns of the two fixed-name crypto tables — the crypto counterpart of {@link
   * #REQUIRED_COLUMNS}. {@code forgotten_subjects}: every tombstone write/read matches on {@code
   * subject_id} ({@code JdbcForgottenSubjectStore}, {@code PostgresCryptoEngine.isForgotten});
   * {@code forgotten_at} is read by {@code JdbcForgottenSubjectStore.forgottenAt}; and {@code
   * key_created_at} is WRITTEN by every Vault erasure — {@code
   * JdbcForgottenSubjectStore.forget(subjectId, keyCreatedAt)} names it in its INSERT, so a table
   * missing it fails the GDPR erasure itself — exactly the boot-green, fail-at-first-use mode this
   * check exists to prevent. {@code erased_key_generations}: {@code deleteKey}'s GREATEST-upsert
   * and every mint's {@code nextKeyVersion} read both columns. Key-table columns live in {@link
   * #CRYPTO_KEY_TABLE_COLUMNS} because that table's NAME is configurable.
   */
  private static final Map<String, Set<String>> CRYPTO_REQUIRED_COLUMNS =
      Map.of(
          "forgotten_subjects",
          Set.of("subject_id", "forgotten_at", "key_created_at"),
          "erased_key_generations",
          Set.of("subject_id", "max_erased_version"));

  /**
   * Runtime-required columns of the per-subject key table ({@code encryption_keys} by default; name
   * configurable via {@code streamrune.crypto.postgres.table-name}): every key SELECT reads {@code
   * key_bytes, key_version}, every mint INSERTs all three, and {@code deleteKey}'s generation
   * recording SELECTs {@code key_version} — a table missing {@code key_version} boots green on
   * presence checks and then fails 100% of encrypt/decrypt/deleteKey at runtime ({@code column
   * "key_version" does not exist}), the exact failure mode this validator exists to prevent.
   */
  private static final Set<String> CRYPTO_KEY_TABLE_COLUMNS =
      Set.of("subject_id", "key_bytes", "key_version");

  /**
   * The key-table name the shipped {@code db/crypto-migration} baseline creates. Spelled out rather
   * than imported because this module cannot depend on the crypto module, and because the baseline
   * hardcodes it too. Any OTHER reported key-table name is operator-owned — see {@link
   * #missingCryptoColumnMessage} and {@link #missingCryptoTableMessage}.
   */
  private static final String DEFAULT_CRYPTO_KEY_TABLE = "encryption_keys";

  /**
   * The crypto baseline's definition of every runtime-required crypto column, so a missing-column
   * error can hand the operator the exact {@code ALTER TABLE … ADD COLUMN} to run. Column names are
   * unique across the three crypto tables except {@code subject_id}, which every table defines the
   * same way.
   */
  private static final Map<String, String> CRYPTO_COLUMN_DDL =
      Map.of(
          "subject_id", "VARCHAR(255) PRIMARY KEY",
          "key_bytes", "BYTEA NOT NULL",
          "key_version", "SMALLINT NOT NULL",
          "forgotten_at", "TIMESTAMPTZ NOT NULL DEFAULT NOW()",
          "key_created_at", "TIMESTAMPTZ",
          "max_erased_version", "SMALLINT NOT NULL");

  private SchemaValidator() {}

  /**
   * Validates the database schema against the expected StreamRune schema.
   *
   * @param dataSource the data source to validate
   * @return validation result with any discovered issues
   */
  public static SchemaValidationResult validate(DataSource dataSource) {
    var issues = new ArrayList<SchemaIssue>();
    try (var conn = dataSource.getConnection()) {
      DatabaseMetaData meta = conn.getMetaData();
      String schema = conn.getSchema();

      // Fail closed when the connection's schema cannot be determined: current_schema() is NULL
      // when the search_path names no existing schema (PostgreSQL accepts such a search_path
      // without error). A null schema pattern would make getTables/getColumns match ANY schema,
      // so a same-named table in a foreign schema (e.g. a fully migrated public) would validate
      // a database this connection cannot actually query.
      if (schema == null) {
        issues.add(
            new SchemaIssue(
                SchemaIssue.Severity.ERROR,
                "<schema>",
                "Cannot determine current schema — the connection's search_path resolves to no"
                    + " existing schema; fix currentSchema/search_path before validating"));
        return new SchemaValidationResult(List.copyOf(issues));
      }

      // Check required tables and columns
      for (var entry : REQUIRED_COLUMNS.entrySet()) {
        String table = entry.getKey();
        Set<String> requiredCols = entry.getValue();

        if (!tableExists(meta, schema, table)) {
          issues.add(new SchemaIssue(SchemaIssue.Severity.ERROR, table, "Required table missing"));
          continue;
        }

        Set<String> actualCols = getColumns(meta, schema, table);
        for (String col : requiredCols) {
          if (!actualCols.contains(col)) {
            issues.add(
                new SchemaIssue(SchemaIssue.Severity.ERROR, table, "Missing column: " + col));
          }
        }
      }

      // global_offset_sequence must contain its id=1 row — a present-but-empty table
      // still fails every append with EventStoreException("global_offset_sequence row missing").
      // And its next_value must not have fallen BEHIND max(event_stream
      // .global_offset), which fails every append just as totally and far less legibly.
      if (tableExists(meta, schema, "global_offset_sequence")) {
        checkGlobalOffsetSequenceRow(conn, tableExists(meta, schema, "event_stream"), issues);
      }

      // Check Flyway version
      // The event-store series is recorded in its OWN history table, never Flyway's
      // default one — that belongs to the consuming application, and reading it here would compare
      // EXPECTED_VERSION against the application's own series.
      if (tableExists(meta, schema, PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)) {
        checkFlywayVersion(conn, issues);
      } else {
        issues.add(
            new SchemaIssue(
                SchemaIssue.Severity.WARNING,
                PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE,
                "Flyway history table not found — schema may not be managed by Flyway"));
      }
    } catch (SQLException e) {
      issues.add(
          new SchemaIssue(
              SchemaIssue.Severity.ERROR,
              "<connection>",
              "Failed to connect to database: " + e.getMessage()));
    }
    return new SchemaValidationResult(List.copyOf(issues));
  }

  /**
   * Validates that the crypto-schema tables an active {@code CryptoEngine} requires (e.g. {@code
   * encryption_keys}, {@code forgotten_subjects}) exist.
   *
   * <p>These tables live in the opt-in {@code db/crypto-migration} Flyway location, which {@link
   * #validate(DataSource)} does not check. A deployment that manages its own schema and omits that
   * location boots clean, then throws {@code CryptoOperationException} on the first
   * {@code @Encrypted} operation and first GDPR forget. The event-store factory calls this so
   * startup instead fails fast with an actionable "apply classpath:db/crypto-migration" message.
   *
   * <p>The remediation text names the SEPARATE-HISTORY-TABLE requirement, not just the location.
   * {@code db/crypto-migration} is a second Flyway <em>history</em> (see {@link
   * PostgresEventStoreFactory#initializeSchema()}, which migrates it with {@code
   * .table("flyway_schema_history_crypto")}), NOT a second location on the main configuration:
   * Flyway locations share one version namespace and both series start at V001, so appending {@code
   * classpath:db/crypto-migration} to an existing {@code flyway.locations} aborts the operator's
   * own Flyway with {@code Found more than one migration with version 1} before any StreamRune code
   * runs. A message naming only the location steers operators into exactly that.
   *
   * @param dataSource the data source to validate
   * @param requiredTables the crypto tables the active engine requires (from {@code
   *     CryptoEngine.requiredCryptoTables()}); an empty set validates trivially
   * @return validation result, with an ERROR issue per missing table
   */
  public static SchemaValidationResult validateCryptoTables(
      DataSource dataSource, Set<String> requiredTables) {
    var issues = new ArrayList<SchemaIssue>();
    if (requiredTables == null || requiredTables.isEmpty()) {
      return new SchemaValidationResult(List.copyOf(issues));
    }
    try (var conn = dataSource.getConnection()) {
      DatabaseMetaData meta = conn.getMetaData();
      String schema = conn.getSchema();
      if (schema == null) {
        issues.add(
            new SchemaIssue(
                SchemaIssue.Severity.ERROR,
                "<schema>",
                "Cannot determine current schema — fix currentSchema/search_path before validating"));
        return new SchemaValidationResult(List.copyOf(issues));
      }
      for (String table : requiredTables) {
        if (!tableExists(meta, schema, table)) {
          issues.add(
              new SchemaIssue(SchemaIssue.Severity.ERROR, table, missingCryptoTableMessage(table)));
          continue;
        }
        // Presence alone is not sufficient — a self-managed
        // schema built from incomplete DDL (or a table created from the minimal literal reading of
        // the missing-table error above) boots green on presence and then fails every crypto
        // operation at runtime. Pin the runtime-required columns per table, mirroring the
        // event-store REQUIRED_COLUMNS check.
        Set<String> requiredCols = requiredCryptoColumns(table, requiredTables);
        if (requiredCols == null) {
          continue; // unknown third-party table: presence-only, no basis for column claims
        }
        Set<String> actualCols = getColumns(meta, schema, table);
        for (String col : requiredCols) {
          if (!actualCols.contains(col)) {
            issues.add(
                new SchemaIssue(
                    SchemaIssue.Severity.ERROR, table, missingCryptoColumnMessage(table, col)));
          }
        }
      }
    } catch (SQLException e) {
      issues.add(
          new SchemaIssue(
              SchemaIssue.Severity.ERROR,
              "<connection>",
              "Failed to connect to database: " + e.getMessage()));
    }
    return new SchemaValidationResult(List.copyOf(issues));
  }

  /**
   * The runtime-required columns for a crypto table the active engine reported, or {@code null} for
   * presence-only validation. The two fixed-name tables map directly. A name outside them is
   * treated as the configurable KEY table only when the reported set carries the {@code
   * erased_key_generations} signature: {@code PostgresCryptoEngine} is the only shipped engine that
   * reports that table, and its set is exactly {@code {keyTable, forgotten_subjects,
   * erased_key_generations}} — so within such a set the remaining name IS the key table (the engine
   * reports the configured name precisely so boot validation can target it). Without that signature
   * (a third-party engine's own tables), there is no basis for column claims and the check stays
   * presence-only rather than raising bogus errors.
   *
   * <p>"The remaining name" is load-bearing and now enforced: the inference holds only when EXACTLY
   * ONE reported name falls outside the fixed set. A set carrying a fourth table — a decorating
   * {@code CryptoEngine} adding its own, which {@code CachedCryptoEngine} makes possible by passing
   * the delegate's set straight through — used to get the key-table columns applied to that table
   * too, producing three bogus {@code Missing column} ERRORs and a boot abort on a schema that is
   * actually complete. When the difference is ambiguous, every unknown name falls back to
   * presence-only: losing a column check in an exotic topology is strictly better than failing a
   * correct deployment's boot.
   */
  private static Set<String> requiredCryptoColumns(String table, Set<String> requiredTables) {
    Set<String> known = CRYPTO_REQUIRED_COLUMNS.get(table);
    if (known != null) {
      return known;
    }
    if (!requiredTables.contains("erased_key_generations")) {
      return null;
    }
    long unknownNames =
        requiredTables.stream().filter(t -> !CRYPTO_REQUIRED_COLUMNS.containsKey(t)).count();
    return unknownNames == 1 ? CRYPTO_KEY_TABLE_COLUMNS : null;
  }

  /**
   * The remediation for a missing crypto table, branched on whether the shipped crypto baseline
   * creates it.
   *
   * <p>For the three baseline tables it does, and it creates each one only when absent, so enabling
   * schema auto-initialization — or applying the location as its own Flyway configuration — creates
   * exactly the missing ones. The message names the SEPARATE history table, not just the location:
   * {@code db/crypto-migration} is a second Flyway <em>history</em>, and appending it to an
   * existing {@code flyway.locations} aborts that Flyway with {@code Found more than one migration
   * with version 1} (both series start at V001). Once {@code flyway_schema_history_crypto} records
   * the baseline it does not run again, so a table dropped after that must be created by hand.
   *
   * <p>Any other name — a custom key-table name ({@code streamrune.crypto.postgres.table-name}) or
   * a third-party engine's own table — is never created by the series, so no re-application can
   * help; the operator creates it.
   */
  private static String missingCryptoTableMessage(String table) {
    String head =
        "Required crypto table missing — the configured CryptoEngine needs it for durable GDPR"
            + " erasure. ";
    if (CRYPTO_REQUIRED_COLUMNS.containsKey(table) || DEFAULT_CRYPTO_KEY_TABLE.equals(table)) {
      return head
          + "Enable schema auto-initialization"
          + " (streamrune.event-store.schema.auto-initialize=true), which creates every missing"
          + " crypto table, or apply classpath:db/crypto-migration as a SEPARATE Flyway"
          + " configuration with table=flyway_schema_history_crypto (that series shares its version"
          + " numbers with the event-store series — both start at V001 — so appending the location"
          + " to your main flyway.locations fails with 'Found more than one migration with version"
          + " 1'), or copy the DDL into your own migration series — see"
          + " streamrune-postgres-crypto/README.md. If flyway_schema_history_crypto already records"
          + " that baseline, it will not run again: create the table from the README's DDL.";
    }
    return head
        + "'"
        + table
        + "' is not created by the shipped db/crypto-migration series — it is a CUSTOM key-table"
        + " name (streamrune.crypto.postgres.table-name) or another engine's own table — so"
        + " create it yourself (the key-table DDL is in streamrune-postgres-crypto/README.md).";
  }

  /**
   * The remediation for a missing crypto column: the exact {@code ALTER TABLE … ADD COLUMN} for the
   * table as reported.
   *
   * <p>No re-application of the shipped series can supply a column to a table that already exists:
   * its baseline creates each crypto table only when ABSENT and never alters one, so "apply the
   * series" would send the operator round a loop that cannot terminate — the boot abort persists.
   * That holds for the default-named tables and even more for a CUSTOM key-table name ({@code
   * streamrune.crypto.postgres.table-name}), which the series never touches at all (and the
   * README's copy-the-DDL block hardcodes {@code encryption_keys}), so that branch says so
   * explicitly. Before this validator checked columns at all, such a deployment at least failed on
   * its first encrypt with a message that named the real object; making the failure earlier must
   * not make the guidance wrong.
   *
   * <p>{@code forgotten_subjects} additionally says WHY it is demanding columns the running engine
   * may never touch. {@link #CRYPTO_REQUIRED_COLUMNS} is keyed by table NAME, not by engine, and
   * that table is shared by every backend that reports it: {@code PostgresCryptoEngine} and {@code
   * AwsKmsCryptoEngine} read neither {@code forgotten_at} nor {@code key_created_at}, yet a
   * hand-created table missing either fails their boot. That is deliberate — a shared table cannot
   * be validated per engine without letting a schema pass here and then fail the Vault engine's
   * erasure, and this check's whole point is to fail closed — but an operator reading a boot abort
   * over a column their engine demonstrably never uses will otherwise conclude the error is wrong.
   */
  private static String missingCryptoColumnMessage(String table, String column) {
    String head =
        "Missing column: "
            + column
            + " — the configured CryptoEngine reads or writes it on every crypto operation, so the"
            + " deployment would boot green and then fail at first use. ";
    if ("forgotten_subjects".equals(table)) {
      head =
          "Missing column: "
              + column
              + " — forgotten_subjects is the GDPR erasure tombstone table, shared by every crypto"
              + " backend, and it is validated as a whole rather than per engine. Your engine may"
              + " never read this particular column (only the vault backend reads forgotten_at and"
              + " key_created_at; the postgres and aws-kms backends do not), but the table is"
              + " shared, so a partial one boots green here and then fails the vault backend's"
              + " erasure — a GDPR operation — at first use. Provision it in full. ";
    }
    String alter =
        "ALTER TABLE "
            + table
            + " ADD COLUMN "
            + column
            + " "
            + CRYPTO_COLUMN_DDL.getOrDefault(column, "<type>")
            + ";";
    if (CRYPTO_REQUIRED_COLUMNS.containsKey(table) || DEFAULT_CRYPTO_KEY_TABLE.equals(table)) {
      return head
          + "classpath:db/crypto-migration creates this table only when it is absent and never"
          + " alters an existing one, so re-applying it cannot add the column. Add it yourself: "
          + alter
          + " (the full DDL is in streamrune-postgres-crypto/README.md)";
    }
    return head
        + "'"
        + table
        + "' is a CUSTOM key-table name (streamrune.crypto.postgres.table-name), which the shipped"
        + " db/crypto-migration series never creates or alters, so re-applying the series cannot"
        + " fix this. Add the column yourself: "
        + alter
        + " (the full key-table DDL, and the same caveat, are in"
        + " streamrune-postgres-crypto/README.md)";
  }

  private static boolean tableExists(DatabaseMetaData meta, String schema, String table)
      throws SQLException {
    try (ResultSet rs = meta.getTables(null, schema, table, new String[] {"TABLE"})) {
      return rs.next();
    }
  }

  private static Set<String> getColumns(DatabaseMetaData meta, String schema, String table)
      throws SQLException {
    var columns = new java.util.HashSet<String>();
    try (ResultSet rs = meta.getColumns(null, schema, table, null)) {
      while (rs.next()) {
        columns.add(rs.getString("COLUMN_NAME"));
      }
    }
    return columns;
  }

  /**
   * Checks the single-row global-offset counter: the {@code id=1} row must exist and its {@code
   * next_value} must not have fallen behind {@code max(event_stream.global_offset)}.
   *
   * <p>A behind counter makes {@code PostgresEventStore.append} reserve offsets that are already
   * committed, so {@code INSERT_EVENT} — which writes {@code global_offset} explicitly — violates
   * {@code event_stream_pkey} on EVERY command. The reservation rolls back with the failed append,
   * so it never catches up on its own and no retry can resolve it. Reachable by restoring {@code
   * event_stream} from a newer dump than the counter, promoting a logical replica whose two tables
   * replicated with different lag, or importing event rows directly (which never advances this
   * counter). Failing boot here is strictly better than failing every append.
   *
   * <p><b>Both values are read in ONE statement.</b> An append commits the counter {@code UPDATE}
   * and the event {@code INSERT} in a single transaction, so the pair only ever advances together
   * and the invariant {@code next_value >= max(global_offset)} holds at every commit boundary — but
   * only if both are observed under the SAME snapshot. Reading them in two statements gives each
   * its own READ COMMITTED snapshot, and any append that commits in between makes the second read
   * see rows the first could not: {@code next_value} from before the append, {@code
   * max(global_offset)} from after it. That is a perfectly healthy database, and it would be
   * reported as a desynced counter — refusing the boot of an HA replica that validates while its
   * sibling serves writes, and prescribing a re-seed of a counter that is exactly right. A single
   * statement (subquery included) evaluates under one snapshot, so the two values are always
   * mutually consistent and only a genuine, committed divergence can trip the check.
   */
  private static void checkGlobalOffsetSequenceRow(
      java.sql.Connection conn, boolean eventStreamExists, List<SchemaIssue> issues)
      throws SQLException {
    // Without event_stream there is nothing to compare against (and the subquery would not even
    // parse) — the missing-table ERROR already covers that database, so only check the row exists.
    String sql =
        eventStreamExists
            ? "SELECT next_value, (SELECT COALESCE(max(global_offset), 0) FROM event_stream)"
                + " FROM global_offset_sequence WHERE id = 1"
            : "SELECT next_value FROM global_offset_sequence WHERE id = 1";
    long nextValue;
    long maxOffset;
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql)) {
      if (!rs.next()) {
        issues.add(
            new SchemaIssue(
                SchemaIssue.Severity.ERROR,
                "global_offset_sequence",
                "Missing the id=1 counter row — PostgresEventStore reserves every global offset from"
                    + " it, so every append will fail at runtime. Re-seed it with: INSERT INTO"
                    + " global_offset_sequence (id, next_value) VALUES (1,"
                    + " COALESCE((SELECT max(global_offset) FROM event_stream), 0))"));
        return;
      }
      nextValue = rs.getLong(1);
      if (!eventStreamExists) {
        return;
      }
      maxOffset = rs.getLong(2);
    }
    if (nextValue < maxOffset) {
      issues.add(
          new SchemaIssue(
              SchemaIssue.Severity.ERROR,
              "global_offset_sequence",
              "next_value ("
                  + nextValue
                  + ") is BEHIND max(event_stream.global_offset) ("
                  + maxOffset
                  + ") — the offset counter is desynced and every append would collide on"
                  + " event_stream_pkey with no retry able to resolve it. Re-seed it with: UPDATE"
                  + " global_offset_sequence SET next_value = (SELECT COALESCE(max(global_offset),"
                  + " 0) FROM event_stream) WHERE id = 1"));
    }
  }

  private static void checkFlywayVersion(java.sql.Connection conn, List<SchemaIssue> issues)
      throws SQLException {
    try (var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                "SELECT MAX(CAST(version AS INTEGER)) FROM "
                    + PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE
                    + " WHERE success = true AND version ~ '^[0-9]+$'")) {
      if (rs.next()) {
        int maxVersion = rs.getInt(1);
        if (maxVersion > EXPECTED_VERSION) {
          issues.add(
              new SchemaIssue(
                  SchemaIssue.Severity.WARNING,
                  PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE,
                  "Database schema version ("
                      + maxVersion
                      + ") is ahead of expected version ("
                      + EXPECTED_VERSION
                      + ")"));
        } else if (maxVersion < EXPECTED_VERSION) {
          issues.add(
              new SchemaIssue(
                  SchemaIssue.Severity.ERROR,
                  PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE,
                  "Database schema version ("
                      + maxVersion
                      + ") is behind expected version ("
                      + EXPECTED_VERSION
                      + ") — run migrations"));
        }
      }
    }
  }
}
