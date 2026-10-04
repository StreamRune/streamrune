package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;

/**
 * The shared crypto schema ({@code forgotten_subjects}, {@code encryption_keys}) lives only in the
 * opt-in {@code db/crypto-migration} Flyway location, which the default schema auto-init never
 * applied and {@code SchemaValidator} never checked. A standard auto-configured Postgres-crypto app
 * therefore booted clean, then threw on the first {@code @Encrypted} operation and first GDPR
 * forget. These tests pin that the event-store factory now (1) auto-provisions the crypto schema
 * through its own init path when the engine needs it, so durable erasure works out of the box, and
 * (2) fails fast at startup with an actionable message when the schema is self-managed and missing.
 */
@org.testcontainers.junit.jupiter.Testcontainers
class PostgresCryptoSchemaProvisioningIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static final EventTypeRegistry EMPTY_REGISTRY =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          return Object.class;
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          return Object.class;
        }
      };

  private static PGSimpleDataSource dataSource() {
    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl());
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    return ds;
  }

  @Test
  void initializeSchema_autoProvisionsCryptoSchema_soErasureWorksOutOfTheBox() {
    var ds = dataSource();
    CryptoEngine engine = PostgresCryptoEngine.builder().dataSource(ds).build();

    // Only StreamRune's own init path — no hand-created crypto tables, no manually-added Flyway
    // location. Because the engine reports required crypto tables, initializeSchema() applies
    // db/crypto-migration on its own history table.
    new PostgresEventStoreFactory(ds, EMPTY_REGISTRY).cryptoEngine(engine).initializeSchema();

    var subject = SubjectId.of("erase-me@example.com");
    byte[] ciphertext = engine.encrypt(subject, "secret-pii".getBytes());
    assertTrue(ciphertext.length > 0, "encrypt must work — encryption_keys was provisioned");

    // Durable, terminal erasure works end to end: deleteKey tombstones into forgotten_subjects and
    // a
    // later encrypt is refused.
    engine.deleteKey(subject);
    assertThrows(
        SubjectForgottenException.class, () -> engine.encrypt(subject, "resurrected".getBytes()));
  }

  @Test
  void initializeSchema_whenCryptoTablesAlreadyExist_appliesTheBaselineWithoutBricking()
      throws Exception {
    // The crypto tables already exist (provisioned out-of-band — e.g. the
    // README's copy-the-DDL route) but flyway_schema_history_crypto does not. A plain CREATE TABLE
    // baseline would fail with "relation already exists" and the app would never boot (the
    // KMS/Vault partial case also leaving a FAILED history row). The crypto baseline creates each
    // table only when absent and the factory always applies it from baseline 0, so this boot
    // records the baseline and leaves the existing tables as they are. Use an isolated schema so
    // this test never collides with the greenfield test's provisioning of `public`.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS crypto_brownfield");
    }
    var ds = dataSource();
    ds.setCurrentSchema("crypto_brownfield");
    CryptoEngine engine = PostgresCryptoEngine.builder().dataSource(ds).build();

    // First boot: greenfield — provisions the event + crypto schema and BOTH Flyway history tables.
    new PostgresEventStoreFactory(ds, EMPTY_REGISTRY).cryptoEngine(engine).initializeSchema();

    // Drop ONLY the crypto history table: the crypto tables now exist with no crypto history — the
    // state of a schema whose crypto tables were provisioned outside the framework.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("DROP TABLE crypto_brownfield.flyway_schema_history_crypto");
    }

    // The adopting boot must not throw or leave a FAILED history row.
    assertDoesNotThrow(
        () ->
            new PostgresEventStoreFactory(ds, EMPTY_REGISTRY)
                .cryptoEngine(engine)
                .initializeSchema(),
        "applying the crypto baseline over existing crypto tables must not fail");

    // No FAILED crypto migration row was left behind (the half-provisioned brick this fix
    // prevents).
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement();
        var rs =
            st.executeQuery(
                "SELECT COUNT(*) FROM crypto_brownfield.flyway_schema_history_crypto"
                    + " WHERE success = false")) {
      assertTrue(rs.next());
      assertEquals(0, rs.getInt(1), "no failed crypto migration row may remain after upgrade");
    }

    // Always baseline 0, never past a migration: the baseline itself is APPLIED (its CREATEs
    // no-op on the existing tables), so no later crypto migration can ever be skipped.
    assertEquals(
        java.util.List.of("BASELINE:0:true", "SQL:001:true"),
        cryptoHistory("crypto_brownfield"),
        "the crypto history must record baseline 0 and the applied baseline migration");

    // Durable, terminal erasure still works end to end after the upgrade.
    var subject = SubjectId.of("brownfield@example.com");
    engine.encrypt(subject, "pii".getBytes());
    engine.deleteKey(subject);
    assertThrows(
        SubjectForgottenException.class, () -> engine.encrypt(subject, "resurrected".getBytes()));
  }

  /** Vault/KMS topology: a durable JDBC tombstone store, no key table (VaultCryptoEngine shape). */
  private static CryptoEngine tombstoneOnlyEngine() {
    return new CryptoEngine() {
      @Override
      public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
        throw new UnsupportedOperationException("schema-provisioning test double");
      }

      @Override
      public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
        throw new UnsupportedOperationException("schema-provisioning test double");
      }

      @Override
      public void deleteKey(SubjectId subjectId) {
        throw new UnsupportedOperationException("schema-provisioning test double");
      }

      @Override
      public boolean isKeyAvailable(SubjectId subjectId) {
        return false;
      }

      @Override
      public Set<String> requiredCryptoTables() {
        // Exactly what VaultCryptoEngine / AwsKmsCryptoEngine report when wired with the
        // auto-configured JdbcForgottenSubjectStore.
        return Set.of("forgotten_subjects");
      }
    };
  }

  @Test
  void initializeSchema_withAPartiallyPreProvisionedCryptoSchema_createsTheRestAndKeepsItsRows()
      throws Exception {
    // A tombstone-only (Vault/KMS) deployment whose forgotten_subjects was created outside the
    // framework before its first boot. The crypto baseline must create only what is missing and
    // leave the existing table and its rows untouched.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS crypto_partial");
    }
    var ds = dataSource();
    ds.setCurrentSchema("crypto_partial");
    new PostgresEventStoreFactory(ds, EMPTY_REGISTRY).initializeSchema(); // event store only
    String tombstone = sha256Hex("pre-provisioned-tombstone@example.com");
    try (var conn = ds.getConnection();
        var st = conn.createStatement()) {
      st.execute(
          "CREATE TABLE forgotten_subjects (subject_id VARCHAR(255) PRIMARY KEY, forgotten_at"
              + " TIMESTAMPTZ NOT NULL DEFAULT NOW(), key_created_at TIMESTAMPTZ)");
      st.execute("INSERT INTO forgotten_subjects (subject_id) VALUES ('" + tombstone + "')");
    }

    assertDoesNotThrow(
        () ->
            new PostgresEventStoreFactory(ds, EMPTY_REGISTRY)
                .cryptoEngine(tombstoneOnlyEngine())
                .initializeSchema());

    assertEquals(
        "1",
        scalar(
            ds,
            "SELECT COUNT(*)::text FROM forgotten_subjects WHERE subject_id = '" + tombstone + "'"),
        "the pre-provisioned tombstone must survive");
    assertEquals("0", scalar(ds, "SELECT COUNT(*)::text FROM erased_key_generations"));
    assertEquals("0", scalar(ds, "SELECT COUNT(*)::text FROM encryption_keys"));
    assertEquals(
        java.util.List.of("BASELINE:0:true", "SQL:001:true"), cryptoHistory("crypto_partial"));
    assertTrue(
        SchemaValidator.validateCryptoTables(ds, Set.of("forgotten_subjects")).isValid(),
        "the adopted schema must validate");
  }

  @Test
  void initializeSchema_withAPreCreatedCustomKeyTable_stillCreatesTheFixedNameCryptoTables()
      throws Exception {
    // Builder#tableName tells an operator using a custom key-table name to create that table
    // themselves. Doing so BEFORE the first boot must not stop the framework from creating the
    // fixed-name tables the engine also needs (forgotten_subjects, erased_key_generations) — the
    // boot must validate and durable erasure must work end to end.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS crypto_custom_first");
      st.execute(
          "CREATE TABLE crypto_custom_first.tenant_keys (subject_id VARCHAR(255) PRIMARY KEY,"
              + " key_bytes BYTEA NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
              + " key_version SMALLINT NOT NULL)");
    }
    var ds = dataSource();
    ds.setCurrentSchema("crypto_custom_first");
    var engine = PostgresCryptoEngine.builder().dataSource(ds).tableName("tenant_keys").build();

    var factory = new PostgresEventStoreFactory(ds, EMPTY_REGISTRY).cryptoEngine(engine);
    assertDoesNotThrow(
        factory::create,
        "a pre-created custom key table must not suppress the fixed-name crypto tables");

    var subject = SubjectId.of("custom-key-table@example.com");
    engine.encrypt(subject, "pii".getBytes());
    assertEquals("1", scalar(ds, "SELECT COUNT(*)::text FROM tenant_keys"));
    engine.deleteKey(subject);
    assertThrows(
        SubjectForgottenException.class, () -> engine.encrypt(subject, "resurrected".getBytes()));
  }

  @Test
  void aMixedCaseCustomKeyTableName_validatesAtBootAndEncrypts() throws Exception {
    // PostgreSQL folds an unquoted identifier to lower case, in the operator's DDL and in the
    // engine's SQL alike, so table-name=CryptoKeys names the table cryptokeys. SchemaValidator
    // looked the reported name up through DatabaseMetaData.getTables, which matches
    // case-sensitively, so the boot failed with a false "missing crypto table" error.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS crypto_mixed_case");
      st.execute(
          "CREATE TABLE crypto_mixed_case.CryptoKeys (subject_id VARCHAR(255) PRIMARY KEY,"
              + " key_bytes BYTEA NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
              + " key_version SMALLINT NOT NULL)");
    }
    var ds = dataSource();
    ds.setCurrentSchema("crypto_mixed_case");
    var engine = PostgresCryptoEngine.builder().dataSource(ds).tableName("CryptoKeys").build();

    var factory = new PostgresEventStoreFactory(ds, EMPTY_REGISTRY).cryptoEngine(engine);
    assertDoesNotThrow(factory::create, "a mixed-case key-table name must validate at boot");

    var subject = SubjectId.of("mixed-case-key-table@example.com");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes());
    assertArrayEquals("pii".getBytes(), engine.decrypt(subject, ciphertext));
    assertEquals("1", scalar(ds, "SELECT COUNT(*)::text FROM cryptokeys"));
  }

  @Test
  void create_warnsOnceAboutASerializerOverrideOverAnEncryptedComponent() {
    // The factory runs the @Encrypted check before it touches the database and then builds the
    // store without repeating it, so the warning about a serializer override that would bypass
    // encryption is logged once per store, not once per check.
    var ds = dataSource();
    CryptoEngine engine = PostgresCryptoEngine.builder().dataSource(ds).build();
    EventTypeRegistry registry =
        org.streamrune.core.SimpleEventTypeRegistry.builder()
            .registerEvent(PostgresEventStoreCryptoValidationTest.OrderPlacedWithOverride.class)
            .build();
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger("org.streamrune.crypto.CryptoConfigValidator");
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      var factory = new PostgresEventStoreFactory(ds, registry).cryptoEngine(engine);
      assertDoesNotThrow(factory::create);
    } finally {
      logger.detachAppender(appender);
      appender.stop();
    }

    var warnings =
        appender.list.stream()
            .filter(e -> e.getLevel().isGreaterOrEqual(ch.qos.logback.classic.Level.WARN))
            .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
            .toList();
    assertEquals(1, warnings.size(), warnings.toString());
    assertTrue(warnings.getFirst().contains("OrderPlacedWithOverride"), warnings.toString());
  }

  /** {@code type:version:success} of every crypto history row, in installed order. */
  private static java.util.List<String> cryptoHistory(String schema) throws Exception {
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement();
        var rs =
            st.executeQuery(
                "SELECT type || ':' || version || ':' || success FROM "
                    + schema
                    + ".flyway_schema_history_crypto ORDER BY installed_rank")) {
      var rows = new java.util.ArrayList<String>();
      while (rs.next()) {
        rows.add(rs.getString(1));
      }
      return rows;
    }
  }

  private static String scalar(PGSimpleDataSource ds, String sql) throws Exception {
    try (var conn = ds.getConnection();
        var st = conn.createStatement();
        var rs = st.executeQuery(sql)) {
      assertTrue(rs.next(), "expected a row for: " + sql);
      return rs.getString(1);
    }
  }

  private static String sha256Hex(String value) throws Exception {
    byte[] digest =
        java.security.MessageDigest.getInstance("SHA-256")
            .digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    return java.util.HexFormat.of().formatHex(digest);
  }

  @Test
  void validateCryptoTables_flagsRuntimeRequiredColumns_notJustTablePresence() throws Exception {
    // A self-managed schema provisioned from incomplete DDL
    // has all three crypto tables but encryption_keys lacks the key_version column — every
    // key SELECT/INSERT the engine issues references it, so the deployment booted green and then
    // failed 100% of encrypt/decrypt/deleteKey at runtime ('column "key_version" does not
    // exist'). Same for an operator who created erased_key_generations from the boot error's
    // minimal literal reading (bare subject_id, no max_erased_version). The validator must pin
    // runtime-required columns exactly like the event-store validator does
    // (ERROR, "Missing column: <col>"), with a remediation that can actually fix it.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS crypto_val_cols");
      st.execute(
          "CREATE TABLE crypto_val_cols.encryption_keys ("
              + "subject_id VARCHAR(255) PRIMARY KEY,"
              + "key_bytes BYTEA NOT NULL,"
              + "created_at TIMESTAMPTZ NOT NULL DEFAULT NOW())"); // no key_version
      st.execute(
          "CREATE TABLE crypto_val_cols.forgotten_subjects ("
              + "subject_id VARCHAR(255) PRIMARY KEY)"); // no forgotten_at / key_created_at
      st.execute(
          "CREATE TABLE crypto_val_cols.erased_key_generations ("
              + "subject_id VARCHAR(255) PRIMARY KEY)"); // operator's minimal literal reading
    }
    var scoped = dataSource();
    scoped.setCurrentSchema("crypto_val_cols");

    var result =
        SchemaValidator.validateCryptoTables(
            scoped, Set.of("encryption_keys", "forgotten_subjects", "erased_key_generations"));

    assertFalse(
        result.isValid(),
        "present-but-column-incomplete crypto tables must fail validation, not boot green and"
            + " fail at first use");
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("encryption_keys")
                        && i.message().contains("Missing column: key_version")),
        () -> "must flag encryption_keys.key_version: " + result.issues());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("erased_key_generations")
                        && i.message().contains("Missing column: max_erased_version")),
        () -> "must flag erased_key_generations.max_erased_version: " + result.issues());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("forgotten_subjects")
                        && i.message().contains("Missing column: forgotten_at")),
        () ->
            "must flag forgotten_subjects.forgotten_at (read by the Vault reinstate identity"
                + " probe via JdbcForgottenSubjectStore.forgottenAt): "
                + result.issues());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.severity() == SchemaIssue.Severity.ERROR
                        && i.table().equals("forgotten_subjects")
                        && i.message().contains("Missing column: key_created_at")),
        () ->
            "must flag forgotten_subjects.key_created_at — every Vault erasure NAMES it in"
                + " its tombstone INSERT, so a schema missing it fails the GDPR erasure itself,"
                + " not merely a later revival: "
                + result.issues());
    // The series creates each crypto table only when absent and never alters one, so re-applying
    // it cannot add a column: the remedy is the exact ALTER, never "apply that series".
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.table().equals("encryption_keys")
                        && i.message()
                            .contains(
                                "ALTER TABLE encryption_keys ADD COLUMN key_version SMALLINT NOT"
                                    + " NULL;")),
        () -> "a missing column must come with its exact ALTER: " + result.issues());
    assertTrue(
        result.issues().stream()
            .filter(i -> i.message().contains("Missing column"))
            .allMatch(
                i ->
                    i.message().contains("ALTER TABLE " + i.table() + " ADD COLUMN ")
                        && !i.message().contains("Apply that series")),
        () -> "every column error must carry an ALTER, never a re-apply loop: " + result.issues());
    // forgotten_subjects is SHARED, and the
    // requirement is keyed by table name, not by engine — so an operator running the postgres or
    // aws-kms engine is boot-blocked over columns THAT engine never touches. Failing closed is
    // deliberate (a shared table cannot be validated per engine, and a partial one fails the Vault
    // engine's erasure itself), but the remediation must SAY so, or the operator reads the error
    // as wrong and works around it.
    assertTrue(
        result.issues().stream()
            .filter(i -> i.table().equals("forgotten_subjects"))
            .allMatch(i -> i.message().contains("shared by every crypto backend")),
        () ->
            "the forgotten_subjects remediation must explain that the requirement is table-scoped,"
                + " not engine-scoped: "
                + result.issues());
  }

  @Test
  void validateCryptoTables_checksKeyColumnsOnACustomNamedKeyTable() throws Exception {
    // Reported the CONFIGURED key-table name so a missing custom table fails at
    // boot; the column check must follow it. The postgres engine is the only engine that reports
    // erased_key_generations, and its required set is exactly {keyTable, forgotten_subjects,
    // erased_key_generations} — so in a set containing erased_key_generations, any name outside
    // the two fixed tables IS the key table and must carry the key columns.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS crypto_val_custom");
      st.execute(
          "CREATE TABLE crypto_val_custom.tenant_keys ("
              + "subject_id VARCHAR(255) PRIMARY KEY,"
              + "key_bytes BYTEA NOT NULL)"); // no key_version
      st.execute(
          "CREATE TABLE crypto_val_custom.forgotten_subjects (subject_id VARCHAR(255) PRIMARY"
              + " KEY, forgotten_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
              + " key_created_at TIMESTAMPTZ)");
      st.execute(
          "CREATE TABLE crypto_val_custom.erased_key_generations (subject_id VARCHAR(255)"
              + " PRIMARY KEY, max_erased_version SMALLINT NOT NULL)");
    }
    var scoped = dataSource();
    scoped.setCurrentSchema("crypto_val_custom");

    var result =
        SchemaValidator.validateCryptoTables(
            scoped, Set.of("tenant_keys", "forgotten_subjects", "erased_key_generations"));

    assertFalse(result.isValid());
    assertTrue(
        result.issues().stream()
            .anyMatch(
                i ->
                    i.table().equals("tenant_keys")
                        && i.message().contains("Missing column: key_version")),
        () -> "the custom-named key table must be column-checked too: " + result.issues());

    // And the remediation must actually work. The crypto series
    // never creates or alters a custom-named key table, so telling this operator to "apply that
    // series" sends them round a loop that can never fix the schema — the boot abort persists,
    // with no correct guidance anywhere (the README's copy-the-DDL block hardcodes
    // encryption_keys). Before
    // the column check existed this configuration at least failed at first use with a message that
    // named the real object; making the failure earlier must not make the guidance wrong.
    String message =
        result.issues().stream()
            .filter(i -> i.table().equals("tenant_keys"))
            .map(SchemaIssue::message)
            .findFirst()
            .orElseThrow();
    assertTrue(
        message.contains("ALTER TABLE tenant_keys ADD COLUMN key_version"),
        () ->
            "a custom key-table name must be told to add the column itself, naming the configured"
                + " table: "
                + message);
    assertFalse(
        message.contains("Apply that series"),
        () ->
            "the crypto series cannot alter a custom-named key table, so it must not be offered as"
                + " the remediation: "
                + message);
  }

  @Test
  void validateCryptoTables_presenceOnlyForAnExtraTableAlongsideTheKeyTable() throws Exception {
    // The key-table inference reads "in a set containing
    // erased_key_generations, the remaining name IS the key table" — but it applied
    // CRYPTO_KEY_TABLE_COLUMNS to EVERY name outside the two fixed ones, so a set with a FOURTH
    // table (a decorating CryptoEngine adding its own — CachedCryptoEngine passes the delegate's
    // set straight through, so a decorator can extend it) produced three bogus
    // "Missing column: subject_id/key_bytes/key_version" ERRORs against that table and aborted the
    // boot on a schema that is actually complete. The inference is only sound when the difference
    // is exactly one name.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS crypto_val_extra");
      st.execute(
          "CREATE TABLE crypto_val_extra.encryption_keys (subject_id VARCHAR(255) PRIMARY KEY,"
              + " key_bytes BYTEA NOT NULL, key_version SMALLINT NOT NULL)");
      st.execute(
          "CREATE TABLE crypto_val_extra.forgotten_subjects (subject_id VARCHAR(255) PRIMARY"
              + " KEY, forgotten_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
              + " key_created_at TIMESTAMPTZ)");
      st.execute(
          "CREATE TABLE crypto_val_extra.erased_key_generations (subject_id VARCHAR(255) PRIMARY"
              + " KEY, max_erased_version SMALLINT NOT NULL)");
      // The decorator's own table — complete for its own purposes, and nothing to do with keys.
      st.execute("CREATE TABLE crypto_val_extra.my_crypto_audit (audit_id BIGSERIAL PRIMARY KEY)");
    }
    var scoped = dataSource();
    scoped.setCurrentSchema("crypto_val_extra");

    var result =
        SchemaValidator.validateCryptoTables(
            scoped,
            Set.of(
                "encryption_keys",
                "forgotten_subjects",
                "erased_key_generations",
                "my_crypto_audit"));

    assertTrue(
        result.isValid(),
        () ->
            "a complete crypto schema plus one extra engine-reported table must validate — the"
                + " key-table inference needs exactly one unknown name to be sound: "
                + result.issues());
  }

  @Test
  void validateCryptoTables_presenceOnlyForUnknownTablesOfThirdPartyEngines() throws Exception {
    // A third-party CryptoEngine may report tables this validator knows nothing about. Without
    // the erased_key_generations signature there is no basis to assume key-table columns — an
    // unknown table validates by presence only, never with bogus column errors.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS crypto_val_thirdparty");
      st.execute(
          "CREATE TABLE crypto_val_thirdparty.custom_hsm_slots (slot_id VARCHAR(64) PRIMARY"
              + " KEY)");
    }
    var scoped = dataSource();
    scoped.setCurrentSchema("crypto_val_thirdparty");

    var result = SchemaValidator.validateCryptoTables(scoped, Set.of("custom_hsm_slots"));

    assertTrue(
        result.isValid(),
        () -> "an unknown third-party table must validate by presence only: " + result.issues());
  }

  @Test
  void validateCryptoTables_failsFast_whenCryptoSchemaMissing() throws Exception {
    // Own empty schema, isolated from the provisioning test (which provisions into public), so this
    // asserts the missing-table detection regardless of test order.
    try (var conn = dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE SCHEMA IF NOT EXISTS crypto_val_missing");
    }
    var scoped = dataSource();
    scoped.setCurrentSchema("crypto_val_missing");

    var result =
        SchemaValidator.validateCryptoTables(
            scoped, Set.of("forgotten_subjects", "encryption_keys"));

    assertFalse(result.isValid(), "missing crypto tables must be reported as an ERROR");
    boolean actionable =
        result.issues().stream()
            .anyMatch(i -> i.message().contains("classpath:db/crypto-migration"));
    assertTrue(actionable, "the failure must name the crypto Flyway migration to apply");

    // Naming only the LOCATION steered operators into appending it to their
    // main flyway.locations — a single Flyway configuration then resolves two migrations for
    // version 1 (both series start at V001) and aborts with "Found more than one migration with
    // version 1". The remediation must therefore name the SEPARATE history table this location is
    // applied on, exactly as PostgresEventStoreFactory.initializeSchema does.
    boolean namesSeparateHistory =
        result.issues().stream()
            .anyMatch(i -> i.message().contains("flyway_schema_history_crypto"));
    assertTrue(
        namesSeparateHistory,
        "the failure must name the separate crypto history table (flyway_schema_history_crypto),"
            + " not just the location — a location-only message steers operators into a"
            + " duplicate-version Flyway boot failure");
  }
}
