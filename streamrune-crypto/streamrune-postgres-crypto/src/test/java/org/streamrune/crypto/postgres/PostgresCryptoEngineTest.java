package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

/** Unit tests for PostgresCryptoEngine. */
class PostgresCryptoEngineTest {

  private FakeDataSource dataSource;
  private PostgresCryptoEngine engine;

  @BeforeEach
  void setUp() {
    dataSource = new FakeDataSource();
    engine = PostgresCryptoEngine.builder().dataSource(dataSource).build();
  }

  // --- Packaging (regression guard) ---

  @Test
  void engineStaysInCryptoOwnedPackage_notTheEventStoreSplitPackage() {
    // The event-store artifact (streamrune-postgres) and this crypto artifact
    // (streamrune-postgres-crypto) must NOT share a Java package: on the JPMS module path
    // two automatic modules exporting org.streamrune.postgres fail to assemble a boot layer
    // (LayerInstantiationException — split package).
    // A ModuleLayer-assembly CI check over the published jars is the proper long-term guard
    // (TODO: add once artifacts are assembled in CI); this cheap assertion pins the engine so
    // it can never regress back into the shared org.streamrune.postgres package.
    assertEquals("org.streamrune.crypto.postgres", PostgresCryptoEngine.class.getPackageName());
  }

  // --- Validation ---

  @Test
  void builderRejectsNullDataSource() {
    assertThrows(IllegalStateException.class, () -> PostgresCryptoEngine.builder().build());
  }

  @Test
  void encryptRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.encrypt(null, new byte[0]));
  }

  @Test
  void encryptRejectsNullPlaintext() {
    assertThrows(
        IllegalArgumentException.class, () -> engine.encrypt(SubjectId.of("subject"), null));
  }

  @Test
  void decryptRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(null, new byte[20]));
  }

  @Test
  void decryptRejectsNullCiphertext() {
    assertThrows(
        IllegalArgumentException.class, () -> engine.decrypt(SubjectId.of("subject"), null));
  }

  @Test
  void isKeyAvailableRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.isKeyAvailable(null));
  }

  @Test
  void deleteKeyRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.deleteKey(null));
  }

  @Test
  void blankSubjectIdIsRejectedBySubjectIdBeforeTheEngineSeesIt() {
    // The engine checks only for null: SubjectId's own constructor rejects an empty or blank
    // value, so a blank subject id cannot be constructed, let alone passed to encrypt, decrypt,
    // deleteKey, reinstate or isKeyAvailable.
    assertThrows(IllegalArgumentException.class, () -> SubjectId.of(""));
    assertThrows(IllegalArgumentException.class, () -> SubjectId.of("  "));
  }

  // --- Builder ---

  @Test
  void builderSetsDataSource() {
    var engine = PostgresCryptoEngine.builder().dataSource(dataSource).build();
    assertNotNull(engine);
  }

  @Test
  void builderRejectsMissingDataSource() {
    assertThrows(IllegalStateException.class, () -> PostgresCryptoEngine.builder().build());
  }

  // --- key-table name ---

  @Test
  void builderRejectsInvalidTableName() {
    // A configured table name is concatenated into SQL (an identifier cannot be a bind parameter),
    // so a non-identifier value would be a SQL-injection vector — reject it at build().
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PostgresCryptoEngine.builder()
                .dataSource(dataSource)
                .tableName("keys; DROP TABLE encryption_keys")
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () -> PostgresCryptoEngine.builder().dataSource(dataSource).tableName("bad-name").build());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PostgresCryptoEngine.builder().dataSource(dataSource).tableName("schema.keys").build());
  }

  @Test
  void requiredCryptoTables_listsEveryTableTheEngineTouches_soAMissingOneFailsAtBootNotAtErasure() {
    // Inventory-coupling guard. The crypto Flyway series and this set MUST move together: the
    // baseline creates erased_key_generations, which deleteKey() upserts into and every key mint
    // reads. A deployment carrying encryption_keys + forgotten_subjects but NOT
    // erased_key_generations has to fail LOUDLY at startup (PostgresEventStoreFactory.create() ->
    // SchemaValidator.validateCryptoTables -> SchemaValidationException) instead of silently at
    // the first GDPR erasure — and that fail-fast is driven entirely by this set. Pin it exactly,
    // so adding a table in a future crypto migration without reporting it here breaks here first.
    assertEquals(
        Set.of("encryption_keys", "forgotten_subjects", "erased_key_generations"),
        engine.requiredCryptoTables(),
        "every crypto table the engine reads or writes must be reported here, or a schema missing"
            + " it boots clean and only fails on the first erasure");
  }

  @Test
  void noCryptoMigrationCombinesADdlLockWithATableScanningDml() throws Exception {
    // Flyway runs each PostgreSQL migration in ONE transaction
    // and PostgreSQL holds every lock until commit, so a file that both ALTERs a live table and
    // runs an INSERT ... SELECT over another one holds ACCESS EXCLUSIVE on the ALTERed table for
    // the whole scan. On a GDPR-active deployment the tombstone table is routinely 10^5..10^7
    // rows, and encryption_keys is read or written by EVERY encrypt/decrypt/deleteKey — so every
    // live replica blocks, or fails outright where lock_timeout/statement_timeout is set, on
    // exactly the rolling upgrade docs/guide/production.md calls safe. Seeds belong in their own
    // migration, whose transaction takes only ROW EXCLUSIVE on its target.
    var offenders = new java.util.ArrayList<String>();
    for (java.nio.file.Path file : cryptoMigrationFiles()) {
      String sql = java.nio.file.Files.readString(file).toUpperCase(java.util.Locale.ROOT);
      // Strip comment lines: the prose in these files legitimately discusses both constructs.
      String code =
          sql.lines().filter(l -> !l.stripLeading().startsWith("--")).reduce("", (a, b) -> a + b);
      if (code.contains("ALTER TABLE") && code.contains("INSERT INTO")) {
        offenders.add(file.getFileName().toString());
      }
    }
    assertTrue(
        offenders.isEmpty(),
        () ->
            "a crypto migration must not hold a DDL lock across a table-scanning DML; split the"
                + " seed into its own migration: "
                + offenders);
  }

  private static java.util.List<java.nio.file.Path> cryptoMigrationFiles() throws Exception {
    var dir = PostgresCryptoEngineTest.class.getResource("/db/crypto-migration");
    assertNotNull(dir, "the crypto migration series must be on the test classpath");
    try (var files = java.nio.file.Files.list(java.nio.file.Path.of(dir.toURI()))) {
      var list = files.filter(p -> p.getFileName().toString().endsWith(".sql")).sorted().toList();
      assertFalse(list.isEmpty(), "the crypto migration series must not be empty");
      return list;
    }
  }

  @Test
  void readmeCopyTheDdlBlockEqualsTheShippedCryptoBaseline() throws Exception {
    // "Copy the DDL into your own migration series" is one of the provisioning routes this module
    // documents, and the one SchemaValidator's missing-table and missing-column errors point at.
    // That block has drifted from the shipped series before (a missing
    // key_version + erased_key_generations), so pin it to the shipped DDL itself: comments and
    // whitespace aside, the README block must be exactly the crypto migration series — every
    // table, column, type, default and constraint.
    String readme = java.nio.file.Files.readString(moduleFile("README.md"));
    int section = readme.indexOf("### Or copy the DDL into your own migration series");
    assertTrue(section > 0, "the copy-the-DDL section must exist — SchemaValidator points at it");
    int open = readme.indexOf("```sql\n", section);
    assertTrue(open > 0, "the copy-the-DDL section must carry a ```sql block");
    int close = readme.indexOf("```", open + "```sql\n".length());
    String block = readme.substring(open + "```sql\n".length(), close);

    var shipped = new StringBuilder();
    for (java.nio.file.Path file : cryptoMigrationFiles()) {
      // The baseline quiets PostgreSQL's "already exists, skipping" NOTICE for its own Flyway run;
      // that session setting is not DDL and is deliberately absent from the README block.
      java.nio.file.Files.readString(file)
          .lines()
          .filter(line -> !line.strip().toLowerCase(java.util.Locale.ROOT).startsWith("set local"))
          .forEach(line -> shipped.append(line).append('\n'));
    }
    assertEquals(
        normalizedSql(shipped.toString()),
        normalizedSql(block),
        "the README's copy-the-DDL block must equal the shipped crypto migration DDL");
  }

  /** SQL with {@code --} comments dropped, whitespace collapsed and case folded. */
  private static String normalizedSql(String sql) {
    return sql.lines()
        .map(line -> line.replaceAll("--.*$", ""))
        .collect(java.util.stream.Collectors.joining(" "))
        .replaceAll("\\s+", " ")
        .replaceAll(" ?([(),;]) ?", "$1")
        .trim()
        .toLowerCase(java.util.Locale.ROOT);
  }

  /** A file in this module's directory, resolved from the test's working directory. */
  private static java.nio.file.Path moduleFile(String name) {
    var path = java.nio.file.Path.of(name).toAbsolutePath();
    assertTrue(
        java.nio.file.Files.isRegularFile(path),
        () -> "expected " + name + " in the module directory, resolved to " + path);
    return path;
  }

  @Test
  void blankOrNullTableNameFallsBackToDefault() {
    assertTrue(
        PostgresCryptoEngine.builder()
            .dataSource(dataSource)
            .tableName("  ")
            .build()
            .requiredCryptoTables()
            .contains("encryption_keys"));
    assertTrue(
        PostgresCryptoEngine.builder()
            .dataSource(dataSource)
            .tableName(null)
            .build()
            .requiredCryptoTables()
            .contains("encryption_keys"));
  }

  @Test
  void configuredTableNameIsUsedInEngineSqlAndRequiredTables() {
    var custom =
        PostgresCryptoEngine.builder().dataSource(dataSource).tableName("secure_keys").build();

    // requiredCryptoTables() reports the CONFIGURED key table (it feeds SchemaValidator), not the
    // default — proving the property is actually consumed rather than silently ignored.
    assertTrue(custom.requiredCryptoTables().contains("secure_keys"));
    assertFalse(custom.requiredCryptoTables().contains("encryption_keys"));

    // The configured name flows into the real SQL: exercise every key-table statement.
    custom.encrypt(SubjectId.of("s"), "data".getBytes());
    custom.isKeyAvailable(SubjectId.of("s"));
    custom.deleteKey(SubjectId.of("s"));

    assertTrue(
        dataSource.sqlSeen().stream().anyMatch(s -> s.contains("secure_keys")),
        "key-table SQL must use the configured table name");
    assertTrue(
        dataSource.sqlSeen().stream().noneMatch(s -> s.contains("encryption_keys")),
        "no SQL may reference the default key table when a custom name is configured: "
            + dataSource.sqlSeen());
  }

  @Test
  void mixedCaseTableName_isFoldedToLowerCase_likePostgresFoldsTheUnquotedIdentifier() {
    // The engine's SQL does not quote the name, so PostgreSQL resolves CryptoKeys to the table
    // cryptokeys. SchemaValidator looks the REPORTED name up through DatabaseMetaData.getTables,
    // which matches case-sensitively, so reporting "CryptoKeys" failed the boot with a false
    // "missing crypto table". The engine now reports, and uses, the folded name.
    var custom =
        PostgresCryptoEngine.builder().dataSource(dataSource).tableName("CryptoKeys").build();

    assertEquals(
        Set.of("cryptokeys", "forgotten_subjects", "erased_key_generations"),
        custom.requiredCryptoTables());
    byte[] ciphertext = custom.encrypt(SubjectId.of("s"), "data".getBytes());
    assertArrayEquals("data".getBytes(), custom.decrypt(SubjectId.of("s"), ciphertext));
    assertTrue(
        dataSource.sqlSeen().stream().anyMatch(s -> s.contains(" cryptokeys ")),
        () -> "key-table SQL must use the folded name: " + dataSource.sqlSeen());
    assertTrue(
        dataSource.sqlSeen().stream().noneMatch(s -> s.contains("CryptoKeys")),
        () -> "no SQL may carry the unfolded name: " + dataSource.sqlSeen());
  }

  // --- encrypt ---

  @Test
  void encryptCreatesKeyOnFirstCall() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("new-subject"), "data".getBytes());

    assertNotNull(ciphertext);
    assertTrue(ciphertext.length > 14); // header(2) + IV(12) + GCM tag + plaintext
  }

  @Test
  void encryptStoresKeyOnFirstCall() {
    engine.encrypt(SubjectId.of("subject-1"), "plaintext".getBytes());

    assertTrue(dataSource.containsKey("subject-1"));
  }

  @Test
  void encryptDoesNotRegenerateKeyOnSubsequentCalls() {
    engine.encrypt(SubjectId.of("subject-2"), "data1".getBytes());
    byte[] key1 = dataSource.getKey("subject-2");

    engine.encrypt(SubjectId.of("subject-2"), "data2".getBytes());
    byte[] key2 = dataSource.getKey("subject-2");

    assertArrayEquals(key1, key2);
  }

  @Test
  void encryptPrependsVersionHeaderAndIvToCiphertext() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("subject"), "hello".getBytes());

    // header(2) + GCM_IV(12) + GCM_TAG(16) + plaintext(5) = 35
    assertEquals(35, ciphertext.length);
    assertEquals(1, ciphertext[0], "format version");
    assertEquals(1, ciphertext[1], "key version");
  }

  // --- decrypt ---

  @Test
  void decryptThrowsKeyNotFoundWhenKeyDoesNotExist() {
    assertThrows(
        KeyNotFoundException.class,
        () -> engine.decrypt(SubjectId.of("unknown-subject"), new byte[20]));
  }

  @Test
  void decryptReturnsDecryptedData() throws Exception {
    byte[] keyBytes = new byte[32];
    java.util.Arrays.fill(keyBytes, (byte) 0x42);
    dataSource.putKey("roundtrip-subject", keyBytes);

    byte[] plaintext = "hello world!".getBytes();
    byte[] iv = new byte[12];
    new java.security.SecureRandom().nextBytes(iv);

    byte[] ciphertext = encryptWithKey(keyBytes, iv, plaintext);

    byte[] result = engine.decrypt(SubjectId.of("roundtrip-subject"), ciphertext);
    assertEquals("hello world!", new String(result));
  }

  @Test
  void encryptThenDecryptRoundtripsThroughStoredKey() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("roundtrip-full"), "top secret".getBytes());

    assertArrayEquals(
        "top secret".getBytes(), engine.decrypt(SubjectId.of("roundtrip-full"), ciphertext));
  }

  @Test
  void preForgetCiphertextDegradesToKeyNotFoundAfterReinstateAndRemint() {
    // Erase -> reinstate -> first re-encrypt must leave
    // PRE-forget ciphertext reading as KeyNotFoundException ([REDACTED] via
    // CryptoShreddingModule), not a GCM tag failure wrapped in CryptoOperationException that
    // permanently blocks aggregate loads and projection rebuilds for the returning subject.
    var subject = SubjectId.of("returning-customer");
    byte[] preForget = engine.encrypt(subject, "old-pii".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] postReinstate = engine.encrypt(subject, "new-pii".getBytes());

    // The generation comes from the erasure itself: deleteKey recorded the destroyed generation 1
    // in erased_key_generations, so the re-mint is generation 2 without reinstate writing it.
    assertEquals(1, preForget[1], "the first mint is generation 1");
    assertEquals(2, postReinstate[1], "the re-mint is one above the destroyed generation");
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, preForget));
    assertArrayEquals("new-pii".getBytes(), engine.decrypt(subject, postReinstate));
  }

  @Test
  void forgetOfANeverEncryptedSubject_reinstateThenEncrypt_mintsGenerationOne() {
    // deleteKey records a destroyed generation only when it destroys a key row, and reinstate
    // records none. A subject forgotten before it ever encrypted has no ciphertext under any
    // generation, so its first key after reinstate is generation 1 — no erase/re-register cycle
    // is burned.
    var subject = SubjectId.of("erased-before-any-key");
    engine.deleteKey(subject); // tombstone, no key ever minted -> no recorded generation
    engine.reinstate(subject);
    byte[] postReinstate = engine.encrypt(subject, "new-pii".getBytes());

    assertEquals(1, postReinstate[1], "no key was destroyed, so the first mint is generation 1");
    assertArrayEquals("new-pii".getBytes(), engine.decrypt(subject, postReinstate));
  }

  @Test
  void everyPriorGenerationStaysRedactedAcrossRepeatedEraseReinstateCycles() {
    var subject = SubjectId.of("twice-returning");
    byte[] gen1 = engine.encrypt(subject, "gen1".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] gen2 = engine.encrypt(subject, "gen2".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] gen3 = engine.encrypt(subject, "gen3".getBytes());

    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, gen1));
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, gen2));
    assertArrayEquals("gen3".getBytes(), engine.decrypt(subject, gen3));
  }

  @Test
  void tamperedCurrentGenerationCiphertextStillFailsLoudAfterReinstate() {
    // Taxonomy preserved: only DESTROYED-generation blobs degrade; tampering with the
    // live generation keeps failing loudly, even for a reinstated subject.
    var subject = SubjectId.of("tamper-after-reinstate");
    engine.encrypt(subject, "old".getBytes());
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] current = engine.encrypt(subject, "new".getBytes());
    current[current.length - 1] ^= 0x01;

    // KeyNotFoundException is NOT a subtype of CryptoOperationException, so this assertThrows
    // by itself proves the tamper did not degrade to the replay-tolerant forget signal.
    var tampered =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.decrypt(subject, current));
    // An AEAD tag mismatch is DETERMINISTIC — the family classifiers must see the
    // CryptoMappingException subtype (poison), never a bare CryptoOperationException (outage,
    // retried forever).
    assertInstanceOf(org.streamrune.core.crypto.CryptoMappingException.class, tampered);
  }

  @Test
  void versionByteBelowOneNeverRedacts_failsLoud() {
    // Destroyed-generation tolerance is bounded to [1, currentKeyVersion): byte 0 / negative is
    // not a generation this engine ever minted — loud corruption error, never silent redaction.
    var subject = SubjectId.of("rollback-guard");
    byte[] ciphertext = engine.encrypt(subject, "data".getBytes());
    ciphertext[1] = 0;
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.decrypt(subject, ciphertext));
  }

  @Test
  void decryptRejectsUnknownFormatVersion() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("format-subject"), "data".getBytes());
    ciphertext[0] = 9; // a future/foreign format

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("format-subject"), ciphertext));
    assertTrue(thrown.getMessage().contains("format version"), thrown.getMessage());
    assertInstanceOf(
        org.streamrune.core.crypto.CryptoMappingException.class,
        thrown,
        "A blob-property rejection is deterministic — never bare (retried forever)");
  }

  @Test
  void decryptRejectsUnknownKeyVersion() {
    byte[] ciphertext = engine.encrypt(SubjectId.of("keyver-subject"), "data".getBytes());
    ciphertext[1] = 7; // no such key version exists

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("keyver-subject"), ciphertext));
    assertTrue(thrown.getMessage().contains("key version"), thrown.getMessage());
    assertInstanceOf(org.streamrune.core.crypto.CryptoMappingException.class, thrown);
  }

  @Test
  void decryptRejectsTruncatedCiphertext() {
    engine.encrypt(SubjectId.of("short-subject"), "data".getBytes());

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("short-subject"), new byte[10]));
    assertTrue(thrown.getMessage().contains("too short"), thrown.getMessage());
    assertInstanceOf(org.streamrune.core.crypto.CryptoMappingException.class, thrown);
  }

  @Test
  void decryptOfATooShortBlobCarryingAnErasedGenerationByteStillReadsAsKeyNotFound() {
    // The erased-generation check must win
    // over the too-short rejection — a blob this short can never decrypt regardless of
    // generation, but a generation the current key has already outlived is still the designed
    // [REDACTED] signal (GDPR erasure), not a length-corruption verdict. This engine's header is
    // 2 bytes (FORMAT_VERSION, generation), unlike InMemoryCryptoEngine's 1-byte header — the
    // synthetic blob below carries both. The erasure destroys a real generation-1 key.
    var subject = SubjectId.of("short-and-erased");
    engine.encrypt(subject, "old-pii".getBytes()); // mints generation 1
    engine.deleteKey(subject); // destroys generation 1 and records it
    engine.reinstate(subject);
    engine.encrypt(subject, "new-pii".getBytes()); // mints generation 2

    assertThrows(
        KeyNotFoundException.class,
        // format=1 (valid), generation=1 (< current generation 2); far below the IV+tag minimum.
        () -> engine.decrypt(subject, new byte[] {1, 1}),
        "a short blob carrying a destroyed generation must read as [REDACTED], never \"too"
            + " short\"");
  }

  @Test
  void decryptOfABlobBelowHeaderLengthStillReadsAsTooShort() {
    // Boundary for the reorder above: a blob shorter than the 2-byte header has no
    // generation byte safely readable at all, so the guard must fall through to "too short"
    // rather than an ArrayIndexOutOfBoundsException.
    engine.encrypt(SubjectId.of("sub-header"), "data".getBytes());

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("sub-header"), new byte[] {1}));
    assertTrue(thrown.getMessage().contains("too short"), thrown.getMessage());
    assertInstanceOf(org.streamrune.core.crypto.CryptoMappingException.class, thrown);
  }

  // --- isKeyAvailable ---

  @Test
  void isKeyAvailableReturnsTrueWhenKeyExists() {
    dataSource.putKey("exists", new byte[32]);

    assertTrue(engine.isKeyAvailable(SubjectId.of("exists")));
  }

  @Test
  void isKeyAvailableReturnsFalseWhenKeyMissing() {
    assertFalse(engine.isKeyAvailable(SubjectId.of("missing")));
  }

  @Test
  void reinstateRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.reinstate(null));
  }

  // --- deleteKey ---

  @Test
  void deleteKeyRemovesKey() {
    dataSource.putKey("to-delete", new byte[32]);
    assertTrue(engine.isKeyAvailable(SubjectId.of("to-delete")));

    engine.deleteKey(SubjectId.of("to-delete"));

    assertFalse(engine.isKeyAvailable(SubjectId.of("to-delete")));
  }

  // --- subject-id hashing (PII must never land in a stored column) ---

  @Test
  void storesHashedSubjectIdNotRawPiiValue() {
    String rawPii = "alice@example.com";
    engine.encrypt(SubjectId.of(rawPii), "pii".getBytes());

    // The encryption_keys row must be keyed by the SHA-256 hash, never the raw PII value.
    assertFalse(
        dataSource.storesColumnLiteral(rawPii),
        "raw PII subject id must never appear as a stored subject_id column value");
    assertTrue(
        dataSource.storesColumnLiteral(sha256Hex(rawPii)),
        "the stored subject_id column must be the SHA-256 hash of the subject id");

    // The permanent tombstone must store the hash too, and a round-trip still works end to end.
    engine.deleteKey(SubjectId.of(rawPii));
    assertTrue(dataSource.isForgotten(rawPii), "tombstone recorded (looked up by hash)");
    assertFalse(
        dataSource.storesColumnLiteral(rawPii),
        "the forgotten_subjects tombstone must store the hash, not raw PII");
    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of(rawPii), "again".getBytes()));
  }

  @Test
  void hashedRoundTripEncryptDecryptSucceeds() {
    var subject = SubjectId.of("bob@example.com");
    byte[] ciphertext = engine.encrypt(subject, "top secret".getBytes());
    assertArrayEquals("top secret".getBytes(), engine.decrypt(subject, ciphertext));
  }

  // --- terminal erasure (crypto-shredding tombstone) ---

  @Test
  void deleteKeyRecordsTombstone() {
    engine.encrypt(SubjectId.of("forget-1"), "secret".getBytes());

    engine.deleteKey(SubjectId.of("forget-1"));

    assertTrue(dataSource.isForgotten("forget-1"));
  }

  @Test
  void encryptAfterDeleteThrowsSubjectForgotten() {
    engine.encrypt(SubjectId.of("forget-2"), "secret".getBytes());
    engine.deleteKey(SubjectId.of("forget-2"));

    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("forget-2"), "new PII".getBytes()));
  }

  @Test
  void encryptAfterDeleteDoesNotRecreateKey() {
    engine.encrypt(SubjectId.of("forget-3"), "secret".getBytes());
    engine.deleteKey(SubjectId.of("forget-3"));

    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("forget-3"), "x".getBytes()));
    // The tombstone must block key re-creation — no orphan key row reappears.
    assertFalse(dataSource.containsKey("forget-3"));
  }

  @Test
  void reinstateClearsTombstoneAndAllowsRecreation() {
    engine.encrypt(SubjectId.of("comeback"), "old".getBytes());
    engine.deleteKey(SubjectId.of("comeback"));
    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("comeback"), "x".getBytes()));

    engine.reinstate(SubjectId.of("comeback"));

    assertFalse(dataSource.isForgotten("comeback"));
    byte[] ciphertext = engine.encrypt(SubjectId.of("comeback"), "new".getBytes());
    assertArrayEquals("new".getBytes(), engine.decrypt(SubjectId.of("comeback"), ciphertext));
  }

  // --- concurrent key creation ---

  @Test
  void encryptDoesNotOverwriteKeyCreatedByConcurrentWriter() {
    byte[] winnerKey = new byte[32];
    java.util.Arrays.fill(winnerKey, (byte) 0x07);
    // Simulate a concurrent writer that inserts the subject's key between this
    // engine's SELECT (no key found) and its INSERT.
    dataSource.beforeInsert(() -> dataSource.putKey("race-subject", winnerKey));

    byte[] ciphertext = engine.encrypt(SubjectId.of("race-subject"), "secret".getBytes());

    // First writer wins: the stored key must be the concurrent writer's key...
    assertArrayEquals(winnerKey, dataSource.getKey("race-subject"));
    // ...and the returned ciphertext must have been produced with that winning key.
    assertArrayEquals(
        "secret".getBytes(), engine.decrypt(SubjectId.of("race-subject"), ciphertext));
  }

  // --- crash points: an Error inside a transaction commits nothing ---
  //
  // Each engine write is one transaction. The rollback used to sit only in catch (Exception), so an
  // Error (OutOfMemoryError, LinkageError) skipped it, and the finally's setAutoCommit(true) then
  // COMMITTED the open transaction (pgjdbc). The fake models that commit, so each test below failed
  // against the old code. A process crash at any of these points is unchanged: PostgreSQL discards
  // the uncommitted transaction.

  /** Stands in for an OutOfMemoryError or a LinkageError thrown inside the transaction. */
  private static final class InjectedCrash extends Error {
    InjectedCrash(String where) {
      super("injected crash " + where);
    }
  }

  @Test
  void errorAtTheTombstoneInsert_commitsNothing_andARerunConverges() {
    // Erasure: the old code committed "key deleted, generation recorded, no tombstone", and the
    // next
    // encrypt minted a fresh key for a subject the caller had asked to forget.
    var subject = SubjectId.of("cp-f1");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes());
    byte[] keyBefore = dataSource.getKey("cp-f1");
    dataSource.failBefore(
        "INSERT INTO forgotten_subjects", new InjectedCrash("at the tombstone INSERT"));

    assertThrows(InjectedCrash.class, () -> engine.deleteKey(subject));

    assertErasureCommittedNothing(subject, keyBefore, ciphertext);
    engine.deleteKey(subject);
    assertErasureConverged(subject);
  }

  @Test
  void errorAfterRecordingTheErasedGeneration_commitsNothing_andARerunConverges() {
    // D1, the earlier point: between recordErasedGeneration and the key DELETE.
    var subject = SubjectId.of("cp-f2");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes());
    byte[] keyBefore = dataSource.getKey("cp-f2");
    dataSource.failAfter(
        "INSERT INTO erased_key_generations",
        new InjectedCrash("after recording the erased generation"));

    assertThrows(InjectedCrash.class, () -> engine.deleteKey(subject));

    assertErasureCommittedNothing(subject, keyBefore, ciphertext);
    engine.deleteKey(subject);
    assertErasureConverged(subject);
  }

  @Test
  void errorAndAFailedRollback_abortsTheConnection_andCommitsNothing() {
    // When the rollback itself fails, the transaction may still be open, and restoring autoCommit
    // would COMMIT it. The engine aborts the connection instead (the server discards the open
    // transaction). The injected Error still reaches the caller unmasked.
    var subject = SubjectId.of("cp-f1-rollback-fails");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes());
    byte[] keyBefore = dataSource.getKey("cp-f1-rollback-fails");
    dataSource.failBefore(
        "INSERT INTO forgotten_subjects", new InjectedCrash("at the tombstone INSERT"));
    dataSource.failNextRollback(new java.sql.SQLException("rollback failed"));

    assertThrows(InjectedCrash.class, () -> engine.deleteKey(subject));

    assertErasureCommittedNothing(subject, keyBefore, ciphertext);
    assertEquals(1, dataSource.physicalAborts(), "the connection whose rollback failed is aborted");
    engine.deleteKey(subject);
    assertErasureConverged(subject);
  }

  // --- a failed rollback on a pool that does not roll back on return ---
  //
  // Agroal, the Quarkus pool, returns a connection by resetting a changed autoCommit with
  // setAutoCommit(true) and no rollback first (ConnectionHandler.resetConnection), and pgjdbc
  // COMMITS the open transaction there. Leaving autoCommit off after a failed rollback therefore
  // handed the partial transaction to the pool's commit. The engine now aborts the PHYSICAL
  // connection (unwrap): the server discards the transaction, and the pool's reset fails on the
  // closed connection, so the pool destroys it. Agroal's own wrapper abort() is a no-op on the
  // physical connection, which is why the fake models it as one.

  @Test
  void errorAndAFailedRollback_onAPoolThatResetsAutoCommitOnReturn_commitsNothing() {
    var subject = SubjectId.of("cp-f1-agroal");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes());
    byte[] keyBefore = dataSource.getKey("cp-f1-agroal");
    dataSource.returnConnectionsLikeAgroal();
    dataSource.failBefore(
        "INSERT INTO forgotten_subjects", new InjectedCrash("at the tombstone INSERT"));
    dataSource.failNextRollback(new java.sql.SQLException("rollback failed"));

    assertThrows(InjectedCrash.class, () -> engine.deleteKey(subject));

    assertErasureCommittedNothing(subject, keyBefore, ciphertext);
    assertEquals(1, dataSource.physicalAborts(), "the physical connection is aborted");
    engine.deleteKey(subject);
    assertErasureConverged(subject);
  }

  @Test
  void errorAndARollbackThatThrowsAnError_onAPoolThatResetsAutoCommitOnReturn_commitsNothing() {
    // An Error from the rollback itself (a second OutOfMemoryError) propagates, but only after the
    // connection is aborted: the pool's reset must not commit the erasure without its tombstone.
    var subject = SubjectId.of("cp-f1-agroal-error");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes());
    byte[] keyBefore = dataSource.getKey("cp-f1-agroal-error");
    dataSource.returnConnectionsLikeAgroal();
    dataSource.failBefore(
        "INSERT INTO forgotten_subjects", new InjectedCrash("at the tombstone INSERT"));
    dataSource.failNextRollback(new InjectedCrash("in the rollback"));

    InjectedCrash thrown = assertThrows(InjectedCrash.class, () -> engine.deleteKey(subject));

    assertEquals("injected crash in the rollback", thrown.getMessage());
    assertErasureCommittedNothing(subject, keyBefore, ciphertext);
    assertEquals(1, dataSource.physicalAborts(), "the physical connection is aborted");
    engine.deleteKey(subject);
    assertErasureConverged(subject);
  }

  @Test
  void aFailedAbortAfterAFailedRollback_keepsTheErrorInFlight() {
    // The abort is best effort: when it fails too, the caller still sees the original Error, and
    // raw pgjdbc's close() discards the open transaction.
    var subject = SubjectId.of("cp-f1-abort-fails");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes());
    byte[] keyBefore = dataSource.getKey("cp-f1-abort-fails");
    dataSource.failBefore(
        "INSERT INTO forgotten_subjects", new InjectedCrash("at the tombstone INSERT"));
    dataSource.failNextRollback(new java.sql.SQLException("rollback failed"));
    dataSource.failNextAbort(new java.sql.SQLException("abort failed"));

    InjectedCrash thrown = assertThrows(InjectedCrash.class, () -> engine.deleteKey(subject));

    assertEquals("injected crash at the tombstone INSERT", thrown.getMessage());
    assertEquals(0, dataSource.physicalAborts(), "the abort failed");
    assertErasureCommittedNothing(subject, keyBefore, ciphertext);
    engine.deleteKey(subject);
    assertErasureConverged(subject);
  }

  @Test
  void errorAndAFailedRollback_onAPoolThatResetsAutoCommitOnReturn_leavesNoKey() {
    // encrypt closes its connection with try-with-resources; the same pool reset applies.
    var subject = SubjectId.of("cp-e1-agroal");
    dataSource.returnConnectionsLikeAgroal();
    dataSource.failAfter("INSERT INTO encryption_keys", new InjectedCrash("after the key INSERT"));
    dataSource.failNextRollback(new java.sql.SQLException("rollback failed"));

    assertThrows(InjectedCrash.class, () -> engine.encrypt(subject, "pii".getBytes()));

    assertFalse(dataSource.containsKey("cp-e1-agroal"), "the pool's reset must not commit the key");
    assertEquals(1, dataSource.physicalAborts(), "the physical connection is aborted");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes());
    assertEquals(1, ciphertext[1], "the rerun mints generation 1");
    assertArrayEquals("pii".getBytes(), engine.decrypt(subject, ciphertext));
  }

  @Test
  void errorAfterTheTombstoneDelete_keepsTheTombstone_andARerunConverges() {
    // Reinstate: the old code committed the lifted tombstone although reinstate had failed.
    var subject = SubjectId.of("cp-r1");
    engine.encrypt(subject, "old-pii".getBytes());
    engine.deleteKey(subject);
    dataSource.failAfter(
        "DELETE FROM forgotten_subjects", new InjectedCrash("after the tombstone DELETE"));

    assertThrows(InjectedCrash.class, () -> engine.reinstate(subject));

    assertTrue(dataSource.isForgotten("cp-r1"), "the failed reinstate must keep the tombstone");
    assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, "x".getBytes()));
    engine.reinstate(subject);
    assertFalse(dataSource.isForgotten("cp-r1"), "the rerun lifts the tombstone");
    byte[] reminted = engine.encrypt(subject, "new-pii".getBytes());
    assertEquals(2, reminted[1], "the re-mint is one above the destroyed generation");
    assertArrayEquals("new-pii".getBytes(), engine.decrypt(subject, reminted));
  }

  @Test
  void errorAfterTheKeyInsert_leavesNoKey_andARerunConverges() {
    // encrypt runs through the same transaction helper: the old code committed the minted key
    // although encrypt had failed. Harmless by itself, but the same partial-commit mechanism.
    var subject = SubjectId.of("cp-e1");
    dataSource.failAfter("INSERT INTO encryption_keys", new InjectedCrash("after the key INSERT"));

    assertThrows(InjectedCrash.class, () -> engine.encrypt(subject, "pii".getBytes()));

    assertFalse(dataSource.containsKey("cp-e1"), "the failed encrypt must not leave a key behind");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes());
    assertEquals(1, ciphertext[1], "the rerun mints generation 1");
    assertArrayEquals("pii".getBytes(), engine.decrypt(subject, ciphertext));
  }

  /** Nothing of the failed erasure is visible: key, tombstone and generation row as before. */
  private void assertErasureCommittedNothing(
      SubjectId subject, byte[] keyBefore, byte[] ciphertext) {
    String id = subject.value();
    assertArrayEquals(keyBefore, dataSource.getKey(id), "the key must survive the failed erasure");
    assertFalse(dataSource.isForgotten(id), "no tombstone may be committed");
    assertNull(dataSource.erasedGeneration(id), "the generation row must be unchanged");
    assertArrayEquals("pii".getBytes(), engine.decrypt(subject, ciphertext));
  }

  /** The rerun reached the full erasure: key gone, tombstone present, generation 1 recorded. */
  private void assertErasureConverged(SubjectId subject) {
    String id = subject.value();
    assertFalse(dataSource.containsKey(id), "the rerun deletes the key");
    assertTrue(dataSource.isForgotten(id), "the rerun writes the tombstone");
    assertEquals(1, dataSource.erasedGeneration(id), "the rerun records the destroyed generation");
    assertThrows(SubjectForgottenException.class, () -> engine.encrypt(subject, "x".getBytes()));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private static String sha256Hex(String input) {
    try {
      var md = java.security.MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));
      return java.util.HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static byte[] encryptWithKey(byte[] keyBytes, byte[] iv, byte[] plaintext)
      throws Exception {
    var keySpec = new javax.crypto.spec.SecretKeySpec(keyBytes, "AES");
    var gcmSpec = new javax.crypto.spec.GCMParameterSpec(128, iv);
    var cipher = javax.crypto.Cipher.getInstance("AES/GCM/NoPadding");
    cipher.init(javax.crypto.Cipher.ENCRYPT_MODE, keySpec, gcmSpec);
    byte[] encrypted = cipher.doFinal(plaintext);
    // Mirror the engine's wire format: [format version][key version][IV][ciphertext+tag].
    byte[] combined = new byte[2 + iv.length + encrypted.length];
    combined[0] = 1;
    combined[1] = 1;
    System.arraycopy(iv, 0, combined, 2, iv.length);
    System.arraycopy(encrypted, 0, combined, 2 + iv.length, encrypted.length);
    return combined;
  }

  // -------------------------------------------------------------------------
  // In-process fake DataSource backed by dynamic proxies.
  // -------------------------------------------------------------------------

  /**
   * In-memory DataSource for unit testing — uses dynamic proxies for JDBC types.
   *
   * <p>SQL-aware: statements are routed by their {@code WHERE}/{@code FROM} clause so the {@code
   * encryption_keys} and {@code forgotten_subjects} tables are modelled separately, and each query
   * filters by the bound {@code subject_id} parameter like the real database does.
   *
   * <p>Transactional the way pgjdbc is: {@code setAutoCommit(false)} opens a transaction, {@code
   * commit()} keeps its writes and {@code rollback()} restores the state it started from. Switching
   * {@code setAutoCommit(true)} on an open transaction COMMITS it, which is the pgjdbc behaviour
   * behind both crash points. {@code close()} on an open transaction discards it, as the server
   * does when the session ends or the process dies, and so does {@code abort()}, after which the
   * connection is closed: pgjdbc throws SQLState 08003 from it. {@link #failBefore}/{@link
   * #failAfter} throw an {@link Error} at a chosen statement, and {@link #failNextRollback} makes
   * the next rollback throw, for the crash-point tests. {@link #returnConnectionsLikeAgroal} wraps
   * each connection the way the Agroal pool does (see {@link #agroalPooled}).
   */
  private static final class FakeDataSource implements DataSource {
    private final Map<String, byte[]> keys = new HashMap<>();
    // Generation of each stored key (the key_version column; every insert binds it).
    private final Map<String, Integer> keyVersions = new HashMap<>();
    // erased_key_generations: highest generation a completed erasure destroyed, per subject.
    private final Map<String, Integer> erasedGenerations = new HashMap<>();
    private final Set<String> forgotten = new HashSet<>();
    private final java.util.List<String> sqlSeen = new java.util.ArrayList<>();
    private Runnable beforeInsert = () -> {};
    // Crash-point injection, one-shot so the rerun that proves convergence runs clean: the Error
    // thrown by the next statement whose SQL contains failSqlFragment, before or after it executes.
    private String failSqlFragment;
    private boolean failAfterExecute;
    private Error injectedError;
    private Throwable rollbackFailure;
    private java.sql.SQLException abortFailure;
    private boolean agroalReturn;
    private int physicalAborts;

    /** The committed-or-open content of the four tables, for a transaction's rollback point. */
    private record State(
        Map<String, byte[]> keys,
        Map<String, Integer> keyVersions,
        Map<String, Integer> erasedGenerations,
        Set<String> forgotten) {}

    State snapshot() {
      return new State(
          new HashMap<>(keys),
          new HashMap<>(keyVersions),
          new HashMap<>(erasedGenerations),
          new HashSet<>(forgotten));
    }

    void restore(State state) {
      keys.clear();
      keys.putAll(state.keys());
      keyVersions.clear();
      keyVersions.putAll(state.keyVersions());
      erasedGenerations.clear();
      erasedGenerations.putAll(state.erasedGenerations());
      forgotten.clear();
      forgotten.addAll(state.forgotten());
    }

    /**
     * Every SQL string the engine prepared, in order — used to assert the configured table name.
     */
    java.util.List<String> sqlSeen() {
      return sqlSeen;
    }

    // The engine binds the SHA-256 hex of the subject id to the subject_id columns, so the fake's
    // maps are keyed by that hash too. These convenience methods accept the natural (raw) subject
    // id and hash it, so callers seed/inspect with plain ids exactly as before.
    void putKey(String subjectId, byte[] keyBytes) {
      keys.put(sha256Hex(subjectId), keyBytes);
      keyVersions.put(sha256Hex(subjectId), 1); // a first mint is generation 1
    }

    boolean containsKey(String subjectId) {
      return keys.containsKey(sha256Hex(subjectId));
    }

    byte[] getKey(String subjectId) {
      return keys.get(sha256Hex(subjectId));
    }

    boolean isForgotten(String subjectId) {
      return forgotten.contains(sha256Hex(subjectId));
    }

    /** The subject's erased_key_generations value, or {@code null} when it has no row. */
    Integer erasedGeneration(String subjectId) {
      return erasedGenerations.get(sha256Hex(subjectId));
    }

    /** Whether {@code literal} appears verbatim as a stored subject_id column value (any table). */
    boolean storesColumnLiteral(String literal) {
      return keys.containsKey(literal) || forgotten.contains(literal);
    }

    /** Hook executed right before an INSERT runs — simulates a concurrent writer. */
    void beforeInsert(Runnable hook) {
      this.beforeInsert = hook;
    }

    /**
     * The next statement containing {@code sqlFragment} throws {@code error} instead of running.
     */
    void failBefore(String sqlFragment, Error error) {
      arm(sqlFragment, false, error);
    }

    /** The next statement containing {@code sqlFragment} runs, then throws {@code error}. */
    void failAfter(String sqlFragment, Error error) {
      arm(sqlFragment, true, error);
    }

    private void arm(String sqlFragment, boolean afterExecute, Error error) {
      this.failSqlFragment = sqlFragment;
      this.failAfterExecute = afterExecute;
      this.injectedError = error;
    }

    /** The next {@code rollback()} throws {@code failure} and leaves the transaction open. */
    void failNextRollback(Throwable failure) {
      this.rollbackFailure = failure;
    }

    /** The next {@code abort()} throws {@code failure} and leaves the connection as it was. */
    void failNextAbort(java.sql.SQLException failure) {
      this.abortFailure = failure;
    }

    void maybeFailAbort() throws java.sql.SQLException {
      if (abortFailure != null) {
        java.sql.SQLException failure = abortFailure;
        abortFailure = null;
        throw failure;
      }
    }

    /** From now on, every connection is handed out behind an Agroal-style pooled wrapper. */
    void returnConnectionsLikeAgroal() {
      this.agroalReturn = true;
    }

    /** How many physical connections were aborted. */
    int physicalAborts() {
      return physicalAborts;
    }

    void maybeFail(String sql, boolean afterExecute) {
      if (injectedError != null
          && failAfterExecute == afterExecute
          && sql.contains(failSqlFragment)) {
        Error error = injectedError;
        injectedError = null;
        throw error;
      }
    }

    void maybeFailRollback() throws Throwable {
      if (rollbackFailure != null) {
        Throwable failure = rollbackFailure;
        rollbackFailure = null;
        throw failure;
      }
    }

    @Override
    public Connection getConnection() {
      Connection physical = fakeConnection(this);
      return agroalReturn ? agroalPooled(physical) : physical;
    }

    @Override
    public Connection getConnection(String username, String password) {
      return getConnection();
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      return null;
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  private static Connection fakeConnection(FakeDataSource ds) {
    // Per-connection transaction state (see FakeDataSource): whether autoCommit is off, and the
    // state the open transaction started from.
    boolean[] manualCommit = {false};
    FakeDataSource.State[] txStart = {null};
    boolean[] closed = {false};
    InvocationHandler h =
        (proxy, method, args) -> {
          if (closed[0] && CLOSED_CONNECTION_REFUSES.contains(method.getName())) {
            throw new java.sql.SQLException("This connection has been closed.", "08003");
          }
          switch (method.getName()) {
            case "unwrap" -> {
              Class<?> iface = (Class<?>) args[0];
              if (iface.isInstance(proxy)) {
                return proxy;
              }
              throw new java.sql.SQLException("Cannot unwrap to " + iface.getName());
            }
            case "isWrapperFor" -> {
              return ((Class<?>) args[0]).isInstance(proxy);
            }
            case "isClosed" -> {
              return closed[0];
            }
            case "abort" -> {
              // pgjdbc closes the socket; the server discards the open transaction.
              ds.maybeFailAbort();
              ((java.util.concurrent.Executor) args[0])
                  .execute(
                      () -> {
                        if (!closed[0]) {
                          ds.physicalAborts++;
                          if (manualCommit[0]) {
                            ds.restore(txStart[0]);
                          }
                          manualCommit[0] = false;
                          txStart[0] = null;
                          closed[0] = true;
                        }
                      });
              return null;
            }
            case "prepareStatement" -> {
              return fakePreparedStatement((String) args[0], ds);
            }
            case "getAutoCommit" -> {
              return !manualCommit[0];
            }
            case "setAutoCommit" -> {
              boolean autoCommit = (Boolean) args[0];
              if (autoCommit == manualCommit[0]) {
                // A real change. Turning autoCommit ON commits the open transaction (pgjdbc):
                // its writes are already in the maps, so only the rollback point is dropped.
                manualCommit[0] = !autoCommit;
                txStart[0] = autoCommit ? null : ds.snapshot();
              }
              return null;
            }
            case "commit" -> {
              if (manualCommit[0]) {
                txStart[0] = ds.snapshot();
              }
              return null;
            }
            case "rollback" -> {
              ds.maybeFailRollback();
              if (manualCommit[0]) {
                ds.restore(txStart[0]);
              }
              return null;
            }
            case "close" -> {
              // Ending the session discards an open transaction.
              if (manualCommit[0]) {
                ds.restore(txStart[0]);
                manualCommit[0] = false;
                txStart[0] = null;
              }
              return null;
            }
            default -> {
              return null;
            }
          }
        };
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, h);
  }

  /** The calls a closed pgjdbc connection refuses with SQLState 08003. */
  private static final Set<String> CLOSED_CONNECTION_REFUSES =
      Set.of("prepareStatement", "getAutoCommit", "setAutoCommit", "commit", "rollback");

  /**
   * Agroal 3.0's pooled connection wrapper, as far as these tests need it. Calls reach the physical
   * connection and {@code unwrap} returns it. {@code close()} returns the connection to the pool
   * the way Agroal does: a changed autoCommit is reset with {@code setAutoCommit(true)}, with NO
   * rollback first ({@code ConnectionHandler.resetConnection}), and a {@code SQLException} there
   * only becomes a pool warning. On an open transaction that reset COMMITS it (pgjdbc). The
   * wrapper's {@code abort()} only marks the wrapper closed, as Agroal's does: it never reaches the
   * physical connection.
   */
  private static Connection agroalPooled(Connection physical) {
    boolean[] autoCommitDirty = {false};
    boolean[] wrapperClosed = {false};
    InvocationHandler h =
        (proxy, method, args) -> {
          switch (method.getName()) {
            case "close" -> {
              if (!wrapperClosed[0]) {
                wrapperClosed[0] = true;
                if (autoCommitDirty[0]) {
                  try {
                    physical.setAutoCommit(true);
                  } catch (java.sql.SQLException _) {
                    // Agroal fires a warning; a fatal SQLState then makes it destroy the
                    // connection.
                  }
                }
              }
              return null;
            }
            case "abort" -> {
              wrapperClosed[0] = true;
              return null;
            }
            case "setAutoCommit" -> {
              if (physical.getAutoCommit() != (Boolean) args[0]) {
                autoCommitDirty[0] = true;
              }
              physical.setAutoCommit((Boolean) args[0]);
              return null;
            }
            default -> {
              try {
                return method.invoke(physical, args);
              } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
              }
            }
          }
        };
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, h);
  }

  private static PreparedStatement fakePreparedStatement(String sql, FakeDataSource ds) {
    ds.sqlSeen.add(sql);
    Map<Integer, Object> params = new HashMap<>();

    InvocationHandler h =
        (proxy, method, args) -> {
          String name = method.getName();
          if (name.equals("setString") || name.equals("setInt")) {
            params.put((Integer) args[0], args[1]);
            return null;
          } else if (name.equals("setBytes")) {
            // Real drivers copy the bytes to the wire; the engine zeroes its buffer afterwards,
            // so the fake must not keep a reference to the caller's array.
            params.put((Integer) args[0], ((byte[]) args[1]).clone());
            return null;
          } else if (name.equals("executeUpdate")) {
            ds.maybeFail(sql, false);
            int updated = executeUpdate(sql, params, ds);
            ds.maybeFail(sql, true);
            return updated;
          } else if (name.equals("executeQuery")) {
            ResultSet rs = executeQuery(sql, params, ds);
            params.clear();
            return rs;
          } else if (name.equals("close")) {
            return null;
          }
          return null;
        };

    return (PreparedStatement)
        Proxy.newProxyInstance(
            PreparedStatement.class.getClassLoader(), new Class<?>[] {PreparedStatement.class}, h);
  }

  private static int executeUpdate(String sql, Map<Integer, Object> params, FakeDataSource ds) {
    String subjectId = (String) params.get(1);
    if (sql.startsWith("DELETE")) {
      params.clear();
      // Faithful row counts, as PostgreSQL reports them.
      if (sql.contains("forgotten_subjects")) {
        return ds.forgotten.remove(subjectId) ? 1 : 0;
      }
      ds.keyVersions.remove(subjectId);
      return ds.keys.remove(subjectId) != null ? 1 : 0;
    } else if (sql.startsWith("INSERT")) {
      if (sql.contains("forgotten_subjects")) {
        params.clear();
        return ds.forgotten.add(subjectId) ? 1 : 0;
      }
      if (sql.contains("erased_key_generations")) {
        // INSERT ... ON CONFLICT DO UPDATE SET max_erased_version = GREATEST(existing, new).
        int erased = (Integer) params.get(2);
        params.clear();
        ds.erasedGenerations.merge(subjectId, erased, Math::max);
        return 1;
      }
      ds.beforeInsert.run();
      byte[] keyBytes = (byte[]) params.get(2);
      Integer version = (Integer) params.get(3);
      params.clear();
      if (sql.contains("DO NOTHING")) {
        // Postgres ON CONFLICT DO NOTHING: 0 rows affected on conflict.
        if (ds.keys.putIfAbsent(subjectId, keyBytes) == null) {
          ds.keyVersions.put(subjectId, version);
          return 1;
        }
        return 0;
      }
      ds.keys.put(subjectId, keyBytes);
      ds.keyVersions.put(subjectId, version);
      return 1;
    }
    params.clear();
    return 0;
  }

  private static ResultSet executeQuery(
      String sql, Map<Integer, Object> params, FakeDataSource ds) {
    String subjectId = (String) params.get(1);
    if (sql.contains("pg_advisory_xact_lock")) {
      // The fake models a single in-process connection, so there is no real concurrency for the
      // advisory lock to serialize against — just hand back a one-row result so the driver-side
      // rs.next() the engine performs succeeds, mirroring what Postgres itself would return.
      return fakeRowResultSet(true, null, null);
    }
    if (sql.contains("forgotten_subjects")) {
      // SELECT 1 FROM forgotten_subjects WHERE subject_id = ?
      return fakeRowResultSet(ds.forgotten.contains(subjectId), null, null);
    }
    if (sql.contains("max_erased_version")) {
      // SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?
      Integer erased = ds.erasedGenerations.get(subjectId);
      return fakeRowResultSet(erased != null, null, erased);
    }
    if (sql.contains("key_bytes")) {
      // SELECT key_bytes, key_version FROM encryption_keys WHERE subject_id = ?
      byte[] key = ds.keys.get(subjectId);
      return fakeRowResultSet(key != null, key, ds.keyVersions.get(subjectId));
    }
    if (sql.contains("key_version")) {
      // SELECT key_version FROM encryption_keys WHERE subject_id = ?  (deleteKey)
      boolean present = ds.keys.containsKey(subjectId);
      return fakeRowResultSet(present, null, ds.keyVersions.get(subjectId));
    }
    // SELECT 1 FROM encryption_keys WHERE subject_id = ?  (isKeyAvailable)
    return fakeRowResultSet(ds.keys.containsKey(subjectId), null, null);
  }

  /**
   * A single-row-or-empty ResultSet. {@code present} controls whether {@code next()} yields one
   * row; {@code keyBytes} (when non-null) is what {@code getBytes("key_bytes")} returns on that
   * row; {@code intValue} (when non-null) is what {@code getInt(...)} returns for any column.
   */
  private static ResultSet fakeRowResultSet(boolean present, byte[] keyBytes, Integer intValue) {
    int[] remaining = {present ? 1 : 0};

    InvocationHandler h =
        (proxy, method, args) -> {
          String name = method.getName();
          if (name.equals("next")) {
            if (remaining[0] > 0) {
              remaining[0]--;
              return true;
            }
            return false;
          } else if (name.equals("getBytes")) {
            String col = (String) args[0];
            if ("key_bytes".equals(col) && keyBytes != null) {
              // Real drivers return a fresh array per call; the engine zeroes it after use,
              // so handing out the stored reference would wipe the fake's key.
              return keyBytes.clone();
            }
            return null;
          } else if (name.equals("getInt")) {
            return intValue == null ? 0 : intValue;
          } else if (name.equals("close")) {
            return null;
          }
          return null;
        };

    return (ResultSet)
        Proxy.newProxyInstance(
            ResultSet.class.getClassLoader(), new Class<?>[] {ResultSet.class}, h);
  }
}
