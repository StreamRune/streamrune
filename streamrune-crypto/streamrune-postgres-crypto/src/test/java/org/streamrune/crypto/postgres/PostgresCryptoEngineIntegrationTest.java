package org.streamrune.crypto.postgres;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class PostgresCryptoEngineIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static PGSimpleDataSource dataSource;
  private static PostgresCryptoEngine engine;

  @BeforeAll
  static void init() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    // Apply the crypto baseline shipped in this artifact, proving the packaged DDL actually works.
    applyMigration("/db/crypto-migration/V001__crypto_baseline.sql");

    engine = PostgresCryptoEngine.builder().dataSource(dataSource).build();
  }

  private static void applyMigration(String resource) throws Exception {
    String ddl;
    try (var in = PostgresCryptoEngineIntegrationTest.class.getResourceAsStream(resource)) {
      assertNotNull(in, "shipped migration must be on the classpath: " + resource);
      ddl = new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(ddl);
    }
  }

  @Test
  void preForgetCiphertextDegradesToKeyNotFound_afterReinstateAndRemint_realSchema() {
    // Against real PostgreSQL and the shipped migrations:
    // the documented reinstate flow must leave pre-forget ciphertext replay-tolerant
    // (KeyNotFoundException -> [REDACTED]), not a GCM tag failure that permanently blocks
    // aggregate loads and full projection rebuilds after the returning subject's first new event.
    var subject = SubjectId.of("returning-customer-it");
    byte[] preForget = engine.encrypt(subject, "old-pii".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] postReinstate = engine.encrypt(subject, "new-pii".getBytes(StandardCharsets.UTF_8));

    // The generation comes from the erasure itself: deleteKey recorded the destroyed generation 1
    // in erased_key_generations, so the re-mint is generation 2 without reinstate writing it.
    assertEquals(1, preForget[1], "the first mint is generation 1");
    assertEquals(2, postReinstate[1], "the re-mint is one above the destroyed generation");
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, preForget));
    assertArrayEquals(
        "new-pii".getBytes(StandardCharsets.UTF_8), engine.decrypt(subject, postReinstate));

    // A second erase/reinstate cycle keeps every prior generation redacted.
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] gen3 = engine.encrypt(subject, "gen3".getBytes(StandardCharsets.UTF_8));
    assertEquals(3, gen3[1], "every completed erasure raises the next mint by one generation");
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, preForget));
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, postReinstate));
    assertArrayEquals("gen3".getBytes(StandardCharsets.UTF_8), engine.decrypt(subject, gen3));
  }

  @Test
  void tamperedCurrentGenerationStillFailsLoudAfterReinstate_realSchema() {
    var subject = SubjectId.of("tamper-after-reinstate-it");
    engine.encrypt(subject, "old".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(subject);
    engine.reinstate(subject);
    byte[] current = engine.encrypt(subject, "new".getBytes(StandardCharsets.UTF_8));
    current[current.length - 1] ^= 0x01;

    // KeyNotFoundException is NOT a subtype of CryptoOperationException, so this assertThrows
    // by itself proves the tamper did not degrade to the replay-tolerant forget signal.
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.decrypt(subject, current));
  }

  @Test
  void roundtrip_encrypt_decrypt() {
    byte[] plaintext = "hello world".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertext = engine.encrypt(SubjectId.of("subject-1"), plaintext);
    byte[] decrypted = engine.decrypt(SubjectId.of("subject-1"), ciphertext);
    assertArrayEquals(plaintext, decrypted);
  }

  @Test
  void different_subjects_get_different_keys() {
    byte[] plaintext = "same data".getBytes(StandardCharsets.UTF_8);
    byte[] ciphertextA = engine.encrypt(SubjectId.of("subject-a"), plaintext);
    byte[] ciphertextB = engine.encrypt(SubjectId.of("subject-b"), plaintext);
    assertFalse(java.util.Arrays.equals(ciphertextA, ciphertextB));
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("subject-a"), ciphertextA));
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("subject-b"), ciphertextB));
  }

  @Test
  void key_available_after_encrypt() {
    engine.encrypt(SubjectId.of("avail-1"), "data".getBytes(StandardCharsets.UTF_8));
    assertTrue(engine.isKeyAvailable(SubjectId.of("avail-1")));
  }

  @Test
  void key_not_available_before_encrypt() {
    assertFalse(engine.isKeyAvailable(SubjectId.of("nonexistent-subject")));
  }

  @Test
  void stored_subject_id_column_is_hashed_not_raw_pii() throws Exception {
    String rawPii = "carol@example.com";
    engine.encrypt(SubjectId.of(rawPii), "data".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(SubjectId.of(rawPii));

    // Against the real database: the raw PII value must appear in neither table's subject_id
    // column, only its SHA-256 hash — the permanent tombstone included.
    assertFalse(columnLiteralExists("encryption_keys", rawPii));
    assertFalse(columnLiteralExists("forgotten_subjects", rawPii));
    assertTrue(columnLiteralExists("forgotten_subjects", sha256Hex(rawPii)));
  }

  private static boolean columnLiteralExists(String table, String literal) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT 1 FROM " + table + " WHERE subject_id = ?")) {
      ps.setString(1, literal);
      try (var rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  @Test
  void delete_key_removes_it() {
    engine.encrypt(SubjectId.of("del-1"), "data".getBytes(StandardCharsets.UTF_8));
    assertTrue(engine.isKeyAvailable(SubjectId.of("del-1")));
    engine.deleteKey(SubjectId.of("del-1"));
    assertFalse(engine.isKeyAvailable(SubjectId.of("del-1")));
  }

  @Test
  void decrypt_after_delete_fails() {
    byte[] ciphertext =
        engine.encrypt(SubjectId.of("del-2"), "secret".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(SubjectId.of("del-2"));
    // Old ciphertext maps to the [REDACTED] tombstone via KeyNotFoundException — NOT
    // SubjectForgottenException, which is reserved for the encrypt path.
    assertThrows(
        KeyNotFoundException.class, () -> engine.decrypt(SubjectId.of("del-2"), ciphertext));
  }

  @Test
  void encrypt_after_delete_throws_subject_forgotten() {
    engine.encrypt(SubjectId.of("forget-1"), "secret".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(SubjectId.of("forget-1"));

    // Terminal erasure: a later encrypt must not silently mint a fresh key for the erased subject.
    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("forget-1"), "new PII".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void subject_forgotten_exception_message_redacts_raw_pii() {
    // The SubjectForgottenException message flows into application logs and, if the
    // failing command is dead-lettered, into dead_letter_queue.error_message. A subject id may
    // itself be PII (an email), so the message must carry only the SHA-256 hash — never the raw id.
    String rawPii = "dave@example.com";
    var subject = SubjectId.of(rawPii);
    engine.encrypt(subject, "secret".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(subject);

    var ex =
        assertThrows(
            SubjectForgottenException.class,
            () -> engine.encrypt(subject, "new PII".getBytes(StandardCharsets.UTF_8)));
    assertFalse(
        ex.getMessage().contains(rawPii),
        "exception message must not leak the raw subject id: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains(subject.redacted()),
        "exception message must carry the subject hash");
  }

  @Test
  void tombstone_survives_a_fresh_engine_instance() {
    engine.encrypt(SubjectId.of("forget-persist"), "secret".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(SubjectId.of("forget-persist"));

    // A new engine over the same database still sees the tombstone row — erasure is durable.
    var reopened = PostgresCryptoEngine.builder().dataSource(dataSource).build();
    assertThrows(
        SubjectForgottenException.class,
        () ->
            reopened.encrypt(
                SubjectId.of("forget-persist"), "data".getBytes(StandardCharsets.UTF_8)));
  }

  @Test
  void reinstate_allows_recreation_after_forget() {
    engine.encrypt(SubjectId.of("comeback"), "old".getBytes(StandardCharsets.UTF_8));
    engine.deleteKey(SubjectId.of("comeback"));
    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("comeback"), "x".getBytes(StandardCharsets.UTF_8)));

    engine.reinstate(SubjectId.of("comeback"));

    byte[] ciphertext =
        engine.encrypt(SubjectId.of("comeback"), "new".getBytes(StandardCharsets.UTF_8));
    assertArrayEquals(
        "new".getBytes(StandardCharsets.UTF_8),
        engine.decrypt(SubjectId.of("comeback"), ciphertext));
  }

  @Test
  void forgetOfANeverEncryptedSubject_reinstateThenEncrypt_mintsGenerationOne_realSchema()
      throws Exception {
    // deleteKey records a destroyed generation only when it destroys a key row, and reinstate
    // records none. A subject forgotten before it ever encrypted has no ciphertext under any
    // generation, so its first key after reinstate is generation 1 and no erased_key_generations
    // row exists — no erase/re-register cycle is burned.
    var subject = SubjectId.of("erased-before-any-key-it@example.com");
    engine.deleteKey(subject); // tombstone, no key ever minted -> no recorded generation
    engine.reinstate(subject);
    byte[] postReinstate = engine.encrypt(subject, "post-reinstate-pii".getBytes(UTF_8));

    assertEquals(1, postReinstate[1], "no key was destroyed, so the first mint is generation 1");
    assertEquals(0, maxErasedGeneration(subject), "reinstate must record no generation");
    assertArrayEquals("post-reinstate-pii".getBytes(UTF_8), engine.decrypt(subject, postReinstate));
  }

  /** The subject's recorded destroyed-generation high-water mark, or 0 when no row exists. */
  private static int maxErasedGeneration(SubjectId subject) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT max_erased_version FROM erased_key_generations WHERE subject_id = ?")) {
      ps.setString(1, sha256Hex(subject.value()));
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getInt(1) : 0;
      }
    }
  }

  @Test
  void same_subject_encrypt_twice_produces_different_ciphertext() {
    byte[] plaintext = "same-data".getBytes(StandardCharsets.UTF_8);
    byte[] ct1 = engine.encrypt(SubjectId.of("iv-subject"), plaintext);
    byte[] ct2 = engine.encrypt(SubjectId.of("iv-subject"), plaintext);
    assertFalse(java.util.Arrays.equals(ct1, ct2));
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("iv-subject"), ct1));
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("iv-subject"), ct2));
  }

  @Test
  void concurrent_key_creation_first_writer_wins() throws Exception {
    int threads = 8;
    byte[] plaintext = "racy secret".getBytes(StandardCharsets.UTF_8);
    var barrier = new java.util.concurrent.CyclicBarrier(threads);
    var executor = java.util.concurrent.Executors.newFixedThreadPool(threads);
    try {
      var futures = new java.util.ArrayList<java.util.concurrent.Future<byte[]>>();
      for (int i = 0; i < threads; i++) {
        futures.add(
            executor.submit(
                () -> {
                  barrier.await();
                  return engine.encrypt(SubjectId.of("race-subject"), plaintext);
                }));
      }
      // Every ciphertext must decrypt with the single stored key — a concurrent
      // create must never overwrite a key another writer already encrypted with.
      for (var future : futures) {
        assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("race-subject"), future.get()));
      }
    } finally {
      executor.shutdownNow();
    }
  }

  // --- encrypt() racing deleteKey() (TOCTOU: forgotten-check vs. key-create) ---

  /**
   * Forces the exact TOCTOU interleave via a test-only seam ({@link
   * PostgresCryptoEngine#setLoadOrCreateKeyRaceHook}): a hook invoked by the engine right after the
   * {@code forgotten_subjects} check and right before a fresh key would be inserted. The hook
   * signals that encrypt() has reached the checkpoint, then gives deleteKey() (running on a second
   * thread) a bounded window to run.
   *
   * <p>On the unfixed engine, deleteKey() is unsynchronized with this checkpoint, so it completes
   * within the window and its tombstone/delete commits BEFORE encrypt() resumes and inserts a fresh
   * key — reproducing the bug: a live key ends up coexisting with a tombstone. On the fixed engine,
   * both operations take the same per-subject {@code pg_advisory_xact_lock}, so deleteKey() cannot
   * even start its critical section until encrypt()'s transaction (holding the lock through the
   * hook) commits or rolls back; the bounded wait simply elapses and deleteKey() runs afterwards.
   * Either way, the interleave is deterministic — no probabilistic timing, no flakiness — and the
   * post-condition (never a live key + tombstone together) is checked after both threads finish.
   */
  @Test
  void encryptRacingDeleteKey_forgottenAlwaysWins() throws Exception {
    int iterations = 20;
    for (int i = 0; i < iterations; i++) {
      // Brand-new subject: no key, no tombstone yet. This is what makes loadOrCreateKey() take the
      // forgotten-check/create-key branch (where the hook fires) instead of the load-existing-key
      // fast path. The race is: does a deleteKey() that starts DURING this subject's very first
      // encrypt() ever leave a live key behind alongside its tombstone?
      SubjectId subject = SubjectId.of("race-forget-" + i);

      var reachedCheckpoint = new java.util.concurrent.CountDownLatch(1);
      PostgresCryptoEngine.setLoadOrCreateKeyRaceHook(
          () -> {
            reachedCheckpoint.countDown();
            // Bounded, not unbounded: once the fix serializes the two operations via the advisory
            // lock, deleteKey() cannot complete while this hook (inside encrypt()'s transaction,
            // holding the lock) is running — waiting unboundedly for it here would deadlock. A
            // short sleep is enough to give the racy (unfixed) implementation its window to
            // interleave; the fixed implementation just lets the sleep elapse harmlessly.
            sleep(300);
          });
      try {
        var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
          var encryptFuture =
              executor.submit(
                  () ->
                      engine.encrypt(subject, "resurrected PII".getBytes(StandardCharsets.UTF_8)));
          var deleteFuture =
              executor.submit(
                  (java.util.concurrent.Callable<Void>)
                      () -> {
                        await(reachedCheckpoint);
                        engine.deleteKey(subject);
                        return null;
                      });

          Boolean encryptThrewForgotten;
          try {
            encryptFuture.get(15, java.util.concurrent.TimeUnit.SECONDS);
            encryptThrewForgotten = false;
          } catch (java.util.concurrent.ExecutionException e) {
            encryptThrewForgotten = e.getCause() instanceof SubjectForgottenException;
            if (!encryptThrewForgotten) {
              throw new AssertionError("unexpected encrypt failure", e.getCause());
            }
          }
          deleteFuture.get(15, java.util.concurrent.TimeUnit.SECONDS);

          // Post-condition: "forgotten wins" must hold no matter how the race resolved. Either
          // encrypt observed the tombstone and threw, or it committed its key before the delete's
          // tombstone landed — but a live key must NEVER coexist with a tombstone for the same
          // subject afterwards.
          boolean keyExists = keyRowExists(subject);
          boolean tombstoneExists = tombstoneRowExists(subject);
          assertFalse(
              keyExists && tombstoneExists,
              "iteration "
                  + i
                  + ": a live key must never coexist with a tombstone (encryptThrewForgotten="
                  + encryptThrewForgotten
                  + ")");
        } finally {
          executor.shutdownNow();
        }
      } finally {
        PostgresCryptoEngine.setLoadOrCreateKeyRaceHook(null);
      }
    }
  }

  private static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static void await(java.util.concurrent.CountDownLatch latch) {
    try {
      if (!latch.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
        throw new AssertionError("timed out waiting for latch");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  private static boolean keyRowExists(SubjectId subjectId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT 1 FROM encryption_keys WHERE subject_id = ?")) {
      // The engine stores the SHA-256 hash of the subject id, so inspect the DB by that hash.
      ps.setString(1, sha256Hex(subjectId.value()));
      try (var rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static boolean tombstoneRowExists(SubjectId subjectId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT 1 FROM forgotten_subjects WHERE subject_id = ?")) {
      ps.setString(1, sha256Hex(subjectId.value()));
      try (var rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** SHA-256 hex of the subject id — the opaque value the engine stores in subject_id columns. */
  private static String sha256Hex(String input) {
    try {
      var md = java.security.MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(input.getBytes(StandardCharsets.UTF_8));
      return java.util.HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
