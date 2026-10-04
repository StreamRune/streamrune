package org.streamrune.crypto.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.types.SubjectId;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The durable {@link JdbcForgottenSubjectStore} must make GDPR erasure survive a restart. A
 * tombstone recorded before a "restart" (a fresh store instance over the same database) must still
 * be visible afterwards, so a KMS/Vault engine keeps redacting the erased subject instead of
 * un-forgetting it. Also asserts the permanent tombstone stores only the SHA-256 hash of the
 * subject id, never the raw (possibly-PII) value.
 */
@Testcontainers
class JdbcForgottenSubjectStoreIntegrationTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_test");

  private static PGSimpleDataSource dataSource;

  @BeforeAll
  static void init() throws Exception {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());

    // The shipped crypto baseline: among others the shared forgotten_subjects table every backend
    // tombstones into, including the key_created_at column the Vault erasure records its
    // key-identity evidence in.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          java.nio.file.Files.readString(
              java.nio.file.Path.of(
                  "src/main/resources/db/crypto-migration/V001__crypto_baseline.sql")));
    }
  }

  @Test
  void tombstoneSurvivesRestart_aFreshStoreInstanceStillReportsForgotten() {
    var subject = SubjectId.of("customer-forget-me@example.com");

    var storeBeforeRestart = new JdbcForgottenSubjectStore(dataSource);
    assertFalse(storeBeforeRestart.isForgotten(subject));
    storeBeforeRestart.forget(subject);
    assertTrue(storeBeforeRestart.isForgotten(subject));

    // "Restart": a brand-new store instance over the SAME database — nothing in JVM heap carries
    // over. The tombstone is a row, so the subject is still forgotten.
    var storeAfterRestart = new JdbcForgottenSubjectStore(dataSource);
    assertTrue(
        storeAfterRestart.isForgotten(subject),
        "a JDBC-backed tombstone must survive a restart — this is exactly what the in-memory"
            + " default failed to do");

    // reinstate lifts the tombstone durably too.
    storeAfterRestart.reinstate(subject);
    assertFalse(new JdbcForgottenSubjectStore(dataSource).isForgotten(subject));
  }

  @Test
  void forgetIsIdempotent() {
    var subject = SubjectId.of("idempotent@example.com");
    var store = new JdbcForgottenSubjectStore(dataSource);
    store.forget(subject);
    store.forget(subject);
    assertTrue(store.isForgotten(subject));
  }

  @Test
  void forgottenAtReportsTheDurableFirstForgetInstant_andEmptyWhenNotTombstoned() {
    // VaultCryptoEngine.reinstate discriminates
    // the revivable crashed-erasure original from post-forget residue by comparing the
    // transit key's creation time with this timestamp. It must be the FIRST forget's instant —
    // the conflict-DO-NOTHING upsert never overwrites forgotten_at on an idempotent re-forget.
    var subject = SubjectId.of("forgotten-at-subject@example.com");
    var store = new JdbcForgottenSubjectStore(dataSource);

    assertTrue(store.forgottenAt(subject).isEmpty(), "not tombstoned -> empty");

    store.forget(subject);
    var first = store.forgottenAt(subject).orElseThrow();

    store.forget(subject); // idempotent re-forget
    assertEquals(
        first,
        store.forgottenAt(subject).orElseThrow(),
        "an idempotent re-forget must not advance forgotten_at");

    store.reinstate(subject);
    assertTrue(store.forgottenAt(subject).isEmpty(), "reinstated -> empty again");
  }

  @Test
  void keyCreatedAtRoundTripsTheErasedKeysCreationInstant_firstWriteWins() {
    // VaultCryptoEngine.reinstate proves the live
    // transit key IS the key the erasure destroyed by comparing this recorded instant against the
    // live key's creation time — both read from Vault, so the proof carries no cross-clock term.
    // The record must survive an idempotent re-erasure UNCHANGED: by the second run the key the
    // engine can read may already be post-forget residue, and recording that would
    // prove the wrong key and hand the residue a revival.
    var subject = SubjectId.of("key-identity-subject@example.com");
    var store = new JdbcForgottenSubjectStore(dataSource);
    // TIMESTAMPTZ has microsecond resolution; the engine truncates before writing for exactly this
    // reason, so a value that round-trips unchanged is what the equality check will later see.
    var original =
        java.time.Instant.now()
            .minus(java.time.Duration.ofDays(365))
            .truncatedTo(java.time.temporal.ChronoUnit.MICROS);

    assertTrue(store.keyCreatedAt(subject).isEmpty(), "not tombstoned -> no evidence");

    store.forget(subject, original);
    assertTrue(store.isForgotten(subject));
    assertEquals(java.util.Optional.of(original), store.keyCreatedAt(subject));

    store.forget(subject, java.time.Instant.now()); // the documented idempotent re-run
    assertEquals(
        java.util.Optional.of(original),
        store.keyCreatedAt(subject),
        "a repeated erasure must never overwrite the FIRST erasure's evidence");

    store.reinstate(subject);
    assertTrue(
        store.keyCreatedAt(subject).isEmpty(),
        "the evidence lives in the tombstone row, so lifting the tombstone takes it with it");
  }

  @Test
  void aTombstoneWithoutEvidence_nullOrOneArgForget_readsAsNoEvidence() {
    // Two ways a tombstone ends up with no evidence, and both must read as "unprovable identity"
    // so the Vault revival fails closed: an erasure whose key read failed (the Vault engine
    // records null here), and the plain one-argument forget the aws-kms engine uses, which never
    // names the column.
    var store = new JdbcForgottenSubjectStore(dataSource);

    var noEvidence = SubjectId.of("no-evidence-subject@example.com");
    store.forget(noEvidence, null);
    assertTrue(store.isForgotten(noEvidence), "the erasure still stands");
    assertTrue(store.keyCreatedAt(noEvidence).isEmpty());

    var oneArg = SubjectId.of("one-arg-tombstone-subject@example.com");
    store.forget(oneArg);
    assertTrue(store.isForgotten(oneArg));
    assertTrue(store.keyCreatedAt(oneArg).isEmpty());
  }

  @Test
  void keyIdentityEvidenceFailuresRedactTheSubjectId_andThrowRatherThanAnswerEmpty() {
    // A read failure must THROW: reinstate must never mistake a database outage for "no evidence"
    // and it must never leak the (possibly-PII) raw subject id into the message that reaches the
    // GDPR audit_log FAILURE row.
    var rawPii = "evidence-leak@example.com";
    var subject = SubjectId.of(rawPii);
    var store = new JdbcForgottenSubjectStore(new FailingDataSource());

    for (var op :
        java.util.List.<org.junit.jupiter.api.function.Executable>of(
            () -> store.keyCreatedAt(subject),
            () -> store.forget(subject, java.time.Instant.now()))) {
      var ex = assertThrows(org.streamrune.core.crypto.CryptoOperationException.class, op);
      assertFalse(
          ex.getMessage().contains(rawPii),
          "exception message must not leak the raw subject id: " + ex.getMessage());
      assertTrue(
          ex.getMessage().contains(subject.redacted()),
          "exception message must carry the redacted subject hash: " + ex.getMessage());
    }
  }

  @Test
  void storedSubjectIdIsHashed_notRawPii() throws Exception {
    var rawPii = "hash-me@example.com";
    var store = new JdbcForgottenSubjectStore(dataSource);
    store.forget(SubjectId.of(rawPii));

    // The permanent tombstone must hold only the SHA-256 hash of the subject id, never the raw PII.
    assertFalse(columnLiteralExists(rawPii));
    assertTrue(columnLiteralExists(sha256Hex(rawPii)));
  }

  @Test
  void failureMessagesRedactTheSubjectId_neverLeakRawPii() {
    // A DB error while checking/recording a tombstone must not embed the raw
    // (possibly-PII) subject id in the CryptoOperationException — that message flows into the GDPR
    // audit_log FAILURE row and into log/DLQ error handlers. It must carry only the SHA-256 hash.
    var rawPii = "leak-me@example.com";
    var subject = SubjectId.of(rawPii);
    var store = new JdbcForgottenSubjectStore(new FailingDataSource());

    for (var op :
        java.util.List.<org.junit.jupiter.api.function.Executable>of(
            () -> store.isForgotten(subject),
            () -> store.forget(subject),
            () -> store.reinstate(subject))) {
      var ex = assertThrows(org.streamrune.core.crypto.CryptoOperationException.class, op);
      assertFalse(
          ex.getMessage().contains(rawPii),
          "exception message must not leak the raw subject id: " + ex.getMessage());
      assertTrue(
          ex.getMessage().contains(subject.redacted()),
          "exception message must carry the redacted subject hash: " + ex.getMessage());
    }
  }

  @Test
  void rejectsNullDataSource() {
    assertThrows(IllegalArgumentException.class, () -> new JdbcForgottenSubjectStore(null));
  }

  /** A {@link javax.sql.DataSource} that always fails to hand out a connection. */
  private static final class FailingDataSource implements javax.sql.DataSource {
    @Override
    public java.sql.Connection getConnection() throws java.sql.SQLException {
      throw new java.sql.SQLException("connection unavailable");
    }

    @Override
    public java.sql.Connection getConnection(String username, String password)
        throws java.sql.SQLException {
      throw new java.sql.SQLException("connection unavailable");
    }

    @Override
    public java.io.PrintWriter getLogWriter() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLogWriter(java.io.PrintWriter out) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setLoginTimeout(int seconds) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getLoginTimeout() {
      throw new UnsupportedOperationException();
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
      throw new UnsupportedOperationException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  @Test
  void rejectsNullSubject() {
    var store = new JdbcForgottenSubjectStore(dataSource);
    assertThrows(IllegalArgumentException.class, () -> store.forget(null));
    assertThrows(IllegalArgumentException.class, () -> store.isForgotten(null));
    assertThrows(IllegalArgumentException.class, () -> store.reinstate(null));
    assertThrows(IllegalArgumentException.class, () -> store.forget(null, java.time.Instant.now()));
    assertThrows(IllegalArgumentException.class, () -> store.keyCreatedAt(null));
  }

  private static boolean columnLiteralExists(String literal) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT 1 FROM forgotten_subjects WHERE subject_id = ?")) {
      ps.setString(1, literal);
      try (var rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static String sha256Hex(String value) throws Exception {
    var md = java.security.MessageDigest.getInstance("SHA-256");
    return java.util.HexFormat.of()
        .formatHex(md.digest(value.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
  }
}
