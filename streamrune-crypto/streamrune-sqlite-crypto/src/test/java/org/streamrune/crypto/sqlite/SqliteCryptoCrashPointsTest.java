package org.streamrune.crypto.sqlite;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.crypto.sqlite.SqliteCryptoCharacterizationTest.writeTransactionOn;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.sqlite.SqliteDatabase;

/**
 * The crash points of every durable write of the SQLite crypto stores. A trigger makes one
 * statement of a write transaction fail after the statements before it ran: the transaction rolls
 * back as it does when the process dies before its {@code COMMIT} (SQLite rolls back an uncommitted
 * transaction when its connection or process dies). Each case asserts that nothing of the write
 * remains, then runs it again without the fault and asserts that it converges.
 */
class SqliteCryptoCrashPointsTest {

  private static final byte[] PLAINTEXT = "pii".getBytes(UTF_8);

  @TempDir Path dir;

  private DataSource dataSource;
  private SqliteDatabase database;
  private SqliteCryptoEngine engine;

  @BeforeEach
  void open() {
    dataSource = SqliteFiles.dataSource(dir.resolve("crash.db"));
    database = SqliteFiles.migrated(dataSource);
    engine = SqliteCryptoEngine.builder().database(database).build();
  }

  @AfterEach
  void close() {
    database.close();
  }

  /** Key creation: the mint fails at its insert; no key, so the rerun mints generation 1. */
  @Test
  void aMintThatDiesBeforeItsCommitLeavesNoKey_andTheRerunMints() throws Exception {
    var subject = SubjectId.of("mint");
    failBefore("INSERT ON encryption_keys");

    assertThatThrownBy(() -> engine.encrypt(subject, PLAINTEXT))
        .isExactlyInstanceOf(CryptoOperationException.class);
    assertThat(count("encryption_keys")).isZero();

    heal();
    byte[] ciphertext = engine.encrypt(subject, PLAINTEXT);
    assertThat(ciphertext[1]).isEqualTo((byte) 1);
    assertThat(engine.decrypt(subject, ciphertext)).isEqualTo(PLAINTEXT);
    assertThat(count("encryption_keys")).isOne();
  }

  /**
   * deleteKey: the generation is recorded and the key deleted, then the tombstone insert fails; the
   * key and the generation are back, the old ciphertext reads, and the rerun erases.
   */
  @Test
  void anErasureThatDiesAtItsTombstoneLeavesTheKey_andTheRerunErases() throws Exception {
    var subject = SubjectId.of("erase-at-tombstone");
    byte[] ciphertext = engine.encrypt(subject, PLAINTEXT);
    failBefore("INSERT ON forgotten_subjects");

    assertThatThrownBy(() -> engine.deleteKey(subject))
        .isExactlyInstanceOf(CryptoOperationException.class);
    assertThat(engine.decrypt(subject, ciphertext)).isEqualTo(PLAINTEXT);
    assertThat(count("erased_key_generations")).isZero();
    assertThat(count("forgotten_subjects")).isZero();

    heal();
    engine.deleteKey(subject);
    assertErased(subject, ciphertext);
  }

  /**
   * deleteKey: the erased-generation upsert itself fails, as the first statement that writes; for a
   * first erasure it inserts the row, for a later one it raises the recorded generation. Nothing
   * remains either way, and the reruns converge on the right generation.
   */
  @Test
  void anErasureThatDiesAtTheGenerationUpsertLeavesNothing_andTheRerunErases() throws Exception {
    var subject = SubjectId.of("erase-at-generation");
    byte[] first = engine.encrypt(subject, PLAINTEXT);
    failBefore("INSERT ON erased_key_generations");

    assertThatThrownBy(() -> engine.deleteKey(subject))
        .isExactlyInstanceOf(CryptoOperationException.class);
    assertThat(engine.decrypt(subject, first)).isEqualTo(PLAINTEXT);
    assertThat(count("erased_key_generations")).isZero();
    assertThat(count("forgotten_subjects")).isZero();

    heal();
    engine.deleteKey(subject);
    assertErased(subject, first);

    engine.reinstate(subject);
    byte[] second = engine.encrypt(subject, PLAINTEXT);
    failBefore("UPDATE ON erased_key_generations");
    assertThatThrownBy(() -> engine.deleteKey(subject))
        .isExactlyInstanceOf(CryptoOperationException.class);
    assertThat(engine.decrypt(subject, second)).isEqualTo(PLAINTEXT);
    assertThat(
            SqliteFiles.query(dataSource, "SELECT max_erased_version FROM erased_key_generations"))
        .containsExactly(List.of(1));

    heal();
    engine.deleteKey(subject);
    assertThat(
            SqliteFiles.query(dataSource, "SELECT max_erased_version FROM erased_key_generations"))
        .containsExactly(List.of(2));
    assertThatThrownBy(() -> engine.decrypt(subject, second))
        .isExactlyInstanceOf(KeyNotFoundException.class);
  }

  /** deleteKey: the key delete fails after the generation was recorded; that record goes too. */
  @Test
  void anErasureThatDiesAtTheKeyDeleteLeavesNoGeneration_andTheRerunErases() throws Exception {
    var subject = SubjectId.of("erase-at-key");
    byte[] ciphertext = engine.encrypt(subject, PLAINTEXT);
    failBefore("DELETE ON encryption_keys");

    assertThatThrownBy(() -> engine.deleteKey(subject))
        .isExactlyInstanceOf(CryptoOperationException.class);
    assertThat(count("erased_key_generations")).isZero();
    assertThat(engine.isKeyAvailable(subject)).isTrue();

    heal();
    engine.deleteKey(subject);
    engine.deleteKey(subject);
    assertErased(subject, ciphertext);
  }

  /**
   * reinstate and the generation bump: a reinstate that dies keeps the tombstone; the rerun lifts
   * it, and the mint after it (a write of its own) uses the generation above the erased one, so the
   * pre-erasure value stays redacted. A mint that dies after the reinstate committed leaves the
   * subject reinstated without a key; its rerun mints the same next generation.
   */
  @Test
  void aReinstateThatDiesKeepsTheTombstone_andTheRerunAndTheReMintConverge() throws Exception {
    var subject = SubjectId.of("reinstate");
    byte[] old = engine.encrypt(subject, PLAINTEXT);
    engine.deleteKey(subject);
    failBefore("DELETE ON forgotten_subjects");

    assertThatThrownBy(() -> engine.reinstate(subject))
        .isExactlyInstanceOf(CryptoOperationException.class);
    assertThatThrownBy(() -> engine.encrypt(subject, PLAINTEXT))
        .isExactlyInstanceOf(SubjectForgottenException.class);

    heal();
    engine.reinstate(subject);
    failBefore("INSERT ON encryption_keys");
    assertThatThrownBy(() -> engine.encrypt(subject, PLAINTEXT))
        .isExactlyInstanceOf(CryptoOperationException.class);
    assertThat(count("encryption_keys")).isZero();

    heal();
    byte[] current = engine.encrypt(subject, PLAINTEXT);
    assertThat(current[1]).as("the generation above the erased one").isEqualTo((byte) 2);
    assertThatThrownBy(() -> engine.decrypt(subject, old))
        .isExactlyInstanceOf(KeyNotFoundException.class);
    assertThat(engine.decrypt(subject, current)).isEqualTo(PLAINTEXT);
  }

  /**
   * Forgotten-subject marking: a forget that dies writes nothing; the rerun writes the tombstone
   * with its evidence, and another rerun keeps the first instant and evidence.
   */
  @Test
  void aForgetThatDiesWritesNothing_andTheRerunsConverge() throws Exception {
    var subject = SubjectId.of("forget");
    Instant first = Instant.parse("2026-11-24T10:00:00.000001Z");
    Instant evidence = Instant.parse("2026-01-01T00:00:00Z");
    var store = new SqliteForgottenSubjectStore(database, Clock.fixed(first, ZoneOffset.UTC));
    failBefore("INSERT ON forgotten_subjects");

    assertThatThrownBy(() -> store.forget(subject, evidence))
        .isExactlyInstanceOf(CryptoOperationException.class);
    assertThat(store.isForgotten(subject)).isFalse();

    heal();
    store.forget(subject, evidence);
    new SqliteForgottenSubjectStore(database, Clock.fixed(first.plusSeconds(5), ZoneOffset.UTC))
        .forget(subject, evidence.plusSeconds(1));
    assertThat(store.forgottenAt(subject)).contains(first);
    assertThat(store.keyCreatedAt(subject)).contains(evidence);
  }

  /** Stands in for an OutOfMemoryError or a LinkageError thrown inside the transaction. */
  private static final class InjectedCrash extends Error {
    InjectedCrash(String where) {
      super("injected crash " + where);
    }
  }

  /**
   * deleteKey: an {@link Error} after the tombstone INSERT ran, before the {@code COMMIT}. The
   * erasure's statements all ran in ONE transaction on ONE connection, which rolls back: the key
   * stays, no generation row or tombstone remains, and the old value still reads. The rerun is
   * again one transaction, commits, and converges on the erased state. A tombstone (or any write of
   * the erasure) sent through another connection or transaction fails the transcript comparison,
   * and one committed on its own would survive the rollback.
   */
  @Test
  void errorAfterTheTombstoneInsert_commitsNothing_andARerunConverges() throws Exception {
    var recording =
        new RecordingDataSource(SqliteFiles.dataSource(dir.resolve("crash-after-tombstone.db")));
    try (SqliteDatabase recorded = SqliteFiles.migrated(recording)) {
      var erasing = SqliteCryptoEngine.builder().database(recorded).build();
      var store = new SqliteForgottenSubjectStore(recorded);
      var subject = SubjectId.of("erase-after-tombstone");
      byte[] ciphertext = erasing.encrypt(subject, PLAINTEXT);
      byte[] keyBefore = keyBytes(recording);
      var crash = new InjectedCrash("after the tombstone INSERT");
      recording.failAfter("INSERT INTO forgotten_subjects", crash);
      recording.resetRecording();

      assertThatThrownBy(() -> erasing.deleteKey(subject)).isSameAs(crash);

      assertThat(recording.transcript())
          .isEqualTo(writeTransactionOn(1, "ROLLBACK", ERASE_STATEMENTS));
      assertThat(keyBytes(recording)).as("the key survives").isEqualTo(keyBefore);
      assertThat(count(recording, "forgotten_subjects")).as("no tombstone").isZero();
      assertThat(store.isForgotten(subject)).as("the store sees no tombstone either").isFalse();
      assertThat(count(recording, "erased_key_generations")).as("no generation row").isZero();
      assertThat(erasing.decrypt(subject, ciphertext)).isEqualTo(PLAINTEXT);
      recording.resetRecording();

      erasing.deleteKey(subject);

      assertThat(recording.transcript())
          .isEqualTo(writeTransactionOn(1, "COMMIT", ERASE_STATEMENTS));
      assertThat(erasing.isKeyAvailable(subject)).as("the rerun deletes the key").isFalse();
      assertThat(store.isForgotten(subject)).as("the rerun writes the tombstone").isTrue();
      assertThat(
              SqliteFiles.query(recording, "SELECT max_erased_version FROM erased_key_generations"))
          .as("the rerun records generation 1")
          .containsExactly(List.of(1));
      assertThatThrownBy(() -> erasing.decrypt(subject, ciphertext))
          .isExactlyInstanceOf(KeyNotFoundException.class);
      assertThatThrownBy(() -> erasing.encrypt(subject, PLAINTEXT))
          .isExactlyInstanceOf(SubjectForgottenException.class);
    }
  }

  /** The statements of an erasure of a subject with a key, in order (default key table). */
  private static final String[] ERASE_STATEMENTS = {
    "SELECT key_version FROM encryption_keys WHERE subject_id = ?",
    "INSERT INTO erased_key_generations (subject_id, max_erased_version) VALUES (?, ?)"
        + " ON CONFLICT (subject_id) DO UPDATE SET max_erased_version ="
        + " max(erased_key_generations.max_erased_version, excluded.max_erased_version)",
    "DELETE FROM encryption_keys WHERE subject_id = ?",
    "INSERT INTO forgotten_subjects (subject_id, forgotten_at) VALUES (?, ?)"
        + " ON CONFLICT (subject_id) DO NOTHING"
  };

  private static byte[] keyBytes(DataSource source) throws Exception {
    List<List<Object>> rows = SqliteFiles.query(source, "SELECT key_bytes FROM encryption_keys");
    assertThat(rows).hasSize(1);
    return (byte[]) rows.getFirst().getFirst();
  }

  private static int count(DataSource source, String table) throws Exception {
    return ((Number)
            SqliteFiles.query(source, "SELECT count(*) FROM " + table).getFirst().getFirst())
        .intValue();
  }

  private void assertErased(SubjectId subject, byte[] ciphertext) throws Exception {
    assertThat(engine.isKeyAvailable(subject)).isFalse();
    assertThatThrownBy(() -> engine.decrypt(subject, ciphertext))
        .isExactlyInstanceOf(KeyNotFoundException.class);
    assertThatThrownBy(() -> engine.encrypt(subject, PLAINTEXT))
        .isExactlyInstanceOf(SubjectForgottenException.class);
    assertThat(
            SqliteFiles.query(dataSource, "SELECT max_erased_version FROM erased_key_generations"))
        .containsExactly(List.of(1));
    assertThat(count("forgotten_subjects")).isOne();
  }

  /** From now on the statement {@code event} (for example {@code INSERT ON t}) fails. */
  private void failBefore(String event) throws Exception {
    SqliteFiles.execute(
        dataSource,
        "CREATE TRIGGER injected_fault BEFORE "
            + event
            + " BEGIN SELECT RAISE(ABORT, 'injected fault'); END");
  }

  private void heal() throws Exception {
    SqliteFiles.execute(dataSource, "DROP TRIGGER injected_fault");
  }

  private int count(String table) throws Exception {
    return ((Number)
            SqliteFiles.query(dataSource, "SELECT count(*) FROM " + table).getFirst().getFirst())
        .intValue();
  }
}
