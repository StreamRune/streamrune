package org.streamrune.crypto.sqlite;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
