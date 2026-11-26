package org.streamrune.crypto.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.sqlite.SqliteDatabase;

/**
 * What the contract leaves to the SQLite store: the stored form (hashed subject id, microsecond
 * timestamps stamped with the store's clock), and that a storage failure throws, with only the
 * subject's hash in the message, never answering "not forgotten" or "no evidence".
 */
class SqliteForgottenSubjectStoreTest {

  private static final Instant NOW = Instant.parse("2026-11-24T10:15:30.123456Z");

  @TempDir Path dir;

  private SqliteFiles.Switchable dataSource;
  private SqliteDatabase database;

  @BeforeEach
  void open() {
    dataSource = new SqliteFiles.Switchable(SqliteFiles.dataSource(dir.resolve("forgotten.db")));
    database = SqliteFiles.migrated(dataSource);
  }

  @AfterEach
  void close() {
    database.close();
  }

  @Test
  void theTombstoneHoldsTheSubjectsHash_andTheClocksInstantInMicroseconds() throws Exception {
    var store = new SqliteForgottenSubjectStore(database, Clock.fixed(NOW, ZoneOffset.UTC));
    var subject = SubjectId.of("hash-me@example.com");
    Instant evidence = Instant.parse("2026-01-01T00:00:00.000001Z");

    store.forget(subject, evidence);

    assertThat(
            SqliteFiles.query(
                dataSource,
                "SELECT subject_id, forgotten_at, key_created_at FROM forgotten_subjects"))
        .containsExactly(
            List.of(
                subject.redacted(),
                NOW.getEpochSecond() * 1_000_000L + 123_456L,
                1_767_225_600_000_001L));
    assertThat(store.forgottenAt(subject)).contains(NOW);
    assertThat(store.keyCreatedAt(subject)).contains(evidence);
  }

  /**
   * A read failure of the key-creation evidence throws: a reinstate must never take a storage
   * failure for "no evidence". Here the table is gone after the store was built.
   */
  @Test
  void aFailedReadOfTheEvidenceThrows_neverAnswersEmpty() throws Exception {
    var store = new SqliteForgottenSubjectStore(database);
    var subject = SubjectId.of("evidence-leak@example.com");
    store.forget(subject, NOW);
    assertThat(store.keyCreatedAt(subject)).contains(NOW);

    SqliteFiles.execute(dataSource, "DROP TABLE forgotten_subjects");

    assertRedactedFailure(() -> store.keyCreatedAt(subject), subject);
    assertRedactedFailure(() -> store.forgottenAt(subject), subject);
    assertRedactedFailure(() -> store.isForgotten(subject), subject);
  }

  /** Every method fails with the subject's hash alone when no connection can be borrowed. */
  @Test
  void anUnreachableDatabaseFailsEveryMethod_withTheSubjectsHashOnly() {
    var store = new SqliteForgottenSubjectStore(database);
    var subject = SubjectId.of("leak-me@example.com");
    dataSource.fail();

    List<ThrowingCallable> calls =
        List.of(
            () -> store.forget(subject),
            () -> store.forget(subject, NOW),
            () -> store.isForgotten(subject),
            () -> store.reinstate(subject),
            () -> store.forgottenAt(subject),
            () -> store.keyCreatedAt(subject));
    for (ThrowingCallable call : calls) {
      assertRedactedFailure(call, subject);
    }
  }

  @Test
  void theStoreKeepsWorkingAfterTheHandleIsClosed_andNeedsTheSchema() {
    var store = new SqliteForgottenSubjectStore(database);
    var subject = SubjectId.of("after-close");
    database.close();

    store.forget(subject);

    assertThat(store.isForgotten(subject)).isTrue();
    assertThat(store.keyCreatedAt(subject)).isEqualTo(Optional.empty());
    assertThat(store.requiresDatabaseSchema()).isTrue();
  }

  @Test
  void nullArgumentsAreRefused() {
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SqliteForgottenSubjectStore(null))
        .withMessage("database must not be null");
    assertThatIllegalArgumentException()
        .isThrownBy(() -> new SqliteForgottenSubjectStore(database, null))
        .withMessage("clock must not be null");
    var store = new SqliteForgottenSubjectStore(database);
    assertThatIllegalArgumentException().isThrownBy(() -> store.forget(null, NOW));
    assertThatIllegalArgumentException().isThrownBy(() -> store.keyCreatedAt(null));
  }

  private static void assertRedactedFailure(ThrowingCallable call, SubjectId subject) {
    assertThatThrownBy(call)
        .isExactlyInstanceOf(CryptoOperationException.class)
        .hasMessageContaining(subject.redacted())
        .hasMessageNotContaining(subject.value());
  }
}
