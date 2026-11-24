package org.streamrune.crypto.sqlite;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.projection.AtomicBatchProcessor.ProjectionUpdater;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubjectId;
import org.streamrune.sqlite.SqliteDatabase;
import org.streamrune.sqlite.SqliteProjectionRepository;
import org.streamrune.sqlite.SqliteReadModelCheckpoints;
import org.streamrune.sqlite.SqliteSchemaSeries;

/**
 * A projection batch holds the file's write lock while its updater serializes read models, so the
 * first {@code @Encrypted} value of a subject without a key would mint that key while the batch's
 * own thread holds the lock. The engine on the same file joins the batch's transaction instead of
 * waiting {@code busy_timeout}: the key commits with the batch, rolls back with it together with
 * every value encrypted under it, and a failed statement aborts the batch. An engine on another
 * file keeps its own transactions, and inside an application transaction of its own on the file the
 * engine cannot mint and says why.
 */
class SqliteCryptoEngineInABatchTest {

  private static final ProjectionName P = ProjectionName.of("customers");

  /** A read model carrying one {@code @Encrypted} PII field keyed off {@code customerId}. */
  record CustomerView(String customerId, @Encrypted(subjectId = "customerId") String email) {
    @JsonCreator
    CustomerView(
        @JsonProperty("customerId") String customerId, @JsonProperty("email") String email) {
      this.customerId = customerId;
      this.email = email;
    }
  }

  @TempDir Path dir;

  private HikariDataSource pool;
  private SqliteDatabase database;
  private SqliteReadModelCheckpoints checkpoints;
  private CryptoEngine engine;
  private SqliteProjectionRepository repo;

  @AfterEach
  void close() {
    if (database != null) {
      database.close();
    }
    if (pool != null) {
      pool.close();
    }
  }

  /** The read model and the keys in one file, opened on {@code dataSource}. */
  private void open(DataSource dataSource) {
    database = SqliteDatabase.open(dataSource);
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    database.migrate(SqliteSchemaSeries.CRYPTO);
    checkpoints = new SqliteReadModelCheckpoints(database);
    engine = SqliteCryptoEngine.builder().database(database).build();
    repo = repositoryWith(engine);
  }

  private SqliteProjectionRepository repositoryWith(CryptoEngine cryptoEngine) {
    return new SqliteProjectionRepository(
        database,
        SqliteProjectionRepository.createObjectMapper(cryptoEngine, StreamRuneMetrics.NOOP),
        checkpoints);
  }

  private void batch(long offset, ProjectionUpdater updater) {
    repo.executeAtomically(P, List.of(), GlobalOffset.of(offset), 0L, updater, checkpoints);
  }

  /** busy_timeout short enough that a mint waiting for the batch's lock fails quickly. */
  private DataSource impatient(String name) {
    return SqliteFiles.dataSource(dir.resolve(name), "busy_timeout=300");
  }

  @Test
  void aKeyMintedInsideABatchCommitsWithIt() throws Exception {
    open(impatient("commit.db"));

    batch(1, tx -> tx.save(P, "c-1", new CustomerView("c-1", "carol@example.com")));

    assertThat(engine.isKeyAvailable(SubjectId.of("c-1"))).isTrue();
    assertThat(repo.findById(P, "c-1", CustomerView.class).orElseThrow().email())
        .isEqualTo("carol@example.com");
    assertThat(rawData("c-1")).doesNotContain("carol@example.com");
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(1));
  }

  /**
   * The batch rolls back: the key it minted goes with the read model that holds the only value
   * encrypted under it, so no ciphertext refers to a missing key and no key is orphaned. The rerun
   * mints a fresh key and commits.
   */
  @Test
  void aKeyMintedInsideABatchThatRollsBackIsGoneWithIt_andTheRerunConverges() throws Exception {
    open(impatient("rollback.db"));
    SubjectId subject = SubjectId.of("c-2");

    assertThatThrownBy(
            () ->
                batch(
                    1,
                    tx -> {
                      tx.save(P, "c-2", new CustomerView("c-2", "dave@example.com"));
                      assertThat(engine.isKeyAvailable(subject))
                          .as("the batch sees its own key")
                          .isTrue();
                      throw new IllegalStateException("the updater fails after the mint");
                    }))
        .isExactlyInstanceOf(IllegalStateException.class);

    assertThat(engine.isKeyAvailable(subject)).isFalse();
    assertThat(SqliteFiles.query(database.dataSource(), "SELECT count(*) FROM encryption_keys"))
        .containsExactly(List.of(0));
    assertThat(repo.findById(P, "c-2", CustomerView.class)).isEmpty();

    batch(1, tx -> tx.save(P, "c-2", new CustomerView("c-2", "dave@example.com")));
    assertThat(repo.findById(P, "c-2", CustomerView.class).orElseThrow().email())
        .isEqualTo("dave@example.com");
  }

  /**
   * On a pool of one connection the batch holds the only connection: the engine borrows none inside
   * it, for the mint, for an existing key, or for the decryption of what the batch wrote.
   */
  @Test
  void onAPoolOfOneTheEngineBorrowsNoConnectionInsideABatch() {
    pool = SqliteFiles.pool(dir.resolve("pool.db"), 1);
    open(pool);
    engine.encrypt(SubjectId.of("existing"), "warm".getBytes(UTF_8));

    batch(
        1,
        tx -> {
          tx.save(P, "fresh", new CustomerView("fresh", "erin@example.com"));
          tx.save(P, "existing", new CustomerView("existing", "frank@example.com"));
          assertThat(tx.findById(P, "fresh", CustomerView.class).orElseThrow().email())
              .isEqualTo("erin@example.com");
          assertThat(engine.isKeyAvailable(SubjectId.of("fresh"))).isTrue();
        });

    assertThat(repo.findById(P, "existing", CustomerView.class).orElseThrow().email())
        .isEqualTo("frank@example.com");
  }

  @Test
  void anErasureAndAReinstateInsideABatchCommitWithIt() {
    open(impatient("erase.db"));
    SubjectId erased = SubjectId.of("erased");
    SubjectId reinstated = SubjectId.of("reinstated");
    byte[] old = engine.encrypt(erased, "old".getBytes(UTF_8));
    engine.deleteKey(reinstated);

    batch(
        1,
        tx -> {
          engine.deleteKey(erased);
          engine.reinstate(reinstated);
        });

    assertThatThrownBy(() -> engine.decrypt(erased, old))
        .isInstanceOf(org.streamrune.core.crypto.KeyNotFoundException.class);
    assertThatThrownBy(() -> engine.encrypt(erased, "new".getBytes(UTF_8)))
        .isInstanceOf(org.streamrune.core.crypto.SubjectForgottenException.class);
    assertThat(engine.encrypt(reinstated, "back".getBytes(UTF_8))).isNotNull();
  }

  /**
   * An engine on another file is not part of the batch: its key commits on its own and stays when
   * the batch rolls back, as a key minted on PostgreSQL does.
   */
  @Test
  void anEngineOnAnotherFileKeepsItsOwnTransactions() {
    open(impatient("models.db"));
    try (SqliteDatabase keys = SqliteDatabase.open(impatient("keys.db"))) {
      keys.migrate(SqliteSchemaSeries.CRYPTO);
      CryptoEngine elsewhere = SqliteCryptoEngine.builder().database(keys).build();
      repo = repositoryWith(elsewhere);

      assertThatThrownBy(
              () ->
                  batch(
                      1,
                      tx -> {
                        tx.save(P, "c-3", new CustomerView("c-3", "gina@example.com"));
                        throw new IllegalStateException("rolled back");
                      }))
          .isExactlyInstanceOf(IllegalStateException.class);

      assertThat(elsewhere.isKeyAvailable(SubjectId.of("c-3"))).isTrue();
      assertThat(repo.findById(P, "c-3", CustomerView.class)).isEmpty();
    }
  }

  /**
   * A statement of the engine that fails inside the batch aborts it, as a failed statement of the
   * batch's own repository does: the updater that swallows the failure can neither write again nor
   * commit.
   */
  @Test
  void aFailedStatementOfTheEngineAbortsTheBatch() throws Exception {
    open(impatient("abort.db"));
    CryptoEngine broken =
        SqliteCryptoEngine.builder().database(database).tableName("no_such_keys").build();
    repo = repositoryWith(broken);
    var laterWrite = new AtomicReference<RuntimeException>();

    assertThatThrownBy(
            () ->
                batch(
                    1,
                    tx -> {
                      try {
                        tx.save(P, "c-4", new CustomerView("c-4", "hank@example.com"));
                      } catch (RuntimeException swallowed) {
                        // the updater goes on
                      }
                      try {
                        tx.save(P, "plain", Map.of("id", "plain"));
                      } catch (RuntimeException e) {
                        laterWrite.set(e);
                      }
                    }))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("aborted by an earlier failure");

    assertThat(laterWrite.get()).isInstanceOf(EventStoreException.class);
    assertThat(
            SqliteFiles.query(
                database.dataSource(),
                "SELECT count(*) FROM sqlite_schema WHERE name = 'customers_view'"))
        .as("the table the batch created rolled back with it")
        .containsExactly(List.of(0));
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.initial());
  }

  /**
   * Another thread's first encryption of the subject the open batch minted for waits for the
   * batch's commit and then uses the batch's key: one key, every value readable.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aConcurrentMintOfTheSameSubjectWaitsForTheBatchAndUsesItsKey() throws Exception {
    open(SqliteFiles.dataSource(dir.resolve("concurrent.db")));
    SubjectId subject = SubjectId.of("c-5");
    var other = new AtomicReference<byte[]>();
    var otherFailure = new AtomicReference<Throwable>();
    var otherThread = new AtomicReference<Thread>();

    batch(
        1,
        tx -> {
          tx.save(P, "c-5", new CustomerView("c-5", "ivy@example.com"));
          otherThread.set(
              Thread.ofPlatform()
                  .start(
                      () -> {
                        try {
                          other.set(engine.encrypt(subject, "other".getBytes(UTF_8)));
                        } catch (Throwable t) {
                          otherFailure.set(t);
                        }
                      }));
          try {
            otherThread.get().join(300);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          assertThat(other.get()).as("the other mint waits for the batch's lock").isNull();
        });
    otherThread.get().join();

    assertThat(otherFailure.get()).isNull();
    assertThat(SqliteFiles.query(database.dataSource(), "SELECT count(*) FROM encryption_keys"))
        .containsExactly(List.of(1));
    assertThat(engine.decrypt(subject, other.get())).isEqualTo("other".getBytes(UTF_8));
    assertThat(repo.findById(P, "c-5", CustomerView.class).orElseThrow().email())
        .isEqualTo("ivy@example.com");
  }

  /**
   * Inside a transaction of the application's own on the same file, the engine cannot see the
   * transaction: the mint waits busy_timeout for the lock the thread holds itself, and the failure
   * says why.
   */
  @Test
  void anApplicationTransactionOnTheSameFileCannotMint_andTheMessageSaysWhy() throws Exception {
    open(impatient("application.db"));
    try (Connection application = database.dataSource().getConnection();
        Statement statement = application.createStatement()) {
      statement.execute("BEGIN IMMEDIATE");
      try {
        assertThatThrownBy(() -> engine.encrypt(SubjectId.of("c-6"), "pii".getBytes(UTF_8)))
            .isExactlyInstanceOf(CryptoOperationException.class)
            .hasMessageContaining("busy_timeout")
            .hasMessageContaining("this thread holds it itself");
      } finally {
        statement.execute("ROLLBACK");
      }
    }
  }

  private String rawData(String id) throws Exception {
    return (String)
        SqliteFiles.query(
                database.dataSource(), "SELECT data FROM customers_view WHERE id = '" + id + "'")
            .getFirst()
            .getFirst();
  }
}
