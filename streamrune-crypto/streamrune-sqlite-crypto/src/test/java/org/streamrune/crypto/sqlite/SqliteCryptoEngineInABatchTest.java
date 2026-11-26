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
import org.streamrune.core.crypto.SubjectForgottenException;
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
 * every value encrypted under it, and a failed statement aborts the batch. An erasure never joins:
 * inside a batch it is refused at once. An engine on another file keeps its own transactions, and
 * inside an application transaction of its own on the file the engine cannot mint and says why.
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

  /**
   * An erasure never joins a batch: inside one on the same file, deleteKey and reinstate are
   * refused at once, before anything is written, and the batch itself can still commit. Joined, the
   * erasure would not be durable when it returns: a rollback would bring the key back.
   */
  @Test
  void theEnginesErasureAndReinstateAreRefusedInsideABatch() {
    open(impatient("erase.db"));
    SubjectId kept = SubjectId.of("kept");
    SubjectId stillForgotten = SubjectId.of("still-forgotten");
    byte[] value = engine.encrypt(kept, "pii".getBytes(UTF_8));
    engine.deleteKey(stillForgotten);
    var refusals = new java.util.ArrayList<Throwable>();

    batch(
        1,
        tx -> {
          refusals.add(catchRefusal(() -> engine.deleteKey(kept)));
          refusals.add(catchRefusal(() -> engine.reinstate(stillForgotten)));
          tx.save(P, "c-0", new CustomerView("c-0", "zoe@example.com"));
        });

    assertThat(refusals).hasSize(2).allSatisfy(SqliteCryptoEngineInABatchTest::isTheRefusal);
    assertThat(engine.decrypt(kept, value)).isEqualTo("pii".getBytes(UTF_8));
    assertThatThrownBy(() -> engine.encrypt(stillForgotten, "x".getBytes(UTF_8)))
        .isInstanceOf(SubjectForgottenException.class);
    assertThat(checkpoints.getLastOffset(P))
        .as("the batch committed")
        .isEqualTo(GlobalOffset.of(1));
    engine.deleteKey(kept);
    assertThat(engine.isKeyAvailable(kept)).as("after the batch the erasure runs").isFalse();
  }

  /**
   * The forgotten-subject store's tombstone writes are refused inside a batch too; its reads join
   * the batch.
   */
  @Test
  void theStoresTombstoneWritesAreRefusedInsideABatch_itsReadsJoin() {
    open(impatient("store.db"));
    var store = new SqliteForgottenSubjectStore(database);
    SubjectId forgotten = SubjectId.of("forgotten");
    SubjectId fresh = SubjectId.of("fresh");
    store.forget(forgotten);
    var refusals = new java.util.ArrayList<Throwable>();
    var read = new AtomicReference<Boolean>();

    batch(
        1,
        tx -> {
          refusals.add(catchRefusal(() -> store.forget(fresh)));
          refusals.add(catchRefusal(() -> store.forget(fresh, java.time.Instant.EPOCH)));
          refusals.add(catchRefusal(() -> store.reinstate(forgotten)));
          read.set(store.isForgotten(forgotten));
        });

    assertThat(refusals).hasSize(3).allSatisfy(SqliteCryptoEngineInABatchTest::isTheRefusal);
    assertThat(read.get()).isTrue();
    assertThat(store.isForgotten(fresh)).isFalse();
    assertThat(store.isForgotten(forgotten)).isTrue();
  }

  /**
   * Through CachedCryptoEngine, as the integrations wire it: the refused erasure publishes no
   * forget, so no other engine evicts or acts on an erasure that never happened, and the value
   * stays readable through the cache. (The cache drops its own entries of the subject before it
   * calls its delegate, whatever the delegate answers; that costs a cache miss, nothing else.)
   */
  @Test
  void aRefusedErasureThroughTheCachePublishesNoForget() {
    open(impatient("cached.db"));
    SubjectId subject = SubjectId.of("cached");
    var heard = new java.util.concurrent.CopyOnWriteArrayList<String>();
    try (var signal = new InProcessCryptoForgetSignal(database);
        var peerSignal = new InProcessCryptoForgetSignal(database)) {
      peerSignal.subscribe(heard::add);
      CryptoEngine cached =
          org.streamrune.crypto.CachedCryptoEngine.builder()
              .delegate(engine)
              .forgetSignal(signal)
              .build();
      byte[] value = cached.encrypt(subject, "pii".getBytes(UTF_8));
      cached.decrypt(subject, value);
      var refusal = new AtomicReference<Throwable>();

      batch(1, tx -> refusal.set(catchRefusal(() -> cached.deleteKey(subject))));

      isTheRefusal(refusal.get());
      assertThat(heard).as("no forget was published").isEmpty();
      assertThat(cached.decrypt(subject, value)).isEqualTo("pii".getBytes(UTF_8));
      assertThat(engine.isKeyAvailable(subject)).isTrue();
    }
  }

  /**
   * The Vault engine's ordering: record the tombstone, and only once that returned destroy the key
   * in the key backend. Inside a batch the tombstone write is refused, so the destroy step never
   * runs and the subject stays as it was: no key destroyed behind an uncommitted tombstone.
   */
  @Test
  void aVaultStyleErasureInsideABatchNeverReachesItsKeyDestroy() {
    open(impatient("vault.db"));
    var store = new SqliteForgottenSubjectStore(database);
    SubjectId subject = SubjectId.of("vault-subject");
    var transitKeyDestroyed = new java.util.concurrent.atomic.AtomicBoolean();
    var refusal = new AtomicReference<Throwable>();

    batch(
        1,
        tx ->
            refusal.set(
                catchRefusal(
                    () -> {
                      store.forget(subject, java.time.Instant.parse("2026-01-01T00:00:00Z"));
                      transitKeyDestroyed.set(true);
                    })));

    isTheRefusal(refusal.get());
    assertThat(transitKeyDestroyed).isFalse();
    assertThat(store.isForgotten(subject)).isFalse();
  }

  private static Throwable catchRefusal(Runnable erasure) {
    try {
      erasure.run();
      return null;
    } catch (RuntimeException e) {
      return e;
    }
  }

  private static void isTheRefusal(Throwable refusal) {
    assertThat(refusal)
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            "cannot run inside a projection batch on the same SQLite database file")
        .hasMessageContaining(
            "Run the erasure after the batch, from a command handler or a saga step");
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
