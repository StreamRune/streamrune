package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.zaxxer.hikari.HikariDataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.LockMode;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * A call on a {@link SqliteProjectionRepository} made by the thread that holds a batch or a replay
 * on the repository's file, while the updater runs, joins that transaction: it runs on the batch's
 * connection, commits and rolls back with the batch, and its failure aborts the batch. So a
 * projection that is handed no repository (an at-least-once registration routed to a SQLite store)
 * is applied in one transaction with its checkpoint, instead of waiting busy_timeout for the write
 * lock its own thread holds.
 *
 * <p>The connections give up on the write lock after {@value #BUSY_MILLIS} ms, and every test that
 * would have waited for it before is bounded well below that or asserts what the wait cannot
 * produce.
 */
class SqliteProjectionRepositoryJoinTest {

  static final int BUSY_MILLIS = 2_000;

  static final ProjectionName ORDERS = ProjectionName.of("orders");
  static final ProjectionName CUSTOMERS = ProjectionName.of("customers");

  record View(String id, String data) {
    @JsonCreator
    View(@JsonProperty("id") String id, @JsonProperty("data") String data) {
      this.id = id;
      this.data = data;
    }
  }

  /**
   * What the runners register as an at-least-once projection over the store's repository: its
   * helpers write through the captured repository, because the runner hands it {@code null}.
   */
  static final class Orders extends BaseProjection {
    private final RuntimeException failure;

    Orders(ProjectionRepository repository, RuntimeException failure) {
      super(repository, ORDERS.value());
      this.failure = failure;
    }

    @Override
    public void process(List<EventEnvelope> batch) {
      int seen = findById("count", View.class).map(v -> Integer.parseInt(v.data())).orElse(0);
      save("count", new View("count", Integer.toString(seen + 1)));
      if (failure != null) {
        throw failure;
      }
    }
  }

  @TempDir Path dir;

  private Path file;
  private SqliteDatabase database;
  private SqliteReadModelCheckpoints checkpoints;
  private SqliteProjectionRepository repo;
  private final List<AutoCloseable> closeables = new ArrayList<>();
  private final ExecutorService otherThread = Executors.newSingleThreadExecutor();

  @BeforeEach
  void open() {
    file = dir.resolve("join.db");
    database = SqliteDatabase.open(TestDatabases.dataSource(file, "busy_timeout=" + BUSY_MILLIS));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    checkpoints = new SqliteReadModelCheckpoints(database);
    repo = repository(database);
  }

  @AfterEach
  void close() throws Exception {
    otherThread.shutdownNow();
    for (AutoCloseable closeable : closeables) {
      closeable.close();
    }
    database.close();
  }

  private static SqliteProjectionRepository repository(SqliteDatabase database) {
    return new SqliteProjectionRepository(
        database,
        SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
        new SqliteReadModelCheckpoints(database));
  }

  private void batch(long offset, AtomicUpdater updater) {
    repo.executeAtomically(
        ORDERS, List.of(), GlobalOffset.of(offset), 0L, updater::update, checkpoints);
  }

  @FunctionalInterface
  interface AtomicUpdater {
    void update(ProjectionRepository handed);
  }

  /** The committed row, read on a connection of its own. */
  private Optional<String> committed(ProjectionName name, String id) throws SQLException {
    try (Connection conn = TestDatabases.dataSource(file).getConnection()) {
      try (PreparedStatement exists =
          conn.prepareStatement("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")) {
        exists.setString(1, name.value() + "_view");
        try (ResultSet rs = exists.executeQuery()) {
          if (!rs.next()) {
            return Optional.empty();
          }
        }
      }
      try (PreparedStatement ps =
          conn.prepareStatement("SELECT data FROM " + name.value() + "_view WHERE id = ?")) {
        ps.setString(1, id);
        try (ResultSet rs = ps.executeQuery()) {
          return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
        }
      }
    }
  }

  // ---- the at-least-once projection routed to the store ----

  /**
   * The runner's call for an at-least-once projection routed to this store: the processor's
   * transaction with the projection handed {@code null}. Its writes through the captured repository
   * commit with the checkpoint, at once.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void anAtLeastOnceProjectionOnTheCapturedRepositoryCommitsWithItsCheckpoint()
      throws SQLException {
    var projection = new Orders(repo, null);
    long start = System.nanoTime();

    batch(1, handed -> projection.process(List.of(), null));
    batch(2, handed -> projection.process(List.of(), null));

    assertThat((System.nanoTime() - start) / 1_000_000)
        .as("neither batch waited for its own write lock (busy_timeout is %d ms)", BUSY_MILLIS)
        .isLessThan(BUSY_MILLIS);
    assertThat(committed(ORDERS, "count")).contains("{\"id\":\"count\",\"data\":\"2\"}");
    assertThat(checkpoints.getLastOffset(ORDERS)).isEqualTo(GlobalOffset.of(2));
  }

  /**
   * Atomic with the checkpoint: a batch whose updater fails after such a write stores neither the
   * write nor the checkpoint, and the redelivered batch applies it once.
   */
  @Test
  void anAtLeastOnceProjectionsWritesRollBackWithTheBatch() throws SQLException {
    var boom = new IllegalStateException("the updater fails after its write");
    var failing = new Orders(repo, boom);

    assertThatThrownBy(() -> batch(1, handed -> failing.process(List.of(), null))).isSameAs(boom);

    assertThat(committed(ORDERS, "count")).isEmpty();
    assertThat(checkpoints.getLastOffset(ORDERS)).isEqualTo(GlobalOffset.initial());

    var projection = new Orders(repo, null);
    batch(1, handed -> projection.process(List.of(), null));
    assertThat(committed(ORDERS, "count")).contains("{\"id\":\"count\",\"data\":\"1\"}");
    assertThat(checkpoints.getLastOffset(ORDERS)).isEqualTo(GlobalOffset.of(1));
  }

  /** The same through a dead-letter replay, on a pool of one connection: no second one is used. */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aReplayOnAPoolOfOneJoinsTheCapturedRepositorysCalls() throws SQLException {
    database.close();
    HikariDataSource pool = TestDatabases.poolOfOne(file, "join-pool-of-one");
    closeables.add(pool);
    database = SqliteDatabase.open(pool);
    checkpoints = new SqliteReadModelCheckpoints(database);
    repo = repository(database);
    var projection = new Orders(repo, null);

    repo.executeReplay(ORDERS, List.of(), handed -> projection.process(List.of(), null));
    batch(1, handed -> projection.process(List.of(), null));

    assertThat(committed(ORDERS, "count")).contains("{\"id\":\"count\",\"data\":\"2\"}");
    assertThat(checkpoints.getLastOffset(ORDERS)).isEqualTo(GlobalOffset.of(1));
  }

  // ---- every operation joins ----

  /**
   * Every read and write of the outer repository, called inside the updater, runs on the batch's
   * connection: each sees what the handed repository wrote and has not committed, and the handed
   * repository sees theirs.
   */
  @Test
  void everyOperationOfTheOuterRepositorySeesTheBatchsUncommittedWrites() throws SQLException {
    var ran = new ArrayList<String>();

    batch(
        1,
        handed -> {
          handed.save(ORDERS, "a", new View("a", "handed"));
          repo.prepareReadModel(CUSTOMERS);
          repo.save(CUSTOMERS, "c", new View("c", "outer"));

          assertThat(repo.findById(ORDERS, "a", View.class)).contains(new View("a", "handed"));
          assertThat(handed.findById(CUSTOMERS, "c", View.class)).contains(new View("c", "outer"));
          assertThat(repo.findAll(ORDERS, View.class)).containsExactly(new View("a", "handed"));
          assertThat(repo.findAll(ORDERS, View.class, PageRequest.of(0, 5)).totalElements())
              .isEqualTo(1);
          assertThat(
                  repo.findAllBy(
                          ORDERS,
                          View.class,
                          FieldEquals.of("data", "handed"),
                          PageRequest.of(0, 5))
                      .content())
              .containsExactly(new View("a", "handed"));
          long version =
              repo.findById(ORDERS, "a", View.class, LockMode.OPTIMISTIC).orElseThrow().version();
          repo.save(ORDERS, "a", new View("a", "versioned"), version);
          assertThatThrownBy(() -> repo.save(ORDERS, "a", new View("a", "stale"), version))
              .isExactlyInstanceOf(OptimisticLockException.class);
          repo.save(ORDERS, "b", new View("b", "to delete"));
          repo.delete(ORDERS, "b");
          repo.afterCommit(() -> ran.add("after commit"));
          assertThat(ran).as("queued with the batch, not run at once").isEmpty();
          try {
            assertThat(committed(ORDERS, "a")).as("nothing is committed yet").isEmpty();
            assertThat(committed(CUSTOMERS, "c")).isEmpty();
          } catch (SQLException e) {
            throw new IllegalStateException(e);
          }
        });

    assertThat(ran).containsExactly("after commit");
    assertThat(committed(ORDERS, "a")).contains("{\"id\":\"a\",\"data\":\"versioned\"}");
    assertThat(committed(ORDERS, "b")).isEmpty();
    assertThat(committed(CUSTOMERS, "c")).contains("{\"id\":\"c\",\"data\":\"outer\"}");
    // The table the joined call created inside the batch is remembered once it committed.
    assertThat(repo.findAll(CUSTOMERS, View.class)).containsExactly(new View("c", "outer"));
  }

  /** An after-commit action registered through the outer repository is dropped on a rollback. */
  @Test
  void anAfterCommitActionOfAJoinedCallIsDiscardedWhenTheBatchRollsBack() {
    var ran = new ArrayList<String>();
    var boom = new IllegalStateException("rolled back");

    assertThatThrownBy(
            () ->
                batch(
                    1,
                    handed -> {
                      repo.afterCommit(() -> ran.add("after commit"));
                      throw boom;
                    }))
        .isSameAs(boom);

    assertThat(ran).isEmpty();
    // Outside a batch a write through the repository has committed when it returns: at once.
    repo.afterCommit(() -> ran.add("at once"));
    assertThat(ran).containsExactly("at once");
  }

  /** An index is declared at wiring: inside a batch the outer repository refuses it at once. */
  @Test
  void declareIndexInsideABatchIsRefusedAtOnce() {
    var index = ProjectionIndex.forFilter(ORDERS, "data", Sort.unsorted());

    assertThatThrownBy(() -> batch(1, handed -> repo.declareIndex(index)))
        .isExactlyInstanceOf(IllegalStateException.class)
        .hasMessageContaining("declare indexes at wiring");
    assertThatThrownBy(() -> batch(1, handed -> repo.declareIndex(null)))
        .isExactlyInstanceOf(IllegalArgumentException.class);

    repo.declareIndex(index);
  }

  /** A joined call validates its arguments as the call outside a batch does. */
  @Test
  void aJoinedCallRefusesBadArgumentsWithoutAbortingTheBatch() throws SQLException {
    batch(
        1,
        handed -> {
          assertThatThrownBy(() -> repo.save(ORDERS, " ", new View("x", "y")))
              .isExactlyInstanceOf(IllegalArgumentException.class);
          assertThatThrownBy(() -> repo.findById(null, "a", View.class))
              .isExactlyInstanceOf(IllegalArgumentException.class);
          assertThatThrownBy(() -> repo.prepareReadModel(null))
              .isExactlyInstanceOf(IllegalArgumentException.class);
          repo.save(ORDERS, "a", new View("a", "kept"));
        });

    assertThat(committed(ORDERS, "a")).isPresent();
  }

  // ---- a failed joined statement poisons the batch ----

  /**
   * The first failed statement of a joined call aborts the batch, even when the updater swallows
   * it: every later call, joined or through the handed repository, runs no statement, and the batch
   * throws and stores nothing.
   */
  @Test
  void aFailedJoinedStatementAbortsTheBatchForEveryView() throws SQLException {
    String tooLong = "x".repeat(256);
    var caught = new ArrayList<RuntimeException>();

    assertThatThrownBy(
            () ->
                batch(
                    1,
                    handed -> {
                      handed.save(ORDERS, "a", new View("a", "before the failure"));
                      try {
                        repo.save(ORDERS, tooLong, new View(tooLong, "refused by the CHECK"));
                      } catch (EventStoreException e) {
                        caught.add(e);
                      }
                      try {
                        handed.save(ORDERS, "b", new View("b", "after the failure"));
                      } catch (EventStoreException e) {
                        caught.add(e);
                      }
                      try {
                        repo.findAll(ORDERS, View.class);
                      } catch (EventStoreException e) {
                        caught.add(e);
                      }
                      try {
                        repo.prepareReadModel(CUSTOMERS);
                      } catch (EventStoreException e) {
                        caught.add(e);
                      }
                    }))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageStartingWith(
            "The projection transaction was aborted by an earlier failure, so it runs no further"
                + " statement and rolls back: Failed to save projection: orders/");

    assertThat(caught).hasSize(4);
    assertThat(caught.getFirst()).hasMessageStartingWith("Failed to save projection: orders/");
    for (RuntimeException later : caught.subList(1, 4)) {
      assertThat(later)
          .hasMessageStartingWith("The projection transaction was aborted by an earlier failure")
          .hasCause(caught.getFirst());
    }
    assertThat(committed(ORDERS, "a")).isEmpty();
    assertThat(committed(CUSTOMERS, "any")).isEmpty();
    assertThat(checkpoints.getLastOffset(ORDERS)).isEqualTo(GlobalOffset.initial());
  }

  /** A failure of the handed repository aborts the joined calls the same way. */
  @Test
  void aFailedStatementOfTheHandedRepositoryAbortsTheJoinedCalls() {
    String tooLong = "x".repeat(256);
    var caught = new ArrayList<RuntimeException>();

    assertThatThrownBy(
            () ->
                batch(
                    1,
                    handed -> {
                      try {
                        handed.save(ORDERS, tooLong, new View(tooLong, "refused"));
                      } catch (EventStoreException e) {
                        caught.add(e);
                      }
                      repo.save(ORDERS, "a", new View("a", "must not run"));
                    }))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageStartingWith("The projection transaction was aborted by an earlier failure")
        .hasCause(caught.getFirst());
  }

  // ---- what joins and what does not ----

  /**
   * Another repository of the same file, with a mapper of its own, joins too, and writes with its
   * own mapper: here one that wraps the document in its type's name.
   */
  @Test
  void anotherRepositoryOfTheSameFileJoinsWithItsOwnMapper() throws SQLException {
    var wrapping = new ObjectMapper().enable(SerializationFeature.WRAP_ROOT_VALUE);
    var other =
        new SqliteProjectionRepository(
            database, wrapping, new SqliteReadModelCheckpoints(database));
    var boom = new IllegalStateException("rolled back");

    assertThatThrownBy(
            () ->
                batch(
                    1,
                    handed -> {
                      other.save(CUSTOMERS, "c", new View("c", "other"));
                      throw boom;
                    }))
        .isSameAs(boom);
    assertThat(committed(CUSTOMERS, "c")).as("rolled back with the batch").isEmpty();

    batch(1, handed -> other.save(CUSTOMERS, "c", new View("c", "other")));
    assertThat(committed(CUSTOMERS, "c")).contains("{\"View\":{\"id\":\"c\",\"data\":\"other\"}}");
  }

  /**
   * A repository of another file does not join: its write commits at once on its own file, whatever
   * becomes of the batch.
   */
  @Test
  void aRepositoryOfAnotherFileDoesNotJoin() throws SQLException {
    SqliteDatabase elsewhere =
        SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("elsewhere.db")));
    closeables.add(elsewhere);
    elsewhere.migrate(SqliteSchemaSeries.READ_MODEL);
    var other = repository(elsewhere);
    var boom = new IllegalStateException("rolled back");

    assertThatThrownBy(
            () ->
                batch(
                    1,
                    handed -> {
                      other.save(CUSTOMERS, "c", new View("c", "elsewhere"));
                      throw boom;
                    }))
        .isSameAs(boom);

    assertThat(other.findById(CUSTOMERS, "c", View.class)).contains(new View("c", "elsewhere"));
  }

  /**
   * Only the updater's thread joins. A thread the updater started works on a connection of its own:
   * its read does not see the batch's uncommitted write, and its write waits for the batch's write
   * lock and fails after busy_timeout with the lock-failure message.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aThreadTheUpdaterStartedDoesNotJoin() {
    repo.prepareReadModel(ORDERS);

    batch(
        1,
        handed -> {
          handed.save(ORDERS, "a", new View("a", "uncommitted"));
          try {
            assertThat(
                    otherThread
                        .submit(() -> repo.findById(ORDERS, "a", View.class))
                        .get(30, TimeUnit.SECONDS))
                .isEmpty();
            assertThatThrownBy(
                    () ->
                        otherThread
                            .submit(() -> repo.save(ORDERS, "b", new View("b", "waits")))
                            .get(30, TimeUnit.SECONDS))
                .hasCauseExactlyInstanceOf(EventStoreException.class)
                .hasMessageContaining("save of projection orders/b stored nothing")
                .hasMessageNotContaining("own projection batch");
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        });

    assertThat(repo.findAll(ORDERS, View.class)).containsExactly(new View("a", "uncommitted"));
  }

  // ---- what cannot join says why it failed ----

  /**
   * A write from inside the updater that cannot join (a nested batch, a checkpoint reset, any other
   * store of the file) waits busy_timeout for the lock its own thread holds. Its failure says so,
   * instead of blaming an application transaction; the batch itself is unharmed.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aWriteThatCannotJoinSaysThatItsOwnBatchHoldsTheLock() throws SQLException {
    var failures = new ArrayList<RuntimeException>();

    batch(
        1,
        handed -> {
          handed.save(ORDERS, "a", new View("a", "kept"));
          try {
            repo.resetOffset(CUSTOMERS);
          } catch (EventStoreException e) {
            failures.add(e);
          }
          try {
            repo.executeAtomically(
                CUSTOMERS, List.of(), GlobalOffset.of(1), 0L, nested -> {}, checkpoints);
          } catch (EventStoreException e) {
            failures.add(e);
          }
          try (Connection own = database.dataSource().getConnection()) {
            SqliteTransactions.writeStatement(
                own,
                c -> {
                  try (var st = c.createStatement()) {
                    return st.executeUpdate(
                        "DELETE FROM readmodel_checkpoint WHERE projection_name = 'none'");
                  }
                });
          } catch (SQLException e) {
            failures.add(SqliteEventStore.lockFailure(e, "one-statement write"));
          }
        });

    assertThat(failures).hasSize(3);
    for (RuntimeException failure : failures) {
      assertThat(failure)
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageContaining("stored nothing. This thread's own projection batch")
          .hasMessageNotContaining("application transaction");
    }
    assertThat(failures.get(0)).hasMessageContaining("so the offset reset of projection customers");
    assertThat(failures.get(1)).hasMessageContaining("projection batch of projection customers");
    assertThat(failures.get(2))
        .hasMessage(SqliteEventStore.ownBatchMessage("one-statement write"))
        .cause()
        .isInstanceOf(SqliteTransactions.WriteLockHeldByOwnBatch.class);
    assertThat(committed(ORDERS, "a")).isPresent();
    assertThat(checkpoints.getLastOffset(ORDERS)).isEqualTo(GlobalOffset.of(1));
  }

  /** Once the updater has returned, the repository's calls are its own again. */
  @Test
  void afterTheBatchTheRepositoryWritesOnItsOwnAgain() throws SQLException {
    batch(1, handed -> repo.save(ORDERS, "a", new View("a", "joined")));

    repo.save(ORDERS, "b", new View("b", "alone"));

    assertThat(committed(ORDERS, "a")).isPresent();
    assertThat(committed(ORDERS, "b")).isPresent();
  }
}
