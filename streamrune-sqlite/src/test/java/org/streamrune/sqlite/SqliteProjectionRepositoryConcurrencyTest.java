package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * Concurrent first use and concurrent batches: repositories on several handles of one file, as
 * several components of one application would hold them, racing on a CyclicBarrier.
 */
class SqliteProjectionRepositoryConcurrencyTest {

  private static final int REPLICAS = 6;

  record Counter(String id, int count) {
    @JsonCreator
    Counter(@JsonProperty("id") String id, @JsonProperty("count") int count) {
      this.id = id;
      this.count = count;
    }
  }

  @TempDir Path dir;

  private Path file;
  private final List<SqliteDatabase> handles = new ArrayList<>();
  private ExecutorService pool;

  @BeforeEach
  void open() {
    file = dir.resolve("concurrent.db");
    handle().migrate(SqliteSchemaSeries.READ_MODEL);
    pool = Executors.newFixedThreadPool(REPLICAS);
  }

  @AfterEach
  void close() {
    pool.shutdownNow();
    handles.forEach(SqliteDatabase::close);
  }

  private SqliteDatabase handle() {
    SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(file));
    handles.add(database);
    return database;
  }

  private SqliteProjectionRepository repository() {
    SqliteDatabase database = handle();
    return new SqliteProjectionRepository(
        database,
        SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
        new SqliteReadModelCheckpoints(database));
  }

  private int views(String table) throws SQLException {
    try (Connection conn = TestDatabases.dataSource(file).getConnection();
        Statement st = conn.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = '"
                    + table
                    + "'")) {
      rs.next();
      return rs.getInt(1);
    }
  }

  /** Repositories reading a new projection at once all succeed, and one table exists. */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void replicasReadingANewProjectionAtOnceAllSucceedAndTheTableExists() throws Exception {
    for (int round = 0; round < 3; round++) {
      var name = ProjectionName.of("first_use_" + round);
      var barrier = new CyclicBarrier(REPLICAS);
      var results = new ArrayList<Future<Integer>>();
      for (int i = 0; i < REPLICAS; i++) {
        var repo = repository();
        results.add(
            pool.submit(
                () -> {
                  barrier.await(30, TimeUnit.SECONDS);
                  return repo.findAll(name, Counter.class).size();
                }));
      }
      for (Future<Integer> result : results) {
        assertThat(result.get(60, TimeUnit.SECONDS)).isZero();
      }
      assertThat(views(name.value() + "_view")).isEqualTo(1);
    }
  }

  /**
   * A read whose table another connection is creating, uncommitted, waits for that creation's write
   * lock and succeeds once it commits; it never fails on a half-created table.
   */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aReadThatWaitsOnAnotherConnectionsCreationSucceedsOnceItCommits() throws Exception {
    var name = ProjectionName.of("waits_on_creation");
    var repo = repository();
    try (Connection other = TestDatabases.dataSource(file).getConnection();
        Statement st = other.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      st.execute(
          "CREATE TABLE waits_on_creation_view (id TEXT NOT NULL PRIMARY KEY, data TEXT NOT NULL,"
              + " version INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL) STRICT");
      st.execute(
          "INSERT INTO waits_on_creation_view VALUES ('c', '{\"id\":\"c\",\"count\":1}', 0, 0)");
      Future<List<Counter>> read = pool.submit(() -> repo.findAll(name, Counter.class));

      assertThat(waits(read)).as("the read waits for the creating transaction").isTrue();
      st.execute("COMMIT");

      assertThat(read.get(30, TimeUnit.SECONDS)).containsExactly(new Counter("c", 1));
    }
  }

  /** A batch whose table another connection is creating commits once that creation commits. */
  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aBatchThatWaitsOnAnotherConnectionsCreationCommitsOnceItCommits() throws Exception {
    var name = ProjectionName.of("batch_waits");
    var repo = repository();
    try (Connection other = TestDatabases.dataSource(file).getConnection();
        Statement st = other.createStatement()) {
      st.execute("BEGIN IMMEDIATE");
      st.execute(
          "CREATE TABLE batch_waits_view (id TEXT NOT NULL PRIMARY KEY, data TEXT NOT NULL,"
              + " version INTEGER NOT NULL DEFAULT 0, updated_at INTEGER NOT NULL) STRICT");
      Future<?> batch =
          pool.submit(
              () ->
                  repo.executeAtomically(
                      name,
                      List.of(),
                      GlobalOffset.of(1),
                      0L,
                      tx -> tx.save(name, "c", new Counter("c", 1)),
                      null));

      assertThat(waits(batch)).as("the batch waits for the write lock").isTrue();
      st.execute("COMMIT");

      batch.get(30, TimeUnit.SECONDS);
      assertThat(repo.findById(name, "c", Counter.class)).contains(new Counter("c", 1));
    }
  }

  /**
   * Two writers applying the same batch of one projection at once: exactly one commits, the other
   * is refused by a guard, and the non-idempotent read model is applied once. Several rounds.
   */
  @Test
  @Timeout(value = 120, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void twoWritersOfTheSameBatchApplyItExactlyOnce() throws Exception {
    var name = ProjectionName.of("double_writer");
    var first = repository();
    var second = repository();
    for (int round = 1; round <= 5; round++) {
      long offset = round;
      var barrier = new CyclicBarrier(2);
      var outcomes = new ArrayList<Future<String>>();
      for (var repo : List.of(first, second)) {
        outcomes.add(
            pool.submit(
                () -> {
                  barrier.await(30, TimeUnit.SECONDS);
                  try {
                    repo.executeAtomically(
                        name,
                        SqliteProjectionRepositoryCrashTest.Batches.of(offset, offset),
                        GlobalOffset.of(offset),
                        0L,
                        tx -> {
                          int count =
                              tx.findById(name, "c", Counter.class).map(Counter::count).orElse(0);
                          tx.save(name, "c", new Counter("c", count + 1));
                        },
                        null);
                    return "committed";
                  } catch (ProjectionCommitFencedException e) {
                    return e.guard().name();
                  }
                }));
      }
      var results = new ArrayList<String>();
      for (Future<String> outcome : outcomes) {
        results.add(outcome.get(60, TimeUnit.SECONDS));
      }
      assertThat(results).containsExactlyInAnyOrder("committed", "OVERLAP");
      assertThat(first.findById(name, "c", Counter.class))
          .as("round %d", round)
          .contains(new Counter("c", round));
    }
  }

  /** Whether {@code task} is still running after half a second. */
  private static boolean waits(Future<?> task) throws Exception {
    try {
      task.get(500, TimeUnit.MILLISECONDS);
      return false;
    } catch (TimeoutException stillWaiting) {
      return true;
    }
  }
}
