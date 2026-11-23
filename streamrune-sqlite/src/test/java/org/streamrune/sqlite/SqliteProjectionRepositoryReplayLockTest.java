package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
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
import org.streamrune.core.EventStoreException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * SQLite's write lock covers the whole database file, so the repository's lock is not per
 * projection: a replay of projection B waits for an open batch of projection A, runs once A commits
 * within B's {@code busy_timeout}, and fails with the lock-failure message when A holds the lock
 * longer. The shared contract skips its per-projection case for SQLite ({@code locksPerProjection()
 * == false}); this pins what happens instead. The waits are asserted as "not finished yet" while A
 * is provably still open, never as wall-clock bounds.
 */
class SqliteProjectionRepositoryReplayLockTest {

  private static final ProjectionName A = ProjectionName.of("open_batch");
  private static final ProjectionName B = ProjectionName.of("replayed");

  record Row(String id, String value) {
    @JsonCreator
    Row(@JsonProperty("id") String id, @JsonProperty("value") String value) {
      this.id = id;
      this.value = value;
    }
  }

  @TempDir Path dir;

  private Path file;
  private SqliteDatabase database;
  private SqliteProjectionRepository repo;
  private ExecutorService threads;
  private final CountDownLatch batchIsOpen = new CountDownLatch(1);
  private final CountDownLatch release = new CountDownLatch(1);

  @BeforeEach
  void open() {
    file = dir.resolve("replay-lock.db");
    database = SqliteDatabase.open(TestDatabases.dataSource(file));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    repo = repository(database);
    repo.prepareReadModel(A);
    repo.prepareReadModel(B);
    threads = Executors.newFixedThreadPool(2);
  }

  @AfterEach
  void close() throws InterruptedException {
    release.countDown();
    threads.shutdown();
    threads.awaitTermination(30, TimeUnit.SECONDS);
    database.close();
  }

  private static SqliteProjectionRepository repository(SqliteDatabase database) {
    return new SqliteProjectionRepository(
        database,
        SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
        new SqliteReadModelCheckpoints(database));
  }

  /** Opens a batch of A that holds SQLite's write lock until {@link #release} counts down. */
  private Future<?> openBatchOfA() throws InterruptedException {
    Future<?> batch =
        threads.submit(
            () ->
                repo.executeAtomically(
                    A,
                    List.of(),
                    GlobalOffset.of(1),
                    0L,
                    tx -> {
                      tx.save(A, "a", new Row("a", "batch"));
                      batchIsOpen.countDown();
                      try {
                        if (!release.await(60, TimeUnit.SECONDS)) {
                          throw new IllegalStateException("the open batch was never released");
                        }
                      } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(e);
                      }
                    },
                    null));
    assertThat(batchIsOpen.await(30, TimeUnit.SECONDS)).as("A's batch is open").isTrue();
    return batch;
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aReplayOfAnotherProjectionWaitsForTheOpenBatchAndRunsOnceItCommits() throws Exception {
    Future<?> batch = openBatchOfA();

    Future<?> replay =
        threads.submit(
            () -> repo.executeReplay(B, List.of(), tx -> tx.save(B, "b", new Row("b", "replay"))));

    // A is provably still open (only release lets it commit), so B has not run yet.
    assertThatThrownBy(() -> replay.get(300, TimeUnit.MILLISECONDS))
        .isInstanceOf(TimeoutException.class);
    assertThat(repo.findById(B, "b", Row.class)).as("B wrote nothing while A was open").isEmpty();

    release.countDown();
    batch.get(30, TimeUnit.SECONDS);
    replay.get(30, TimeUnit.SECONDS);

    assertThat(repo.findById(A, "a", Row.class)).contains(new Row("a", "batch"));
    assertThat(repo.findById(B, "b", Row.class)).contains(new Row("b", "replay"));
  }

  @Test
  @Timeout(value = 60, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aReplayThatWaitsLongerThanBusyTimeoutFailsWithTheLockFailureMessage() throws Exception {
    Future<?> batch = openBatchOfA();
    try (SqliteDatabase impatient =
        SqliteDatabase.open(TestDatabases.dataSource(file, "busy_timeout=100"))) {
      var replaying = repository(impatient);

      assertThatThrownBy(
              () ->
                  replaying.executeReplay(
                      B, List.of(), tx -> tx.save(B, "b", new Row("b", "replay"))))
          .isExactlyInstanceOf(EventStoreException.class)
          .hasMessageStartingWith(
              "SQLite's write lock was not granted within the connection's busy_timeout, so the"
                  + " projection dead-letter replay of projection replayed stored nothing.");
    }

    release.countDown();
    batch.get(30, TimeUnit.SECONDS);
    assertThat(repo.findById(B, "b", Row.class)).as("the failed replay wrote nothing").isEmpty();
    assertThat(repo.findById(A, "a", Row.class)).contains(new Row("a", "batch"));
  }
}
