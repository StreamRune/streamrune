package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.sqlite.SqliteCrashPoints.Kill;
import org.streamrune.sqlite.SqliteCrashPoints.Point;

/**
 * The crash points of the repository's durable writes, each killed there the way the death of the
 * process would kill it (SQLite rolls back the open transaction of a connection that closes), then
 * run again through the real repository:
 *
 * <ul>
 *   <li>a batch killed inside its checkpoint advance, before the {@code COMMIT}: neither the
 *       read-model writes nor the advance remain, and the rerun applies the batch once;
 *   <li>a first-ever batch, which seeds the checkpoint row and creates the view table inside its
 *       transaction, killed there: neither the row nor the table remains, and the rerun creates
 *       both;
 *   <li>a batch killed right after its {@code COMMIT}: both are durable, the after-commit actions
 *       ran, the rerun of the same range is refused by the overlap guard, and reading on resumes;
 *   <li>the view-table creation killed right after its {@code COMMIT}: the table is there, and the
 *       rerun finds it;
 *   <li>a replay killed inside its second read-model write: nothing of the range remains and the
 *       checkpoint row is untouched; the rerun applies it;
 *   <li>a replay killed right after its {@code COMMIT}: the range is durable; the caller saw a
 *       failure and replays again, which applies the (idempotent) range a second time.
 * </ul>
 */
class SqliteProjectionRepositoryCrashTest {

  private static final ProjectionName P = ProjectionName.of("crash_projection");
  private static final Point CHECKPOINT_ROW_1 = new Point("readmodel_checkpoint", 1);
  private static final Point CHECKPOINT_ROW_2 = new Point("readmodel_checkpoint", 2);
  private static final Point SECOND_VIEW_ROW = new Point("crash_projection_view", 2);

  record Applied(String id, long offset) {
    @JsonCreator
    Applied(@JsonProperty("id") String id, @JsonProperty("offset") long offset) {
      this.id = id;
      this.offset = offset;
    }
  }

  @TempDir Path dir;

  private SqliteDatabase database;
  private SqliteDatabase crashingDatabase;
  private DataSource clean;
  private final SqliteCrashPoints crashPoints = new SqliteCrashPoints(Kill.CLOSE_CONNECTION);
  private SqliteProjectionRepository repo;
  private SqliteReadModelCheckpoints checkpoints;

  @BeforeEach
  void open() {
    Path file = dir.resolve("crash.db");
    database = SqliteDatabase.open(TestDatabases.dataSource(file));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    clean = TestDatabases.dataSource(file);
    // A new physical connection on every borrow, so a killed one is never handed out again.
    crashingDatabase = SqliteDatabase.open(crashPoints.connections(TestDatabases.dataSource(file)));
    checkpoints = new SqliteReadModelCheckpoints(database);
    repo =
        new SqliteProjectionRepository(
            crashingDatabase,
            SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
            new SqliteReadModelCheckpoints(crashingDatabase));
  }

  @AfterEach
  void close() {
    crashingDatabase.close();
    database.close();
  }

  /** One batch applying offsets {@code first..last}, one read-model row each. */
  private void batch(long first, long last) {
    repo.executeAtomically(
        P,
        Batches.of(first, last),
        GlobalOffset.of(last),
        0L,
        tx -> {
          for (long offset = first; offset <= last; offset++) {
            tx.save(P, "e" + offset, new Applied("e" + offset, offset));
          }
        },
        checkpoints);
  }

  private void replay(long first, long last) {
    repo.executeReplay(
        P,
        Batches.of(first, last),
        tx -> {
          for (long offset = first; offset <= last; offset++) {
            tx.save(P, "e" + offset, new Applied("e" + offset, offset));
          }
        });
  }

  private long rows() throws SQLException {
    if (!tableExists()) {
      return 0;
    }
    try (Connection conn = clean.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM crash_projection_view");
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private long version(String id) throws SQLException {
    try (Connection conn = clean.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT version FROM crash_projection_view WHERE id = ?")) {
      ps.setString(1, id);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private boolean tableExists() throws SQLException {
    try (Connection conn = clean.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT 1 FROM sqlite_schema WHERE name = 'crash_projection_view'");
        ResultSet rs = ps.executeQuery()) {
      return rs.next();
    }
  }

  private boolean checkpointRowExists() throws SQLException {
    try (Connection conn = clean.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT count(*) FROM readmodel_checkpoint");
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      return rs.getLong(1) > 0;
    }
  }

  @Test
  void aBatchKilledInsideItsCheckpointAdvanceLeavesNeitherItsWritesNorTheAdvance()
      throws SQLException {
    batch(1, 10);
    crashPoints.arm(CHECKPOINT_ROW_1);

    assertThatThrownBy(() -> batch(11, 12))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Transaction failed during projection update");

    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(10));
    assertThat(rows()).as("the read-model writes rolled back with the advance").isEqualTo(10);

    batch(11, 12);
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(12));
    assertThat(rows()).as("the rerun applies the batch once").isEqualTo(12);
    assertThat(version("e11")).isZero();
  }

  @Test
  void aFirstBatchKilledInsideItsAdvanceLeavesNeitherTheSeedRowNorTheTable() throws SQLException {
    crashPoints.arm(CHECKPOINT_ROW_2);

    assertThatThrownBy(() -> batch(1, 3)).isExactlyInstanceOf(EventStoreException.class);

    assertThat(checkpointRowExists()).as("the seed rolled back with the batch").isFalse();
    assertThat(tableExists()).as("the table created in the batch rolled back too").isFalse();

    batch(1, 3);
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(3));
    assertThat(rows()).isEqualTo(3);
  }

  @Test
  void aBatchKilledRightAfterItsCommitIsDurableWholeAndItsRerunIsFencedOut() throws SQLException {
    batch(1, 10);
    crashPoints.arm(Point.AFTER_COMMIT);
    var afterCommit = new AtomicInteger();

    assertThatThrownBy(
            () ->
                repo.executeAtomically(
                    P,
                    Batches.of(11, 12),
                    GlobalOffset.of(12),
                    0L,
                    tx -> {
                      tx.save(P, "e11", new Applied("e11", 11));
                      tx.save(P, "e12", new Applied("e12", 12));
                      tx.afterCommit(afterCommit::incrementAndGet);
                    },
                    checkpoints))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Transaction failed during projection update");

    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(12));
    assertThat(rows()).as("the committed batch is durable whole").isEqualTo(12);
    assertThat(afterCommit).as("the outcome was unknown, so the actions ran").hasValue(1);

    // The caller saw a failure and runs the same range again: the overlap guard refuses it.
    assertThatThrownBy(() -> batch(11, 12))
        .isExactlyInstanceOf(ProjectionCommitFencedException.class)
        .satisfies(
            e ->
                assertThat(((ProjectionCommitFencedException) e).guard())
                    .isSameAs(ProjectionCommitFencedException.Guard.OVERLAP));
    assertThat(version("e11")).as("nothing is applied twice").isZero();

    batch(13, 13);
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(13));
    assertThat(rows()).isEqualTo(13);
  }

  @Test
  void aViewTableCreationKilledRightAfterItsCommitLeavesTheTableAndItsRerunFindsIt()
      throws SQLException {
    crashPoints.arm(Point.AFTER_COMMIT);

    assertThatThrownBy(() -> repo.prepareReadModel(P))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to create projection table: crash_projection_view");
    assertThat(tableExists()).isTrue();

    repo.prepareReadModel(P);
    batch(1, 2);
    assertThat(rows()).isEqualTo(2);
  }

  @Test
  void aReplayKilledInsideItsWritesLeavesNothingAndItsRerunAppliesTheRange() throws SQLException {
    batch(1, 5);
    crashPoints.arm(SECOND_VIEW_ROW);

    assertThatThrownBy(() -> replay(2, 3))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed to save projection: crash_projection/e3");

    assertThat(version("e2")).as("the first write of the range rolled back").isZero();
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(5));

    replay(2, 3);
    assertThat(version("e2")).isEqualTo(1);
    assertThat(version("e3")).isEqualTo(1);
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(5));
  }

  @Test
  void aReplayKilledRightAfterItsCommitIsDurableAndItsRerunAppliesTheRangeAgain()
      throws SQLException {
    batch(1, 5);
    crashPoints.arm(Point.AFTER_COMMIT);

    assertThatThrownBy(() -> replay(2, 3))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Transaction failed during projection dead-letter replay");
    assertThat(version("e2")).as("the replay is durable").isEqualTo(1);

    replay(2, 3);
    assertThat(version("e2")).as("the caller's second replay applies it again").isEqualTo(2);
    assertThat(rows()).isEqualTo(5);
    assertThat(checkpoints.getLastOffset(P)).isEqualTo(GlobalOffset.of(5));
  }

  /** One envelope per global offset in {@code [first, last]}: what a batch carries. */
  static final class Batches {
    private static final StreamId STREAM =
        StreamId.of(AggregateType.of("crash"), AggregateId.of("crash-stream"));

    record Happened(String payload) implements DomainEvent {}

    static List<EventEnvelope> of(long first, long last) {
      var events = new ArrayList<EventEnvelope>();
      for (long offset = first; offset <= last; offset++) {
        events.add(
            new EventEnvelope(
                GlobalOffset.of(offset),
                STREAM,
                new Version(offset),
                new EventType("Happened"),
                new Happened("p" + offset),
                new EventMetadata(
                    IdGenerator.generateEventId(),
                    IdGenerator.generateCommandId(),
                    null,
                    CorrelationId.of("crash"),
                    null,
                    null,
                    Instant.now())));
      }
      return events;
    }
  }
}
