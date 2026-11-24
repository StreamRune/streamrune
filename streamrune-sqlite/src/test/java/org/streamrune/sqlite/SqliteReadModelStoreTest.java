package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStore;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;

/**
 * {@link SqliteReadModelStore#create} applies the read-model series on its own history table and
 * validates it, each step switchable; the store it builds is one write target whose checkpoints are
 * the ones its processor commits; a store on a second database file holds that file's instance
 * guard through its own handle; and the handle stays the caller's to close.
 */
class SqliteReadModelStoreTest {

  /**
   * The guard's refusal when another process holds the file: a probe refused for any other reason
   * (a hard link, an unreadable lock file) does not count as the guard being held.
   */
  private static final String HELD_BY_ANOTHER_PROCESS = "is in use by another process";

  record View(String id) {
    @JsonCreator
    View(@JsonProperty("id") String id) {
      this.id = id;
    }
  }

  @TempDir Path dir;

  private final ObjectMapper mapper =
      SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP);

  @Test
  void createMigratesIntoItsOwnHistoryTable() throws SQLException {
    Path file = dir.resolve("migrate.db");
    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(file))) {

      SqliteReadModelStore.create("sqlite", database, mapper, true, true);

      DataSource ds = database.dataSource();
      assertThat(appliedScripts(ds, "flyway_schema_history_readmodel"))
          .containsExactly("V001__readmodel_baseline.sql");
      assertThat(tableExists(ds, "readmodel_checkpoint")).isTrue();
      // The read-model database need not hold the events: the event-store series is not applied.
      assertThat(tableExists(ds, "event_stream")).isFalse();
      assertThat(tableExists(ds, SqliteSchemaSeries.EVENT_STORE.historyTable())).isFalse();
    }
  }

  @Test
  void createWithValidateOnlyFailsOnMissingTable() throws SQLException {
    Path file = dir.resolve("validate.db");
    try (SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(file))) {

      assertThatThrownBy(() -> SqliteReadModelStore.create("sqlite", database, mapper, false, true))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("readmodel_checkpoint");

      DataSource ds = database.dataSource();
      assertThat(tableExists(ds, "readmodel_checkpoint")).as("nothing was migrated").isFalse();
      assertThat(tableExists(ds, SqliteSchemaSeries.READ_MODEL.historyTable())).isFalse();
    }
  }

  @Test
  void repositoryAndProcessorShareTheWriteTarget() {
    try (SqliteDatabase database = open("parts.db")) {
      var store = SqliteReadModelStore.create("sqlite", database, mapper, true, true);

      assertThat(store.processor()).isSameAs(store.repository());
      assertThat(store.repository()).isInstanceOf(SqliteProjectionRepository.class);
      assertThat(store.processor().writesTo(store.repository())).isTrue();
      assertThat(store.processor().transactional()).isTrue();
      assertThat(store.processor().fencesCheckpoint()).isTrue();
      assertThat(store.name()).isEqualTo("sqlite");
      assertThat(store.checkpoints()).isInstanceOf(SqliteReadModelCheckpoints.class);
      var target = store.checkpointTarget();
      assertThat(target.name()).isEqualTo("sqlite");
      assertThat(target.checkpoints()).isSameAs(store.checkpoints());
      assertThat(target.processor()).isSameAs(store.processor());
    }
  }

  /**
   * A batch the processor commits is the checkpoint {@code checkpoints()} reads, and it lands in
   * {@code readmodel_checkpoint}, never in the event store's {@code subscription_checkpoint} of the
   * same file.
   */
  @Test
  void theProcessorCommitsTheCheckpointsTheStoreReads() {
    try (SqliteDatabase database = open("commit.db")) {
      database.migrate(SqliteSchemaSeries.EVENT_STORE);
      ReadModelStore store = SqliteReadModelStore.create("sqlite", database, mapper, true, true);
      var name = ProjectionName.of("orders");

      store
          .processor()
          .executeAtomically(
              name,
              List.of(),
              GlobalOffset.of(7),
              0L,
              repo -> repo.save(name, "o-1", new View("o-1")),
              store.checkpoints());

      assertThat(store.checkpoints().getLastOffset(name)).isEqualTo(GlobalOffset.of(7));
      assertThat(new SqliteOffsetStore(database).getLastOffset(name))
          .as("the read-model checkpoint is not the subscription checkpoint")
          .isEqualTo(GlobalOffset.initial());
      assertThat(store.repository().findById(name, "o-1", View.class)).contains(new View("o-1"));
    }
  }

  /**
   * The read-model series applies in full to a file that already holds the event-store series (the
   * default single-file deployment), and a second start applies nothing.
   */
  @Test
  void createAppliesTheSeriesBesideTheEventStoreSeriesAndAgainAsANoOp() throws SQLException {
    try (SqliteDatabase database = open("both.db")) {
      database.migrate(SqliteSchemaSeries.EVENT_STORE);
      DataSource ds = database.dataSource();
      assertThat(tableExists(ds, "readmodel_checkpoint")).isFalse();

      SqliteReadModelStore.create("sqlite", database, mapper, true, true);
      SqliteReadModelStore.create("sqlite", database, mapper, true, true);

      assertThat(appliedScripts(ds, SqliteSchemaSeries.READ_MODEL.historyTable()))
          .containsExactly("V001__readmodel_baseline.sql");
      assertThat(appliedScripts(ds, SqliteSchemaSeries.EVENT_STORE.historyTable()))
          .containsExactly("V001__baseline.sql");
      assertThat(tableExists(ds, "readmodel_checkpoint")).isTrue();
      assertThat(tableExists(ds, "subscription_checkpoint")).isTrue();
    }
  }

  /** With both schema steps off, the store is built on the schema as it is. */
  @Test
  void createWithNeitherStepLeavesTheSchemaAlone() throws SQLException {
    try (SqliteDatabase database = open("neither.db")) {

      var store = SqliteReadModelStore.create("search", database, mapper, false, false);

      assertThat(store.name()).isEqualTo("search");
      DataSource ds = database.dataSource();
      assertThat(tableExists(ds, "readmodel_checkpoint")).isFalse();
      assertThat(tableExists(ds, SqliteSchemaSeries.READ_MODEL.historyTable())).isFalse();
    }
  }

  /**
   * A closed handle is refused before anything is written, whatever the flags: with both off, a
   * store built on it would write without the instance guard.
   */
  @Test
  void aClosedHandleIsRefusedWhateverTheFlags() throws SQLException {
    SqliteDatabase database = open("closed.db");
    database.close();

    for (boolean[] flags :
        new boolean[][] {{true, true}, {true, false}, {false, true}, {false, false}}) {
      assertThatThrownBy(
              () -> SqliteReadModelStore.create("sqlite", database, mapper, flags[0], flags[1]))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("is closed");
    }
    assertThat(tableExists(database.dataSource(), "readmodel_checkpoint")).isFalse();
  }

  @Test
  void requiredArgumentsAreCheckedBeforeTheDatabaseIsTouched() throws SQLException {
    try (SqliteDatabase database = open("arguments.db")) {

      assertThatThrownBy(() -> SqliteReadModelStore.create(null, database, mapper, true, true))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("name is required and must not be blank");
      assertThatThrownBy(() -> SqliteReadModelStore.create(" ", database, mapper, true, true))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("name is required and must not be blank");
      assertThatThrownBy(() -> SqliteReadModelStore.create("sqlite", null, mapper, true, true))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("database is required");
      assertThatThrownBy(() -> SqliteReadModelStore.create("sqlite", database, null, true, true))
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage("objectMapper is required");

      assertThat(tableExists(database.dataSource(), "readmodel_checkpoint")).isFalse();
      assertThat(tableExists(database.dataSource(), SqliteSchemaSeries.READ_MODEL.historyTable()))
          .isFalse();
    }
  }

  /**
   * {@code "default"} is refused with core's own message, before the database is touched. The match
   * is exact, as on PostgreSQL and in {@link ReadModelStore#of}: another spelling is an ordinary
   * name.
   */
  @Test
  void theReservedDefaultNameIsRefusedAsReadModelStoreOfRefusesIt() throws SQLException {
    try (SqliteDatabase database = open("default.db")) {

      var refused =
          assertThatThrownBy(
              () ->
                  SqliteReadModelStore.create(
                      ReadModelStore.DEFAULT, database, mapper, true, true));
      var store = SqliteReadModelStore.create("sqlite", database, mapper, false, false);
      Throwable fromCore =
          org.assertj.core.api.Assertions.catchThrowable(
              () ->
                  ReadModelStore.of(
                      ReadModelStore.DEFAULT,
                      store.repository(),
                      store.checkpoints(),
                      store.processor()));
      refused
          .isExactlyInstanceOf(IllegalArgumentException.class)
          .hasMessage(fromCore.getMessage())
          .hasMessage("'default' is reserved: it names the backend's default read-model store");
      assertThat(tableExists(database.dataSource(), "readmodel_checkpoint")).isFalse();

      assertThat(SqliteReadModelStore.create("Default", database, mapper, true, true).name())
          .isEqualTo("Default");
    }
  }

  /**
   * The event store in file A, a read-model store in file B: B is opened through a handle of its
   * own, so it has its own instance guard. A second process can open neither file, the store's
   * batches and checkpoints land in B alone, and closing B's handle releases B's guard and leaves
   * A's held.
   */
  @Test
  void aReadModelStoreInASecondFileHoldsThatFilesOwnGuard() throws Exception {
    Path fileA = dir.resolve("events.db");
    Path fileB = dir.resolve("readmodels.db");
    try (SqliteDatabase eventsDb = SqliteDatabase.open(TestDatabases.dataSource(fileA))) {
      EventStore events =
          new SqliteEventStoreFactory(eventsDb, SimpleEventTypeRegistry.builder().build()).create();
      SqliteDatabase readModelsDb = SqliteDatabase.open(TestDatabases.dataSource(fileB));
      try {
        var store = SqliteReadModelStore.create("search", readModelsDb, mapper, true, true);
        var name = ProjectionName.of("search_rows");
        store
            .processor()
            .executeAtomically(
                name,
                List.of(),
                GlobalOffset.of(3),
                0L,
                repo -> repo.save(name, "r-1", new View("r-1")),
                store.checkpoints());

        assertThat(events.lastGlobalOffset()).isEqualTo(GlobalOffset.initial());
        assertThat(store.checkpoints().getLastOffset(name)).isEqualTo(GlobalOffset.of(3));
        assertThat(tableExists(eventsDb.dataSource(), "readmodel_checkpoint"))
            .as("the read-model series lives in B only")
            .isFalse();
        assertThat(tableExists(readModelsDb.dataSource(), "event_stream")).isFalse();
        assertThat(Files.exists(Path.of(fileB.toRealPath() + ".lock"))).isTrue();

        assertThat(ForkedJvm.probe(fileA))
            .startsWith(GuardChild.REFUSED)
            .contains(HELD_BY_ANOTHER_PROCESS);
        assertThat(ForkedJvm.probe(fileB))
            .startsWith(GuardChild.REFUSED)
            .contains(HELD_BY_ANOTHER_PROCESS);
      } finally {
        readModelsDb.close();
      }
      assertThat(ForkedJvm.probe(fileB))
          .as("B's guard is released with B's handle")
          .startsWith(GuardChild.ACQUIRED);
      assertThat(ForkedJvm.probe(fileA))
          .as("A's guard stays held by A's handle")
          .startsWith(GuardChild.REFUSED)
          .contains(HELD_BY_ANOTHER_PROCESS);
    }
  }

  /**
   * The store does not own the handle: it has no close of its own, it keeps no reference to the
   * guard beyond the caller's handle, and dropping it leaves the guard held. Only the caller's
   * {@link SqliteDatabase#close()} releases it.
   */
  @Test
  void theHandleStaysTheCallersToClose() throws Exception {
    assertThat(AutoCloseable.class.isAssignableFrom(SqliteReadModelStore.class)).isFalse();
    Path file = dir.resolve("owned.db");
    SqliteDatabase database = SqliteDatabase.open(TestDatabases.dataSource(file));
    try {
      var store = SqliteReadModelStore.create("sqlite", database, mapper, true, true);
      store.repository().save(ProjectionName.of("owned"), "o", new View("o"));

      assertThat(ForkedJvm.probe(file))
          .startsWith(GuardChild.REFUSED)
          .contains(HELD_BY_ANOTHER_PROCESS);
    } finally {
      database.close();
    }
    assertThat(ForkedJvm.probe(file)).startsWith(GuardChild.ACQUIRED);
  }

  // --- helpers ---

  private SqliteDatabase open(String name) {
    return SqliteDatabase.open(TestDatabases.dataSource(dir.resolve(name)));
  }

  private static boolean tableExists(DataSource ds, String table) throws SQLException {
    try (Connection conn = ds.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")) {
      ps.setString(1, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private static List<String> appliedScripts(DataSource ds, String historyTable)
      throws SQLException {
    var scripts = new ArrayList<String>();
    try (Connection conn = ds.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT script FROM "
                    + historyTable
                    + " WHERE success AND version IS NOT NULL AND version <> '0'"
                    + " ORDER BY installed_rank");
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        scripts.add(rs.getString(1));
      }
    }
    return scripts;
  }
}
