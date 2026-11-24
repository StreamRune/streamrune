package org.streamrune.sqlite;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.AtomicBatchProcessorContract;

/**
 * The processor of a {@link SqliteReadModelStore} honours the {@code transactional()} and {@code
 * fencesCheckpoint()} contract over {@code readmodel_checkpoint}, read through the store's own
 * {@link SqliteReadModelStore#checkpoints()}. The file holds the read-model series only, migrated
 * by the store. The second reader of the contract is a separate data source on the same file: in
 * WAL mode it reads the last committed state while a batch holds the write lock.
 */
class SqliteReadModelStoreProcessorContractTest extends AtomicBatchProcessorContract {

  static final ObjectMapper READER = new ObjectMapper();

  @TempDir Path dir;

  SqliteDatabase database;
  SqliteReadModelStore store;
  DataSource outside;

  @BeforeEach
  void open() {
    Path file = dir.resolve("store-processor.db");
    database = SqliteDatabase.open(TestDatabases.dataSource(file));
    outside = TestDatabases.dataSource(file);
    store =
        SqliteReadModelStore.create(
            "sqlite",
            database,
            SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
            true,
            true);
  }

  @AfterEach
  void close() {
    database.close();
  }

  @Override
  protected AtomicBatchProcessor processor() {
    return store.processor();
  }

  /**
   * Same database, ANOTHER ObjectMapper: a different write target, because the mapper decides how
   * (and whether encrypted) a read model is persisted.
   */
  @Override
  protected ProjectionRepository repositoryOfAnotherStore() {
    return SqliteReadModelStore.create("other", database, new ObjectMapper(), false, false)
        .repository();
  }

  /**
   * SQLite's write lock covers the whole file: a replay of one projection waits for an open batch
   * of any other. SqliteProjectionRepositoryReplayLockTest pins that behaviour.
   */
  @Override
  protected boolean locksPerProjection() {
    return false;
  }

  @Override
  protected OffsetStore offsetStore() {
    return store.checkpoints();
  }

  @Override
  protected GlobalOffset committedOffset(ProjectionName name) {
    try (Connection conn = outside.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT last_offset FROM readmodel_checkpoint WHERE projection_name = ?")) {
      ps.setString(1, name.value());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? GlobalOffset.of(rs.getLong(1)) : GlobalOffset.initial();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Raw JDBC on a connection of its own. A batch creates the view table inside its transaction when
   * nothing prepared it, so before the commit the table itself is not there: that is "the row is
   * invisible before the commit" too.
   */
  @Override
  protected Optional<ContractView> readFromOutside(ProjectionName name, String id) {
    try (Connection conn = outside.getConnection();
        PreparedStatement exists =
            conn.prepareStatement(
                "SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = ?")) {
      exists.setString(1, name.value() + "_view");
      try (ResultSet rs = exists.executeQuery()) {
        if (!rs.next()) {
          return Optional.empty();
        }
      }
      try (PreparedStatement ps =
          conn.prepareStatement("SELECT data FROM " + name.value() + "_view WHERE id = ?")) {
        ps.setString(1, id);
        try (ResultSet rs = ps.executeQuery()) {
          return rs.next()
              ? Optional.of(READER.readValue(rs.getString(1), ContractView.class))
              : Optional.empty();
        }
      }
    } catch (SQLException | IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
