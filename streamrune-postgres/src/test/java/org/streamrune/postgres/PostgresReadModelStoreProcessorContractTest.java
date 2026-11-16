package org.streamrune.postgres;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.SQLException;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.AtomicBatchProcessorContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The processor of a {@link PostgresReadModelStore} honours the {@code transactional()} and {@code
 * fencesCheckpoint()} contract over {@code readmodel_checkpoint}, read through the store's own
 * {@link PostgresReadModelStore#checkpoints()}. The database holds the read-model series only.
 */
@Testcontainers
class PostgresReadModelStoreProcessorContractTest extends AtomicBatchProcessorContract {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME)
          .withDatabaseName("readmodel_processor_contract");

  static PGSimpleDataSource dataSource;
  static PostgresReadModelStore store;
  static final ObjectMapper READER = new ObjectMapper();

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    store =
        PostgresReadModelStore.create(
            "postgres", dataSource, JdbcProjectionRepository.createObjectMapper(null), true, true);
  }

  @Override
  protected AtomicBatchProcessor processor() {
    return store.processor();
  }

  /**
   * Same DataSource, ANOTHER ObjectMapper: a different write target, because the mapper decides how
   * (and whether encrypted) a read model is persisted.
   */
  @Override
  protected ProjectionRepository repositoryOfAnotherStore() {
    return PostgresReadModelStore.create("other", dataSource, new ObjectMapper(), false, false)
        .repository();
  }

  @Override
  protected OffsetStore offsetStore() {
    return store.checkpoints();
  }

  /** Plain SELECT on a fresh connection: not blocked by the FOR UPDATE the transaction holds. */
  @Override
  protected GlobalOffset committedOffset(ProjectionName name) {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT last_offset FROM readmodel_checkpoint WHERE projection_name = ?")) {
      ps.setString(1, name.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? GlobalOffset.of(rs.getLong(1)) : GlobalOffset.initial();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Raw JDBC on a fresh connection: the outside view, independent of any repository. The {@code
   * <name>_view} table is created in autocommit before the batch transaction opens, so an
   * uncommitted row is simply not visible; the 42P01 arm only guards a table never created.
   */
  @Override
  protected Optional<ContractView> readFromOutside(ProjectionName name, String id) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT data FROM " + name.value() + "_view WHERE id = ?")) {
      ps.setString(1, id);
      try (var rs = ps.executeQuery()) {
        return rs.next()
            ? Optional.of(READER.readValue(rs.getString(1), ContractView.class))
            : Optional.empty();
      }
    } catch (SQLException e) {
      if ("42P01".equals(e.getSQLState())) {
        return Optional.empty();
      }
      throw new IllegalStateException(e);
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
