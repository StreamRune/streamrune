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
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** {@link JdbcProjectionRepository} honours the {@code supportsFencing()} contract. */
@Testcontainers
class JdbcProjectionRepositoryContractTest extends AtomicBatchProcessorContract {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_contract_test");

  static PGSimpleDataSource dataSource;
  static JdbcProjectionRepository repo;
  static final ObjectMapper READER = new ObjectMapper();

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
    repo = new JdbcProjectionRepository(dataSource);
  }

  @Override
  protected AtomicBatchProcessor processor() {
    return repo;
  }

  /**
   * Same DataSource, ANOTHER ObjectMapper: a different write target, because the mapper decides how
   * (and whether encrypted) a read model is persisted.
   */
  @Override
  protected ProjectionRepository repositoryOfAnotherStore() {
    return new JdbcProjectionRepository(dataSource, new ObjectMapper());
  }

  @Override
  protected OffsetStore offsetStore() {
    return new PostgresOffsetStore(dataSource);
  }

  /** Plain SELECT on a fresh connection: not blocked by the FOR UPDATE the transaction holds. */
  @Override
  protected GlobalOffset committedOffset(ProjectionName name) {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT last_offset FROM projection_offset WHERE projection_name = ?")) {
      ps.setString(1, name.value());
      try (var rs = ps.executeQuery()) {
        return rs.next() ? GlobalOffset.of(rs.getLong(1)) : GlobalOffset.initial();
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Raw JDBC on a fresh connection: the outside view, independent of any repository. {@code
   * executeAtomically} creates the projection's {@code <name>_view} table in autocommit BEFORE its
   * transaction opens, so the table exists here and an uncommitted row is simply not visible — that
   * IS "the row is invisible before commit". The 42P01 arm only guards a read of a table that has
   * not been created at all.
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
