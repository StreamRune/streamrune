package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.ProjectionRepositoryQueryContract;

/**
 * {@link SqliteProjectionRepository} honours the paged-read contract with PostgreSQL's text
 * semantics, and creates the indexes an application declares on exactly the expressions its reads
 * compare, so the reads use them.
 */
class SqliteProjectionRepositoryQueryContractTest extends ProjectionRepositoryQueryContract {

  @TempDir static Path dir;

  static SqliteDatabase database;
  static SqliteProjectionRepository repo;

  private static final Sort NEWEST_FIRST = Sort.by("createdAt", SortDirection.DESC);

  @BeforeAll
  static void open() {
    database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("query-contract.db")));
    database.migrate(SqliteSchemaSeries.READ_MODEL);
    repo =
        new SqliteProjectionRepository(
            database,
            SqliteProjectionRepository.createObjectMapper(null, StreamRuneMetrics.NOOP),
            new SqliteReadModelCheckpoints(database));
  }

  @AfterAll
  static void close() {
    database.close();
  }

  @Override
  protected ProjectionRepository repository() {
    return repo;
  }

  private static List<String> explain(String sql, int parameters) throws SQLException {
    var plan = new ArrayList<String>();
    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps = conn.prepareStatement("EXPLAIN QUERY PLAN " + sql)) {
      for (int i = 1; i <= parameters; i++) {
        ps.setString(i, "1");
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          plan.add(rs.getString("detail"));
        }
      }
    }
    return plan;
  }

  private static String indexSql(String name) throws SQLException {
    try (Connection conn = database.dataSource().getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT sql FROM sqlite_schema WHERE type = 'index' AND name = ?")) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  @Test
  void aDeclaredIndexIsOnTheReadExpressionsWithTheIdLast() throws SQLException {
    var name = freshName();
    var index = ProjectionIndex.forFilter(name, "customer", NEWEST_FIRST);

    repo.declareIndex(index);

    assertThat(indexSql(SqliteProjectionRepository.indexName(index)))
        .isEqualTo(SqliteProjectionRepository.createIndexSql(index).replace(" IF NOT EXISTS", ""))
        .contains(
            " ON "
                + name.value()
                + "_view ("
                + JsonFieldSql.text("customer")
                + ", "
                + JsonFieldSql.text("createdAt")
                + " DESC, id)");
  }

  @Test
  void anIndexThatNamesTheIdKeepsItWhereItIsDeclared() throws SQLException {
    var name = freshName();
    var index =
        ProjectionIndex.forSort(
            name,
            new Sort(
                List.of(
                    new Sort.Order("id", SortDirection.DESC),
                    new Sort.Order("version", SortDirection.ASC))));

    repo.declareIndex(index);

    assertThat(indexSql(SqliteProjectionRepository.indexName(index)))
        .endsWith("(id DESC, version)");
  }

  /**
   * The page's {@code ORDER BY ... LIMIT} reads the declared index in order (no temporary B-tree
   * under it), and the filter is an index lookup; the outer sort over the page's rows is the only
   * other step.
   */
  @Test
  void theReadsUseTheDeclaredIndexes() throws SQLException {
    var name = freshName();
    var forFilter = ProjectionIndex.forFilter(name, "customer", NEWEST_FIRST);
    var forSort = ProjectionIndex.forSort(name, NEWEST_FIRST);
    repo.declareIndex(forFilter);
    repo.declareIndex(forSort);
    var filter = FieldEquals.of("customer", "alice");

    List<String> filtered =
        explain(SqliteProjectionRepository.pageSql(name, filter, NEWEST_FIRST), 4);
    List<String> sorted = explain(SqliteProjectionRepository.pageSql(name, null, NEWEST_FIRST), 2);
    List<String> byId = explain(SqliteProjectionRepository.pageSql(name, null, Sort.unsorted()), 2);

    String filterIndex = SqliteProjectionRepository.indexName(forFilter);
    String sortIndex = SqliteProjectionRepository.indexName(forSort);
    assertThat(filtered)
        .anySatisfy(line -> assertThat(line).contains("USING COVERING INDEX " + filterIndex))
        .anySatisfy(
            line -> assertThat(line).contains("USING INDEX " + filterIndex + " (<expr>=?)"));
    assertThat(sorted).anySatisfy(line -> assertThat(line).contains("USING INDEX " + sortIndex));
    assertThat(byId)
        .anySatisfy(
            line -> assertThat(line).contains("USING INDEX sqlite_autoindex_" + name.value()));
    for (List<String> plan : List.of(filtered, sorted, byId)) {
      assertThat(plan.stream().filter(line -> line.contains("TEMP B-TREE")).count())
          .as("only the outer sort of the page's rows: %s", plan)
          .isEqualTo(1);
    }
  }

  @Test
  void indexNamesAreDeterministicAndDistinct() {
    var name = ProjectionName.of("orders");
    var a = ProjectionIndex.forSort(name, Sort.by("createdAt"));
    var b = ProjectionIndex.forSort(name, Sort.by("createdat"));
    var c = ProjectionIndex.forSort(name, Sort.by("createdAt", SortDirection.DESC));

    assertThat(SqliteProjectionRepository.indexName(a))
        .isEqualTo(SqliteProjectionRepository.indexName(a))
        .startsWith("orders_createdat_id_ix_");
    assertThat(List.of(a, b, c).stream().map(SqliteProjectionRepository::indexName).distinct())
        .hasSize(3);
    var longKeys = ProjectionIndex.forSort(name, Sort.by("x".repeat(200)));
    assertThat(SqliteProjectionRepository.indexName(longKeys)).hasSizeLessThanOrEqualTo(63);
  }

  @Test
  void aRelationOfTheDeclaredNameThatIsNotItsIndexFailsTheDeclaration() throws SQLException {
    var name = freshName();
    var other = freshName();
    var index = ProjectionIndex.forSort(name, NEWEST_FIRST);
    repo.findAll(other, Object.class);
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute(
          "CREATE INDEX "
              + SqliteProjectionRepository.indexName(index)
              + " ON "
              + other.value()
              + "_view (version)");
    }

    assertThatThrownBy(() -> repo.declareIndex(index))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage(
            "A schema object named "
                + SqliteProjectionRepository.indexName(index)
                + " exists and is not an index of "
                + name.value()
                + "_view: the projection index of that name cannot be created; rename or drop it");
  }

  @Test
  void anIndexThatExistsIsLeftAsItIs() throws SQLException {
    var name = freshName();
    var index = ProjectionIndex.forSort(name, NEWEST_FIRST);
    repo.declareIndex(index);
    String before = indexSql(SqliteProjectionRepository.indexName(index));

    repo.declareIndex(index);
    new SqliteProjectionRepository(
            database, new ObjectMapper(), new SqliteReadModelCheckpoints(database))
        .declareIndex(index);

    assertThat(indexSql(SqliteProjectionRepository.indexName(index))).isEqualTo(before);
  }

  @Test
  void aDeclarationThatFailsIsAnEventStoreException() throws SQLException {
    var name = freshName();
    var index = ProjectionIndex.forSort(name, NEWEST_FIRST);
    repo.findAll(name, Object.class);
    try (Connection conn = database.dataSource().getConnection();
        Statement st = conn.createStatement()) {
      st.execute("CREATE VIEW " + SqliteProjectionRepository.indexName(index) + " AS SELECT 1");
    }

    assertThatThrownBy(() -> repo.declareIndex(index))
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessageContaining("exists and is not an index of " + name.value() + "_view");
  }

  @Test
  void nullIndexesAreRefused() {
    assertThatThrownBy(() -> SqliteProjectionRepository.createIndexSql(null))
        .isExactlyInstanceOf(IllegalArgumentException.class)
        .hasMessage("index cannot be null");
  }
}
