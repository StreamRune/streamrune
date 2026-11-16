package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.projection.FieldEquals;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.ProjectionRepositoryQueryContract;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * {@link JdbcProjectionRepository} honours the paged-read contract, and creates the indexes an
 * application declares: once, under the deterministic name, with the keys the reads order by, and
 * when several replicas declare them at once.
 */
@Testcontainers
class JdbcProjectionRepositoryQueryContractTest extends ProjectionRepositoryQueryContract {

  /**
   * A database whose default collation is linguistic (ICU {@code en-US}, as a Debian image's {@code
   * en_US.utf8} is), so the code-point cases prove the repository's own {@code COLLATE "C"} instead
   * of the Alpine image's byte-order default.
   */
  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_query_contract")
          .withEnv("POSTGRES_INITDB_ARGS", "--locale-provider=icu --icu-locale=en-US");

  static PGSimpleDataSource dataSource;
  static JdbcProjectionRepository repo;

  private static final Sort NEWEST_FIRST = Sort.by("createdAt", SortDirection.DESC);

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    ReadModelTestSchema.apply(dataSource);
    repo = new JdbcProjectionRepository(dataSource);
  }

  @Override
  protected ProjectionRepository repository() {
    return repo;
  }

  @Test
  void theDatabaseCollationIsLinguistic_soTheCodePointCasesProveTheCollateClause()
      throws SQLException {
    try (var conn = dataSource.getConnection();
        var rs = conn.createStatement().executeQuery("SELECT 'a1' < 'B1'")) {
      rs.next();
      assertThat(rs.getBoolean(1)).as("en_US orders a1 before B1; code points do not").isTrue();
    }
  }

  @Test
  void aDeclaredIndexExistsWithItsKeysInOrder_andTheIdLast() throws SQLException {
    var name = freshName();
    var index = ProjectionIndex.forFilter(name, "customer", NEWEST_FIRST);

    repo.declareIndex(index);

    String definition = indexDefinition(JdbcProjectionRepository.indexName(index));
    assertThat(definition)
        .isEqualTo(
            "CREATE INDEX "
                + JdbcProjectionRepository.indexName(index)
                + " ON public."
                + name.value()
                + "_view USING btree (((data ->> 'customer'::text)) COLLATE \"C\","
                + " ((data ->> 'createdAt'::text)) COLLATE \"C\" DESC, id)");
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

    assertThat(indexDefinition(JdbcProjectionRepository.indexName(index)))
        .endsWith("USING btree (id DESC, version)");
  }

  @Test
  void theReadsUseTheDeclaredIndexes() throws SQLException {
    var name = freshName();
    repo.declareIndex(ProjectionIndex.forFilter(name, "customer", NEWEST_FIRST));
    repo.declareIndex(ProjectionIndex.forSort(name, NEWEST_FIRST));
    var filter = FieldEquals.of("customer", "alice");

    String filtered = explain(JdbcProjectionRepository.pageSql(name, filter, NEWEST_FIRST), true);
    String sorted = explain(JdbcProjectionRepository.pageSql(name, null, NEWEST_FIRST), false);
    String byId = explain(JdbcProjectionRepository.pageSql(name, null, Sort.unsorted()), false);

    var forFilter =
        JdbcProjectionRepository.indexName(
            ProjectionIndex.forFilter(name, "customer", NEWEST_FIRST));
    var forSort = JdbcProjectionRepository.indexName(ProjectionIndex.forSort(name, NEWEST_FIRST));
    // The page's LIMIT reads the index in order (no sort under it); the one sort left is the outer
    // ORDER BY over the page's rows, at most size of them, after the join with the count.
    assertThat(filtered).containsPattern(limitOver(forFilter)).contains("Index Cond");
    assertThat(sorted).containsPattern(limitOver(forSort));
    assertThat(byId).containsPattern(limitOver(name.value() + "_view_pkey"));
  }

  @Test
  void indexNamesAreDeterministic_distinct_andWithinTheIdentifierLimit() {
    var name = ProjectionName.of("a".repeat(58));
    var longField = "f".repeat(200);
    var one = ProjectionIndex.forFilter(name, longField, Sort.unsorted());
    var other = ProjectionIndex.forFilter(name, longField + "g", Sort.unsorted());
    var caseOnly = ProjectionIndex.forFilter(name, "customerId", Sort.unsorted());
    var lowerCase = ProjectionIndex.forFilter(name, "customerid", Sort.unsorted());
    var descending = ProjectionIndex.forSort(name, Sort.by("customerId", SortDirection.DESC));

    var names =
        List.of(
            JdbcProjectionRepository.indexName(one),
            JdbcProjectionRepository.indexName(other),
            JdbcProjectionRepository.indexName(caseOnly),
            JdbcProjectionRepository.indexName(lowerCase),
            JdbcProjectionRepository.indexName(descending));

    assertThat(names).doesNotHaveDuplicates().allMatch(n -> n.length() <= 63);
    assertThat(names).allMatch(n -> n.matches("[a-z_][a-z0-9_]*"));
    assertThat(JdbcProjectionRepository.indexName(one))
        .isEqualTo(
            JdbcProjectionRepository.indexName(
                ProjectionIndex.forFilter(name, longField, Sort.unsorted())));
  }

  @Test
  void anIndexOnALongFieldIsCreated() throws SQLException {
    var name = freshName();
    var index = ProjectionIndex.forFilter(name, "f".repeat(200), Sort.unsorted());

    repo.declareIndex(index);

    assertThat(indexDefinition(JdbcProjectionRepository.indexName(index))).contains("fffff");
  }

  @Test
  void replicasDeclaringTheSameIndexAtOnce_allSucceed() throws Exception {
    int replicas = 6;
    int rounds = 15;
    List<String> failures = Collections.synchronizedList(new ArrayList<>());
    ExecutorService pool = Executors.newFixedThreadPool(replicas);
    try {
      for (int round = 0; round < rounds; round++) {
        ProjectionName name = freshName();
        var index = ProjectionIndex.forFilter(name, "customer", NEWEST_FIRST);
        CyclicBarrier start = new CyclicBarrier(replicas);
        List<Future<?>> declarations = new ArrayList<>();
        for (int r = 0; r < replicas; r++) {
          // one repository per replica: each has its own in-process table check
          JdbcProjectionRepository replica = new JdbcProjectionRepository(dataSource);
          declarations.add(
              pool.submit(
                  () -> {
                    try {
                      start.await(10, TimeUnit.SECONDS);
                      replica.declareIndex(index);
                    } catch (Throwable t) {
                      failures.add(t.toString());
                    }
                  }));
        }
        for (Future<?> declaration : declarations) {
          declaration.get(60, TimeUnit.SECONDS);
        }
        assertThat(indexDefinition(JdbcProjectionRepository.indexName(index))).isNotEmpty();
      }
    } finally {
      pool.shutdownNow();
    }
    assertThat(failures).isEmpty();
  }

  @Test
  void anIndexThatExistsIsNotCreatedAgain() throws SQLException {
    var name = freshName();
    var index = ProjectionIndex.forSort(name, NEWEST_FIRST);
    repo.declareIndex(index);
    var counting = new CountingDataSource(dataSource);

    new JdbcProjectionRepository(counting.proxy()).declareIndex(index);

    assertThat(counting.statements).noneMatch(sql -> sql.startsWith("CREATE INDEX"));
  }

  @Test
  void eachPagedReadIsOneStatement_soItsCountAndItsRowsComeFromOneSnapshot() {
    var name = freshName();
    repo.save(name, "r1", new ContractRow("r1", "alice", null, null));
    var counting = new CountingDataSource(dataSource);
    var counted = new JdbcProjectionRepository(counting.proxy());
    counted.findAll(name, ContractRow.class, PageRequest.of(0, 1));
    counting.statements.clear();

    var page = counted.findAll(name, ContractRow.class, PageRequest.of(0, 10));
    var filtered =
        counted.findAllBy(
            name, ContractRow.class, FieldEquals.of("customer", "alice"), PageRequest.of(5, 10));

    assertThat(counting.statements).hasSize(2);
    assertThat(page.totalElements()).isEqualTo(1L);
    assertThat(filtered.totalElements()).isEqualTo(1L);
    assertThat(filtered.isEmpty()).isTrue();
  }

  @Test
  void aPageBeyondTheEndReadsOnlyTheCount() throws SQLException {
    var name = freshName();
    repo.declareIndex(ProjectionIndex.forFilter(name, "customer", NEWEST_FIRST));
    repo.save(name, "r1", new ContractRow("r1", "alice", "2026-01-01T00:00:01Z", null));

    String plan =
        explain(
            "EXPLAIN (ANALYZE) ",
            JdbcProjectionRepository.pageSql(
                name, FieldEquals.of("customer", "alice"), NEWEST_FIRST),
            true,
            1_000_000L);

    assertThat(plan).contains("One-Time Filter").contains("never executed");
  }

  /** The INFO line carries the whole statement, so an operator can run it by hand. */
  @Test
  void theCreatedIndexIsLoggedWithItsWholeStatement_andAnExistingOneIsNot() {
    var name = freshName();
    var index = ProjectionIndex.forFilter(name, "customerId", NEWEST_FIRST);
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(JdbcProjectionRepository.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    var level = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.INFO); // the test config keeps the root at WARN
    logger.addAppender(appender);
    try {
      repo.declareIndex(index);
      new JdbcProjectionRepository(dataSource).declareIndex(index);
    } finally {
      logger.detachAppender(appender);
      logger.setLevel(level);
      appender.stop();
    }

    assertThat(
            appender.list.stream()
                .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.INFO)
                .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
                .filter(m -> m.startsWith("Created projection index"))
                .toList())
        .containsExactly(
            "Created projection index: " + JdbcProjectionRepository.createIndexSql(index));
  }

  @Test
  void anInvalidIndexOfTheDeclaredNameFailsTheDeclarationNamingTheDrop() throws SQLException {
    var name = freshName();
    var index = ProjectionIndex.forSort(name, NEWEST_FIRST);
    repo.declareIndex(index);
    String indexName = JdbcProjectionRepository.indexName(index);
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE pg_index SET indisvalid = false WHERE indexrelid = ?::regclass")) {
      ps.setString(1, indexName);
      ps.executeUpdate();
    }

    assertThatThrownBy(() -> new JdbcProjectionRepository(dataSource).declareIndex(index))
        .isInstanceOf(org.streamrune.core.EventStoreException.class)
        .hasMessageContaining("INVALID")
        .hasMessageContaining("DROP INDEX " + indexName);
  }

  @Test
  void aRelationOfTheDeclaredNameThatIsNotItsIndexFailsTheDeclaration() throws SQLException {
    var name = freshName();
    var index = ProjectionIndex.forSort(name, NEWEST_FIRST);
    repo.prepareReadModel(name);
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE " + JdbcProjectionRepository.indexName(index) + " (x int)");
    }

    assertThatThrownBy(() -> repo.declareIndex(index))
        .isInstanceOf(org.streamrune.core.EventStoreException.class)
        .hasMessageContaining("is not an index of " + name.value() + "_view");
  }

  @Test
  void aDeclarationThatFailsForAnotherReasonIsAnEventStoreException() {
    var name = freshName();
    var index = ProjectionIndex.forSort(name, NEWEST_FIRST);
    repo.prepareReadModel(name);
    var failing = new CountingDataSource(dataSource, sql -> sql.startsWith("CREATE INDEX"));

    assertThatThrownBy(() -> new JdbcProjectionRepository(failing.proxy()).declareIndex(index))
        .isInstanceOf(org.streamrune.core.EventStoreException.class)
        .hasMessageContaining("Failed to create projection index");
  }

  @Test
  void nullIndexesAreRefused() {
    org.assertj.core.api.Assertions.assertThatIllegalArgumentException()
        .isThrownBy(() -> JdbcProjectionRepository.createIndexSql(null));
  }

  /**
   * A table whose id is in the database's linguistic collation (created by an older version, or by
   * the application): the reads still order by code point, because they name the collation.
   */
  @Test
  void aTableWhoseIdIsNotInTheCCollationStillOrdersByCodePoint() throws SQLException {
    var name = freshName();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE "
              + name.value()
              + "_view (id VARCHAR(255) PRIMARY KEY, data JSONB NOT NULL,"
              + " version BIGINT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW())");
    }
    for (String id : List.of("order-a1", "order-B1", "order-b0")) {
      repo.save(name, id, new ContractRow(id, "x", "2026-01-01T00:00:00Z", null));
    }
    var expected = List.of("order-B1", "order-a1", "order-b0");

    assertThat(ids(repo.findAll(name, ContractRow.class, PageRequest.of(0, 10))))
        .isEqualTo(expected);
    assertThat(
            ids(
                repo.findAllBy(
                    name,
                    ContractRow.class,
                    FieldEquals.of("customer", "x"),
                    PageRequest.of(0, 10, NEWEST_FIRST))))
        .isEqualTo(expected);
  }

  private static List<String> ids(org.streamrune.core.Page<ContractRow> page) {
    return page.content().stream().map(ContractRow::id).toList();
  }

  /**
   * A {@code Limit} node whose input is a scan of {@code index} in index order, possibly through
   * the {@code Result} of the count gate.
   */
  private static java.util.regex.Pattern limitOver(String index) {
    return java.util.regex.Pattern.compile(
        "Limit [^\\n]*\\n(?:\\s*->  Result[^\\n]*\\n(?:\\s+One-Time Filter[^\\n]*\\n)?)?"
            + "\\s*->  Index Scan(?: Backward)? using "
            + index
            + " ");
  }

  private static String indexDefinition(String indexName) throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT indexdef FROM pg_indexes WHERE indexname = ?")) {
      ps.setString(1, indexName);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : "";
      }
    }
  }

  /** The plan of {@code sql}, with sequential scans priced out so a usable index is chosen. */
  private static String explain(String sql, boolean filtered) throws SQLException {
    return explain("EXPLAIN ", sql, filtered, 0L);
  }

  private static String explain(String explain, String sql, boolean filtered, long offset)
      throws SQLException {
    try (var conn = dataSource.getConnection()) {
      try (var off = conn.createStatement()) {
        off.execute("SET enable_seqscan = off");
        off.execute("SET enable_bitmapscan = off");
      }
      try (var ps = conn.prepareStatement(explain + sql)) {
        int p = 1;
        if (filtered) {
          ps.setString(p++, "alice");
        }
        ps.setLong(p++, offset);
        if (filtered) {
          ps.setString(p++, "alice");
        }
        ps.setInt(p++, 20);
        ps.setLong(p, offset);
        var plan = new StringBuilder();
        try (var rs = ps.executeQuery()) {
          while (rs.next()) {
            plan.append(rs.getString(1)).append('\n');
          }
        }
        return plan.toString();
      }
    }
  }

  /** A DataSource whose connections record the SQL of every statement they prepare or run. */
  static final class CountingDataSource {
    final DataSource target;
    final List<String> statements = Collections.synchronizedList(new ArrayList<>());
    final AtomicInteger connections = new AtomicInteger();

    final java.util.function.Predicate<String> failing;

    CountingDataSource(DataSource target) {
      this(target, sql -> false);
    }

    /** Fails every statement {@code failing} matches with a SQL state that is not a race. */
    CountingDataSource(DataSource target, java.util.function.Predicate<String> failing) {
      this.target = target;
      this.failing = failing;
    }

    DataSource proxy() {
      return (DataSource)
          Proxy.newProxyInstance(
              DataSource.class.getClassLoader(),
              new Class<?>[] {DataSource.class},
              (p, method, args) -> {
                Object result = invoke(target, method, args);
                if ("getConnection".equals(method.getName())) {
                  connections.incrementAndGet();
                  return connection((Connection) result);
                }
                return result;
              });
    }

    private Connection connection(Connection conn) {
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (p, method, args) -> {
                if ("prepareStatement".equals(method.getName())) {
                  statements.add((String) args[0]);
                } else if ("createStatement".equals(method.getName())) {
                  var statement = (java.sql.Statement) invoke(conn, method, args);
                  return Proxy.newProxyInstance(
                      java.sql.Statement.class.getClassLoader(),
                      new Class<?>[] {java.sql.Statement.class},
                      (sp, sm, sargs) -> {
                        if (sm.getName().startsWith("execute") && sargs != null) {
                          statements.add((String) sargs[0]);
                          if (failing.test((String) sargs[0])) {
                            throw new SQLException("injected", "XX000");
                          }
                        }
                        return invoke(statement, sm, sargs);
                      });
                }
                return invoke(conn, method, args);
              });
    }

    private static Object invoke(Object target, java.lang.reflect.Method method, Object[] args)
        throws Throwable {
      try {
        return method.invoke(target, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }
}
