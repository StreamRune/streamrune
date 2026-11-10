package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Replicas that read the same projection for the first time at once each create its table. {@code
 * CREATE TABLE IF NOT EXISTS} is not safe under concurrency in PostgreSQL: the statement that loses
 * the race fails on the catalog's unique index ({@code 23505}), as a duplicate table ({@code
 * 42P07}) or as a duplicate row type ({@code 42710}) once the winner has committed, and the
 * repository then takes the table as created.
 */
class JdbcProjectionRepositoryConcurrentTableCreationTest {

  static PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_proj_race");

  static PGSimpleDataSource dataSource;

  record View(String id) {}

  @BeforeAll
  static void setup() {
    PG.start();
    dataSource = new PGSimpleDataSource();
    dataSource.setURL(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    // the batch transaction locks the projection's checkpoint row in the event-store baseline
    org.flywaydb.core.Flyway.configure()
        .dataSource(dataSource)
        .locations(PostgresEventStoreFactory.EVENT_STORE_MIGRATION_LOCATION)
        .table(PostgresEventStoreFactory.EVENT_STORE_HISTORY_TABLE)
        .load()
        .migrate();
  }

  @AfterAll
  static void teardown() {
    PG.stop();
  }

  @Test
  void replicasReadingANewProjectionAtOnce_allSucceedAndTheTableExists() throws Exception {
    int replicas = 8;
    int rounds = 25;
    List<String> failures = Collections.synchronizedList(new ArrayList<>());
    ExecutorService pool = Executors.newFixedThreadPool(replicas);
    try {
      for (int round = 0; round < rounds; round++) {
        ProjectionName name = ProjectionName.of("race_" + round);
        CyclicBarrier start = new CyclicBarrier(replicas);
        List<Future<?>> reads = new ArrayList<>();
        for (int r = 0; r < replicas; r++) {
          // one repository per replica: each has its own in-process table check
          JdbcProjectionRepository replica = new JdbcProjectionRepository(dataSource);
          reads.add(
              pool.submit(
                  () -> {
                    try {
                      start.await(10, TimeUnit.SECONDS);
                      replica.findById(name, "id-1", View.class);
                    } catch (Throwable t) {
                      failures.add(describe(t));
                    }
                  }));
        }
        for (Future<?> read : reads) {
          read.get(30, TimeUnit.SECONDS);
        }
        assertThat(tableExists(name.value() + "_view")).as("table of %s", name).isTrue();
      }
    } finally {
      pool.shutdownNow();
    }
    assertThat(failures).isEmpty();
  }

  @ParameterizedTest
  @ValueSource(strings = {"23505", "42P07", "42710"})
  void eachWayPostgresReportsALostCreationRace_isTakenAsTheTableCreatedByAnotherReplica(
      String sqlState) {
    var injecting = new FailingCreate(dataSource, true, sqlState, 1);
    var repo = new JdbcProjectionRepository(injecting.proxy());
    String name = "dup_" + sqlState.toLowerCase(java.util.Locale.ROOT);

    assertThat(repo.findById(ProjectionName.of(name), "id-1", View.class)).isEmpty();

    assertThat(injecting.creates.get()).isEqualTo(2);
    assertThat(tableExists(name + "_view")).isTrue();
  }

  @Test
  void aDuplicateTable_isTakenAsTheTableCreatedByAnotherReplica_onAManualCommitConnection() {
    var injecting = new FailingCreate(dataSource, false, "42P07", 1);
    var repo = new JdbcProjectionRepository(injecting.proxy());

    assertThat(repo.findById(ProjectionName.of("dup_table"), "id-1", View.class)).isEmpty();

    assertThat(injecting.creates.get()).isEqualTo(2);
    assertThat(tableExists("dup_table_view")).isTrue();
  }

  @Test
  void aSecondCollision_isRecoveredByTheThirdAttempt() {
    var injecting = new FailingCreate(dataSource, true, "42P07", 2);
    var repo = new JdbcProjectionRepository(injecting.proxy());

    assertThat(repo.findById(ProjectionName.of("dup_twice"), "id-1", View.class)).isEmpty();

    assertThat(injecting.creates.get()).isEqualTo(3);
    assertThat(tableExists("dup_twice_view")).isTrue();
  }

  @Test
  void aCollisionThatPersists_failsAfterThreeAttempts_andTheNextCallCreatesTheTable() {
    var injecting = new FailingCreate(dataSource, true, "23505", 3);
    var repo = new JdbcProjectionRepository(injecting.proxy());
    var name = ProjectionName.of("dup_thrice");

    assertThatThrownBy(() -> repo.findById(name, "id-1", View.class))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("dup_thrice_view");
    assertThat(injecting.creates.get()).isEqualTo(3);
    assertThat(tableExists("dup_thrice_view")).isFalse();

    // the failure was not recorded as a created table: the next call creates it
    assertThat(repo.findById(name, "id-1", View.class)).isEmpty();
    assertThat(injecting.creates.get()).isEqualTo(4);
    assertThat(tableExists("dup_thrice_view")).isTrue();
  }

  @Test
  void anyOtherFailure_isNotRetried() {
    var injecting = new FailingCreate(dataSource, true, "42501", 1);
    var repo = new JdbcProjectionRepository(injecting.proxy());

    assertThatThrownBy(() -> repo.findById(ProjectionName.of("denied"), "id-1", View.class))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("denied_view");
    assertThat(injecting.creates.get()).isEqualTo(1);
    assertThat(tableExists("denied_view")).isFalse();
  }

  @Test
  void inABatchTransaction_aLostCreationRace_rollsBackOnlyTheCreate_andTheBatchCommits() {
    var injecting = new FailingCreate(dataSource, false, "23505", 1);
    var repo = new JdbcProjectionRepository(injecting.proxy());
    var name = ProjectionName.of("dup_in_batch");

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> tx.save(name, "a", new View("a")),
        new PostgresOffsetStore(dataSource));

    assertThat(injecting.creates.get()).isEqualTo(2);
    assertThat(new JdbcProjectionRepository(dataSource).findById(name, "a", View.class))
        .contains(new View("a"));
  }

  @Test
  void inABatchTransaction_aCollisionThatPersists_failsTheBatch_andTheNextBatchCreatesTheTable() {
    var injecting = new FailingCreate(dataSource, false, "42P07", 3);
    var repo = new JdbcProjectionRepository(injecting.proxy());
    var name = ProjectionName.of("dup_in_batch_twice");

    assertThatThrownBy(
            () ->
                repo.executeAtomically(
                    name,
                    List.of(),
                    GlobalOffset.of(1),
                    0L,
                    tx -> tx.save(name, "a", new View("a")),
                    new PostgresOffsetStore(dataSource)))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("dup_in_batch_twice_view");
    assertThat(injecting.creates.get()).isEqualTo(3);
    assertThat(tableExists("dup_in_batch_twice_view")).isFalse();

    repo.executeAtomically(
        name,
        List.of(),
        GlobalOffset.of(1),
        0L,
        tx -> tx.save(name, "a", new View("a")),
        new PostgresOffsetStore(dataSource));
    assertThat(injecting.creates.get()).isEqualTo(4);
    assertThat(new JdbcProjectionRepository(dataSource).findById(name, "a", View.class))
        .contains(new View("a"));
  }

  @Test
  void inABatchTransaction_anyOtherFailure_isNotRetried() {
    var injecting = new FailingCreate(dataSource, false, "42501", 1);
    var repo = new JdbcProjectionRepository(injecting.proxy());
    var name = ProjectionName.of("denied_in_batch");

    assertThatThrownBy(
            () ->
                repo.executeAtomically(
                    name,
                    List.of(),
                    GlobalOffset.of(1),
                    0L,
                    tx -> tx.save(name, "a", new View("a")),
                    new PostgresOffsetStore(dataSource)))
        .isInstanceOf(EventStoreException.class)
        .hasMessageContaining("denied_in_batch_view");
    assertThat(injecting.creates.get()).isEqualTo(1);
    assertThat(tableExists("denied_in_batch_view")).isFalse();
  }

  @Test
  void aReadThatWaitsOnAnotherSessionsCreation_succeedsOnceThatSessionCommits() throws Exception {
    var name = ProjectionName.of("real_race_read");
    var counting = new FailingCreate(dataSource, true, "unused", 0);
    var repo = new JdbcProjectionRepository(counting.proxy());

    try (Connection other = dataSource.getConnection()) {
      other.setAutoCommit(false);
      createUncommitted(other, "real_race_read_view");
      Future<?> read = runAsync(() -> repo.findById(name, "id-1", View.class));
      awaitBlockedCreate("real_race_read_view");
      other.commit();
      read.get(30, TimeUnit.SECONDS);
    }

    // the first attempt waited on the other session's commit and then lost; the second found it
    assertThat(counting.creates.get()).isEqualTo(2);
  }

  @Test
  void aBatchThatWaitsOnAnotherSessionsCreation_commitsOnceThatSessionCommits() throws Exception {
    var name = ProjectionName.of("real_race_batch");
    var counting = new FailingCreate(dataSource, false, "unused", 0);
    var repo = new JdbcProjectionRepository(counting.proxy());

    try (Connection other = dataSource.getConnection()) {
      other.setAutoCommit(false);
      createUncommitted(other, "real_race_batch_view");
      Future<?> batch =
          runAsync(
              () ->
                  repo.executeAtomically(
                      name,
                      List.of(),
                      GlobalOffset.of(1),
                      0L,
                      tx -> tx.save(name, "a", new View("a")),
                      new PostgresOffsetStore(dataSource)));
      awaitBlockedCreate("real_race_batch_view");
      other.commit();
      batch.get(30, TimeUnit.SECONDS);
    }

    // the lost statement was rolled back to its savepoint, and the batch went on and committed
    assertThat(counting.creates.get()).isEqualTo(2);
    assertThat(new JdbcProjectionRepository(dataSource).findById(name, "a", View.class))
        .contains(new View("a"));
  }

  /** Another session's creation of {@code table}, the repository's own statement, not committed. */
  private static void createUncommitted(Connection other, String table) throws SQLException {
    try (Statement stmt = other.createStatement()) {
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS "
              + table
              + " (id VARCHAR(255) PRIMARY KEY, data JSONB NOT NULL,"
              + " version BIGINT NOT NULL DEFAULT 0, updated_at TIMESTAMPTZ DEFAULT NOW())");
    }
  }

  private static Future<?> runAsync(Runnable work) {
    ExecutorService single = Executors.newSingleThreadExecutor();
    try {
      return single.submit(work);
    } finally {
      single.shutdown();
    }
  }

  /** Waits until a session's CREATE TABLE of {@code table} waits on a lock. */
  private static void awaitBlockedCreate(String table) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
    try (Connection c = dataSource.getConnection();
        var ps =
            c.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'"
                    + " AND query LIKE ?")) {
      ps.setString(1, "CREATE TABLE IF NOT EXISTS " + table + "%");
      while (System.nanoTime() < deadline) {
        try (var rs = ps.executeQuery()) {
          rs.next();
          if (rs.getInt(1) > 0) {
            return;
          }
        }
        Thread.sleep(20);
      }
    }
    throw new AssertionError("no CREATE TABLE of " + table + " waited on a lock");
  }

  /** The failure and its causes, each with its SQLState, so a failed race names what lost. */
  private static String describe(Throwable failure) {
    var text = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      text.append(t);
      if (t instanceof SQLException sql) {
        text.append(" [SQLState ").append(sql.getSQLState()).append(']');
      }
      text.append(" <- ");
    }
    return text.toString();
  }

  private static boolean tableExists(String table) {
    try (Connection c = dataSource.getConnection();
        var ps = c.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
      ps.setString(1, "public." + table);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * A data source whose connections have the given autoCommit mode and whose first {@code failures}
   * {@code CREATE TABLE} statements fail with {@code sqlState} instead of running.
   */
  private static final class FailingCreate {
    final AtomicInteger creates = new AtomicInteger();
    private final DataSource target;
    private final boolean autoCommit;
    private final String sqlState;
    private final int failures;

    FailingCreate(DataSource target, boolean autoCommit, String sqlState, int failures) {
      this.target = target;
      this.autoCommit = autoCommit;
      this.sqlState = sqlState;
      this.failures = failures;
    }

    DataSource proxy() {
      return proxy(
          DataSource.class,
          target,
          (method, args, result) ->
              "getConnection".equals(method.getName()) ? connection((Connection) result) : result);
    }

    private Connection connection(Connection real) throws SQLException {
      real.setAutoCommit(autoCommit);
      return proxy(
          Connection.class,
          real,
          (method, args, result) ->
              "createStatement".equals(method.getName()) ? statement((Statement) result) : result);
    }

    private Statement statement(Statement real) {
      InvocationHandler handler =
          (p, method, args) -> {
            if ("execute".equals(method.getName())
                && args != null
                && args[0] instanceof String sql
                && sql.startsWith("CREATE TABLE")
                && creates.incrementAndGet() <= failures) {
              throw new SQLException("injected collision", sqlState);
            }
            return invoke(real, method, args);
          };
      return (Statement)
          Proxy.newProxyInstance(
              Statement.class.getClassLoader(), new Class<?>[] {Statement.class}, handler);
    }

    @FunctionalInterface
    private interface Wrap {
      Object apply(java.lang.reflect.Method method, Object[] args, Object result) throws Exception;
    }

    private static <T> T proxy(Class<T> type, T real, Wrap wrap) {
      InvocationHandler handler =
          (p, method, args) -> wrap.apply(method, args, invoke(real, method, args));
      return type.cast(
          Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, handler));
    }

    private static Object invoke(Object real, java.lang.reflect.Method method, Object[] args)
        throws Throwable {
      try {
        return method.invoke(real, args);
      } catch (InvocationTargetException e) {
        throw e.getCause();
      }
    }
  }
}
