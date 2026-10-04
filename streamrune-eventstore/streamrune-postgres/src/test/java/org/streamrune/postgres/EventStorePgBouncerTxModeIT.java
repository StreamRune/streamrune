package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * What the production guide tells an operator who puts a transaction-mode PgBouncer between the
 * application and PostgreSQL, proven against a real one. {@link LeaseLeadershipPgBouncerTxModeIT}
 * covers leadership and fencing; this class covers the rest of the guide's pooler note:
 *
 * <ul>
 *   <li><b>The event store leaves no session state behind.</b> Its statement bound is {@code SET
 *       LOCAL} inside an append transaction or a JDBC query timeout (a cancel request that has to
 *       find its way back through the pooler) on every other statement, so nothing of it stays on a
 *       server connection another client is later linked to. A session-level {@code SET} the
 *       <em>application</em> issues (a pool's connection-init SQL) is the opposite: PgBouncer does
 *       not track it, so it stays on the one server connection it ran on. A role or database
 *       setting reaches every server connection.
 *   <li><b>Server-side prepared statements need pooler support.</b> pgjdbc switches to named
 *       prepared statements after five executions. A pooler that does not track them (PgBouncer's
 *       {@code max_prepared_statements} exists from 1.21 and is 0 unless set) can link the next
 *       execution to a server connection that never saw the {@code Parse}, which fails with
 *       SQLState 26000. {@code prepareThreshold=0} or a tracking pooler avoids it.
 *   <li><b>Flyway needs two server connections at once.</b> Flyway 13 takes its PostgreSQL lock as
 *       {@code pg_try_advisory_xact_lock} inside a transaction, which a transaction-mode pooler
 *       keeps on one server connection for the whole migration, and then runs the migration on a
 *       second connection. Replicas started together migrate once while the pool has server
 *       connections to spare; a pool of one cannot migrate at all.
 * </ul>
 *
 * <p>Three PgBouncer instances (the image and version of {@link LeaseLeadershipPgBouncerTxModeIT})
 * front one PostgreSQL: the default {@code max_prepared_statements=0} with four server connections,
 * the same tracking prepared statements, and one with a single server connection. The wildcard
 * database entry lets each test use a database of its own.
 */
@Testcontainers
class EventStorePgBouncerTxModeIT {

  private static final String USER = "srune";
  private static final String PASSWORD = "srune";

  /** PgBouncer's {@code default_pool_size}: server connections per database and user. */
  private static final int POOL_SIZE = 4;

  /** Statement bound of the bounded tests: short enough to be quick, long enough to be real. */
  private static final Duration BOUND = Duration.ofMillis(300);

  /** PostgreSQL {@code query_canceled}: a statement timeout or a cancel request. */
  private static final String QUERY_CANCELED = "57014";

  /** PostgreSQL {@code invalid_sql_statement_name}: the named statement is not on this backend. */
  private static final String INVALID_STATEMENT_NAME = "26000";

  private static final Network NETWORK = Network.newNetwork();

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("srune")
          .withUsername(USER)
          .withPassword(PASSWORD)
          .withNetwork(NETWORK)
          .withNetworkAliases("pg");

  /** Transaction pooling, {@code max_prepared_statements} left at its default of 0. */
  @Container static final GenericContainer<?> PGBOUNCER = pgbouncer(POOL_SIZE, 20, null);

  /** The same pooler, tracking protocol-level prepared statements (PgBouncer 1.21 and later). */
  @Container static final GenericContainer<?> PGBOUNCER_TRACKING = pgbouncer(POOL_SIZE, 20, "100");

  /** The project's other pooler topology: one server connection for every client. */
  @Container static final GenericContainer<?> PGBOUNCER_ONE_SERVER = pgbouncer(1, 10, null);

  record Noted(String value) implements DomainEvent {}

  record NotedState(String value) implements AggregateState {}

  static final EventTypeRegistry REGISTRY =
      SimpleEventTypeRegistry.builder()
          .registerEvent("Noted", Noted.class)
          .registerState("NotedState", NotedState.class)
          .build();

  private static GenericContainer<?> pgbouncer(
      int poolSize, int queryWaitTimeoutSeconds, String maxPreparedStatements) {
    GenericContainer<?> container =
        new GenericContainer<>(DockerImageName.parse("edoburu/pgbouncer:v1.23.1-p3"))
            .withNetwork(NETWORK)
            .withEnv("DB_HOST", "pg")
            .withEnv("DB_PORT", "5432")
            .withEnv("DB_USER", USER)
            .withEnv("DB_PASSWORD", PASSWORD)
            .withEnv("POOL_MODE", "transaction")
            .withEnv("AUTH_TYPE", "scram-sha-256")
            .withEnv("DEFAULT_POOL_SIZE", String.valueOf(poolSize))
            .withEnv("MAX_CLIENT_CONN", "200")
            // A client that waits for a server connection that never frees up fails after this
            // long instead of PgBouncer's default two minutes.
            .withEnv("QUERY_WAIT_TIMEOUT", String.valueOf(queryWaitTimeoutSeconds))
            .withExposedPorts(5432)
            .dependsOn(PG)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)));
    if (maxPreparedStatements != null) {
      container.withEnv("MAX_PREPARED_STATEMENTS", maxPreparedStatements);
    }
    return container;
  }

  // ---------------------------------------------------------------- the event store

  @Test
  @Timeout(180)
  void theEventStoreRunsConcurrentWorkThroughTheTransactionPooler() throws Exception {
    String db = createDatabase("store_through_pooler");
    migrateDirectly(db);
    // More clients than server connections: statements of different clients really share backends.
    try (HikariDataSource pool = pool(PGBOUNCER, db, "prepareThreshold=0", 12)) {
      EventStore store = new PostgresEventStoreFactory(pool, REGISTRY).create();

      int writers = 12;
      int rounds = 25;
      ExecutorService executor = Executors.newFixedThreadPool(writers);
      try {
        List<Future<Integer>> runs = new ArrayList<>();
        for (int w = 0; w < writers; w++) {
          StreamId stream = TestStreams.stream("pooled-" + w);
          runs.add(
              executor.submit(
                  () -> {
                    for (int v = 1; v <= rounds; v++) {
                      store.append(stream, List.of(envelope(stream, v)), new Version(v - 1));
                      assertEquals(v, store.load(stream).events().size());
                      store.readStream(stream, Version.initial(), 10);
                    }
                    return store.load(stream).events().size();
                  }));
        }
        for (Future<Integer> run : runs) {
          assertEquals(rounds, run.get(120, TimeUnit.SECONDS));
        }
      } finally {
        executor.shutdownNow();
      }
      assertEquals(writers * rounds, store.readGlobalStream(GlobalOffset.initial(), 1000).size());
    }
  }

  @Test
  @Timeout(120)
  void theStatementBoundHoldsThroughThePoolerAndLeavesNothingOnAnyServerConnection()
      throws Exception {
    String db = createDatabase("bound_through_pooler");
    migrateDirectly(db);
    try (HikariDataSource pool = pool(PGBOUNCER, db, "prepareThreshold=0", 4)) {
      EventStore store =
          new PostgresEventStoreFactory(pool, REGISTRY).statementTimeout(BOUND).create();
      StreamId stream = TestStreams.stream("bound-through-pooler");
      store.append(stream, List.of(envelope(stream, 1)), Version.initial());

      // An append queued on the global-offset counter: the SET LOCAL of its transaction cancels it.
      withDirectLock(
          db,
          "SELECT next_value FROM global_offset_sequence WHERE id = 1 FOR UPDATE",
          () ->
              assertCanceled(
                  () -> store.append(stream, List.of(envelope(stream, 2)), new Version(1))));
      // Reads queued on the table: the JDBC query timeout is a cancel request that has to find its
      // way back through the pooler to the server connection running the statement.
      withDirectLock(
          db,
          "LOCK TABLE event_stream IN ACCESS EXCLUSIVE MODE",
          () -> {
            assertCanceled(() -> store.load(stream));
            assertCanceled(() -> store.readGlobalStream(GlobalOffset.initial(), 10));
          });

      // Every server connection of the pool, probed at the same moment so each client has its own:
      // none carries a statement_timeout.
      assertThat(statementTimeoutOnEveryServerConnection(PGBOUNCER, db)).containsExactly("0");
      // And the application's own long statement is not cut short by the store's bound.
      try (Connection conn = pool.getConnection();
          var st = conn.createStatement()) {
        st.execute("SELECT pg_sleep(" + (BOUND.toMillis() * 2) / 1000.0 + ")");
      }
      assertEquals(1, store.load(stream).events().size());
    }
  }

  @Test
  @Timeout(120)
  void aSessionSetIssuedThroughThePoolerStaysOnTheServerConnectionItRanOn() throws Exception {
    String db = createDatabase("session_set_through_pooler");
    // What an application's connection-init SQL does: one client, one autocommit SET.
    try (HikariDataSource pool = pool(PGBOUNCER, db, "prepareThreshold=0", 1);
        Connection conn = pool.getConnection();
        var st = conn.createStatement()) {
      st.execute("SET statement_timeout = 12345");
    }
    // The client is gone; a client that never asked for a bound finds it on one of the shared
    // server connections.
    assertThat(statementTimeoutOnEveryServerConnection(PGBOUNCER, db)).contains("12345ms");
  }

  @Test
  @Timeout(120)
  void aRoleOrDatabaseSettingReachesEveryServerConnectionThePoolerOpens() throws Exception {
    String db = createDatabase("database_setting_through_pooler");
    try (Connection conn = directDataSource(db).getConnection();
        var st = conn.createStatement()) {
      st.execute("ALTER DATABASE " + db + " SET statement_timeout = '45s'");
    }
    assertThat(statementTimeoutOnEveryServerConnection(PGBOUNCER, db)).containsExactly("45s");
  }

  // ---------------------------------------------------------------- prepared statements

  @Test
  @Timeout(120)
  void pgjdbcDefaultsFailOnAPoolerThatDoesNotTrackPreparedStatements() throws Exception {
    EventStoreException failure =
        loadsWhileAnotherClientTakesOverTheServerConnection(PGBOUNCER, "");

    assertThat(failure)
        .as("the named statement was prepared on a server connection this client then left")
        .isNotNull();
    assertTrue(
        hasSqlState(failure, INVALID_STATEMENT_NAME),
        "expected prepared statement ... does not exist (26000), got: " + failure);
  }

  @Test
  @Timeout(120)
  void aPrepareThresholdOfZeroNeedsNoPreparedStatementTrackingInThePooler() throws Exception {
    assertThat(loadsWhileAnotherClientTakesOverTheServerConnection(PGBOUNCER, "prepareThreshold=0"))
        .isNull();
  }

  @Test
  @Timeout(120)
  void pgjdbcDefaultsWorkOnAPoolerThatTracksPreparedStatements() throws Exception {
    assertThat(loadsWhileAnotherClientTakesOverTheServerConnection(PGBOUNCER_TRACKING, ""))
        .isNull();
  }

  /**
   * One application client, one server connection: the client loads a stream often enough for the
   * driver to prepare the statement on the server connection it was linked to, then another client
   * takes that server connection (the pooler hands out the one it used last) and the first client's
   * next transactions are linked to a different one.
   *
   * @return the first failure, or {@code null} when every load succeeded
   */
  private static EventStoreException loadsWhileAnotherClientTakesOverTheServerConnection(
      GenericContainer<?> bouncer, String driverParams) throws Exception {
    String db = createDatabase("prepared_" + System.nanoTime());
    migrateDirectly(db);
    try (HikariDataSource application = pool(bouncer, db, driverParams, 1);
        HikariDataSource other = pool(bouncer, db, "prepareThreshold=0", 1)) {
      EventStore store =
          new PostgresEventStoreFactory(application, REGISTRY).autoInitializeSchema(false).create();
      StreamId stream = TestStreams.stream("prepared-statements");
      store.append(stream, List.of(envelope(stream, 1)), Version.initial());
      // More executions than pgjdbc's default threshold of five.
      for (int i = 0; i < 12; i++) {
        store.load(stream);
      }
      try (Connection takeover = other.getConnection();
          var st = takeover.createStatement()) {
        takeover.setAutoCommit(false);
        st.execute("SELECT 1");
        try {
          for (int i = 0; i < 12; i++) {
            store.load(stream);
          }
          return null;
        } catch (EventStoreException e) {
          return e;
        } finally {
          takeover.rollback();
        }
      }
    }
  }

  // ---------------------------------------------------------------- Flyway

  @Test
  @Timeout(120)
  void replicasMigratingThroughThePoolerAtTheSameTimeApplyTheSeriesOnce() throws Exception {
    String db = createDatabase("migrate_through_pooler");
    // Each replica needs one server connection for Flyway's lock transaction and, once it holds the
    // lock, a second one for the migration itself; the replicas waiting for the lock hold theirs
    // meanwhile. With replicas < POOL_SIZE the one holding the lock always finds its second.
    int replicas = POOL_SIZE - 1;
    var start = new CyclicBarrier(replicas);
    ExecutorService executor = Executors.newFixedThreadPool(replicas);
    try {
      List<Future<?>> migrations = new ArrayList<>();
      for (int r = 0; r < replicas; r++) {
        migrations.add(
            executor.submit(
                () -> {
                  try (HikariDataSource application =
                      pool(PGBOUNCER, db, "prepareThreshold=0", 2)) {
                    start.await(30, TimeUnit.SECONDS);
                    new PostgresEventStoreFactory(application, REGISTRY).initializeSchema();
                  }
                  return null;
                }));
      }
      for (Future<?> migration : migrations) {
        migration.get(90, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }

    try (Connection conn = directDataSource(db).getConnection();
        var st = conn.createStatement();
        var rs =
            st.executeQuery(
                "SELECT count(*) FROM flyway_schema_history_streamrune"
                    + " WHERE version = '001' AND success")) {
      assertTrue(rs.next());
      assertEquals(1, rs.getInt(1), "the baseline must be recorded exactly once");
    }
    var validation = SchemaValidator.validate(directDataSource(db));
    assertTrue(
        validation.isValid(), () -> "schema invalid after migrating: " + validation.issues());
  }

  @Test
  @Timeout(120)
  void aPoolerWithOneServerConnectionCannotRunFlywayAndLeavesTheSchemaUntouched() throws Exception {
    String db = createDatabase("migrate_one_server_connection");
    // Flyway holds one server connection in its lock transaction and needs a second one for the
    // migration. The pooler has no second one to give while the first is held: the migration waits
    // for it until query_wait_timeout and fails.
    try (HikariDataSource application = pool(PGBOUNCER_ONE_SERVER, db, "prepareThreshold=0", 2)) {
      var failure =
          assertThrows(
              RuntimeException.class,
              () -> new PostgresEventStoreFactory(application, REGISTRY).initializeSchema());
      assertTrue(
          causeChainMentions(failure, "query_wait_timeout"),
          "the migration must fail waiting for a second server connection, got: " + failure);
    }
    try (Connection conn = directDataSource(db).getConnection();
        var st = conn.createStatement();
        var rs = st.executeQuery("SELECT to_regclass('event_stream')")) {
      assertTrue(rs.next());
      assertThat(rs.getString(1)).as("nothing of the series may be applied").isNull();
    }
  }

  // ---------------------------------------------------------------- helpers

  private static String createDatabase(String name) throws SQLException {
    try (Connection conn = directDataSource("srune").getConnection();
        var st = conn.createStatement()) {
      st.execute("CREATE DATABASE " + name);
    }
    return name;
  }

  private static void migrateDirectly(String database) {
    new PostgresEventStoreFactory(directDataSource(database), REGISTRY).initializeSchema();
  }

  private static PGSimpleDataSource directDataSource(String database) {
    var ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {PG.getHost()});
    ds.setPortNumbers(new int[] {PG.getMappedPort(5432)});
    ds.setDatabaseName(database);
    ds.setUser(USER);
    ds.setPassword(PASSWORD);
    return ds;
  }

  private static String pooledUrl(GenericContainer<?> bouncer, String database, String params) {
    return "jdbc:postgresql://"
        + bouncer.getHost()
        + ":"
        + bouncer.getMappedPort(5432)
        + "/"
        + database
        + (params.isEmpty() ? "" : "?" + params);
  }

  /** A pool the way an application configures it, its URL pointing at the pooler. */
  private static HikariDataSource pool(
      GenericContainer<?> bouncer, String database, String params, int size) {
    var config = new HikariConfig();
    config.setJdbcUrl(pooledUrl(bouncer, database, params));
    config.setUsername(USER);
    config.setPassword(PASSWORD);
    config.setMaximumPoolSize(size);
    config.setMinimumIdle(size);
    config.setConnectionTimeout(30_000);
    config.setPoolName("application-" + System.nanoTime());
    return new HikariDataSource(config);
  }

  /**
   * The {@code statement_timeout} each server connection of {@code bouncer}'s pool for {@code
   * database} reports. {@link #POOL_SIZE} clients each hold a transaction open at the same time, so
   * the pooler has to link every one of them to a server connection of its own.
   */
  private static Set<String> statementTimeoutOnEveryServerConnection(
      GenericContainer<?> bouncer, String database) throws Exception {
    var started = new CountDownLatch(POOL_SIZE);
    var release = new CountDownLatch(1);
    Set<String> seen = java.util.Collections.synchronizedSet(new HashSet<>());
    ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);
    try {
      List<Future<?>> probes = new ArrayList<>();
      for (int i = 0; i < POOL_SIZE; i++) {
        probes.add(
            executor.submit(
                () -> {
                  var ds = new PGSimpleDataSource();
                  ds.setUrl(pooledUrl(bouncer, database, "prepareThreshold=0"));
                  ds.setUser(USER);
                  ds.setPassword(PASSWORD);
                  try (Connection conn = ds.getConnection();
                      var st = conn.createStatement()) {
                    conn.setAutoCommit(false);
                    try (var rs = st.executeQuery("SHOW statement_timeout")) {
                      rs.next();
                      seen.add(rs.getString(1));
                    }
                    started.countDown();
                    release.await(30, TimeUnit.SECONDS);
                    conn.rollback();
                  }
                  return null;
                }));
      }
      assertTrue(
          started.await(30, TimeUnit.SECONDS), "every probe must be linked to a server connection");
      release.countDown();
      for (Future<?> probe : probes) {
        probe.get(30, TimeUnit.SECONDS);
      }
    } finally {
      executor.shutdownNow();
    }
    return seen;
  }

  /**
   * Runs {@code body} while another session, directly on PostgreSQL, holds the lock {@code lockSql}
   * takes.
   */
  private static void withDirectLock(String database, String lockSql, ThrowingRunnable body)
      throws Exception {
    try (Connection holder = directDataSource(database).getConnection()) {
      holder.setAutoCommit(false);
      try (var st = holder.createStatement()) {
        st.execute(lockSql);
      }
      try {
        body.run();
      } finally {
        holder.rollback();
      }
    }
  }

  /** The call must fail with {@code query_canceled} well before the lock holder lets go. */
  private static void assertCanceled(Supplier<?> call) {
    CompletableFuture<?> future = CompletableFuture.supplyAsync(call);
    try {
      future.get(10, TimeUnit.SECONDS);
      fail("the call succeeded while another session held the lock it needs");
    } catch (TimeoutException _) {
      fail("the call was still waiting after 10 s: the statement bound did not reach the server");
    } catch (InterruptedException _) {
      Thread.currentThread().interrupt();
      fail("interrupted");
    } catch (ExecutionException e) {
      Throwable failure = e.getCause();
      assertThat(failure).isInstanceOf(EventStoreException.class);
      assertTrue(
          hasSqlState(failure, QUERY_CANCELED),
          "expected query_canceled (57014) in the cause chain, got: " + failure);
    }
  }

  private static boolean causeChainMentions(Throwable t, String text) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c.getMessage() != null && c.getMessage().contains(text)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasSqlState(Throwable t, String sqlState) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof SQLException sql && sqlState.equals(sql.getSQLState())) {
        return true;
      }
    }
    return false;
  }

  private static EventEnvelope envelope(StreamId stream, long version) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        new Version(version),
        new EventType("Noted"),
        new Noted("v" + version),
        new EventMetadata(
            EventId.of("evt-" + stream.value() + "-" + version),
            CommandId.of("cmd-" + stream.value() + "-" + version),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }
}
