package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.postgres.SagaFenceNodes.TYPE;
import static org.streamrune.postgres.SagaFenceNodes.onAnotherThread;
import static org.streamrune.postgres.SagaFenceNodes.outcomeOf;
import static org.streamrune.postgres.SagaFenceNodes.sagaOf;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.jdbc.PgStatement;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.postgres.SagaFenceNodes.Node;
import org.streamrune.postgres.SagaFenceNodes.Opened;
import org.streamrune.postgres.SagaFenceNodes.Reserve;
import org.streamrune.postgres.SagaFenceNodes.Ship;
import org.streamrune.postgres.SagaFenceNodes.State;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * The forward fence and its bounds through a transaction-pooling PgBouncer: two processes reach the
 * database through the pooler, a helper connection reaches it directly.
 *
 * <p>The pooler runs {@code POOL_MODE=transaction} with four server connections: the frozen-append
 * case needs three at once (the helper is direct, but the append and the claim each hold one, and
 * the claim's poll reads the row on another), and with one server connection the claim would wait
 * in the pooler's queue instead of on the saga's fence lock. Client server-side prepared statements
 * are off ({@code prepareThreshold=0}), as the production guide tells operators.
 *
 * <p>A saga-row write's bound is the statement's JDBC query timeout: the driver sends a cancel
 * request, which the pooler forwards to the server connection its client is linked to. The first
 * cases show that the cancel ends the write's wait at its bound, and that a cancel arriving after
 * the statement ended cancels nothing else.
 */
@Testcontainers
@Timeout(180)
class SagaForwardFencePgBouncerTxModeIT {

  private static final String DB = "srune";
  private static final String USER = "srune";
  private static final String PASSWORD = "srune";

  private static final Network NETWORK = Network.newNetwork();

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName(DB)
          .withUsername(USER)
          .withPassword(PASSWORD)
          .withNetwork(NETWORK)
          .withNetworkAliases("pg");

  @Container
  static final GenericContainer<?> PGBOUNCER =
      new GenericContainer<>(DockerImageName.parse("edoburu/pgbouncer:v1.23.1-p3"))
          .withNetwork(NETWORK)
          .withEnv("DB_HOST", "pg")
          .withEnv("DB_PORT", "5432")
          .withEnv("DB_USER", USER)
          .withEnv("DB_PASSWORD", PASSWORD)
          .withEnv("DB_NAME", DB)
          .withEnv("POOL_MODE", "transaction")
          .withEnv("AUTH_TYPE", "scram-sha-256")
          .withEnv("DEFAULT_POOL_SIZE", "4")
          .withEnv("MAX_CLIENT_CONN", "100")
          .withExposedPorts(5432)
          .dependsOn(PG)
          .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(120)));

  private static final String ORDER = "1";
  private static final long SLACK_MILLIS = 1_000;

  static PGSimpleDataSource direct;

  private SagaFenceNodes nodes;
  private final List<Connection> opened = new ArrayList<>();

  @BeforeAll
  static void schema() {
    direct = dataSource(PG.getHost(), PG.getMappedPort(5432), "pooler-test");
    new PostgresEventStoreFactory(direct, SagaFenceNodes.REGISTRY).create();
  }

  @BeforeEach
  void fresh() throws Exception {
    nodes = new SagaFenceNodes(direct);
    nodes.clean();
  }

  @AfterEach
  void closeAll() {
    nodes.closeAll();
    for (Connection conn : opened) {
      try {
        conn.close();
      } catch (SQLException ignored) {
        // a connection the server or the pooler ended
      }
    }
    opened.clear();
  }

  // ---- the bound of a saga-row write, through the pooler ----

  /**
   * A forward append in flight holds the saga's shared fence lock through one client; a claim with
   * a one-second bound, through another client, is cancelled at its bound (SQLState {@code 57014})
   * and leaves the row as it was. Once the append ends the same claim is granted, and the next
   * statement through the same client — longer than the bound — is not cancelled.
   */
  @Test
  void aSagaRowWriteWaitingOnTheFenceIsCancelledAtItsBound_throughPgBouncer() throws Exception {
    SagaId saga = sagaOf("bound");
    Connection writer = open("writer");
    PostgresSagaStore store =
        new PostgresSagaStore(onlyThrough(writer), SagaFenceNodes.mapper(), Duration.ofSeconds(1));
    store.create(saga, TYPE, new State(SagaStatus.RUNNING, "bound"), SagaStatus.RUNNING, false);
    Connection append = open("append");
    append.setAutoCommit(false);
    execute(
        append,
        "SELECT pg_advisory_xact_lock_shared("
            + SagaFenceLock.CLASS_ID
            + ", "
            + SagaFenceLock.objectId(TYPE, saga)
            + ")");

    long started = System.nanoTime();
    Throwable thrown =
        failureWithinCap(
            () ->
                store.claimCompensating(
                    saga, TYPE, new State(SagaStatus.COMPENSATING, "bound"), 1L, null));
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

    assertThat(thrown).isInstanceOf(EventStoreException.class);
    assertThat(sqlStateOf(thrown)).isEqualTo("57014");
    assertThat(elapsed).isLessThan(1_000 + SLACK_MILLIS);
    assertThat(nodes.row("bound").orElseThrow().episodeVersion()).isNull();

    append.commit();
    store.claimCompensating(saga, TYPE, new State(SagaStatus.COMPENSATING, "bound"), 1L, null);
    assertThat(nodes.status("bound")).isEqualTo(SagaStatus.COMPENSATING);
    execute(writer, "SELECT pg_sleep(1.5)");
  }

  /**
   * The cancel race: the append lets go at about the moment the writer's query timeout fires, so
   * the cancel request can reach the pooler after the statement ended. The next statement through
   * the writer's client, and a statement of another client running on the shared server
   * connections, are not cancelled.
   */
  @Test
  void aCancelArrivingAfterItsStatementCancelsNothingElse_throughPgBouncer() throws Exception {
    String lockShared =
        "SELECT pg_advisory_xact_lock_shared("
            + SagaFenceLock.CLASS_ID
            + ", "
            + SagaFenceLock.objectId(TYPE, sagaOf("race"))
            + ")";
    String lockExclusive =
        "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SELECT pg_advisory_xact_lock("
            + SagaFenceLock.CLASS_ID
            + ", "
            + SagaFenceLock.objectId(TYPE, sagaOf("race"))
            + ")";
    for (int i = 0; i < 10; i++) {
      Connection append = open("race-append");
      Connection writer = open("race-writer");
      Connection other = open("race-other");
      append.setAutoCommit(false);
      writer.setAutoCommit(false);
      execute(append, lockShared);
      long letGoAfter = 950 + ThreadLocalRandom.current().nextLong(100);
      CompletableFuture<Void> letGo =
          onAnotherThread(
              () -> {
                try {
                  TimeUnit.MILLISECONDS.sleep(letGoAfter);
                  append.commit();
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      try (Statement st = writer.createStatement()) {
        st.unwrap(PgStatement.class).setQueryTimeoutMs(1_000);
        st.execute(lockExclusive);
        writer.commit();
      } catch (SQLException cancelled) {
        assertThat(cancelled.getSQLState()).isEqualTo("57014");
        writer.rollback();
      }
      assertThat(outcomeOf(letGo)).isNull();

      CompletableFuture<Void> otherClient =
          onAnotherThread(
              () -> {
                try {
                  execute(other, "SELECT pg_sleep(0.3)");
                } catch (SQLException e) {
                  throw new IllegalStateException(e);
                }
              });
      execute(writer, "SELECT pg_sleep(0.3)");
      writer.commit();
      assertThat(outcomeOf(otherClient)).as("another client's statement").isNull();
    }
  }

  // ---- two processes through the pooler ----

  @Test
  void theClaimCommitsBeforeTheFirstForwardAppend() throws Exception {
    Node a = nodes.node("node-a", pooled("node-a"));
    Node b = nodes.node("node-b", pooled("node-b"));
    a.saga.startAlsoShips = true;
    EventEnvelope start = nodes.append(new Opened(ORDER));
    a.bus.runOnceBefore(Reserve.class, () -> nodes.timeOut(b));

    a.deliver(start);

    assertThat(nodes.storedOrder(ORDER)).containsExactly("Released", "Refunded");
    assertThat(a.fencedSamples).containsExactly(TYPE.value());
    assertThat(nodes.status(ORDER)).isEqualTo(SagaStatus.COMPENSATED);
    nodes.assertTheFenceHeld(ORDER);
  }

  @Test
  void theClaimArrivesWhileTheAppendIsFrozenHoldingTheFence() throws Exception {
    Node a = nodes.node("node-a", pooled("node-a"));
    Node b = nodes.node("node-b", pooled("node-b"));
    a.saga.startAlsoShips = true;
    EventEnvelope start = nodes.append(new Opened(ORDER));
    Connection freezer = nodes.freezeAppends();
    opened.add(freezer);
    int freezerPid = SagaFenceNodes.backendPid(freezer);

    CompletableFuture<Void> delivery = onAnotherThread(() -> a.deliver(start));
    int appendPid = nodes.awaitBlockedBy(freezerPid, "node-a");
    CompletableFuture<Void> claim = onAnotherThread(() -> nodes.timeOut(b));
    int claimPid = nodes.awaitBlockedBy(appendPid, "node-b");

    SagaFenceNodes.Wait waiting = nodes.waitOf(claimPid);
    assertThat(waiting.eventType()).isEqualTo("Lock");
    assertThat(waiting.event()).as("the saga's fence lock").isEqualTo("advisory");
    assertThat(waiting.query()).contains("pg_advisory_xact_lock(");
    assertThat(claim).as("the claim waits for the frozen append").isNotDone();

    freezer.commit();

    assertThat(outcomeOf(claim)).isNull();
    assertThat(outcomeOf(delivery)).isNull();
    assertThat(nodes.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
    assertThat(a.fencedSamples).as("the second forward command").containsExactly(TYPE.value());
    assertThat(nodes.status(ORDER)).isEqualTo(SagaStatus.COMPENSATED);
    nodes.assertTheFenceHeld(ORDER);
  }

  @Test
  void theClaimLandsBetweenTwoForwardCommands() throws Exception {
    Node a = nodes.node("node-a", pooled("node-a"));
    Node b = nodes.node("node-b", pooled("node-b"));
    a.saga.startAlsoShips = true;
    EventEnvelope start = nodes.append(new Opened(ORDER));
    a.bus.runOnceBefore(Ship.class, () -> nodes.timeOut(b));

    a.deliver(start);

    assertThat(nodes.storedOrder(ORDER)).containsExactly("Reserved", "Released", "Refunded");
    assertThat(a.fencedSamples).containsExactly(TYPE.value());
    assertThat(nodes.status(ORDER)).isEqualTo(SagaStatus.COMPENSATED);
    nodes.assertTheFenceHeld(ORDER);
  }

  /**
   * A forward append through the pooler stops after its fence read with its calling thread parked:
   * its server connection is idle in transaction, holding the saga's shared fence lock. The append
   * transaction's idle bound (A's statement timeout, one second) ends that session, and B's claim
   * through the pooler is granted within the bound.
   */
  @Test
  void anIdleAppendIsEndedAtTheStatementTimeout() throws Exception {
    Duration statementTimeout = Duration.ofSeconds(1);
    PausingDataSource pausing = new PausingDataSource(pooled("node-a"));
    Node a = nodes.node("node-a", pausing, statementTimeout, PostgresSagaStore.ROW_WRITE_TIMEOUT);
    Node b = nodes.node("node-b", pooled("node-b"));
    EventEnvelope start = nodes.append(new Opened(ORDER));

    PausingDataSource.Pause pause = pausing.pauseBefore("UPDATE global_offset_sequence");
    CompletableFuture<Void> delivery = onAnotherThread(() -> a.deliver(start));
    pause.awaitPaused();
    long idleSince = System.nanoTime();
    int appendPid = nodes.idleInTransaction("node-a");

    nodes.timeOut(b);
    long claimedAfter = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - idleSince);

    assertThat(nodes.row(ORDER).orElseThrow().episodeVersion()).as("B claimed").isNotNull();
    assertThat(claimedAfter).isLessThan(statementTimeout.toMillis() + SLACK_MILLIS + 500);
    nodes.awaitGone(appendPid);

    pause.release();
    assertThat(outcomeOf(delivery)).isInstanceOf(EventStoreException.class);
    a.deliver(start);
    assertThat(nodes.storedOrder(ORDER)).containsExactly("Released", "Refunded");
    assertThat(nodes.status(ORDER)).isEqualTo(SagaStatus.COMPENSATED);
    nodes.assertTheFenceHeld(ORDER);
  }

  // ---------------------------------------------------------------------------------------------

  private static PGSimpleDataSource dataSource(String host, int port, String applicationName) {
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {host});
    ds.setPortNumbers(new int[] {port});
    ds.setDatabaseName(DB);
    ds.setUser(USER);
    ds.setPassword(PASSWORD);
    ds.setApplicationName(applicationName);
    ds.setPrepareThreshold(0);
    return ds;
  }

  /** A data source whose connections are clients of the pooler. */
  private static PGSimpleDataSource pooled(String applicationName) {
    return dataSource(PGBOUNCER.getHost(), PGBOUNCER.getMappedPort(5432), applicationName);
  }

  private Connection open(String applicationName) throws SQLException {
    Connection conn = pooled(applicationName).getConnection();
    opened.add(conn);
    return conn;
  }

  /** A data source that hands out {@code connection} every time and never closes it. */
  private static DataSource onlyThrough(Connection connection) {
    Connection unclosable =
        (Connection)
            Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("close")) {
                    return null;
                  }
                  try {
                    return method.invoke(connection, args);
                  } catch (InvocationTargetException e) {
                    throw e.getCause();
                  }
                });
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              if (method.getName().equals("getConnection")) {
                return unclosable;
              }
              throw new UnsupportedOperationException(method.getName());
            });
  }

  private static void execute(Connection conn, String sql) throws SQLException {
    try (Statement st = conn.createStatement()) {
      st.execute(sql);
    }
  }

  /** Runs {@code write} on another thread; a write still waiting after ten seconds fails. */
  private static Throwable failureWithinCap(Runnable write) {
    CompletableFuture<Void> running = CompletableFuture.runAsync(write);
    try {
      running.get(10, TimeUnit.SECONDS);
      return null;
    } catch (ExecutionException e) {
      return e.getCause();
    } catch (TimeoutException | InterruptedException e) {
      throw new AssertionError("the write was still waiting after ten seconds", e);
    }
  }

  private static String sqlStateOf(Throwable thrown) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t instanceof SQLException sql) {
        return sql.getSQLState();
      }
    }
    return null;
  }
}
