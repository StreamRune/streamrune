package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreFixture;
import org.streamrune.test.SagaForwardFenceContract;
import org.streamrune.test.SagaForwardFenceContract.FenceEvent;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * A transaction left idle while it holds a saga's shared fence lock is ended by its own {@code SET
 * LOCAL idle_in_transaction_session_timeout}, so the exclusive lock a saga-row write asks for is
 * granted within that bound: on a direct connection, through a transaction-pooling PgBouncer, and
 * behind a peer that went silent without closing its socket.
 *
 * <p>The {@code rawSql_…} methods test PostgreSQL (and the pooler) with the statements the stores
 * run; every wait is observed as elapsed time against the bound, and the end of the idle session is
 * read from {@code pg_stat_activity}.
 */
@Testcontainers
class AppendIdleTransactionBoundIT {

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

  /** Transaction pooling, four server connections, SCRAM client auth. */
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

  private static final SagaType TYPE = SagaType.of("bound.IdleSaga");
  private static final SagaId ID = SagaId.of("idle-saga-1");

  /** The idle bound the raw methods set, and the slack a test allows on top of it. */
  private static final long BOUND_MILLIS = 1_000;

  private static final long SLACK_MILLIS = 1_000;

  private static final String LOCK_SHARED =
      "SELECT pg_advisory_xact_lock_shared("
          + SagaFenceLock.CLASS_ID
          + ", "
          + SagaFenceLock.objectId(TYPE, ID)
          + ")";

  private static final String PIN_AND_LOCK_EXCLUSIVE =
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SELECT pg_advisory_xact_lock("
          + SagaFenceLock.CLASS_ID
          + ", "
          + SagaFenceLock.objectId(TYPE, ID)
          + ")";

  private static final String CLAIM =
      "UPDATE saga_state SET status = 'COMPENSATING', version = version + 1, updated_at = NOW(),"
          + " episode_version = version + 1, episode_claimed_at = NOW()"
          + " WHERE saga_id = ? AND saga_type = ? AND version = 1";

  static PGSimpleDataSource direct;

  private final List<Connection> opened = new ArrayList<>();
  private final List<AutoCloseable> closeables = new ArrayList<>();

  @BeforeAll
  static void schema() {
    direct = dataSource(PG.getHost(), PG.getMappedPort(5432), "bound-direct");
    new PostgresEventStoreFactory(direct, NO_EVENTS).create();
  }

  @BeforeEach
  void forwardRow() throws SQLException {
    try (Connection conn = direct.getConnection();
        Statement stmt = conn.createStatement()) {
      stmt.execute("TRUNCATE saga_state, event_stream, command_inbox");
      try (PreparedStatement ps =
          conn.prepareStatement(
              "INSERT INTO saga_state (saga_id, saga_type, status, state_json)"
                  + " VALUES (?, ?, 'RUNNING', '{}'::jsonb)")) {
        ps.setString(1, ID.value());
        ps.setString(2, TYPE.value());
        ps.executeUpdate();
      }
    }
  }

  @AfterEach
  void closeAll() throws Exception {
    for (Connection conn : opened) {
      try {
        conn.close();
      } catch (SQLException ignored) {
        // a connection the server ended
      }
    }
    opened.clear();
    for (AutoCloseable closeable : closeables) {
      closeable.close();
    }
    closeables.clear();
  }

  // ---------------------------------------------------------------------------------------------
  // the event store
  // ---------------------------------------------------------------------------------------------

  /**
   * A saga's forward append stops after its fence read, before its offset reservation, with its
   * calling thread parked: its backend is idle in transaction, holding the command's inbox key and
   * the saga's shared fence lock. The append transaction's own idle bound — the statement timeout,
   * one second here — ends the session; the key and the lock are free again, so the same command
   * through another node commits at once, and the parked call fails once it goes on.
   */
  @Test
  void anAppendLeftIdleInsideItsTransactionIsEndedAtTheStatementTimeout() throws Exception {
    PausingDataSource pausing =
        new PausingDataSource(dataSource(PG.getHost(), PG.getMappedPort(5432), "idle-append"));
    PostgresEventStore silentNode = eventStore(pausing, Duration.ofSeconds(1));
    PostgresEventStore otherNode = eventStore(direct, StatementTimeout.DEFAULT);
    IdempotencyKey key = IdempotencyKey.of("saga:" + ID.value() + ":1:0");

    PausingDataSource.Pause pause = pausing.pauseBefore("UPDATE global_offset_sequence");
    CompletableFuture<IdempotentAppendResult> parked =
        CompletableFuture.supplyAsync(() -> fencedAppend(silentNode, key));
    pause.awaitPaused();
    long idleSince = System.nanoTime();

    awaitNoBackendNamed("idle-append", BOUND_MILLIS + SLACK_MILLIS, idleSince);

    IdempotentAppendResult second = fencedAppend(otherNode, key);
    assertThat(second.alreadyApplied())
        .as("the key was free: the command is stored once")
        .isFalse();

    pause.release();
    Throwable thrown = catchThrowable(() -> parked.get(30, TimeUnit.SECONDS));
    assertThat(thrown).isInstanceOf(ExecutionException.class);
    assertThat(thrown.getCause()).isInstanceOf(EventStoreException.class);
    assertThat(otherNode.readStream(TARGET, Version.initial(), 10)).hasSize(1);
  }

  // ---------------------------------------------------------------------------------------------
  // PostgreSQL and the pooler themselves
  // ---------------------------------------------------------------------------------------------

  @Test
  void rawSql_anIdleTransactionEndsAtItsSetLocalBound_direct() throws Exception {
    Connection holder = open(dataSource(PG.getHost(), PG.getMappedPort(5432), "idle-direct"));
    anIdleHolderOfTheSharedLockIsEndedAtItsBound(holder, "idle-direct", () -> {});
  }

  @Test
  void rawSql_anIdleTransactionEndsAtItsSetLocalBound_throughPgBouncer() throws Exception {
    Connection holder =
        open(dataSource(PGBOUNCER.getHost(), PGBOUNCER.getMappedPort(5432), "idle-pooled"));
    // A pooled server connection keeps the application name of the last client linked to it, so
    // only the holder's own backend is looked for.
    anIdleHolderOfTheSharedLockIsEndedAtItsBound(holder, null, () -> {});
  }

  @Test
  void rawSql_anIdleTransactionEndsAtItsSetLocalBound_behindABlackHole() throws Exception {
    SilentPeerProxy proxy = new SilentPeerProxy(PG.getHost(), PG.getMappedPort(5432));
    closeables.add(proxy);
    PGSimpleDataSource silent = new PGSimpleDataSource();
    silent.setUrl(proxy.jdbcUrl(DB));
    silent.setUser(USER);
    silent.setPassword(PASSWORD);
    silent.setApplicationName("idle-silent");
    silent.setSocketTimeout(10);
    Connection holder = open(silent);
    anIdleHolderOfTheSharedLockIsEndedAtItsBound(
        holder, "idle-silent", proxy::silenceOpenConnections);
  }

  /**
   * {@code holder} opens a transaction, sets the idle bound, takes the saga's shared fence lock and
   * goes idle; {@code cutOff} then runs. A direct connection asks for the exclusive lock and writes
   * the claim: both are granted within the bound plus the slack, and the holder's backend is gone.
   */
  private void anIdleHolderOfTheSharedLockIsEndedAtItsBound(
      Connection holder, String applicationName, Runnable cutOff) throws Exception {
    holder.setAutoCommit(false);
    int holderPid;
    try (Statement st = holder.createStatement()) {
      st.execute("SET LOCAL idle_in_transaction_session_timeout = " + BOUND_MILLIS);
      try (ResultSet rs = st.executeQuery("SELECT pg_backend_pid()")) {
        rs.next();
        holderPid = rs.getInt(1);
      }
      try (ResultSet rs = st.executeQuery(LOCK_SHARED)) {
        rs.next();
      }
    }
    long idleSince = System.nanoTime();
    cutOff.run();

    Connection claimer = open(dataSource(PG.getHost(), PG.getMappedPort(5432), "claimer"));
    claimer.setAutoCommit(false);
    CompletableFuture<Integer> claimed =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                try (Statement st = claimer.createStatement()) {
                  st.execute(PIN_AND_LOCK_EXCLUSIVE);
                }
                int updated;
                try (PreparedStatement ps = claimer.prepareStatement(CLAIM)) {
                  ps.setString(1, ID.value());
                  ps.setString(2, TYPE.value());
                  updated = ps.executeUpdate();
                }
                claimer.commit();
                return updated;
              } catch (SQLException e) {
                throw new IllegalStateException(e);
              }
            });
    int updated;
    try {
      updated = claimed.get(BOUND_MILLIS + SLACK_MILLIS + 10_000, TimeUnit.MILLISECONDS);
    } finally {
      terminate(holderPid); // a red run leaves no holder behind
    }
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - idleSince);
    assertThat(updated).as("the claim writes the row").isEqualTo(1);
    assertThat(elapsed)
        .as("the claim is granted within the idle bound plus the slack")
        .isLessThan(BOUND_MILLIS + SLACK_MILLIS);
    assertThat(backendAlive(holderPid)).as("the idle holder's backend is gone").isFalse();
    if (applicationName != null) {
      assertThat(backendsNamed(applicationName)).as("no backend of " + applicationName).isZero();
    }
  }

  // ---------------------------------------------------------------------------------------------
  // support
  // ---------------------------------------------------------------------------------------------

  private static final StreamId TARGET =
      StreamId.of(AggregateType.of("bound_target"), AggregateId.of("target-1"));

  private static PostgresEventStore eventStore(DataSource source, Duration statementTimeout) {
    return PostgresEventStore.builder()
        .dataSource(source)
        .typeRegistry(FENCE_EVENTS)
        .commandInbox(new PostgresCommandInbox(source))
        .statementTimeout(statementTimeout)
        .build();
  }

  private static IdempotentAppendResult fencedAppend(PostgresEventStore store, IdempotencyKey key) {
    return store.appendWithKey(
        TARGET,
        List.of(
            EventStoreFixture.event(
                TARGET, new Version(1), SagaForwardFenceContract.EVENT_TYPE, new FenceEvent("v1"))),
        Version.initial(),
        key,
        "FenceCommand",
        SagaOrigin.dispatchedBy(TYPE, ID));
  }

  /** Waits until no backend carries {@code applicationName}, failing past {@code boundMillis}. */
  private static void awaitNoBackendNamed(String applicationName, long boundMillis, long since)
      throws Exception {
    while (backendsNamed(applicationName) > 0) {
      long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - since);
      assertThat(elapsed)
          .as("the backend of " + applicationName + " is ended within " + boundMillis + "ms")
          .isLessThan(boundMillis);
      TimeUnit.MILLISECONDS.sleep(20);
    }
  }

  private static final EventTypeRegistry FENCE_EVENTS =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          return FenceEvent.class;
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          throw new IllegalArgumentException("no snapshots here: " + stateType);
        }

        @Override
        public Collection<Class<?>> registeredTypes() {
          return List.of(FenceEvent.class);
        }
      };

  private static final EventTypeRegistry NO_EVENTS =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          throw new IllegalArgumentException("no events here: " + eventType);
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          throw new IllegalArgumentException("no snapshots here: " + stateType);
        }

        @Override
        public Collection<Class<?>> registeredTypes() {
          return List.of();
        }
      };

  static PGSimpleDataSource dataSource(String host, int port, String applicationName) {
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {host});
    ds.setPortNumbers(new int[] {port});
    ds.setDatabaseName(DB);
    ds.setUser(USER);
    ds.setPassword(PASSWORD);
    ds.setApplicationName(applicationName);
    // through a transaction pooler that does not track prepared statements
    ds.setPrepareThreshold(0);
    return ds;
  }

  private Connection open(PGSimpleDataSource source) throws SQLException {
    Connection conn = source.getConnection();
    opened.add(conn);
    return conn;
  }

  private static boolean backendAlive(int pid) throws SQLException {
    try (Connection conn = direct.getConnection();
        PreparedStatement ps =
            conn.prepareStatement("SELECT count(*) FROM pg_stat_activity WHERE pid = ?")) {
      ps.setInt(1, pid);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1) > 0;
      }
    }
  }

  private static int backendsNamed(String applicationName) throws SQLException {
    try (Connection conn = direct.getConnection();
        PreparedStatement ps =
            conn.prepareStatement(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = ?")) {
      ps.setString(1, applicationName);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  private static void terminate(int pid) throws SQLException {
    try (Connection conn = direct.getConnection();
        PreparedStatement ps = conn.prepareStatement("SELECT pg_terminate_backend(?)")) {
      ps.setInt(1, pid);
      ps.executeQuery().close();
    }
  }
}
