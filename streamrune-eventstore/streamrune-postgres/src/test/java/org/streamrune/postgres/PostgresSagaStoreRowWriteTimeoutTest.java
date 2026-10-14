package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.jdbc.PgStatement;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * Every {@code saga_state} write of {@link PostgresSagaStore} waits at most its bound: a claim
 * behind a forward append that holds the saga's shared fence lock, and a write behind another
 * transaction that holds the row, fail with SQLState {@code 57014} once the bound has passed, so
 * the caller retries on its next pass instead of waiting for as long as the holder lives. Reads and
 * inserts carry no bound of the store's own.
 */
@Testcontainers
class PostgresSagaStoreRowWriteTimeoutTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("saga_row_write_bound");

  record BoundState(String step) implements SagaState {
    @Override
    public SagaStatus status() {
      return SagaStatus.RUNNING;
    }
  }

  private static final SagaType TYPE = SagaType.fromClass(BoundState.class);
  private static final SagaId ID = SagaId.of("bounded-saga-1");
  private static final long BOUND_MILLIS = 500;
  private static final long SLACK_MILLIS = 1_000;

  private static final String LOCK_SHARED =
      "SELECT pg_advisory_xact_lock_shared("
          + SagaFenceLock.CLASS_ID
          + ", "
          + SagaFenceLock.objectId(TYPE, ID)
          + ")";

  static PGSimpleDataSource dataSource;

  private final List<Connection> holders = new ArrayList<>();
  private PostgresSagaStore store;

  @BeforeAll
  static void schema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    new PostgresEventStoreFactory(dataSource, NO_EVENTS).create();
  }

  @BeforeEach
  void forwardRow() throws SQLException {
    try (Connection conn = dataSource.getConnection();
        Statement st = conn.createStatement()) {
      st.execute("TRUNCATE saga_state");
    }
    store = new PostgresSagaStore(dataSource, mapper(), Duration.ofMillis(BOUND_MILLIS));
    store.create(ID, TYPE, new BoundState("start"), SagaStatus.RUNNING, false);
  }

  @AfterEach
  void releaseHolders() throws SQLException {
    for (Connection holder : holders) {
      holder.rollback();
      holder.close();
    }
    holders.clear();
  }

  @Test
  void aClaimWaitingOnTheFenceFailsAtItsBound() throws Exception {
    holdTheSharedFenceLock();

    long started = System.nanoTime();
    Throwable thrown = failureWithinCap(() -> claim(1L));
    long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

    assertThat(thrown).isInstanceOf(EventStoreException.class);
    assertThat(sqlStateOf(thrown)).isEqualTo("57014");
    assertThat(elapsed).isLessThan(BOUND_MILLIS + SLACK_MILLIS);
    var row = store.load(ID, TYPE, BoundState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(row.episodeVersion()).as("nothing of the claim is stored").isNull();
  }

  @Test
  void aClaimSucceedsOnceTheFenceIsFree() throws Exception {
    Connection holder = holdTheSharedFenceLock();
    assertThat(failureWithinCap(() -> claim(1L))).isInstanceOf(EventStoreException.class);

    // a holder that lets go within the bound: the waiting claim is granted
    CompletableFuture<Void> waiting = CompletableFuture.runAsync(() -> claim(1L));
    TimeUnit.MILLISECONDS.sleep(BOUND_MILLIS / 5);
    holder.commit();
    waiting.get(BOUND_MILLIS + SLACK_MILLIS, TimeUnit.MILLISECONDS);

    var row = store.load(ID, TYPE, BoundState.class).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.episodeVersion()).isEqualTo(2L);
  }

  /**
   * A claimer that goes silent inside its transaction — after it took the saga's exclusive fence
   * lock, before its write and its commit — is ended by the transaction's own idle bound (the
   * row-write bound): the fence lock is free again within the bound, another claim is made, and the
   * silent call fails with {@link EventStoreException} once it goes on. Its pooled connection is
   * discarded: the next use of the pool works.
   */
  @Test
  void aClaimerSilentInsideItsTransactionIsEndedAtTheBound_andItsConnectionIsDiscarded()
      throws Exception {
    PGSimpleDataSource physical = new PGSimpleDataSource();
    physical.setUrl(PG.getJdbcUrl());
    physical.setUser(PG.getUsername());
    physical.setPassword(PG.getPassword());
    physical.setApplicationName("silent-claimer");
    HikariConfig config = new HikariConfig();
    config.setDataSource(physical);
    config.setMaximumPoolSize(1);
    config.setConnectionTimeout(5_000);
    try (HikariDataSource pool = new HikariDataSource(config)) {
      PausingDataSource pausing = new PausingDataSource(pool);
      PostgresSagaStore silent =
          new PostgresSagaStore(pausing, mapper(), Duration.ofMillis(BOUND_MILLIS));
      PausingDataSource.Pause pause = pausing.pauseBefore("UPDATE saga_state");
      CompletableFuture<Void> claiming = CompletableFuture.runAsync(() -> claimThrough(silent));
      pause.awaitPaused();
      long idleSince = System.nanoTime();

      awaitNoBackendNamed("silent-claimer", BOUND_MILLIS + SLACK_MILLIS, idleSince);
      Connection append = holdTheSharedFenceLock();
      append.rollback();
      claim(1L);
      assertThat(store.load(ID, TYPE, BoundState.class).orElseThrow().episodeVersion())
          .as("the other claim is made")
          .isEqualTo(2L);

      pause.release();
      Throwable thrown = catchFailure(claiming);
      assertThat(thrown).isInstanceOf(EventStoreException.class);
      assertThat(sqlStateOf(thrown)).as("the session was ended").matches("25P03|08.*");
      assertThat(silent.load(ID, TYPE, BoundState.class))
          .as("the pool hands out a working connection")
          .isPresent();
    }
  }

  @Test
  void aWriteThatKeepsTheForwardPhaseFailsAtItsBoundBehindAHeldRow() throws Exception {
    holdTheRow();
    List<Consumer<PostgresSagaStore>> writes =
        List.of(
            s -> s.update(ID, TYPE, new BoundState("next"), SagaStatus.RUNNING, 1L),
            s ->
                s.applyEvent(
                    ID,
                    TYPE,
                    new BoundState("next"),
                    SagaStatus.RUNNING,
                    1L,
                    AppliedEvent.live(GlobalOffset.of(7))),
            s -> s.markFaulted(ID, TYPE, 1L),
            s -> s.setDeadLetterPending(ID, TYPE, true),
            s -> s.delete(ID, TYPE));
    for (Consumer<PostgresSagaStore> write : writes) {
      long started = System.nanoTime();
      Throwable thrown = failureWithinCap(() -> write.accept(store));
      long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
      assertThat(thrown).isInstanceOf(EventStoreException.class);
      assertThat(sqlStateOf(thrown)).isEqualTo("57014");
      assertThat(elapsed).isLessThan(BOUND_MILLIS + SLACK_MILLIS);
    }
  }

  /**
   * Which statements carry the bound, read from the driver's own statement as it executes: every
   * {@code UPDATE} and {@code DELETE} of {@code saga_state} and the exclusive-lock statement before
   * a fence-changing one; no read and no insert.
   */
  @Test
  void readsAndInsertsCarryNoBound() {
    List<Executed> executed = new CopyOnWriteArrayList<>();
    PostgresSagaStore recorded =
        new PostgresSagaStore(
            recording(dataSource, executed), mapper(), Duration.ofMillis(BOUND_MILLIS));
    SagaId other = SagaId.of("bounded-saga-2");
    SagaId faulted = SagaId.of("bounded-saga-3");

    recorded.create(other, TYPE, new BoundState("start"), SagaStatus.RUNNING, false);
    recorded.createGenesisPending(
        SagaId.of("bounded-saga-4"), TYPE, new BoundState("s"), SagaStatus.STARTED, false, null);
    recorded.createFaulted(faulted, TYPE, new BoundState("start"));
    recorded.load(other, TYPE, BoundState.class);
    recorded.findTimedOut(TYPE, Instant.now(), 10);
    recorded.findByStatus(TYPE, SagaStatus.RUNNING, 10);
    recorded.countByStatus(TYPE, SagaStatus.RUNNING);
    recorded.findCompensating(TYPE, Instant.now(), 10);
    recorded.update(other, TYPE, new BoundState("next"), SagaStatus.RUNNING, 1L);
    recorded.applyEvent(
        other,
        TYPE,
        new BoundState("next"),
        SagaStatus.RUNNING,
        2L,
        AppliedEvent.live(GlobalOffset.of(3)));
    recorded.setDeadLetterPending(other, TYPE, true);
    recorded.markFaulted(other, TYPE, 3L);
    recorded.claimCompensating(other, TYPE, new BoundState("c"), 4L, EventId.of("evt-1"));
    recorded.update(other, TYPE, new BoundState("done"), SagaStatus.COMPENSATED, 5L);
    recorded.delete(other, TYPE);

    List<Executed> bounded = executed.stream().filter(Executed::writesTheRow).toList();
    List<Executed> unbounded = executed.stream().filter(e -> !e.writesTheRow()).toList();
    assertThat(bounded)
        .as("every saga_state update and delete, and every exclusive-lock statement")
        .hasSize(10)
        .allSatisfy(e -> assertThat(e.timeoutMillis()).as(e.sql()).isEqualTo(BOUND_MILLIS));
    assertThat(unbounded)
        .as("reads and inserts")
        .isNotEmpty()
        .allSatisfy(e -> assertThat(e.timeoutMillis()).as(e.sql()).isZero());
    assertThat(unbounded.stream().map(Executed::sql))
        .anyMatch(sql -> sql.startsWith("INSERT INTO saga_state"))
        .anyMatch(sql -> sql.startsWith("SELECT"))
        .anyMatch(sql -> sql.startsWith("WITH forward_timeouts"));
  }

  // ---------------------------------------------------------------------------------------------

  /**
   * Runs {@code write} on another thread and returns what it threw; a write still waiting after ten
   * seconds — a bound that does not hold — fails the test instead of hanging it (the holders are
   * rolled back after the test, which lets the write go).
   */
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

  private static void claimThrough(PostgresSagaStore through) {
    through.claimCompensating(ID, TYPE, new BoundState("claimed"), 1L, null);
  }

  private static Throwable catchFailure(CompletableFuture<Void> running) {
    try {
      running.get(30, TimeUnit.SECONDS);
      return null;
    } catch (ExecutionException e) {
      return e.getCause();
    } catch (TimeoutException | InterruptedException e) {
      throw new AssertionError("the call did not end", e);
    }
  }

  /** Waits until no backend carries {@code applicationName}, failing past {@code boundMillis}. */
  static void awaitNoBackendNamed(String applicationName, long boundMillis, long since)
      throws Exception {
    while (true) {
      try (Connection conn = dataSource.getConnection();
          PreparedStatement ps =
              conn.prepareStatement(
                  "SELECT count(*) FROM pg_stat_activity WHERE application_name = ?")) {
        ps.setString(1, applicationName);
        try (ResultSet rs = ps.executeQuery()) {
          rs.next();
          if (rs.getInt(1) == 0) {
            return;
          }
        }
      }
      assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - since))
          .as("the backend of " + applicationName + " is ended within " + boundMillis + "ms")
          .isLessThan(boundMillis);
      TimeUnit.MILLISECONDS.sleep(20);
    }
  }

  private void claim(long expectedVersion) {
    store.claimCompensating(ID, TYPE, new BoundState("claimed"), expectedVersion, null);
  }

  /** A forward append in flight: a transaction holding the saga's shared fence lock. */
  private Connection holdTheSharedFenceLock() throws SQLException {
    Connection holder = holder();
    try (Statement st = holder.createStatement();
        ResultSet rs = st.executeQuery(LOCK_SHARED)) {
      rs.next();
    }
    return holder;
  }

  /** Another transaction that holds the row's lock. */
  private void holdTheRow() throws SQLException {
    Connection holder = holder();
    try (PreparedStatement ps =
        holder.prepareStatement("SELECT 1 FROM saga_state WHERE saga_id = ? FOR UPDATE")) {
      ps.setString(1, ID.value());
      try (ResultSet rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
      }
    }
  }

  private Connection holder() throws SQLException {
    Connection holder = dataSource.getConnection();
    holder.setAutoCommit(false);
    holders.add(holder);
    return holder;
  }

  private static String sqlStateOf(Throwable thrown) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t instanceof SQLException sql) {
        return sql.getSQLState();
      }
    }
    return null;
  }

  private static ObjectMapper mapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule());
  }

  /** A statement as it executed: its SQL and the query timeout the driver's statement carried. */
  record Executed(String sql, long timeoutMillis) {
    boolean writesTheRow() {
      return sql.contains("UPDATE saga_state")
          || sql.startsWith("DELETE FROM saga_state")
          || sql.contains("pg_advisory_xact_lock(");
    }
  }

  /** Records every statement its connections execute; the statements stay the driver's own. */
  private static DataSource recording(DataSource delegate, List<Executed> executed) {
    return (DataSource)
        Proxy.newProxyInstance(
            DataSource.class.getClassLoader(),
            new Class<?>[] {DataSource.class},
            (proxy, method, args) -> {
              Object result = invoke(delegate, method, args);
              return method.getName().equals("getConnection")
                  ? recordingConnection((Connection) result, executed)
                  : result;
            });
  }

  private static Connection recordingConnection(Connection connection, List<Executed> executed) {
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, method, args) -> {
              Object result = invoke(connection, method, args);
              if (method.getName().equals("prepareStatement")) {
                return recordingStatement(
                    (Statement) result, PreparedStatement.class, (String) args[0], executed);
              }
              if (method.getName().equals("createStatement")) {
                return recordingStatement((Statement) result, Statement.class, null, executed);
              }
              return result;
            });
  }

  private static Object recordingStatement(
      Statement statement, Class<?> type, String preparedSql, List<Executed> executed) {
    return Proxy.newProxyInstance(
        Statement.class.getClassLoader(),
        new Class<?>[] {type},
        (proxy, method, args) -> {
          if (method.getName().startsWith("execute")) {
            String sql = args != null && args.length > 0 ? (String) args[0] : preparedSql;
            executed.add(
                new Executed(sql, statement.unwrap(PgStatement.class).getQueryTimeoutMs()));
          }
          return invoke(statement, method, args);
        });
  }

  private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

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
}
