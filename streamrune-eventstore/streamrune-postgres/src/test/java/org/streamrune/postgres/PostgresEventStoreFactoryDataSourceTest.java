package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The event store built by {@link PostgresEventStoreFactory} borrows every connection from the
 * application's {@link DataSource} exactly as it was given — no pool of its own in between, no
 * driver settings changed — and bounds its statements without leaving anything on the connection it
 * hands back.
 */
@Testcontainers
class PostgresEventStoreFactoryDataSourceTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("factory_datasource_test");

  /** Bound used by the blocking tests: short enough to keep them quick, long enough to be real. */
  private static final Duration BOUND = Duration.ofMillis(300);

  /** PostgreSQL {@code query_canceled}: a statement timeout or a cancel request. */
  private static final String QUERY_CANCELED = "57014";

  record Noted(String value) implements DomainEvent {}

  record NotedState(String value) implements AggregateState {}

  static final EventTypeRegistry REGISTRY =
      SimpleEventTypeRegistry.builder()
          .registerEvent("Noted", Noted.class)
          .registerState("NotedState", NotedState.class)
          .build();

  @BeforeAll
  static void migrate() {
    new PostgresEventStoreFactory(plainDataSource(), REGISTRY).initializeSchema();
  }

  @BeforeEach
  void clean() throws SQLException {
    try (Connection conn = plainDataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("TRUNCATE event_stream, snapshot_store, command_inbox");
      st.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
  }

  // ---------------------------------------------------------------- the DataSource as given

  @Test
  void aDataSourceThatIsNotAHikariPoolIsUsedAsGiven() {
    var counting = new CountingDataSource(plainDataSource());
    EventStore store = new PostgresEventStoreFactory(counting, REGISTRY).create();

    StreamId stream = TestStreams.stream("as-given");
    store.append(stream, List.of(envelope(stream, 1)), Version.initial());
    assertEquals(1, store.load(stream).events().size());
    assertEquals(1, store.readGlobalStream(GlobalOffset.initial(), 10).size());

    assertTrue(counting.borrowed() > 0, "the store must borrow from the application DataSource");
    assertEquals(
        0,
        counting.open(),
        "every connection the store borrowed must be back with the application DataSource; one"
            + " still open means something between the store and the DataSource holds it");
    assertFalse(
        Thread.getAllStackTraces().keySet().stream()
            .anyMatch(t -> t.getName().startsWith("streamrune-event-store")),
        "no connection pool of the framework's own may run between the store and the DataSource");
  }

  @Test
  void theDriverSettingsOfTheDataSourceAreLeftAlone() {
    PGSimpleDataSource ds = plainDataSource();
    int prepareThreshold = ds.getPrepareThreshold();
    int cacheQueries = ds.getPreparedStatementCacheQueries();
    int cacheSizeMiB = ds.getPreparedStatementCacheSizeMiB();

    new PostgresEventStoreFactory(ds, REGISTRY).create();

    assertEquals(prepareThreshold, ds.getPrepareThreshold());
    assertEquals(cacheQueries, ds.getPreparedStatementCacheQueries());
    assertEquals(cacheSizeMiB, ds.getPreparedStatementCacheSizeMiB());
  }

  // ---------------------------------------------------------------- the statement bound

  @Test
  void anAppendQueuedPastTheBoundFailsOnAHikariApplicationPool() throws Exception {
    try (HikariDataSource pool = hikari(4)) {
      EventStore store =
          new PostgresEventStoreFactory(pool, REGISTRY).statementTimeout(BOUND).create();
      StreamId stream = TestStreams.stream("append-bound");

      withLock(
          "SELECT next_value FROM global_offset_sequence WHERE id = 1 FOR UPDATE",
          () ->
              assertCanceled(
                  () -> store.append(stream, List.of(envelope(stream, 1)), Version.initial())));
    }
  }

  @Test
  void aKeyedAppendQueuedPastTheBoundFailsOnAHikariApplicationPool() throws Exception {
    try (HikariDataSource pool = hikari(4)) {
      EventStore store =
          new PostgresEventStoreFactory(pool, REGISTRY)
              .commandInbox(new PostgresCommandInbox(pool))
              .statementTimeout(BOUND)
              .create();
      StreamId stream = TestStreams.stream("keyed-append-bound");

      withLock(
          "SELECT next_value FROM global_offset_sequence WHERE id = 1 FOR UPDATE",
          () ->
              assertCanceled(
                  () ->
                      store.appendWithKey(
                          stream,
                          List.of(envelope(stream, 1)),
                          Version.initial(),
                          IdempotencyKey.of("keyed-append-bound"),
                          "Note")));
    }
  }

  @Test
  void everyReadQueuedPastTheBoundFailsOnAHikariApplicationPool() throws Exception {
    try (HikariDataSource pool = hikari(4)) {
      EventStore store =
          new PostgresEventStoreFactory(pool, REGISTRY).statementTimeout(BOUND).create();
      StreamId stream = TestStreams.stream("read-bound");

      withLock(
          "LOCK TABLE event_stream IN ACCESS EXCLUSIVE MODE",
          () -> {
            assertCanceled(() -> store.load(stream));
            assertCanceled(() -> store.load(stream, EventStore.IGNORE_SNAPSHOT));
            assertCanceled(() -> store.readStream(stream, Version.initial(), 10));
            assertCanceled(() -> store.readGlobalStream(GlobalOffset.initial(), 10));
            assertCanceled(store::lastGlobalOffset);
          });
    }
  }

  @Test
  void aSnapshotSaveQueuedPastTheBoundFailsOnAHikariApplicationPool() throws Exception {
    try (HikariDataSource pool = hikari(4)) {
      EventStore store =
          new PostgresEventStoreFactory(pool, REGISTRY).statementTimeout(BOUND).create();
      StreamId stream = TestStreams.stream("snapshot-bound");

      withLock(
          "LOCK TABLE snapshot_store IN ACCESS EXCLUSIVE MODE",
          () ->
              assertCanceled(
                  () -> {
                    store.saveSnapshot(stream, new Version(1), new NotedState("s"));
                    return null;
                  }));
    }
  }

  @Test
  void theBoundLeavesNothingOnTheConnectionItHandsBack() throws Exception {
    // One connection in the application pool: every operation below runs on it, so whatever the
    // store set on its session is what the next borrower sees. The schema is already migrated
    // (Flyway wants a second connection for its lock, which a one-connection pool cannot give).
    try (HikariDataSource pool = hikari(1)) {
      EventStore store =
          new PostgresEventStoreFactory(pool, REGISTRY)
              .autoInitializeSchema(false)
              .statementTimeout(BOUND)
              .create();
      StreamId stream = TestStreams.stream("no-residue");

      store.append(stream, List.of(envelope(stream, 1)), Version.initial());
      store.saveSnapshot(stream, new Version(1), new NotedState("s"));
      store.load(stream);
      store.readStream(stream, Version.initial(), 10);
      store.readGlobalStream(GlobalOffset.initial(), 10);
      store.lastGlobalOffset();

      try (Connection conn = pool.getConnection();
          var st = conn.createStatement();
          var rs = st.executeQuery("SHOW statement_timeout")) {
        assertTrue(rs.next());
        assertEquals("0", rs.getString(1), "the server default must be back in force");
      }
      try (Connection conn = pool.getConnection();
          var st = conn.createStatement()) {
        // A statement slower than the store's bound still runs to completion for the application.
        st.execute("SELECT pg_sleep(" + (BOUND.toMillis() * 2) / 1000.0 + ")");
      }
    }
  }

  @Test
  void aZeroBoundLeavesAQueuedAppendWaiting() throws Exception {
    try (HikariDataSource pool = hikari(4)) {
      EventStore store =
          new PostgresEventStoreFactory(pool, REGISTRY).statementTimeout(Duration.ZERO).create();
      StreamId stream = TestStreams.stream("unbounded");

      CompletableFuture<?> append;
      try (Connection holder = plainDataSource().getConnection()) {
        holder.setAutoCommit(false);
        try (var st = holder.createStatement()) {
          st.execute("SELECT next_value FROM global_offset_sequence WHERE id = 1 FOR UPDATE");
        }
        append =
            CompletableFuture.supplyAsync(
                () -> store.append(stream, List.of(envelope(stream, 1)), Version.initial()));
        Thread.sleep(BOUND.toMillis() * 4);
        assertFalse(append.isDone(), "with no bound the append waits for the lock holder");
        holder.rollback();
      }
      append.get(10, TimeUnit.SECONDS);
      assertEquals(1, store.load(stream).events().size());
    }
  }

  // ---------------------------------------------------------------- helpers

  /**
   * Runs {@code body} while another session holds the lock {@code lockSql} takes, then releases it.
   */
  private static void withLock(String lockSql, ThrowingRunnable body) throws Exception {
    try (Connection holder = plainDataSource().getConnection()) {
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

  /**
   * The call must fail with the server's {@code query_canceled} well before the lock holder lets
   * go; a call still running after 10 s was never bounded.
   */
  private static void assertCanceled(Supplier<?> call) throws InterruptedException {
    var latch = new CountDownLatch(1);
    CompletableFuture<?> future =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return call.get();
              } finally {
                latch.countDown();
              }
            });
    try {
      future.get(10, TimeUnit.SECONDS);
      fail("the call succeeded while another session held the lock it needs");
    } catch (TimeoutException _) {
      fail("the call was still waiting after 10 s: the statement bound was not applied");
    } catch (ExecutionException e) {
      Throwable failure = e.getCause();
      assertThat(failure).isInstanceOf(EventStoreException.class);
      assertTrue(
          hasSqlState(failure, QUERY_CANCELED),
          "expected query_canceled (57014) in the cause chain, got: " + failure);
    }
    assertTrue(latch.await(1, TimeUnit.SECONDS));
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

  private static PGSimpleDataSource plainDataSource() {
    var ds = new PGSimpleDataSource();
    ds.setUrl(PG.getJdbcUrl());
    ds.setUser(PG.getUsername());
    ds.setPassword(PG.getPassword());
    return ds;
  }

  private static HikariDataSource hikari(int size) {
    var config = new HikariConfig();
    config.setJdbcUrl(PG.getJdbcUrl());
    config.setUsername(PG.getUsername());
    config.setPassword(PG.getPassword());
    config.setMaximumPoolSize(size);
    config.setMinimumIdle(size);
    config.setPoolName("application-" + System.nanoTime());
    return new HikariDataSource(config);
  }

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  /**
   * An application DataSource that is not a HikariCP pool (as Agroal or a proxying wrapper is not),
   * counting the connections it has handed out and not yet had back.
   */
  private static final class CountingDataSource implements DataSource {
    private final DataSource delegate;
    private final AtomicInteger borrowed = new AtomicInteger();
    private final AtomicInteger open = new AtomicInteger();

    CountingDataSource(DataSource delegate) {
      this.delegate = delegate;
    }

    int borrowed() {
      return borrowed.get();
    }

    int open() {
      return open.get();
    }

    @Override
    public Connection getConnection() throws SQLException {
      return track(delegate.getConnection());
    }

    @Override
    public Connection getConnection(String user, String password) throws SQLException {
      return track(delegate.getConnection(user, password));
    }

    private Connection track(Connection real) {
      borrowed.incrementAndGet();
      open.incrementAndGet();
      var closed = new java.util.concurrent.atomic.AtomicBoolean();
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if (method.getName().equals("close") && closed.compareAndSet(false, true)) {
                  open.decrementAndGet();
                }
                try {
                  return method.invoke(real, args);
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
              });
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      delegate.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      delegate.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return delegate.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface.isInstance(this)) {
        return iface.cast(this);
      }
      throw new SQLException("not a wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return iface.isInstance(this);
    }
  }
}
