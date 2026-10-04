package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
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
 * {@code append}/{@code appendWithKey} pin READ COMMITTED for the duration of the append
 * transaction. pgjdbc implements {@code setTransactionIsolation} as {@code SET SESSION
 * CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...} — a SESSION-scoped change that survives the
 * transaction and the connection's return to the pool. The store must therefore restore the
 * borrower's level on the way out, exactly as it already restores {@code autoCommit}; otherwise a
 * DataSource shared with the application (or any pool that does not reset isolation on return —
 * Agroal without {@code jdbc.transaction-isolation}, Tomcat-JDBC without the ConnectionState
 * interceptor, DBCP2 without {@code defaultTransactionIsolation}, or a plain {@code
 * PGSimpleDataSource} handed to the public {@code PostgresEventStore.builder()}) silently
 * downgrades every later borrower to READ COMMITTED.
 *
 * <p>These tests differ from {@code PostgresGaplessOffsetIT}'s {@code RepeatableReadDataSource},
 * which re-forces REPEATABLE READ on EVERY {@code getConnection()} and so structurally cannot
 * observe a missing restore. Here the level is forced ONCE on the single physical connection, which
 * is exactly what a pool that hands the same connection back does.
 */
@Testcontainers
class PostgresEventStoreIsolationRestoreIT {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("isolation_restore");

  static PGSimpleDataSource realDataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;

  /** The one physical connection every borrow returns — a pool of size one. */
  static Connection physical;

  static PinnedConnectionDataSource pooled;

  PostgresEventStore store;
  PostgresCommandInbox commandInbox;

  record TestEvent(String value) implements DomainEvent {}

  @BeforeAll
  static void initSchema() throws Exception {
    realDataSource = new PGSimpleDataSource();
    realDataSource.setUrl(PG.getJdbcUrl());
    realDataSource.setUser(PG.getUsername());
    realDataSource.setPassword(PG.getPassword());
    objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return TestEvent.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of(TestEvent.class);
          }
        };
    new PostgresEventStoreFactory(realDataSource, typeRegistry).create();

    physical = realDataSource.getConnection();
    pooled = new PinnedConnectionDataSource(physical);
  }

  @AfterAll
  static void closePhysical() throws Exception {
    if (physical != null) {
      physical.close();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    commandInbox = new PostgresCommandInbox(pooled);
    store =
        new PostgresEventStore(
            pooled, objectMapper, typeRegistry, null, null, null, null, commandInbox);
    try (var conn = realDataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM command_inbox");
      stmt.execute("UPDATE global_offset_sequence SET next_value = 0 WHERE id = 1");
    }
    // The application's own hardening choice: this pooled connection serves REPEATABLE READ.
    physical.setAutoCommit(true);
    physical.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
    pooled.isolationWrites.set(0);
  }

  @Test
  void append_restoresTheBorrowersIsolationLevel() throws Exception {
    StreamId streamId = TestStreams.stream("iso-append");
    store.append(streamId, List.of(envelope(streamId, 1)), Version.initial());

    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        physical.getTransactionIsolation(),
        "append() must hand the connection back at the isolation level it borrowed it at —"
            + " SET SESSION CHARACTERISTICS is session-scoped and outlives the transaction");
  }

  @Test
  void append_restoresTheBorrowersIsolationLevelOnTheConflictAbortPath() throws Exception {
    StreamId streamId = TestStreams.stream("iso-append-conflict");
    store.append(streamId, List.of(envelope(streamId, 1)), Version.initial());
    physical.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

    assertThrows(
        OptimisticLockException.class,
        () -> store.append(streamId, List.of(envelope(streamId, 1)), Version.initial()));

    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        physical.getTransactionIsolation(),
        "the rollback path must restore isolation too — it already restores autoCommit there");
  }

  @Test
  void appendWithKey_restoresTheBorrowersIsolationLevel() throws Exception {
    StreamId streamId = TestStreams.stream("iso-keyed");
    store.appendWithKey(
        streamId,
        List.of(envelope(streamId, 1)),
        Version.initial(),
        IdempotencyKey.of("iso-key-1"),
        "TestCommand");

    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        physical.getTransactionIsolation(),
        "appendWithKey() carries the identical pin and must carry the identical restore");
  }

  @Test
  void appendWithKey_restoresTheBorrowersIsolationLevelOnTheConflictAbortPath() throws Exception {
    StreamId streamId = TestStreams.stream("iso-keyed-conflict");
    store.appendWithKey(
        streamId,
        List.of(envelope(streamId, 1)),
        Version.initial(),
        IdempotencyKey.of("iso-key-2"),
        "TestCommand");
    physical.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

    assertThrows(
        OptimisticLockException.class,
        () ->
            store.appendWithKey(
                streamId,
                List.of(envelope(streamId, 1)),
                Version.initial(),
                IdempotencyKey.of("iso-key-3"),
                "TestCommand"));

    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        physical.getTransactionIsolation(),
        "the keyed rollback path must restore isolation too");
  }

  @Test
  void append_neverMutatesTheConnectionsSessionIsolation() throws Exception {
    StreamId streamId = TestStreams.stream("iso-session-untouched");
    store.append(streamId, List.of(envelope(streamId, 1)), Version.initial());
    store.appendWithKey(
        streamId,
        List.of(envelope(streamId, 2)),
        new Version(1),
        IdempotencyKey.of("iso-key-4"),
        "TestCommand");

    assertEquals(
        0,
        pooled.isolationWrites.get(),
        "the pin is scoped to the append transaction (SET TRANSACTION ISOLATION LEVEL), so the"
            + " connection's SESSION isolation is never written at all — there is no residue to"
            + " restore and no restore that can fail on a dying connection");
    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        physical.getTransactionIsolation(),
        "the borrower's session level is exactly what it was before the appends");
  }

  private EventEnvelope envelope(StreamId streamId, long version) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new TestEvent("v" + version),
        new EventMetadata(
            EventId.of("evt-" + streamId.value() + "-" + version),
            CommandId.of("cmd-" + streamId.value() + "-" + version),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now()));
  }

  /**
   * A pool of exactly one physical connection that is never actually closed on return — the shape
   * every real pool has, and the only shape in which a session-scoped isolation change is
   * observable by the next borrower.
   */
  private static final class PinnedConnectionDataSource implements DataSource {
    private final Connection handle;
    final AtomicInteger isolationWrites = new AtomicInteger();

    PinnedConnectionDataSource(Connection physical) {
      InvocationHandler handler =
          (proxy, method, args) -> {
            switch (method.getName()) {
              case "close" -> {
                return null; // returned to the pool, not closed
              }
              case "isClosed" -> {
                return Boolean.FALSE;
              }
              case "setTransactionIsolation" -> isolationWrites.incrementAndGet();
              default -> {
                // fall through to the delegate
              }
            }
            try {
              return method.invoke(physical, args);
            } catch (InvocationTargetException e) {
              throw e.getCause();
            }
          };
      this.handle =
          (Connection)
              Proxy.newProxyInstance(
                  PinnedConnectionDataSource.class.getClassLoader(),
                  new Class<?>[] {Connection.class},
                  handler);
    }

    @Override
    public Connection getConnection() {
      return handle;
    }

    @Override
    public Connection getConnection(String username, String password) {
      return handle;
    }

    @Override
    public PrintWriter getLogWriter() {
      return null;
    }

    @Override
    public void setLogWriter(PrintWriter out) {}

    @Override
    public void setLoginTimeout(int seconds) {}

    @Override
    public int getLoginTimeout() {
      return 0;
    }

    @Override
    public Logger getParentLogger() {
      return Logger.getGlobal();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLException("not a wrapper for " + iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }
}
