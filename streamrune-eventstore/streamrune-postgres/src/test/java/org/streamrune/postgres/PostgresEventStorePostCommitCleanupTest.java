package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.PrintWriter;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore.AppendResult;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.EventTypeRegistry;
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
 * Regression: {@link PostgresEventStore#append} / {@link PostgresEventStore#appendWithKey} commit
 * the transaction (events durable) and then run post-commit connection cleanup ({@code
 * setAutoCommit(true)}, {@code close()}). If the connection dies in that tiny window the cleanup
 * throws — but the events are ALREADY committed, so append MUST report success. Reporting a failure
 * here makes the command bus dead-letter a committed command; a DLQ replay of an UNKEYED command
 * (no inbox row to dedup) then re-appends duplicate domain events.
 *
 * <p>Each test wraps the pooled {@link Connection} so a chosen cleanup call throws <em>only after
 * commit()</em>. Against the pre-fix code the unguarded {@code finally { conn.setAutoCommit(true);
 * }} (and the try-with-resources close) let that throw propagate and be re-wrapped as an
 * EventStoreException — the assertions below fail. The fix swallows post-commit cleanup failures
 * and returns the committed result.
 */
@Testcontainers
class PostgresEventStorePostCommitCleanupTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("postcommit_cleanup_test");

  static PGSimpleDataSource realDataSource;
  static ObjectMapper objectMapper;
  static EventTypeRegistry typeRegistry;

  record TestEvent(String value) implements DomainEvent {}

  @BeforeAll
  static void initSchema() {
    realDataSource = new PGSimpleDataSource();
    realDataSource.setUrl(PG.getJdbcUrl());
    realDataSource.setUser(PG.getUsername());
    realDataSource.setPassword(PG.getPassword());
    objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return TestEvent.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return null;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return java.util.List.of(TestEvent.class);
          }
        };
    new PostgresEventStoreFactory(realDataSource, typeRegistry).create();
  }

  @BeforeEach
  void cleanTables() throws Exception {
    try (var conn = realDataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM command_inbox");
      stmt.execute("DELETE FROM event_audit_log");
    }
  }

  private EventEnvelope envelope(StreamId streamId, long version) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(version),
        new EventType("TestEvent"),
        new TestEvent("v" + version),
        new EventMetadata(
            EventId.of("evt-v" + version),
            CommandId.of("cmd-v" + version),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now()));
  }

  private long eventCount(StreamId streamId) throws Exception {
    try (var conn = realDataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM event_stream WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, streamId.aggregateType().value());
      ps.setString(2, streamId.aggregateId().value());
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  @Test
  void append_returnsSuccess_whenSetAutoCommitThrowsAfterCommit() throws Exception {
    var faultyDs =
        new PostCommitCleanupFailingDataSource(realDataSource, CleanupFault.SET_AUTOCOMMIT);
    var store =
        PostgresEventStore.builder().dataSource(faultyDs).typeRegistry(typeRegistry).build();
    var streamId = TestStreams.stream("post-commit-sac");

    AppendResult result =
        assertDoesNotThrow(
            () -> store.append(streamId, List.of(envelope(streamId, 1)), Version.initial()),
            "a committed append must not report failure when post-commit setAutoCommit throws");

    assertEquals(new Version(1), result.finalVersion());
    assertEquals(1, result.globalOffsets().size());
    assertEquals(1, eventCount(streamId), "the event is durably committed");
    assertTrue(faultyDs.faultTriggered(), "the post-commit cleanup fault must actually have fired");
  }

  @Test
  void append_returnsSuccess_whenConnectionCloseThrowsAfterCommit() throws Exception {
    var faultyDs = new PostCommitCleanupFailingDataSource(realDataSource, CleanupFault.CLOSE);
    var store =
        PostgresEventStore.builder().dataSource(faultyDs).typeRegistry(typeRegistry).build();
    var streamId = TestStreams.stream("post-commit-close");

    AppendResult result =
        assertDoesNotThrow(
            () -> store.append(streamId, List.of(envelope(streamId, 1)), Version.initial()),
            "a committed append must not report failure when the connection close() throws");

    assertEquals(new Version(1), result.finalVersion());
    assertEquals(1, eventCount(streamId), "the event is durably committed");
    assertTrue(faultyDs.faultTriggered(), "the post-commit close fault must actually have fired");
  }

  @Test
  void appendWithKey_returnsSuccess_whenSetAutoCommitThrowsAfterCommit() throws Exception {
    var faultyDs =
        new PostCommitCleanupFailingDataSource(realDataSource, CleanupFault.SET_AUTOCOMMIT);
    var commandInbox = new PostgresCommandInbox(realDataSource);
    var store =
        new PostgresEventStore(
            faultyDs, objectMapper, typeRegistry, null, null, null, null, commandInbox);
    var streamId = TestStreams.stream("post-commit-keyed");
    var key = IdempotencyKey.of("post-commit-key-1");

    IdempotentAppendResult result =
        assertDoesNotThrow(
            () ->
                store.appendWithKey(
                    streamId,
                    List.of(envelope(streamId, 1)),
                    Version.initial(),
                    key,
                    "TestCommand"),
            "a committed keyed append must not report failure when post-commit setAutoCommit throws");

    assertFalse(result.alreadyApplied());
    assertEquals(new Version(1), result.finalVersion());
    assertEquals(1, eventCount(streamId), "the event is durably committed");
    assertTrue(faultyDs.faultTriggered(), "the post-commit cleanup fault must actually have fired");

    // The inbox row committed atomically with the event, so a replay short-circuits as idempotent.
    IdempotentAppendResult replay =
        store.appendWithKey(
            streamId, List.of(envelope(streamId, 1)), Version.initial(), key, "TestCommand");
    assertTrue(replay.alreadyApplied(), "the committed inbox row must dedup a replay");
    assertEquals(1, eventCount(streamId), "replay must not append a second event");
  }

  enum CleanupFault {
    SET_AUTOCOMMIT,
    CLOSE
  }

  /**
   * Wraps a {@link DataSource} so the returned {@link Connection} throws a {@link SQLException} on
   * the chosen cleanup call, but ONLY after {@code commit()} has been invoked — modelling a
   * connection that dies in the window right after the transaction commits. Everything else
   * delegates to the real connection.
   */
  static final class PostCommitCleanupFailingDataSource implements DataSource {
    private final DataSource delegate;
    private final CleanupFault fault;
    private volatile boolean triggered = false;

    PostCommitCleanupFailingDataSource(DataSource delegate, CleanupFault fault) {
      this.delegate = delegate;
      this.fault = fault;
    }

    boolean faultTriggered() {
      return triggered;
    }

    @Override
    public Connection getConnection() throws SQLException {
      Connection real = delegate.getConnection();
      var handler =
          new InvocationHandler() {
            private boolean committed = false;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
              String name = method.getName();
              if (committed
                  && fault == CleanupFault.SET_AUTOCOMMIT
                  && name.equals("setAutoCommit")
                  && args != null
                  && Boolean.TRUE.equals(args[0])) {
                triggered = true;
                throw new SQLException(
                    "injected post-commit setAutoCommit failure (connection died)", "08006");
              }
              if (committed && fault == CleanupFault.CLOSE && name.equals("close")) {
                triggered = true;
                // Close the real connection first so the container connection is not leaked, then
                // surface the failure the store must swallow.
                try {
                  real.close();
                } catch (SQLException _) {
                  // best effort
                }
                throw new SQLException(
                    "injected post-commit close failure (connection died)", "08006");
              }
              Object out;
              try {
                out = method.invoke(real, args);
              } catch (java.lang.reflect.InvocationTargetException e) {
                throw e.getCause();
              }
              if (name.equals("commit")) {
                committed = true;
              }
              return out;
            }
          };
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(), new Class<?>[] {Connection.class}, handler);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return delegate.getConnection(username, password);
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
    public Logger getParentLogger() {
      return Logger.getLogger("PostCommitCleanupFailingDataSource");
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      if (iface.isInstance(this)) {
        return iface.cast(this);
      }
      return delegate.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return iface.isInstance(this) || delegate.isWrapperFor(iface);
    }
  }
}
