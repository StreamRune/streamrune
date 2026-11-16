package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreFixture;

/**
 * The argument checks of every append overload of {@link PostgresEventStore}, with and without
 * tracing, against a data source that has no database: a refused call never asks it for a
 * connection, so these need no container. {@code EventStoreContract} holds the same rules for a
 * running store.
 */
class PostgresEventStoreNullArgumentsTest {

  record Plain(String value) implements DomainEvent {}

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("probe"), AggregateId.of("1"));
  private static final String COMMAND_TYPE = "com.example.Probe";

  /** A data source that fails when a connection is requested. */
  private static final class NoDatabase implements DataSource {
    @Override
    public Connection getConnection() throws SQLException {
      throw new SQLException("no database: a refused call must not ask for a connection");
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      return getConnection();
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
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      throw new SQLException("not a wrapper");
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) {
      return false;
    }
  }

  private final AtomicInteger spans = new AtomicInteger();
  private final AtomicInteger mapperCalls = new AtomicInteger();

  private final StreamRuneTracing tracing =
      new StreamRuneTracing() {
        @Override
        public boolean enabled(TracingArea area) {
          return true;
        }

        @Override
        public TraceScope start(
            TracingArea area,
            String spanName,
            SpanKind kind,
            SpanAttributes attributes,
            SpanLink link,
            SpanBaggage baggage) {
          spans.incrementAndGet();
          return TraceScope.NOOP;
        }
      };

  private final List<EventEnvelope> events =
      List.of(
          EventStoreFixture.event(STREAM, new Version(1), new EventType("Plain"), new Plain("x")));

  private PostgresEventStore store(boolean traced) {
    DataSource source = new NoDatabase();
    var builder =
        PostgresEventStore.builder()
            .dataSource(source)
            .typeRegistry(
                SimpleEventTypeRegistry.builder().registerEvent("Plain", Plain.class).build())
            .commandInbox(new PostgresCommandInbox(source))
            .outboxStore(new PostgresOutboxStore(source))
            .outboxEventMapper(
                event -> {
                  mapperCalls.incrementAndGet();
                  throw new IllegalStateException("the outbox mapper ran");
                });
    if (traced) {
      builder.tracing(tracing);
    }
    return builder.build();
  }

  private static void assertRefused(
      String message, org.junit.jupiter.api.function.Executable call) {
    var refused = assertThrows(NullPointerException.class, call);
    assertEquals(message, refused.getMessage());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aNullExpectedVersionIsRefusedByEveryAppendOverloadForAnEmptyAndAFilledList(boolean traced) {
    PostgresEventStore store = store(traced);
    var key = IdempotencyKey.of("key-1");
    var entries = List.<OutboxEntry>of();

    for (List<EventEnvelope> list : List.of(List.<EventEnvelope>of(), events)) {
      assertRefused("expectedVersion is required", () -> store.append(STREAM, list, null));
      assertRefused("expectedVersion is required", () -> store.append(STREAM, list, null, entries));
      assertRefused(
          "expectedVersion is required",
          () -> store.appendWithKey(STREAM, list, null, key, COMMAND_TYPE));
      assertRefused(
          "expectedVersion is required",
          () -> store.appendWithKey(STREAM, list, null, key, COMMAND_TYPE, null));
    }
    assertEquals(0, spans.get(), "a refused call starts no span");
    assertEquals(0, mapperCalls.get(), "a refused call does not reach the outbox mapper");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void theArgumentsAreCheckedInOneOrderOnEveryOverload(boolean traced) {
    PostgresEventStore store = store(traced);

    assertRefused("streamId is required", () -> store.append(null, null, null));
    assertRefused("events is required", () -> store.append(STREAM, null, null));
    assertRefused("streamId is required", () -> store.append(null, null, null, null));
    assertRefused("events is required", () -> store.append(STREAM, null, null, null));
    assertRefused("expectedVersion is required", () -> store.append(STREAM, events, null, null));
    assertRefused("streamId is required", () -> store.appendWithKey(null, null, null, null, null));
    assertRefused("events is required", () -> store.appendWithKey(STREAM, null, null, null, null));
    assertRefused(
        "expectedVersion is required", () -> store.appendWithKey(STREAM, events, null, null, null));
    assertRefused(
        "idempotencyKey is required",
        () -> store.appendWithKey(STREAM, events, Version.initial(), null, null));
    assertEquals(0, mapperCalls.get(), "the outbox mapper sees no event of a refused call");
  }

  @Test
  void anEmptyAppendWithAVersionIsStillTheNoOp() {
    var result = store(false).append(STREAM, List.of(), new Version(7));

    assertEquals(new Version(7), result.finalVersion());
    assertEquals(List.of(), result.globalOffsets());
  }
}
