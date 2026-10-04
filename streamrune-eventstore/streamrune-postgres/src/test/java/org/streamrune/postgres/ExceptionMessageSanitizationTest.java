package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The exception messages the PostgreSQL stores raise on an infrastructure failure quote the stream
 * id, projection id, command id or outbox id the failed call was about. Each of those is
 * caller-supplied text — a decoded id carries no charset rule — and the message reaches log lines,
 * audit rows and dead-letter rows, so it renders the value through {@link
 * LogSanitizer#sanitizeForLog}. A pooled-connection failure is injected with a {@link DataSource}
 * that cannot hand out a connection, so no container is needed.
 */
class ExceptionMessageSanitizationTest {

  /** A line break, a forged log prefix, a control and a right-to-left override. */
  private static final String FORGED = "x\n2026-10-04 10:00:00 WARN forged\r\u0085\u202e";

  private static final String SANITIZED = LogSanitizer.sanitizeForLog(FORGED);

  private static DataSource down() {
    DataSource ds = mock(DataSource.class);
    try {
      when(ds.getConnection()).thenThrow(new SQLException("refused"));
    } catch (SQLException e) {
      throw new AssertionError(e);
    }
    return ds;
  }

  private static void assertQuotesTheValueSanitized(String message) {
    assertTrue(message.contains(SANITIZED), message);
    assertFalse(message.chars().anyMatch(Character::isISOControl), message);
    assertFalse(message.contains("\u202e"), message);
  }

  private static StreamId forgedStream() {
    return StreamId.of(new AggregateType("order"), new AggregateId(FORGED));
  }

  @Test
  void eventStoreLoad_quotesTheStreamSanitized() {
    var store =
        new PostgresEventStore(
            down(), new ObjectMapper(), mock(EventTypeRegistry.class), null, null);

    var ex = assertThrows(EventStoreException.class, () -> store.load(forgedStream()));

    assertTrue(ex.getMessage().startsWith("Failed to load stream: order:"), ex.getMessage());
    assertQuotesTheValueSanitized(ex.getMessage());
  }

  @Test
  void eventStoreReadStream_quotesTheStreamSanitized() {
    var store =
        new PostgresEventStore(
            down(), new ObjectMapper(), mock(EventTypeRegistry.class), null, null);

    var ex =
        assertThrows(
            EventStoreException.class,
            () -> store.readStream(forgedStream(), Version.initial(), 10));

    assertQuotesTheValueSanitized(ex.getMessage());
  }

  @Test
  void offsetStore_quotesTheProjectionNameSanitized() {
    var store = new PostgresOffsetStore(down());

    var ex =
        assertThrows(
            EventStoreException.class, () -> store.getLastOffset(new ProjectionName(FORGED)));

    assertQuotesTheValueSanitized(ex.getMessage());
  }

  @Test
  void deadLetterQueue_quotesTheCommandIdSanitized() {
    var dlq = new PostgresDeadLetterQueue(down());

    var ex = assertThrows(EventStoreException.class, () -> dlq.discard(new CommandId(FORGED)));

    assertQuotesTheValueSanitized(ex.getMessage());
  }

  @Test
  void outboxStore_quotesTheEntryIdSanitized() {
    var store = new PostgresOutboxStore(down());

    var ex =
        assertThrows(
            EventStoreException.class,
            () -> store.markDelivered(new OutboxEntryId(FORGED), "claimer"));

    assertQuotesTheValueSanitized(ex.getMessage());
  }

  @Test
  void outboxStoreResetFailedToPending_quotesTheEntryIdSanitized() {
    var store = new PostgresOutboxStore(down());

    var ex =
        assertThrows(
            EventStoreException.class, () -> store.resetFailedToPending(new OutboxEntryId(FORGED)));

    assertQuotesTheValueSanitized(ex.getMessage());
  }

  @Test
  void outboxStoreFindById_quotesTheEntryIdSanitized() {
    var store = new PostgresOutboxStore(down());

    var ex =
        assertThrows(EventStoreException.class, () -> store.findById(new OutboxEntryId(FORGED)));

    assertQuotesTheValueSanitized(ex.getMessage());
  }

  @Test
  void outboxStoreSkipFailed_quotesTheEntryIdSanitized() {
    var store = new PostgresOutboxStore(down());

    var ex =
        assertThrows(
            EventStoreException.class,
            () -> store.skipFailed(new OutboxEntryId(FORGED), "operator", "obsolete"));

    assertQuotesTheValueSanitized(ex.getMessage());
  }

  @Test
  void projectionDeadLetterStoreRead_quotesTheProjectionNameSanitized() {
    var store = new PostgresProjectionDeadLetterStore(down());

    var ex =
        assertThrows(EventStoreException.class, () -> store.read(new ProjectionName(FORGED), 10));

    assertQuotesTheValueSanitized(ex.getMessage());
  }

  /**
   * The read-model repository creates the table on a first connection, then fails on the second —
   * the failure that quotes {@code projection/id}.
   */
  @Test
  void projectionRepository_quotesTheRowIdSanitized() throws Exception {
    DataSource ds = mock(DataSource.class);
    Connection first = mock(Connection.class, RETURNS_DEEP_STUBS);
    var calls = new AtomicInteger();
    when(ds.getConnection())
        .thenAnswer(
            inv -> {
              if (calls.getAndIncrement() == 0) {
                return first;
              }
              throw new SQLException("refused");
            });
    var repository = new JdbcProjectionRepository(ds);

    var ex =
        assertThrows(
            EventStoreException.class,
            () -> repository.findById(new ProjectionName("orders"), FORGED, String.class));

    assertTrue(ex.getMessage().startsWith("Failed to find projection: orders/"), ex.getMessage());
    assertQuotesTheValueSanitized(ex.getMessage());
  }

  @Test
  void sanitizedValueStaysReadable() {
    assertEquals("x2026-10-04 10:00:00 WARN forged", SANITIZED);
  }
}
