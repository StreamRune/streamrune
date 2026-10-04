package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * Driver-anomaly behavior of {@link PostgresEventStore#append} around the {@code
 * global_offset_sequence} reservation, exercised with a mocked JDBC stack (no real database):
 *
 * <ul>
 *   <li>a missing counter row (the {@code RESERVE_OFFSETS} query returns no row) must fail BEFORE
 *       commit (rollback, no spurious success report), and
 *   <li>a post-commit NOTIFY failure of any kind must never fail the already-committed append.
 * </ul>
 */
class PostgresEventStoreOffsetReservationTest {

  record TestEvent(String value) implements DomainEvent {}

  private final DataSource dataSource = mock(DataSource.class);
  private final Connection conn = mock(Connection.class);
  private final PreparedStatement reservePs = mock(PreparedStatement.class);
  private final PreparedStatement insertPs = mock(PreparedStatement.class);
  private final PreparedStatement headPs = mock(PreparedStatement.class);
  private final ResultSet reserveRs = mock(ResultSet.class);
  private final ResultSet headRs = mock(ResultSet.class);
  private final Statement pinStmt = mock(Statement.class);

  private final EventTypeRegistry typeRegistry =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          return TestEvent.class;
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          return TestEvent.class;
        }
      };

  private PostgresEventStore store;

  @BeforeEach
  void setUp() throws Exception {
    var objectMapper = new ObjectMapper();
    objectMapper.registerModule(new JavaTimeModule());
    when(dataSource.getConnection()).thenReturn(conn);
    // The append transaction pins its isolation with a plain Statement executing
    // SET TRANSACTION ISOLATION LEVEL READ COMMITTED as its FIRST statement — transaction-scoped,
    // so nothing is left on the pooled connection's session. Model it here or every append NPEs on
    // the unstubbed createStatement().
    when(conn.createStatement()).thenReturn(pinStmt);
    // The RESERVE_OFFSETS UPDATE ... RETURNING and the INSERT_EVENT batch insert are both 1-arg
    // prepareStatement calls (no RETURN_GENERATED_KEYS anymore — offsets are computed, not
    // generated).
    when(conn.prepareStatement(argThat(containsSql("global_offset_sequence"))))
        .thenReturn(reservePs);
    when(conn.prepareStatement(argThat(containsSql("INSERT INTO event_stream"))))
        .thenReturn(insertPs);
    when(reservePs.executeQuery()).thenReturn(reserveRs);
    when(insertPs.executeBatch()).thenReturn(new int[] {1});
    // insertEvents now reads the stream's actual head, right after
    // RESERVE_OFFSETS, before the INSERT batch — every test in this file appends to a brand-new
    // stream at Version.initial(), so the mocked head is 0 (matching) by default.
    when(conn.prepareStatement(argThat(containsSql("MAX(version)")))).thenReturn(headPs);
    when(headPs.executeQuery()).thenReturn(headRs);
    when(headRs.next()).thenReturn(true);
    when(headRs.getLong(1)).thenReturn(0L);
    // Append no longer takes any advisory lock (gap-freeness is a read-side watermark now), so the
    // only other 1-arg prepareStatement call in append is the post-commit NOTIFY.
    store = new PostgresEventStore(dataSource, objectMapper, typeRegistry, null, null);
  }

  private static org.mockito.ArgumentMatcher<String> containsSql(String needle) {
    return sql -> sql != null && sql.contains(needle);
  }

  private EventEnvelope envelope(StreamId streamId) {
    var metadata =
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.now());
    return new EventEnvelope(
        GlobalOffset.initial(),
        streamId,
        new Version(1),
        new EventType("TestEvent"),
        new TestEvent("x"),
        metadata);
  }

  @Test
  void appendFailsBeforeCommitWhenCounterRowIsMissing() throws Exception {
    when(reserveRs.next()).thenReturn(false); // driver anomaly: global_offset_sequence row missing

    StreamId streamId = TestStreams.stream("missing-counter-row");
    var ex =
        assertThrows(
            EventStoreException.class,
            () -> store.append(streamId, List.of(envelope(streamId)), Version.initial()));

    assertTrue(ex.getMessage().contains("global_offset_sequence"), ex.getMessage());
    verify(conn).rollback();
    verify(conn, never()).commit();
  }

  @Test
  void appendRestoresAutoCommitOnErrorPathBeforeReturningConnection() throws Exception {
    // Hygiene parity with appendWithKey: when the append aborts before commit (here the counter row
    // is missing), the pooled connection's autoCommit must be restored to true in a finally BEFORE
    // it returns to the pool — never left at the false the append set. Without the finally-restore
    // added in append(), setAutoCommit(true) is only ever reached via the post-commit NOTIFY, which
    // this error path never runs, so the connection would go back to the pool at autoCommit=false.
    when(reserveRs.next()).thenReturn(false); // driver anomaly: pre-commit failure

    StreamId streamId = TestStreams.stream("restore-autocommit");
    assertThrows(
        EventStoreException.class,
        () -> store.append(streamId, List.of(envelope(streamId)), Version.initial()));

    verify(conn).setAutoCommit(false); // the append opened the transaction
    verify(conn).setAutoCommit(true); // the finally restored it before the connection returned
    verify(conn).rollback();
  }

  @Test
  void appendSucceedsWhenPostCommitNotifyFails() throws Exception {
    when(reserveRs.next()).thenReturn(true);
    when(reserveRs.getLong(1)).thenReturn(7L); // reserved exactly 1 offset -> base=6, offset=7

    // NOTIFY is best-effort: any failure after commit must not fail the append. RuntimeException
    // (not SQLException) proves the catch is not limited to SQL errors.
    when(conn.prepareStatement(argThat(containsSql("pg_notify"))))
        .thenThrow(new RuntimeException("listener wakeup failed"));

    StreamId streamId = TestStreams.stream("notify-fails");
    var result = store.append(streamId, List.of(envelope(streamId)), Version.initial());

    assertEquals(List.of(GlobalOffset.of(7L)), result.globalOffsets());
    assertEquals(1, result.finalVersion().value());
    verify(conn).commit();
    verify(conn, never()).rollback();
  }

  @Test
  void appendPinsIsolationOnTheTransaction_neverOnTheConnectionsSession() throws Exception {
    // Connection.setTransactionIsolation is SET SESSION CHARACTERISTICS on pgjdbc —
    // a session-scoped change that outlives the transaction AND the connection's return to the
    // pool, silently downgrading every later borrower. The pin must be the transaction-scoped
    // statement instead, issued while the transaction is still empty so PostgreSQL accepts it.
    when(reserveRs.next()).thenReturn(true);
    when(reserveRs.getLong(1)).thenReturn(7L);

    StreamId streamId = TestStreams.stream("isolation-pin");
    store.append(streamId, List.of(envelope(streamId)), Version.initial());

    var order = org.mockito.Mockito.inOrder(conn, pinStmt, reservePs);
    order.verify(conn).setAutoCommit(false);
    // The statement bound rides on the same round trip, transaction-scoped as well.
    order
        .verify(pinStmt)
        .execute(
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED; SET LOCAL statement_timeout = 30000");
    order.verify(reservePs).executeQuery();
    verify(conn, never()).setTransactionIsolation(org.mockito.ArgumentMatchers.anyInt());
  }
}
