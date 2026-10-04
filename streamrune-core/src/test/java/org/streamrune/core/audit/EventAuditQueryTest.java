package org.streamrune.core.audit;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.streamrune.core.PageRequest;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;

/** Pins the in-memory slicing contract of the default paged {@code find*} overloads. */
class EventAuditQueryTest {

  private static final AggregateType TYPE = AggregateType.of("audit");

  private static final Instant BASE = Instant.parse("2026-06-12T00:00:00Z");
  private static final Instant FROM = BASE.minusSeconds(3600);
  private static final Instant TO = BASE.plusSeconds(3600);

  private static EventAuditEntry entry(int i) {
    return new EventAuditEntry(
        EventId.of("evt_" + i),
        new EventType("ItemAdded"),
        StreamId.of(TYPE, AggregateId.of("cart-1")),
        new Version(i + 1),
        CommandId.of("cmd_" + i),
        CorrelationId.of("corr-1"),
        null,
        UserId.of("user-1"),
        BASE.plusSeconds(i));
  }

  private static List<EventAuditEntry> entries(int count) {
    return IntStream.range(0, count).mapToObj(EventAuditQueryTest::entry).toList();
  }

  /** Stub whose unpaged methods all return the same fixed, occurredAt-ascending list. */
  private static EventAuditQuery queryOver(List<EventAuditEntry> all) {
    return new EventAuditQuery() {
      @Override
      public List<EventAuditEntry> findByCorrelationId(CorrelationId correlationId) {
        return all;
      }

      @Override
      public List<EventAuditEntry> findByCommandId(CommandId commandId) {
        return all;
      }

      @Override
      public List<EventAuditEntry> findByCausationId(CausationId causationId) {
        return all;
      }

      @Override
      public List<EventAuditEntry> findByStreamId(StreamId streamId, Instant from, Instant to) {
        return all;
      }

      @Override
      public List<EventAuditEntry> findByUserId(UserId userId, Instant from, Instant to) {
        return all;
      }

      @Override
      public List<EventAuditEntry> traceFullCausationChain(EventId eventId) {
        return all;
      }
    };
  }

  @Test
  void firstPagePreservesOrderAndReportsTotal() {
    var query = queryOver(entries(5));

    var page = query.findByCorrelationId(CorrelationId.of("corr-1"), PageRequest.of(0, 2));

    assertEquals(
        List.of("evt_0", "evt_1"), page.content().stream().map(e -> e.eventId().value()).toList());
    assertEquals(5, page.totalElements());
    assertEquals(3, page.totalPages());
    assertTrue(page.hasNext());
    assertFalse(page.hasPrevious());
  }

  @Test
  void lastPageIsPartial() {
    var query = queryOver(entries(5));

    var page = query.findByCommandId(CommandId.of("cmd_0"), PageRequest.of(2, 2));

    assertEquals(List.of("evt_4"), page.content().stream().map(e -> e.eventId().value()).toList());
    assertFalse(page.hasNext());
    assertTrue(page.hasPrevious());
  }

  @Test
  void pageBeyondResultIsEmptyButKeepsTotal() {
    var query = queryOver(entries(3));

    var page = query.findByCausationId(CausationId.of("cause-1"), PageRequest.of(7, 2));

    assertTrue(page.isEmpty());
    assertEquals(3, page.totalElements());
    assertFalse(page.hasNext());
  }

  @Test
  void emptyResultYieldsEmptyFirstPage() {
    var query = queryOver(List.of());

    var page =
        query.findByStreamId(
            StreamId.of(TYPE, AggregateId.of("cart-1")), FROM, TO, PageRequest.of(0, 10));

    assertTrue(page.isEmpty());
    assertEquals(0, page.totalElements());
    assertEquals(0, page.totalPages());
  }

  @Test
  void pageLargerThanResultReturnsEverything() {
    var query = queryOver(entries(3));

    var page = query.findByUserId(UserId.of("user-1"), FROM, TO, PageRequest.of(0, 100));

    assertEquals(3, page.content().size());
    assertEquals(3, page.totalElements());
    assertFalse(page.hasNext());
  }

  @Test
  void hugePageIndexDoesNotOverflow() {
    var query = queryOver(entries(3));

    var page =
        query.findByUserId(UserId.of("user-1"), FROM, TO, PageRequest.of(Integer.MAX_VALUE, 1000));

    assertTrue(page.isEmpty());
    assertEquals(3, page.totalElements());
  }

  @Test
  void pagedOverloadsRejectNullPageRequest() {
    var query = queryOver(entries(1));

    assertThrows(
        NullPointerException.class,
        () -> query.findByCorrelationId(CorrelationId.of("corr-1"), null));
    assertThrows(
        NullPointerException.class, () -> query.findByCommandId(CommandId.of("cmd_0"), null));
    assertThrows(
        NullPointerException.class, () -> query.findByCausationId(CausationId.of("cause-1"), null));
    assertThrows(
        NullPointerException.class,
        () -> query.findByStreamId(StreamId.of(TYPE, AggregateId.of("cart-1")), FROM, TO, null));
    assertThrows(
        NullPointerException.class, () -> query.findByUserId(UserId.of("user-1"), FROM, TO, null));
  }
}
