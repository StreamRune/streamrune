package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.audit.EventAuditEntry;
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

class InMemoryEventAuditStoreTest {

  private static final AggregateType TYPE = AggregateType.of("stream");

  private static final Instant T0 = Instant.parse("2026-01-01T10:00:00Z");
  private static final Instant T1 = Instant.parse("2026-01-01T10:00:01Z");
  private static final Instant T2 = Instant.parse("2026-01-01T10:00:02Z");
  private static final Instant T3 = Instant.parse("2026-01-01T10:00:03Z");
  private static final Instant T4 = Instant.parse("2026-01-01T10:00:04Z");

  private InMemoryEventAuditStore store;

  @BeforeEach
  void setUp() {
    store = new InMemoryEventAuditStore();
  }

  // ---- helper ---------------------------------------------------------------

  private EventAuditEntry entry(
      String eventId,
      String correlationId,
      String commandId,
      String causationId,
      String streamId,
      String userId,
      Instant occurredAt) {
    return new EventAuditEntry(
        EventId.of(eventId),
        new EventType("OrderCreated"),
        StreamId.of(TYPE, AggregateId.of(streamId)),
        Version.initial(),
        CommandId.of(commandId),
        CorrelationId.of(correlationId),
        causationId != null ? CausationId.of(causationId) : null,
        userId != null ? UserId.of(userId) : null,
        occurredAt);
  }

  // ---- tests ----------------------------------------------------------------

  /** Save 3 entries (2 with corr-1, 1 with corr-2); findByCorrelationId(corr-1) returns 2. */
  @Test
  void saveAndFindByCorrelationId() {
    store.save(entry("e1", "corr-1", "cmd-1", null, "stream-A", null, T0));
    store.save(entry("e2", "corr-1", "cmd-2", null, "stream-A", null, T1));
    store.save(entry("e3", "corr-2", "cmd-3", null, "stream-B", null, T2));

    List<EventAuditEntry> result = store.findByCorrelationId(CorrelationId.of("corr-1"));

    assertEquals(2, result.size());
    assertEquals("e1", result.get(0).eventId().value());
    assertEquals("e2", result.get(1).eventId().value());
  }

  /** 3 entries, 2 with cmd-1; findByCommandId(cmd-1) returns 2 sorted by occurredAt. */
  @Test
  void findByCommandId() {
    store.save(entry("e1", "corr-1", "cmd-1", null, "stream-A", null, T1));
    store.save(entry("e2", "corr-1", "cmd-1", null, "stream-A", null, T0));
    store.save(entry("e3", "corr-2", "cmd-2", null, "stream-B", null, T2));

    List<EventAuditEntry> result = store.findByCommandId(CommandId.of("cmd-1"));

    assertEquals(2, result.size());
    // sorted ascending by occurredAt
    assertEquals("e2", result.get(0).eventId().value());
    assertEquals("e1", result.get(1).eventId().value());
  }

  /** Entries with causationId "e1"; findByCausationId("e1") returns 2 children. */
  @Test
  void findByCausationId() {
    store.save(entry("e1", "corr-1", "cmd-1", null, "stream-A", null, T0));
    store.save(entry("e2", "corr-1", "cmd-2", "e1", "stream-A", null, T1));
    store.save(entry("e3", "corr-1", "cmd-3", "e1", "stream-A", null, T2));
    store.save(entry("e4", "corr-2", "cmd-4", "e2", "stream-B", null, T3));

    List<EventAuditEntry> result = store.findByCausationId(CausationId.of("e1"));

    assertEquals(2, result.size());
    assertEquals("e2", result.get(0).eventId().value());
    assertEquals("e3", result.get(1).eventId().value());
  }

  /** Filter by streamId + time range; verify inclusive boundary handling. */
  @Test
  void findByStreamId() {
    store.save(entry("e1", "corr-1", "cmd-1", null, "stream-A", null, T0)); // before range
    store.save(entry("e2", "corr-1", "cmd-2", null, "stream-A", null, T1)); // at from
    store.save(entry("e3", "corr-1", "cmd-3", null, "stream-A", null, T2)); // inside
    store.save(entry("e4", "corr-1", "cmd-4", null, "stream-A", null, T3)); // at to
    store.save(entry("e5", "corr-2", "cmd-5", null, "stream-A", null, T4)); // after range
    store.save(entry("e6", "corr-3", "cmd-6", null, "stream-B", null, T2)); // different stream

    List<EventAuditEntry> result =
        store.findByStreamId(StreamId.of(TYPE, AggregateId.of("stream-A")), T1, T3);

    assertEquals(3, result.size());
    assertEquals("e2", result.get(0).eventId().value());
    assertEquals("e3", result.get(1).eventId().value());
    assertEquals("e4", result.get(2).eventId().value());
  }

  /** Filter by userId + time range; entries with null userId are excluded. */
  @Test
  void findByUserId() {
    store.save(entry("e1", "corr-1", "cmd-1", null, "stream-A", "user-1", T1)); // at from
    store.save(entry("e2", "corr-1", "cmd-2", null, "stream-A", "user-1", T2)); // inside
    store.save(entry("e3", "corr-1", "cmd-3", null, "stream-A", null, T2)); // null userId
    store.save(entry("e4", "corr-2", "cmd-4", null, "stream-B", "user-1", T3)); // at to
    store.save(entry("e5", "corr-3", "cmd-5", null, "stream-C", "user-1", T0)); // before from

    List<EventAuditEntry> result = store.findByUserId(UserId.of("user-1"), T1, T3);

    assertEquals(3, result.size());
    assertEquals("e1", result.get(0).eventId().value());
    assertEquals("e2", result.get(1).eventId().value());
    assertEquals("e4", result.get(2).eventId().value());
  }

  /**
   * BFS causation chain: e1 -> e2 -> e3, e1 -> e4 (branch), e5 unrelated. Chain from e1 should
   * include e1, e2, e3, e4 (4 events); e5 excluded.
   */
  @Test
  void traceFullCausationChain() {
    store.save(entry("e1", "corr-1", "cmd-1", null, "stream-A", null, T0));
    store.save(entry("e2", "corr-1", "cmd-2", "e1", "stream-A", null, T1));
    store.save(entry("e3", "corr-1", "cmd-3", "e2", "stream-A", null, T2));
    store.save(entry("e4", "corr-1", "cmd-4", "e1", "stream-A", null, T3));
    store.save(entry("e5", "corr-2", "cmd-5", null, "stream-B", null, T4)); // unrelated

    List<EventAuditEntry> chain = store.traceFullCausationChain(EventId.of("e1"));

    assertEquals(4, chain.size());
    // sorted by occurredAt ascending
    assertEquals("e1", chain.get(0).eventId().value());
    assertEquals("e2", chain.get(1).eventId().value());
    assertEquals("e3", chain.get(2).eventId().value());
    assertEquals("e4", chain.get(3).eventId().value());
  }

  /** A leaf event (no children) produces a chain of 1 (itself). */
  @Test
  void traceFullCausationChainReturnsOnlyRootForLeafEvent() {
    store.save(entry("e1", "corr-1", "cmd-1", null, "stream-A", null, T0));

    List<EventAuditEntry> chain = store.traceFullCausationChain(EventId.of("e1"));

    assertEquals(1, chain.size());
    assertEquals("e1", chain.get(0).eventId().value());
  }

  /** Unknown event ID produces an empty list, not an exception. */
  @Test
  void traceFullCausationChainReturnsEmptyForUnknownEvent() {
    store.save(entry("e1", "corr-1", "cmd-1", null, "stream-A", null, T0));

    List<EventAuditEntry> chain = store.traceFullCausationChain(EventId.of("unknown"));

    assertTrue(chain.isEmpty());
  }

  /** save(null) must throw NullPointerException. */
  @Test
  void saveRejectsNull() {
    assertThrows(NullPointerException.class, () -> store.save(null));
  }

  /** saveAll(null) must throw NullPointerException. */
  @Test
  void saveAllRejectsNull() {
    assertThrows(NullPointerException.class, () -> store.saveAll(null));
  }
}
