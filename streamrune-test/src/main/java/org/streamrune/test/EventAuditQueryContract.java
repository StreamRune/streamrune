package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.audit.EventAuditQuery;
import org.streamrune.core.audit.EventAuditStore;
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

/**
 * What every {@link EventAuditQuery} must honour over the entries an {@link EventAuditStore} wrote,
 * taken from the interface's Javadoc: the five finders by correlation id, command id, causation id,
 * stream and user (the last two with an inclusive time window), each in an unpaged and a paged
 * form; {@code occurredAt}-ascending order; an empty result (never {@code null}) for no match; the
 * paged forms' slicing, total, and ignored sort; the {@code traceFullCausationChain} of an event
 * (itself plus every transitive descendant, none of its ancestors); {@code save} and {@code
 * saveAll} writing alike, and a failed batch leaving nothing to find; and null arguments failing
 * with {@link NullPointerException}. Every field of an entry reads back as written. There is no
 * in-memory audit store: run by {@code PostgresAuditContractsTest}.
 */
public abstract class EventAuditQueryContract {

  private static final Instant T0 = Instant.parse("2026-01-01T10:00:00.123Z");
  private static final Instant FROM = T0;
  private static final Instant TO = T0.plusSeconds(3600);
  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("order"), AggregateId.of("order-1"));
  private static final CorrelationId CORRELATION = CorrelationId.of("corr-1");
  private static final CommandId COMMAND = CommandId.of("cmd-1");
  private static final CausationId CAUSATION = CausationId.of("cause-1");
  private static final UserId USER = UserId.of("user-1");

  /** The sink the entries are written through; it shares its storage with {@link #query()}. */
  protected abstract EventAuditStore writer();

  /** The query under test, over the storage {@link #writer()} writes. */
  protected abstract EventAuditQuery query();

  private EventAuditStore writer;
  private EventAuditQuery query;

  @BeforeEach
  void fresh() {
    writer = writer();
    query = query();
  }

  // ---------------------------------------------------------------- fixtures

  private static EventAuditEntry entry(
      String eventId,
      StreamId stream,
      CommandId command,
      CorrelationId correlation,
      CausationId causation,
      UserId user,
      Instant at) {
    return new EventAuditEntry(
        EventId.of(eventId),
        EventType.of("OrderPlaced"),
        stream,
        new Version(1),
        command,
        correlation,
        causation,
        user,
        at);
  }

  /** The entry every finder matches: one correlation, command, causation, stream and user. */
  private static EventAuditEntry match(String eventId, Instant at) {
    return entry(eventId, STREAM, COMMAND, CORRELATION, CAUSATION, USER, at);
  }

  /** An entry that differs from {@link #match} in every finder's key. */
  private static EventAuditEntry noise(String eventId, Instant at) {
    return entry(
        eventId,
        StreamId.of(AggregateType.of("order"), AggregateId.of("order-2")),
        CommandId.of("cmd-other"),
        CorrelationId.of("corr-other"),
        CausationId.of("cause-other"),
        UserId.of("user-other"),
        at);
  }

  private static List<String> ids(List<EventAuditEntry> entries) {
    return entries.stream().map(e -> e.eventId().value()).toList();
  }

  private static List<String> ids(Page<EventAuditEntry> page) {
    return ids(page.content());
  }

  /** One finder, in its unpaged and paged form, asked for the entries {@link #match} writes. */
  private interface Finder {
    List<EventAuditEntry> all(EventAuditQuery q, Instant from, Instant to);

    Page<EventAuditEntry> page(EventAuditQuery q, Instant from, Instant to, PageRequest request);
  }

  /** Correlation, command and causation take no window: from and to are ignored by them. */
  private static final List<Finder> FINDERS =
      List.of(
          new Finder() {
            public List<EventAuditEntry> all(EventAuditQuery q, Instant from, Instant to) {
              return q.findByCorrelationId(CORRELATION);
            }

            public Page<EventAuditEntry> page(
                EventAuditQuery q, Instant from, Instant to, PageRequest r) {
              return q.findByCorrelationId(CORRELATION, r);
            }
          },
          new Finder() {
            public List<EventAuditEntry> all(EventAuditQuery q, Instant from, Instant to) {
              return q.findByCommandId(COMMAND);
            }

            public Page<EventAuditEntry> page(
                EventAuditQuery q, Instant from, Instant to, PageRequest r) {
              return q.findByCommandId(COMMAND, r);
            }
          },
          new Finder() {
            public List<EventAuditEntry> all(EventAuditQuery q, Instant from, Instant to) {
              return q.findByCausationId(CAUSATION);
            }

            public Page<EventAuditEntry> page(
                EventAuditQuery q, Instant from, Instant to, PageRequest r) {
              return q.findByCausationId(CAUSATION, r);
            }
          },
          new Finder() {
            public List<EventAuditEntry> all(EventAuditQuery q, Instant from, Instant to) {
              return q.findByStreamId(STREAM, from, to);
            }

            public Page<EventAuditEntry> page(
                EventAuditQuery q, Instant from, Instant to, PageRequest r) {
              return q.findByStreamId(STREAM, from, to, r);
            }
          },
          new Finder() {
            public List<EventAuditEntry> all(EventAuditQuery q, Instant from, Instant to) {
              return q.findByUserId(USER, from, to);
            }

            public Page<EventAuditEntry> page(
                EventAuditQuery q, Instant from, Instant to, PageRequest r) {
              return q.findByUserId(USER, from, to, r);
            }
          });

  /** The two finders that take a time window: stream and user. */
  private static final List<Finder> WINDOWED = List.of(FINDERS.get(3), FINDERS.get(4));

  // ---------------------------------------------------------------- round trip

  @Test
  void anEntryReadsBackWithEveryFieldAsWritten() {
    EventAuditEntry written =
        new EventAuditEntry(
            EventId.of("e-1"),
            EventType.of("OrderPlaced"),
            STREAM,
            new Version(7),
            COMMAND,
            CORRELATION,
            CAUSATION,
            USER,
            Instant.parse("2026-01-01T10:00:00.123Z"));
    writer.save(written);

    assertEquals(List.of(written), query.findByCorrelationId(CORRELATION));
  }

  @Test
  void aRootEntryReadsBackWithoutCausationOrUser() {
    EventAuditEntry written = entry("e-1", STREAM, COMMAND, CORRELATION, null, null, T0);
    writer.save(written);

    EventAuditEntry read = query.findByCorrelationId(CORRELATION).get(0);
    assertEquals(written, read);
    assertNull(read.causationId());
    assertNull(read.userId());
  }

  // ---------------------------------------------------------------- filters

  @Test
  void eachFinderReturnsOnlyTheEntriesOfItsKey() {
    writer.save(match("m-1", T0.plusSeconds(1)));
    writer.save(match("m-2", T0.plusSeconds(2)));
    writer.save(noise("n-1", T0.plusSeconds(3)));

    for (Finder finder : FINDERS) {
      assertEquals(List.of("m-1", "m-2"), ids(finder.all(query, FROM, TO)));
      assertEquals(List.of("m-1", "m-2"), ids(finder.page(query, FROM, TO, PageRequest.of(0, 10))));
    }
  }

  @Test
  void anAnonymousEntryIsNeverReturnedByUser() {
    writer.save(match("m-1", T0.plusSeconds(1)));
    writer.save(entry("anon", STREAM, COMMAND, CORRELATION, CAUSATION, null, T0.plusSeconds(2)));

    assertEquals(List.of("m-1"), ids(query.findByUserId(USER, FROM, TO)));
  }

  /** A stream is an aggregate type and an id: two types sharing an id never see each other. */
  @Test
  void aStreamIsATypeAndAnIdSoTwoTypesSharingAnIdDoNotMix() {
    StreamId otherType = StreamId.of(AggregateType.of("cart"), AggregateId.of("order-1"));
    writer.save(match("order", T0.plusSeconds(1)));
    writer.save(entry("cart", otherType, COMMAND, CORRELATION, CAUSATION, USER, T0.plusSeconds(2)));

    assertEquals(List.of("order"), ids(query.findByStreamId(STREAM, FROM, TO)));
    assertEquals(List.of("cart"), ids(query.findByStreamId(otherType, FROM, TO)));
  }

  // ---------------------------------------------------------------- time window

  @Test
  void theTimeWindowOfStreamAndUserIsInclusiveAtBothEnds() {
    writer.save(match("before", FROM.minusMillis(1)));
    writer.save(match("at-from", FROM));
    writer.save(match("inside", FROM.plusSeconds(1)));
    writer.save(match("at-to", TO));
    writer.save(match("after", TO.plusMillis(1)));

    for (Finder finder : WINDOWED) {
      assertEquals(List.of("at-from", "inside", "at-to"), ids(finder.all(query, FROM, TO)));
      assertEquals(
          List.of("at-from", "inside", "at-to"),
          ids(finder.page(query, FROM, TO, PageRequest.of(0, 10))));
    }
  }

  @Test
  void aWindowHoldingNoEntryYieldsAnEmptyResult() {
    writer.save(match("m-1", T0.plusSeconds(1)));
    Instant later = TO.plusSeconds(10);

    for (Finder finder : WINDOWED) {
      assertTrue(finder.all(query, later, later.plusSeconds(60)).isEmpty());
      Page<EventAuditEntry> page =
          finder.page(query, later, later.plusSeconds(60), PageRequest.of(0, 10));
      assertTrue(page.content().isEmpty());
      assertEquals(0, page.totalElements());
    }
  }

  // ---------------------------------------------------------------- order

  /** Written newest-first, a millisecond apart: every finder answers occurredAt ascending. */
  @Test
  void entriesAreOrderedByOccurredAtAscendingEvenAMillisecondApart() {
    writer.save(match("c", T0.plusMillis(2)));
    writer.save(match("a", T0));
    writer.save(match("b", T0.plusMillis(1)));

    for (Finder finder : FINDERS) {
      assertEquals(List.of("a", "b", "c"), ids(finder.all(query, FROM, TO)));
      assertEquals(
          List.of("a", "b", "c"), ids(finder.page(query, FROM, TO, PageRequest.of(0, 10))));
    }
  }

  @Test
  void noMatchYieldsAnEmptyListNeverNull() {
    for (Finder finder : FINDERS) {
      List<EventAuditEntry> all = finder.all(query, FROM, TO);
      assertNotNull(all);
      assertTrue(all.isEmpty());
    }
    assertTrue(query.traceFullCausationChain(EventId.of("none")).isEmpty());
  }

  // ---------------------------------------------------------------- paging

  @Test
  void theSortOfThePageRequestIsIgnored() {
    writer.save(match("a", T0));
    writer.save(match("b", T0.plusMillis(1)));
    writer.save(match("c", T0.plusMillis(2)));
    PageRequest descending = PageRequest.of(0, 10, Sort.by("occurredAt", SortDirection.DESC));

    for (Finder finder : FINDERS) {
      assertEquals(List.of("a", "b", "c"), ids(finder.page(query, FROM, TO, descending)));
    }
  }

  @Test
  void pagesSliceTheOrderedResultAndCarryTheTotal() {
    for (int i = 0; i < 5; i++) {
      writer.save(match("m-" + i, T0.plusSeconds(i)));
    }
    writer.save(noise("n-1", T0.plusSeconds(9)));

    for (Finder finder : FINDERS) {
      Page<EventAuditEntry> first = finder.page(query, FROM, TO, PageRequest.of(0, 2));
      Page<EventAuditEntry> second = finder.page(query, FROM, TO, PageRequest.of(1, 2));
      Page<EventAuditEntry> last = finder.page(query, FROM, TO, PageRequest.of(2, 2));

      assertEquals(List.of("m-0", "m-1"), ids(first));
      assertEquals(List.of("m-2", "m-3"), ids(second));
      assertEquals(List.of("m-4"), ids(last));
      for (Page<EventAuditEntry> page : List.of(first, second, last)) {
        assertEquals(5, page.totalElements());
        assertEquals(2, page.size());
      }
      assertEquals(0, first.page());
      assertEquals(2, last.page());
    }
  }

  @Test
  void aPageBeyondTheLastIsEmptyAndStillCarriesTheTotal() {
    writer.save(match("m-1", T0.plusSeconds(1)));
    writer.save(match("m-2", T0.plusSeconds(2)));

    for (Finder finder : FINDERS) {
      Page<EventAuditEntry> page = finder.page(query, FROM, TO, PageRequest.of(5, 10));
      assertTrue(page.content().isEmpty());
      assertEquals(2, page.totalElements());
      assertEquals(5, page.page());
    }
  }

  @Test
  void aPageOfNoMatchIsEmptyNeverNull() {
    for (Finder finder : FINDERS) {
      Page<EventAuditEntry> page = finder.page(query, FROM, TO, PageRequest.of(0, 10));
      assertNotNull(page);
      assertTrue(page.content().isEmpty());
      assertEquals(0, page.totalElements());
    }
  }

  // ---------------------------------------------------------------- save and saveAll

  @Test
  void saveAndSaveAllWriteAlike() {
    EventAuditEntry single = match("single", T0);
    EventAuditEntry batched1 = match("batched-1", T0.plusMillis(1));
    EventAuditEntry batched2 =
        entry("batched-2", STREAM, COMMAND, CORRELATION, null, null, T0.plusMillis(2));
    writer.save(single);
    writer.saveAll(List.of(batched1, batched2));

    assertEquals(List.of(single, batched1, batched2), query.findByCorrelationId(CORRELATION));
  }

  @Test
  void aFailedBatchLeavesNoEntryToFind() {
    writer.save(match("existing", T0));

    assertThrows(
        RuntimeException.class,
        () -> writer.saveAll(List.of(match("fresh", T0.plusSeconds(1)), match("existing", T0))));

    assertEquals(List.of("existing"), ids(query.findByCorrelationId(CORRELATION)));
  }

  // ---------------------------------------------------------------- traceFullCausationChain

  /**
   * root -> child-1 -> grandchild, root -> child-2; parent caused root, stranger is unrelated.
   * Written out of time order.
   */
  private void writeCausalTree() {
    writer.saveAll(
        List.of(
            causedBy("grandchild", "child-1", T0.plusSeconds(3)),
            causedBy("child-2", "root", T0.plusSeconds(2)),
            causedBy("root", "parent", T0.plusSeconds(1)),
            causedBy("child-1", "root", T0.plusMillis(1500)),
            entry("parent", STREAM, COMMAND, CORRELATION, null, USER, T0),
            entry("stranger", STREAM, COMMAND, CORRELATION, null, USER, T0.plusSeconds(4))));
  }

  private static EventAuditEntry causedBy(String eventId, String cause, Instant at) {
    return entry(eventId, STREAM, COMMAND, CORRELATION, CausationId.of(cause), USER, at);
  }

  @Test
  void theCausalChainHoldsTheEventAndEveryTransitiveDescendantOldestFirst() {
    writeCausalTree();

    assertEquals(
        List.of("root", "child-1", "child-2", "grandchild"),
        ids(query.traceFullCausationChain(EventId.of("root"))));
  }

  @Test
  void theCausalChainOfADescendantHoldsOnlyItsOwnSubtree() {
    writeCausalTree();

    assertEquals(
        List.of("child-1", "grandchild"),
        ids(query.traceFullCausationChain(EventId.of("child-1"))));
    assertEquals(List.of("child-2"), ids(query.traceFullCausationChain(EventId.of("child-2"))));
  }

  @Test
  void theCausalChainOfALeafIsItself() {
    writeCausalTree();

    assertEquals(
        List.of("grandchild"), ids(query.traceFullCausationChain(EventId.of("grandchild"))));
  }

  @Test
  void theCausalChainOfAnUnknownEventIsEmpty() {
    writeCausalTree();

    assertFalse(query.traceFullCausationChain(EventId.of("root")).isEmpty());
    assertTrue(query.traceFullCausationChain(EventId.of("unknown")).isEmpty());
  }

  // ---------------------------------------------------------------- null arguments

  @Test
  void nullArgumentsAreRefusedWithANullPointerException() {
    PageRequest page = PageRequest.of(0, 10);
    assertThrows(NullPointerException.class, () -> query.findByCorrelationId(null));
    assertThrows(NullPointerException.class, () -> query.findByCorrelationId(null, page));
    assertThrows(NullPointerException.class, () -> query.findByCorrelationId(CORRELATION, null));
    assertThrows(NullPointerException.class, () -> query.findByCommandId(null));
    assertThrows(NullPointerException.class, () -> query.findByCommandId(null, page));
    assertThrows(NullPointerException.class, () -> query.findByCommandId(COMMAND, null));
    assertThrows(NullPointerException.class, () -> query.findByCausationId(null));
    assertThrows(NullPointerException.class, () -> query.findByCausationId(null, page));
    assertThrows(NullPointerException.class, () -> query.findByCausationId(CAUSATION, null));
    assertThrows(NullPointerException.class, () -> query.findByStreamId(null, FROM, TO));
    assertThrows(NullPointerException.class, () -> query.findByStreamId(STREAM, null, TO));
    assertThrows(NullPointerException.class, () -> query.findByStreamId(STREAM, FROM, null));
    assertThrows(NullPointerException.class, () -> query.findByStreamId(null, FROM, TO, page));
    assertThrows(NullPointerException.class, () -> query.findByStreamId(STREAM, null, TO, page));
    assertThrows(NullPointerException.class, () -> query.findByStreamId(STREAM, FROM, null, page));
    assertThrows(NullPointerException.class, () -> query.findByStreamId(STREAM, FROM, TO, null));
    assertThrows(NullPointerException.class, () -> query.findByUserId(null, FROM, TO));
    assertThrows(NullPointerException.class, () -> query.findByUserId(USER, null, TO));
    assertThrows(NullPointerException.class, () -> query.findByUserId(USER, FROM, null));
    assertThrows(NullPointerException.class, () -> query.findByUserId(null, FROM, TO, page));
    assertThrows(NullPointerException.class, () -> query.findByUserId(USER, null, TO, page));
    assertThrows(NullPointerException.class, () -> query.findByUserId(USER, FROM, null, page));
    assertThrows(NullPointerException.class, () -> query.findByUserId(USER, FROM, TO, null));
    assertThrows(NullPointerException.class, () -> query.traceFullCausationChain(null));
  }
}
