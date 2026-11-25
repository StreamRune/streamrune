package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Page;
import org.streamrune.core.PageRequest;
import org.streamrune.core.Sort;
import org.streamrune.core.SortDirection;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.audit.CommandAuditQuery;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * What every {@link CommandAuditQuery} must honour over the entries an {@link AuditStore} wrote,
 * taken from the interface's Javadoc: {@code findByCommandId} (the most recent entry when a retry
 * wrote several), the three paged finders ({@code findByUserId}, {@code findByOutcome}, {@code
 * findByCommandType}) with their inclusive time window, {@code occurredAt}-ascending order that
 * ignores the request's sort, store-side paging with the total, the empty page, command and
 * query-origin entries returned alike, anonymous entries never returned by user; and that null
 * arguments fail with {@link NullPointerException}. Every field of an entry reads back as written.
 * There is no in-memory command audit store: run by {@code PostgresAuditContractsTest}.
 */
public abstract class CommandAuditQueryContract {

  private static final Instant T0 = Instant.parse("2026-01-01T10:00:00.123Z");
  private static final Instant FROM = T0;
  private static final Instant TO = T0.plusSeconds(3600);
  private static final UserId USER = UserId.of("user-1");
  private static final String TYPE = "PlaceOrder";

  /** The sink the entries are written through; it shares its storage with {@link #query()}. */
  protected abstract AuditStore writer();

  /** The query under test, over the storage {@link #writer()} writes. */
  protected abstract CommandAuditQuery query();

  private AuditStore writer;
  private CommandAuditQuery query;

  @BeforeEach
  void fresh() {
    writer = writer();
    query = query();
  }

  // ---------------------------------------------------------------- fixtures

  private static AuditEntry entry(
      String commandId, String type, UserId user, AuditOutcome outcome, Instant at) {
    String error = outcome == AuditOutcome.SUCCESS ? null : "error of " + commandId;
    int events = outcome == AuditOutcome.SUCCESS ? 2 : 0;
    return new AuditEntry(
        CommandId.of(commandId),
        type,
        AggregateType.of("order"),
        AggregateId.of("order-1"),
        user,
        at,
        outcome,
        error,
        events,
        CorrelationId.of("corr-" + commandId));
  }

  /** The entry every filter matches: one user, SUCCESS, one type. */
  private static AuditEntry match(String commandId, Instant at) {
    return entry(commandId, TYPE, USER, AuditOutcome.SUCCESS, at);
  }

  private static List<String> ids(Page<AuditEntry> page) {
    return page.content().stream().map(e -> e.commandId().value()).toList();
  }

  @FunctionalInterface
  private interface Finder {
    Page<AuditEntry> find(CommandAuditQuery query, Instant from, Instant to, PageRequest request);
  }

  /** The three paged finders, each asked for the entries {@link #match} writes. */
  private static final List<Finder> FINDERS =
      List.of(
          (q, from, to, request) -> q.findByUserId(USER, from, to, request),
          (q, from, to, request) -> q.findByOutcome(AuditOutcome.SUCCESS, from, to, request),
          (q, from, to, request) -> q.findByCommandType(TYPE, from, to, request));

  /**
   * Entries that each match two of the three filters and miss one: another user, another outcome,
   * another type. A finder returns the ones that miss a different filter than its own.
   */
  private void writeNoise() {
    writer.save(entry("n-user", TYPE, UserId.of("other"), AuditOutcome.SUCCESS, T0.plusSeconds(5)));
    writer.save(entry("n-outcome", TYPE, USER, AuditOutcome.FAILURE, T0.plusSeconds(6)));
    writer.save(entry("n-type", "CancelOrder", USER, AuditOutcome.SUCCESS, T0.plusSeconds(7)));
  }

  // ---------------------------------------------------------------- findByCommandId

  @Test
  void findByCommandIdReadsBackEveryFieldAsWritten() {
    AuditEntry written =
        new AuditEntry(
            CommandId.of("c-1"),
            TYPE,
            AggregateType.of("order"),
            AggregateId.of("order-7"),
            USER,
            Instant.parse("2026-01-01T10:00:00.123Z"),
            AuditOutcome.SUCCESS,
            null,
            3,
            CorrelationId.of("corr-1"));
    writer.save(written);

    assertEquals(Optional.of(written), query.findByCommandId(CommandId.of("c-1")));
  }

  @Test
  void aFailureEntryReadsBackWithItsErrorMessage() {
    AuditEntry written = entry("c-1", TYPE, USER, AuditOutcome.FAILURE, T0);
    writer.save(written);

    assertEquals(Optional.of(written), query.findByCommandId(CommandId.of("c-1")));
  }

  @Test
  void aVetoedEntryReadsBackWithTheRejectingInterceptor() {
    AuditEntry written =
        new AuditEntry(
            CommandId.of("c-1"),
            TYPE,
            AggregateType.of("order"),
            AggregateId.of("order-1"),
            USER,
            T0,
            AuditOutcome.VETOED,
            "QuotaInterceptor",
            0,
            null);
    writer.save(written);

    assertEquals(Optional.of(written), query.findByCommandId(CommandId.of("c-1")));
  }

  @Test
  void anAnonymousEntryReadsBackWithoutUserOrCorrelation() {
    AuditEntry written =
        new AuditEntry(
            CommandId.of("c-1"),
            TYPE,
            AggregateType.of("order"),
            AggregateId.of("order-1"),
            null,
            T0,
            AuditOutcome.SUCCESS,
            null,
            1,
            null);
    writer.save(written);

    AuditEntry read = query.findByCommandId(CommandId.of("c-1")).orElseThrow();
    assertEquals(written, read);
    assertNull(read.userId());
    assertNull(read.correlationId());
  }

  @Test
  void aSubjectEntryReadsBackWithItsIdAndNoAggregateType() {
    AuditEntry written =
        new AuditEntry(
            CommandId.of("c-1"),
            "ForgetSubject",
            null,
            AggregateId.of("subject-hash"),
            UserId.of("dpo"),
            T0,
            AuditOutcome.SUCCESS,
            null,
            0,
            null);
    writer.save(written);

    assertEquals(Optional.of(written), query.findByCommandId(CommandId.of("c-1")));
  }

  @Test
  void findByCommandIdOfAnUnknownCommandIsEmpty() {
    writer.save(match("c-1", T0));

    assertEquals(Optional.empty(), query.findByCommandId(CommandId.of("unknown")));
  }

  /** A retry runs the same command again and adds an entry: the latest attempt answers. */
  @Test
  void findByCommandIdReturnsTheMostRecentEntryWhenARetryWroteSeveral() {
    AuditEntry latest = entry("c-1", TYPE, USER, AuditOutcome.SUCCESS, T0.plusMillis(2));
    writer.save(latest);
    writer.save(entry("c-1", TYPE, USER, AuditOutcome.FAILURE, T0));
    writer.save(entry("c-1", TYPE, USER, AuditOutcome.FAILURE, T0.plusMillis(1)));

    assertEquals(Optional.of(latest), query.findByCommandId(CommandId.of("c-1")));
  }

  /**
   * Every attempt is kept: three entries under one command id, the latest written in the middle,
   * are all stored, the latest answers findByCommandId, and a paged finder returns all three.
   */
  @Test
  void everyAttemptUnderOneCommandIdIsKept() {
    AuditEntry first = entry("c-1", TYPE, USER, AuditOutcome.FAILURE, T0);
    AuditEntry latest = entry("c-1", TYPE, USER, AuditOutcome.SUCCESS, T0.plusMillis(2));
    AuditEntry middle = entry("c-1", TYPE, USER, AuditOutcome.FAILURE, T0.plusMillis(1));
    writer.save(first);
    writer.save(latest);
    writer.save(middle);

    assertEquals(Optional.of(latest), query.findByCommandId(CommandId.of("c-1")));
    Page<AuditEntry> page = query.findByUserId(USER, FROM, TO, PageRequest.of(0, 10));
    assertEquals(3, page.totalElements());
    assertEquals(List.of(first, middle, latest), page.content());
  }

  @Test
  void aMicrosecondInstantRoundTripsUnchanged() {
    Instant micros = Instant.parse("2026-01-01T10:00:00.123456Z");
    AuditEntry written = entry("c-1", TYPE, USER, AuditOutcome.SUCCESS, micros);
    writer.save(written);

    assertEquals(Optional.of(written), query.findByCommandId(CommandId.of("c-1")));
    assertEquals(
        micros,
        query.findByUserId(USER, FROM, TO, PageRequest.of(0, 1)).content().get(0).occurredAt());
  }

  @Test
  void aNullCommandIdIsRefusedWithANullPointerException() {
    assertThrows(NullPointerException.class, () -> query.findByCommandId(null));
  }

  // ---------------------------------------------------------------- query-origin entries

  /** Queries are audited with no command id and no aggregate; the finders do not discriminate. */
  @Test
  void aQueryOriginEntryIsReturnedByEveryFinderWithNoCommandIdOrAggregate() {
    AuditEntry written =
        new AuditEntry(
            null,
            TYPE,
            null,
            null,
            USER,
            T0.plusMillis(1),
            AuditOutcome.SUCCESS,
            null,
            0,
            CorrelationId.of("corr-q"));
    writer.save(written);

    for (Finder finder : FINDERS) {
      Page<AuditEntry> page = finder.find(query, FROM, TO, PageRequest.of(0, 10));
      assertEquals(List.of(written), page.content());
      assertNull(page.content().get(0).commandId());
      assertNull(page.content().get(0).aggregateType());
      assertNull(page.content().get(0).aggregateId());
    }
  }

  // ---------------------------------------------------------------- filters

  @Test
  void eachFinderAppliesOnlyItsOwnFilter() {
    writer.save(match("m-1", T0.plusSeconds(1)));
    writer.save(match("m-2", T0.plusSeconds(2)));
    writeNoise();

    // The user finder skips n-user only; the outcome finder n-outcome only; the type finder n-type.
    assertEquals(
        List.of("m-1", "m-2", "n-outcome", "n-type"),
        ids(FINDERS.get(0).find(query, FROM, TO, PageRequest.of(0, 10))));
    assertEquals(
        List.of("m-1", "m-2", "n-user", "n-type"),
        ids(FINDERS.get(1).find(query, FROM, TO, PageRequest.of(0, 10))));
    assertEquals(
        List.of("m-1", "m-2", "n-user", "n-outcome"),
        ids(FINDERS.get(2).find(query, FROM, TO, PageRequest.of(0, 10))));
  }

  @Test
  void anAnonymousEntryIsNeverReturnedByUserId() {
    writer.save(match("m-1", T0.plusSeconds(1)));
    writer.save(
        new AuditEntry(
            CommandId.of("anon"),
            TYPE,
            AggregateType.of("order"),
            AggregateId.of("order-1"),
            null,
            T0.plusSeconds(2),
            AuditOutcome.SUCCESS,
            null,
            1,
            null));

    assertEquals(List.of("m-1"), ids(query.findByUserId(USER, FROM, TO, PageRequest.of(0, 10))));
  }

  @Test
  void findByOutcomeSeparatesAllThreeOutcomes() {
    writer.save(entry("s", TYPE, USER, AuditOutcome.SUCCESS, T0.plusSeconds(1)));
    writer.save(entry("f", TYPE, USER, AuditOutcome.FAILURE, T0.plusSeconds(2)));
    writer.save(entry("v", TYPE, USER, AuditOutcome.VETOED, T0.plusSeconds(3)));

    for (AuditOutcome outcome : AuditOutcome.values()) {
      Page<AuditEntry> page = query.findByOutcome(outcome, FROM, TO, PageRequest.of(0, 10));
      assertEquals(1, page.totalElements(), outcome.name());
      assertEquals(outcome, page.content().get(0).outcome());
    }
  }

  // ---------------------------------------------------------------- time window

  /** Both ends of the window are inclusive; an entry a millisecond outside is not returned. */
  @Test
  void theTimeWindowIsInclusiveAtBothEndsAndExcludesMillisecondsOutside() {
    writer.save(match("before", FROM.minusMillis(1)));
    writer.save(match("at-from", FROM));
    writer.save(match("inside", FROM.plusSeconds(1)));
    writer.save(match("at-to", TO));
    writer.save(match("after", TO.plusMillis(1)));

    for (Finder finder : FINDERS) {
      assertEquals(
          List.of("at-from", "inside", "at-to"),
          ids(finder.find(query, FROM, TO, PageRequest.of(0, 10))));
    }
  }

  @Test
  void aWindowHoldingNoEntryYieldsAnEmptyPage() {
    writer.save(match("m-1", T0.plusSeconds(1)));
    Instant later = TO.plusSeconds(10);

    for (Finder finder : FINDERS) {
      Page<AuditEntry> page =
          finder.find(query, later, later.plusSeconds(60), PageRequest.of(0, 10));
      assertNotNull(page);
      assertTrue(page.content().isEmpty());
      assertEquals(0, page.totalElements());
    }
  }

  /** A window whose from is after its to holds nothing. */
  @Test
  void aWindowWhoseFromIsAfterItsToYieldsAnEmptyPage() {
    writer.save(match("m-1", T0.plusSeconds(1)));
    writer.save(match("m-2", T0.plusSeconds(2)));

    for (Finder finder : FINDERS) {
      Page<AuditEntry> page = finder.find(query, TO, FROM, PageRequest.of(0, 10));
      assertTrue(page.content().isEmpty());
      assertEquals(0, page.totalElements());
    }
  }

  // ---------------------------------------------------------------- order and paging

  /** Written newest-first, a millisecond apart: the page is occurredAt ascending regardless. */
  @Test
  void entriesAreOrderedByOccurredAtAscendingEvenAMillisecondApart() {
    writer.save(match("c", T0.plusMillis(2)));
    writer.save(match("a", T0));
    writer.save(match("b", T0.plusMillis(1)));

    for (Finder finder : FINDERS) {
      assertEquals(
          List.of("a", "b", "c"), ids(finder.find(query, FROM, TO, PageRequest.of(0, 10))));
    }
  }

  @Test
  void theSortOfThePageRequestIsIgnored() {
    writer.save(match("a", T0));
    writer.save(match("b", T0.plusMillis(1)));
    writer.save(match("c", T0.plusMillis(2)));
    PageRequest descending = PageRequest.of(0, 10, Sort.by("occurredAt", SortDirection.DESC));

    for (Finder finder : FINDERS) {
      assertEquals(List.of("a", "b", "c"), ids(finder.find(query, FROM, TO, descending)));
    }
  }

  @Test
  void pagesSliceTheOrderedResultAndCarryTheTotal() {
    for (int i = 0; i < 5; i++) {
      writer.save(match("m-" + i, T0.plusSeconds(i)));
    }

    for (Finder finder : FINDERS) {
      Page<AuditEntry> first = finder.find(query, FROM, TO, PageRequest.of(0, 2));
      Page<AuditEntry> second = finder.find(query, FROM, TO, PageRequest.of(1, 2));
      Page<AuditEntry> last = finder.find(query, FROM, TO, PageRequest.of(2, 2));

      assertEquals(List.of("m-0", "m-1"), ids(first));
      assertEquals(List.of("m-2", "m-3"), ids(second));
      assertEquals(List.of("m-4"), ids(last));
      for (Page<AuditEntry> page : List.of(first, second, last)) {
        assertEquals(5, page.totalElements());
        assertEquals(2, page.size());
      }
      assertEquals(0, first.page());
      assertEquals(2, last.page());
    }
  }

  /**
   * Entries with one timestamp keep one position across pages: read page by page, each appears
   * exactly once, before the later entry, and a second reading gives the same order.
   */
  @Test
  void entriesWithAnEqualTimestampAppearExactlyOnceAcrossPages() {
    for (int i = 0; i < 5; i++) {
      writer.save(match("tie-" + i, T0.plusSeconds(1)));
    }
    writer.save(match("later", T0.plusSeconds(2)));

    for (Finder finder : FINDERS) {
      List<String> read = new java.util.ArrayList<>();
      List<String> readAgain = new java.util.ArrayList<>();
      for (int p = 0; p < 3; p++) {
        Page<AuditEntry> page = finder.find(query, FROM, TO, PageRequest.of(p, 2));
        assertEquals(6, page.totalElements());
        read.addAll(ids(page));
        readAgain.addAll(ids(finder.find(query, FROM, TO, PageRequest.of(p, 2))));
      }
      assertEquals(
          List.of("later", "tie-0", "tie-1", "tie-2", "tie-3", "tie-4"),
          read.stream().sorted().toList(),
          "every entry once");
      assertEquals("later", read.getLast());
      assertEquals(read, readAgain);
    }
  }

  @Test
  void aPageBeyondTheLastIsEmptyAndStillCarriesTheTotal() {
    writer.save(match("m-1", T0.plusSeconds(1)));
    writer.save(match("m-2", T0.plusSeconds(2)));

    for (Finder finder : FINDERS) {
      Page<AuditEntry> page = finder.find(query, FROM, TO, PageRequest.of(5, 10));
      assertTrue(page.content().isEmpty());
      assertEquals(2, page.totalElements());
      assertEquals(5, page.page());
    }
  }

  @Test
  void noMatchYieldsAnEmptyPageNeverNull() {
    for (Finder finder : FINDERS) {
      Page<AuditEntry> page = finder.find(query, FROM, TO, PageRequest.of(0, 10));
      assertNotNull(page);
      assertTrue(page.content().isEmpty());
      assertEquals(0, page.totalElements());
    }
  }

  // ---------------------------------------------------------------- null arguments

  @Test
  void nullArgumentsAreRefusedWithANullPointerException() {
    PageRequest page = PageRequest.of(0, 10);
    assertThrows(NullPointerException.class, () -> query.findByUserId(null, FROM, TO, page));
    assertThrows(NullPointerException.class, () -> query.findByUserId(USER, null, TO, page));
    assertThrows(NullPointerException.class, () -> query.findByUserId(USER, FROM, null, page));
    assertThrows(NullPointerException.class, () -> query.findByUserId(USER, FROM, TO, null));
    assertThrows(NullPointerException.class, () -> query.findByOutcome(null, FROM, TO, page));
    assertThrows(
        NullPointerException.class,
        () -> query.findByOutcome(AuditOutcome.SUCCESS, null, TO, page));
    assertThrows(
        NullPointerException.class,
        () -> query.findByOutcome(AuditOutcome.SUCCESS, FROM, null, page));
    assertThrows(
        NullPointerException.class,
        () -> query.findByOutcome(AuditOutcome.SUCCESS, FROM, TO, null));
    assertThrows(NullPointerException.class, () -> query.findByCommandType(null, FROM, TO, page));
    assertThrows(NullPointerException.class, () -> query.findByCommandType(TYPE, null, TO, page));
    assertThrows(NullPointerException.class, () -> query.findByCommandType(TYPE, FROM, null, page));
    assertThrows(NullPointerException.class, () -> query.findByCommandType(TYPE, FROM, TO, null));
  }
}
