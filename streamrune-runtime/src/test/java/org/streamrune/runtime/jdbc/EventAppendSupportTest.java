package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Closeable;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore.AppendResult;
import org.streamrune.core.EventStore.IdempotentAppendResult;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.audit.EventAuditEntry;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.jdbc.EventAppendSupport.EventRow;
import org.streamrune.runtime.jdbc.EventAppendSupport.JsonGuard;
import org.streamrune.runtime.jdbc.EventAppendSupport.SnapshotPayload;
import org.streamrune.test.InMemoryCommandInbox;

class EventAppendSupportTest {

  /** An event. */
  public record Opened(String name) implements DomainEvent {}

  /** A snapshot state, registered under its simple name. */
  public record CounterState(int count) implements AggregateState {}

  /** A snapshot state the registry does not know. */
  public record UnregisteredState(int count) implements AggregateState {}

  /** Holds a second state class whose simple name is {@code CounterState}. */
  static final class Elsewhere {
    /** Shares its simple name with the outer {@code CounterState}. */
    public record CounterState(int count) implements AggregateState {}

    private Elsewhere() {}
  }

  private static final String CATEGORY = "test.store";
  private static final StreamId STREAM =
      StreamId.of(new AggregateType("Account"), new AggregateId("a-1"));
  private static final String SHOWN = "Account:a-1";
  private static final ObjectMapper MAPPER =
      StoreObjectMappers.eventsAndSagas(null, StreamRuneMetrics.NOOP);
  private static final EventTypeRegistry REGISTRY =
      SimpleEventTypeRegistry.builder()
          .registerEvent("Opened", Opened.class)
          .registerState("CounterState", CounterState.class)
          .build();
  private static final String OUTBOX_NOT_CONFIGURED = "no outbox store on this test store";
  private static final Instant AT = Instant.parse("2026-11-27T09:00:00Z");

  private final RecordingTracing tracing = new RecordingTracing();
  private Capture store;
  private Capture own;

  @BeforeEach
  void attach() {
    store = Capture.attach(CATEGORY);
    own = Capture.attach(EventAppendSupport.class.getName());
  }

  @AfterEach
  void detach() {
    store.close();
    own.close();
  }

  // ── Construction ───────────────────────────────────────────────────────────────────────────────

  @Test
  void constructor_requiresEveryArgument() {
    org.slf4j.Logger log = LoggerFactory.getLogger(CATEGORY);

    assertThat(assertThrows(NullPointerException.class, () -> support(null, REGISTRY, log)))
        .hasMessage("objectMapper is required");
    assertThat(assertThrows(NullPointerException.class, () -> support(MAPPER, null, log)))
        .hasMessage("typeRegistry is required");
    assertThat(
            assertThrows(
                NullPointerException.class,
                () -> new EventAppendSupport(MAPPER, REGISTRY, null, log)))
        .hasMessage("guardedTracing is required");
    assertThat(assertThrows(NullPointerException.class, () -> support(MAPPER, REGISTRY, null)))
        .hasMessage("log is required");
  }

  // ── Serialization ──────────────────────────────────────────────────────────────────────────────

  @Test
  void serializeRow_ordersPayloadMetadataGuardsThenSchemaVersion() throws Exception {
    List<String> order = new ArrayList<>();
    var mapper = new RecordingMapper(order, value -> false);
    JsonGuard guard = (json, what) -> order.add("guard " + what);
    EventAppendSupport support = support(mapper, REGISTRY, LoggerFactory.getLogger(CATEGORY));
    EventEnvelope envelope = envelope(STREAM, 1, metadata("e-1", SpanLink.NONE));

    EventRow row =
        support.serializeRow(
            envelope,
            7,
            () -> {
              order.add("schema version");
              return 3;
            },
            guard);

    assertThat(order)
        .containsExactly(
            "payload", "metadata", "guard event payload", "guard event metadata", "schema version");
    assertThat(row.version()).isEqualTo(7);
    assertThat(row.eventType()).isEqualTo("Opened");
    assertThat(row.payloadJson()).isEqualTo(MAPPER.writeValueAsString(envelope.event()));
    assertThat(row.metadataJson()).isEqualTo(MAPPER.writeValueAsString(envelope.metadata()));
    assertThat(row.schemaVersion()).isEqualTo(3);
  }

  @Test
  void serializeRow_aMetadataFailureWinsOverAGuardThatRefusesThePayload() {
    List<String> order = new ArrayList<>();
    var mapper = new RecordingMapper(order, value -> value instanceof EventMetadata);
    JsonGuard refusesPayload =
        (json, what) -> {
          throw new EventStoreException("refused " + what);
        };
    EventAppendSupport support = support(mapper, REGISTRY, LoggerFactory.getLogger(CATEGORY));

    assertThrows(
        JsonProcessingException.class,
        () ->
            support.serializeRow(
                envelope(STREAM, 1, metadata("e-1", SpanLink.NONE)),
                1,
                () -> {
                  order.add("schema version");
                  return 1;
                },
                refusesPayload));

    assertThat(order).containsExactly("payload", "metadata");
  }

  @Test
  void serializeRow_aRefusingGuardStopsBeforeTheSchemaVersion() {
    List<String> order = new ArrayList<>();
    EventAppendSupport support =
        support(new RecordingMapper(order, value -> false), REGISTRY, logger());
    JsonGuard refusesMetadata =
        (json, what) -> {
          order.add("guard " + what);
          if (what.equals("event metadata")) {
            throw new EventStoreException("refused " + what);
          }
        };

    var e =
        assertThrows(
            EventStoreException.class,
            () ->
                support.serializeRow(
                    envelope(STREAM, 1, metadata("e-1", SpanLink.NONE)),
                    1,
                    () -> {
                      order.add("schema version");
                      return 1;
                    },
                    refusesMetadata));

    assertThat(e).hasMessage("refused event metadata");
    assertThat(order)
        .containsExactly("payload", "metadata", "guard event payload", "guard event metadata");
  }

  @Test
  void jsonGuardNone_acceptsEveryText() throws Exception {
    EventRow row =
        support()
            .serializeRow(
                envelope(STREAM, 1, metadata("e-1", SpanLink.NONE)), 1, () -> 1, JsonGuard.NONE);

    assertThat(row.payloadJson()).isEqualTo("{\"name\":\"n\"}");
    JsonGuard.NONE.check("\\u0000", "anything");
  }

  @Test
  void serialize_tracesOnlyWhenTraced() throws Exception {
    EventAppendSupport support = support();
    List<EventRow> rows = List.of(new EventRow(1, "Opened", "{}", "{}", 1));

    assertSame(rows, support.serialize(2, false, () -> rows));
    assertThat(tracing.names()).isEmpty();

    assertSame(rows, support.serialize(2, true, () -> rows));
    var span = tracing.only(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    assertThat(span.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(span.attributes()).containsOnlyKeys(TraceAttributes.EVENT_COUNT);
    assertThat(span.attribute(TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(span.ok).isTrue();
    assertThat(span.error).isNull();
    assertThat(span.closed).isTrue();
  }

  @Test
  void serialize_tracedFailure_isTheSpansErrorAndPropagates() {
    EventAppendSupport support = support();
    var failure = new JsonMappingException((Closeable) null, "cannot serialize");

    var thrown =
        assertThrows(
            JsonProcessingException.class,
            () ->
                support.serialize(
                    1,
                    true,
                    () -> {
                      throw failure;
                    }));

    assertSame(failure, thrown);
    var span = tracing.only(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    assertSame(failure, span.error);
    assertThat(span.ok).isFalse();
    assertThat(span.closed).isTrue();
  }

  // ── The append span ────────────────────────────────────────────────────────────────────────────

  @Test
  void tracedAppend_outcomes() {
    EventAppendSupport support = support();

    String result =
        support.tracedAppend(STREAM, 2, new Version(3), 1, () -> "done", r -> "APPENDED");

    assertThat(result).isEqualTo("done");
    var appended = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(appended.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(appended.attribute(TraceAttributes.STREAM_ID)).isEqualTo(SHOWN);
    assertThat(appended.attribute(TraceAttributes.AGGREGATE_TYPE)).isEqualTo("Account");
    assertThat(appended.attribute(TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(appended.attribute(TraceAttributes.EXPECTED_VERSION)).isEqualTo(3L);
    assertThat(appended.attribute(TraceAttributes.OUTBOX_ENTRY_COUNT)).isEqualTo(1L);
    assertThat(appended.attribute(TraceAttributes.APPEND_OUTCOME)).isEqualTo("APPENDED");
    assertThat(appended.ok).isTrue();
    assertThat(appended.closed).isTrue();

    tracing.clear();
    String duplicate =
        support.tracedAppend(
            STREAM, 1, new Version(0), 0, () -> "again", r -> r.equals("again") ? "DUPLICATE" : "");
    assertThat(duplicate).isEqualTo("again");
    assertThat(
            tracing
                .only(TraceAttributes.EVENT_STORE_APPEND)
                .attribute(TraceAttributes.APPEND_OUTCOME))
        .isEqualTo("DUPLICATE");

    tracing.clear();
    var conflict = new OptimisticLockException("conflict");
    assertSame(
        conflict,
        assertThrows(
            OptimisticLockException.class,
            () ->
                support.tracedAppend(
                    STREAM, 1, new Version(0), 0, () -> thrower(conflict), r -> "")));
    var conflicted = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(conflicted.attribute(TraceAttributes.APPEND_OUTCOME)).isEqualTo("CONFLICT");
    assertThat(conflicted.error).isNull();
    assertThat(conflicted.ok).isFalse();
    assertThat(conflicted.closed).isTrue();

    tracing.clear();
    var fenced =
        new SagaForwardFencedException(new SagaOrigin(SagaType.of("Order"), SagaId.of("s-1"), 0));
    assertSame(
        fenced,
        assertThrows(
            SagaForwardFencedException.class,
            () ->
                support.tracedAppend(
                    STREAM, 1, new Version(0), 0, () -> thrower(fenced), r -> "")));
    var refused = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(refused.attribute(TraceAttributes.APPEND_OUTCOME)).isEqualTo("FENCED");
    assertThat(refused.error).isNull();
    assertThat(refused.closed).isTrue();

    tracing.clear();
    var broken = new EventStoreException("broken");
    assertSame(
        broken,
        assertThrows(
            EventStoreException.class,
            () ->
                support.tracedAppend(
                    STREAM, 1, new Version(0), 0, () -> thrower(broken), r -> "")));
    var failed = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(failed.attribute(TraceAttributes.APPEND_OUTCOME)).isNull();
    assertSame(broken, failed.error);
    assertThat(failed.ok).isFalse();
    assertThat(failed.closed).isTrue();
  }

  @Test
  void tracedAppend_sanitizesTheStreamForTheSpan() {
    StreamId forged = StreamId.of(new AggregateType("Account"), new AggregateId("a-1\nforged"));

    support().tracedAppend(forged, 1, new Version(0), 0, () -> "ok", r -> "APPENDED");

    assertThat(
            (String)
                tracing
                    .only(TraceAttributes.EVENT_STORE_APPEND)
                    .attribute(TraceAttributes.STREAM_ID))
        .doesNotContain("\n")
        .startsWith("Account:a-1");
  }

  // ── Argument rules ─────────────────────────────────────────────────────────────────────────────

  @Test
  void preamble_nullChecksInOrder() {
    List<EventEnvelope> events = List.of();
    List<OutboxEntry> entries = List.of();
    Version v = new Version(0);

    assertThat(npe(() -> preamble(null, null, null, null, false)))
        .hasMessage("streamId is required");
    assertThat(npe(() -> preamble(STREAM, null, null, null, false)))
        .hasMessage("events is required");
    assertThat(npe(() -> preamble(STREAM, events, null, null, false)))
        .hasMessage("expectedVersion is required");
    assertThat(npe(() -> preamble(STREAM, events, v, null, false)))
        .hasMessage("outboxEntries is required");
    assertThat(preamble(STREAM, events, v, entries, false)).isPresent();
  }

  @Test
  void preamble_outboxWithoutStore_usesTheGivenMessage() {
    List<OutboxEntry> entries = List.of(entry("o-1", SpanLink.NONE));
    List<EventEnvelope> events = List.of(envelope(STREAM, 1, metadata("e-1", SpanLink.NONE)));

    var withEvents =
        assertThrows(
            IllegalStateException.class,
            () -> preamble(STREAM, events, new Version(0), entries, false));
    // The store check comes before the empty-events rule.
    var withoutEvents =
        assertThrows(
            IllegalStateException.class,
            () -> preamble(STREAM, List.of(), new Version(0), entries, false));

    assertThat(withEvents).hasMessage(OUTBOX_NOT_CONFIGURED);
    assertThat(withoutEvents).hasMessage(OUTBOX_NOT_CONFIGURED);
  }

  @Test
  void preamble_entriesWithoutEvents_isIllegalArgument() {
    List<OutboxEntry> entries = List.of(entry("o-1", SpanLink.NONE));

    var e =
        assertThrows(
            IllegalArgumentException.class,
            () -> preamble(STREAM, List.of(), new Version(0), entries, true));

    assertThat(e)
        .hasMessage(
            "outboxEntries were passed with an empty events list — outbox entries are persisted"
                + " atomically with the events of this append and cannot be appended alone. To"
                + " emit an outbox entry that has no accompanying domain event, persist it"
                + " directly with OutboxStore.save(entry), one entry per call — that write commits"
                + " on its own connection and is atomic with nothing else.");
  }

  @Test
  void preamble_emptyEvents_returnsTheEmptyResult() {
    Optional<AppendResult> empty = preamble(STREAM, List.of(), new Version(4), List.of(), true);

    assertThat(empty).contains(new AppendResult(List.of(), new Version(4)));
  }

  @Test
  void preamble_eventsToAppend_goOn() {
    List<EventEnvelope> events = List.of(envelope(STREAM, 1, metadata("e-1", SpanLink.NONE)));

    assertThat(preamble(STREAM, events, new Version(0), List.of(), false)).isEmpty();
    assertThat(preamble(STREAM, events, new Version(0), List.of(entry("o-1", SpanLink.NONE)), true))
        .isEmpty();
  }

  @Test
  void requireKeyedArguments_checksInOrder() {
    List<EventEnvelope> events = List.of();
    Version v = new Version(0);
    IdempotencyKey key = IdempotencyKey.of("k-1");

    assertThat(npe(() -> EventAppendSupport.requireKeyedArguments(null, null, null, null, null)))
        .hasMessage("streamId is required");
    assertThat(npe(() -> EventAppendSupport.requireKeyedArguments(STREAM, null, null, null, null)))
        .hasMessage("events is required");
    assertThat(
            npe(() -> EventAppendSupport.requireKeyedArguments(STREAM, events, null, null, null)))
        .hasMessage("expectedVersion is required");
    assertThat(npe(() -> EventAppendSupport.requireKeyedArguments(STREAM, events, v, null, null)))
        .hasMessage("idempotencyKey is required");
    assertThat(npe(() -> EventAppendSupport.requireKeyedArguments(STREAM, events, v, key, null)))
        .hasMessage("commandType is required");
    assertThrows(
        IllegalArgumentException.class,
        () -> EventAppendSupport.requireKeyedArguments(STREAM, events, v, key, "c".repeat(256)));
    EventAppendSupport.requireKeyedArguments(STREAM, events, v, key, "Cmd");
  }

  @Test
  void requireSnapshotArguments_messages() {
    var state = new CounterState(1);

    assertThat(npe(() -> EventAppendSupport.requireSnapshotArguments(null, null, 0, -1)))
        .hasMessage("streamId is required");
    assertThat(iae(() -> EventAppendSupport.requireSnapshotArguments(STREAM, null, 0, -1)))
        .hasMessage("state must not be null");
    assertThat(iae(() -> EventAppendSupport.requireSnapshotArguments(STREAM, state, 0, -1)))
        .hasMessage("snapshotVersion must be >= 1");
    assertThat(iae(() -> EventAppendSupport.requireSnapshotArguments(STREAM, state, 1, -1)))
        .hasMessage("snapshotPurgeGeneration must not be negative, got -1");
    EventAppendSupport.requireSnapshotArguments(STREAM, state, 1, 0);
  }

  @Test
  void validateCryptoConfig_requiresARegistry() {
    assertThat(iae(() -> EventAppendSupport.validateCryptoConfig(null, null)))
        .hasMessage("typeRegistry is required");
    // No registered type carries @Encrypted: accepted without an engine.
    EventAppendSupport.validateCryptoConfig(REGISTRY, null);
  }

  // ── Outbox entries and audit entries ───────────────────────────────────────────────────────────

  @Test
  void outboxEntriesOf_linksEachEntryToItsEvent() {
    SpanLink first = RecordingTracing.randomLink();
    SpanLink second = RecordingTracing.randomLink();
    var e1 = envelope(STREAM, 1, metadata("e-1", first));
    var e2 = envelope(STREAM, 2, metadata("e-2", second));
    SpanLink mapperLink = RecordingTracing.randomLink();
    List<EventEnvelope> mapped = new ArrayList<>();

    List<OutboxEntry> entries =
        EventAppendSupport.outboxEntriesOf(
            List.of(e1, e2),
            true,
            event -> {
              mapped.add(event);
              String id = event.metadata().eventId().value();
              return List.of(entry(id + "-a", mapperLink), entry(id + "-b", SpanLink.NONE));
            });

    assertThat(mapped).containsExactly(e1, e2);
    assertThat(entries)
        .extracting(e -> e.id().value())
        .containsExactly("e-1-a", "e-1-b", "e-2-a", "e-2-b");
    assertThat(entries)
        .extracting(OutboxEntry::traceLink)
        .containsExactly(first, first, second, second);
  }

  @Test
  void outboxEntriesOf_withoutAStoreOrAMapper_isEmptyAndNeverMaps() {
    List<EventEnvelope> events = List.of(envelope(STREAM, 1, metadata("e-1", SpanLink.NONE)));

    assertThat(
            EventAppendSupport.outboxEntriesOf(
                events,
                false,
                event -> {
                  throw new AssertionError("mapped without an outbox store");
                }))
        .isEmpty();
    assertThat(EventAppendSupport.outboxEntriesOf(events, true, null)).isEmpty();
  }

  @Test
  void withTraceLink_returnsTheSameListWhenNothingChanges() {
    List<OutboxEntry> unlinked = List.of(entry("o-1", SpanLink.NONE), entry("o-2", SpanLink.NONE));
    SpanLink link = RecordingTracing.randomLink();
    List<OutboxEntry> mixed = List.of(entry("o-1", link), entry("o-2", SpanLink.NONE));

    assertSame(unlinked, EventAppendSupport.withTraceLink(unlinked, SpanLink.NONE));
    List<OutboxEntry> linked = EventAppendSupport.withTraceLink(mixed, link);

    assertThat(linked).isNotSameAs(mixed);
    assertSame(mixed.get(0), linked.get(0));
    assertThat(linked).extracting(OutboxEntry::traceLink).containsExactly(link, link);
    assertThat(mixed.get(1).traceLink()).isEqualTo(SpanLink.NONE);
  }

  @Test
  void traceLinkOf_isTheEventsSpanContext() {
    SpanLink link = RecordingTracing.randomLink();

    assertThat(EventAppendSupport.traceLinkOf(null)).isEqualTo(SpanLink.NONE);
    assertThat(EventAppendSupport.traceLinkOf(envelope(STREAM, 1, metadata("e-1", SpanLink.NONE))))
        .isEqualTo(SpanLink.NONE);
    assertThat(EventAppendSupport.traceLinkOf(envelope(STREAM, 1, metadata("e-1", link))))
        .isEqualTo(link);
  }

  @Test
  void auditEntries_numberVersionsFromExpected() {
    StreamId elsewhere = StreamId.of(new AggregateType("Other"), new AggregateId("o-9"));
    var full =
        new EventMetadata(
            EventId.of("e-1"),
            CommandId.of("c-1"),
            null,
            CorrelationId.of("k-1"),
            CausationId.of("x-1"),
            UserId.of("u-1"),
            AT);
    // The envelopes name another stream and version: the audit rows take the append's.
    var e1 = envelope(elsewhere, 40, full);
    var e2 = envelope(elsewhere, 41, metadata("e-2", SpanLink.NONE));

    List<EventAuditEntry> entries =
        EventAppendSupport.auditEntries(STREAM, List.of(e1, e2), new Version(5));

    assertThat(entries)
        .containsExactly(
            new EventAuditEntry(
                EventId.of("e-1"),
                EventType.of("Opened"),
                STREAM,
                new Version(6),
                CommandId.of("c-1"),
                CorrelationId.of("k-1"),
                CausationId.of("x-1"),
                UserId.of("u-1"),
                AT),
            new EventAuditEntry(
                EventId.of("e-2"),
                EventType.of("Opened"),
                STREAM,
                new Version(7),
                CommandId.of("c-1"),
                CorrelationId.of("k-1"),
                null,
                null,
                AT));
    assertThat(EventAppendSupport.auditEntries(STREAM, List.of(), new Version(5))).isEmpty();
  }

  // ── Head check, lost claims and failures ───────────────────────────────────────────────────────

  @Test
  void requireExpectedHead_message() {
    EventAppendSupport.requireExpectedHead(STREAM, new Version(3), 3);

    var below =
        assertThrows(
            OptimisticLockException.class,
            () -> EventAppendSupport.requireExpectedHead(STREAM, new Version(3), 5));
    var above =
        assertThrows(
            OptimisticLockException.class,
            () -> EventAppendSupport.requireExpectedHead(STREAM, new Version(3), 1));
    StreamId forged = StreamId.of(new AggregateType("Account"), new AggregateId("a-1\nforged"));
    var sanitized =
        assertThrows(
            OptimisticLockException.class,
            () -> EventAppendSupport.requireExpectedHead(forged, new Version(0), 1));

    assertThat(below)
        .hasMessage(
            "Version conflict on stream 'Account:a-1': expected version 3 but actual head is 5")
        .hasNoCause();
    assertThat(above)
        .hasMessage(
            "Version conflict on stream 'Account:a-1': expected version 3 but actual head is 1");
    assertThat(sanitized.getMessage()).doesNotContain("\n");
  }

  @Test
  void inboxConflictWithoutRow_sanitizesTheKey() {
    assertThat(EventAppendSupport.inboxConflictWithoutRow(IdempotencyKey.of("k-1")))
        .hasMessage("Inbox conflict but no row found for key: k-1")
        .hasNoCause();
    assertThat(
            EventAppendSupport.inboxConflictWithoutRow(new IdempotencyKey("k-1\nforged"))
                .getMessage())
        .doesNotContain("\n")
        .startsWith("Inbox conflict but no row found for key: k-1");
  }

  @Test
  void replayLostClaim_returnsTheStoredResult_andRefusesAMissingRow() {
    var inbox = new InMemoryCommandInbox();
    IdempotencyKey key = IdempotencyKey.of("k-1");
    List<GlobalOffset> offsets = List.of(GlobalOffset.of(7), GlobalOffset.of(8));
    inbox.record(new CommandInbox.InboxResult(key, "Cmd", STREAM, new Version(4), offsets, AT));

    IdempotentAppendResult replayed = EventAppendSupport.replayLostClaim(inbox, key, "Cmd", STREAM);

    assertThat(replayed).isEqualTo(new IdempotentAppendResult(offsets, new Version(4), true));

    var missing =
        assertThrows(
            EventStoreException.class,
            () ->
                EventAppendSupport.replayLostClaim(inbox, IdempotencyKey.of("k-2"), "Cmd", STREAM));
    assertThat(missing).hasMessage("Inbox conflict but no row found for key: k-2");

    StreamId other = StreamId.of(new AggregateType("Account"), new AggregateId("a-2"));
    assertThrows(
        IllegalArgumentException.class,
        () -> EventAppendSupport.replayLostClaim(inbox, key, "OtherCmd", STREAM));
    assertThrows(
        IllegalArgumentException.class,
        () -> EventAppendSupport.replayLostClaim(inbox, key, "Cmd", other));
  }

  @Test
  void appendConflict_mapping() {
    var e = new SQLException("driver failure");

    var collision =
        EventAppendSupport.appendConflict(
            e, new FakeClassifier(true, true, false), SHOWN, new Version(3));
    var retryable =
        EventAppendSupport.appendConflict(
            e, new FakeClassifier(false, true, true), SHOWN, new Version(3));
    var lockClassifier = new FakeClassifier(false, false, true);
    var locked = EventAppendSupport.appendConflict(e, lockClassifier, SHOWN, new Version(3));
    var none =
        EventAppendSupport.appendConflict(
            e, new FakeClassifier(false, false, false), SHOWN, new Version(3));

    assertThat(collision)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("desync of " + SHOWN)
        .hasCause(e);
    assertThat(retryable)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Version conflict on stream 'Account:a-1' at expected version 3")
        .hasCause(e);
    assertSame(lockClassifier.lastLockFailure.get(), locked);
    assertThat(lockClassifier.lastWhat.get()).isEqualTo("append to stream 'Account:a-1'");
    assertThat(locked).hasCause(e);
    assertNull(none);
  }

  @Test
  void keyedAppendFailure_mapping() {
    var e = new SQLException("driver failure");

    var collision =
        EventAppendSupport.keyedAppendFailure(e, new FakeClassifier(true, true, true), SHOWN);
    var retryable =
        EventAppendSupport.keyedAppendFailure(e, new FakeClassifier(false, true, true), SHOWN);
    var lockClassifier = new FakeClassifier(false, false, true);
    var locked = EventAppendSupport.keyedAppendFailure(e, lockClassifier, SHOWN);
    var other =
        EventAppendSupport.keyedAppendFailure(e, new FakeClassifier(false, false, false), SHOWN);

    assertThat(collision)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("desync of " + SHOWN)
        .hasCause(e);
    assertThat(retryable)
        .isExactlyInstanceOf(OptimisticLockException.class)
        .hasMessage("Concurrent modification of stream: Account:a-1")
        .hasCause(e);
    assertSame(lockClassifier.lastLockFailure.get(), locked);
    assertThat(lockClassifier.lastWhat.get())
        .isEqualTo("idempotent append to stream 'Account:a-1'");
    assertThat(other)
        .isExactlyInstanceOf(EventStoreException.class)
        .hasMessage("Failed idempotent append to stream: Account:a-1")
        .hasCause(e);
  }

  // ── Snapshots ──────────────────────────────────────────────────────────────────────────────────

  @Test
  void serializeSnapshot_validationAndGuardOrder() throws Exception {
    List<String> guarded = new ArrayList<>();
    JsonGuard guard = (json, what) -> guarded.add(what + " " + json);
    EventAppendSupport support = support();

    SnapshotPayload payload = support.serializeSnapshot(new CounterState(5), SHOWN, guard);

    assertThat(payload).isEqualTo(new SnapshotPayload("CounterState", "{\"count\":5}"));
    assertThat(guarded).containsExactly("snapshot state {\"count\":5}");

    guarded.clear();
    var unresolved =
        assertThrows(
            EventStoreException.class,
            () -> support.serializeSnapshot(new UnregisteredState(1), SHOWN, guard));
    assertThat(unresolved)
        .hasMessage(
            "Snapshot state type 'UnregisteredState' does not resolve in the EventTypeRegistry —"
                + " register "
                + UnregisteredState.class.getName()
                + " under its simple name so the snapshot can be rehydrated on load")
        .hasCauseInstanceOf(UnknownEventTypeException.class);

    var otherClass =
        assertThrows(
            EventStoreException.class,
            () -> support.serializeSnapshot(new Elsewhere.CounterState(1), SHOWN, guard));
    assertThat(otherClass)
        .hasMessage(
            "Snapshot state type 'CounterState' resolves to "
                + CounterState.class.getName()
                + " in the EventTypeRegistry, but the state being saved is "
                + Elsewhere.CounterState.class.getName()
                + " — snapshots are stored by simple class name, so two state classes sharing a"
                + " simple name would rehydrate as the wrong type")
        .hasNoCause();
    assertThat(guarded).isEmpty();

    var failing =
        support(
            new RecordingMapper(new ArrayList<>(), value -> value instanceof CounterState),
            REGISTRY,
            logger());
    var unserializable =
        assertThrows(
            EventStoreException.class,
            () -> failing.serializeSnapshot(new CounterState(1), SHOWN, guard));
    assertThat(unserializable)
        .hasMessage("Failed to serialize snapshot for stream: Account:a-1")
        .hasCauseInstanceOf(JsonProcessingException.class);
    assertThat(guarded).isEmpty();

    var refused =
        assertThrows(
            EventStoreException.class,
            () ->
                support.serializeSnapshot(
                    new CounterState(1),
                    SHOWN,
                    (json, what) -> {
                      throw new EventStoreException("refused " + what);
                    }));
    assertThat(refused).hasMessage("refused snapshot state");
  }

  @Test
  void snapshotNotStored_logsInfoOnTheGivenLogger() {
    boolean stored = support().snapshotNotStored(SHOWN, new Version(4), 2);

    assertThat(stored).isFalse();
    List<ILoggingEvent> info = store.events(Level.INFO);
    assertThat(info).hasSize(1);
    assertThat(info.getFirst().getFormattedMessage())
        .isEqualTo(
            "Snapshot of stream Account:a-1 at version 4 not stored: its snapshot_store row carries"
                + " a purge generation other than 2, so a GDPR forget purged the stream's"
                + " snapshot after the state being saved was loaded; the next command loads and"
                + " snapshots again");
    assertThat(store.all()).hasSize(1);
    assertThat(own.all()).isEmpty();
  }

  @Test
  void tracedSaveSnapshot_outcomes() {
    EventAppendSupport support = support();

    assertThat(support.tracedSaveSnapshot(STREAM, new Version(4), () -> true)).isTrue();
    var saved = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE);
    assertThat(saved.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(saved.attribute(TraceAttributes.STREAM_ID)).isEqualTo(SHOWN);
    assertThat(saved.attribute(TraceAttributes.STREAM_VERSION)).isEqualTo(4L);
    assertThat(saved.attribute(TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("SAVED");
    assertThat(saved.ok).isTrue();
    assertThat(saved.closed).isTrue();

    tracing.clear();
    assertThat(support.tracedSaveSnapshot(STREAM, new Version(4), () -> false)).isFalse();
    var skipped = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE);
    assertThat(skipped.attribute(TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("SKIPPED");
    assertThat(skipped.ok).isTrue();

    tracing.clear();
    var broken = new EventStoreException("broken");
    assertSame(
        broken,
        assertThrows(
            EventStoreException.class,
            () ->
                support.tracedSaveSnapshot(
                    STREAM,
                    new Version(4),
                    () -> {
                      throw broken;
                    })));
    var failed = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE);
    assertThat(failed.attribute(TraceAttributes.SNAPSHOT_OUTCOME)).isNull();
    assertSame(broken, failed.error);
    assertThat(failed.ok).isFalse();
    assertThat(failed.closed).isTrue();
  }

  @Test
  void logsOnlyThroughTheGivenLogger() {
    EventAppendSupport support = support();

    support.snapshotNotStored(SHOWN, new Version(1), 0);
    support.tracedAppend(STREAM, 1, new Version(0), 0, () -> "ok", r -> "APPENDED");
    support.tracedSaveSnapshot(STREAM, new Version(1), () -> false);

    assertThat(store.all()).hasSize(1);
    assertThat(own.all()).isEmpty();
  }

  // ── Fixtures ───────────────────────────────────────────────────────────────────────────────────

  private EventAppendSupport support() {
    return support(MAPPER, REGISTRY, logger());
  }

  private EventAppendSupport support(
      ObjectMapper mapper, EventTypeRegistry registry, org.slf4j.Logger log) {
    return new EventAppendSupport(mapper, registry, tracing, log);
  }

  private static org.slf4j.Logger logger() {
    return LoggerFactory.getLogger(CATEGORY);
  }

  private static Optional<AppendResult> preamble(
      StreamId streamId,
      List<EventEnvelope> events,
      Version expectedVersion,
      List<OutboxEntry> outboxEntries,
      boolean outboxStoreConfigured) {
    return EventAppendSupport.preamble(
        streamId,
        events,
        expectedVersion,
        outboxEntries,
        outboxStoreConfigured,
        OUTBOX_NOT_CONFIGURED);
  }

  private static <T> T thrower(RuntimeException e) {
    throw e;
  }

  private static NullPointerException npe(Runnable call) {
    return assertThrows(NullPointerException.class, call::run);
  }

  private static IllegalArgumentException iae(Runnable call) {
    return assertThrows(IllegalArgumentException.class, call::run);
  }

  private static EventMetadata metadata(String eventId, SpanLink spanContext) {
    return new EventMetadata(
        EventId.of(eventId),
        CommandId.of("c-1"),
        null,
        CorrelationId.of("k-1"),
        null,
        null,
        AT,
        null,
        null,
        spanContext);
  }

  private static EventEnvelope envelope(StreamId streamId, long version, EventMetadata metadata) {
    return new EventEnvelope(
        GlobalOffset.of(version),
        streamId,
        new Version(version),
        EventType.of("Opened"),
        new Opened("n"),
        metadata);
  }

  private static OutboxEntry entry(String id, SpanLink link) {
    return OutboxEntry.pending(OutboxEntryId.of(id), "{}", "Integration").withTraceLink(link);
  }

  /** A classifier with fixed answers that records what a lock failure was asked for. */
  private static final class FakeClassifier implements AppendFailureClassifier {
    private final boolean collision;
    private final boolean retryable;
    private final boolean lock;
    final AtomicReference<String> lastWhat = new AtomicReference<>();
    final AtomicReference<EventStoreException> lastLockFailure = new AtomicReference<>();

    FakeClassifier(boolean collision, boolean retryable, boolean lock) {
      this.collision = collision;
      this.retryable = retryable;
      this.lock = lock;
    }

    @Override
    public boolean isGlobalOffsetCollision(SQLException e) {
      return collision;
    }

    @Override
    public boolean isRetryableConflict(SQLException e) {
      return retryable;
    }

    @Override
    public EventStoreException lockFailure(SQLException e, String what) {
      lastWhat.set(what);
      if (!lock) {
        return null;
      }
      var failure = new EventStoreException("lock refused " + what, e);
      lastLockFailure.set(failure);
      return failure;
    }

    @Override
    public String globalOffsetDesyncMessage(String shownStream) {
      return "desync of " + shownStream;
    }
  }

  /**
   * A mapper that records what it serializes, {@code metadata} for event metadata and {@code
   * payload} for anything else, and fails on the values {@code failOn} accepts.
   */
  private static final class RecordingMapper extends ObjectMapper {
    private final transient List<String> order;
    private final transient Predicate<Object> failOn;

    RecordingMapper(List<String> order, Predicate<Object> failOn) {
      super(MAPPER);
      this.order = order;
      this.failOn = failOn;
    }

    @Override
    public String writeValueAsString(Object value) throws JsonProcessingException {
      order.add(value instanceof EventMetadata ? "metadata" : "payload");
      if (failOn.test(value)) {
        throw new JsonMappingException((Closeable) null, "cannot serialize " + value);
      }
      return super.writeValueAsString(value);
    }
  }

  private static final class Capture implements AutoCloseable {
    private final Logger logger;
    private final ListAppender<ILoggingEvent> appender = new ListAppender<>();
    private final Level previousLevel;
    private final boolean previousAdditivity;

    private Capture(Logger logger) {
      this.logger = logger;
      this.previousLevel = logger.getLevel();
      this.previousAdditivity = logger.isAdditive();
      appender.start();
      logger.addAppender(appender);
      logger.setLevel(Level.TRACE);
      logger.setAdditive(false);
    }

    static Capture attach(String name) {
      return new Capture((Logger) LoggerFactory.getLogger(name));
    }

    List<ILoggingEvent> events(Level level) {
      return all().stream().filter(e -> e.getLevel() == level).toList();
    }

    List<ILoggingEvent> all() {
      synchronized (appender) {
        return List.copyOf(appender.list);
      }
    }

    @Override
    public void close() {
      logger.setLevel(previousLevel);
      logger.setAdditive(previousAdditivity);
      logger.detachAppender(appender);
      appender.stop();
    }
  }
}
