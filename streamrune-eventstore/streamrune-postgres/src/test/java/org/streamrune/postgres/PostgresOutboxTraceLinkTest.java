package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxEventMapper;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.OutboxPoller;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * The link from an outbox entry to the trace of the command that wrote it, against PostgreSQL:
 * {@link PostgresEventStore} stores the {@code traceId} and {@code spanId} of the source event's
 * metadata in {@code outbox_events.trace_id} and {@code span_id} in the append transaction, {@link
 * PostgresOutboxStore} reads them back as {@link OutboxEntry#traceLink()}, and the relay's {@code
 * outbox/publish} span links to them. Ids that are not W3C ids are never stored, and a malformed
 * stored value gives no link while the entry is still delivered. The spans are recorded through
 * {@link RecordingTracing}: this module has no OpenTelemetry on its test classpath.
 */
@Testcontainers
class PostgresOutboxTraceLinkTest {

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_trace_link");

  static PGSimpleDataSource dataSource;

  record Switched(String value) implements DomainEvent {}

  static final EventTypeRegistry REGISTRY =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          return Switched.class;
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          return Object.class;
        }

        @Override
        public Collection<Class<?>> registeredTypes() {
          return List.of(Switched.class);
        }
      };

  /** The ids of the command span, as the events of its append carry them in their metadata. */
  static final String TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";

  static final String SPAN = "00f067aa0ba902b7";
  static final SpanLink COMMAND = new SpanLink(TraceId.of(TRACE), SpanId.of(SPAN));

  /** A link of another span, with valid W3C ids. */
  static final SpanLink OWN =
      new SpanLink(TraceId.of("0af7651916cd43dd8448eb211c80319c"), SpanId.of("b7ad6b7169203331"));

  /** One entry per event, its id the event's, built without a link as a mapper builds it. */
  static final OutboxEventMapper MAPPER =
      envelope ->
          List.of(
              OutboxEntry.pending(
                  OutboxEntryId.of(envelope.metadata().eventId().value()),
                  "{\"k\":1}",
                  "Switched",
                  envelope.streamId()));

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    new PostgresEventStoreFactory(dataSource, REGISTRY).create();
  }

  private PostgresOutboxStore outboxStore;

  @BeforeEach
  void clean() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM outbox_events");
      stmt.execute("DELETE FROM event_stream");
      stmt.execute("DELETE FROM command_inbox");
    }
    outboxStore = new PostgresOutboxStore(dataSource, Duration.ofMinutes(5));
  }

  private PostgresEventStore store(OutboxEventMapper mapper) {
    return PostgresEventStore.builder()
        .dataSource(dataSource)
        .typeRegistry(REGISTRY)
        .outboxStore(outboxStore)
        .outboxEventMapper(mapper)
        .commandInbox(new PostgresCommandInbox(dataSource))
        .build();
  }

  private static StreamId stream() {
    return TestStreams.stream("lamp-" + System.nanoTime());
  }

  private static EventEnvelope event(StreamId stream, int version, String traceId, String spanId) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        new Version(version),
        new EventType("Switched"),
        new Switched("on"),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            traceId == null ? null : TraceId.of(traceId),
            spanId == null ? null : SpanId.of(spanId),
            CorrelationId.of("trace-link"),
            null,
            null,
            Instant.now()));
  }

  private static String idOf(EventEnvelope event) {
    return event.metadata().eventId().value();
  }

  /** The stored {@code trace_id} and {@code span_id} of {@code entryId}. */
  private static List<String> storedIds(String entryId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT trace_id, span_id FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, entryId);
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).as("row %s", entryId).isTrue();
        List<String> ids = new ArrayList<>();
        ids.add(rs.getString(1));
        ids.add(rs.getString(2));
        return ids;
      }
    }
  }

  private SpanLink linkOf(String entryId) {
    return outboxStore.findById(OutboxEntryId.of(entryId)).orElseThrow().traceLink();
  }

  /**
   * Runs a relay over the outbox until every entry in {@code ids} is delivered, and returns the
   * {@code outbox/publish} spans it recorded.
   */
  private List<RecordingTracing.RecordedSpan> relay(List<String> ids) {
    var tracing = new RecordingTracing();
    List<OutboxEntry> published = new CopyOnWriteArrayList<>();
    var poller =
        OutboxPoller.builder()
            .outboxStore(outboxStore)
            .publisher(published::add)
            .batchSize(10)
            .pollInterval(Duration.ofMillis(50))
            .tracing(tracing)
            .build();
    poller.start();
    try (poller) {
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(
              () -> {
                for (String id : ids) {
                  assertThat(outboxStore.findById(OutboxEntryId.of(id)).orElseThrow().status())
                      .isEqualTo(OutboxStatus.DELIVERED);
                }
              });
    }
    assertThat(published).extracting(e -> e.id().value()).containsExactlyElementsOf(ids);
    return tracing.spans.stream()
        .filter(s -> s.name.equals(TraceAttributes.OUTBOX_PUBLISH))
        .toList();
  }

  @Test
  void aFreshDatabase_hasTheTwoNullableColumns() throws Exception {
    Map<String, Integer> lengths = new HashMap<>();
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT column_name, is_nullable, character_maximum_length"
                    + " FROM information_schema.columns WHERE table_name = 'outbox_events'"
                    + " AND column_name IN ('trace_id', 'span_id')");
        var rs = ps.executeQuery()) {
      while (rs.next()) {
        assertThat(rs.getString("is_nullable")).isEqualTo("YES");
        lengths.put(rs.getString("column_name"), rs.getInt("character_maximum_length"));
      }
    }
    assertThat(lengths).isEqualTo(Map.of("trace_id", 32, "span_id", 16));
  }

  @Test
  void anAppendUnderATracedCommand_storesItsSpan_andTheRelaysPublishSpanLinksToIt()
      throws Exception {
    var stream = stream();
    var first = event(stream, 1, TRACE, SPAN);
    var second = event(stream, 2, TRACE, SPAN);

    store(MAPPER).append(stream, List.of(first, second), Version.initial());

    assertThat(storedIds(idOf(first))).containsExactly(TRACE, SPAN);
    assertThat(storedIds(idOf(second))).containsExactly(TRACE, SPAN);
    assertThat(linkOf(idOf(first))).isEqualTo(COMMAND);

    var publishes = relay(List.of(idOf(first), idOf(second)));
    assertThat(publishes).hasSize(2).allSatisfy(s -> assertThat(s.link).isEqualTo(COMMAND));
  }

  @Test
  void aKeyedAppend_storesTheSpanToo() throws Exception {
    var stream = stream();
    var event = event(stream, 1, TRACE, SPAN);

    store(MAPPER)
        .appendWithKey(
            stream, List.of(event), Version.initial(), IdempotencyKey.of("k-link"), "Switch");

    assertThat(storedIds(idOf(event))).containsExactly(TRACE, SPAN);
  }

  @Test
  void aLinkTheMapperSets_isKept() throws Exception {
    var stream = stream();
    var event = event(stream, 1, TRACE, SPAN);

    store(envelope -> MAPPER.toOutbox(envelope).stream().map(e -> e.withTraceLink(OWN)).toList())
        .append(stream, List.of(event), Version.initial());

    assertThat(linkOf(idOf(event))).isEqualTo(OWN);
  }

  @Test
  void entriesPassedToTheAppend_getTheFirstEventsSpan_unlessTheyCarryALink() throws Exception {
    var stream = stream();
    var unlinked = OutboxEntry.pending(OutboxEntryId.of("explicit-1"), "{}", "Switched", stream);
    var linked =
        OutboxEntry.pending(OutboxEntryId.of("explicit-2"), "{}", "Switched", stream)
            .withTraceLink(OWN);

    store(envelope -> List.of())
        .append(
            stream,
            List.of(event(stream, 1, TRACE, SPAN), event(stream, 2, null, null)),
            Version.initial(),
            List.of(unlinked, linked));

    assertThat(storedIds("explicit-1")).containsExactly(TRACE, SPAN);
    assertThat(linkOf("explicit-2")).isEqualTo(OWN);
  }

  @Test
  void metadataWithoutBothIds_storesNoLink_andThePublishSpanHasNone() throws Exception {
    var stream = stream();
    var none = event(stream, 1, null, null);
    var traceOnly = event(stream, 2, TRACE, null);

    store(MAPPER).append(stream, List.of(none, traceOnly), Version.initial());

    assertThat(storedIds(idOf(none))).containsExactly(null, null);
    assertThat(storedIds(idOf(traceOnly))).containsExactly(null, null);
    assertThat(linkOf(idOf(none))).isSameAs(SpanLink.NONE);
    assertThat(relay(List.of(idOf(none), idOf(traceOnly))))
        .hasSize(2)
        .allSatisfy(s -> assertThat(s.link).isSameAs(SpanLink.NONE));
  }

  @Test
  void none_isStoredAsNullColumns_andReadBackAsNone() throws Exception {
    outboxStore.save(
        OutboxEntry.pending(OutboxEntryId.of("none-1"), "{}", "Switched", stream())
            .withTraceLink(SpanLink.NONE));

    assertThat(storedIds("none-1")).containsExactly(null, null);
    assertThat(linkOf("none-1")).isSameAs(SpanLink.NONE);
    assertThat(outboxStore.loadPending(10).getFirst().traceLink()).isSameAs(SpanLink.NONE);
  }

  @Test
  void metadataIdsThatAreNotW3cIds_areNotStored_andTheAppendSucceeds() throws Exception {
    var stream = stream();
    var event = event(stream, 1, "trace-from-an-http-header", "not-a-span-id");

    store(MAPPER).append(stream, List.of(event), Version.initial());

    assertThat(storedIds(idOf(event))).containsExactly(null, null);
  }

  @Test
  void aCorruptStoredId_givesNoLink_andTheEntryIsStillDelivered() throws Exception {
    var stream = stream();
    var badTrace = event(stream, 1, TRACE, SPAN);
    var badSpan = event(stream, 2, TRACE, SPAN);
    var halfLink = event(stream, 3, TRACE, SPAN);
    store(MAPPER).append(stream, List.of(badTrace, badSpan, halfLink), Version.initial());
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE outbox_events SET trace_id = ?, span_id = ? WHERE entry_id = ?")) {
      ps.setString(1, "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz");
      ps.setString(2, SPAN);
      ps.setString(3, idOf(badTrace));
      ps.executeUpdate();
      ps.setString(1, TRACE);
      ps.setString(2, "00F067AA0BA902B7");
      ps.setString(3, idOf(badSpan));
      ps.executeUpdate();
      ps.setString(1, TRACE);
      ps.setString(2, null);
      ps.setString(3, idOf(halfLink));
      ps.executeUpdate();
    }

    assertThat(linkOf(idOf(badTrace))).isSameAs(SpanLink.NONE);
    assertThat(linkOf(idOf(badSpan))).isSameAs(SpanLink.NONE);
    assertThat(linkOf(idOf(halfLink))).isSameAs(SpanLink.NONE);
    var publishes = relay(List.of(idOf(badTrace), idOf(badSpan), idOf(halfLink)));
    assertThat(publishes)
        .hasSize(3)
        .allSatisfy(
            s -> {
              assertThat(s.link).isSameAs(SpanLink.NONE);
              assertThat(s.ok).isTrue();
            });
  }
}
