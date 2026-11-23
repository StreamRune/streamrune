package org.streamrune.sqlite;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.IdGenerator;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreContract;
import org.streamrune.test.EventStoreContract.ContractEvent;
import org.streamrune.test.EventStoreContract.ContractState;

/** The spans {@link SqliteEventStore} starts, as {@code PostgresEventStore} starts them. */
class SqliteEventStoreTracingTest {

  private static final StreamId STREAM =
      StreamId.of(AggregateType.of("lamp"), AggregateId.of("l-1"));

  @TempDir Path dir;

  private SqliteDatabase database;

  @BeforeEach
  void open() {
    database = SqliteDatabase.open(TestDatabases.dataSource(dir.resolve("tracing.db")));
    database.migrate(SqliteSchemaSeries.EVENT_STORE);
  }

  @AfterEach
  void close() {
    database.close();
  }

  private EventStore store(StreamRuneTracing tracing) {
    return new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
        .tracing(tracing)
        .create();
  }

  private static EventEnvelope event(String value) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        STREAM,
        Version.initial(),
        EventStoreContract.EVENT_TYPE,
        new ContractEvent(value),
        new EventMetadata(
            IdGenerator.generateEventId(),
            IdGenerator.generateCommandId(),
            null,
            CorrelationId.of(UUID.randomUUID().toString()),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z")));
  }

  private static void assertFailed(RecordingTracing.RecordedSpan span) {
    assertThat(span.error).as(span.name).isNotNull();
    assertThat(span.ok).isFalse();
    assertThat(span.closed).isTrue();
  }

  @Test
  void append_startsTheAppendSpan_withTheSerializeSpanUnderIt() {
    var tracing = new RecordingTracing();

    store(tracing).append(STREAM, List.of(event("a"), event("b")), Version.initial());

    var append = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    var serialize = tracing.only(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    assertThat(append.kind).isEqualTo(SpanKind.INTERNAL);
    assertThat(append.attributes())
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                TraceAttributes.STREAM_ID,
                "lamp:l-1",
                TraceAttributes.AGGREGATE_TYPE,
                "lamp",
                TraceAttributes.EVENT_COUNT,
                2L,
                TraceAttributes.EXPECTED_VERSION,
                0L,
                TraceAttributes.OUTBOX_ENTRY_COUNT,
                0L,
                TraceAttributes.APPEND_OUTCOME,
                "APPENDED"));
    assertThat(append.ok).isTrue();
    assertThat(append.closed).isTrue();
    assertThat(serialize.attributes())
        .containsExactlyEntriesOf(Map.of(TraceAttributes.EVENT_COUNT, 2L));
    assertThat(serialize.parent).isSameAs(append);
    assertThat(serialize.ok).isTrue();
  }

  @Test
  void aConflict_isTheOutcomeConflict_notAnError() {
    var tracing = new RecordingTracing();
    EventStore store = store(tracing);
    store.append(STREAM, List.of(event("a")), Version.initial());
    tracing.clear();

    assertThatThrownBy(() -> store.append(STREAM, List.of(event("b")), Version.initial()))
        .isInstanceOf(OptimisticLockException.class);
    var conflict = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(conflict.attribute(TraceAttributes.APPEND_OUTCOME)).isEqualTo("CONFLICT");
    assertThat(conflict.error).isNull();
    assertThat(conflict.closed).isTrue();
  }

  @Test
  void aFailedAppend_marksTheAppendSpanFailed() throws Exception {
    var tracing = new RecordingTracing();
    EventStore store = store(tracing);
    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("DROP TABLE event_stream");
    }

    assertThatThrownBy(() -> store.append(STREAM, List.of(event("a")), Version.initial()))
        .isInstanceOf(EventStoreException.class);
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_APPEND));
  }

  @Test
  void load_startsTheLoadSpan_withTheSnapshotAndDeserializeSpansUnderIt() {
    var tracing = new RecordingTracing();
    EventStore store = store(tracing);
    store.append(STREAM, List.of(event("a"), event("b")), Version.initial());
    store.saveSnapshot(STREAM, new Version(1), new ContractState("s"));
    tracing.clear();

    store.load(STREAM);

    var load = tracing.only(TraceAttributes.EVENT_STORE_LOAD);
    var hit = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    var deserialize = tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(hit.parent).isSameAs(load);
    assertThat(hit.attribute(TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("HIT");
    assertThat(hit.attribute(TraceAttributes.STREAM_VERSION)).isEqualTo(1L);
    assertThat(deserialize.parent).isSameAs(load);
    assertThat(load.attribute(TraceAttributes.SNAPSHOT_USED)).isEqualTo(true);
    assertThat(load.attribute(TraceAttributes.EVENT_COUNT)).isEqualTo(1L);
    tracing.clear();

    store.load(STREAM, 2);

    var discarded = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    assertThat(discarded.attribute(TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("DISCARDED");
    assertThat(
            tracing.only(TraceAttributes.EVENT_STORE_LOAD).attribute(TraceAttributes.SNAPSHOT_USED))
        .isEqualTo(false);
  }

  @Test
  void anUndeserializableSnapshot_isDiscarded_andAFailedEventDeserializationFailsTheLoad()
      throws Exception {
    var tracing = new RecordingTracing();
    EventStore store = store(tracing);
    store.append(STREAM, List.of(event("a")), Version.initial());
    store.saveSnapshot(STREAM, new Version(1), new ContractState("s"));
    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("UPDATE snapshot_store SET state_payload = '{\"marker\": [1]}'");
    }
    tracing.clear();

    assertThat(store.load(STREAM).events()).hasSize(1);
    assertThat(
            tracing
                .only(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD)
                .attribute(TraceAttributes.SNAPSHOT_OUTCOME))
        .isEqualTo("DISCARDED");

    try (Connection conn = database.dataSource().getConnection();
        var st = conn.createStatement()) {
      st.execute("UPDATE event_stream SET payload = '{\"value\": [1]}'");
    }
    tracing.clear();
    assertThatThrownBy(() -> store.load(STREAM, EventStore.IGNORE_SNAPSHOT))
        .isInstanceOf(EventStoreException.class);
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_LOAD));
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE));
    tracing.clear();
    tracing.inside(
        "consumer",
        () ->
            assertThatThrownBy(() -> store.readStream(STREAM, Version.initial(), 10))
                .isInstanceOf(EventStoreException.class));
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_READ));
  }

  @Test
  void saveSnapshot_isTheOutcomeSaved_orSkippedAfterAPurge() {
    var tracing = new RecordingTracing();
    EventStore store = store(tracing);

    store.saveSnapshot(STREAM, new Version(1), new ContractState("s"), 1, 0);
    var save = tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE);
    assertThat(save.attribute(TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("SAVED");
    assertThat(save.attribute(TraceAttributes.STREAM_VERSION)).isEqualTo(1L);
    tracing.clear();

    new SqliteSnapshotStorePurger(database, s -> List.of(STREAM)).purge(SubjectId.of("s"));
    store.saveSnapshot(STREAM, new Version(1), new ContractState("s"), 1, 0);
    assertThat(
            tracing
                .only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE)
                .attribute(TraceAttributes.SNAPSHOT_OUTCOME))
        .isEqualTo("SKIPPED");
    tracing.clear();

    record Unregistered(String x) implements org.streamrune.core.AggregateState {}
    assertThatThrownBy(() -> store.saveSnapshot(STREAM, new Version(1), new Unregistered("x")))
        .isInstanceOf(EventStoreException.class);
    assertFailed(tracing.only(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE));
  }

  @Test
  void aReadThatReturnsEvents_startsTheReadSpanUnderTheCurrentSpan_andAnEmptyReadStartsNone() {
    var tracing = new RecordingTracing();
    EventStore store = store(tracing);
    store.append(STREAM, List.of(event("a")), Version.initial());
    tracing.clear();

    var consumer =
        tracing.inside(
            "consumer",
            () -> {
              store.readGlobalStream(GlobalOffset.initial(), 10);
              store.readGlobalStream(GlobalOffset.of(1), 10);
            });

    var read = tracing.only(TraceAttributes.EVENT_STORE_READ);
    assertThat(read.parent).isSameAs(consumer);
    assertThat(read.startTime).isNotNull();
    assertThat(read.attribute(TraceAttributes.READ_KIND)).isEqualTo("GLOBAL");
    assertThat(read.attribute(TraceAttributes.EVENT_COUNT)).isEqualTo(1L);
    tracing.clear();

    store.readStream(STREAM, Version.initial(), 10);
    assertThat(tracing.names()).as("no span is current: the poll starts none").isEmpty();
  }

  @Test
  void anUpcastLoadCountsItsUpcasts() {
    EventStore writer = store(StreamRuneTracing.NOOP);
    writer.append(STREAM, List.of(event("a")), Version.initial());
    var tracing = new RecordingTracing();
    EventStore reader =
        new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
            .tracing(tracing)
            .upcasters(
                List.of(
                    new org.streamrune.core.upcasting.EventUpcaster() {
                      @Override
                      public org.streamrune.core.types.EventType eventType() {
                        return EventStoreContract.EVENT_TYPE;
                      }

                      @Override
                      public int currentVersion() {
                        return 2;
                      }

                      @Override
                      public Map<String, Object> upcast(
                          Map<String, Object> eventData, int fromVersion) {
                        return eventData;
                      }
                    }))
            .create();

    reader.load(STREAM, EventStore.IGNORE_SNAPSHOT);

    assertThat(
            tracing
                .only(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE)
                .attribute(TraceAttributes.UPCAST_COUNT))
        .isEqualTo(1L);
  }

  @Test
  void withTheEventStoreAreaOff_noSpanStarts() {
    var off = RecordingTracing.without(TracingArea.EVENT_STORE);
    EventStore store = store(off);

    store.append(STREAM, List.of(event("a")), Version.initial());
    store.saveSnapshot(STREAM, new Version(1), new ContractState("s"));
    store.load(STREAM);
    off.inside("consumer", () -> store.readGlobalStream(GlobalOffset.initial(), 10));

    assertThat(off.names()).containsExactly("consumer");
  }

  @Test
  void theNullArgumentChecksAreTheSameWhenTraced() {
    EventStore store = store(new RecordingTracing());
    assertThatThrownBy(() -> store.append(null, List.of(event("a")), Version.initial()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("streamId is required");
    assertThatThrownBy(() -> store.append(STREAM, null, Version.initial()))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("events is required");
    assertThatThrownBy(() -> store.append(STREAM, List.of(event("a")), null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("expectedVersion is required");
    assertThatThrownBy(() -> store.saveSnapshot(STREAM, null, new ContractState("s")))
        .isInstanceOf(NullPointerException.class);
    assertThat(store.append(STREAM, List.of(), Version.initial()).globalOffsets()).isEmpty();
  }

  /** The registry the store was built with resolves the events it appends. */
  @Test
  void theFactoryPassesItsTracingToTheStore() {
    var tracing = new RecordingTracing();
    new SqliteEventStoreFactory(
            database,
            SimpleEventTypeRegistry.builder()
                .registerEvent(EventStoreContract.EVENT_TYPE, ContractEvent.class)
                .build())
        .tracing(tracing)
        .create()
        .append(STREAM, List.of(event("a")), Version.initial());

    assertThat(tracing.names())
        .containsExactly(
            TraceAttributes.EVENT_STORE_APPEND, TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
  }

  /**
   * A keyed append is the outcome APPENDED, its replay DUPLICATE; the span counts the outbox
   * entries the mapper derived.
   */
  @Test
  void aKeyedAppend_isAppendedThenDuplicate_andCountsItsOutboxEntries() {
    var tracing = new RecordingTracing();
    EventStore store =
        new SqliteEventStoreFactory(database, EventStoreContract.Setup.defaults().registry())
            .tracing(tracing)
            .outboxStore(new SqliteOutboxStore(database))
            .outboxEventMapper(EventStoreContract.OUTBOX_MAPPER)
            .commandInbox(new SqliteCommandInbox(database))
            .create();
    List<EventEnvelope> events = List.of(event("a"), event("b"));
    IdempotencyKey key = IdempotencyKey.of("traced-key");

    store.appendWithKey(STREAM, events, Version.initial(), key, "Cmd");
    var first = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(first.attribute(TraceAttributes.APPEND_OUTCOME)).isEqualTo("APPENDED");
    assertThat(first.attribute(TraceAttributes.OUTBOX_ENTRY_COUNT)).isEqualTo(2L);
    tracing.clear();

    store.appendWithKey(STREAM, events, Version.initial(), key, "Cmd");
    var replay = tracing.only(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(replay.attribute(TraceAttributes.APPEND_OUTCOME)).isEqualTo("DUPLICATE");
    assertThat(replay.ok).isTrue();
    tracing.clear();

    assertThatThrownBy(() -> store.appendWithKey(STREAM, events, Version.initial(), null, "Cmd"))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("idempotencyKey is required");
    assertThat(tracing.names()).as("no span for refused arguments").isEmpty();
  }
}
