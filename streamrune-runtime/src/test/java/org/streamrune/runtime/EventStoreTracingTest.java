package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.AggregateState;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * The event-store spans of {@link InMemoryEventStore} in its serializing mode, recorded through
 * {@link OpenTelemetryTracing}: each span's name, kind and exact attribute keys, its outcome, its
 * nesting, the error status of a failure, and no span for a read that returns nothing, for a
 * by-reference store, for a switched-off area or for NOOP.
 */
class EventStoreTracingTest {

  private static final AggregateType LAMP = AggregateType.of("lamp");
  private static final StreamId STREAM = StreamId.of(LAMP, AggregateId.of("l-1"));

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  record Switched(String to) implements DomainEvent {}

  /** Serializes to {@code {"x": ...}}, which {@link Switched} cannot bind. */
  record Renamed(String x) implements DomainEvent {}

  /** A payload Jackson cannot serialize: its accessor throws. */
  record Unwritable(String value) implements DomainEvent {
    @Override
    public String value() {
      throw new IllegalStateException("cannot be written");
    }
  }

  record LampState(int switches) implements AggregateState {}

  static StreamRuneTracing tracing() {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled());
  }

  static SimpleEventTypeRegistry registry() {
    return SimpleEventTypeRegistry.builder()
        .registerEvent("Switched", Switched.class)
        .registerState("LampState", LampState.class)
        .build();
  }

  private static InMemoryEventStore store(StreamRuneTracing tracing) {
    return InMemoryEventStore.serializing(registry(), null).withTracing(tracing);
  }

  static EventEnvelope event(StreamId stream, DomainEvent payload) {
    return new EventEnvelope(
        GlobalOffset.initial(),
        stream,
        Version.initial(),
        EventType.of("Switched"),
        payload,
        new EventMetadata(
            EventId.of("evt-1"),
            CommandId.of("cmd-1"),
            null,
            null,
            CorrelationId.of("corr-1"),
            null,
            null,
            Instant.parse("2026-01-01T00:00:00Z"),
            Map.of()));
  }

  private static EventEnvelope switched(String to) {
    return event(STREAM, new Switched(to));
  }

  static SpanData span(String name) {
    List<SpanData> spans =
        otelTesting.getSpans().stream().filter(s -> s.getName().equals(name)).toList();
    assertThat(spans).as("spans named %s", name).hasSize(1);
    return spans.getFirst();
  }

  static Set<String> keys(SpanData span) {
    return span.getAttributes().asMap().keySet().stream()
        .map(AttributeKey::getKey)
        .collect(Collectors.toSet());
  }

  static String string(SpanData span, String key) {
    return span.getAttributes().get(AttributeKey.stringKey(key));
  }

  static Long number(SpanData span, String key) {
    return span.getAttributes().get(AttributeKey.longKey(key));
  }

  static Boolean flag(SpanData span, String key) {
    return span.getAttributes().get(AttributeKey.booleanKey(key));
  }

  static void assertFailed(SpanData span) {
    assertThat(span.getStatus().getStatusCode()).as(span.getName()).isEqualTo(StatusCode.ERROR);
    assertThat(span.getEvents()).extracting(EventData::getName).contains("exception");
  }

  private static List<String> names() {
    return otelTesting.getSpans().stream().map(SpanData::getName).toList();
  }

  @Test
  void append_startsTheAppendSpan_withTheSerializeSpanUnderIt() {
    store(tracing()).append(STREAM, List.of(switched("on"), switched("off")), new Version(0));

    SpanData append = span(TraceAttributes.EVENT_STORE_APPEND);
    SpanData serialize = span(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE);
    assertThat(append.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(keys(append))
        .containsExactlyInAnyOrder(
            TraceAttributes.STREAM_ID,
            TraceAttributes.AGGREGATE_TYPE,
            TraceAttributes.EVENT_COUNT,
            TraceAttributes.EXPECTED_VERSION,
            TraceAttributes.OUTBOX_ENTRY_COUNT,
            TraceAttributes.APPEND_OUTCOME);
    assertThat(string(append, TraceAttributes.STREAM_ID)).isEqualTo("lamp:l-1");
    assertThat(string(append, TraceAttributes.AGGREGATE_TYPE)).isEqualTo("lamp");
    assertThat(number(append, TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(number(append, TraceAttributes.EXPECTED_VERSION)).isZero();
    assertThat(number(append, TraceAttributes.OUTBOX_ENTRY_COUNT)).isZero();
    assertThat(string(append, TraceAttributes.APPEND_OUTCOME)).isEqualTo("APPENDED");
    assertThat(append.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);

    assertThat(serialize.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(keys(serialize)).containsExactly(TraceAttributes.EVENT_COUNT);
    assertThat(number(serialize, TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(serialize.getParentSpanId()).isEqualTo(append.getSpanId());
  }

  @Test
  void anEmptyAppend_startsNoSpan() {
    store(tracing()).append(STREAM, List.of(), new Version(0));

    assertThat(otelTesting.getSpans()).isEmpty();
  }

  @Test
  void anAppendThatLosesItsVersionCheck_isTheOutcomeConflict_notAnError() {
    InMemoryEventStore store = store(tracing());
    store.append(STREAM, List.of(switched("on")), new Version(0));
    otelTesting.clearSpans();

    assertThatThrownBy(() -> store.append(STREAM, List.of(switched("off")), new Version(0)))
        .isInstanceOf(OptimisticLockException.class);

    SpanData append = span(TraceAttributes.EVENT_STORE_APPEND);
    assertThat(string(append, TraceAttributes.APPEND_OUTCOME)).isEqualTo("CONFLICT");
    assertThat(append.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
    assertThat(append.getEvents()).isEmpty();
  }

  @Test
  void aKeyedAppend_isTheOutcomeAppended_andARecordedKeyTheOutcomeDuplicate() {
    InMemoryEventStore store = store(tracing()).withCommandInbox(new InMemoryCommandInbox());
    IdempotencyKey key = IdempotencyKey.of("key-1");
    store.appendWithKey(STREAM, List.of(switched("on")), new Version(0), key, "Switch");
    assertThat(string(span(TraceAttributes.EVENT_STORE_APPEND), TraceAttributes.APPEND_OUTCOME))
        .isEqualTo("APPENDED");
    otelTesting.clearSpans();

    store.appendWithKey(STREAM, List.of(switched("on")), new Version(0), key, "Switch");

    assertThat(string(span(TraceAttributes.EVENT_STORE_APPEND), TraceAttributes.APPEND_OUTCOME))
        .isEqualTo("DUPLICATE");
  }

  @Test
  void aFailedSerialization_marksTheSerializeAndTheAppendSpansFailed() {
    assertThatThrownBy(
            () ->
                store(tracing())
                    .append(STREAM, List.of(event(STREAM, new Unwritable("v"))), new Version(0)))
        .isInstanceOf(EventStoreException.class);

    assertFailed(span(TraceAttributes.EVENT_STORE_APPEND));
    assertFailed(span(TraceAttributes.EVENT_STORE_APPEND_SERIALIZE));
    assertThat(keys(span(TraceAttributes.EVENT_STORE_APPEND)))
        .doesNotContain(TraceAttributes.APPEND_OUTCOME);
  }

  @Test
  void load_startsTheLoadSpan_withTheSnapshotAndDeserializeSpansUnderIt() {
    InMemoryEventStore store = store(tracing());
    store.append(STREAM, List.of(switched("on"), switched("off")), new Version(0));
    otelTesting.clearSpans();

    store.load(STREAM, 1);

    SpanData load = span(TraceAttributes.EVENT_STORE_LOAD);
    SpanData snapshot = span(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    SpanData deserialize = span(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(load.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(keys(load))
        .containsExactlyInAnyOrder(
            TraceAttributes.STREAM_ID,
            TraceAttributes.AGGREGATE_TYPE,
            TraceAttributes.EVENT_COUNT,
            TraceAttributes.SNAPSHOT_USED);
    assertThat(number(load, TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(flag(load, TraceAttributes.SNAPSHOT_USED)).isFalse();

    assertThat(snapshot.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(keys(snapshot))
        .containsExactlyInAnyOrder(
            TraceAttributes.STREAM_ID,
            TraceAttributes.SNAPSHOT_VERSION,
            TraceAttributes.SNAPSHOT_OUTCOME);
    assertThat(string(snapshot, TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("MISS");
    assertThat(number(snapshot, TraceAttributes.SNAPSHOT_VERSION)).isZero();
    assertThat(snapshot.getParentSpanId()).isEqualTo(load.getSpanId());

    assertThat(deserialize.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(keys(deserialize))
        .containsExactlyInAnyOrder(TraceAttributes.EVENT_COUNT, TraceAttributes.UPCAST_COUNT);
    assertThat(number(deserialize, TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(number(deserialize, TraceAttributes.UPCAST_COUNT)).isZero();
    assertThat(deserialize.getParentSpanId()).isEqualTo(load.getSpanId());
  }

  @Test
  void aLoadThatIgnoresSnapshots_startsNoSnapshotSpan() {
    InMemoryEventStore store = store(tracing());
    store.append(STREAM, List.of(switched("on")), new Version(0));
    otelTesting.clearSpans();

    store.load(STREAM, EventStore.IGNORE_SNAPSHOT);

    assertThat(names())
        .containsExactlyInAnyOrder(
            TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE, TraceAttributes.EVENT_STORE_LOAD);
  }

  @Test
  void aLoadFromASnapshot_isTheOutcomeHit_andAnUnusableSnapshotIsDiscarded() {
    InMemoryEventStore store = store(tracing());
    store.append(STREAM, List.of(switched("on"), switched("off")), new Version(0));
    store.saveSnapshot(STREAM, new Version(1), new LampState(1), 1, 0);
    otelTesting.clearSpans();

    store.load(STREAM, 1);

    SpanData hit = span(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    assertThat(string(hit, TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("HIT");
    assertThat(number(hit, TraceAttributes.SNAPSHOT_VERSION)).isEqualTo(1L);
    assertThat(flag(span(TraceAttributes.EVENT_STORE_LOAD), TraceAttributes.SNAPSHOT_USED))
        .isTrue();
    assertThat(
            number(span(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE), TraceAttributes.EVENT_COUNT))
        .isEqualTo(1L);
    otelTesting.clearSpans();

    // Stored at snapshot version 1, expected at 2, no migration: discarded, the stream replays.
    store.load(STREAM, 2);

    SpanData discarded = span(TraceAttributes.EVENT_STORE_SNAPSHOT_LOAD);
    assertThat(string(discarded, TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("DISCARDED");
    assertThat(number(discarded, TraceAttributes.SNAPSHOT_VERSION)).isZero();
    assertThat(flag(span(TraceAttributes.EVENT_STORE_LOAD), TraceAttributes.SNAPSHOT_USED))
        .isFalse();
    assertThat(
            number(span(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE), TraceAttributes.EVENT_COUNT))
        .isEqualTo(2L);
  }

  @Test
  void saveSnapshot_startsTheSaveSpan() {
    InMemoryEventStore store = store(tracing());
    store.append(STREAM, List.of(switched("on")), new Version(0));
    otelTesting.clearSpans();

    store.saveSnapshot(STREAM, new Version(1), new LampState(1), 1, 0);

    SpanData save = span(TraceAttributes.EVENT_STORE_SNAPSHOT_SAVE);
    assertThat(save.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(keys(save))
        .containsExactlyInAnyOrder(
            TraceAttributes.STREAM_ID,
            TraceAttributes.SNAPSHOT_VERSION,
            TraceAttributes.SNAPSHOT_OUTCOME);
    assertThat(number(save, TraceAttributes.SNAPSHOT_VERSION)).isEqualTo(1L);
    assertThat(string(save, TraceAttributes.SNAPSHOT_OUTCOME)).isEqualTo("SAVED");
  }

  @Test
  void anEventBelowItsCurrentSchemaVersion_countsAsUpcast() {
    InMemoryEventStore store = store(tracing());
    store.append(STREAM, List.of(switched("on")), new Version(0));
    store.withUpcasters(
        List.of(
            new EventUpcaster() {
              @Override
              public EventType eventType() {
                return EventType.of("Switched");
              }

              @Override
              public int currentVersion() {
                return 2;
              }

              @Override
              public Map<String, Object> upcast(Map<String, Object> eventData, int fromVersion) {
                return eventData;
              }
            }));
    store.append(STREAM, List.of(switched("off")), new Version(1));
    otelTesting.clearSpans();

    store.load(STREAM, EventStore.IGNORE_SNAPSHOT);

    SpanData deserialize = span(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE);
    assertThat(number(deserialize, TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(number(deserialize, TraceAttributes.UPCAST_COUNT)).isEqualTo(1L);
  }

  @Test
  void aFailedDeserialization_marksTheDeserializeAndTheLoadSpansFailed() {
    InMemoryEventStore store = store(tracing());
    store.append(STREAM, List.of(event(STREAM, new Renamed("on"))), new Version(0));
    otelTesting.clearSpans();

    assertThatThrownBy(() -> store.load(STREAM, EventStore.IGNORE_SNAPSHOT))
        .isInstanceOf(EventDeserializationException.class);

    assertFailed(span(TraceAttributes.EVENT_STORE_LOAD));
    assertFailed(span(TraceAttributes.EVENT_STORE_LOAD_DESERIALIZE));
    assertThat(keys(span(TraceAttributes.EVENT_STORE_LOAD)))
        .doesNotContain(TraceAttributes.EVENT_COUNT, TraceAttributes.SNAPSHOT_USED);
  }

  @Test
  void aFailedRead_marksTheReadSpanFailed() {
    InMemoryEventStore store = store(tracing());
    store.append(STREAM, List.of(event(STREAM, new Renamed("on"))), new Version(0));
    otelTesting.clearSpans();

    assertThatThrownBy(() -> store.readGlobalStream(GlobalOffset.initial(), 10))
        .isInstanceOf(EventDeserializationException.class);

    assertFailed(span(TraceAttributes.EVENT_STORE_READ));
  }

  @Test
  void aReadThatReturnsEvents_startsTheReadSpan_andAnEmptyReadStartsNone() {
    InMemoryEventStore store = store(tracing());
    store.append(STREAM, List.of(switched("on"), switched("off")), new Version(0));
    otelTesting.clearSpans();

    store.readGlobalStream(GlobalOffset.of(2), 10);
    store.readStream(STREAM, new Version(2), 10);
    assertThat(otelTesting.getSpans()).isEmpty();

    store.readGlobalStream(GlobalOffset.initial(), 10);
    SpanData global = span(TraceAttributes.EVENT_STORE_READ);
    assertThat(global.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(keys(global))
        .containsExactlyInAnyOrder(TraceAttributes.EVENT_COUNT, TraceAttributes.READ_KIND);
    assertThat(number(global, TraceAttributes.EVENT_COUNT)).isEqualTo(2L);
    assertThat(string(global, TraceAttributes.READ_KIND)).isEqualTo("GLOBAL");
    otelTesting.clearSpans();

    store.readStream(STREAM, new Version(1), 10);
    SpanData stream = span(TraceAttributes.EVENT_STORE_READ);
    assertThat(number(stream, TraceAttributes.EVENT_COUNT)).isEqualTo(1L);
    assertThat(string(stream, TraceAttributes.READ_KIND)).isEqualTo("STREAM");
  }

  @Test
  void anIdleSubscriptionPoll_startsNoSpan_andAPollThatDeliversStartsTheReadAndDeliverySpans() {
    StreamRuneTracing tracing = tracing();
    InMemoryEventStore store = store(tracing);
    List<EventEnvelope> delivered = new java.util.ArrayList<>();
    PollingEventSubscription subscription =
        PollingEventSubscription.builder()
            .subscriptionName(SubscriptionName.of("lamps"))
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(delivered::addAll)
            .tracing(tracing)
            .build();

    assertThat(subscription.pollOnce()).isZero();
    assertThat(subscription.pollOnce()).isZero();
    assertThat(otelTesting.getSpans()).isEmpty();

    store.append(STREAM, List.of(switched("on")), new Version(0));
    otelTesting.clearSpans();
    assertThat(subscription.pollOnce()).isEqualTo(1);

    assertThat(delivered).hasSize(1);
    assertThat(names())
        .containsExactlyInAnyOrder(TraceAttributes.EVENT_STORE_READ, "subscription/lamps/deliver");
  }

  @Test
  void aByReferenceStore_startsNoSpan() {
    InMemoryEventStore store = new InMemoryEventStore().withTracing(tracing());
    store.append(STREAM, List.of(switched("on")), new Version(0));
    store.saveSnapshot(STREAM, new Version(1), new LampState(1), 1, 0);
    store.load(STREAM, 1);
    store.readGlobalStream(GlobalOffset.initial(), 10);

    assertThat(otelTesting.getSpans()).isEmpty();
  }

  @Test
  void withTheEventStoreAreaOff_orNoop_noSpanStarts() {
    for (StreamRuneTracing off :
        List.of(
            OpenTelemetryTracing.of(
                otelTesting.getOpenTelemetry(),
                TracingSettings.builder().area(TracingArea.EVENT_STORE, false).build()),
            StreamRuneTracing.NOOP)) {
      InMemoryEventStore store = store(off).withCommandInbox(new InMemoryCommandInbox());
      store.append(STREAM, List.of(switched("on")), new Version(0));
      store.appendWithKey(
          STREAM, List.of(switched("off")), new Version(1), IdempotencyKey.of("k"), "Switch");
      store.saveSnapshot(STREAM, new Version(1), new LampState(1), 1, 0);
      store.load(STREAM, 1);
      store.readGlobalStream(GlobalOffset.initial(), 10);
    }

    assertThat(otelTesting.getSpans()).isEmpty();
  }
}
