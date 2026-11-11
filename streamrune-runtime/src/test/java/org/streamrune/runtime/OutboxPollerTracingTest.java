package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.test.InMemoryOutboxStore;

/**
 * The outbox spans of {@link OutboxPoller}, recorded through {@link OpenTelemetryTracing}: one
 * {@code outbox/relay} per batch that claimed entries, with an {@code outbox/publish} of kind
 * {@code PRODUCER} under it per entry of a publisher that publishes one entry at a time; none for a
 * poll that claims nothing, and none with the area off or NOOP.
 */
class OutboxPollerTracingTest {

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private final InMemoryOutboxStore store =
      new InMemoryOutboxStore(
          Clock.systemUTC(),
          InMemoryOutboxStore.DEFAULT_CLAIM_LEASE,
          OutboxOrderingMode.AVAILABILITY_FIRST);

  private static StreamRuneTracing tracing() {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled());
  }

  private OutboxPoller poller(OutboxPublisher publisher, StreamRuneTracing tracing) {
    return OutboxPoller.builder()
        .outboxStore(store)
        .publisher(publisher)
        .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .tracing(tracing)
        .build();
  }

  private void save(String id) {
    store.save(OutboxEntry.pending(OutboxEntryId.of(id), "{}", "LampSwitched"));
  }

  private static List<SpanData> spans(String name) {
    return otelTesting.getSpans().stream().filter(s -> s.getName().equals(name)).toList();
  }

  @Test
  void aBatch_startsTheRelaySpan_withAPublishSpanPerEntryUnderIt() {
    save("e-1");
    save("e-2");
    List<OutboxEntry> published = new ArrayList<>();

    poller(published::add, tracing()).processBatch();

    assertThat(published).hasSize(2);
    List<SpanData> relays = spans(TraceAttributes.OUTBOX_RELAY);
    assertThat(relays).hasSize(1);
    SpanData relay = relays.getFirst();
    assertThat(relay.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(EventStoreTracingTest.keys(relay))
        .containsExactlyInAnyOrder(
            TraceAttributes.BATCH_SIZE,
            TraceAttributes.OUTBOX_PUBLISHED,
            TraceAttributes.OUTBOX_FAILED);
    assertThat(EventStoreTracingTest.number(relay, TraceAttributes.BATCH_SIZE)).isEqualTo(2L);
    assertThat(EventStoreTracingTest.number(relay, TraceAttributes.OUTBOX_PUBLISHED)).isEqualTo(2L);
    assertThat(EventStoreTracingTest.number(relay, TraceAttributes.OUTBOX_FAILED)).isZero();
    assertThat(relay.getParentSpanContext().isValid()).isFalse();

    List<SpanData> publishes = spans(TraceAttributes.OUTBOX_PUBLISH);
    assertThat(publishes).hasSize(2);
    for (SpanData publish : publishes) {
      assertThat(publish.getKind()).isEqualTo(SpanKind.PRODUCER);
      assertThat(EventStoreTracingTest.keys(publish))
          .containsExactlyInAnyOrder(
              TraceAttributes.OUTBOX_ENTRY_ID, TraceAttributes.OUTBOX_PAYLOAD_TYPE);
      assertThat(EventStoreTracingTest.string(publish, TraceAttributes.OUTBOX_PAYLOAD_TYPE))
          .isEqualTo("LampSwitched");
      assertThat(publish.getParentSpanId()).isEqualTo(relay.getSpanId());
      assertThat(publish.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
    }
    assertThat(publishes)
        .extracting(s -> EventStoreTracingTest.string(s, TraceAttributes.OUTBOX_ENTRY_ID))
        .containsExactlyInAnyOrder("e-1", "e-2");
  }

  @Test
  void aFailedPublish_marksItsPublishSpanFailed_andCountsInTheRelay() {
    save("e-1");
    save("e-2");

    poller(
            entry -> {
              if (entry.id().value().equals("e-2")) {
                throw new IllegalStateException("broker refused");
              }
            },
            tracing())
        .processBatch();

    SpanData relay = spans(TraceAttributes.OUTBOX_RELAY).getFirst();
    assertThat(EventStoreTracingTest.number(relay, TraceAttributes.OUTBOX_PUBLISHED)).isEqualTo(1L);
    assertThat(EventStoreTracingTest.number(relay, TraceAttributes.OUTBOX_FAILED)).isEqualTo(1L);
    SpanData failed =
        spans(TraceAttributes.OUTBOX_PUBLISH).stream()
            .filter(
                s -> "e-2".equals(EventStoreTracingTest.string(s, TraceAttributes.OUTBOX_ENTRY_ID)))
            .findFirst()
            .orElseThrow();
    EventStoreTracingTest.assertFailed(failed);
  }

  @Test
  void aPollThatClaimsNothing_startsNoSpan() {
    poller(entry -> {}, tracing()).processBatch();

    assertThat(otelTesting.getSpans()).isEmpty();
  }

  @Test
  void aPublisherWithABatchPathOfItsOwn_keepsIt_andGetsTheRelaySpanAlone() {
    save("e-1");
    List<List<OutboxEntry>> batches = new ArrayList<>();
    OutboxPublisher pipelining =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {
            throw new AssertionError("the batch path publishes");
          }

          @Override
          public BatchResult publishBatch(List<OutboxEntry> entries) {
            return publishBatch(entries, null);
          }

          @Override
          public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
            batches.add(entries);
            var confirmed = new LinkedHashSet<OutboxEntryId>();
            entries.forEach(e -> confirmed.add(e.id()));
            return new BatchResult(confirmed, Map.of());
          }
        };

    poller(pipelining, tracing()).processBatch();

    assertThat(batches).hasSize(1);
    assertThat(otelTesting.getSpans())
        .extracting(SpanData::getName)
        .containsExactly(TraceAttributes.OUTBOX_RELAY);
    assertThat(OutboxPoller.publishesOneAtATime(pipelining)).isFalse();
    assertThat(OutboxPoller.publishesOneAtATime(entry -> {})).isTrue();
  }

  @Test
  void withTheOutboxAreaOff_orNoop_noSpanStarts() {
    for (StreamRuneTracing off :
        List.of(
            OpenTelemetryTracing.of(
                otelTesting.getOpenTelemetry(),
                TracingSettings.builder().area(TracingArea.OUTBOX, false).build()),
            StreamRuneTracing.NOOP)) {
      save("e-" + off.hashCode());
      poller(entry -> {}, off).processBatch();
    }

    assertThat(otelTesting.getSpans()).isEmpty();
  }
}
