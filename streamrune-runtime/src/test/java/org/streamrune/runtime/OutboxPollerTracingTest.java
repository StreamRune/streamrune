package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
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
import org.streamrune.core.outbox.OutboxEntrySpans;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanBaggage;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.TraceId;
import org.streamrune.test.ForwardingOutboxStore;
import org.streamrune.test.InMemoryOutboxStore;

/**
 * The outbox spans of {@link OutboxPoller}, recorded through {@link OpenTelemetryTracing}: one
 * {@code outbox/relay} per batch that claimed entries, with an {@code outbox/publish} of kind
 * {@code PRODUCER} under it per entry's hand-off, through the {@link OutboxEntrySpans} the poller
 * hands the publisher; none for a poll that claims nothing, and none with the area off or NOOP. An
 * entry's {@code outbox/publish} links to the command span whose ids the entry carries.
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

    poller(published::add, tracing()).pollCycle();

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
  void aCycleThatDrainsSeveralBatches_startsOneRelaySpanPerBatch_eachItsOwnTrace() {
    for (int i = 0; i < 25; i++) {
      save("e-" + i);
    }

    poller(entry -> {}, tracing()).pollCycle();

    List<SpanData> relays = spans(TraceAttributes.OUTBOX_RELAY);
    assertThat(relays)
        .extracting(r -> EventStoreTracingTest.number(r, TraceAttributes.BATCH_SIZE))
        .containsExactly(10L, 10L, 5L);
    assertThat(relays).allSatisfy(r -> assertThat(r.getParentSpanContext().isValid()).isFalse());
    assertThat(relays).extracting(SpanData::getTraceId).doesNotHaveDuplicates();
    assertThat(spans(TraceAttributes.OUTBOX_PUBLISH)).hasSize(25);
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
        .pollCycle();

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
    poller(entry -> {}, tracing()).pollCycle();

    assertThat(otelTesting.getSpans()).isEmpty();
  }

  /**
   * Starts and ends a {@code command/Switch} span, the way the command bus does around a command,
   * and returns the link the event store stores on the entries that command's append writes: the
   * command span's own trace id and span id, which the events' metadata carries.
   */
  private static SpanLink commandSpan() {
    StreamRuneTracing tracing = tracing();
    try (TraceScope command =
        tracing.start(
            TracingArea.COMMAND,
            TraceAttributes.commandSpanName("Switch"),
            org.streamrune.core.tracing.SpanKind.INTERNAL,
            SpanAttributes.empty())) {
      SpanContext current = Span.current().getSpanContext();
      command.recordSuccess();
      return new SpanLink(TraceId.of(current.getTraceId()), SpanId.of(current.getSpanId()));
    }
  }

  private static SpanData only(String name) {
    List<SpanData> named = spans(name);
    assertThat(named).hasSize(1);
    return named.getFirst();
  }

  @Test
  void anEntryWrittenUnderACommand_linksItsPublishSpanToTheCommandSpan() {
    SpanLink command = commandSpan();
    store.save(
        OutboxEntry.pending(OutboxEntryId.of("e-1"), "{}", "LampSwitched").withTraceLink(command));
    save("e-2");
    List<OutboxEntry> published = new ArrayList<>();

    poller(published::add, tracing()).pollCycle();

    assertThat(published)
        .extracting(OutboxEntry::traceLink)
        .containsExactly(command, SpanLink.NONE);
    SpanData commandSpan = only(TraceAttributes.commandSpanName("Switch"));
    assertThat(commandSpan.getTraceId()).isEqualTo(command.traceId().value());
    assertThat(commandSpan.getSpanId()).isEqualTo(command.spanId().value());
    SpanData relay = only(TraceAttributes.OUTBOX_RELAY);
    assertThat(relay.getLinks()).isEmpty();
    SpanData linked = publishSpanOf("e-1");
    assertThat(linked.getTraceId())
        .as("the relay runs in a trace of its own")
        .isNotEqualTo(command.traceId().value());
    assertThat(linked.getLinks())
        .singleElement()
        .satisfies(
            link -> {
              assertThat(link.getSpanContext().getTraceId()).isEqualTo(commandSpan.getTraceId());
              assertThat(link.getSpanContext().getSpanId()).isEqualTo(commandSpan.getSpanId());
            });
    assertThat(publishSpanOf("e-2").getLinks()).as("an entry without a link").isEmpty();
  }

  @Test
  void aBatchPublisher_startsTheLinkedSpanOfEachHandOffThroughTheHook() {
    SpanLink command = commandSpan();
    store.save(
        OutboxEntry.pending(OutboxEntryId.of("e-1"), "{}", "LampSwitched").withTraceLink(command));
    OutboxPublisher pipelining =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {
            throw new AssertionError("the batch path publishes");
          }

          @Override
          public BatchResult publishBatch(
              List<OutboxEntry> entries, Instant deadline, OutboxEntrySpans spans) {
            var confirmed = new LinkedHashSet<OutboxEntryId>();
            for (OutboxEntry entry : entries) {
              try (TraceScope hand = spans.start(entry)) {
                hand.recordSuccess();
              }
              confirmed.add(entry.id());
            }
            return new BatchResult(confirmed, Map.of());
          }
        };

    poller(pipelining, tracing()).pollCycle();

    assertThat(publishSpanOf("e-1").getLinks())
        .singleElement()
        .satisfies(
            link -> {
              assertThat(link.getSpanContext().getTraceId()).isEqualTo(command.traceId().value());
              assertThat(link.getSpanContext().getSpanId()).isEqualTo(command.spanId().value());
            });
  }

  @Test
  void anEntryWhoseStoreHandsBackIdsThatAreNotW3cIds_isDeliveredWithAnUnlinkedSpan() {
    save("e-1");
    SpanLink malformed = new SpanLink(TraceId.of("from-a-header"), SpanId.of("abc"));
    OutboxStore corrupting =
        new ForwardingOutboxStore(store) {
          @Override
          public List<OutboxEntry> loadPending(int limit) {
            return super.loadPending(limit).stream().map(e -> e.withTraceLink(malformed)).toList();
          }
        };
    List<OutboxEntry> published = new ArrayList<>();
    // The links the poller hands the tracing: a tracing of the application's own sees no link
    // whose ids are not W3C ids, whatever OpenTelemetry would make of one.
    List<SpanLink> links = new java.util.concurrent.CopyOnWriteArrayList<>();
    StreamRuneTracing otel = tracing();
    StreamRuneTracing recording =
        new StreamRuneTracing() {
          @Override
          public boolean enabled(TracingArea area) {
            return otel.enabled(area);
          }

          @Override
          public TraceScope start(
              TracingArea area,
              String spanName,
              org.streamrune.core.tracing.SpanKind kind,
              SpanAttributes attributes,
              SpanLink link,
              SpanBaggage baggage) {
            if (spanName.equals(TraceAttributes.OUTBOX_PUBLISH)) {
              links.add(link);
            }
            return otel.start(area, spanName, kind, attributes, link, baggage);
          }
        };

    OutboxPoller.builder()
        .outboxStore(corrupting)
        .publisher(published::add)
        .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .tracing(recording)
        .build()
        .pollCycle();

    assertThat(published).extracting(e -> e.id().value()).containsExactly("e-1");
    assertThat(links).containsExactly(SpanLink.NONE);
    assertThat(store.findById(OutboxEntryId.of("e-1")).orElseThrow().status())
        .isEqualTo(org.streamrune.core.outbox.OutboxStatus.DELIVERED);
    SpanData publish = publishSpanOf("e-1");
    assertThat(publish.getLinks()).isEmpty();
    assertThat(publish.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
  }

  private static SpanData publishSpanOf(String entryId) {
    return spans(TraceAttributes.OUTBOX_PUBLISH).stream()
        .filter(
            s -> entryId.equals(EventStoreTracingTest.string(s, TraceAttributes.OUTBOX_ENTRY_ID)))
        .findFirst()
        .orElseThrow();
  }

  @Test
  void aPublisherWithABatchPathOfItsOwn_startsTheSpanOfEachHandOff() {
    save("e-1");
    save("e-2");
    List<List<OutboxEntry>> batches = new ArrayList<>();
    OutboxPublisher pipelining =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {
            throw new AssertionError("the batch path publishes");
          }

          @Override
          public BatchResult publishBatch(
              List<OutboxEntry> entries, Instant deadline, OutboxEntrySpans spans) {
            batches.add(entries);
            var confirmed = new LinkedHashSet<OutboxEntryId>();
            for (OutboxEntry entry : entries) {
              try (TraceScope hand = spans.start(entry)) {
                hand.recordSuccess();
              }
              confirmed.add(entry.id());
            }
            return new BatchResult(confirmed, Map.of());
          }
        };

    poller(pipelining, tracing()).pollCycle();

    assertThat(batches).hasSize(1);
    SpanData relay = spans(TraceAttributes.OUTBOX_RELAY).getFirst();
    assertThat(spans(TraceAttributes.OUTBOX_PUBLISH))
        .hasSize(2)
        .allSatisfy(publish -> assertThat(publish.getParentSpanId()).isEqualTo(relay.getSpanId()));
  }

  @Test
  void aPublisherThatDelegates_passesTheSpansOn_andEachEntryGetsOneSpan() {
    save("e-1");
    save("e-2");
    List<OutboxEntry> published = new ArrayList<>();
    OutboxPublisher inner = published::add;
    OutboxPublisher wrapper =
        new OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) throws Exception {
            inner.publish(entry);
          }

          @Override
          public BatchResult publishBatch(
              List<OutboxEntry> entries, Instant deadline, OutboxEntrySpans spans) {
            return inner.publishBatch(entries, deadline, spans);
          }
        };

    poller(wrapper, tracing()).pollCycle();

    assertThat(published).hasSize(2);
    assertThat(spans(TraceAttributes.OUTBOX_PUBLISH)).hasSize(2);
  }

  @Test
  void none_startsNothing_andTheDefaultBatchPublishesTheSame() {
    save("e-1");
    OutboxEntry entry = store.loadPending(10).getFirst();
    List<OutboxEntry> published = new ArrayList<>();
    OutboxPublisher publisher = published::add;

    assertThat(OutboxEntrySpans.NONE.start(entry)).isSameAs(TraceScope.NOOP);
    var result = publisher.publishBatch(List.of(entry), null, OutboxEntrySpans.NONE);

    assertThat(result.confirmed()).containsExactly(entry.id());
    assertThat(published).containsExactly(entry);
    assertThat(otelTesting.getSpans()).isEmpty();
  }

  /** A tracing that throws from {@code start}, or whose scopes throw from every method. */
  private static StreamRuneTracing throwingTracing(boolean fromStart) {
    TraceScope throwingScope =
        new TraceScope() {
          @Override
          public void setAttribute(String key, String value) {
            throw new IllegalStateException("setAttribute");
          }

          @Override
          public void setAttribute(String key, long value) {
            throw new IllegalStateException("setAttribute");
          }

          @Override
          public void setAttribute(String key, boolean value) {
            throw new IllegalStateException("setAttribute");
          }

          @Override
          public void recordSuccess() {
            throw new IllegalStateException("recordSuccess");
          }

          @Override
          public void recordError(Throwable error) {
            throw new IllegalStateException("recordError");
          }

          @Override
          public void close() {
            throw new IllegalStateException("close");
          }
        };
    return new StreamRuneTracing() {
      @Override
      public boolean enabled(TracingArea area) {
        return true;
      }

      @Override
      public TraceScope start(
          TracingArea area,
          String spanName,
          org.streamrune.core.tracing.SpanKind kind,
          org.streamrune.core.tracing.SpanAttributes attributes,
          org.streamrune.core.tracing.SpanLink link,
          SpanBaggage baggage) {
        if (fromStart) {
          throw new IllegalStateException("start");
        }
        return throwingScope;
      }
    };
  }

  /** A publisher with a batch path of its own: one span per hand-off, as the RabbitMQ one. */
  private static OutboxPublisher pipelining(List<OutboxEntry> published) {
    return new OutboxPublisher() {
      @Override
      public void publish(OutboxEntry entry) {
        throw new AssertionError("the batch path publishes");
      }

      @Override
      public BatchResult publishBatch(
          List<OutboxEntry> entries, Instant deadline, OutboxEntrySpans spans) {
        var confirmed = new LinkedHashSet<OutboxEntryId>();
        for (OutboxEntry entry : entries) {
          try (TraceScope hand = spans.start(entry)) {
            published.add(entry);
            hand.recordSuccess();
          }
          confirmed.add(entry.id());
        }
        return new BatchResult(confirmed, Map.of());
      }
    };
  }

  @Test
  void aTracingThatThrows_neverStopsADelivery() {
    for (boolean fromStart : List.of(true, false)) {
      for (boolean ownBatchPath : List.of(false, true)) {
        save("a-" + fromStart + ownBatchPath);
        save("b-" + fromStart + ownBatchPath);
        save("c-" + fromStart + ownBatchPath);
        List<OutboxEntry> published = new ArrayList<>();
        OutboxPublisher publisher =
            ownBatchPath
                ? pipelining(published)
                : entry -> {
                  published.add(entry);
                  if (entry.id().value().startsWith("b-")) {
                    throw new IllegalStateException("refused");
                  }
                };

        poller(publisher, throwingTracing(fromStart)).pollCycle();

        assertThat(published)
            .as("start throws: %s, own batch path: %s", fromStart, ownBatchPath)
            .hasSize(3);
      }
    }
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
      poller(entry -> {}, off).pollCycle();
    }

    assertThat(otelTesting.getSpans()).isEmpty();
  }
}
