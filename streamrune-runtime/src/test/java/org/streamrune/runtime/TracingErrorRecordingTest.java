package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.EventData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.tracing.SpanAttributes;
import org.streamrune.core.tracing.SpanKind;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceScope;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.test.InMemoryEventStore;

/**
 * What {@link OpenTelemetryTracing} records for an error — the exception's type, its own message
 * and a stack trace that names each cause and suppressed exception by its type alone — for every
 * span, the command span included; and the start time a span can be given.
 */
class TracingErrorRecordingTest {

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder()
          .registerEvent(Switched.class)
          .registerState("LampState", LampState.class)
          .build();

  private static final String MARKER = "payload-value-4711";

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private static StreamRuneTracing tracing() {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled());
  }

  private static RuntimeException failureQuoting(String marker) {
    var cause = new IllegalArgumentException("bad value " + marker, new IOException(marker));
    var failure = new IllegalStateException("the append failed", cause);
    failure.addSuppressed(new RuntimeException("cleanup saw " + marker));
    return failure;
  }

  private static EventData exceptionEvent(SpanData span) {
    List<EventData> events = span.getEvents();
    assertThat(events).extracting(EventData::getName).containsExactly("exception");
    return events.getFirst();
  }

  @Test
  void anError_recordsItsTypeAndOwnMessage_andNoMessageOfItsCauses() {
    try (TraceScope scope =
        tracing()
            .start(
                TracingArea.EVENT_STORE,
                "eventstore/append",
                SpanKind.INTERNAL,
                SpanAttributes.empty())) {
      scope.recordError(failureQuoting(MARKER));
    }

    SpanData span = otelTesting.getSpans().getFirst();
    EventData event = exceptionEvent(span);
    assertThat(event.getAttributes().get(AttributeKey.stringKey("exception.type")))
        .isEqualTo(IllegalStateException.class.getName());
    assertThat(event.getAttributes().get(AttributeKey.stringKey("exception.message")))
        .isEqualTo("the append failed");
    String trace = event.getAttributes().get(AttributeKey.stringKey("exception.stacktrace"));
    assertThat(trace)
        .startsWith(IllegalStateException.class.getName() + ": the append failed\n")
        .contains("\tat ")
        .contains("Caused by: " + IllegalArgumentException.class.getName() + "\n")
        .contains("Caused by: " + IOException.class.getName() + "\n")
        .contains("\tSuppressed: " + RuntimeException.class.getName() + "\n")
        .doesNotContain(MARKER);
    assertThat(span.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    assertThat(span.getStatus().getDescription()).isEqualTo("the append failed");
    assertThat(span.toString()).doesNotContain(MARKER);
  }

  @Test
  void aMessageTheCauseOnlyConstructorMade_isLeftOut_atEveryLevel() {
    for (Throwable failure :
        List.<Throwable>of(
            new RuntimeException(new IOException(MARKER)),
            new IllegalStateException(
                "the publish failed", new RuntimeException(new IOException(MARKER))),
            new java.util.concurrent.CompletionException(
                new java.io.UncheckedIOException(new IOException(MARKER))))) {
      otelTesting.clearSpans();
      try (TraceScope scope =
          tracing()
              .start(
                  TracingArea.OUTBOX,
                  "outbox/publish",
                  SpanKind.PRODUCER,
                  SpanAttributes.empty())) {
        scope.recordError(failure);
      }

      SpanData span = otelTesting.getSpans().getFirst();
      EventData event = exceptionEvent(span);
      assertThat(span.toString()).as(failure.getClass().getName()).doesNotContain(MARKER);
      assertThat(event.getAttributes().get(AttributeKey.stringKey("exception.stacktrace")))
          .startsWith(failure.getClass().getName())
          .doesNotContain(MARKER);
    }
    assertThat(OpenTelemetryTracing.ownMessage(new RuntimeException(new IOException(MARKER))))
        .isNull();
    assertThat(OpenTelemetryTracing.ownMessage(new RuntimeException("own", new IOException("x"))))
        .isEqualTo("own");
  }

  @Test
  void anOwnMessage_isSanitizedAndBounded_inTheEvent_theStackTrace_andTheStatus() {
    String echoed =
        "Coupon not valid: " + "x".repeat(5000) + "\r\nforged line\u202Ereversed\u0000end";

    try (TraceScope scope =
        tracing()
            .start(
                TracingArea.COMMAND, "command/Redeem", SpanKind.INTERNAL, SpanAttributes.empty())) {
      scope.recordError(new IllegalStateException(echoed));
    }

    SpanData span = otelTesting.getSpans().getFirst();
    EventData event = exceptionEvent(span);
    String message = event.getAttributes().get(AttributeKey.stringKey("exception.message"));
    assertThat(message)
        .startsWith("Coupon not valid: xxx")
        .endsWith("...")
        .hasSize(LogSanitizer.MAX_TRACE_LENGTH + 3)
        .doesNotContain("\r", "\n", "\u202E", "\u0000");
    assertThat(span.getStatus().getDescription()).isEqualTo(message);
    String trace = event.getAttributes().get(AttributeKey.stringKey("exception.stacktrace"));
    assertThat(trace)
        .startsWith(IllegalStateException.class.getName() + ": " + message + "\n\tat ");

    assertThat(
            OpenTelemetryTracing.tracedMessage(
                new IllegalStateException("line one\r\n\r\nline two\u202E")))
        .isEqualTo("line one line two");
    assertThat(OpenTelemetryTracing.tracedMessage(new IllegalStateException())).isNull();
  }

  @Test
  void aCyclicCauseChain_endsTheStackTrace() {
    var first = new IllegalStateException("first");
    var second = new IllegalStateException("second " + MARKER, first);
    first.initCause(second);

    String trace = OpenTelemetryTracing.stackTraceWithoutCauseMessages(first);

    assertThat(trace).contains("(already shown)").doesNotContain(MARKER);
  }

  record Switch(String lampId) implements Command {}

  record Switched() implements DomainEvent {}

  record LampState() implements AggregateState {}

  @Test
  void theCommandSpan_recordsNoMessageOfACause() {
    Decider<Switch, LampState, Switched> failing =
        new Decider<>() {
          @Override
          public LampState initialState() {
            return new LampState();
          }

          @Override
          public List<Switched> decide(Switch command, LampState state) {
            throw failureQuoting(MARKER);
          }

          @Override
          public LampState evolve(LampState state, Switched event) {
            return state;
          }
        };
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore(TYPES))
            .register(
                AggregateType.of("lamp"), Switch.class, c -> AggregateId.of(c.lampId()), failing)
            .tracing(tracing())
            .build()) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1"))).isInstanceOf(RuntimeException.class);
    }

    SpanData command =
        otelTesting.getSpans().stream()
            .filter(s -> s.getName().equals("command/Switch"))
            .findFirst()
            .orElseThrow();
    assertThat(command.getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
    assertThat(command.getEvents()).isNotEmpty();
    assertThat(command.toString()).doesNotContain(MARKER);
  }

  @Test
  void aSpanGivenAStartTime_startsThen_andASwitchedOffAreaStartsNone() {
    Instant started = Instant.parse("2026-11-11T10:00:00.123456789Z");

    try (TraceScope scope =
        tracing()
            .start(
                TracingArea.EVENT_STORE,
                "eventstore/read",
                SpanKind.INTERNAL,
                SpanAttributes.empty(),
                started)) {
      scope.recordSuccess();
    }
    TraceScope off =
        OpenTelemetryTracing.of(
                otelTesting.getOpenTelemetry(),
                TracingSettings.builder().area(TracingArea.EVENT_STORE, false).build())
            .start(
                TracingArea.EVENT_STORE,
                "eventstore/read",
                SpanKind.INTERNAL,
                SpanAttributes.empty(),
                started);

    assertThat(off).isSameAs(TraceScope.NOOP);
    SpanData span = otelTesting.getSpans().getFirst();
    assertThat(span.getStartEpochNanos())
        .isEqualTo(started.getEpochSecond() * 1_000_000_000L + started.getNano());
    assertThat(
            StreamRuneTracing.NOOP.start(
                TracingArea.EVENT_STORE, "x", SpanKind.INTERNAL, SpanAttributes.empty(), started))
        .isSameAs(TraceScope.NOOP);
  }
}
