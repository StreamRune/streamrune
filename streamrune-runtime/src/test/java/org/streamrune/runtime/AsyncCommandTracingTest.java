package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.test.InMemoryEventStore;

/**
 * An {@code executeAsync} command continues the caller's trace: its span, and the span of its lock
 * under it, are in the trace of the span current on the submitting thread, as a synchronous
 * command's are, with no {@code asyncTaskWrapper}.
 */
class AsyncCommandTracingTest {

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder()
          .registerEvent(Switched.class)
          .registerState("Lamp", Lamp.class)
          .build();

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  record Switch(String lampId) implements Command {}

  record Switched() implements DomainEvent {}

  record Lamp() implements AggregateState {}

  static final class LampDecider implements Decider<Switch, Lamp, Switched> {
    @Override
    public Lamp initialState() {
      return new Lamp();
    }

    @Override
    public List<Switched> decide(Switch command, Lamp state) {
      return List.of(new Switched());
    }

    @Override
    public Lamp evolve(Lamp state, Switched event) {
      return state;
    }
  }

  private static StreamRuneTracing tracing() {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled());
  }

  private static VirtualThreadCommandBus.Builder lampBus() {
    return VirtualThreadCommandBus.builder()
        .eventStore(new InMemoryEventStore(TYPES))
        .register(
            AggregateType.of("lamp"),
            Switch.class,
            c -> AggregateId.of(c.lampId()),
            new LampDecider());
  }

  /** Runs one async command under a request span and returns that span. */
  private static SpanData runUnderARequest(VirtualThreadCommandBus bus) throws Exception {
    Span request =
        otelTesting.getOpenTelemetry().getTracer("http").spanBuilder("POST /lamps").startSpan();
    try (Scope ignored = request.makeCurrent()) {
      bus.executeAsync(new Switch("l-async")).get();
    } finally {
      request.end();
    }
    return otelTesting.getSpans().stream()
        .filter(s -> s.getName().equals("POST /lamps"))
        .findFirst()
        .orElseThrow();
  }

  private static SpanData named(String name) {
    return otelTesting.getSpans().stream()
        .filter(s -> s.getName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError(name + " not among " + otelTesting.getSpans()));
  }

  @Test
  void anAsyncCommandOfABusBuiltWithATracing_isAChildOfTheCallersSpan() throws Exception {
    try (var bus = lampBus().tracing(tracing()).build()) {
      SpanData request = runUnderARequest(bus);

      SpanData command = named("command/Switch");
      assertThat(command.getTraceId()).isEqualTo(request.getTraceId());
      assertThat(command.getParentSpanId()).isEqualTo(request.getSpanId());
      assertThat(named(TraceAttributes.LOCK_ACQUIRE).getParentSpanId())
          .isEqualTo(command.getSpanId());
    }
    assertThat(Span.current().getSpanContext().isValid()).isFalse();
  }

  @Test
  void anInterceptorTheApplicationRegistered_carriesTheCallersTraceToo() throws Exception {
    try (var bus = lampBus().interceptors(new TracingCommandInterceptor(tracing())).build()) {
      SpanData request = runUnderARequest(bus);

      assertThat(named("command/Switch").getParentSpanId()).isEqualTo(request.getSpanId());
    }
  }

  @Test
  void aSpanTheAsyncTaskWrapperOpens_nestsUnderTheCaller_andTheCommandUnderIt() throws Exception {
    io.opentelemetry.api.trace.Tracer wrapperTracer =
        otelTesting.getOpenTelemetry().getTracer("wrapper");
    // The command's future completes inside task.run(), before the wrapper ends its own span:
    // the test waits for the wrapper to have ended it, not only for the future.
    java.util.concurrent.CountDownLatch dispatchEnded = new java.util.concurrent.CountDownLatch(1);
    try (var bus =
        lampBus()
            .tracing(tracing())
            .asyncTaskWrapper(
                task ->
                    () -> {
                      Span dispatch = wrapperTracer.spanBuilder("async dispatch").startSpan();
                      try (Scope ignored = dispatch.makeCurrent()) {
                        task.run();
                      } finally {
                        dispatch.end();
                        dispatchEnded.countDown();
                      }
                    })
            .build()) {
      SpanData request = runUnderARequest(bus);
      assertThat(dispatchEnded.await(10, java.util.concurrent.TimeUnit.SECONDS))
          .as("the wrapper ended its span")
          .isTrue();

      SpanData dispatch = named("async dispatch");
      assertThat(dispatch.getParentSpanId()).isEqualTo(request.getSpanId());
      assertThat(named("command/Switch").getParentSpanId()).isEqualTo(dispatch.getSpanId());
    }
  }

  @Test
  void aWrapperThatMakesTheRootContextCurrent_putsTheCommandInATraceOfItsOwn() throws Exception {
    try (var bus =
        lampBus()
            .tracing(tracing())
            .asyncTaskWrapper(task -> io.opentelemetry.context.Context.root().wrap(task))
            .build()) {
      SpanData request = runUnderARequest(bus);

      SpanData command = named("command/Switch");
      assertThat(command.getParentSpanContext().isValid()).isFalse();
      assertThat(command.getTraceId()).isNotEqualTo(request.getTraceId());
    }
  }

  @Test
  void theSeamsDefault_returnsTheTaskItself() {
    Runnable task = () -> {};
    StreamRuneTracing custom =
        new StreamRuneTracing() {
          @Override
          public boolean enabled(org.streamrune.core.tracing.TracingArea area) {
            return false;
          }

          @Override
          public org.streamrune.core.tracing.TraceScope start(
              org.streamrune.core.tracing.TracingArea area,
              String spanName,
              org.streamrune.core.tracing.SpanKind kind,
              org.streamrune.core.tracing.SpanAttributes attributes,
              org.streamrune.core.tracing.SpanLink link,
              org.streamrune.core.tracing.SpanBaggage baggage) {
            return org.streamrune.core.tracing.TraceScope.NOOP;
          }
        };

    assertThat(custom.propagating(task)).isSameAs(task);
    assertThat(StreamRuneTracing.NOOP.propagating(task)).isSameAs(task);
  }
}
