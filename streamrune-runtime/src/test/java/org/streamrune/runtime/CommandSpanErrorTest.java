package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.test.InMemoryEventStore;

/**
 * An {@link Error} that leaves a command — from its decider, from an interceptor's {@code before()}
 * or {@code after()} — ends the command's span, restores the trace context that was current on the
 * calling thread, gives every interceptor whose {@code before()} completed exactly one callback,
 * and propagates as it is.
 */
class CommandSpanErrorTest {

  private static final AggregateType LAMP = AggregateType.of("lamp");
  private static final String COMMAND_SPAN = TraceAttributes.commandSpanName("Switch");

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  record Switched(String lampId) implements DomainEvent {}

  record Lamp() implements AggregateState {}

  record Switch(String lampId, boolean error) implements Command {}

  static final class LampDecider implements Decider<Switch, Lamp, Switched> {
    @Override
    public Lamp initialState() {
      return new Lamp();
    }

    @Override
    public List<Switched> decide(Switch command, Lamp state) {
      if (command.error()) {
        throw new NoClassDefFoundError("a class of the decider is missing");
      }
      return List.of(new Switched(command.lampId()));
    }

    @Override
    public Lamp evolve(Lamp state, Switched event) {
      return state;
    }
  }

  /** Records the callbacks it receives. */
  static final class Recording implements CommandInterceptor {
    final List<String> calls = new ArrayList<>();

    @Override
    public boolean before(CommandContext ctx) {
      calls.add("before");
      return true;
    }

    @Override
    public void after(CommandContext ctx) {
      calls.add("after");
    }

    @Override
    public void onError(CommandContext ctx, Throwable error) {
      calls.add("onError:" + error.getClass().getSimpleName());
    }
  }

  private static StreamRuneTracing tracing() {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled());
  }

  private static VirtualThreadCommandBus bus(CommandInterceptor... interceptors) {
    VirtualThreadCommandBus.Builder builder =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .tracing(tracing());
    for (CommandInterceptor interceptor : interceptors) {
      builder.interceptor(interceptor);
    }
    return builder.build();
  }

  private static List<SpanData> commandSpans() {
    return otelTesting.getSpans().stream().filter(s -> s.getName().equals(COMMAND_SPAN)).toList();
  }

  @Test
  void anErrorFromTheDecider_endsTheCommandSpan_andLeavesNoSpanCurrent() {
    Recording recording = new Recording();
    try (VirtualThreadCommandBus bus = bus(recording)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true)))
          .isInstanceOf(NoClassDefFoundError.class)
          .hasMessage("a class of the decider is missing");

      assertThat(Span.current().getSpanContext().isValid())
          .as("no span is current on the calling thread after the Error")
          .isFalse();
      assertThat(commandSpans()).as("the command span ended").hasSize(1);
      assertThat(commandSpans().getFirst().getStatus().getStatusCode()).isEqualTo(StatusCode.ERROR);
      assertThat(recording.calls).containsExactly("before", "onError:NoClassDefFoundError");

      // The next command on this thread starts a trace of its own.
      otelTesting.clearSpans();
      bus.execute(new Switch("l-2", false));
      assertThat(commandSpans().getFirst().getParentSpanContext().isValid()).isFalse();
    }
  }

  @Test
  void anErrorInsideARequestsSpan_leavesTheRequestsOwnContextRestorable() {
    try (VirtualThreadCommandBus bus = bus()) {
      Span request1 =
          otelTesting.getOpenTelemetry().getTracer("http").spanBuilder("r1").startSpan();
      try (io.opentelemetry.context.Scope ignored = request1.makeCurrent()) {
        assertThatThrownBy(() -> bus.execute(new Switch("l-1", true)))
            .isInstanceOf(NoClassDefFoundError.class);
        assertThat(Span.current()).as("the request's span is current again").isSameAs(request1);
      } finally {
        request1.end();
      }
      assertThat(Span.current().getSpanContext().isValid()).isFalse();

      // A later request on the same thread does not join the first one's trace.
      Span request2 =
          otelTesting.getOpenTelemetry().getTracer("http").spanBuilder("r2").startSpan();
      request2.end();
      SpanData r1 = named("r1");
      SpanData r2 = named("r2");
      assertThat(r2.getTraceId()).isNotEqualTo(r1.getTraceId());
      assertThat(r2.getParentSpanContext().isValid()).isFalse();
    }
  }

  private static SpanData named(String name) {
    return otelTesting.getSpans().stream()
        .filter(s -> s.getName().equals(name))
        .findFirst()
        .orElseThrow();
  }

  @Test
  void anErrorFromALaterInterceptorsBefore_endsTheCommandSpan() {
    CommandInterceptor failing =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            throw new StackOverflowError("in a gate");
          }
        };
    Recording recording = new Recording();
    try (VirtualThreadCommandBus bus = bus(recording, failing)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", false)))
          .isInstanceOf(StackOverflowError.class);

      assertThat(Span.current().getSpanContext().isValid()).isFalse();
      assertThat(commandSpans()).hasSize(1);
      assertThat(recording.calls).containsExactly("before", "onError:StackOverflowError");
    }
  }

  @Test
  void anErrorFromAnInterceptorsAfter_stillGivesTheOthersTheirAfter_once() {
    CommandInterceptor failing =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            throw new AssertionError("an inline projection failed an assertion");
          }
        };
    Recording recording = new Recording();
    // after() runs in reverse order: the failing interceptor's first, then the recording one's
    // and the command span's.
    try (VirtualThreadCommandBus bus = bus(recording, failing)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", false)))
          .isInstanceOf(AssertionError.class)
          .hasMessage("an inline projection failed an assertion");

      assertThat(Span.current().getSpanContext().isValid()).isFalse();
      assertThat(commandSpans()).hasSize(1);
      assertThat(commandSpans().getFirst().getStatus().getStatusCode())
          .as("the command committed: its span is not a failed one")
          .isEqualTo(StatusCode.OK);
      assertThat(recording.calls)
          .as("after() once, and no onError() for a command whose after phase ran")
          .containsExactly("before", "after");
    }
  }

  @Test
  void anErrorFromAnInterceptorsOnError_stillGivesTheOthersTheirOnError() {
    CommandInterceptor failing =
        new CommandInterceptor() {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            throw new AssertionError("onError failed an assertion");
          }
        };
    Recording recording = new Recording();
    try (VirtualThreadCommandBus bus = bus(recording, failing)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true))).isInstanceOf(Error.class);

      assertThat(Span.current().getSpanContext().isValid()).isFalse();
      assertThat(commandSpans()).hasSize(1);
      assertThat(recording.calls).containsExactly("before", "onError:NoClassDefFoundError");
    }
  }

  @Test
  void aComposedInterceptorGivesBothMembersTheirCallbackWhenOneThrowsAnError() {
    Recording first = new Recording();
    CommandInterceptor second =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            throw new AssertionError("second.after");
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            throw new AssertionError("second.onError");
          }
        };
    CommandInterceptor composed = CommandInterceptor.compose(first, second);

    assertThatThrownBy(() -> composed.after(null)).hasMessage("second.after");
    assertThatThrownBy(() -> composed.onError(null, new IllegalStateException()))
        .hasMessage("second.onError");
    assertThat(first.calls).containsExactly("after", "onError:IllegalStateException");

    Recording notified = new Recording();
    CommandInterceptor failingBefore =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            throw new AssertionError("second.before");
          }
        };
    assertThatThrownBy(() -> CommandInterceptor.compose(notified, failingBefore).before(null))
        .hasMessage("second.before");
    assertThat(notified.calls).containsExactly("before", "onError:AssertionError");
  }
}
