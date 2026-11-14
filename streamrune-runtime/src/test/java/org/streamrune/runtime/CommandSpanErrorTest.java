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

  private static final org.streamrune.core.EventTypeRegistry TYPES =
      org.streamrune.core.SimpleEventTypeRegistry.builder()
          .registerEvent("Switched", Switched.class)
          .registerState("Lamp", Lamp.class)
          .build();

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
    return VirtualThreadCommandBus.builder()
        .eventStore(new InMemoryEventStore(TYPES))
        .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
        .tracing(tracing())
        .interceptors(interceptors)
        .build();
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
      // The work's own Error is the one that propagates, with the callback's suppressed on it.
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true)))
          .isInstanceOf(NoClassDefFoundError.class)
          .hasMessage("a class of the decider is missing")
          .satisfies(
              thrown -> {
                assertThat(thrown.getSuppressed()).hasSize(1);
                assertThat(thrown.getSuppressed()[0])
                    .isInstanceOf(AssertionError.class)
                    .hasMessage("onError failed an assertion");
              });

      assertThat(Span.current().getSpanContext().isValid()).isFalse();
      assertThat(commandSpans()).hasSize(1);
      assertThat(recording.calls).containsExactly("before", "onError:NoClassDefFoundError");
    }
  }

  /** Counts the commands the bus reports failed and succeeded. */
  static final class CountingMetrics implements org.streamrune.core.StreamRuneMetrics {
    final java.util.concurrent.atomic.AtomicInteger failed =
        new java.util.concurrent.atomic.AtomicInteger();
    final java.util.concurrent.atomic.AtomicInteger succeeded =
        new java.util.concurrent.atomic.AtomicInteger();

    @Override
    public void recordCommandFailed() {
      failed.incrementAndGet();
    }

    @Override
    public void recordCommandSucceeded() {
      succeeded.incrementAndGet();
    }
  }

  private static VirtualThreadCommandBus bus(
      CountingMetrics metrics, CommandInterceptor... interceptors) {
    return VirtualThreadCommandBus.builder()
        .eventStore(new InMemoryEventStore(TYPES))
        .register(
            LAMP,
            Switch.class,
            c -> {
              if (c.lampId().equals("no-id")) {
                throw new NoClassDefFoundError("a class of the id extractor is missing");
              }
              return AggregateId.of(c.lampId());
            },
            new LampDecider())
        .metrics(metrics)
        .interceptors(interceptors)
        .build();
  }

  @Test
  void everyErrorThatLeavesAnUncommittedCommandCountsOneFailedCommand() {
    CommandInterceptor onErrorThrows =
        new CommandInterceptor() {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            throw new AssertionError("onError failed an assertion");
          }
        };
    CountingMetrics metrics = new CountingMetrics();
    try (VirtualThreadCommandBus bus = bus(metrics, onErrorThrows)) {
      // The work's Error, with an onError() that throws an Error of its own.
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true)))
          .isInstanceOf(NoClassDefFoundError.class);
      assertThat(metrics.failed.get()).isEqualTo(1);

      // An Error before the interceptor chain started.
      assertThatThrownBy(() -> bus.execute(new Switch("no-id", false)))
          .isInstanceOf(NoClassDefFoundError.class)
          .hasMessage("a class of the id extractor is missing");
      assertThat(metrics.failed.get()).isEqualTo(2);
      assertThat(metrics.succeeded.get()).isZero();
    }
  }

  @Test
  void anExceptionOfTheWorkIsCarriedByTheErrorOfAnOnErrorCallback_andCountedOnce() {
    IllegalStateException infrastructure = new IllegalStateException("the store is down");
    CommandInterceptor failingBefore =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            throw infrastructure;
          }
        };
    CommandInterceptor onErrorThrows =
        new CommandInterceptor() {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            throw new AssertionError("onError failed an assertion");
          }
        };
    CountingMetrics metrics = new CountingMetrics();
    try (VirtualThreadCommandBus bus = bus(metrics, onErrorThrows, failingBefore)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", false)))
          .isInstanceOf(AssertionError.class)
          .hasMessage("onError failed an assertion")
          .satisfies(thrown -> assertThat(thrown.getSuppressed()).containsExactly(infrastructure));
      assertThat(metrics.failed.get()).isEqualTo(1);
    }
  }

  @Test
  void anOnErrorCallbackThatRethrowsTheErrorItWasGivenSuppressesNothing() {
    CommandInterceptor rethrows =
        new CommandInterceptor() {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            throw (Error) error;
          }
        };
    try (VirtualThreadCommandBus bus = bus(rethrows)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true)))
          .isInstanceOf(NoClassDefFoundError.class)
          .satisfies(thrown -> assertThat(thrown.getSuppressed()).isEmpty());
    }
  }

  @Test
  void aCommittedCommandWhoseAfterThrowsAnErrorIsNotCountedFailed() {
    CommandInterceptor failing =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            throw new AssertionError("after failed an assertion");
          }
        };
    CountingMetrics metrics = new CountingMetrics();
    try (VirtualThreadCommandBus bus = bus(metrics, failing)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", false)))
          .isInstanceOf(AssertionError.class);
      assertThat(metrics.failed.get()).isZero();
    }
  }

  // ── a checked exception a callback lets out without declaring it ───────────────────────────

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
    throw (T) t;
  }

  @Test
  void anUndeclaredCheckedExceptionFromALaterBefore_endsTheCommandSpan_andIsCountedFailed() {
    CommandInterceptor failing =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            sneakyThrow(new java.sql.SQLException("the gate's database is down"));
            return true;
          }
        };
    Recording recording = new Recording();
    CountingMetrics metrics = new CountingMetrics();
    try (VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore(TYPES))
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .tracing(tracing())
            .metrics(metrics)
            .interceptors(recording, failing)
            .build()) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", false)))
          .as("wrapped, as the decider's checked exception is")
          .isInstanceOf(CommandExecutionException.class)
          .hasCauseInstanceOf(java.sql.SQLException.class);

      assertThat(Span.current().getSpanContext().isValid()).isFalse();
      assertThat(commandSpans()).as("the command span ended").hasSize(1);
      assertThat(recording.calls)
          .as("onError() is given the exception the caller gets")
          .containsExactly("before", "onError:CommandExecutionException");
      assertThat(metrics.failed.get()).isEqualTo(1);
    }
  }

  @Test
  void anInterruptedExceptionLetOutOfBefore_leavesTheInterruptStatusSet() {
    CommandInterceptor failing =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            sneakyThrow(new InterruptedException("interrupted in a gate"));
            return true;
          }
        };
    try (VirtualThreadCommandBus bus = bus(failing)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", false)))
          .isInstanceOf(CommandExecutionException.class)
          .hasCauseInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  @Test
  void anUndeclaredCheckedExceptionOfTheIdExtractorIsWrappedAndCountedFailed() {
    CountingMetrics metrics = new CountingMetrics();
    try (VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore(TYPES))
            .register(
                LAMP,
                Switch.class,
                c -> {
                  sneakyThrow(new java.io.IOException("the id could not be read"));
                  return AggregateId.of(c.lampId());
                },
                new LampDecider())
            .metrics(metrics)
            .build()) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", false)))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Switch")
          .hasCauseInstanceOf(java.io.IOException.class);
      assertThat(metrics.failed.get()).isEqualTo(1);
    }
  }

  @Test
  void anUndeclaredCheckedExceptionFromAnAfter_stillGivesTheOthersTheirAfter() {
    CommandInterceptor failing =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            sneakyThrow(new java.io.IOException("the read model is down"));
          }
        };
    Recording recording = new Recording();
    try (VirtualThreadCommandBus bus = bus(recording, failing)) {
      // The command committed; an exception of an after() callback is logged, as always.
      assertThat(bus.execute(new Switch("l-1", false)).events()).hasSize(1);

      assertThat(Span.current().getSpanContext().isValid()).isFalse();
      assertThat(commandSpans()).hasSize(1);
      assertThat(recording.calls).containsExactly("before", "after");
    }
  }

  @Test
  void anUndeclaredCheckedExceptionFromAnOnError_doesNotReplaceTheWorksError() {
    CommandInterceptor failing =
        new CommandInterceptor() {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            sneakyThrow(new java.io.IOException("the audit store is down"));
          }
        };
    Recording recording = new Recording();
    try (VirtualThreadCommandBus bus = bus(recording, failing)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true)))
          .isInstanceOf(NoClassDefFoundError.class)
          .hasMessage("a class of the decider is missing");

      assertThat(Span.current().getSpanContext().isValid()).isFalse();
      assertThat(commandSpans()).hasSize(1);
      assertThat(recording.calls).containsExactly("before", "onError:NoClassDefFoundError");
    }
  }

  @Test
  void aComposedInterceptorHandlesAMembersUndeclaredCheckedException() {
    Recording first = new Recording();
    CommandInterceptor second =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            sneakyThrow(new java.io.IOException("second.before"));
            return true;
          }

          @Override
          public void after(CommandContext ctx) {
            sneakyThrow(new java.io.IOException("second.after"));
          }
        };
    CommandInterceptor composed = CommandInterceptor.compose(first, second);

    assertThatThrownBy(() -> composed.after(null)).isInstanceOf(java.io.IOException.class);
    assertThatThrownBy(() -> composed.before(null)).isInstanceOf(java.io.IOException.class);
    assertThat(first.calls).containsExactly("after", "before", "onError:IOException");
  }

  // ── an Error of the metrics backend on the way out ─────────────────────────────────────────

  @Test
  void anErrorOfTheMetricsBackendDoesNotReplaceTheWorksError() {
    org.streamrune.core.StreamRuneMetrics broken =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordCommandFailed() {
            throw new NoClassDefFoundError("a class of the metrics backend is missing");
          }

          @Override
          public void recordCommandDuration(long durationNanos) {
            throw new NoClassDefFoundError("a class of the metrics backend is missing (duration)");
          }
        };
    try (VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore(TYPES))
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .metrics(broken)
            .build()) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true)))
          .isInstanceOf(NoClassDefFoundError.class)
          .hasMessage("a class of the decider is missing")
          .satisfies(
              thrown ->
                  assertThat(thrown.getSuppressed())
                      .extracting(Throwable::getMessage)
                      .containsExactly(
                          "a class of the metrics backend is missing",
                          "a class of the metrics backend is missing (duration)"));
    }
  }

  @Test
  void theWorksErrorAndACallbacksErrorNeverSuppressEachOther() {
    CommandInterceptor throwsItsOwn =
        new CommandInterceptor() {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            throw new AssertionError("onError failed an assertion");
          }
        };
    CommandInterceptor rethrows =
        new CommandInterceptor() {
          @Override
          public void onError(CommandContext ctx, Throwable error) {
            throw (Error) error;
          }
        };
    // onError() runs in reverse order: throwsItsOwn first, then rethrows.
    try (VirtualThreadCommandBus bus = bus(rethrows, throwsItsOwn)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true)))
          .isInstanceOf(NoClassDefFoundError.class)
          .satisfies(
              thrown -> {
                assertThat(thrown.getSuppressed()).hasSize(1);
                assertThat(thrown.getSuppressed()[0])
                    .isInstanceOf(AssertionError.class)
                    .satisfies(
                        callback ->
                            assertThat(callback.getSuppressed())
                                .as("no cycle: the callback's Error does not carry the work's")
                                .isEmpty());
              });
    }
  }

  // ── the lock between its grant and the caller's ownership ──────────────────────────────────

  @Test
  void anErrorOfTheMetricsBackendRightAfterTheLockWasGrantedReleasesTheLock() throws Exception {
    java.util.concurrent.atomic.AtomicInteger thrown =
        new java.util.concurrent.atomic.AtomicInteger();
    org.streamrune.core.StreamRuneMetrics breaksOnce =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordLockWait(long durationNanos) {
            if (thrown.getAndIncrement() == 0) {
              throw new NoClassDefFoundError("a class of the metrics backend is missing");
            }
          }
        };
    try (VirtualThreadCommandBus bus =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore(TYPES))
            .lockTimeout(java.time.Duration.ofMillis(300))
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .metrics(breaksOnce)
            .build()) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", false)))
          .isInstanceOf(NoClassDefFoundError.class)
          .hasMessage("a class of the metrics backend is missing");

      // Another thread takes the same aggregate at once: the lock did not stay with this one.
      java.util.concurrent.atomic.AtomicReference<Throwable> other =
          new java.util.concurrent.atomic.AtomicReference<>();
      Thread thread =
          Thread.ofPlatform()
              .start(
                  () -> {
                    try {
                      bus.execute(new Switch("l-1", false));
                    } catch (Throwable t) {
                      other.set(t);
                    }
                  });
      thread.join(10_000);
      assertThat(other.get()).isNull();
      // And this thread keeps no record of a lock it does not hold.
      assertThat(bus.execute(new Switch("l-1", false)).events()).hasSize(1);
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

  @Test
  void aComposedInterceptorNeverDemotesAnErrorToASuppressedException() {
    // after() and onError() run second's callback first: it throws an exception, first's an Error.
    CommandInterceptor first =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            throw new OutOfMemoryError("first.after");
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            throw new OutOfMemoryError("first.onError");
          }
        };
    CommandInterceptor second =
        new CommandInterceptor() {
          @Override
          public boolean before(CommandContext ctx) {
            throw new IllegalStateException("second.before");
          }

          @Override
          public void after(CommandContext ctx) {
            throw new IllegalStateException("second.after");
          }

          @Override
          public void onError(CommandContext ctx, Throwable error) {
            throw new IllegalStateException("second.onError");
          }
        };
    CommandInterceptor composed = CommandInterceptor.compose(first, second);

    assertThatThrownBy(() -> composed.after(null))
        .isInstanceOf(OutOfMemoryError.class)
        .hasMessage("first.after")
        .satisfies(
            thrown ->
                assertThat(thrown.getSuppressed())
                    .singleElement()
                    .isInstanceOf(IllegalStateException.class));
    assertThatThrownBy(() -> composed.onError(null, new IllegalArgumentException()))
        .isInstanceOf(OutOfMemoryError.class)
        .hasMessage("first.onError");
    assertThatThrownBy(() -> composed.before(null))
        .as("second.before failed, then first.onError threw an Error")
        .isInstanceOf(OutOfMemoryError.class)
        .hasMessage("first.onError");

    // Through the bus the Error reaches the caller: it is not logged and dropped.
    CommandInterceptor quietFirst =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            throw new OutOfMemoryError("first.after");
          }
        };
    CommandInterceptor noisySecond =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            throw new IllegalStateException("second.after");
          }
        };
    try (VirtualThreadCommandBus bus = bus(CommandInterceptor.compose(quietFirst, noisySecond))) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", false)))
          .isInstanceOf(OutOfMemoryError.class)
          .hasMessage("first.after");
    }
  }

  @Test
  void aComposedInterceptorWhoseMembersThrowTheSameThrowableSuppressesNothing() {
    IllegalStateException same = new IllegalStateException("thrown by both");
    CommandInterceptor throwsIt =
        new CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            throw same;
          }
        };
    assertThatThrownBy(() -> CommandInterceptor.compose(throwsIt, throwsIt).after(null))
        .isSameAs(same)
        .satisfies(thrown -> assertThat(thrown.getSuppressed()).isEmpty());
  }
}
