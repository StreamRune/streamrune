package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.sdk.trace.samplers.SamplingResult;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.ThrowingTracing.Fault;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.SagaFixture;

/**
 * A tracing that throws never fails, or changes the outcome of, the work it traces: every traced
 * call site, driven with a {@link StreamRuneTracing} that breaks its contract, and with the
 * OpenTelemetry tracing over an SDK whose sampler or span processor throws.
 */
class TracingFailureIsolationTest {

  private static final AggregateType LAMP = AggregateType.of("lamp");
  private static final StreamId LAMP_1 = StreamId.of(LAMP, AggregateId.of("l-1"));

  record Switched(String lampId) implements DomainEvent {}

  record Lamp() implements AggregateState {}

  record Switch(String lampId, boolean rejected) implements Command {
    Switch(String lampId) {
      this(lampId, false);
    }
  }

  static final class Refused extends DomainException {
    Refused() {
      super("the lamp refuses");
    }
  }

  static final class LampDecider implements Decider<Switch, Lamp, Switched> {
    @Override
    public Lamp initialState() {
      return new Lamp();
    }

    @Override
    public List<Switched> decide(Switch command, Lamp state) {
      if (command.rejected()) {
        throw new Refused();
      }
      return List.of(new Switched(command.lampId()));
    }

    @Override
    public Lamp evolve(Lamp state, Switched event) {
      return state;
    }
  }

  /** The store in the mode that starts the event-store spans. */
  private static InMemoryEventStore serializingStore(StreamRuneTracing tracing) {
    return InMemoryEventStore.serializing(
            SimpleEventTypeRegistry.builder()
                .registerEvent("Switched", Switched.class)
                .registerState("Lamp", Lamp.class)
                .build(),
            null)
        .withTracing(tracing);
  }

  private static VirtualThreadCommandBus bus(InMemoryEventStore store, StreamRuneTracing tracing) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .lockTimeout(Duration.ofMillis(500))
        .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
        .tracing(tracing)
        .build();
  }

  /** The ways a hand-written tracing breaks its contract. */
  static Stream<ThrowingTracing> brokenTracings() {
    return Stream.of(
        ThrowingTracing.on(Fault.START),
        ThrowingTracing.on(Fault.SET_ATTRIBUTE),
        ThrowingTracing.on(Fault.RECORD_SUCCESS),
        ThrowingTracing.on(Fault.RECORD_ERROR),
        ThrowingTracing.on(Fault.LINK),
        ThrowingTracing.on(Fault.CLOSE),
        ThrowingTracing.on(Fault.PROPAGATING),
        ThrowingTracing.onEveryScopeMethod(),
        ThrowingTracing.on(
            Fault.START,
            Fault.PROPAGATING,
            Fault.SET_ATTRIBUTE,
            Fault.RECORD_SUCCESS,
            Fault.RECORD_ERROR,
            Fault.LINK,
            Fault.CLOSE),
        ThrowingTracing.on(Fault.ENABLED),
        ThrowingTracing.throwing(
            () -> new LinkageError("a class of the tracing is missing"), Fault.START, Fault.CLOSE));
  }

  // ── command bus, command span, lock span, event-store spans ────────────────────────────────

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void aCommandSucceeds_itsEventsAreStoredOnce_andTheAggregateStaysUsable(ThrowingTracing broken) {
    InMemoryEventStore store = serializingStore(broken);
    try (VirtualThreadCommandBus bus = bus(store, broken)) {
      for (int i = 0; i < 5; i++) {
        CommandBus.CommandResult result = bus.execute(new Switch("l-1"));
        assertThat(result.events()).hasSize(1);
      }
      assertThat(store.readStream(LAMP_1, Version.initial(), 100)).hasSize(5);
    }
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void aRejectedCommandIsStillRejectedWithTheDomainsException(ThrowingTracing broken) {
    InMemoryEventStore store = serializingStore(broken);
    try (VirtualThreadCommandBus bus = bus(store, broken)) {
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true))).isInstanceOf(Refused.class);
      assertThat(bus.execute(new Switch("l-1")).events()).hasSize(1);
    }
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void anAsynchronousCommandRunsAndCompletesItsFuture(ThrowingTracing broken) throws Exception {
    InMemoryEventStore store = serializingStore(broken);
    try (VirtualThreadCommandBus bus = bus(store, broken)) {
      CommandBus.CommandResult result =
          bus.executeAsync(new Switch("l-1")).get(10, TimeUnit.SECONDS);
      assertThat(result.events()).hasSize(1);
      assertThat(store.readStream(LAMP_1, Version.initial(), 100)).hasSize(1);
    }
  }

  // ── subscription delivery, projection batch ────────────────────────────────────────────────

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void aListenerReceivesItsBatch_andItsOwnFailureIsTheOneThatPropagates(ThrowingTracing broken) {
    List<List<EventEnvelope>> received = new ArrayList<>();
    EventListener traced =
        TracingEventListener.traced(received::add, broken, SubscriptionName.of("notify"));
    assertThatCode(() -> traced.onEvents(List.of())).doesNotThrowAnyException();
    assertThat(received).hasSize(1);

    IllegalStateException own = new IllegalStateException("the listener's own");
    EventListener failing =
        TracingEventListener.traced(
            events -> {
              throw own;
            },
            broken,
            SubscriptionName.of("notify"));
    assertThatThrownBy(() -> failing.onEvents(List.of())).isSameAs(own);
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void aProjectionProcessesItsBatch_andItsOwnFailureIsTheOneThatPropagates(ThrowingTracing broken) {
    AtomicInteger processed = new AtomicInteger();
    Projection traced =
        TracingProjectionDecorator.traced(
            ProjectionName.of("lamps"), batch -> processed.incrementAndGet(), broken);
    assertThatCode(() -> traced.process(List.of())).doesNotThrowAnyException();
    assertThat(processed.get()).isEqualTo(1);

    AssertionError own = new AssertionError("the projection's own");
    Projection failing =
        TracingProjectionDecorator.traced(
            ProjectionName.of("lamps"),
            batch -> {
              throw own;
            },
            broken);
    assertThatThrownBy(() -> failing.process(List.of())).isSameAs(own);
  }

  @Test
  void anErrorOfAProjectionIsRecordedOnItsSpan() {
    io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter exporter =
        io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter.create();
    StreamRuneTracing tracing =
        over(
            SdkTracerProvider.builder()
                .addSpanProcessor(
                    io.opentelemetry.sdk.trace.export.SimpleSpanProcessor.create(exporter))
                .build());
    AssertionError own = new AssertionError("the projection's own");
    Projection failing =
        TracingProjectionDecorator.traced(
            ProjectionName.of("lamps"),
            batch -> {
              throw own;
            },
            tracing);

    assertThatThrownBy(() -> failing.process(List.of())).isSameAs(own);

    assertThat(exporter.getFinishedSpanItems())
        .singleElement()
        .satisfies(
            span -> {
              assertThat(span.getStatus().getStatusCode())
                  .isEqualTo(io.opentelemetry.api.trace.StatusCode.ERROR);
              assertThat(span.getEvents())
                  .extracting(e -> e.getName())
                  .containsExactly("exception");
            });
  }

  // ── saga step, compensation, timeout ───────────────────────────────────────────────────────

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final StreamId ORDER_1 = StreamId.of(ORDER, AggregateId.of("o-1"));
  private static final SagaId SAGA_1 = SagaId.of("order-o-1");

  record OrderState(SagaStatus status, String orderId) implements SagaState {}

  record OrderPlaced(String orderId) implements DomainEvent {}

  record StockReserved(String orderId) implements DomainEvent {}

  record ReserveStock(String orderId) implements Command {}

  record CancelOrder(String orderId) implements Command {}

  static final class OrderSaga implements SagaDecider<OrderState> {
    @Override
    public Class<OrderState> stateType() {
      return OrderState.class;
    }

    @Override
    public OrderState initialState(SagaId sagaId) {
      return new OrderState(SagaStatus.STARTED, null);
    }

    @Override
    public OrderState evolve(OrderState state, EventEnvelope event) {
      return switch (event.event()) {
        case OrderPlaced e -> new OrderState(SagaStatus.RUNNING, e.orderId());
        case StockReserved e -> new OrderState(SagaStatus.COMPLETED, state.orderId());
        default -> state;
      };
    }

    @Override
    public List<SagaCommand> handle(OrderState state, EventEnvelope event) {
      if (event.event() instanceof OrderPlaced e) {
        return List.of(SagaCommand.of(new ReserveStock(e.orderId()), AggregateId.of(e.orderId())));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        OrderState state, Throwable failure, SagaCommand failedCommand) {
      return List.of(
          SagaCommand.of(new CancelOrder(state.orderId()), AggregateId.of(state.orderId())));
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(5));
    }
  }

  private static SagaFixture<OrderState> sagaFixture(StreamRuneTracing tracing, SagaStepLock lock) {
    return SagaFixture.of(
            OrderState.class,
            builder ->
                builder
                    .decider(new OrderSaga())
                    .startWhen(event -> event.event() instanceof OrderPlaced)
                    .extractSagaId(
                        event -> SagaId.of("order-" + ((OrderPlaced) event.event()).orderId()))
                    .stepLock(lock)
                    .tracing(tracing))
        .withTimeouts(builder -> builder.decider(new OrderSaga()).tracing(tracing));
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void aSagaStepsCompletesAndTimesOutAsItWouldUntraced(ThrowingTracing broken) {
    SagaStepLock lock = new SagaStepLock(Duration.ofMillis(400));
    SagaFixture<OrderState> fixture = sagaFixture(broken, lock);

    fixture.when(ORDER_1, new OrderPlaced("o-1")).expectCommands(new ReserveStock("o-1"));
    assertThat(lock.sagasInFlight()).isZero();

    // The deadline passes: the timeout is claimed once, the compensation is dispatched once, and
    // the claim's hold on the saga's step lock is released.
    fixture.advance(Duration.ofMinutes(6)).runTimeouts();
    assertThat(fixture.dispatched())
        .containsExactly(new ReserveStock("o-1"), new CancelOrder("o-1"));
    assertThat(lock.sagasInFlight()).as("the claim hold is released").isZero();
    assertThat(fixture.saga(SAGA_1).orElseThrow().status().isHalted()).isTrue();

    // A second poll finds nothing left to time out.
    fixture.runTimeouts();
    assertThat(fixture.dispatched()).hasSize(2);
  }

  /**
   * The claim of a saga past its deadline takes a hold on the saga's step lock before it starts its
   * span. A span that cannot be started must not leave that hold with the poll thread: the timeout
   * is claimed and compensated once, the hold is released, and a later delivery for the saga, from
   * another thread, is not refused.
   */
  @Test
  void aTimeoutSpanThatCannotBeStarted_leavesNoHoldOnTheSagasStepLock() throws Exception {
    AtomicInteger thrown = new AtomicInteger();
    StreamRuneTracing throwsForTheTimeoutSpan =
        new StreamRuneTracing() {
          @Override
          public boolean enabled(org.streamrune.core.tracing.TracingArea area) {
            return area == org.streamrune.core.tracing.TracingArea.SAGA;
          }

          @Override
          public org.streamrune.core.tracing.TraceScope start(
              org.streamrune.core.tracing.TracingArea area,
              String spanName,
              org.streamrune.core.tracing.SpanKind kind,
              org.streamrune.core.tracing.SpanAttributes attributes,
              org.streamrune.core.tracing.SpanLink link,
              org.streamrune.core.tracing.SpanBaggage baggage) {
            if (spanName.endsWith("/timeout")) {
              thrown.incrementAndGet();
              throw new IllegalStateException("the sampler failed");
            }
            return org.streamrune.core.tracing.TraceScope.NOOP;
          }
        };
    SagaStepLock lock = new SagaStepLock(Duration.ofMillis(400));
    SagaFixture<OrderState> fixture = sagaFixture(throwsForTheTimeoutSpan, lock);
    fixture.when(ORDER_1, new OrderPlaced("o-1"));

    // The poll runs on a thread of its own that lives on, as the runner's poll thread does.
    java.util.concurrent.CountDownLatch polled = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(1);
    Thread poll =
        Thread.ofPlatform()
            .start(
                () -> {
                  try {
                    fixture.advance(Duration.ofMinutes(6)).runTimeouts();
                  } finally {
                    polled.countDown();
                  }
                  try {
                    done.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
    try {
      assertThat(polled.await(10, TimeUnit.SECONDS)).isTrue();
      assertThat(thrown.get()).as("the timeout span's start threw").isPositive();
      assertThat(lock.sagasInFlight()).as("the claim hold is released").isZero();
      assertThat(fixture.dispatched())
          .as("the timeout fired once")
          .containsExactly(new ReserveStock("o-1"), new CancelOrder("o-1"));

      // A late reply for the saga, delivered on this thread, is not refused for the step lock.
      assertThatCode(() -> fixture.replyingTo(SAGA_1, new StockReserved("o-1")))
          .doesNotThrowAnyException();
      // A second poll claims nothing more.
      fixture.runTimeouts();
      assertThat(fixture.dispatched()).hasSize(2);
    } finally {
      done.countDown();
      poll.join(10_000);
    }
  }

  @ParameterizedTest
  @MethodSource("brokenTracings")
  void aSagaThatCompletesIsCompleted(ThrowingTracing broken) {
    SagaFixture<OrderState> fixture = sagaFixture(broken, new SagaStepLock(Duration.ofMillis(400)));

    fixture
        .when(ORDER_1, new OrderPlaced("o-1"))
        .replyingTo(SAGA_1, new StockReserved("o-1"))
        .expectCompleted(SAGA_1);
  }

  // ── the OpenTelemetry tracing over an SDK extension that throws ────────────────────────────

  /** Throws from {@code onStart} and {@code onEnd} of every span. */
  static final class ThrowingProcessor implements SpanProcessor {
    final AtomicInteger thrown = new AtomicInteger();
    private final boolean onStart;
    private final boolean onEnd;

    ThrowingProcessor(boolean onStart, boolean onEnd) {
      this.onStart = onStart;
      this.onEnd = onEnd;
    }

    @Override
    public void onStart(Context parentContext, ReadWriteSpan span) {
      thrown.incrementAndGet();
      throw new IllegalStateException("span processor failed in onStart");
    }

    @Override
    public boolean isStartRequired() {
      return onStart;
    }

    @Override
    public void onEnd(ReadableSpan span) {
      thrown.incrementAndGet();
      throw new IllegalStateException("span processor failed in onEnd");
    }

    @Override
    public boolean isEndRequired() {
      return onEnd;
    }
  }

  private static StreamRuneTracing over(SdkTracerProvider provider) {
    return OpenTelemetryTracing.of(
        OpenTelemetrySdk.builder().setTracerProvider(provider).build(),
        TracingSettings.allEnabled());
  }

  private static void commandsSucceedAndNoContextIsLeft(StreamRuneTracing tracing) {
    InMemoryEventStore store = serializingStore(tracing);
    try (VirtualThreadCommandBus bus = bus(store, tracing)) {
      for (int i = 0; i < 3; i++) {
        assertThat(bus.execute(new Switch("l-1")).events()).hasSize(1);
      }
      assertThatThrownBy(() -> bus.execute(new Switch("l-1", true))).isInstanceOf(Refused.class);
      assertThat(store.readStream(LAMP_1, Version.initial(), 100)).hasSize(3);
      assertThat(Span.current().getSpanContext().isValid())
          .as("no span is left current on the calling thread")
          .isFalse();
    }
  }

  @Test
  void aSpanProcessorWhoseOnEndThrows_failsNoCommand_andLeavesNoContextCurrent() {
    ThrowingProcessor processor = new ThrowingProcessor(false, true);
    commandsSucceedAndNoContextIsLeft(
        over(SdkTracerProvider.builder().addSpanProcessor(processor).build()));
    assertThat(processor.thrown.get()).isPositive();
  }

  @Test
  void aSpanProcessorWhoseOnStartThrows_failsNoCommand_andLeavesNoContextCurrent() {
    ThrowingProcessor processor = new ThrowingProcessor(true, false);
    commandsSucceedAndNoContextIsLeft(
        over(SdkTracerProvider.builder().addSpanProcessor(processor).build()));
    assertThat(processor.thrown.get()).isPositive();
  }

  @Test
  void aSamplerThatThrows_failsNoCommand_andLeavesNoContextCurrent() {
    AtomicInteger thrown = new AtomicInteger();
    Sampler sampler =
        new Sampler() {
          @Override
          public SamplingResult shouldSample(
              Context parentContext,
              String traceId,
              String name,
              io.opentelemetry.api.trace.SpanKind spanKind,
              Attributes attributes,
              List<LinkData> parentLinks) {
            thrown.incrementAndGet();
            throw new IllegalStateException("the sampler failed");
          }

          @Override
          public String getDescription() {
            return "throws";
          }
        };
    commandsSucceedAndNoContextIsLeft(
        over(SdkTracerProvider.builder().setSampler(sampler).build()));
    assertThat(thrown.get()).isPositive();
  }

  /**
   * A span that was started and cannot be made current is ended: nothing else would end it. The
   * OpenTelemetry here hands out spans that refuse to be stored in a context, which fails between
   * {@code startSpan()} and {@code makeCurrent()}.
   */
  @Test
  void aSpanThatCannotBeMadeCurrentIsEnded() {
    SdkTracerProvider sdk = SdkTracerProvider.builder().build();
    AtomicInteger started = new AtomicInteger();
    AtomicInteger ended = new AtomicInteger();
    io.opentelemetry.api.OpenTelemetry openTelemetry =
        new io.opentelemetry.api.OpenTelemetry() {
          @Override
          public io.opentelemetry.api.trace.TracerProvider getTracerProvider() {
            return new io.opentelemetry.api.trace.TracerProvider() {
              @Override
              public io.opentelemetry.api.trace.Tracer get(String name) {
                io.opentelemetry.api.trace.Tracer real = sdk.get(name);
                return spanName -> builder(real.spanBuilder(spanName));
              }

              @Override
              public io.opentelemetry.api.trace.Tracer get(String name, String version) {
                return get(name);
              }
            };
          }

          @Override
          public io.opentelemetry.context.propagation.ContextPropagators getPropagators() {
            return io.opentelemetry.context.propagation.ContextPropagators.noop();
          }

          private io.opentelemetry.api.trace.SpanBuilder builder(
              io.opentelemetry.api.trace.SpanBuilder real) {
            return (io.opentelemetry.api.trace.SpanBuilder)
                java.lang.reflect.Proxy.newProxyInstance(
                    io.opentelemetry.api.trace.SpanBuilder.class.getClassLoader(),
                    new Class<?>[] {io.opentelemetry.api.trace.SpanBuilder.class},
                    (proxy, method, args) -> {
                      Object result = invoke(method, real, args);
                      if (method.getName().equals("startSpan")) {
                        started.incrementAndGet();
                        return span((Span) result);
                      }
                      return result == real ? proxy : result;
                    });
          }

          private Span span(Span real) {
            return (Span)
                java.lang.reflect.Proxy.newProxyInstance(
                    Span.class.getClassLoader(),
                    new Class<?>[] {Span.class},
                    (proxy, method, args) -> {
                      if (method.getName().equals("storeInContext")) {
                        throw new IllegalStateException("the context storage failed");
                      }
                      if (method.getName().equals("end")) {
                        ended.incrementAndGet();
                      }
                      return invoke(method, real, args);
                    });
          }

          private Object invoke(java.lang.reflect.Method method, Object target, Object[] args)
              throws Throwable {
            try {
              return method.invoke(target, args);
            } catch (java.lang.reflect.InvocationTargetException e) {
              throw e.getCause();
            }
          }
        };
    StreamRuneTracing tracing =
        OpenTelemetryTracing.of(openTelemetry, TracingSettings.allEnabled());

    org.streamrune.core.tracing.TraceScope scope =
        tracing.start(
            org.streamrune.core.tracing.TracingArea.COMMAND,
            "command/Switch",
            org.streamrune.core.tracing.SpanKind.INTERNAL,
            org.streamrune.core.tracing.SpanAttributes.empty());
    org.streamrune.core.tracing.TraceScope withBaggage =
        tracing.start(
            org.streamrune.core.tracing.TracingArea.SUBSCRIPTION,
            "subscription/notify/deliver",
            org.streamrune.core.tracing.SpanKind.CONSUMER,
            org.streamrune.core.tracing.SpanAttributes.empty(),
            org.streamrune.core.tracing.SpanLink.NONE,
            org.streamrune.core.tracing.SpanBaggage.replace(java.util.Map.of("tenant", "acme")));

    assertThat(scope).isSameAs(org.streamrune.core.tracing.TraceScope.NOOP);
    assertThat(withBaggage).isSameAs(org.streamrune.core.tracing.TraceScope.NOOP);
    assertThat(started.get()).isEqualTo(2);
    assertThat(ended.get()).as("each started span was ended").isEqualTo(2);
    assertThat(((GuardedTracing) tracing).failureCount()).isEqualTo(2);
    assertThat(Span.current().getSpanContext().isValid()).isFalse();
  }

  @Test
  void theOpenTelemetryTracingIsGuarded_startChildAndPropagatingIncluded() {
    ThrowingProcessor processor = new ThrowingProcessor(true, true);
    StreamRuneTracing tracing =
        over(SdkTracerProvider.builder().addSpanProcessor(processor).build());
    assertThat(tracing).isInstanceOf(GuardedTracing.class);

    OpenTelemetrySdk healthy =
        OpenTelemetrySdk.builder().setTracerProvider(SdkTracerProvider.builder().build()).build();
    Span parent = healthy.getTracer("app").spanBuilder("request").startSpan();
    try (io.opentelemetry.context.Scope ignored = parent.makeCurrent()) {
      assertThatCode(
              () ->
                  tracing
                      .startChild(
                          org.streamrune.core.tracing.TracingArea.EVENT_STORE,
                          "eventstore/read",
                          org.streamrune.core.tracing.SpanKind.INTERNAL,
                          org.streamrune.core.tracing.SpanAttributes.empty(),
                          java.time.Instant.now())
                      .close())
          .doesNotThrowAnyException();
      assertThat(Span.current()).as("the caller's span is current again").isSameAs(parent);
      AtomicInteger ran = new AtomicInteger();
      tracing.propagating(ran::incrementAndGet).run();
      assertThat(ran.get()).isEqualTo(1);
    } finally {
      parent.end();
    }
    assertThat(processor.thrown.get()).isPositive();
  }
}
