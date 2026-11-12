package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.LinkData;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.SpanId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.SagaFixture;

/**
 * The saga spans, recorded through {@link OpenTelemetryTracing}: {@code saga/{SagaType}/step} per
 * event a saga handles, linked to the span that wrote the event and carrying the step's outcome;
 * {@code saga/{SagaType}/compensate} per compensation command; {@code saga/{SagaType}/timeout} per
 * saga the timeout runner acts on and none for a poll that finds none; the commands a step
 * dispatches nest under it; none with the saga area off or NOOP.
 */
class SagaTracingTest {

  private static final AggregateType ORDER = AggregateType.of("order");
  private static final AggregateType STOCK = AggregateType.of("stock");
  private static final StreamId ORDER_1 = StreamId.of(ORDER, AggregateId.of("o-1"));
  private static final SagaId SAGA_1 = SagaId.of("order-o-1");
  private static final TraceId WRITER_TRACE = TraceId.of("0af7651916cd43dd8448eb211c80319c");
  private static final SpanId WRITER_SPAN = SpanId.of("b7ad6b7169203331");

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  record OrderState(SagaStatus status, String orderId) implements SagaState {}

  record OrderPlaced(String orderId) implements DomainEvent {}

  record StockReserved(String orderId) implements DomainEvent {}

  record ReserveStock(String orderId) implements Command {}

  record CancelOrder(String orderId) implements Command {}

  private static final SagaType TYPE = SagaType.fromClass(OrderState.class);

  /** Reserves stock for a placed order, cancels the order to compensate, times out after 5m. */
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
        if (e.orderId().equals("poison")) {
          throw new IllegalStateException("the saga's own logic failed");
        }
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

  private static StreamRuneTracing tracing() {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled());
  }

  private static SagaRunner.Builder<OrderState> routed(
      SagaRunner.Builder<OrderState> builder, StreamRuneTracing tracing) {
    return builder
        .decider(new OrderSaga())
        .startWhen(event -> event.event() instanceof OrderPlaced)
        .extractSagaId(event -> SagaId.of("order-" + ((OrderPlaced) event.event()).orderId()))
        .tracing(tracing);
  }

  private static SagaFixture<OrderState> fixture(StreamRuneTracing tracing) {
    return SagaFixture.of(OrderState.class, builder -> routed(builder, tracing))
        .withTimeouts(builder -> builder.decider(new OrderSaga()).tracing(tracing));
  }

  /** An order placed by a command whose span is {@link #WRITER_SPAN}. */
  private static EventEnvelope placedUnderTheWritersSpan(
      SagaFixture<OrderState> fixture, String id) {
    EventEnvelope event =
        fixture.event(StreamId.of(ORDER, AggregateId.of(id)), new OrderPlaced(id));
    EventMetadata m = event.metadata();
    return new EventEnvelope(
        event.globalOffset(),
        event.streamId(),
        event.version(),
        event.eventType(),
        event.event(),
        new EventMetadata(
            m.eventId(),
            m.commandId(),
            m.traceId(),
            m.spanId(),
            m.correlationId(),
            m.causationId(),
            m.userId(),
            m.timestamp(),
            Map.of(),
            m.sagaOrigin(),
            new org.streamrune.core.tracing.SpanLink(WRITER_TRACE, WRITER_SPAN)));
  }

  private static List<SpanData> spans(String name) {
    return otelTesting.getSpans().stream().filter(s -> s.getName().equals(name)).toList();
  }

  private static SpanData only(String name) {
    List<SpanData> spans = spans(name);
    assertThat(spans).as("spans named %s", name).hasSize(1);
    return spans.getFirst();
  }

  @Test
  void aStep_carriesTheSagaAndItsOutcome_andLinksToTheSpanThatWroteTheEvent() {
    SagaFixture<OrderState> fixture = fixture(tracing());

    fixture.when(placedUnderTheWritersSpan(fixture, "o-1"));

    SpanData step = only(TraceAttributes.sagaStepSpanName(TYPE));
    assertThat(step.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(EventStoreTracingTest.keys(step))
        .containsExactlyInAnyOrder(
            TraceAttributes.SAGA_TYPE, TraceAttributes.SAGA_ID, TraceAttributes.SAGA_STEP_OUTCOME);
    assertThat(EventStoreTracingTest.string(step, TraceAttributes.SAGA_TYPE))
        .isEqualTo(TYPE.value());
    assertThat(EventStoreTracingTest.string(step, TraceAttributes.SAGA_ID)).isEqualTo("order-o-1");
    assertThat(EventStoreTracingTest.string(step, TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("ADVANCED");
    assertThat(step.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
    assertThat(step.getLinks()).hasSize(1);
    LinkData link = step.getLinks().getFirst();
    assertThat(link.getSpanContext().getTraceId()).isEqualTo(WRITER_TRACE.value());
    assertThat(link.getSpanContext().getSpanId()).isEqualTo(WRITER_SPAN.value());
    otelTesting.clearSpans();

    fixture.replyingTo(SAGA_1, new StockReserved("o-1"));

    SpanData reply = only(TraceAttributes.sagaStepSpanName(TYPE));
    assertThat(EventStoreTracingTest.string(reply, TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("COMPLETED");
    assertThat(reply.getLinks()).isEmpty();
    otelTesting.clearSpans();

    fixture.redeliver();
    assertThat(
            EventStoreTracingTest.string(
                only(TraceAttributes.sagaStepSpanName(TYPE)), TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("SKIPPED");
  }

  @Test
  void aRejectedForwardCommand_compensates_withOneCompensateSpanPerCommandUnderTheStep() {
    SagaFixture<OrderState> fixture =
        fixture(tracing()).failing(ReserveStock.class, () -> new DomainException("no stock"));

    fixture.when(ORDER_1, new OrderPlaced("o-1"));

    SpanData step = only(TraceAttributes.sagaStepSpanName(TYPE));
    SpanData compensate = only(TraceAttributes.sagaCompensateSpanName(TYPE));
    assertThat(EventStoreTracingTest.string(step, TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("COMPENSATED");
    assertThat(compensate.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(EventStoreTracingTest.keys(compensate))
        .containsExactlyInAnyOrder(
            TraceAttributes.SAGA_TYPE,
            TraceAttributes.SAGA_ID,
            TraceAttributes.SAGA_COMPENSATION_INDEX);
    assertThat(EventStoreTracingTest.number(compensate, TraceAttributes.SAGA_COMPENSATION_INDEX))
        .isZero();
    assertThat(EventStoreTracingTest.string(compensate, TraceAttributes.SAGA_ID))
        .isEqualTo("order-o-1");
    assertThat(compensate.getParentSpanId()).isEqualTo(step.getSpanId());
    assertThat(compensate.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
  }

  @Test
  void aFailedCompensationCommand_marksItsSpanFailed() {
    SagaFixture<OrderState> fixture =
        fixture(tracing())
            .failing(ReserveStock.class, () -> new DomainException("no stock"))
            .failing(CancelOrder.class, () -> new DomainException("cannot cancel"));

    fixture.when(ORDER_1, new OrderPlaced("o-1"));

    EventStoreTracingTest.assertFailed(only(TraceAttributes.sagaCompensateSpanName(TYPE)));
    assertThat(
            EventStoreTracingTest.string(
                only(TraceAttributes.sagaStepSpanName(TYPE)), TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("FAILED");
  }

  @Test
  void aPoisonEvent_isTheOutcomeFaulted_withItsError_andALaterEventIsDeferred() {
    SagaFixture<OrderState> fixture = fixture(tracing());

    fixture.when(StreamId.of(ORDER, AggregateId.of("poison")), new OrderPlaced("poison"));

    SpanData poisoned = only(TraceAttributes.sagaStepSpanName(TYPE));
    assertThat(EventStoreTracingTest.string(poisoned, TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("FAULTED");
    EventStoreTracingTest.assertFailed(poisoned);
    otelTesting.clearSpans();

    fixture.replyingTo(SagaId.of("order-poison"), new StockReserved("poison"));

    SpanData held = only(TraceAttributes.sagaStepSpanName(TYPE));
    assertThat(EventStoreTracingTest.string(held, TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("DEFERRED");
    assertThat(held.getStatus().getStatusCode()).isEqualTo(StatusCode.OK);
  }

  @Test
  void theTimeoutRunner_tracesEachSagaItTimesOut_andNothingForAPollThatFindsNone() {
    SagaFixture<OrderState> fixture = fixture(tracing());
    fixture.when(ORDER_1, new OrderPlaced("o-1"));
    otelTesting.clearSpans();

    fixture.runTimeouts();
    assertThat(otelTesting.getSpans()).isEmpty();

    fixture.advance(Duration.ofMinutes(6)).runTimeouts();

    SpanData timeout = only(TraceAttributes.sagaTimeoutSpanName(TYPE));
    assertThat(timeout.getKind()).isEqualTo(SpanKind.INTERNAL);
    assertThat(EventStoreTracingTest.keys(timeout))
        .containsExactlyInAnyOrder(TraceAttributes.SAGA_TYPE, TraceAttributes.SAGA_ID);
    assertThat(EventStoreTracingTest.string(timeout, TraceAttributes.SAGA_ID))
        .isEqualTo("order-o-1");
    assertThat(timeout.getParentSpanContext().isValid()).isFalse();
    SpanData compensate = only(TraceAttributes.sagaCompensateSpanName(TYPE));
    assertThat(compensate.getParentSpanId()).isEqualTo(timeout.getSpanId());
    fixture.expectCommands(new CancelOrder("o-1"));
  }

  @Test
  void withTheSagaAreaOff_orNoop_noSagaSpanStarts() {
    for (StreamRuneTracing off :
        List.of(
            OpenTelemetryTracing.of(
                otelTesting.getOpenTelemetry(),
                TracingSettings.builder().area(TracingArea.SAGA, false).build()),
            StreamRuneTracing.NOOP)) {
      SagaFixture<OrderState> fixture =
          fixture(off).failing(ReserveStock.class, () -> new DomainException("no stock"));
      fixture.when(ORDER_1, new OrderPlaced("o-1"));
      fixture.when(StreamId.of(ORDER, AggregateId.of("o-2")), new OrderPlaced("o-2"));
      fixture.advance(Duration.ofMinutes(6)).runTimeouts();
    }

    assertThat(otelTesting.getSpans()).isEmpty();
  }

  /** {@code event} as stored at {@code offset}. */
  private static EventEnvelope at(long offset, EventEnvelope event) {
    return new EventEnvelope(
        GlobalOffset.of(offset),
        event.streamId(),
        new org.streamrune.core.types.Version(1),
        event.eventType(),
        event.event(),
        event.metadata());
  }

  /** A step lock held on another thread; {@link #release} lets it go and waits until it has. */
  private record HeldStep(java.util.concurrent.CountDownLatch latch, Thread holder) {
    void release() throws InterruptedException {
      latch.countDown();
      holder.join();
    }
  }

  /** Holds the step lock of {@code sagaId} on another thread until it is released. */
  private static HeldStep holdStep(SagaStepLock lock, SagaId sagaId) throws InterruptedException {
    var held = new java.util.concurrent.CountDownLatch(1);
    var release = new java.util.concurrent.CountDownLatch(1);
    Thread holder =
        Thread.ofVirtual()
            .start(
                () -> {
                  try (SagaStepLock.Hold _ = lock.enterStep(TYPE, sagaId)) {
                    held.countDown();
                    release.await();
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                });
    assertThat(held.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
    return new HeldStep(release, holder);
  }

  @Test
  void aStepThatWaitsForAnotherStepOfItsSaga_isTheOutcomeDeferred_notAnError()
      throws InterruptedException {
    var lock = new SagaStepLock(Duration.ofMillis(50));
    SagaFixture<OrderState> fixture =
        SagaFixture.of(OrderState.class, builder -> routed(builder, tracing()).stepLock(lock));
    var release = holdStep(lock, SAGA_1);
    try {
      assertThatThrownBy(() -> fixture.when(ORDER_1, new OrderPlaced("o-1")))
          .isInstanceOf(SagaStepLockException.class);
    } finally {
      release.release();
    }

    SpanData step = only(TraceAttributes.sagaStepSpanName(TYPE));
    assertThat(EventStoreTracingTest.string(step, TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("DEFERRED");
    assertThat(step.getStatus().getStatusCode()).isEqualTo(StatusCode.UNSET);
    assertThat(step.getEvents()).isEmpty();
  }

  @Test
  void aTimeoutClaimDeferredWhileAStepIsInFlight_startsNoSpan() throws InterruptedException {
    var lock = new SagaStepLock(Duration.ofSeconds(5));
    SagaFixture<OrderState> fixture =
        SagaFixture.of(OrderState.class, builder -> routed(builder, tracing()).stepLock(lock))
            .withTimeouts(builder -> builder.decider(new OrderSaga()).tracing(tracing()));
    fixture.when(ORDER_1, new OrderPlaced("o-1")).advance(Duration.ofMinutes(6));
    otelTesting.clearSpans();

    var release = holdStep(lock, SAGA_1);
    try {
      fixture.runTimeouts().expectNoCommands();
    } finally {
      release.release();
    }
    assertThat(otelTesting.getSpans()).isEmpty();

    fixture.runTimeouts().expectCommands(new CancelOrder("o-1"));
    assertThat(spans(TraceAttributes.sagaTimeoutSpanName(TYPE))).hasSize(1);
  }

  @Test
  void theReplayFeed_tracesItsStep_withTheOutcomeFaultedForAPoisonItSwallows() {
    SagaFixture<OrderState> fixture = fixture(tracing());

    StepOutcome advanced =
        fixture
            .runner()
            .feedReplay(SAGA_1, at(10, placedUnderTheWritersSpan(fixture, "o-1")), true, false);

    assertThat(advanced).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    SpanData step = only(TraceAttributes.sagaStepSpanName(TYPE));
    assertThat(EventStoreTracingTest.string(step, TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("ADVANCED");
    assertThat(step.getLinks()).hasSize(1);
    otelTesting.clearSpans();

    StepOutcome poisoned =
        fixture
            .runner()
            .feedReplay(
                SagaId.of("order-poison"),
                at(
                    11,
                    fixture.event(
                        StreamId.of(ORDER, AggregateId.of("poison")), new OrderPlaced("poison"))),
                true,
                false);

    assertThat(poisoned).isEqualTo(StepOutcome.FAULTED);
    SpanData faulted = only(TraceAttributes.sagaStepSpanName(TYPE));
    assertThat(EventStoreTracingTest.string(faulted, TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("FAULTED");
  }

  // ==================== With a real command bus ====================

  record StockState() implements AggregateState {}

  static final class StockDecider implements Decider<ReserveStock, StockState, StockReserved> {
    @Override
    public StockState initialState() {
      return new StockState();
    }

    @Override
    public List<StockReserved> decide(ReserveStock command, StockState state) {
      return List.of(new StockReserved(command.orderId()));
    }

    @Override
    public StockState evolve(StockState state, StockReserved event) {
      return state;
    }
  }

  @Test
  void theCommandAStepDispatches_nestsUnderTheStep() {
    StreamRuneTracing tracing = tracing();
    var inbox = new InMemoryCommandInbox();
    var sagaStore = new InMemorySagaStore();
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore().withCommandInbox(inbox))
            .commandInbox(inbox)
            .register(
                STOCK, ReserveStock.class, c -> AggregateId.of(c.orderId()), new StockDecider())
            .tracing(tracing)
            .build()) {
      SagaRunner<OrderState> runner =
          routed(SagaRunner.<OrderState>builder(), tracing)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
              .forwardFence(false)
              .build();
      SagaFixture<OrderState> envelopes =
          SagaFixture.of(OrderState.class, builder -> routed(builder, StreamRuneTracing.NOOP));
      EventEnvelope placed = envelopes.event(ORDER_1, new OrderPlaced("o-1"));
      EventEnvelope stored =
          new EventEnvelope(
              GlobalOffset.of(1),
              placed.streamId(),
              new org.streamrune.core.types.Version(1),
              placed.eventType(),
              placed.event(),
              placed.metadata());

      runner.asEventListener().onEvents(List.of(stored));
    }

    SpanData step = only(TraceAttributes.sagaStepSpanName(TYPE));
    SpanData command = only("command/ReserveStock");
    assertThat(command.getParentSpanId()).isEqualTo(step.getSpanId());
    assertThat(command.getTraceId()).isEqualTo(step.getTraceId());
    assertThat(EventStoreTracingTest.string(step, TraceAttributes.SAGA_STEP_OUTCOME))
        .isEqualTo("ADVANCED");
  }

  record OrderOpen() implements AggregateState {}

  record PlaceOrder(String orderId) implements Command {}

  static final class OrderDecider implements Decider<PlaceOrder, OrderOpen, OrderPlaced> {
    @Override
    public OrderOpen initialState() {
      return new OrderOpen();
    }

    @Override
    public List<OrderPlaced> decide(PlaceOrder command, OrderOpen state) {
      return List.of(new OrderPlaced(command.orderId()));
    }

    @Override
    public OrderOpen evolve(OrderOpen state, OrderPlaced event) {
      return state;
    }
  }

  /**
   * No hand-built metadata: the bus writes each command's own span into the metadata of its events,
   * the step links to the span of the command that placed the order, and the command the step
   * dispatches writes its own span, a child of the step, into the events it appends.
   */
  @Test
  void aStep_linksToTheCommandThatAppendedItsEvent_andTheCommandItDispatchesWritesItsOwnSpan() {
    StreamRuneTracing tracing = tracing();
    var inbox = new InMemoryCommandInbox();
    var sagaStore = new InMemorySagaStore();
    var store = new InMemoryEventStore().withCommandInbox(inbox);
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .commandInbox(inbox)
            .register(ORDER, PlaceOrder.class, c -> AggregateId.of(c.orderId()), new OrderDecider())
            .register(
                STOCK, ReserveStock.class, c -> AggregateId.of(c.orderId()), new StockDecider())
            .tracing(tracing)
            .build()) {
      SagaRunner<OrderState> runner =
          routed(SagaRunner.<OrderState>builder(), tracing)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
              .forwardFence(false)
              .build();
      bus.execute(new PlaceOrder("o-1"));
      List<EventEnvelope> placed = store.readStream(ORDER_1, Version.initial(), 10);

      runner.asEventListener().onEvents(placed);
    }

    SpanData placeOrder = only("command/PlaceOrder");
    SpanData step = only(TraceAttributes.sagaStepSpanName(TYPE));
    assertThat(step.getLinks()).hasSize(1);
    assertThat(step.getLinks().getFirst().getSpanContext().getTraceId())
        .isEqualTo(placeOrder.getTraceId());
    assertThat(step.getLinks().getFirst().getSpanContext().getSpanId())
        .isEqualTo(placeOrder.getSpanId());
    SpanData reserve = only("command/ReserveStock");
    assertThat(reserve.getParentSpanId()).isEqualTo(step.getSpanId());
    EventMetadata reserved =
        store
            .readStream(StreamId.of(STOCK, AggregateId.of("o-1")), Version.initial(), 10)
            .getFirst()
            .metadata();
    assertThat(reserved.spanContext().traceId().value()).isEqualTo(reserve.getTraceId());
    assertThat(reserved.spanContext().spanId().value()).isEqualTo(reserve.getSpanId());
  }

  @Test
  void theStepOutcome_namesEveryExecutorOutcome() {
    for (StepOutcome outcome : StepOutcome.values()) {
      assertThat(SagaRunner.stepOutcome(outcome))
          .isIn(
              "ADVANCED",
              "COMPLETED",
              "COMPENSATING",
              "COMPENSATED",
              "FAILED",
              "FAULTED",
              "SKIPPED");
    }
    assertThat(SagaRunner.stepOutcome(StepOutcome.COMPENSATING_REFUSED)).isEqualTo("COMPENSATING");
    assertThat(SagaRunner.stepOutcome(StepOutcome.CLAIM_LOST)).isEqualTo("SKIPPED");
    assertThatThrownBy(() -> SagaRunner.<OrderState>builder().tracing(null))
        .isInstanceOf(NullPointerException.class);
  }
}
