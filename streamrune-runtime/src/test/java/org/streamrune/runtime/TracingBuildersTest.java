package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRunner;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.subscription.SubscriptionConfig;
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
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * The {@code tracing(StreamRuneTracing)} option of the builders: with the area on, a command, a
 * subscription delivery and a projection batch each produce exactly their span, and with the area
 * switched off none.
 */
class TracingBuildersTest {

  private static final AggregateType TYPE = AggregateType.of("lamp");

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  record Switch(String lampId) implements Command {}

  record Switched() implements DomainEvent {}

  record LampState() implements AggregateState {}

  static final class LampDecider implements Decider<Switch, LampState, Switched> {
    @Override
    public LampState initialState() {
      return new LampState();
    }

    @Override
    public List<Switched> decide(Switch command, LampState state) {
      return List.of(new Switched());
    }

    @Override
    public LampState evolve(LampState state, Switched event) {
      return state;
    }
  }

  private static StreamRuneTracing tracing() {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled());
  }

  private static StreamRuneTracing tracingWithout(TracingArea area) {
    return OpenTelemetryTracing.of(
        otelTesting.getOpenTelemetry(), TracingSettings.builder().area(area, false).build());
  }

  private static List<String> spanNames() {
    return otelTesting.getSpans().stream().map(SpanData::getName).toList();
  }

  // ==================== Command bus ====================

  private static VirtualThreadCommandBus.Builder bus(StreamRuneTracing tracing) {
    return VirtualThreadCommandBus.builder()
        .eventStore(new InMemoryEventStore())
        .register(TYPE, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
        .tracing(tracing);
  }

  @Test
  void commandBus_tracesEachCommand() {
    try (var bus = bus(tracing()).build()) {
      bus.execute(new Switch("l-1"));
    }

    // The bus's default locker traces its acquisition too, under the command span.
    assertThat(spanNames())
        .containsExactlyInAnyOrder("command/Switch", TraceAttributes.LOCK_ACQUIRE);
    SpanData span =
        otelTesting.getSpans().stream()
            .filter(s -> s.getName().equals("command/Switch"))
            .findFirst()
            .orElseThrow();
    assertThat(span.getAttributes().get(AttributeKey.stringKey(TraceAttributes.STREAM_ID)))
        .isEqualTo("lamp:l-1");
  }

  @Test
  void commandBus_withTheCommandAreaOff_addsNoInterceptor_andTracesNothing() {
    var userInterceptor = CommandInterceptor.noop();
    try (var bus = bus(tracingWithout(TracingArea.COMMAND)).interceptors(userInterceptor).build()) {
      bus.execute(new Switch("l-1"));
      assertThat(bus.interceptors()).containsExactly(userInterceptor);
    }

    // Only the default locker's span: the lock area is still on.
    assertThat(spanNames()).containsExactly(TraceAttributes.LOCK_ACQUIRE);
  }

  @Test
  void commandBus_withNoopTracing_runsTheChainAsAdded() {
    var userInterceptor = CommandInterceptor.noop();
    try (var bus = bus(StreamRuneTracing.NOOP).interceptors(userInterceptor).build()) {
      assertThat(bus.interceptors()).containsExactly(userInterceptor);
    }
  }

  @Test
  void commandBus_keepsAnInterceptorTheApplicationAdded_andTracesOnce() {
    var own =
        new OpenTelemetryCommandInterceptor(
            OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), TracingSettings.allEnabled()));
    try (var bus = bus(tracing()).interceptors(own).build()) {
      bus.execute(new Switch("l-1"));
      assertThat(bus.interceptors()).containsExactly(own);
    }

    assertThat(spanNames())
        .containsExactlyInAnyOrder("command/Switch", TraceAttributes.LOCK_ACQUIRE);
  }

  @Test
  void commandBus_putsTheTracingInterceptorAheadOfTheFrameworkAndUserInterceptors() {
    var user = CommandInterceptor.noop();
    try (var bus = bus(tracing()).interceptors(user).build()) {
      assertThat(bus.interceptors()).hasSize(2);
      assertThat(bus.interceptors().getFirst()).isInstanceOf(OpenTelemetryCommandInterceptor.class);
      assertThat(bus.interceptors().get(1)).isSameAs(user);
    }
  }

  @jakarta.annotation.Priority(500)
  static final class OutermostInterceptor implements CommandInterceptor {}

  @Test
  void commandBus_leavesAnInterceptorOrderedAheadOfTheSlotInFront() {
    var outermost = new OutermostInterceptor();
    var user = CommandInterceptor.noop();
    try (var bus = bus(tracing()).interceptors(outermost, user).build()) {
      assertThat(bus.interceptors()).hasSize(3);
      assertThat(bus.interceptors().get(0)).isSameAs(outermost);
      assertThat(bus.interceptors().get(1)).isInstanceOf(OpenTelemetryCommandInterceptor.class);
      assertThat(bus.interceptors().get(2)).isSameAs(user);
    }
  }

  @Test
  void commandBus_appendsTheTracingInterceptorBehindOnlyEarlierOrderedOnes() {
    var outermost = new OutermostInterceptor();
    try (var bus = bus(tracing()).interceptors(outermost).build()) {
      assertThat(bus.interceptors()).hasSize(2);
      assertThat(bus.interceptors().get(1)).isInstanceOf(OpenTelemetryCommandInterceptor.class);
    }
  }

  // ==================== Subscriptions ====================

  record Seen() implements DomainEvent {}

  private static EventEnvelope envelope(StreamId stream, long version) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        stream,
        new Version(version),
        new EventType("Seen"),
        new Seen(),
        new EventMetadata(
            EventId.of("evt_" + version),
            CommandId.of("cmd_" + version),
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }

  private static InMemoryEventStore storeWithTwoEvents(StreamId stream) {
    var store = new InMemoryEventStore();
    store.append(stream, List.of(envelope(stream, 1), envelope(stream, 2)), Version.initial());
    return store;
  }

  private static SubscriptionConfig fastConfig() {
    return new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(2));
  }

  private static PollingEventSubscription polling(
      StreamRuneTracing tracing, EventListener listener) {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("l-1"));
    return PollingEventSubscription.builder()
        .subscriptionName(SubscriptionName.of("lights"))
        .eventStore(storeWithTwoEvents(stream))
        .offsetStore(new InMemoryOffsetStore())
        .listener(listener)
        .config(fastConfig())
        .tracing(tracing)
        .build();
  }

  @Test
  void pollingSubscription_tracesEachDelivery() {
    var received = new CopyOnWriteArrayList<EventEnvelope>();

    assertThat(polling(tracing(), received::addAll).pollOnce()).isEqualTo(2);

    assertThat(received).hasSize(2);
    assertThat(spanNames()).containsExactly("subscription/lights/deliver");
    assertThat(
            otelTesting
                .getSpans()
                .getFirst()
                .getAttributes()
                .get(AttributeKey.longKey(TraceAttributes.BATCH_SIZE)))
        .isEqualTo(2L);
  }

  @Test
  void pollingSubscription_withTheSubscriptionAreaOff_tracesNothing() {
    var received = new CopyOnWriteArrayList<EventEnvelope>();

    polling(tracingWithout(TracingArea.SUBSCRIPTION), received::addAll).pollOnce();

    assertThat(received).hasSize(2);
    assertThat(otelTesting.getSpans()).isEmpty();
  }

  @Test
  void pollingSubscription_doesNotWrapATracingListenerAgain() {
    EventListener own =
        new TracingEventListener(events -> {}, tracing(), SubscriptionName.of("lights"));

    polling(tracing(), own).pollOnce();

    assertThat(spanNames()).containsExactly("subscription/lights/deliver");
  }

  @Test
  void pollingSubscription_withAHealthContributor_tracesTheApplicationsTracingListenerOnce() {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("l-h"));
    var store = storeWithTwoEvents(stream);
    var offsets = new InMemoryOffsetStore();
    EventListener own =
        new TracingEventListener(events -> {}, tracing(), SubscriptionName.of("lights"));

    var sub =
        PollingEventSubscription.builder()
            .subscriptionName(SubscriptionName.of("lights"))
            .eventStore(store)
            .offsetStore(offsets)
            .listener(own)
            .config(fastConfig())
            .healthContributor(new SubscriptionHealthContributor(store, offsets))
            .tracing(tracing())
            .build();

    assertThat(sub.pollOnce()).isEqualTo(2);
    assertThat(spanNames()).containsExactly("subscription/lights/deliver");
  }

  @Test
  void pollingSubscription_withAHealthContributor_tracesAPlainListenerOnce() {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("l-p"));
    var store = storeWithTwoEvents(stream);
    var offsets = new InMemoryOffsetStore();

    var sub =
        PollingEventSubscription.builder()
            .subscriptionName(SubscriptionName.of("lights"))
            .eventStore(store)
            .offsetStore(offsets)
            .listener(events -> {})
            .config(fastConfig())
            .healthContributor(new SubscriptionHealthContributor(store, offsets))
            .tracing(tracing())
            .build();

    assertThat(sub.pollOnce()).isEqualTo(2);
    assertThat(spanNames()).containsExactly("subscription/lights/deliver");
  }

  @Test
  void streamSubscription_tracesEachDelivery_andNotWithTheAreaOff() {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("l-2"));
    for (StreamRuneTracing tracing : List.of(tracingWithout(TracingArea.SUBSCRIPTION), tracing())) {
      var sub =
          StreamEventSubscription.builder()
              .subscriptionName(SubscriptionName.of("lamp-2"))
              .streamId(stream)
              .eventStore(storeWithTwoEvents(stream))
              .offsetStore(new InMemoryOffsetStore())
              .listener(events -> {})
              .config(fastConfig())
              .tracing(tracing)
              .build();
      assertThat(sub.pollOnce()).isEqualTo(2);
    }

    assertThat(spanNames()).containsExactly("subscription/lamp-2/deliver");
  }

  // ==================== Projection runners ====================

  /** A projection that counts the batches it was handed. */
  static final class CountingProjection implements Projection {
    final CopyOnWriteArrayList<Integer> batches = new CopyOnWriteArrayList<>();

    @Override
    public void process(List<EventEnvelope> batch) {
      batches.add(batch.size());
    }
  }

  private static void await(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertThat(condition.getAsBoolean()).as("condition within 5s").isTrue();
  }

  private static ContinuousProjectionRunner continuous(
      InMemoryEventStore store, StreamRuneTracing tracing) {
    return ContinuousProjectionRunner.builder()
        .eventStore(store)
        .offsetStore(new InMemoryOffsetStore())
        .subscriptionConfig(fastConfig())
        .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
        .tracing(tracing)
        .build();
  }

  private static void runUntilProcessed(
      ContinuousProjectionRunner runner, Projection projection, CountingProjection counting)
      throws InterruptedException {
    Thread thread =
        Thread.ofVirtual()
            .start(
                () -> runner.run(ProjectionName.of("lamps"), projection, AT_LEAST_ONCE_IDEMPOTENT));
    try {
      await(() -> !counting.batches.isEmpty());
    } finally {
      runner.close();
      thread.join(TimeUnit.SECONDS.toMillis(5));
    }
  }

  @Test
  void continuousRunner_tracesEachBatch() throws InterruptedException {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("l-3"));
    var projection = new CountingProjection();

    runUntilProcessed(continuous(storeWithTwoEvents(stream), tracing()), projection, projection);

    assertThat(spanNames()).isNotEmpty().allMatch("projection/lamps/process"::equals);
    assertThat(
            otelTesting
                .getSpans()
                .getFirst()
                .getAttributes()
                .get(AttributeKey.stringKey(TraceAttributes.PROJECTION_NAME)))
        .isEqualTo("lamps");
  }

  @Test
  void continuousRunner_withTheProjectionAreaOff_tracesNothing() throws InterruptedException {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("l-4"));
    var projection = new CountingProjection();

    runUntilProcessed(
        continuous(storeWithTwoEvents(stream), tracingWithout(TracingArea.PROJECTION)),
        projection,
        projection);

    assertThat(otelTesting.getSpans()).isEmpty();
  }

  @Test
  void continuousRunner_doesNotDecorateATracedProjectionAgain() throws InterruptedException {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("l-5"));
    var counting = new CountingProjection();
    var own = new TracingProjectionDecorator(ProjectionName.of("lamps"), counting, tracing());

    runUntilProcessed(continuous(storeWithTwoEvents(stream), tracing()), own, counting);

    assertThat(otelTesting.getSpans()).hasSize(counting.batches.size());
  }

  @Test
  void multiProjectionRunner_tracesEachRegistrationsBatches() throws InterruptedException {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("l-6"));
    var projection = new CountingProjection();
    var runner =
        MultiProjectionRunner.builder()
            .eventStore(storeWithTwoEvents(stream))
            .offsetStore(new InMemoryOffsetStore())
            .subscriptionConfig(fastConfig())
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .register(ProjectionName.of("multi"), projection, AT_LEAST_ONCE_IDEMPOTENT)
            .tracing(tracing())
            .build();

    runner.start();
    try {
      await(() -> !projection.batches.isEmpty());
    } finally {
      runner.stop();
    }

    assertThat(spanNames()).isNotEmpty().allMatch("projection/multi/process"::equals);
  }

  @Test
  void scheduledProjectionRunner_tracesEachDrainsBatches() throws InterruptedException {
    StreamId stream = StreamId.of(TYPE, AggregateId.of("l-7"));
    var projection = new CountingProjection();
    var runner =
        ScheduledProjectionRunner.builder()
            .eventStore(storeWithTwoEvents(stream))
            .offsetStore(new InMemoryOffsetStore())
            .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
            .register(
                ProjectionName.of("nightly"), projection, "* * * * * *", AT_LEAST_ONCE_IDEMPOTENT)
            .tracing(tracing())
            .build();

    runner.start();
    try {
      await(() -> !projection.batches.isEmpty());
    } finally {
      runner.stop();
    }

    assertThat(spanNames()).isNotEmpty().allMatch("projection/nightly/process"::equals);
  }

  // ==================== StreamRune facade ====================

  /** A runner that records the projection it is handed and processes nothing. */
  static final class RecordingRunner implements ProjectionRunner {
    final AtomicReference<Projection> handed = new AtomicReference<>();

    @Override
    public void run(
        ProjectionName projectionName, Projection projection, ProjectionDeliveryMode mode) {
      handed.set(projection);
    }

    @Override
    public void reset(ProjectionName projectionName) {}
  }

  @Test
  void streamRune_tracesItsCommandsAndDecoratesItsProjections() throws InterruptedException {
    var runner = new RecordingRunner();
    var projection = new CountingProjection();
    try (var streamRune =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .register(TYPE, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .registerProjection(ProjectionName.of("facade"), projection, AT_LEAST_ONCE_IDEMPOTENT)
            .projectionRunner(runner)
            .tracing(tracing())
            .build()) {
      streamRune.execute(new Switch("l-8"));
      streamRune.startProjections();
      await(() -> runner.handed.get() != null);
    }

    Projection handed = runner.handed.get();
    assertThat(handed).isInstanceOf(TracingProjectionDecorator.class);
    handed.process(List.of());
    assertThat(spanNames())
        .containsExactlyInAnyOrder(
            "command/Switch", TraceAttributes.LOCK_ACQUIRE, "projection/facade/process");
  }

  @Test
  void streamRune_withoutTracing_handsTheProjectionAsRegistered() throws InterruptedException {
    var runner = new RecordingRunner();
    var projection = new CountingProjection();
    try (var streamRune =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .registerProjection(ProjectionName.of("facade"), projection, AT_LEAST_ONCE_IDEMPOTENT)
            .projectionRunner(runner)
            .build()) {
      streamRune.startProjections();
      await(() -> runner.handed.get() != null);
    }

    assertThat(runner.handed.get()).isSameAs(projection);
  }

  @Test
  void theTracedFactories_returnTheirArgumentWhenTheyCannotOrNeedNotWrap() {
    EventListener listener = events -> {};
    Projection projection = batch -> {};
    var name = SubscriptionName.of("s");
    var projectionName = ProjectionName.of("p");

    assertThatThrownBy(() -> TracingEventListener.traced(listener, null, name))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("tracing is required");
    assertThat(TracingEventListener.traced(listener, StreamRuneTracing.NOOP, name))
        .isSameAs(listener);
    assertThat(TracingEventListener.traced(null, tracing(), name)).isNull();
    assertThat(TracingEventListener.traced(listener, tracing(), null)).isSameAs(listener);
    assertThat(TracingEventListener.traced(listener, tracing(), name))
        .isInstanceOf(TracingEventListener.class);

    assertThatThrownBy(() -> TracingProjectionDecorator.traced(projectionName, projection, null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("tracing is required");
    assertThat(
            TracingProjectionDecorator.traced(projectionName, projection, StreamRuneTracing.NOOP))
        .isSameAs(projection);
    assertThat(TracingProjectionDecorator.traced(projectionName, null, tracing())).isNull();
    assertThat(TracingProjectionDecorator.traced(null, projection, tracing())).isSameAs(projection);
    assertThat(TracingProjectionDecorator.traced(projectionName, projection, tracing()))
        .isInstanceOf(TracingProjectionDecorator.class);
  }

  @Test
  void decoratorsBuiltWithTheAreaOff_onlyDelegate() {
    var delivered = new CopyOnWriteArrayList<EventEnvelope>();
    var listener =
        new TracingEventListener(
            delivered::addAll, tracingWithout(TracingArea.SUBSCRIPTION), SubscriptionName.of("s"));
    var counting = new CountingProjection();
    var projection =
        new TracingProjectionDecorator(
            ProjectionName.of("p"), counting, tracingWithout(TracingArea.PROJECTION));
    var interceptor = new OpenTelemetryCommandInterceptor(StreamRuneTracing.NOOP);
    var ctx =
        new CommandInterceptor.CommandContext(
            new Switch("l-9"),
            "Switch",
            CommandId.of("cmd_1"),
            TYPE,
            AggregateId.of("l-9"),
            null,
            Instant.now());

    listener.onEvents(List.of());
    projection.process(List.of());
    projection.process(List.of(), null);
    assertThat(projection.processDeadLetterReplay(List.of(), null)).isTrue();
    assertThat(interceptor.before(ctx)).isTrue();
    interceptor.after(ctx);
    interceptor.onError(ctx, new IllegalStateException("x"));

    assertThat(counting.batches).hasSize(3);
    assertThat(otelTesting.getSpans()).isEmpty();
  }

  @Test
  void theCommandSpan_carriesNoAggregateAttributes_withoutAnAggregate() {
    var interceptor = new OpenTelemetryCommandInterceptor(tracing());
    var ctx =
        new CommandInterceptor.CommandContext(
            new Switch("x"), "Switch", CommandId.of("cmd_2"), null, null, null, Instant.now());

    interceptor.before(ctx);
    interceptor.after(ctx);

    assertThat(otelTesting.getSpans().getFirst().getAttributes().asMap().keySet())
        .extracting(AttributeKey::getKey)
        .containsExactlyInAnyOrder("streamrune.command.type", "streamrune.command.id");
  }
}
