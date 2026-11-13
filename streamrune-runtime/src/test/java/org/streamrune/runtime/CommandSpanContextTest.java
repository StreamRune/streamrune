package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.tracing.SpanLink;
import org.streamrune.core.tracing.StreamRuneTracing;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.tracing.TracingArea;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;

/**
 * The span context the command bus writes into event metadata, recorded through {@link
 * OpenTelemetryTracing}, with no hand-built metadata: each event carries the span of the command
 * that appended it, the {@code subscription/…/deliver} span of those events links to that command,
 * and a caller-supplied trace id never reaches a link. With the command area off, or a bus without
 * tracing, the span context is {@link SpanLink#NONE}.
 */
class CommandSpanContextTest {

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private static final AggregateType LAMP = AggregateType.of("lamp");

  /** A W3C trace id an HTTP client sent in {@code X-Trace-Id}. */
  private static final String CLIENT_TRACE = "4bf92f3577b34da6a3ce929d0e0e4736";

  record Switched(String lampId) implements DomainEvent {}

  record Lamp() implements AggregateState {}

  record Switch(String lampId) implements Command {}

  static final class LampDecider implements Decider<Switch, Lamp, Switched> {
    @Override
    public Lamp initialState() {
      return new Lamp();
    }

    @Override
    public List<Switched> decide(Switch command, Lamp state) {
      return List.of(new Switched(command.lampId()), new Switched(command.lampId()));
    }

    @Override
    public Lamp evolve(Lamp state, Switched event) {
      return state;
    }
  }

  private static StreamRuneTracing tracing(TracingSettings settings) {
    return OpenTelemetryTracing.of(otelTesting.getOpenTelemetry(), settings);
  }

  /** Runs {@code Switch(lampId)} through a bus traced by {@code tracing}; returns its events. */
  private static List<EventEnvelope> run(
      InMemoryEventStore store, String lampId, StreamRuneTracing tracing) {
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .tracing(tracing)
            .build()) {
      bus.execute(new Switch(lampId));
    }
    return store.readStream(StreamId.of(LAMP, AggregateId.of(lampId)), Version.initial(), 10);
  }

  private static SpanData only(String name) {
    List<SpanData> named =
        otelTesting.getSpans().stream().filter(s -> s.getName().equals(name)).toList();
    assertThat(named).as("spans named %s", name).hasSize(1);
    return named.getFirst();
  }

  private static void assertLinksTo(SpanData span, SpanData target) {
    assertThat(span.getLinks()).hasSize(1);
    assertThat(span.getLinks().getFirst().getSpanContext().getTraceId())
        .isEqualTo(target.getTraceId());
    assertThat(span.getLinks().getFirst().getSpanContext().getSpanId())
        .isEqualTo(target.getSpanId());
  }

  @Test
  void eachEventCarriesTheSpanOfTheCommandThatAppendedIt() {
    List<EventEnvelope> events =
        run(new InMemoryEventStore(), "l-1", tracing(TracingSettings.allEnabled()));

    SpanData command = only("command/Switch");
    assertThat(events)
        .hasSize(2)
        .allSatisfy(
            event -> {
              SpanLink context = event.metadata().spanContext();
              assertThat(context.traceId().value()).isEqualTo(command.getTraceId());
              assertThat(context.spanId().value()).isEqualTo(command.getSpanId());
            });
  }

  @Test
  void theSpanContextSurvivesTheSerializingStore() {
    InMemoryEventStore store =
        InMemoryEventStore.serializing(
            org.streamrune.core.SimpleEventTypeRegistry.builder()
                .registerEvent("Switched", Switched.class)
                .registerState("Lamp", Lamp.class)
                .build(),
            null);

    List<EventEnvelope> events = run(store, "l-1", tracing(TracingSettings.allEnabled()));

    SpanData command = only("command/Switch");
    assertThat(events.getFirst().metadata().spanContext().spanId().value())
        .isEqualTo(command.getSpanId());
  }

  @Test
  void theDeliverySpan_linksToTheCommandThatAppendedTheEvents() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());
    List<EventEnvelope> events = run(new InMemoryEventStore(), "l-1", tracing);

    new TracingEventListener(e -> {}, tracing, SubscriptionName.of("lamps")).onEvents(events);

    assertLinksTo(
        only(TraceAttributes.subscriptionDeliverSpanName(SubscriptionName.of("lamps"))),
        only("command/Switch"));
  }

  @Test
  void aClientSuppliedTraceId_neverReachesALink() throws Exception {
    var client =
        StreamRuneContext.RequestContext.fromRequest(
            TraceId.of(CLIENT_TRACE), null, CorrelationId.of("client"), Instant.now(), Map.of());
    InMemoryEventStore store = new InMemoryEventStore();
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());

    List<EventEnvelope> untraced =
        ScopedValue.where(StreamRuneContext.CURRENT, client)
            .call(() -> run(store, "l-untraced", StreamRuneTracing.NOOP));
    List<EventEnvelope> traced =
        ScopedValue.where(StreamRuneContext.CURRENT, client)
            .call(() -> run(store, "l-traced", tracing));
    new TracingEventListener(e -> {}, tracing, SubscriptionName.of("untraced")).onEvents(untraced);

    assertThat(untraced.getFirst().metadata().traceId()).isEqualTo(TraceId.of(CLIENT_TRACE));
    assertThat(untraced.getFirst().metadata().spanContext()).isSameAs(SpanLink.NONE);
    assertThat(SpanLink.of(untraced.getFirst().metadata())).isSameAs(SpanLink.NONE);
    assertThat(
            only(TraceAttributes.subscriptionDeliverSpanName(SubscriptionName.of("untraced")))
                .getLinks())
        .isEmpty();
    SpanData command = only("command/Switch");
    assertThat(command.getTraceId()).isNotEqualTo(CLIENT_TRACE);
    assertThat(traced.getFirst().metadata().traceId()).isEqualTo(TraceId.of(CLIENT_TRACE));
    assertThat(traced.getFirst().metadata().spanContext().spanId().value())
        .isEqualTo(command.getSpanId());
    assertThat(traced.getFirst().metadata().spanContext().traceId().value())
        .isEqualTo(command.getTraceId());
  }

  @Test
  void anInterceptorTheApplicationRegisters_withoutTracingOnTheBuilder_writesTheSpanContext() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());
    InMemoryEventStore store = new InMemoryEventStore();
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .interceptors(new TracingCommandInterceptor(tracing))
            .build()) {
      bus.execute(new Switch("l-registered"));
    }

    SpanData command = only("command/Switch");
    assertThat(
            store.readStream(
                StreamId.of(LAMP, AggregateId.of("l-registered")), Version.initial(), 10))
        .hasSize(2)
        .allSatisfy(
            event ->
                assertThat(event.metadata().spanContext().spanId().value())
                    .isEqualTo(command.getSpanId()));
  }

  @Test
  void aCommandDispatchedFromAnAfterCallback_writesItsOwnSpan_andTheOuterKeepsTheOuterSpan() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());
    InMemoryEventStore store = new InMemoryEventStore();
    var busRef = new java.util.concurrent.atomic.AtomicReference<VirtualThreadCommandBus>();
    org.streamrune.core.CommandInterceptor dispatching =
        new org.streamrune.core.CommandInterceptor() {
          @Override
          public void after(CommandContext ctx) {
            // Runs while the outer command's span is still open: the tracing interceptor sits
            // ahead of this one, so its after() runs later.
            if (ctx.command() instanceof Switch s && s.lampId().equals("outer")) {
              busRef.get().execute(new Switch("inner"));
            }
          }
        };
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .interceptors(dispatching)
            .tracing(tracing)
            .build()) {
      busRef.set(bus);
      bus.execute(new Switch("outer"));
    }

    List<SpanData> commands =
        otelTesting.getSpans().stream().filter(s -> s.getName().equals("command/Switch")).toList();
    assertThat(commands).hasSize(2);
    SpanData inner = commands.getFirst(); // ends first
    SpanData outer = commands.get(1);
    assertThat(inner.getParentSpanId()).isEqualTo(outer.getSpanId());
    assertThat(store.readStream(StreamId.of(LAMP, AggregateId.of("outer")), Version.initial(), 10))
        .allSatisfy(
            e ->
                assertThat(e.metadata().spanContext().spanId().value())
                    .isEqualTo(outer.getSpanId()));
    assertThat(store.readStream(StreamId.of(LAMP, AggregateId.of("inner")), Version.initial(), 10))
        .hasSize(2)
        .allSatisfy(
            e ->
                assertThat(e.metadata().spanContext().spanId().value())
                    .isEqualTo(inner.getSpanId()));
  }

  @Test
  void anAsyncCommand_writesTheSpanOfItsOwnExecution() {
    StreamRuneTracing tracing = tracing(TracingSettings.allEnabled());
    InMemoryEventStore store = new InMemoryEventStore();
    try (var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .register(LAMP, Switch.class, c -> AggregateId.of(c.lampId()), new LampDecider())
            .tracing(tracing)
            .build()) {
      bus.executeAsync(new Switch("l-async")).join();
    }

    SpanData command = only("command/Switch");
    assertThat(
            store.readStream(StreamId.of(LAMP, AggregateId.of("l-async")), Version.initial(), 10))
        .hasSize(2)
        .allSatisfy(
            e -> {
              assertThat(e.metadata().spanContext().traceId().value())
                  .isEqualTo(command.getTraceId());
              assertThat(e.metadata().spanContext().spanId().value())
                  .isEqualTo(command.getSpanId());
            });
  }

  @Test
  void withTheCommandAreaOff_orNoTracing_theSpanContextIsNone() {
    List<EventEnvelope> areaOff =
        run(
            new InMemoryEventStore(),
            "l-off",
            tracing(TracingSettings.builder().area(TracingArea.COMMAND, false).build()));
    List<EventEnvelope> noop = run(new InMemoryEventStore(), "l-noop", StreamRuneTracing.NOOP);
    List<EventEnvelope> disabled =
        run(new InMemoryEventStore(), "l-disabled", tracing(TracingSettings.disabled()));

    assertThat(areaOff)
        .allSatisfy(e -> assertThat(e.metadata().spanContext()).isSameAs(SpanLink.NONE));
    assertThat(noop)
        .allSatisfy(e -> assertThat(e.metadata().spanContext()).isSameAs(SpanLink.NONE));
    assertThat(disabled)
        .allSatisfy(e -> assertThat(e.metadata().spanContext()).isSameAs(SpanLink.NONE));
  }

  @Test
  void theNoopScope_andAnUnstartedArea_haveNoLink() {
    assertThat(org.streamrune.core.tracing.TraceScope.NOOP.link()).isSameAs(SpanLink.NONE);
    StreamRuneTracing off =
        tracing(TracingSettings.builder().area(TracingArea.COMMAND, false).build());
    try (var scope =
        off.start(
            TracingArea.COMMAND,
            "command/X",
            org.streamrune.core.tracing.SpanKind.INTERNAL,
            org.streamrune.core.tracing.SpanAttributes.empty())) {
      assertThat(scope.link()).isSameAs(SpanLink.NONE);
    }
    StreamRuneTracing on = tracing(TracingSettings.allEnabled());
    String spanId;
    SpanLink link;
    try (var scope =
        on.start(
            TracingArea.COMMAND,
            "command/X",
            org.streamrune.core.tracing.SpanKind.INTERNAL,
            org.streamrune.core.tracing.SpanAttributes.empty())) {
      link = scope.link();
      spanId = io.opentelemetry.api.trace.Span.current().getSpanContext().getSpanId();
    }
    assertThat(link.isW3c()).isTrue();
    assertThat(link.spanId().value()).isEqualTo(spanId);
  }
}
