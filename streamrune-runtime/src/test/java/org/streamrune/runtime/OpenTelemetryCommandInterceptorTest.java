package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class OpenTelemetryCommandInterceptorTest {

  private static final AggregateType TYPE = AggregateType.of("orders");

  record TestCommand() implements Command {}

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private CommandInterceptor.CommandContext ctx(String commandType) {
    return new CommandInterceptor.CommandContext(
        new TestCommand(),
        commandType,
        CommandId.of(UUID.randomUUID().toString()),
        TYPE,
        AggregateId.of("agg-1"),
        null,
        Instant.now());
  }

  private CommandInterceptor.CommandContext ctxWithResult(String commandType) {
    CommandBus.CommandResult result =
        new CommandBus.CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("agg-1")),
            new Version(1),
            List.of(GlobalOffset.of(1)));
    return new CommandInterceptor.CommandContext(
        new TestCommand(),
        commandType,
        CommandId.of(UUID.randomUUID().toString()),
        TYPE,
        AggregateId.of("agg-1"),
        result,
        Instant.now());
  }

  @Test
  void shouldCreateSpanForSuccessfulCommand() {
    var interceptor = new OpenTelemetryCommandInterceptor(otelTesting.getOpenTelemetry());

    var ctx = ctx("PlaceOrder");
    interceptor.before(ctx);
    interceptor.after(ctxWithResult("PlaceOrder"));

    List<SpanData> spans = otelTesting.getSpans();
    assertEquals(1, spans.size());
    SpanData span = spans.get(0);
    assertEquals(TraceAttributes.commandSpanName("PlaceOrder"), span.getName());
    assertEquals(SpanKind.INTERNAL, span.getKind());
    assertEquals(StatusCode.OK, span.getStatus().getStatusCode());
    assertEquals("PlaceOrder", span.getAttributes().get(TraceAttributes.COMMAND_TYPE));
    assertEquals("orders", span.getAttributes().get(TraceAttributes.AGGREGATE_TYPE));
    assertEquals("orders:agg-1", span.getAttributes().get(TraceAttributes.STREAM_ID));
  }

  /**
   * The span analogue of the metric-name drift guard. {@code docs/guide/production.md} publishes
   * the command span's name and attribute keys as an operator contract; before this the emitted
   * attributes were {@code command.type} / {@code command.id} / {@code aggregate.id} (no {@code
   * streamrune.} prefix on any of them) and {@code stream.id} was never set at all, so every
   * Jaeger/Tempo query written against the published contract returned zero results. This pins the
   * EXACT key set so a rename cannot silently diverge again.
   */
  @Test
  void spanNameAndAttributeKeysMatchThePublishedContract() {
    var interceptor = new OpenTelemetryCommandInterceptor(otelTesting.getOpenTelemetry());

    interceptor.before(ctx("PlaceOrder"));
    interceptor.after(ctxWithResult("PlaceOrder"));

    SpanData span = otelTesting.getSpans().getFirst();
    assertEquals("command/PlaceOrder", span.getName());
    assertEquals(
        java.util.Set.of(
            "streamrune.command.type",
            "streamrune.command.id",
            "streamrune.aggregate.type",
            "streamrune.aggregate.id",
            "streamrune.stream.id"),
        span.getAttributes().asMap().keySet().stream()
            .map(io.opentelemetry.api.common.AttributeKey::getKey)
            .collect(java.util.stream.Collectors.toSet()),
        "the emitted attribute keys are the published contract — no more, no less");
    assertEquals(
        "PlaceOrder",
        span.getAttributes()
            .get(io.opentelemetry.api.common.AttributeKey.stringKey("streamrune.command.type")));
    assertEquals(
        "agg-1",
        span.getAttributes()
            .get(io.opentelemetry.api.common.AttributeKey.stringKey("streamrune.aggregate.id")));
    assertEquals(
        "orders",
        span.getAttributes()
            .get(io.opentelemetry.api.common.AttributeKey.stringKey("streamrune.aggregate.type")));
    assertEquals(
        "orders:agg-1",
        span.getAttributes()
            .get(io.opentelemetry.api.common.AttributeKey.stringKey("streamrune.stream.id")),
        "stream.id is documented and must actually be emitted; it is known before the command runs");
  }

  @Test
  void shouldSetErrorStatusOnException() {
    var interceptor = new OpenTelemetryCommandInterceptor(otelTesting.getOpenTelemetry());

    var ctx = ctx("PlaceOrder");
    interceptor.before(ctx);
    interceptor.onError(ctx, new RuntimeException("conflict"));

    List<SpanData> spans = otelTesting.getSpans();
    assertEquals(1, spans.size());
    assertEquals(StatusCode.ERROR, spans.get(0).getStatus().getStatusCode());
    assertFalse(spans.get(0).getEvents().isEmpty());
    // The stream is composed before any interceptor runs, so a failed command's span names it too.
    assertEquals("orders", spans.get(0).getAttributes().get(TraceAttributes.AGGREGATE_TYPE));
    assertEquals("orders:agg-1", spans.get(0).getAttributes().get(TraceAttributes.STREAM_ID));
  }

  @Test
  void aContextWithoutAStream_carriesNeitherTypeNorStreamAttribute() {
    var interceptor = new OpenTelemetryCommandInterceptor(otelTesting.getOpenTelemetry());
    var ctx =
        new CommandInterceptor.CommandContext(
            new TestCommand(),
            "PlaceOrder",
            CommandId.of(UUID.randomUUID().toString()),
            null,
            null,
            null,
            Instant.now());

    interceptor.before(ctx);
    interceptor.onError(ctx, new RuntimeException("no stream"));

    SpanData span = otelTesting.getSpans().getFirst();
    assertNull(span.getAttributes().get(TraceAttributes.AGGREGATE_TYPE));
    assertNull(span.getAttributes().get(TraceAttributes.AGGREGATE_ID));
    assertNull(span.getAttributes().get(TraceAttributes.STREAM_ID));
  }

  @Test
  void beforeShouldReturnTrue() {
    var interceptor = new OpenTelemetryCommandInterceptor(otelTesting.getOpenTelemetry());
    boolean result = interceptor.before(ctx("PlaceOrder"));
    assertTrue(result);
    // clean up span
    interceptor.after(ctxWithResult("PlaceOrder"));
  }
}
