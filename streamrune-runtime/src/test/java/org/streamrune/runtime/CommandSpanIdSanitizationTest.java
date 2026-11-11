package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.tracing.TraceAttributes;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.LogSanitizer;

/**
 * The command span's identifier attributes pass {@link LogSanitizer#sanitizeForTrace(String)}: an
 * ordinary id is unchanged, an id with a control or format character loses it, and an id longer
 * than {@value LogSanitizer#MAX_TRACE_LENGTH} characters is cut there.
 */
class CommandSpanIdSanitizationTest {

  private static final AggregateType TYPE = AggregateType.of("orders");

  record Ship() implements Command {}

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private SpanData spanFor(CommandId commandId, AggregateId aggregateId) {
    var interceptor = new OpenTelemetryCommandInterceptor(otelTesting.getOpenTelemetry());
    var ctx =
        new CommandInterceptor.CommandContext(
            new Ship(), "Ship", commandId, TYPE, aggregateId, null, Instant.now());
    interceptor.before(ctx);
    interceptor.after(ctx);
    return otelTesting.getSpans().getFirst();
  }

  private static String attribute(SpanData span, String key) {
    return span.getAttributes().get(AttributeKey.stringKey(key));
  }

  @Test
  void ordinaryIds_areUnchanged() {
    SpanData span = spanFor(CommandId.of("cmd_42"), AggregateId.of("ord-42"));

    assertThat(attribute(span, TraceAttributes.COMMAND_ID)).isEqualTo("cmd_42");
    assertThat(attribute(span, TraceAttributes.AGGREGATE_ID)).isEqualTo("ord-42");
    assertThat(attribute(span, TraceAttributes.STREAM_ID)).isEqualTo("orders:ord-42");
  }

  @Test
  void controlAndFormatCharacters_areRemoved() {
    // The constructors (the read path) do not bound the charset; only of() does.
    SpanData span = spanFor(new CommandId("cmd_1\nforged=1"), new AggregateId("ord‮-7\r"));

    assertThat(attribute(span, TraceAttributes.COMMAND_ID)).isEqualTo("cmd_1forged=1");
    assertThat(attribute(span, TraceAttributes.AGGREGATE_ID)).isEqualTo("ord-7");
    assertThat(attribute(span, TraceAttributes.STREAM_ID)).isEqualTo("orders:ord-7");
  }

  @Test
  void idsLongerThanTheCap_areCutThere_andAnIdOfTheIngressMaximumIsWhole() {
    String longest = "a".repeat(255);
    SpanData span = spanFor(new CommandId("c".repeat(300)), AggregateId.of(longest));

    assertThat(attribute(span, TraceAttributes.COMMAND_ID))
        .isEqualTo("c".repeat(LogSanitizer.MAX_TRACE_LENGTH) + "...");
    assertThat(attribute(span, TraceAttributes.AGGREGATE_ID)).isEqualTo(longest);
    // orders:<255 characters> is 262 characters long.
    assertThat(attribute(span, TraceAttributes.STREAM_ID))
        .isEqualTo(("orders:" + longest).substring(0, LogSanitizer.MAX_TRACE_LENGTH) + "...");
  }
}
