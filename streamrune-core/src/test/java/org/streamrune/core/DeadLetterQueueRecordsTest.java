package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

class DeadLetterQueueRecordsTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  @Test
  void publishRequestAccessors() {
    Instant now = Instant.now();
    var req =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{\"x\":1}",
            "CommandType",
            CommandId.of("cmd_1"),
            StreamId.of(TYPE, AggregateId.of("stream-1")),
            "RuntimeException",
            "boom",
            3,
            now,
            null,
            null,
            null,
            null);

    assertEquals("{\"x\":1}", req.commandPayload());
    assertEquals("CommandType", req.commandType());
    assertEquals(CommandId.of("cmd_1"), req.commandId());
    assertEquals(StreamId.of(TYPE, AggregateId.of("stream-1")), req.streamId());
    assertEquals(AggregateId.of("stream-1"), req.aggregateId());
    assertEquals("RuntimeException", req.errorType());
    assertEquals("boom", req.errorMessage());
    assertEquals(3, req.attempts());
    assertEquals(now, req.timestamp());
  }

  @Test
  void publishRequestCarriesRequestContextFields() {
    Instant now = Instant.now();
    var req =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "CommandType",
            CommandId.of("cmd_1"),
            StreamId.of(TYPE, AggregateId.of("stream-1")),
            "RuntimeException",
            "boom",
            3,
            now,
            CorrelationId.of("corr-9"),
            UserId.of("user-7"),
            TraceId.of("trace-3"),
            null);

    assertEquals(CorrelationId.of("corr-9"), req.correlationId());
    assertEquals(UserId.of("user-7"), req.userId());
    assertEquals(TraceId.of("trace-3"), req.traceId());
  }

  @Test
  void publishRequestWithNullContextFieldsLeavesContextNull() {
    var req =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "CommandType",
            CommandId.of("cmd_1"),
            StreamId.of(TYPE, AggregateId.of("stream-1")),
            "RuntimeException",
            "boom",
            3,
            Instant.now(),
            null,
            null,
            null,
            null);

    assertNull(req.correlationId());
    assertNull(req.userId());
    assertNull(req.traceId());
  }

  @Test
  void entryAccessors() {
    Instant first = Instant.parse("2026-01-01T00:00:00Z");
    Instant published = Instant.parse("2026-01-02T00:00:00Z");
    var entry =
        new DeadLetterQueue.DeadLetterEntry(
            CommandId.of("cmd_1"),
            "CommandType",
            "{}",
            StreamId.of(TYPE, AggregateId.of("stream-1")),
            "Err",
            "msg",
            2,
            first,
            published,
            0,
            null,
            null,
            null,
            null,
            null);

    assertEquals(CommandId.of("cmd_1"), entry.commandId());
    assertEquals(first, entry.firstAttemptAt());
    assertEquals(published, entry.publishedAt());
    assertEquals(2, entry.attempts());
  }

  @Test
  void entryWithRetryFieldsAccessors() {
    Instant first = Instant.parse("2026-01-01T00:00:00Z");
    Instant published = Instant.parse("2026-01-02T00:00:00Z");
    Instant lastAttempt = Instant.parse("2026-01-03T00:00:00Z");
    var entry =
        new DeadLetterQueue.DeadLetterEntry(
            CommandId.of("cmd_1"),
            "CommandType",
            "{}",
            StreamId.of(TYPE, AggregateId.of("stream-1")),
            "Err",
            "msg",
            2,
            first,
            published,
            3,
            lastAttempt,
            null,
            null,
            null,
            null);

    assertEquals(3, entry.dlqAttempts());
    assertEquals(lastAttempt, entry.lastAttemptAt());
  }

  @Test
  void entryWithZeroDlqAttemptsAndNullContextSetsDefaults() {
    Instant first = Instant.parse("2026-01-01T00:00:00Z");
    Instant published = Instant.parse("2026-01-02T00:00:00Z");
    var entry =
        new DeadLetterQueue.DeadLetterEntry(
            CommandId.of("cmd_1"),
            "CommandType",
            "{}",
            StreamId.of(TYPE, AggregateId.of("stream-1")),
            "Err",
            "msg",
            2,
            first,
            published,
            0,
            null,
            null,
            null,
            null,
            null);

    assertEquals(0, entry.dlqAttempts());
    assertNull(entry.lastAttemptAt());
  }

  @Test
  void entryCarriesRequestContextFields() {
    Instant first = Instant.parse("2026-01-01T00:00:00Z");
    Instant published = Instant.parse("2026-01-02T00:00:00Z");
    var entry =
        new DeadLetterQueue.DeadLetterEntry(
            CommandId.of("cmd_1"),
            "CommandType",
            "{}",
            StreamId.of(TYPE, AggregateId.of("stream-1")),
            "Err",
            "msg",
            2,
            first,
            published,
            3,
            null,
            CorrelationId.of("corr-9"),
            UserId.of("user-7"),
            TraceId.of("trace-3"),
            null);

    assertEquals(CorrelationId.of("corr-9"), entry.correlationId());
    assertEquals(UserId.of("user-7"), entry.userId());
    assertEquals(TraceId.of("trace-3"), entry.traceId());
  }

  @Test
  void entryWithNullContextFieldsLeavesContextNull() {
    Instant first = Instant.parse("2026-01-01T00:00:00Z");
    Instant published = Instant.parse("2026-01-02T00:00:00Z");
    var entry =
        new DeadLetterQueue.DeadLetterEntry(
            CommandId.of("cmd_1"),
            "CommandType",
            "{}",
            StreamId.of(TYPE, AggregateId.of("stream-1")),
            "Err",
            "msg",
            2,
            first,
            published,
            3,
            null,
            null,
            null,
            null,
            null);

    assertNull(entry.correlationId());
    assertNull(entry.userId());
    assertNull(entry.traceId());
  }

  // --- DeadLetterPublishRequest validation ---

  private static DeadLetterQueue.DeadLetterPublishRequest request(
      String payload,
      String type,
      CommandId commandId,
      String errorType,
      int attempts,
      Instant ts) {
    return new DeadLetterQueue.DeadLetterPublishRequest(
        payload,
        type,
        commandId,
        StreamId.of(TYPE, AggregateId.of("s1")),
        errorType,
        "msg",
        attempts,
        ts,
        null,
        null,
        null,
        null);
  }

  @Test
  void publishRequestRejectsNullCommandPayload() {
    assertThrows(
        NullPointerException.class,
        () -> request(null, "Cmd", CommandId.of("cmd_1"), "Err", 1, Instant.now()));
  }

  @Test
  void publishRequestRejectsNullCommandType() {
    assertThrows(
        NullPointerException.class,
        () -> request("{}", null, CommandId.of("cmd_1"), "Err", 1, Instant.now()));
  }

  @Test
  void publishRequestRejectsBlankCommandType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> request("{}", "  ", CommandId.of("cmd_1"), "Err", 1, Instant.now()));
  }

  @Test
  void publishRequestRejectsNullCommandId() {
    assertThrows(
        NullPointerException.class, () -> request("{}", "Cmd", null, "Err", 1, Instant.now()));
  }

  @Test
  void publishRequestRejectsNullErrorType() {
    assertThrows(
        NullPointerException.class,
        () -> request("{}", "Cmd", CommandId.of("cmd_1"), null, 1, Instant.now()));
  }

  @Test
  void publishRequestRejectsBlankErrorType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> request("{}", "Cmd", CommandId.of("cmd_1"), "  ", 1, Instant.now()));
  }

  @Test
  void publishRequestRejectsNullTimestamp() {
    assertThrows(
        NullPointerException.class,
        () -> request("{}", "Cmd", CommandId.of("cmd_1"), "Err", 1, null));
  }

  @Test
  void publishRequestRejectsNegativeAttempts() {
    assertThrows(
        IllegalArgumentException.class,
        () -> request("{}", "Cmd", CommandId.of("cmd_1"), "Err", -1, Instant.now()));
  }

  @Test
  void publishRequestAcceptsNullableFields() {
    var req =
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Cmd",
            CommandId.of("cmd_1"),
            null,
            "Err",
            null,
            0,
            Instant.now(),
            null,
            null,
            null,
            null);
    assertNull(req.streamId());
    assertNull(req.aggregateId());
    assertNull(req.errorMessage());
  }

  // --- DeadLetterEntry validation ---

  private static DeadLetterQueue.DeadLetterEntry entry(
      CommandId commandId,
      String type,
      String payload,
      String errorType,
      int attempts,
      Instant first,
      Instant published,
      int dlqAttempts) {
    return new DeadLetterQueue.DeadLetterEntry(
        commandId,
        type,
        payload,
        StreamId.of(TYPE, AggregateId.of("s1")),
        errorType,
        "msg",
        attempts,
        first,
        published,
        dlqAttempts,
        null,
        null,
        null,
        null,
        null);
  }

  @Test
  void entryRejectsNullCommandId() {
    assertThrows(
        NullPointerException.class,
        () -> entry(null, "Cmd", "{}", "Err", 1, Instant.now(), Instant.now(), 0));
  }

  @Test
  void entryRejectsNullCommandType() {
    assertThrows(
        NullPointerException.class,
        () -> entry(CommandId.of("cmd_1"), null, "{}", "Err", 1, Instant.now(), Instant.now(), 0));
  }

  @Test
  void entryRejectsBlankCommandType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> entry(CommandId.of("cmd_1"), "  ", "{}", "Err", 1, Instant.now(), Instant.now(), 0));
  }

  @Test
  void entryRejectsNullCommandPayload() {
    assertThrows(
        NullPointerException.class,
        () -> entry(CommandId.of("cmd_1"), "Cmd", null, "Err", 1, Instant.now(), Instant.now(), 0));
  }

  @Test
  void entryRejectsNullErrorType() {
    assertThrows(
        NullPointerException.class,
        () -> entry(CommandId.of("cmd_1"), "Cmd", "{}", null, 1, Instant.now(), Instant.now(), 0));
  }

  @Test
  void entryRejectsBlankErrorType() {
    assertThrows(
        IllegalArgumentException.class,
        () -> entry(CommandId.of("cmd_1"), "Cmd", "{}", "  ", 1, Instant.now(), Instant.now(), 0));
  }

  @Test
  void entryRejectsNullFirstAttemptAt() {
    assertThrows(
        NullPointerException.class,
        () -> entry(CommandId.of("cmd_1"), "Cmd", "{}", "Err", 1, null, Instant.now(), 0));
  }

  @Test
  void entryRejectsNullPublishedAt() {
    assertThrows(
        NullPointerException.class,
        () -> entry(CommandId.of("cmd_1"), "Cmd", "{}", "Err", 1, Instant.now(), null, 0));
  }

  @Test
  void entryRejectsNegativeAttempts() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            entry(CommandId.of("cmd_1"), "Cmd", "{}", "Err", -1, Instant.now(), Instant.now(), 0));
  }

  @Test
  void entryRejectsNegativeDlqAttempts() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            entry(CommandId.of("cmd_1"), "Cmd", "{}", "Err", 1, Instant.now(), Instant.now(), -1));
  }

  @Test
  void entryAcceptsNullableFields() {
    var e =
        new DeadLetterQueue.DeadLetterEntry(
            CommandId.of("cmd_1"),
            "Cmd",
            "{}",
            null,
            "Err",
            null,
            0,
            Instant.now(),
            Instant.now(),
            0,
            null,
            null,
            null,
            null,
            null);
    assertNull(e.streamId());
    assertNull(e.aggregateId());
    assertNull(e.errorMessage());
    assertNull(e.lastAttemptAt());
  }

  // --- the stream holds the id once ---

  private static DeadLetterQueue.DeadLetterPublishRequest requestWith(StreamId stream) {
    return new DeadLetterQueue.DeadLetterPublishRequest(
        "{}",
        "CommandType",
        CommandId.of("cmd_1"),
        stream,
        "RuntimeException",
        "boom",
        3,
        Instant.parse("2026-01-01T00:00:00Z"),
        null,
        null,
        null,
        null);
  }

  private static DeadLetterQueue.DeadLetterEntry entryWith(StreamId stream) {
    return new DeadLetterQueue.DeadLetterEntry(
        CommandId.of("cmd_1"),
        "CommandType",
        "{}",
        stream,
        "Err",
        "msg",
        2,
        Instant.parse("2026-01-01T00:00:00Z"),
        Instant.parse("2026-01-02T00:00:00Z"),
        0,
        null,
        null,
        null,
        null,
        null);
  }

  @Test
  void requestAndEntry_deriveAggregateIdFromTheStream_nullSafe() {
    var stream = StreamId.of(AggregateType.of("inventory"), AggregateId.of("p-1"));
    var request = requestWith(stream);
    assertEquals(AggregateId.of("p-1"), request.aggregateId());
    assertNull(requestWith(null).aggregateId());
    var entry = entryWith(stream);
    assertEquals(AggregateId.of("p-1"), entry.aggregateId());
    assertNull(entryWith(null).aggregateId());
  }

  @Test
  void neitherRecordHasAnAggregateIdComponent() {
    for (var type :
        List.of(
            DeadLetterQueue.DeadLetterPublishRequest.class,
            DeadLetterQueue.DeadLetterEntry.class)) {
      assertTrue(
          Arrays.stream(type.getRecordComponents())
              .noneMatch(c -> c.getName().equals("aggregateId")),
          type.getSimpleName() + " stores the id once, inside streamId");
    }
  }
}
