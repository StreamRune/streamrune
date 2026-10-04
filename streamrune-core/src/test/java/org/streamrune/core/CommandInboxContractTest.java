package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/** Validates {@link CommandInbox.InboxResult} null-guards and defensive copy semantics. */
class CommandInboxContractTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  private static final IdempotencyKey KEY = IdempotencyKey.of("test-key-1");
  private static final String CMD_TYPE = "com.example.CreateOrder";
  private static final StreamId STREAM = StreamId.of(TYPE, AggregateId.of("order-stream-1"));
  private static final Version VERSION = new Version(3);
  private static final List<GlobalOffset> OFFSETS = List.of(GlobalOffset.of(1L));
  private static final Instant NOW = Instant.parse("2026-06-30T10:00:00Z");

  @Test
  void inboxResult_nullKey_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> new CommandInbox.InboxResult(null, CMD_TYPE, STREAM, VERSION, OFFSETS, NOW));
  }

  @Test
  void inboxResult_nullCommandType_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> new CommandInbox.InboxResult(KEY, null, STREAM, VERSION, OFFSETS, NOW));
  }

  @Test
  void inboxResult_nullStreamId_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> new CommandInbox.InboxResult(KEY, CMD_TYPE, null, VERSION, OFFSETS, NOW));
  }

  @Test
  void inboxResult_nullFinalVersion_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> new CommandInbox.InboxResult(KEY, CMD_TYPE, STREAM, null, OFFSETS, NOW));
  }

  @Test
  void inboxResult_nullGlobalOffsets_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> new CommandInbox.InboxResult(KEY, CMD_TYPE, STREAM, VERSION, null, NOW));
  }

  @Test
  void inboxResult_nullProcessedAt_throwsNullPointerException() {
    assertThrows(
        NullPointerException.class,
        () -> new CommandInbox.InboxResult(KEY, CMD_TYPE, STREAM, VERSION, OFFSETS, null));
  }

  @Test
  void inboxResult_globalOffsets_areDefensivelyCopied() {
    var mutableOffsets = new ArrayList<>(List.of(GlobalOffset.of(10L)));
    var result = new CommandInbox.InboxResult(KEY, CMD_TYPE, STREAM, VERSION, mutableOffsets, NOW);
    mutableOffsets.add(GlobalOffset.of(99L)); // mutate source after construction
    assertEquals(List.of(GlobalOffset.of(10L)), result.globalOffsets());
    assertThrows(
        UnsupportedOperationException.class, () -> result.globalOffsets().add(GlobalOffset.of(1L)));
  }

  @Test
  void inboxResult_validArgs_constructsCorrectly() {
    var result = new CommandInbox.InboxResult(KEY, CMD_TYPE, STREAM, VERSION, OFFSETS, NOW);
    assertEquals(KEY, result.key());
    assertEquals(CMD_TYPE, result.commandType());
    assertEquals(STREAM, result.streamId());
    assertEquals(VERSION, result.finalVersion());
    assertEquals(OFFSETS, result.globalOffsets());
    assertEquals(NOW, result.processedAt());
  }

  // === requireBoundTo — the single shared key-collision guard ===

  @Test
  void requireBoundTo_sameCommandTypeAndStream_isAReplayAndPasses() {
    var result = new CommandInbox.InboxResult(KEY, CMD_TYPE, STREAM, VERSION, OFFSETS, NOW);
    assertDoesNotThrow(() -> result.requireBoundTo(CMD_TYPE, STREAM));
  }

  @Test
  void requireBoundTo_differentCommandType_throwsNamingKeyAndBothTypes() {
    var result = new CommandInbox.InboxResult(KEY, CMD_TYPE, STREAM, VERSION, OFFSETS, NOW);
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> result.requireBoundTo("com.example.RefundPayment", STREAM));
    assertTrue(ex.getMessage().contains(KEY.value()), ex.getMessage());
    assertTrue(ex.getMessage().contains(CMD_TYPE), ex.getMessage());
    assertTrue(ex.getMessage().contains("com.example.RefundPayment"), ex.getMessage());
  }

  @Test
  void requireBoundTo_differentStream_throwsNamingKeyAndBothStreams() {
    var result = new CommandInbox.InboxResult(KEY, CMD_TYPE, STREAM, VERSION, OFFSETS, NOW);
    var other = StreamId.of(TYPE, AggregateId.of("order-stream-2"));
    var ex =
        assertThrows(IllegalArgumentException.class, () -> result.requireBoundTo(CMD_TYPE, other));
    assertTrue(ex.getMessage().contains(KEY.value()), ex.getMessage());
    assertTrue(ex.getMessage().contains(STREAM.value()), ex.getMessage());
    assertTrue(ex.getMessage().contains(other.value()), ex.getMessage());
  }

  @Test
  void requireBoundTo_refusesTheSameIdValueUnderAnotherType() {
    var recorded =
        new CommandInbox.InboxResult(
            IdempotencyKey.of("k-1"),
            "CreateProduct",
            StreamId.of(AggregateType.of("product"), AggregateId.of("x")),
            new Version(1),
            List.of(),
            Instant.now());
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                recorded.requireBoundTo(
                    "CreateProduct",
                    StreamId.of(AggregateType.of("inventory"), AggregateId.of("x"))));
    assertTrue(ex.getMessage().contains("product:x"), ex.getMessage());
    assertTrue(ex.getMessage().contains("inventory:x"), ex.getMessage());
  }

  /**
   * The key is caller-supplied text, and this message is persisted — {@code
   * AuditCommandInterceptor.onError} writes it to {@code audit_log} — and logged. A key or stream
   * id carrying CR/LF must not reach either verbatim, so all three ids render through {@link
   * LogSanitizer#sanitizeForLog}. A key built through the canonical constructor (the decode door,
   * which keeps no charset rule) is exactly how such a value arrives.
   */
  @Test
  void requireBoundTo_rendersTheKeyAndBothStreamIdsSanitized() {
    var forgedKey = new IdempotencyKey("k1\n2026-10-02 10:00:00 WARN forged\r\u0000");
    var recordedStream = StreamId.of(TYPE, new AggregateId("order\n-1"));
    var presentedStream = StreamId.of(TYPE, new AggregateId("order\r-2"));
    var result =
        new CommandInbox.InboxResult(forgedKey, CMD_TYPE, recordedStream, VERSION, OFFSETS, NOW);

    var ex =
        assertThrows(
            IllegalArgumentException.class, () -> result.requireBoundTo(CMD_TYPE, presentedStream));

    String message = ex.getMessage();
    assertFalse(message.contains("\n"), message);
    assertFalse(message.contains("\r"), message);
    assertFalse(message.contains("\u0000"), message);
    assertTrue(message.contains(LogSanitizer.sanitizeForLog(forgedKey.value())), message);
    assertTrue(message.contains(LogSanitizer.sanitizeForLog(recordedStream.value())), message);
    assertTrue(message.contains(LogSanitizer.sanitizeForLog(presentedStream.value())), message);
    assertTrue(message.contains(CMD_TYPE), message);
  }
}
