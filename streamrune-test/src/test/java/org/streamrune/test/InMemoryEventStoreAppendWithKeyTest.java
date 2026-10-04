package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class InMemoryEventStoreAppendWithKeyTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  record OrderPlaced(String orderId) implements DomainEvent {}

  private InMemoryCommandInbox inbox;
  private InMemoryEventStore store;
  private final StreamId streamId = StreamId.of(TYPE, AggregateId.of("order-99"));
  private final IdempotencyKey key = IdempotencyKey.of("cmd-abc-123");

  @BeforeEach
  void setUp() {
    inbox = new InMemoryCommandInbox();
    store = new InMemoryEventStore().withCommandInbox(inbox);
  }

  private EventEnvelope envelope(String id) {
    return new EventEnvelope(
        GlobalOffset.of(0), // placeholder; real offset assigned by the store
        streamId,
        Version.initial(),
        new EventType("OrderPlaced"),
        new OrderPlaced(id),
        new EventMetadata(
            EventId.of("evt-" + id),
            CommandId.of("cmd-" + id),
            null,
            null,
            CorrelationId.of("corr-" + id),
            null,
            null,
            Instant.now()));
  }

  @Test
  void firstAppend_returnsNotAlreadyApplied_andEventsAreVisible() {
    var result =
        store.appendWithKey(streamId, List.of(envelope("A")), Version.initial(), key, "PlaceOrder");

    assertFalse(result.alreadyApplied());
    assertEquals(1, result.globalOffsets().size());
    assertEquals(1, result.finalVersion().value());

    // Events visible in the global stream
    var globalEvents = store.readGlobalStream(GlobalOffset.of(0), 100);
    assertEquals(1, globalEvents.size());

    // Inbox entry is recorded
    assertTrue(inbox.find(key).isPresent());
    assertEquals(key, inbox.find(key).get().key());
  }

  @Test
  void secondAppend_sameKey_returnsAlreadyApplied_streamDoesNotGrow() {
    // First call
    var first =
        store.appendWithKey(streamId, List.of(envelope("A")), Version.initial(), key, "PlaceOrder");
    assertFalse(first.alreadyApplied());
    long globalSizeAfterFirst = store.readGlobalStream(GlobalOffset.of(0), 100).size();

    // Second call with same key (different events — should be ignored)
    var second =
        store.appendWithKey(
            streamId, List.of(envelope("B"), envelope("C")), new Version(1), key, "PlaceOrder");

    assertTrue(second.alreadyApplied());
    assertEquals(first.globalOffsets(), second.globalOffsets());
    assertEquals(first.finalVersion(), second.finalVersion());

    // Stream did NOT grow
    long globalSizeAfterSecond = store.readGlobalStream(GlobalOffset.of(0), 100).size();
    assertEquals(globalSizeAfterFirst, globalSizeAfterSecond);
  }

  @Test
  void zeroEventAppend_claimsKey_secondCallReturnsAlreadyApplied() {
    var result = store.appendWithKey(streamId, List.of(), Version.initial(), key, "NoOpCommand");

    assertFalse(result.alreadyApplied());
    assertTrue(result.globalOffsets().isEmpty());

    // Key is claimed
    assertTrue(inbox.find(key).isPresent());
    assertTrue(inbox.find(key).get().globalOffsets().isEmpty());

    // Second call — alreadyApplied
    var second =
        store.appendWithKey(
            streamId, List.of(envelope("A")), Version.initial(), key, "NoOpCommand");
    assertTrue(second.alreadyApplied());
    assertTrue(second.globalOffsets().isEmpty());
  }

  @Test
  void appendWithKey_withNoInboxWired_throwsIllegalStateException() {
    var unwiredStore = new InMemoryEventStore();
    assertThrows(
        IllegalStateException.class,
        () ->
            unwiredStore.appendWithKey(
                streamId, List.of(envelope("A")), Version.initial(), key, "PlaceOrder"));
  }

  @Test
  void twoDistinctKeys_bothProceed_independentResults() {
    var key2 = IdempotencyKey.of("cmd-xyz-456");

    var r1 =
        store.appendWithKey(streamId, List.of(envelope("A")), Version.initial(), key, "PlaceOrder");
    var r2 =
        store.appendWithKey(streamId, List.of(envelope("B")), new Version(1), key2, "PlaceOrder");

    assertFalse(r1.alreadyApplied());
    assertFalse(r2.alreadyApplied());
    assertEquals(1, r1.globalOffsets().size());
    assertEquals(1, r2.globalOffsets().size());
    // Different offsets assigned
    assertNotEquals(r1.globalOffsets(), r2.globalOffsets());

    assertTrue(inbox.find(key).isPresent());
    assertTrue(inbox.find(key2).isPresent());
  }

  /**
   * The test double must enforce the same key-collision contract as {@code PostgresEventStore}.
   * Before the fix this returned {@code alreadyApplied=true} carrying the OTHER command's offsets
   * and final version, so every in-memory test was a false witness to a defect that silently
   * discards a command in production.
   */
  @Test
  void sameKey_differentCommandType_isRejected() {
    store.appendWithKey(streamId, List.of(envelope("A")), Version.initial(), key, "PlaceOrder");

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.appendWithKey(
                    streamId, List.of(envelope("B")), new Version(1), key, "RefundPayment"));
    assertTrue(ex.getMessage().contains(key.value()), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("PlaceOrder") && ex.getMessage().contains("RefundPayment"),
        ex.getMessage());
    assertEquals(
        1,
        store.readStream(streamId, Version.initial(), 100).size(),
        "the rejected append must not have been persisted");
  }

  /** Same key, same command type, different stream: another aggregate's result. */
  @Test
  void sameKey_differentStream_isRejected() {
    store.appendWithKey(streamId, List.of(envelope("A")), Version.initial(), key, "PlaceOrder");

    var otherStream = StreamId.of(TYPE, AggregateId.of("order-100"));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                store.appendWithKey(
                    otherStream, List.of(envelope("B")), Version.initial(), key, "PlaceOrder"));
    assertTrue(ex.getMessage().contains(key.value()), ex.getMessage());
    assertTrue(
        ex.getMessage().contains(streamId.value()) && ex.getMessage().contains(otherStream.value()),
        ex.getMessage());
    assertTrue(
        store.readStream(otherStream, Version.initial(), 100).isEmpty(),
        "the rejected append must not have been persisted");
  }
}
