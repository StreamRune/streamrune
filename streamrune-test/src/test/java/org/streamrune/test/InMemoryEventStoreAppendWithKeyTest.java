package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
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

  private static final EventTypeRegistry TYPES =
      SimpleEventTypeRegistry.builder().registerEvent(OrderPlaced.class).build();

  private static final AggregateType TYPE = AggregateType.of("order");

  record OrderPlaced(String orderId) implements DomainEvent {}

  private InMemoryCommandInbox inbox;
  private InMemoryEventStore store;
  private final StreamId streamId = StreamId.of(TYPE, AggregateId.of("order-99"));
  private final IdempotencyKey key = IdempotencyKey.of("cmd-abc-123");

  @BeforeEach
  void setUp() {
    inbox = new InMemoryCommandInbox();
    store = new InMemoryEventStore(TYPES).withCommandInbox(inbox);
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
            CorrelationId.of("corr-" + id),
            null,
            null,
            Instant.now()));
  }

  @Test
  void appendWithKey_withNoInboxWired_isUnsupported_beforeItsArgumentsAreChecked() {
    var unwiredStore = new InMemoryEventStore(TYPES);
    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                unwiredStore.appendWithKey(
                    streamId, List.of(envelope("A")), Version.initial(), key, "PlaceOrder"));
    assertEquals("No InMemoryCommandInbox wired; call withCommandInbox(...)", ex.getMessage());
    assertThrows(
        UnsupportedOperationException.class,
        () -> unwiredStore.appendWithKey(null, null, null, null, null),
        "the missing inbox is reported before a missing argument");
    assertTrue(unwiredStore.readStream(streamId, Version.initial(), 10).isEmpty());
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
