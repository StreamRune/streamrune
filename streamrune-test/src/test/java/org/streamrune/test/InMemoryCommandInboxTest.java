package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInbox.InboxResult;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

class InMemoryCommandInboxTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  private final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  private final IdempotencyKey key = IdempotencyKey.of("cmd-1");
  private final StreamId streamId = StreamId.of(TYPE, AggregateId.of("order-42"));

  private InboxResult makeResult(IdempotencyKey k, Instant processedAt) {
    return new InboxResult(
        k, "PlaceOrder", streamId, new Version(1), List.of(GlobalOffset.of(1L)), processedAt);
  }

  @Test
  void find_returnsResult_afterRecord() {
    var result = makeResult(key, Instant.now());
    inbox.record(result);
    var found = inbox.find(key);
    assertTrue(found.isPresent());
    assertEquals(result, found.get());
  }

  @Test
  void setProcessedAt_refusesAKeyWithNoRow() {
    Instant at = Instant.parse("2026-01-01T00:00:00Z");

    assertThrows(IllegalStateException.class, () -> inbox.setProcessedAt(key, at));
    assertTrue(inbox.find(key).isEmpty());
  }
}
