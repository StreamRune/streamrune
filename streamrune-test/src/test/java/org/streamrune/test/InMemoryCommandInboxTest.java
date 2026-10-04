package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandInbox;
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
  void find_returnsEmpty_whenNothingRecorded() {
    assertTrue(inbox.find(key).isEmpty());
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
  void deleteProcessedBefore_removesOnlyOldRows() {
    var cutoff = Instant.parse("2024-01-01T00:00:00Z");
    var keyOld = IdempotencyKey.of("old");
    var keyNew = IdempotencyKey.of("new");
    var keyExact = IdempotencyKey.of("exact");

    inbox.record(makeResult(keyOld, cutoff.minusSeconds(1)));
    inbox.record(makeResult(keyNew, cutoff.plusSeconds(1)));
    inbox.record(makeResult(keyExact, cutoff)); // exactly at cutoff — NOT before, must survive

    int deleted = inbox.deleteProcessedBefore(cutoff);

    assertEquals(1, deleted);
    assertFalse(inbox.find(keyOld).isPresent());
    assertTrue(inbox.find(keyNew).isPresent());
    assertTrue(inbox.find(keyExact).isPresent());
  }

  @Test
  void deleteProcessedBefore_returnsZero_whenNothingToDelete() {
    var future = Instant.parse("2099-01-01T00:00:00Z");
    inbox.record(makeResult(key, future));
    assertEquals(0, inbox.deleteProcessedBefore(Instant.now()));
  }

  @Test
  void deleteProcessedBefore_removesAll_whenAllOld() {
    var past = Instant.parse("2000-01-01T00:00:00Z");
    inbox.record(makeResult(IdempotencyKey.of("a"), past));
    inbox.record(makeResult(IdempotencyKey.of("b"), past));
    assertEquals(2, inbox.deleteProcessedBefore(Instant.now()));
    assertTrue(inbox.find(IdempotencyKey.of("a")).isEmpty());
    assertTrue(inbox.find(IdempotencyKey.of("b")).isEmpty());
  }

  @Test
  void implementsCommandInbox() {
    // Verifies structural contract: InMemoryCommandInbox IS-A CommandInbox
    CommandInbox asInterface = inbox;
    assertTrue(asInterface.find(key).isEmpty());
  }
}
