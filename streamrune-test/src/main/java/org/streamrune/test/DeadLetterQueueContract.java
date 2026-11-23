package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterQueue.DeadLetterEntry;
import org.streamrune.core.DeadLetterQueue.DeadLetterPublishRequest;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

/**
 * The behaviour every {@link DeadLetterQueue} must honour, taken from the interface's Javadoc: what
 * {@code publish} stores and {@code read} returns, newest-first {@code read} order, {@code find},
 * {@code discard}, the retry bookkeeping ({@code updateAttempts}, {@code readRetryable}), the
 * counts, the lookup by idempotency key with its one-entry-per-command-under-a-key rule, and
 * retention pruning. Run by {@code InMemoryDeadLetterQueueContractTest} and {@code
 * PostgresDeadLetterQueueContractTest}.
 *
 * <p>Deliberately not covered, because the interface leaves it open: what a second publish of an
 * already-stored {@code commandId} does ("implementations may reject a duplicate"), and the
 * rejection of null arguments.
 *
 * <p>{@code publishedAt} is stamped by the queue, so tests that depend on the order of publishing
 * pause briefly between publishes ({@link #tick()}).
 */
public abstract class DeadLetterQueueContract {

  private static final Instant T0 = Instant.parse("2026-01-01T00:00:00Z");
  private static final String PAYLOAD = "{\"amount\":42}";
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String TYPE = "com.example.PlaceOrder";

  /** A fresh, empty queue. */
  protected abstract DeadLetterQueue newQueue();

  private DeadLetterQueue queue;

  @BeforeEach
  void freshQueue() {
    queue = newQueue();
  }

  // ---------------------------------------------------------------- helpers

  /**
   * The payload is a JSON document: a store may normalise its text (key order, whitespace), so it
   * must read back as the same JSON, not necessarily the same characters.
   */
  private static void assertSamePayload(String expected, String actual) {
    try {
      assertEquals(JSON.readTree(expected), JSON.readTree(actual));
    } catch (JsonProcessingException e) {
      throw new AssertionError("payload is not JSON: " + actual, e);
    }
  }

  /** Lets the queue's own clock advance so consecutive publishedAt values differ. */
  private static void tick() {
    try {
      Thread.sleep(20);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  private static StreamId stream(String id) {
    return StreamId.of(AggregateType.of("order"), AggregateId.of(id));
  }

  private static DeadLetterPublishRequest request(
      String commandId, String commandType, StreamId streamId, IdempotencyKey key, Instant first) {
    return new DeadLetterPublishRequest(
        PAYLOAD,
        commandType,
        CommandId.of(commandId),
        streamId,
        "java.lang.IllegalStateException",
        "boom",
        3,
        first,
        null,
        null,
        null,
        key);
  }

  private static DeadLetterPublishRequest unkeyed(String commandId) {
    return request(commandId, TYPE, null, null, T0);
  }

  private static DeadLetterPublishRequest keyed(String commandId, String key) {
    return request(commandId, TYPE, null, IdempotencyKey.of(key), T0);
  }

  private static List<String> ids(List<DeadLetterEntry> entries) {
    return entries.stream().map(e -> e.commandId().value()).toList();
  }

  private void publishAll(String... commandIds) {
    for (String id : commandIds) {
      queue.publish(unkeyed(id));
      tick();
    }
  }

  // ---------------------------------------------------------------- publish / read

  @Test
  void publishStoresEveryField() {
    queue.publish(
        new DeadLetterPublishRequest(
            PAYLOAD,
            TYPE,
            CommandId.of("c-1"),
            stream("order-1"),
            "java.lang.IllegalStateException",
            "boom",
            3,
            T0,
            CorrelationId.of("corr-1"),
            UserId.of("user-1"),
            TraceId.of("0af7651916cd43dd8448eb211c80319c"),
            IdempotencyKey.of("key-1")));

    List<DeadLetterEntry> entries = queue.read(10);

    assertEquals(1, entries.size());
    DeadLetterEntry e = entries.getFirst();
    assertEquals(CommandId.of("c-1"), e.commandId());
    assertEquals(TYPE, e.commandType());
    assertSamePayload(PAYLOAD, e.commandPayload());
    assertEquals(stream("order-1"), e.streamId());
    assertEquals(AggregateId.of("order-1"), e.aggregateId());
    assertEquals("java.lang.IllegalStateException", e.errorType());
    assertEquals("boom", e.errorMessage());
    assertEquals(3, e.attempts());
    assertEquals(T0, e.firstAttemptAt());
    assertEquals(CorrelationId.of("corr-1"), e.correlationId());
    assertEquals(UserId.of("user-1"), e.userId());
    assertEquals(TraceId.of("0af7651916cd43dd8448eb211c80319c"), e.traceId());
    assertEquals(IdempotencyKey.of("key-1"), e.idempotencyKey());
  }

  @Test
  void publishStoresOptionalFieldsAsNullAndStartsWithNoRetries() {
    queue.publish(
        new DeadLetterPublishRequest(
            PAYLOAD,
            TYPE,
            CommandId.of("c-1"),
            null,
            "java.lang.Error",
            null,
            0,
            T0,
            null,
            null,
            null,
            null));

    DeadLetterEntry e = queue.read(10).getFirst();

    assertNull(e.streamId());
    assertNull(e.aggregateId());
    assertNull(e.errorMessage());
    assertNull(e.correlationId());
    assertNull(e.userId());
    assertNull(e.traceId());
    assertNull(e.idempotencyKey());
    assertEquals(0, e.attempts());
    assertEquals(0, e.dlqAttempts());
    assertNull(e.lastAttemptAt());
    assertTrue(e.publishedAt() != null, "the queue stamps publishedAt");
  }

  @Test
  void readOnAnEmptyQueueIsEmpty() {
    assertTrue(queue.read(10).isEmpty());
  }

  @Test
  void readReturnsNewestFirst() {
    publishAll("c-1", "c-2", "c-3");

    assertEquals(List.of("c-3", "c-2", "c-1"), ids(queue.read(10)));
  }

  @Test
  void readHonoursTheLimitKeepingTheNewest() {
    publishAll("c-1", "c-2", "c-3");

    assertEquals(List.of("c-3", "c-2"), ids(queue.read(2)));
  }

  // ---------------------------------------------------------------- find

  @Test
  void findReturnsTheEntryOfThatCommand() {
    publishAll("c-1", "c-2");

    DeadLetterEntry found = queue.find(CommandId.of("c-1")).orElseThrow();

    assertEquals(CommandId.of("c-1"), found.commandId());
    assertSamePayload(PAYLOAD, found.commandPayload());
  }

  @Test
  void findIsEmptyForAnUnknownCommand() {
    publishAll("c-1");

    assertTrue(queue.find(CommandId.of("nope")).isEmpty());
  }

  // ---------------------------------------------------------------- discard

  @Test
  void discardRemovesOnlyThatEntry() {
    publishAll("c-1", "c-2");

    queue.discard(CommandId.of("c-1"));

    assertEquals(List.of("c-2"), ids(queue.read(10)));
    assertTrue(queue.find(CommandId.of("c-1")).isEmpty());
    assertEquals(1, queue.countPending());
  }

  @Test
  void discardOfAnUnknownCommandIsANoOp() {
    publishAll("c-1");

    queue.discard(CommandId.of("nope"));

    assertEquals(List.of("c-1"), ids(queue.read(10)));
  }

  // ---------------------------------------------------------------- updateAttempts

  @Test
  void updateAttemptsRecordsTheRetryAndLeavesTheRestUntouched() {
    queue.publish(keyed("c-1", "key-1"));
    Instant at = T0.plusSeconds(500);
    DeadLetterEntry before = queue.find(CommandId.of("c-1")).orElseThrow();

    queue.updateAttempts(CommandId.of("c-1"), 2, at);

    DeadLetterEntry after = queue.find(CommandId.of("c-1")).orElseThrow();
    assertEquals(2, after.dlqAttempts());
    assertEquals(at, after.lastAttemptAt());
    assertEquals(before.publishedAt(), after.publishedAt());
    assertEquals(before.attempts(), after.attempts());
    assertEquals(before.commandPayload(), after.commandPayload());
    assertEquals(before.idempotencyKey(), after.idempotencyKey());
    assertEquals(1, queue.countPending());
  }

  @Test
  void updateAttemptsOfAnUnknownCommandIsANoOp() {
    publishAll("c-1");

    queue.updateAttempts(CommandId.of("nope"), 2, T0);

    assertEquals(0, queue.find(CommandId.of("c-1")).orElseThrow().dlqAttempts());
    assertEquals(1, queue.countPending());
  }

  // ---------------------------------------------------------------- readRetryable

  @Test
  void readRetryableExcludesEntriesAtOrAboveMaxRetries() {
    queue.publish(keyed("c-1", "k1"));
    queue.publish(keyed("c-2", "k2"));
    queue.publish(keyed("c-3", "k3"));
    queue.updateAttempts(CommandId.of("c-2"), 3, T0);
    queue.updateAttempts(CommandId.of("c-3"), 4, T0);

    List<String> retryable = ids(queue.readRetryable(3, 10, false, null));

    assertEquals(List.of("c-1"), retryable);
  }

  @Test
  void readRetryableLeavesUnkeyedEntriesOutUnlessAsked() {
    queue.publish(keyed("keyed", "k1"));
    tick();
    queue.publish(unkeyed("unkeyed"));

    assertEquals(List.of("keyed"), ids(queue.readRetryable(3, 10, false, null)));
    assertEquals(List.of("keyed", "unkeyed"), ids(queue.readRetryable(3, 10, true, null)));
  }

  @Test
  void readRetryableCutoffAppliesToKeyedEntriesOnlyAndIsInclusive() {
    Instant cutoff = T0.plusSeconds(100);
    queue.publish(
        request("old-keyed", TYPE, null, IdempotencyKey.of("k1"), cutoff.minusSeconds(1)));
    queue.publish(request("edge-keyed", TYPE, null, IdempotencyKey.of("k2"), cutoff));
    queue.publish(request("new-keyed", TYPE, null, IdempotencyKey.of("k3"), cutoff.plusSeconds(1)));
    queue.publish(request("old-unkeyed", TYPE, null, null, cutoff.minusSeconds(1)));

    List<String> retryable = ids(queue.readRetryable(3, 10, true, cutoff));

    assertEquals(3, retryable.size());
    assertTrue(retryable.containsAll(List.of("edge-keyed", "new-keyed", "old-unkeyed")));
    assertFalse(retryable.contains("old-keyed"));
  }

  @Test
  void readRetryableOrdersByLeastRecentlyActiveFirst() {
    queue.publish(keyed("c-1", "k1"));
    tick();
    queue.publish(keyed("c-2", "k2"));
    tick();
    queue.publish(keyed("c-3", "k3"));
    Instant c3Published = queue.find(CommandId.of("c-3")).orElseThrow().publishedAt();
    // c-1 was retried after c-3 was published, so it is now the most recently active.
    queue.updateAttempts(CommandId.of("c-1"), 1, c3Published.plusSeconds(60));

    assertEquals(List.of("c-2", "c-3", "c-1"), ids(queue.readRetryable(3, 10, false, null)));
  }

  @Test
  void readRetryableHonoursTheLimitAfterOrdering() {
    queue.publish(keyed("c-1", "k1"));
    tick();
    queue.publish(keyed("c-2", "k2"));
    tick();
    queue.publish(keyed("c-3", "k3"));

    assertEquals(List.of("c-1", "c-2"), ids(queue.readRetryable(3, 2, false, null)));
  }

  @Test
  void readRetryableIsNotAClaim() {
    queue.publish(keyed("c-1", "k1"));

    assertEquals(List.of("c-1"), ids(queue.readRetryable(3, 10, false, null)));
    assertEquals(List.of("c-1"), ids(queue.readRetryable(3, 10, false, null)));
  }

  // ---------------------------------------------------------------- counts

  @Test
  void countPendingCountsEveryQueuedEntryRegardlessOfRetryEligibility() {
    assertEquals(0, queue.countPending());
    queue.publish(keyed("c-1", "k1"));
    queue.publish(unkeyed("c-2"));
    queue.updateAttempts(CommandId.of("c-1"), 99, T0);

    assertEquals(2, queue.countPending());
  }

  @Test
  void countAwaitingOperatorCountsUnkeyedAndKeyedEntriesOlderThanTheCutoff() {
    Instant cutoff = T0.plusSeconds(100);
    queue.publish(
        request("old-keyed", TYPE, null, IdempotencyKey.of("k1"), cutoff.minusSeconds(1)));
    queue.publish(request("edge-keyed", TYPE, null, IdempotencyKey.of("k2"), cutoff));
    queue.publish(request("unkeyed", TYPE, null, null, cutoff.plusSeconds(5)));

    // keyed counts only when strictly before the cutoff; unkeyed counts when asked.
    assertEquals(2, queue.countAwaitingOperator(3, true, cutoff));
    assertEquals(1, queue.countAwaitingOperator(3, false, cutoff));
    assertEquals(1, queue.countAwaitingOperator(3, true, null));
    assertEquals(0, queue.countAwaitingOperator(3, false, null));
  }

  @Test
  void countAwaitingOperatorLeavesOutExhaustedEntries() {
    queue.publish(unkeyed("c-1"));
    queue.publish(unkeyed("c-2"));
    queue.updateAttempts(CommandId.of("c-2"), 3, T0);

    assertEquals(1, queue.countAwaitingOperator(3, true, null));
  }

  // ---------------------------------------------------------------- findByIdempotencyKey

  @Test
  void findByIdempotencyKeyReturnsTheKeysEntriesOldestFirst() {
    queue.publish(request("c-1", "com.example.A", null, IdempotencyKey.of("k1"), T0));
    tick();
    queue.publish(request("c-2", "com.example.B", null, IdempotencyKey.of("other"), T0));
    tick();
    queue.publish(request("c-3", "com.example.B", null, IdempotencyKey.of("k1"), T0));

    List<DeadLetterEntry> found = queue.findByIdempotencyKey(IdempotencyKey.of("k1"));

    assertEquals(List.of("c-1", "c-3"), ids(found));
  }

  @Test
  void findByIdempotencyKeyIsEmptyWhenNoEntryHoldsTheKey() {
    queue.publish(unkeyed("c-1"));

    assertTrue(queue.findByIdempotencyKey(IdempotencyKey.of("k1")).isEmpty());
  }

  @Test
  void aKeyedRequestForACommandAlreadyHeldUnderTheKeyStoresNothing() {
    queue.publish(request("c-1", TYPE, stream("order-1"), IdempotencyKey.of("k1"), T0));

    queue.publish(request("c-2", TYPE, stream("order-1"), IdempotencyKey.of("k1"), T0));

    assertEquals(List.of("c-1"), ids(queue.read(10)));
    assertEquals(List.of("c-1"), ids(queue.findByIdempotencyKey(IdempotencyKey.of("k1"))));
    assertEquals(1, queue.countPending());
  }

  @Test
  void theOneEntryRuleAlsoHoldsForCommandsWithoutAStream() {
    queue.publish(request("c-1", TYPE, null, IdempotencyKey.of("k1"), T0));

    queue.publish(request("c-2", TYPE, null, IdempotencyKey.of("k1"), T0));

    assertEquals(List.of("c-1"), ids(queue.read(10)));
  }

  @Test
  void aDifferentCommandTypeStreamOrKeyStoresAnotherEntry() {
    queue.publish(request("c-1", TYPE, stream("order-1"), IdempotencyKey.of("k1"), T0));

    queue.publish(
        request("c-2", "com.example.Other", stream("order-1"), IdempotencyKey.of("k1"), T0));
    queue.publish(request("c-3", TYPE, stream("order-2"), IdempotencyKey.of("k1"), T0));
    queue.publish(request("c-4", TYPE, stream("order-1"), IdempotencyKey.of("k2"), T0));

    assertEquals(4, queue.countPending());
  }

  @Test
  void unkeyedRequestsAreNeverDeduplicated() {
    queue.publish(request("c-1", TYPE, stream("order-1"), null, T0));
    queue.publish(request("c-2", TYPE, stream("order-1"), null, T0));

    assertEquals(2, queue.countPending());
  }

  // ---------------------------------------------------------------- deleteOlderThan

  @Test
  void deleteOlderThanRemovesEntriesPublishedStrictlyBeforeTheCutoff() {
    publishAll("c-1", "c-2", "c-3");
    Instant c2Published = queue.find(CommandId.of("c-2")).orElseThrow().publishedAt();

    int deleted = queue.deleteOlderThan(c2Published);

    assertEquals(1, deleted);
    assertEquals(List.of("c-3", "c-2"), ids(queue.read(10)));
  }

  @Test
  void deleteOlderThanReturnsZeroWhenNothingIsOldEnough() {
    publishAll("c-1");
    Instant published = queue.find(CommandId.of("c-1")).orElseThrow().publishedAt();

    assertEquals(0, queue.deleteOlderThan(published.minusSeconds(1)));
    assertEquals(1, queue.countPending());
  }

  @Test
  void deleteOlderThanRemovesEverythingBeforeALaterCutoff() {
    publishAll("c-1", "c-2");
    Instant later = queue.read(1).getFirst().publishedAt().plusSeconds(1);

    assertEquals(2, queue.deleteOlderThan(later));
    assertEquals(0, queue.countPending());
  }
}
