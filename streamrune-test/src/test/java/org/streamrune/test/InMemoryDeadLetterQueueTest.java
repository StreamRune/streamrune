package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

class InMemoryDeadLetterQueueTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  InMemoryDeadLetterQueue dlq;

  @BeforeEach
  void setUp() {
    dlq = new InMemoryDeadLetterQueue();
  }

  private DeadLetterQueue.DeadLetterPublishRequest request(String id, String type) {
    return new DeadLetterQueue.DeadLetterPublishRequest(
        "{}",
        type,
        CommandId.of(id),
        StreamId.of(TYPE, AggregateId.of("s1")),
        "Err",
        "msg",
        1,
        Instant.now(),
        null,
        null,
        null,
        null);
  }

  @Test
  void publishAndReadRoundTrip() {
    dlq.publish(request("cmd-1", "CreateOrder"));
    var entries = dlq.read(10);
    assertEquals(1, entries.size());
    assertEquals("CreateOrder", entries.getFirst().commandType());
  }

  @Test
  void readReturnsNewestFirst() {
    dlq.publish(request("cmd-1", "First"));
    dlq.publish(request("cmd-2", "Second"));
    var entries = dlq.read(10);
    assertEquals("Second", entries.get(0).commandType());
    assertEquals("First", entries.get(1).commandType());
  }

  @Test
  void readRespectsLimit() {
    for (int i = 0; i < 5; i++) {
      dlq.publish(request("cmd-" + i, "Cmd"));
    }
    assertEquals(3, dlq.read(3).size());
  }

  @Test
  void discardRemovesEntry() {
    var cmdId = CommandId.of("cmd-1");
    dlq.publish(request("cmd-1", "Cmd"));
    dlq.discard(cmdId);
    assertTrue(dlq.read(10).isEmpty());
  }

  @Test
  void updateAttemptsPersists() {
    dlq.publish(request("cmd-1", "Cmd"));
    Instant retryTime = Instant.parse("2026-03-01T12:00:00Z");
    dlq.updateAttempts(CommandId.of("cmd-1"), 2, retryTime);

    var entry = dlq.read(10).getFirst();
    assertEquals(2, entry.dlqAttempts());
    assertEquals(retryTime, entry.lastAttemptAt());
  }

  @Test
  void publishCarriesRequestContextAndUpdateAttemptsPreservesIt() {
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            "{}",
            "Cmd",
            CommandId.of("cmd-ctx"),
            StreamId.of(TYPE, AggregateId.of("s1")),
            "Err",
            "msg",
            1,
            Instant.now(),
            CorrelationId.of("corr-1"),
            UserId.of("user-1"),
            TraceId.of("trace-1"),
            null));

    var published = dlq.read(10).getFirst();
    assertEquals("corr-1", published.correlationId().value());
    assertEquals("user-1", published.userId().value());
    assertEquals("trace-1", published.traceId().value());

    // updateAttempts must not drop the persisted context when rewriting the entry.
    dlq.updateAttempts(CommandId.of("cmd-ctx"), 2, Instant.now());
    var afterUpdate = dlq.read(10).getFirst();
    assertEquals(2, afterUpdate.dlqAttempts());
    assertEquals("corr-1", afterUpdate.correlationId().value());
    assertEquals("user-1", afterUpdate.userId().value());
    assertEquals("trace-1", afterUpdate.traceId().value());
  }

  @Test
  void readRetryableExcludesExhaustedEntries() {
    dlq.publish(request("cmd-1", "Eligible"));
    dlq.publish(request("cmd-2", "Exhausted"));
    dlq.updateAttempts(CommandId.of("cmd-2"), 3, Instant.now());

    var retryable = dlq.readRetryable(3, 10);
    assertEquals(1, retryable.size());
    assertEquals("Eligible", retryable.getFirst().commandType());
  }

  @Test
  void readRetryableOrdersOldestFirst() {
    dlq.publish(request("cmd-1", "First"));
    dlq.publish(request("cmd-2", "Second"));

    var retryable = dlq.readRetryable(5, 10);
    assertEquals("First", retryable.get(0).commandType());
    assertEquals("Second", retryable.get(1).commandType());
  }

  @Test
  void readRetryableRespectsLimit() {
    for (int i = 0; i < 5; i++) {
      dlq.publish(request("cmd-" + i, "Cmd"));
    }
    assertEquals(3, dlq.readRetryable(5, 3).size());
  }

  @Test
  void readRetryableDoesNotClaimSoASecondSequentialPollReturnsTheSameEntry() {
    // Documented divergence: PostgresDeadLetterQueue uses FOR UPDATE SKIP LOCKED, a
    // transaction-scoped lock released on commit — NOT a persistent claim. So in both production
    // and this double a sequential second readRetryable returns the same entry. Deduplication
    // across retry cycles is driven by dlqAttempts gating (and discard), not by claiming.
    dlq.publish(request("cmd-1", "Eligible"));

    assertEquals(1, dlq.readRetryable(3, 10).size(), "first poll returns the eligible entry");
    assertEquals(
        1,
        dlq.readRetryable(3, 10).size(),
        "a second sequential poll returns it again — readRetryable does not claim");

    // Bumping dlqAttempts to the limit is what removes it from the retryable set.
    dlq.updateAttempts(CommandId.of("cmd-1"), 3, Instant.now());
    assertTrue(
        dlq.readRetryable(3, 10).isEmpty(),
        "exhausted entry drops out of the retryable set via dlqAttempts gating");
  }

  @Test
  void allReturnsAllEntries() {
    dlq.publish(request("cmd-1", "A"));
    dlq.publish(request("cmd-2", "B"));
    assertEquals(2, dlq.all().size());
  }

  @Test
  void constructorRejectsNullClock() {
    assertThrows(NullPointerException.class, () -> new InMemoryDeadLetterQueue(null));
  }

  @Test
  void deleteOlderThanRemovesStrictlyOlderEntriesAndReturnsCount() {
    var clock = MutableClock.startingAt(Instant.parse("2026-06-21T00:00:00Z"));
    var q = new InMemoryDeadLetterQueue(clock);

    q.publish(request("cmd-old", "Old")); // published_at = 2026-06-21T00:00:00Z
    clock.advance(Duration.ofDays(2));
    q.publish(request("cmd-fresh", "Fresh")); // published_at = 2026-06-23T00:00:00Z

    Instant cutoff = Instant.parse("2026-06-22T00:00:00Z");
    int deleted = q.deleteOlderThan(cutoff);

    assertEquals(1, deleted);
    var remaining = q.all();
    assertEquals(1, remaining.size());
    assertEquals("Fresh", remaining.getFirst().commandType());
  }

  @Test
  void deleteOlderThanKeepsEntriesExactlyAtCutoff() {
    var clock = MutableClock.startingAt(Instant.parse("2026-06-21T00:00:00Z"));
    var q = new InMemoryDeadLetterQueue(clock);
    q.publish(request("cmd-at-cutoff", "AtCutoff"));

    int deleted = q.deleteOlderThan(Instant.parse("2026-06-21T00:00:00Z"));

    assertEquals(0, deleted, "strictly-before semantics: entry at cutoff must be retained");
    assertEquals(1, q.all().size());
  }

  @Test
  void deleteOlderThanWithNoEligibleEntriesReturnsZero() {
    dlq.publish(request("cmd-1", "Cmd"));
    int deleted = dlq.deleteOlderThan(Instant.EPOCH);
    assertEquals(0, deleted);
    assertEquals(1, dlq.all().size());
  }

  @Test
  void publishedAtIsStampedFromTheInjectedClock() {
    var clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
    var q = new InMemoryDeadLetterQueue(clock);
    q.publish(request("cmd-1", "First"));
    clock.advance(Duration.ofSeconds(1));
    q.publish(request("cmd-2", "Second"));

    var entries = q.read(10); // newest first
    assertEquals("Second", entries.get(0).commandType());
    assertEquals(Instant.parse("2026-01-01T00:00:01Z"), entries.get(0).publishedAt());
    assertEquals("First", entries.get(1).commandType());
    assertEquals(Instant.parse("2026-01-01T00:00:00Z"), entries.get(1).publishedAt());
  }
}
