package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.DeadLetterRetryPolicy.UnkeyedReplay;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.DeadLetterRetryRunner.RetryOutcome;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.MutableClock;

/**
 * When a dead-letter replay is a second execution of a command its caller already completed.
 *
 * <p>Two shapes, both driven through the real command bus, command inbox and retry runner:
 *
 * <ul>
 *   <li>an <b>unkeyed</b> relative command ("adjust stock by +50") fails on infrastructure, its
 *       caller retries it successfully, and the scheduled retry then replays the dead-letter entry
 *       — nothing pairs the replay with the caller's retry, so the delta would be applied twice;
 *   <li>a <b>keyed</b> command's caller retries successfully under the same key, the inbox
 *       retention sweeper later prunes that key's row, and an operator replays the retained entry —
 *       the inbox no longer recognises the key, so the command would execute again.
 * </ul>
 */
class DeadLetterReplaySafetyTest {

  private static final AggregateType PRODUCT = AggregateType.of("product");
  private static final Duration INBOX_WINDOW = Duration.ofDays(7);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  record AdjustStock(String productId, int delta) implements Command {}

  sealed interface StockEvent extends DomainEvent {
    record StockAdjusted(int delta) implements StockEvent {}
  }

  record Stock(int units) implements AggregateState {}

  /** Relative: every execution adds its delta to whatever the stock is now. */
  static final class StockDecider implements Decider<AdjustStock, Stock, StockEvent> {
    @Override
    public Stock initialState() {
      return new Stock(100);
    }

    @Override
    public List<StockEvent> decide(AdjustStock command, Stock state) {
      return List.of(new StockEvent.StockAdjusted(command.delta()));
    }

    @Override
    public Stock evolve(Stock state, StockEvent event) {
      return new Stock(state.units() + ((StockEvent.StockAdjusted) event).delta());
    }
  }

  /** Fails the next {@code failures} appends, keyed or not, with a transient store fault. */
  static final class FailingAppends implements EventStore {
    private final EventStore delegate;
    private final AtomicInteger failures;

    FailingAppends(EventStore delegate, int failures) {
      this.delegate = delegate;
      this.failures = new AtomicInteger(failures);
    }

    private void maybeFail(StreamId streamId) {
      if (failures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        throw new EventStoreException("seeded transient append failure for stream: " + streamId);
      }
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      maybeFail(streamId);
      return delegate.append(streamId, events, expectedVersion);
    }

    @Override
    public IdempotentAppendResult appendWithKey(
        StreamId streamId,
        List<EventEnvelope> events,
        Version expectedVersion,
        IdempotencyKey idempotencyKey,
        String commandType) {
      maybeFail(streamId);
      return delegate.appendWithKey(streamId, events, expectedVersion, idempotencyKey, commandType);
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      return delegate.load(streamId);
    }

    @Override
    public AggregateHistory load(StreamId streamId, int expectedSnapshotVersion) {
      return delegate.load(streamId, expectedSnapshotVersion);
    }

    @Override
    public boolean saveSnapshot(
        StreamId streamId,
        Version version,
        AggregateState state,
        int snapshotVersion,
        long snapshotPurgeGeneration) {
      return delegate.saveSnapshot(
          streamId, version, state, snapshotVersion, snapshotPurgeGeneration);
    }

    @Override
    public GlobalOffset lastGlobalOffset() {
      return delegate.lastGlobalOffset();
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return delegate.readGlobalStream(afterOffset, maxCount);
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return delegate.readStream(streamId, afterVersion, maxCount);
    }
  }

  /** Records the stale-replay refusals the runner reports. */
  static final class RecordingMetrics implements StreamRuneMetrics {
    final List<String> staleBlocked = new ArrayList<>();

    @Override
    public void recordDeadLetterStaleReplayBlocked(String commandType) {
      staleBlocked.add(commandType);
    }
  }

  /** One bus + inbox + DLQ + runner, wired as the integrations wire them. */
  static final class Harness {
    final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
    final InMemoryEventStore store;
    final MutableClock runnerClock = MutableClock.startingAt(Instant.now());
    // The queue stamps publishedAt from the runner's clock, so "due" is decided on one timeline.
    final InMemoryDeadLetterQueue dlq = new InMemoryDeadLetterQueue(runnerClock);
    final RecordingMetrics metrics = new RecordingMetrics();
    final VirtualThreadCommandBus bus;
    final DeadLetterRetryRunner runner;

    Harness(boolean withInbox, int appendFailures, DeadLetterRetryPolicy policy) {
      store =
          withInbox ? new InMemoryEventStore().withCommandInbox(inbox) : new InMemoryEventStore();
      var busBuilder =
          VirtualThreadCommandBus.builder()
              .eventStore(new FailingAppends(store, appendFailures))
              .deadLetterQueue(dlq)
              .objectMapper(MAPPER)
              .register(
                  PRODUCT,
                  AdjustStock.class,
                  c -> AggregateId.of(c.productId()),
                  new StockDecider());
      if (withInbox) {
        busBuilder.commandInbox(inbox);
      }
      bus = busBuilder.build();
      var runnerBuilder =
          DeadLetterRetryRunner.builder()
              .deadLetterQueue(dlq)
              .commandBus(bus)
              .objectMapper(MAPPER)
              .pollInterval(Duration.ofMinutes(1))
              .clock(runnerClock)
              .metrics(metrics)
              .inboxRetentionMaxAge(INBOX_WINDOW)
              .registerCommand(AdjustStock.class);
      if (policy != null) {
        runnerBuilder.policy(policy);
      }
      runner = runnerBuilder.build();
    }

    int units(String productId) {
      int units = 100;
      for (var envelope :
          store.readStream(
              StreamId.of(PRODUCT, AggregateId.of(productId)), Version.initial(), 1000)) {
        units += ((StockEvent.StockAdjusted) envelope.event()).delta();
      }
      return units;
    }

    DeadLetterQueue.DeadLetterEntry onlyEntry() {
      assertEquals(1, dlq.all().size(), "exactly one dead-letter entry expected");
      return dlq.all().getFirst();
    }

    void publish(
        String commandId, AdjustStock command, Instant firstAttemptAt, IdempotencyKey key) {
      try {
        dlq.publish(
            new DeadLetterQueue.DeadLetterPublishRequest(
                MAPPER.writeValueAsString(command),
                AdjustStock.class.getName(),
                CommandId.of(commandId),
                StreamId.of(PRODUCT, AggregateId.of(command.productId())),
                EventStoreException.class.getName(),
                "seeded",
                1,
                firstAttemptAt,
                null,
                null,
                null,
                key));
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }
  }

  private static DeadLetterRetryPolicy immediate(UnkeyedReplay unkeyedReplay) {
    return new DeadLetterRetryPolicy(5, Duration.ZERO, 2.0, false, unkeyedReplay);
  }

  private static IdempotencyKey key(String value) {
    return IdempotencyKey.of(value);
  }

  // ── Unkeyed: the client's own retry, then the scheduled replay ────────────────────────────────

  @Test
  void
      unkeyedRelativeCommand_clientRetrySucceeds_thenTheDefaultScheduledRetry_doesNotApplyItAgain() {
    // No policy: the runner runs the production default.
    var h = new Harness(true, 1, null);
    var restock = new AdjustStock("p-1", 50);

    // The first attempt hits a transient store fault: dead-lettered, and the caller sees it fail.
    assertThrows(EventStoreException.class, () -> h.bus.execute(restock));
    assertEquals(100, h.units("p-1"));
    assertNull(h.onlyEntry().idempotencyKey(), "the command ran unkeyed");

    // The caller (an admin re-sending the request, a retrying client) tries again and succeeds.
    h.bus.execute(restock);
    assertEquals(150, h.units("p-1"));

    // Long after every backoff of the default ladder has elapsed, the scheduled retry runs.
    h.runnerClock.advance(Duration.ofHours(2));
    h.runner.processBatch();
    h.runnerClock.advance(Duration.ofHours(2));
    h.runner.processBatch();

    assertEquals(150, h.units("p-1"), "the restock is applied once, not twice");
    assertEquals(0, h.onlyEntry().dlqAttempts(), "the entry waits for an operator, untouched");
  }

  @Test
  void unkeyedRelativeCommand_underAutomaticUnkeyedReplay_isAppliedTwice_theDocumentedHazard() {
    var h = new Harness(true, 1, immediate(UnkeyedReplay.AUTOMATIC));
    var restock = new AdjustStock("p-1", 50);
    assertThrows(EventStoreException.class, () -> h.bus.execute(restock));
    h.bus.execute(restock);

    h.runner.processBatch();

    assertEquals(
        200,
        h.units("p-1"),
        "AUTOMATIC is the explicit opt-in: a relative command's replay after the caller's own"
            + " retry is a second execution");
    assertTrue(h.dlq.all().isEmpty());
  }

  @Test
  void unkeyedEntry_operatorRetry_replaysIt_thePolicyGovernsOnlyTheSchedule() {
    var h = new Harness(true, 1, immediate(UnkeyedReplay.OPERATOR_ONLY));
    var restock = new AdjustStock("p-1", 50);
    assertThrows(EventStoreException.class, () -> h.bus.execute(restock));
    var commandId = h.onlyEntry().commandId();

    // The operator checked: the caller never retried, so the restock is missing.
    assertEquals(RetryOutcome.REPLAYED, h.runner.retry(commandId));

    assertEquals(150, h.units("p-1"));
    assertTrue(h.dlq.all().isEmpty());
  }

  @Test
  void keyedEntry_isStillReplayedAutomatically_underTheDefaultPolicy() {
    var h = new Harness(true, 1, null);
    var restock = new AdjustStock("p-1", 50);
    var key = key("restock-p-1-1");
    assertThrows(EventStoreException.class, () -> h.bus.execute(restock, key));

    h.runnerClock.advance(Duration.ofHours(2));
    h.runner.processBatch();

    assertEquals(150, h.units("p-1"));
    assertTrue(h.dlq.all().isEmpty(), "the keyed entry was replayed on the schedule");
    // The caller's own retry under the same key meets the inbox row the replay wrote.
    assertTrue(h.bus.execute(restock, key).idempotentReplay());
    assertEquals(150, h.units("p-1"));
  }

  @Test
  void aBacklogOfUnkeyedEntries_neverStarvesTheKeyedEntryBehindIt() {
    var h = new Harness(true, 0, immediate(UnkeyedReplay.OPERATOR_ONLY));
    // More unkeyed entries than the runner's over-fetched page (batch 10 x 10), all published
    // before the keyed one — so all ahead of it in least-recently-active order.
    for (int i = 0; i < 150; i++) {
      h.publish("unkeyed-" + i, new AdjustStock("u-" + i, 1), Instant.now(), null);
    }
    h.publish("keyed-1", new AdjustStock("k-1", 7), Instant.now(), key("k-1"));

    h.runner.processBatch();

    assertEquals(107, h.units("k-1"), "the keyed entry is replayed despite the backlog ahead");
    assertEquals(150, h.dlq.all().size(), "every unkeyed entry still waits for an operator");
    assertEquals(100, h.units("u-0"));
  }

  @Test
  void aStoreThatIgnoresTheKeyFilter_stillNeverHasItsUnkeyedEntriesReplayedOnTheSchedule() {
    var h = new Harness(true, 0, immediate(UnkeyedReplay.OPERATOR_ONLY));
    // A third-party store that returns every retryable entry whatever includeUnkeyed says.
    DeadLetterQueue lax =
        new DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {
            h.dlq.publish(request);
          }

          @Override
          public List<DeadLetterEntry> read(int limit) {
            return h.dlq.read(limit);
          }

          @Override
          public void discard(CommandId commandId) {
            h.dlq.discard(commandId);
          }

          @Override
          public void updateAttempts(CommandId commandId, int dlqAttempts, Instant lastAttemptAt) {
            h.dlq.updateAttempts(commandId, dlqAttempts, lastAttemptAt);
          }

          @Override
          public List<DeadLetterEntry> readRetryable(
              int maxRetries, int limit, boolean includeUnkeyed) {
            return h.dlq.readRetryable(maxRetries, limit, true);
          }
        };
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(lax)
            .commandBus(h.bus)
            .objectMapper(MAPPER)
            .policy(immediate(UnkeyedReplay.OPERATOR_ONLY))
            .registerCommand(AdjustStock.class)
            .build();
    h.publish("unkeyed-1", new AdjustStock("p-1", 50), Instant.now(), null);

    runner.processBatch();

    assertEquals(100, h.units("p-1"));
    assertEquals(0, h.onlyEntry().dlqAttempts());
  }

  @Test
  void aBusWithoutAnInbox_replaysNothingOnTheSchedule_underOperatorOnly() {
    var h = new Harness(false, 0, immediate(UnkeyedReplay.OPERATOR_ONLY));
    // Even a keyed entry: without an inbox its replay would run unkeyed, deduplicated by nothing.
    h.publish("keyed-1", new AdjustStock("p-1", 50), Instant.now(), key("k-1"));

    h.runner.processBatch();
    h.runner.processBatch();

    assertEquals(100, h.units("p-1"));
    assertEquals(0, h.onlyEntry().dlqAttempts());
  }

  @Test
  void aBusWithoutAnInbox_underOperatorOnly_doesNotEvenReadTheQueue() {
    var h = new Harness(false, 0, immediate(UnkeyedReplay.OPERATOR_ONLY));
    DeadLetterQueue unreadable =
        new DeadLetterQueue() {
          @Override
          public void publish(DeadLetterPublishRequest request) {}

          @Override
          public List<DeadLetterEntry> read(int limit) {
            return List.of();
          }

          @Override
          public void discard(CommandId commandId) {}

          @Override
          public List<DeadLetterEntry> readRetryable(
              int maxRetries, int limit, boolean includeUnkeyed) {
            throw new AssertionError("nothing can be replayed automatically; no read is due");
          }
        };
    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(unreadable)
            .commandBus(h.bus)
            .objectMapper(MAPPER)
            .policy(immediate(UnkeyedReplay.OPERATOR_ONLY))
            .registerCommand(AdjustStock.class)
            .build();

    assertDoesNotThrow(runner::processBatch);
  }

  // ── Keyed: the inbox window ───────────────────────────────────────────────────────────────────

  @Test
  void keyedCommand_clientRetrySucceeds_inboxRowPruned_operatorRetryTenDaysLater_isRefused() {
    var h = new Harness(true, 1, null);
    var reserve = new AdjustStock("p-1", -3);
    var key = key("reserve-o-1");

    // Day 0: the keyed command is dead-lettered, then the caller's retry under the same key
    // succeeds and writes the inbox row.
    assertThrows(EventStoreException.class, () -> h.bus.execute(reserve, key));
    assertFalse(h.bus.execute(reserve, key).idempotentReplay());
    assertEquals(97, h.units("p-1"));
    var commandId = h.onlyEntry().commandId();

    // Day 10: the inbox sweeper (window 7 days) has pruned the row.
    h.runnerClock.advance(Duration.ofDays(10));
    assertEquals(1, h.inbox.deleteProcessedBefore(h.runnerClock.instant().minus(INBOX_WINDOW)));

    // The operator clicks Retry on the retained entry.
    assertEquals(RetryOutcome.STALE_REPLAY_BLOCKED, h.runner.retry(commandId));

    assertEquals(97, h.units("p-1"), "the reservation is not applied a second time");
    var entry = h.onlyEntry();
    assertEquals(0, entry.dlqAttempts(), "a refusal leaves the entry unchanged");
    assertNull(entry.lastAttemptAt());
    assertEquals(List.of("AdjustStock"), h.metrics.staleBlocked);
  }

  @Test
  void staleKeyedEntry_forcedOperatorRetry_runsTheCommandAgain_atLeastOnceAcknowledged() {
    var h = new Harness(true, 1, null);
    var restock = new AdjustStock("p-1", 50);
    assertThrows(EventStoreException.class, () -> h.bus.execute(restock, key("restock-p-1-1")));
    var commandId = h.onlyEntry().commandId();
    h.runnerClock.advance(Duration.ofDays(8));

    // The operator checked: the caller never retried, the restock is missing.
    assertEquals(RetryOutcome.REPLAYED, h.runner.retry(commandId, true));

    assertEquals(150, h.units("p-1"));
    assertTrue(h.dlq.all().isEmpty());
    assertTrue(h.metrics.staleBlocked.isEmpty(), "a forced replay is not a refusal");
  }

  @Test
  void staleKeyedEntry_isNotReplayedOnTheSchedule_andStaysForAnOperator() {
    var h = new Harness(true, 0, immediate(UnkeyedReplay.OPERATOR_ONLY));
    Instant now = h.runnerClock.instant();
    h.publish("stale", new AdjustStock("p-1", 50), now.minus(Duration.ofDays(8)), key("k-stale"));
    h.publish("fresh", new AdjustStock("p-2", 50), now.minus(Duration.ofDays(6)), key("k-fresh"));

    h.runner.processBatch();

    assertEquals(100, h.units("p-1"), "the stale entry is refused on the schedule too");
    assertEquals(150, h.units("p-2"), "the fresh keyed entry is replayed");
    var stale = h.onlyEntry();
    assertEquals(CommandId.of("stale"), stale.commandId());
    assertEquals(0, stale.dlqAttempts(), "the refusal is not a failed attempt");
    assertEquals(List.of("AdjustStock"), h.metrics.staleBlocked);
  }

  @Test
  void theWindowIsMeasuredFromFirstAttemptAt_inclusiveAtTheBoundary() {
    var h = new Harness(true, 0, immediate(UnkeyedReplay.OPERATOR_ONLY));
    Instant now = h.runnerClock.instant();
    h.publish("edge", new AdjustStock("p-1", 1), now.minus(INBOX_WINDOW), key("k-edge"));
    h.publish(
        "past", new AdjustStock("p-2", 1), now.minus(INBOX_WINDOW).minusMillis(1), key("k-past"));

    assertEquals(RetryOutcome.REPLAYED, h.runner.retry(CommandId.of("edge")));
    assertEquals(RetryOutcome.STALE_REPLAY_BLOCKED, h.runner.retry(CommandId.of("past")));
  }

  @Test
  void anUnprunedInbox_zeroOrNegativeOrUnsetWindow_refusesNothing() {
    for (Duration window : new Duration[] {null, Duration.ZERO, Duration.ofDays(-1)}) {
      var h = new Harness(true, 0, immediate(UnkeyedReplay.OPERATOR_ONLY));
      var runner =
          DeadLetterRetryRunner.builder()
              .deadLetterQueue(h.dlq)
              .commandBus(h.bus)
              .objectMapper(MAPPER)
              .clock(h.runnerClock)
              .inboxRetentionMaxAge(window)
              .registerCommand(AdjustStock.class)
              .build();
      h.publish(
          "old",
          new AdjustStock("p-1", 1),
          h.runnerClock.instant().minus(Duration.ofDays(400)),
          key("k-old"));

      assertEquals(RetryOutcome.REPLAYED, runner.retry(CommandId.of("old")), "window " + window);
    }
  }

  @Test
  void theStaleGuardDoesNotApplyToUnkeyedEntries_nothingDeduplicatesThemAnyway() {
    var h = new Harness(true, 0, immediate(UnkeyedReplay.AUTOMATIC));
    h.publish(
        "old-unkeyed",
        new AdjustStock("p-1", 1),
        h.runnerClock.instant().minus(Duration.ofDays(30)),
        null);

    assertEquals(RetryOutcome.REPLAYED, h.runner.retry(CommandId.of("old-unkeyed")));
  }

  @Test
  void retryOutcomes_notFoundAndFailedAndAdmissionRefused() {
    var h = new Harness(true, 2, immediate(UnkeyedReplay.OPERATOR_ONLY));
    assertEquals(RetryOutcome.NOT_FOUND, h.runner.retry(CommandId.of("absent")));

    var restock = new AdjustStock("p-1", 50);
    assertThrows(EventStoreException.class, () -> h.bus.execute(restock, key("k-1")));
    var commandId = h.onlyEntry().commandId();
    // The second seeded failure hits the replay.
    assertEquals(RetryOutcome.FAILED, h.runner.retry(commandId));
    assertEquals(1, h.onlyEntry().dlqAttempts());

    h.bus.close();
    assertEquals(RetryOutcome.ADMISSION_REFUSED, h.runner.retry(commandId));
    assertEquals(1, h.onlyEntry().dlqAttempts(), "a refused admission is not an attempt");
  }

  @Test
  void builderRejectsANullPolicy() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                DeadLetterRetryRunner.builder()
                    .deadLetterQueue(new InMemoryDeadLetterQueue())
                    .commandBus(org.mockito.Mockito.mock(CommandBus.class))
                    .objectMapper(MAPPER)
                    .policy(null)
                    .build());
    assertTrue(ex.getMessage().contains("policy"), ex.getMessage());
  }
}
