package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;

/**
 * Regression: the effectively-once guarantee of {@code execute(command, IdempotencyKey)} must
 * survive a full dead-letter-queue round trip.
 *
 * <p>Scenario: a KEYED command fails on a seeded transient store fault (uncommitted) and is
 * dead-lettered → the {@link DeadLetterRetryRunner} replays it → the client, which retries
 * precisely because it is keyed, then redelivers {@code execute(command, sameKey)}. The domain
 * events must be produced EXACTLY ONCE: the redelivery short-circuits at the {@link
 * org.streamrune.core.CommandInbox} because the DLQ replay ran under the ORIGINAL key, not a
 * synthetic {@code dlq-replay:<commandId>} key.
 *
 * <p>Against the pre-fix code (replay under the synthetic key) the redelivery misses the inbox and
 * re-appends the events, so the stream would contain two copies — this test fails. With the
 * original key persisted on the DLQ entry and used on replay, it passes.
 */
class DlqReplayKeyedIdempotencyTest {

  private static final AggregateType TYPE = AggregateType.of("widget");

  // === Minimal test domain ===

  record WidgetCommand(String widgetId) implements Command {}

  sealed interface WidgetEvent extends DomainEvent {
    record WidgetCreated(String widgetId) implements WidgetEvent {}
  }

  record WidgetState(boolean created) implements AggregateState {}

  static final class OneEventDecider implements Decider<WidgetCommand, WidgetState, WidgetEvent> {
    @Override
    public WidgetState initialState() {
      return new WidgetState(false);
    }

    @Override
    public List<WidgetEvent> decide(WidgetCommand command, WidgetState state) {
      return List.of(new WidgetEvent.WidgetCreated(command.widgetId()));
    }

    @Override
    public WidgetState evolve(WidgetState state, WidgetEvent event) {
      return new WidgetState(true);
    }
  }

  /**
   * Event-store decorator that fails the next {@code appendWithKey} calls with a transient,
   * DLQ-eligible {@link EventStoreException} (like an infra blip during insert), then delegates
   * normally. Everything else passes straight through to the delegate.
   */
  static final class FailFirstAppendEventStore implements EventStore {
    private final EventStore delegate;
    private final AtomicInteger appendFailuresRemaining;

    FailFirstAppendEventStore(EventStore delegate, int appendFailures) {
      this.delegate = delegate;
      this.appendFailuresRemaining = new AtomicInteger(appendFailures);
    }

    @Override
    public IdempotentAppendResult appendWithKey(
        StreamId streamId,
        List<EventEnvelope> events,
        Version expectedVersion,
        IdempotencyKey idempotencyKey,
        String commandType) {
      if (appendFailuresRemaining.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        throw new EventStoreException("seeded transient append failure for stream: " + streamId);
      }
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
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      return delegate.append(streamId, events, expectedVersion);
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      delegate.saveSnapshot(streamId, version, state);
    }

    @Override
    public void saveSnapshot(
        StreamId streamId, Version version, AggregateState state, int snapshotVersion) {
      delegate.saveSnapshot(streamId, version, state, snapshotVersion);
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

  @Test
  void keyedCommand_failsThenDlqReplaysThenClientRedelivers_eventsProducedExactlyOnce() {
    var inbox = new InMemoryCommandInbox();
    var backing = new InMemoryEventStore().withCommandInbox(inbox);
    // Fail the FIRST appendWithKey (the initial keyed execute), succeed on the DLQ replay.
    var faultStore = new FailFirstAppendEventStore(backing, 1);
    var dlq = new InMemoryDeadLetterQueue();
    var mapper = new ObjectMapper();

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(faultStore)
            .commandInbox(inbox)
            .deadLetterQueue(dlq)
            .objectMapper(mapper)
            .register(
                TYPE,
                WidgetCommand.class,
                cmd -> AggregateId.of(cmd.widgetId()),
                new OneEventDecider())
            .build();

    var runner =
        DeadLetterRetryRunner.builder()
            .deadLetterQueue(dlq)
            .commandBus(bus)
            .objectMapper(mapper)
            .policy(new DeadLetterRetryPolicy(3, Duration.ZERO, 2.0, false))
            .pollInterval(Duration.ofMinutes(1))
            // registerCommand keys by the fully-qualified class name: this test drives the REAL
            // bus, which persists command.getClass().getName() on the DLQ entry, so
            // the retry runner's registry resolves that FQN exactly.
            .registerCommand(WidgetCommand.class)
            .build();

    var cmd = new WidgetCommand("w-dlq-1");
    var key = IdempotencyKey.of("client-idem-key-1");
    var streamId = StreamId.of(TYPE, AggregateId.of("w-dlq-1"));

    // (1) The first keyed execute hits the seeded transient fault, rolls back uncommitted, and is
    // dead-lettered. The failure propagates to the caller.
    assertThrows(EventStoreException.class, () -> bus.execute(cmd, key));

    assertEquals(
        0, streamLength(backing, streamId), "the failed command must have committed nothing");
    var entries = dlq.all();
    assertEquals(1, entries.size(), "the failed keyed command must be dead-lettered");
    assertEquals(
        key,
        entries.get(0).idempotencyKey(),
        "the DLQ entry must carry the ORIGINAL client idempotency key");

    // (2) The retry runner replays the entry. The fault is gone, so it commits exactly one event —
    // under the ORIGINAL key — and discards the entry.
    runner.processBatch();

    assertEquals(1, streamLength(backing, streamId), "the DLQ replay must commit the event once");
    assertTrue(dlq.all().isEmpty(), "a successful replay must discard the DLQ entry");

    // (3) The client redelivers the SAME command under the SAME key (at-least-once). It must
    // short-circuit at the inbox as an idempotent replay — NOT re-append the events.
    CommandResult redelivery = bus.execute(cmd, key);
    assertTrue(
        redelivery.idempotentReplay(),
        "client redelivery under the original key must short-circuit as an idempotent replay");
    assertEquals(
        1,
        streamLength(backing, streamId),
        "the events must exist exactly once after the DLQ round trip + client redelivery");
  }

  private static int streamLength(InMemoryEventStore store, StreamId streamId) {
    return store.readStream(streamId, Version.initial(), 1000).size();
  }
}
