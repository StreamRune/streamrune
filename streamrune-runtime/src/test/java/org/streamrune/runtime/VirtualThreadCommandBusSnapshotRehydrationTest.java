package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryEventStore;

/**
 * Bus-level snapshot-rehydration round trip. Every other bus snapshot test installs an event-store
 * subclass whose {@code saveSnapshot} DISCARDS the snapshot, so {@code load()} never returns a
 * non-null {@code snapshotState} and {@link VirtualThreadCommandBus}'s snapshot-consuming branch —
 * {@code reconstructState} folding only the post-snapshot events, plus the {@code maybeSnapshot ->
 * next-command load -> reconstruct} round trip across a snapshot boundary — is dead code in the
 * suite. A defect there (not folding post-snapshot events, folding events already baked into the
 * snapshot, or an off-by-one snapshot version) would silently rehydrate wrong state.
 *
 * <p>This test drives the REAL {@link InMemoryEventStore} (which persists snapshots) with {@code
 * snapshotPolicy(everyNEvents(k))}: it runs more than {@code k} commands so a snapshot is written
 * mid-stream, then more commands, and asserts the final state reflects ALL commands and that a load
 * across the boundary returns the snapshot plus only the post-snapshot events. Each event carries
 * the resulting counter value, so a rehydration that dropped or double-applied events would produce
 * an observably wrong value on the very next command.
 */
class VirtualThreadCommandBusSnapshotRehydrationTest {

  private static final AggregateType TYPE = AggregateType.of("counter");

  sealed interface CountingCommand extends Command {
    record Inc(String id) implements CountingCommand {}
  }

  sealed interface CountingEvent extends DomainEvent {
    // The resulting counter value is baked into the event, so it can only be correct if decide()
    // saw the correctly rehydrated state.
    record Incremented(int newCount) implements CountingEvent {}
  }

  record CountingState(int count) implements AggregateState {
    CountingState() {
      this(0);
    }
  }

  static final class CountingDecider
      implements Decider<CountingCommand, CountingState, CountingEvent> {
    @Override
    public CountingState initialState() {
      return new CountingState();
    }

    @Override
    public List<CountingEvent> decide(CountingCommand command, CountingState state) {
      return switch (command) {
        case CountingCommand.Inc _ -> List.of(new CountingEvent.Incremented(state.count() + 1));
      };
    }

    @Override
    public CountingState evolve(CountingState state, CountingEvent event) {
      return switch (event) {
        case CountingEvent.Incremented e -> new CountingState(e.newCount());
      };
    }
  }

  private void runWithContext(Runnable action) {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("corr-1"), Instant.now(), Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, ctx).run(action);
  }

  @Test
  void busRehydratesFromSnapshotAcrossBoundaryAndReflectsAllCommands() {
    int k = 3;
    int total = 7; // > k, and crosses the snapshot boundary more than once (snapshots at v3, v6)
    var store = new InMemoryEventStore();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .snapshotPolicy(SnapshotPolicy.everyNEvents(k))
            .register(
                TYPE,
                CountingCommand.class,
                cmd ->
                    switch (cmd) {
                      case CountingCommand.Inc c -> AggregateId.of(c.id());
                    },
                new CountingDecider())
            .build();

    runWithContext(
        () -> {
          for (int i = 0; i < total; i++) {
            bus.execute(new CountingCommand.Inc("counter-1"));
          }
        });

    // Load across the snapshot boundary exactly as the bus does (snapshotVersion 1 for
    // everyNEvents(k)). A snapshot MUST have been persisted and reloaded — proving the bus took the
    // snapshotState != null branch on the commands after the boundary.
    AggregateHistory history = store.load(StreamId.of(TYPE, AggregateId.of("counter-1")), 1);
    assertNotNull(
        history.snapshotState(),
        "a snapshot must have been written mid-stream and reloaded through the bus's expected"
            + " snapshot version");

    var snapshot = (CountingState) history.snapshotState();
    long snapshotVersion = history.lastSnapshotVersion().value();
    assertTrue(snapshotVersion > 0 && snapshotVersion < total, "snapshot taken mid-stream");
    // For a pure monotonic counter, count == version at the snapshot point.
    assertEquals(
        snapshotVersion,
        snapshot.count(),
        "the persisted snapshot's count must equal its stream version (no double-apply at save)");

    // The reloaded window must contain ONLY the post-snapshot events, in order.
    for (EventEnvelope e : history.events()) {
      assertTrue(
          e.version().value() > snapshotVersion,
          "load across the boundary must return only events after the snapshot");
    }
    assertEquals(
        total - snapshotVersion,
        history.events().size(),
        "exactly the post-snapshot events replay on top of the snapshot");

    // Fold snapshot + post-snapshot events exactly as reconstructState does: the final state must
    // reflect ALL commands (no lost updates, no double-apply).
    var decider = new CountingDecider();
    CountingState rehydrated = snapshot;
    for (EventEnvelope e : history.events()) {
      rehydrated = decider.evolve(rehydrated, (CountingEvent) e.event());
    }
    assertEquals(total, rehydrated.count(), "final rehydrated state must reflect every command");

    // And the LAST event the bus produced carries the correct running total — it could only be
    // correct if the bus rehydrated the aggregate correctly from the snapshot before decide().
    var lastEvent = (CountingEvent.Incremented) history.events().getLast().event();
    assertEquals(
        total,
        lastEvent.newCount(),
        "the last command's event must carry the total, proving reconstructState consumed the"
            + " snapshot plus post-snapshot events correctly");
  }
}
