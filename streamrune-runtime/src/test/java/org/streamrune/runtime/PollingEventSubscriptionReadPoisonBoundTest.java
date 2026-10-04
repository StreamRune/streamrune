package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * The live read-poison bound must actually bound a deterministic upcaster failure.
 *
 * <p>A user upcaster's deterministic throw (NPE, CCE, malformed output) used to propagate raw out
 * of the store; {@code ReadPoisonClassifier} matched none of its rules, classified it TRANSIENT,
 * and {@code PollingEventSubscription.onReadFailure} then RESET the poison streak on every
 * occurrence — so the subscription retried the identical unreadable event forever: nothing
 * dead-lettered, health UP, checkpoint frozen. With the typed {@link EventDeserializationException}
 * frame and the classifier treating it as poison, the streak now counts up and the subscription
 * stops terminally at the configured bound.
 */
class PollingEventSubscriptionReadPoisonBoundTest {

  private static final SubscriptionConfig CONFIG =
      new SubscriptionConfig(false, Duration.ofMillis(50), Duration.ofMillis(5));

  static final class InMemoryOffsetStore implements OffsetStore {
    final ConcurrentHashMap<String, GlobalOffset> offsets = new ConcurrentHashMap<>();

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return offsets.getOrDefault(projectionName.value(), GlobalOffset.initial());
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      offsets.put(projectionName.value(), offset);
    }
  }

  /** Read-only store whose {@code readGlobalStream} throws whatever the test scripts. */
  static final class ThrowingEventStore implements EventStore {
    Supplier<RuntimeException> nextFailure;

    ThrowingEventStore(Supplier<RuntimeException> nextFailure) {
      this.nextFailure = nextFailure;
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      throw nextFailure.get();
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public EventStore.AppendResult append(
        StreamId streamId, List<EventEnvelope> newEvents, Version expectedVersion) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      throw new UnsupportedOperationException();
    }
  }

  private static RuntimeException upcasterPoison() {
    // The exact shape PostgresEventStore.toEnvelope now produces for a throwing upcaster:
    // the typed deterministic wrapper over the user code's own exception.
    return new EventDeserializationException(
        "Failed to deserialize event data for stream: cart-1",
        new NullPointerException("upcaster bug"));
  }

  private static PollingEventSubscription subscription(EventStore store, int bound) {
    return PollingEventSubscription.builder()
        .subscriptionName("poison-bound-sub")
        .eventStore(store)
        .offsetStore(new InMemoryOffsetStore())
        .listener(events -> {})
        .config(CONFIG)
        .readPoisonBound(bound)
        .build();
  }

  @Test
  void deterministicUpcasterFailureStopsTerminallyAtTheBound_counterNotReset() {
    var store = new ThrowingEventStore(PollingEventSubscriptionReadPoisonBoundTest::upcasterPoison);
    var sub = subscription(store, 3);

    // The first bound-1 polls rethrow the read error (streak counting, not yet terminal) — the
    // pre-fix defect was that each of these RESET the streak, so the bound was never reached.
    for (int i = 0; i < 2; i++) {
      assertThrows(EventDeserializationException.class, sub::pollOnce);
      assertThat(sub.readPoisonError())
          .as("below the bound the subscription must not be terminally stopped yet")
          .isNull();
    }

    // The bound-th consecutive poison read at the same checkpoint stops the subscription.
    assertThatThrownBy(sub::pollOnce).isInstanceOf(ProjectionReadPoisonException.class);
    assertThat(sub.readPoisonError())
        .as("the terminal poison error must be recorded for the owning runner")
        .isNotNull();
    assertThat(sub.state()).isEqualTo(SubscriptionLifecycleState.STOPPED);
  }

  @Test
  void transientInfraFailureBetweenPoisonReadsResetsTheStreak() {
    // A genuine transient failure (SQL-caused) between poison reads legitimately resets the
    // streak: the bound counts CONSECUTIVE deterministic failures only.
    var failures =
        new java.util.ArrayDeque<RuntimeException>(
            List.of(
                upcasterPoison(),
                upcasterPoison(),
                new EventStoreException("blip", new SQLException("conn reset")),
                upcasterPoison(),
                upcasterPoison()));
    var store = new ThrowingEventStore(failures::poll);
    var sub = subscription(store, 3);

    for (int i = 0; i < 5; i++) {
      assertThrows(RuntimeException.class, sub::pollOnce);
    }
    assertThat(sub.readPoisonError())
        .as("2 poisons + transient + 2 poisons never reaches a streak of 3")
        .isNull();
    assertThat(sub.state())
        .as("the subscription must still be retryable after a healed transient")
        .isNotEqualTo(SubscriptionLifecycleState.STOPPED);
  }
}
