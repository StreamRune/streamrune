package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.infra.Blackhole;
import org.streamrune.runtime.StreamRuneBenchmark.CommandBusState;
import org.streamrune.runtime.StreamRuneBenchmark.DeciderState;
import org.streamrune.runtime.StreamRuneBenchmark.EventStoreState;

/**
 * Runs every {@link StreamRuneBenchmark} {@code @Benchmark} method twice against a real
 * {@code @Setup} state, outside the JMH harness (no forks, no warmup/measurement iterations — this
 * asserts correctness, not performance).
 *
 * <p>Wired into {@code check} via the {@code jmhSmokeTest} Gradle task (this module's build script)
 * so a benchmark that compiles but throws — or silently measures an unpopulated/degenerate state —
 * on a real invocation fails the build. Compiling {@code src/jmh} as part of the root build closed
 * the "this code doesn't even build" blind spot; it does not, and cannot, catch a benchmark that
 * builds cleanly and breaks (or lies) at runtime — which is exactly that class of bug this test
 * exists to pin.
 *
 * <p>Twice, not once, is deliberate: it is exactly the repro shape for it — {@code
 * eventStoreAppend} always re-appended at {@code Version.initial()}, so a single invocation looked
 * completely fine and only the SECOND ever call within a JMH iteration threw {@code
 * OptimisticLockException}.
 */
class StreamRuneBenchmarkSmokeTest {

  // JMH's Blackhole deliberately refuses construction without this exact acknowledgement string
  // (see org.openjdk.jmh.infra.Blackhole's public constructor) -- outside the JMH harness, nothing
  // else supplies one.
  private static final String BLACKHOLE_CONSENT =
      "Today's password is swordfish. I understand instantiating Blackholes directly is"
          + " dangerous.";

  private final StreamRuneBenchmark benchmark = new StreamRuneBenchmark();

  @Test
  @DisplayName("commandBusThroughput runs twice without throwing")
  void commandBusThroughputRunsTwice() {
    CommandBusState state = new CommandBusState();
    state.setup();

    assertDoesNotThrow(() -> benchmark.commandBusThroughput(state));
    assertDoesNotThrow(() -> benchmark.commandBusThroughput(state));
  }

  @Test
  @DisplayName(
      "eventStoreAppend runs twice without throwing (pin: the 2nd call used to throw OptimisticLockException)")
  void eventStoreAppendRunsTwice() {
    EventStoreState state = new EventStoreState();
    state.batchSize = 10;
    state.setup();

    assertDoesNotThrow(() -> benchmark.eventStoreAppend(state));
    assertDoesNotThrow(() -> benchmark.eventStoreAppend(state));
  }

  @Test
  @DisplayName("eventStoreAppend rotates onto a fresh stream instead of growing one without bound")
  void eventStoreAppendRotatesStreams() {
    EventStoreState state = new EventStoreState();
    state.batchSize = 100;
    state.setup();
    var firstStream = state.streamId;

    // MAX_STREAM_EVENTS is 200 and batchSize is 100, so the 3rd call must rotate onto a new
    // stream (2 calls fill the current one to exactly the cap; the 3rd starts a fresh one first).
    benchmark.eventStoreAppend(state);
    benchmark.eventStoreAppend(state);
    benchmark.eventStoreAppend(state);

    assertThat(state.streamId).isNotEqualTo(firstStream);
  }

  @Test
  @DisplayName(
      "eventStoreLoad runs twice and actually sees populated events (pin: was an always-empty stream)")
  void eventStoreLoadRunsTwiceAgainstPopulatedStream() {
    EventStoreState state = new EventStoreState();
    state.batchSize = 10;
    state.setup();
    Blackhole bh = new Blackhole(BLACKHOLE_CONSENT);

    assertDoesNotThrow(() -> benchmark.eventStoreLoad(state, bh));
    assertDoesNotThrow(() -> benchmark.eventStoreLoad(state, bh));

    assertThat(state.store.load(state.loadStreamId).events()).hasSize(10);
  }

  @Test
  @DisplayName("eventStoreLoadWithSnapshot runs twice and only replays the post-snapshot tail")
  void eventStoreLoadWithSnapshotRunsTwiceAndReplaysOnlyTail() {
    EventStoreState state = new EventStoreState();
    state.batchSize = 50;
    state.setup();
    Blackhole bh = new Blackhole(BLACKHOLE_CONSENT);

    assertDoesNotThrow(() -> benchmark.eventStoreLoadWithSnapshot(state, bh));
    assertDoesNotThrow(() -> benchmark.eventStoreLoadWithSnapshot(state, bh));

    var history = state.store.load(state.snapshotStreamId);
    assertThat(history.snapshotState()).isNotNull();
    assertThat(history.events()).hasSize(5);
  }

  @Test
  @DisplayName("deciderDecide runs twice without throwing")
  void deciderDecideRunsTwice() {
    DeciderState state = new DeciderState();
    state.stateSize = 10;
    state.setup();

    assertDoesNotThrow(() -> benchmark.deciderDecide(state));
    assertDoesNotThrow(() -> benchmark.deciderDecide(state));
  }
}
