package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStartFixtures.TYPE;
import static org.streamrune.runtime.SagaStartFixtures.correlatedEvent;
import static org.streamrune.runtime.SagaStartFixtures.startEvent;

import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaDeadLetterStore.HoldResult;
import org.streamrune.core.saga.SagaDeadLetterStore.SagaDeadLetterEntry;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.runtime.SagaStartFixtures.CorrelatingStartOrchestrator;
import org.streamrune.runtime.SagaStartFixtures.ScriptedBus;
import org.streamrune.runtime.SagaStartFixtures.StartState;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * Every crash point of the poison handler on a correlated event converges, on the redelivery of the
 * page, to the end state of the run that did not crash.
 *
 * <p>The saga is started (its genesis applied at offset 3); the page holds the correlated event at
 * offset 5, whose {@code evolve} throws, and the correlated event at offset 6. The run that does
 * not crash ends with the row {@code FAULTED} (resumable from {@code RUNNING}) and shielded, and
 * with two entries: offset 5 carrying the poison's own error type and message, offset 6 recorded as
 * delivered while the saga was {@code FAULTED}. Each crash point stops the process right before or
 * right after one durable write; the subscription then delivers the page again until a delivery
 * completes, and twice more, as at-least-once delivery may.
 */
class SagaCorrelatedPoisonCrashPointTest {

  /** Where the process stops while the page is handled the first time. */
  enum CrashPoint {
    /** No crash: the page is delivered once, then twice again. */
    NONE,
    /** Right before the poison's entry and shield commit: neither is written. */
    BEFORE_ENTRY,
    /** Right after the poison's entry and shield committed, before the fault write. */
    AFTER_ENTRY,
    /** Right after the fault write committed. */
    AFTER_FAULT,
    /** Right after the next event's hold — its entry and shield — committed. */
    AFTER_HOLD
  }

  private static final SagaId SAGA = SagaId.of("saga-1");

  /** The durable facts a run leaves behind, and what it dispatched and applied. */
  record EndState(
      SagaStatus status,
      SagaStatus preFaultStatus,
      long version,
      boolean shielded,
      boolean genesisApplied,
      Long lastAppliedOffset,
      List<String> entries,
      List<Long> consumed,
      int stepOneDispatches) {}

  /** One process's stores and runner; a crash keeps what the stores committed. */
  private static final class World {
    final InMemorySagaStore rows = new InMemorySagaStore();
    final ForwardingSagaStore sagaStore = new ForwardingSagaStore(rows);
    final InMemorySagaDeadLetterStore entries = new InMemorySagaDeadLetterStore(rows);
    final CrashingDeadLetters deadLetters = new CrashingDeadLetters(entries);
    final ScriptedBus bus = new ScriptedBus();
    final CorrelatingStartOrchestrator orchestrator = new CorrelatingStartOrchestrator();
    final SagaRunner<StartState> runner =
        SagaRunner.<StartState>builder()
            .orchestrator(orchestrator)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(deadLetters)
            // The bus is a test double that stores nothing; the fence is tested elsewhere.
            .forwardFence(false)
            .build();

    World() {
      orchestrator.poisonCorrelatedAt = 5L;
    }

    void deliver(List<EventEnvelope> page) {
      runner.asEventListener().onEvents(page);
    }

    EndState endState() {
      LoadedSaga<StartState> row = rows.load(SAGA, TYPE, StartState.class).orElseThrow();
      List<String> recorded =
          entries.findBySaga(SAGA).stream()
              .sorted(Comparator.comparingLong(e -> e.eventOffset().value()))
              .map(World::describe)
              .toList();
      return new EndState(
          row.status(),
          row.preFaultStatus(),
          row.version(),
          row.deadLetterPending(),
          row.genesisApplied(),
          row.lastAppliedOffset(),
          recorded,
          List.copyOf(orchestrator.consumedCorrelatedOffsets),
          bus.count("step-1"));
    }

    private static String describe(SagaDeadLetterEntry e) {
      return e.eventOffset().value()
          + " "
          + e.sagaType().value()
          + " "
          + e.errorType()
          + ": "
          + e.errorMessage();
    }
  }

  /**
   * The dead-letter store with two more seams: a crash right before the poison's entry write
   * delegates, and one right after a hold wrote its entry.
   */
  private static final class CrashingDeadLetters extends ForwardingSagaDeadLetterStore {
    private boolean crashBeforeTheNextEntry;
    private boolean crashAfterTheNextHold;

    CrashingDeadLetters(InMemorySagaDeadLetterStore delegate) {
      super(delegate);
    }

    void crashOnceBeforeTheEntry() {
      crashBeforeTheNextEntry = true;
    }

    void crashOnceAfterTheNextHold() {
      crashAfterTheNextHold = true;
    }

    @Override
    public HoldResult publishShieldedIfAbsent(SagaDeadLetterEntry entry) {
      HoldResult result = super.publishShieldedIfAbsent(entry);
      if (crashAfterTheNextHold && result instanceof HoldResult.Written) {
        crashAfterTheNextHold = false;
        throw new ForwardingSagaStore.SimulatedCrash("the hold's entry and shield");
      }
      return result;
    }

    @Override
    public void publishShielded(SagaDeadLetterEntry entry) {
      if (crashBeforeTheNextEntry) {
        crashBeforeTheNextEntry = false;
        throw new ForwardingSagaStore.SimulatedCrash("nothing: before the entry write");
      }
      super.publishShielded(entry);
    }
  }

  private static List<EventEnvelope> page() {
    return List.of(correlatedEvent(5L), correlatedEvent(6L));
  }

  private static World started() {
    World world = new World();
    world.deliver(List.of(startEvent(3L)));
    return world;
  }

  private static EndState uninterrupted() {
    World world = started();
    world.deliver(page());
    world.deliver(page());
    world.deliver(page());
    return world.endState();
  }

  @ParameterizedTest
  @EnumSource(CrashPoint.class)
  void theRerunEndsWhereTheUninterruptedRunEnds(CrashPoint point) {
    EndState expected = uninterrupted();
    assertThat(expected.status()).isEqualTo(SagaStatus.FAULTED);
    assertThat(expected.preFaultStatus()).isEqualTo(SagaStatus.RUNNING);
    assertThat(expected.shielded()).isTrue();
    assertThat(expected.lastAppliedOffset()).isEqualTo(3L);
    assertThat(expected.consumed()).isEmpty();
    assertThat(expected.entries())
        .containsExactly(
            "5 " + TYPE.value() + " java.lang.IllegalStateException: poison at 5",
            "6 "
                + TYPE.value()
                + " "
                + SagaSkippedWhileFaultedException.class.getName()
                + ": "
                + new SagaSkippedWhileFaultedException(SAGA, GlobalOffset.of(6L)).getMessage());

    World world = started();
    switch (point) {
      case NONE -> {}
      case BEFORE_ENTRY -> world.deadLetters.crashOnceBeforeTheEntry();
      case AFTER_ENTRY -> world.deadLetters.crashOnceAfter("publish");
      case AFTER_FAULT -> world.sagaStore.crashOnceAfter("markFaulted");
      case AFTER_HOLD -> world.deadLetters.crashOnceAfterTheNextHold();
    }

    int crashes = 0;
    while (true) {
      try {
        world.deliver(page());
        break;
      } catch (ForwardingSagaStore.SimulatedCrash crash) {
        crashes++;
        assertThat(crashes).as("an armed crash point fires once").isEqualTo(1);
      }
    }
    world.deliver(page());
    world.deliver(page());

    assertThat(crashes).isEqualTo(point == CrashPoint.NONE ? 0 : 1);
    assertThat(world.endState()).isEqualTo(expected);
    assertThat(world.sagaStore.calls())
        .as("no shield is written on its own: always with its entry, at the dead-letter store")
        .doesNotContain("setDeadLetterPending");
  }
}
