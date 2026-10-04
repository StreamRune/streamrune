package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStartFixtures.TYPE;
import static org.streamrune.runtime.SagaStartFixtures.correlatedEvent;
import static org.streamrune.runtime.SagaStartFixtures.startEvent;

import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.SagaId;
import org.streamrune.runtime.SagaStartFixtures.CorrelatingStartOrchestrator;
import org.streamrune.runtime.SagaStartFixtures.ScriptedBus;
import org.streamrune.runtime.SagaStartFixtures.StartState;
import org.streamrune.test.InMemorySagaStore;

/** Live redeliveries dedup on the maximum; replay reruns dedup on the exact offset. */
class SagaLiveIdempotencyTest {
  private final SagaId sagaId = SagaId.of("saga-1");

  @Test
  void liveRedeliveryOfAnAppliedOffset_isSkippedDedup_stateEvolvesOnce() {
    var store = new InMemorySagaStore();
    var orchestrator = new CorrelatingStartOrchestrator();
    var exec =
        new SagaStepExecutor<>(orchestrator, store, new ScriptedBus(), StreamRuneMetrics.NOOP);
    exec.execute(
        sagaId, Optional.empty(), new SagaTrigger.ForwardStep(startEvent(3L), true, false));
    var corr = correlatedEvent(5L);

    StepOutcome first =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(corr, false, false));
    StepOutcome again =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(corr, false, false));

    assertThat(first).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(again).isEqualTo(StepOutcome.SKIPPED_DEDUP);
    assertThat(orchestrator.consumedCorrelatedOffsets).containsExactly(5L);
    assertThat(store.load(sagaId, TYPE, StartState.class).orElseThrow().lastAppliedOffset())
        .isEqualTo(5L);
  }

  @Test
  void replayOfAnOlderDeferredEntry_isApplied_thenItsRerunIsSkippedDedup() {
    var store = new InMemorySagaStore();
    var orchestrator = new CorrelatingStartOrchestrator();
    var exec =
        new SagaStepExecutor<>(orchestrator, store, new ScriptedBus(), StreamRuneMetrics.NOOP);
    exec.execute(
        sagaId, Optional.empty(), new SagaTrigger.ForwardStep(startEvent(3L), true, false));
    var older = correlatedEvent(2L); // held before the genesis, drained after it

    StepOutcome fed =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(older, false, true));
    StepOutcome rerun =
        exec.execute(
            sagaId,
            store.load(sagaId, TYPE, StartState.class),
            new SagaTrigger.ForwardStep(older, false, true));

    assertThat(fed).isEqualTo(StepOutcome.FORWARD_PROGRESSED);
    assertThat(rerun)
        .as("the crashed drain's rerun sees its own exact offset")
        .isEqualTo(StepOutcome.SKIPPED_DEDUP);
    assertThat(orchestrator.consumedCorrelatedOffsets).containsExactly(2L);
    var row = store.load(sagaId, TYPE, StartState.class).orElseThrow();
    assertThat(row.lastAppliedOffset()).as("the live maximum never goes backwards").isEqualTo(3L);
    assertThat(row.lastReplayedOffset()).isEqualTo(2L);
  }

  @Test
  void correlatedEventOnAnAbsentRow_isSkippedNoRow_neverCreatesARow() {
    var store = new InMemorySagaStore();
    var exec =
        new SagaStepExecutor<>(
            new CorrelatingStartOrchestrator(), store, new ScriptedBus(), StreamRuneMetrics.NOOP);

    StepOutcome outcome =
        exec.execute(
            sagaId,
            Optional.empty(),
            new SagaTrigger.ForwardStep(correlatedEvent(5L), false, false));

    assertThat(outcome).isEqualTo(StepOutcome.SKIPPED_NO_ROW);
    assertThat(store.load(sagaId, TYPE, StartState.class)).isEmpty();
  }
}
