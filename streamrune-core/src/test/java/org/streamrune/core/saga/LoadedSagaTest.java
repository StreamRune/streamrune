package org.streamrune.core.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.EventId;

class LoadedSagaTest {
  private record S(SagaStatus status) implements SagaState {}

  @Test
  void effectiveStatus_ofAFaultedRow_isTheRecordedPreFaultStatus() {
    var faulted =
        new LoadedSaga<>(
            new S(SagaStatus.RUNNING),
            SagaStatus.FAULTED,
            3L,
            Instant.EPOCH,
            2L,
            Instant.EPOCH,
            EventId.of("evt-1"),
            true,
            SagaStatus.COMPENSATING,
            true,
            10L,
            null);
    assertThat(faulted.effectiveStatus()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(faulted.episodeCauseEventId())
        .as("a fault out of an episode keeps the event that opened it")
        .isEqualTo(EventId.of("evt-1"));
    var genesisFault =
        new LoadedSaga<>(
            new S(SagaStatus.STARTED),
            SagaStatus.FAULTED,
            1L,
            Instant.EPOCH,
            null,
            null,
            null,
            false,
            null,
            true,
            null,
            null);
    assertThat(genesisFault.effectiveStatus())
        .as("genesis-pending fault: nothing to resume")
        .isNull();
    assertThat(genesisFault.episodeCauseEventId())
        .as("a row that never entered an episode names no cause")
        .isNull();
  }

  @Test
  void effectiveStatus_onANonFaultedRow_ignoresAStalePreFaultStatus() {
    var running =
        new LoadedSaga<>(
            new S(SagaStatus.RUNNING),
            SagaStatus.RUNNING,
            2L,
            Instant.EPOCH,
            null,
            null,
            null,
            true,
            SagaStatus.COMPENSATING,
            false,
            null,
            null);
    assertThat(running.effectiveStatus())
        .as("non-FAULTED row: preFaultStatus is stale and must not be read")
        .isEqualTo(SagaStatus.RUNNING);
  }
}
