package org.streamrune.core.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

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
            true,
            SagaStatus.COMPENSATING,
            true,
            10L,
            null);
    assertThat(faulted.effectiveStatus()).isEqualTo(SagaStatus.COMPENSATING);
    var genesisFault =
        new LoadedSaga<>(
            new S(SagaStatus.STARTED),
            SagaStatus.FAULTED,
            1L,
            Instant.EPOCH,
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
