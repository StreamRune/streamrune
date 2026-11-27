package org.streamrune.runtime.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.ProjectionCommitFencedException;
import org.streamrune.core.projection.ProjectionCommitFencedException.Guard;
import org.streamrune.core.projection.ProjectionEpochRegressionException;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;

class CheckpointFenceRulesTest {

  private static final ProjectionName P = ProjectionName.of("p");

  @Test
  void staleEpochIsFencedWithItsGuardAndMessage() {
    var e =
        assertThrows(
            ProjectionCommitFencedException.class,
            () -> CheckpointFenceRules.rejectStaleEpoch(P, 4L, 5L));

    assertThat(e.guard()).isSameAs(Guard.EPOCH_FENCE);
    assertThat(e.getMessage())
        .isEqualTo(
            "Projection 'p' commit fenced out: caller epoch 4 is below the stored epoch 5 — a"
                + " newer leader has taken over the lease. Rolling back the stale leader's write"
                + " (epoch fencing).");
  }

  @Test
  void anEpochAtOrAboveTheStoredOnePasses() {
    assertThatCode(() -> CheckpointFenceRules.rejectStaleEpoch(P, 5L, 5L))
        .doesNotThrowAnyException();
    assertThatCode(() -> CheckpointFenceRules.rejectStaleEpoch(P, 6L, 5L))
        .doesNotThrowAnyException();
  }

  @Test
  void zeroCallerEpochNeverFences() {
    assertThatCode(() -> CheckpointFenceRules.rejectStaleEpoch(P, 0L, 5L))
        .doesNotThrowAnyException();
  }

  @Test
  void overlappingBatchIsFencedWithItsGuardAndMessage() {
    var e =
        assertThrows(
            ProjectionCommitFencedException.class,
            () -> CheckpointFenceRules.rejectOverlappingBatch(P, GlobalOffset.of(9), 10L));

    assertThat(e.guard()).isSameAs(Guard.OVERLAP);
    assertThat(e.getMessage())
        .isEqualTo(
            "Projection 'p' batch starting at offset 9 overlaps the committed checkpoint (10):"
                + " those events were already applied by another writer (a concurrent or"
                + " failed-over stale leader, or a redelivery). Rolling back so the read-model"
                + " write is not applied a second time.");
  }

  @Test
  void equalOffsetOverlaps() {
    var e =
        assertThrows(
            ProjectionCommitFencedException.class,
            () -> CheckpointFenceRules.rejectOverlappingBatch(P, GlobalOffset.of(10), 10L));

    assertThat(e.guard()).isSameAs(Guard.OVERLAP);
  }

  @Test
  void aBatchStartingAfterTheCheckpointPasses() {
    assertThatCode(() -> CheckpointFenceRules.rejectOverlappingBatch(P, GlobalOffset.of(11), 10L))
        .doesNotThrowAnyException();
  }

  @Test
  void nullFirstOffsetNeverOverlaps() {
    assertThatCode(() -> CheckpointFenceRules.rejectOverlappingBatch(P, null, Long.MAX_VALUE))
        .doesNotThrowAnyException();
  }

  @Test
  void monotonicRejectionIsReturnedWithItsGuardAndMessage() {
    ProjectionCommitFencedException e =
        CheckpointFenceRules.monotonicRejection(P, GlobalOffset.of(10));

    assertThat(e.guard()).isSameAs(Guard.MONOTONIC);
    assertThat(e.getMessage())
        .isEqualTo(
            "Projection 'p' offset advance to 10 was rejected by the monotonic guard (stored"
                + " offset already at/past it): the batch is already applied. Rolling back so the"
                + " read-model write is not committed a second time.");
  }

  @Test
  void epochRegressionIsTheRegressionTypeWithBothEpochs() {
    var e =
        assertThrows(
            ProjectionEpochRegressionException.class,
            () -> CheckpointFenceRules.rejectEpochRegression(P, 3L, 7L));

    assertThat(e.getClass()).isSameAs(ProjectionEpochRegressionException.class);
    assertThat(e.getMessage())
        .isEqualTo(new ProjectionEpochRegressionException(P, 3L, 7L).getMessage());
  }

  @Test
  void regressionOnlyWhenStoredIsHigher() {
    assertThatCode(() -> CheckpointFenceRules.rejectEpochRegression(P, 7L, 7L))
        .as("equal: the ordinary re-stamp")
        .doesNotThrowAnyException();
    assertThatCode(() -> CheckpointFenceRules.rejectEpochRegression(P, 8L, 7L))
        .doesNotThrowAnyException();
  }

  @Test
  void shownSanitizesTheName() {
    var name = ProjectionName.of("a\nb");

    assertThat(CheckpointFenceRules.shown(name))
        .isEqualTo(LogSanitizer.sanitizeForLog("a\nb"))
        .doesNotContain("\n");
    var e =
        assertThrows(
            ProjectionCommitFencedException.class,
            () -> CheckpointFenceRules.rejectStaleEpoch(name, 1L, 2L));
    assertThat(e.getMessage()).doesNotContain("\n");
  }
}
