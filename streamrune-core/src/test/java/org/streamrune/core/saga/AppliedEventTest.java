package org.streamrune.core.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.types.GlobalOffset;

class AppliedEventTest {

  private static final GlobalOffset OFFSET = GlobalOffset.of(42L);

  @Test
  void live_isACorrelatedEventOnTheLivePath() {
    var applied = AppliedEvent.live(OFFSET);
    assertThat(applied.eventOffset()).isEqualTo(OFFSET);
    assertThat(applied.startPath()).isFalse();
    assertThat(applied.replayRedrive()).isFalse();
  }

  @Test
  void liveStart_isTheStartEventOnTheLivePath() {
    var applied = AppliedEvent.liveStart(OFFSET);
    assertThat(applied.eventOffset()).isEqualTo(OFFSET);
    assertThat(applied.startPath()).isTrue();
    assertThat(applied.replayRedrive()).isFalse();
  }

  @Test
  void replayed_isACorrelatedEventFedByTheReplayDrain() {
    var applied = AppliedEvent.replayed(OFFSET);
    assertThat(applied.eventOffset()).isEqualTo(OFFSET);
    assertThat(applied.startPath()).isFalse();
    assertThat(applied.replayRedrive()).isTrue();
  }

  @Test
  void replayedStart_isTheStartEventFedByTheReplayDrain() {
    var applied = AppliedEvent.replayedStart(OFFSET);
    assertThat(applied.eventOffset()).isEqualTo(OFFSET);
    assertThat(applied.startPath()).isTrue();
    assertThat(applied.replayRedrive()).isTrue();
  }

  @Test
  void aNullOffsetIsRejected() {
    assertThatThrownBy(() -> AppliedEvent.live(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("eventOffset is required");
  }
}
