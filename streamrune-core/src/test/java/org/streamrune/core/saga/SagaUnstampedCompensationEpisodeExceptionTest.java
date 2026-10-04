package org.streamrune.core.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import org.junit.jupiter.api.Test;

/** The refusal of a compensation episode whose row carries no durable stamp. */
class SagaUnstampedCompensationEpisodeExceptionTest {

  private static final SagaId RAW_ID = SagaId.of("saga-1\nFORGED LINE");

  @Test
  void ofRow_namesBothHalvesOfTheStamp_asTheRowCarriesThem() {
    var row =
        new LoadedSaga<>(
            new TestState(),
            SagaStatus.COMPENSATING,
            3L,
            Instant.parse("2026-10-02T10:00:00Z"),
            /* episodeVersion= */ 3L,
            /* episodeClaimedAt= */ null,
            true,
            null,
            false,
            null,
            null);

    var e = SagaUnstampedCompensationEpisodeException.ofRow(RAW_ID, row);

    assertThat(e)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("episode_version=3, episode_claimed_at=null")
        .hasMessageContaining("Nothing was dispatched")
        .hasMessageContaining("SagaStore.claimCompensating");
    assertThat(e.sagaId()).as("the raw id is kept for callers").isEqualTo(RAW_ID);
  }

  @Test
  void ofEnumeration_namesTheSweepersView() {
    var e = SagaUnstampedCompensationEpisodeException.ofEnumeration(RAW_ID);
    assertThat(e)
        .hasMessageContaining("episode_claimed_at=null")
        .hasMessageContaining("findCompensating");
  }

  /** The message reaches log records and dead-letter entries: the id is sanitized there. */
  @Test
  void theMessageSanitizesTheSagaId() {
    var e = new SagaUnstampedCompensationEpisodeException(RAW_ID, "episode_version=null");
    assertThat(e.getMessage()).doesNotContain("\n").contains("saga-1");
    assertThat(e.sagaId().value()).contains("\n");
  }

  private record TestState() implements SagaState {
    @Override
    public SagaStatus status() {
      return SagaStatus.COMPENSATING;
    }
  }
}
