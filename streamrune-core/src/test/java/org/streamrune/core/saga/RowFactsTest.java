package org.streamrune.core.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaStore.RowFacts;

class RowFactsTest {

  @Test
  void aNullStatus_isRefused_likeTheOtherRecordsOfTheInterface() {
    assertThatThrownBy(() -> new RowFacts(null, false, 3L))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("status");
  }

  @Test
  void theFacts_areKeptAsGiven_aNullAppliedOffsetIncluded() {
    var facts = new RowFacts(SagaStatus.RUNNING, true, null);
    assertThat(facts.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(facts.deadLetterPending()).isTrue();
    assertThat(facts.lastAppliedOffset()).isNull();
  }
}
