package org.streamrune.core.saga;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class SagaStatusTest {

  @Test
  void activeStatuses_areExactlyTheNonHaltedSet() {
    // Pins the coupling between SELECT_TIMED_OUT's positive IN list, the event-store baseline's
    // partial-index predicate, and SagaStatus: adding a status without updating both breaks this
    // test.
    var active =
        Arrays.stream(SagaStatus.values())
            .filter(s -> !s.isHalted())
            .map(Enum::name)
            .collect(Collectors.toSet());
    assertThat(active).containsExactlyInAnyOrder("STARTED", "RUNNING", "COMPENSATING");
  }
}
