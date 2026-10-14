package org.streamrune.core.saga;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.core.types.SagaType;

/** The refusal of a saga's forward command at its commit. */
class SagaForwardFencedExceptionTest {

  @Test
  void namesTheSagaAndSanitisesItsId() {
    var origin =
        SagaOrigin.dispatchedBy(SagaType.of("orders.Fulfillment"), SagaId.of("saga-1\rFORGED"));

    var e = new SagaForwardFencedException(origin);

    assertThat(e.getMessage())
        .isEqualTo(
            "Forward command of saga orders.Fulfillment saga-1FORGED refused: the saga has"
                + " left its forward phase (a compensation episode is open, the saga is terminal,"
                + " or its row is gone)");
    assertThat(e.getMessage()).as("the control character is removed").doesNotContain("\r");
    assertThat(e.sagaType()).isEqualTo(SagaType.of("orders.Fulfillment"));
    assertThat(e.sagaId().value()).as("the raw id is kept for callers").contains("\r");
    assertThat(e).isInstanceOf(RuntimeException.class).isNotInstanceOf(IllegalStateException.class);
  }

  @Test
  void requiresAnOrigin() {
    assertThatThrownBy(() -> new SagaForwardFencedException(null))
        .isInstanceOf(NullPointerException.class)
        .hasMessage("forwardOf is required");
  }
}
