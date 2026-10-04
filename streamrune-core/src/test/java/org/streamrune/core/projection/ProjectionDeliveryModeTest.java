package org.streamrune.core.projection;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/** Three values; only AT_LEAST_ONCE_IDEMPOTENT keeps the runner's repository away. */
class ProjectionDeliveryModeTest {

  @Test
  void exactlyThreeModes_inDeclarationOrder() {
    assertThat(ProjectionDeliveryMode.values())
        .containsExactly(
            ProjectionDeliveryMode.TRANSACTIONAL_LOCAL,
            ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT,
            ProjectionDeliveryMode.EXTERNAL_EFFECT);
  }

  @Test
  void onlyTheTwoTransactionalModesWriteInTheCheckpointTransaction() {
    assertThat(ProjectionDeliveryMode.TRANSACTIONAL_LOCAL.writesInCheckpointTransaction()).isTrue();
    assertThat(ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT.writesInCheckpointTransaction())
        .isFalse();
    assertThat(ProjectionDeliveryMode.EXTERNAL_EFFECT.writesInCheckpointTransaction()).isTrue();
  }
}
