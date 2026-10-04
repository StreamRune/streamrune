package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.types.ProjectionName;

class ProjectionRegistrationTest {

  private static final Projection P = batch -> {};

  @Test
  void threeArgForm_defaultsToHalt() {
    var reg = new ProjectionRegistration("orders", P, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);
    assertThat(reg.name()).isEqualTo(ProjectionName.of("orders"));
    assertThat(reg.errorStrategy()).isEqualTo(ProjectionErrorStrategy.HALT);
    assertThat(reg.deliveryMode()).isEqualTo(ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);
  }

  @Test
  void fourArgForm_carriesStrategyAndMode() {
    var reg =
        new ProjectionRegistration(
            "orders",
            P,
            ProjectionErrorStrategy.DLQ,
            ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);
    assertThat(reg.errorStrategy()).isEqualTo(ProjectionErrorStrategy.DLQ);
    assertThat(reg.deliveryMode()).isEqualTo(ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);
  }

  @Test
  void nullModeIsRefused_byBothRecords() {
    assertThatThrownBy(() -> new ProjectionRegistration("orders", P, (ProjectionDeliveryMode) null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("deliveryMode is required");
    assertThatThrownBy(
            () ->
                new ScheduledProjectionRegistration(
                    ProjectionName.of("orders"),
                    P,
                    CronExpression.parse("* * * * * *", java.time.ZoneOffset.UTC),
                    ProjectionErrorStrategy.HALT,
                    null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("deliveryMode is required");
  }

  @Test
  void theBooleanComponentIsGone() {
    assertThat(ProjectionRegistration.class.getRecordComponents())
        .extracting(java.lang.reflect.RecordComponent::getName)
        .containsExactly("name", "projection", "errorStrategy", "deliveryMode");
  }
}
