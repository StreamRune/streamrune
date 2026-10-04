package org.streamrune.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;

class ProjectionConfigTest {

  @ProjectionConfig(
      name = "default-mode-bean",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class DefaultModeBean {}

  @ProjectionConfig(
      name = "scheduled-bean",
      mode = ProjectionConfig.Mode.SCHEDULED,
      cron = "0 0 * * * *",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class ScheduledBean {}

  @ProjectionConfig(
      name = "fully-configured",
      mode = ProjectionConfig.Mode.INLINE,
      errorStrategy = ProjectionErrorStrategy.DLQ,
      batchSize = 200,
      fetchSize = 500,
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class FullyConfigured {}

  @Test
  void modeContinuousByDefault() {
    var cfg = DefaultModeBean.class.getAnnotation(ProjectionConfig.class);
    assertThat(cfg).isNotNull();
    assertThat(cfg.name()).isEqualTo("default-mode-bean");
    assertThat(cfg.mode()).isEqualTo(ProjectionConfig.Mode.CONTINUOUS);
    assertThat(cfg.cron()).isEmpty();
    assertThat(cfg.errorStrategy()).isEqualTo(ProjectionErrorStrategy.HALT);
    assertThat(cfg.batchSize()).isEqualTo(-1);
    assertThat(cfg.fetchSize()).isEqualTo(-1);
    assertThat(cfg.deliveryMode()).isEqualTo(ProjectionDeliveryMode.TRANSACTIONAL_LOCAL);
  }

  @Test
  void scheduledModeReadsCron() {
    var cfg = ScheduledBean.class.getAnnotation(ProjectionConfig.class);
    assertThat(cfg.name()).isEqualTo("scheduled-bean");
    assertThat(cfg.mode()).isEqualTo(ProjectionConfig.Mode.SCHEDULED);
    assertThat(cfg.cron()).isEqualTo("0 0 * * * *");
  }

  @Test
  void allFieldsConfigurable() {
    var cfg = FullyConfigured.class.getAnnotation(ProjectionConfig.class);
    assertThat(cfg.name()).isEqualTo("fully-configured");
    assertThat(cfg.mode()).isEqualTo(ProjectionConfig.Mode.INLINE);
    assertThat(cfg.errorStrategy()).isEqualTo(ProjectionErrorStrategy.DLQ);
    assertThat(cfg.batchSize()).isEqualTo(200);
    assertThat(cfg.fetchSize()).isEqualTo(500);
    assertThat(cfg.deliveryMode()).isEqualTo(ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT);
  }

  @Test
  void deliveryModeHasNoDefault_andAtLeastOnceIsGone() throws Exception {
    // A missing deliveryMode attribute is a COMPILE error, which is stronger than a startup
    // refusal — pinned here so nobody re-adds a default.
    assertThat(ProjectionConfig.class.getMethod("deliveryMode").getDefaultValue()).isNull();
    assertThatThrownBy(() -> ProjectionConfig.class.getMethod("atLeastOnce"))
        .isInstanceOf(NoSuchMethodException.class);
  }
}
