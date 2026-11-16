package org.streamrune.runtime;

import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.types.ProjectionName;

/**
 * Internal registration record for {@link ScheduledProjectionRunner}.
 *
 * @param name unique projection name (offset key)
 * @param projection the projection to run
 * @param cron parsed cron expression
 * @param errorStrategy how failures are handled
 * @param deliveryMode what this projection promises; see {@link
 *     ProjectionRegistration#deliveryMode()}. Required; no registration path supplies a default.
 * @param target the pair this registration reads its checkpoint from and commits through, or {@code
 *     null} to use the runner's default pair; see {@link ProjectionRegistration#target()}
 */
record ScheduledProjectionRegistration(
    ProjectionName name,
    Projection projection,
    CronExpression cron,
    ProjectionErrorStrategy errorStrategy,
    ProjectionDeliveryMode deliveryMode,
    CheckpointTarget target) {

  /** Creates a registration on the runner's default pair. */
  ScheduledProjectionRegistration(
      ProjectionName name,
      Projection projection,
      CronExpression cron,
      ProjectionErrorStrategy errorStrategy,
      ProjectionDeliveryMode deliveryMode) {
    this(name, projection, cron, errorStrategy, deliveryMode, null);
  }

  ScheduledProjectionRegistration {
    if (name == null) {
      throw new IllegalArgumentException("name is required");
    }
    if (projection == null) {
      throw new IllegalArgumentException("projection is required");
    }
    if (cron == null) {
      throw new IllegalArgumentException("cron is required");
    }
    if (errorStrategy == null) {
      throw new IllegalArgumentException("errorStrategy is required");
    }
    if (deliveryMode == null) {
      throw new IllegalArgumentException("deliveryMode is required");
    }
  }
}
