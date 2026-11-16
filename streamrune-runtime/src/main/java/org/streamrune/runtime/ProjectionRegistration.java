package org.streamrune.runtime;

import org.streamrune.core.Internal;
import org.streamrune.core.projection.CheckpointTarget;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.types.ProjectionName;

/**
 * Registration of a projection with its name, error handling strategy, delivery mode and, when it
 * has one, the checkpoint target it commits through.
 *
 * @param name unique name for offset tracking
 * @param projection the projection to run
 * @param errorStrategy how failures are handled
 * @param deliveryMode what this projection promises: {@code TRANSACTIONAL_LOCAL} / {@code
 *     EXTERNAL_EFFECT} are handed the processor's transaction-scoped repository and are checked
 *     against it at {@code build()}; {@code AT_LEAST_ONCE_IDEMPOTENT} is handed {@code null} under
 *     every processor. Required; no registration path supplies a default.
 * @param target the pair this registration reads its checkpoint from and commits through, or {@code
 *     null} to use its runner's default pair (the runner-wide {@code offsetStore} and {@code
 *     atomicProcessor}). A runner never mixes the two: a registration with a target reads and
 *     commits through the target alone.
 * @hidden
 */
@Internal
public record ProjectionRegistration(
    ProjectionName name,
    Projection projection,
    ProjectionErrorStrategy errorStrategy,
    ProjectionDeliveryMode deliveryMode,
    CheckpointTarget target) {

  /** Creates a registration with the default HALT error strategy on its runner's default pair. */
  public ProjectionRegistration(
      ProjectionName name, Projection projection, ProjectionDeliveryMode mode) {
    this(name, projection, ProjectionErrorStrategy.HALT, mode);
  }

  /** Creates a registration on its runner's default pair. */
  public ProjectionRegistration(
      ProjectionName name,
      Projection projection,
      ProjectionErrorStrategy strategy,
      ProjectionDeliveryMode mode) {
    this(name, projection, strategy, mode, null);
  }

  public ProjectionRegistration {
    if (name == null) {
      throw new IllegalArgumentException("name is required");
    }
    if (projection == null) {
      throw new IllegalArgumentException("projection is required");
    }
    if (errorStrategy == null) {
      throw new IllegalArgumentException("errorStrategy is required");
    }
    if (deliveryMode == null) {
      throw new IllegalArgumentException("deliveryMode is required");
    }
  }
}
