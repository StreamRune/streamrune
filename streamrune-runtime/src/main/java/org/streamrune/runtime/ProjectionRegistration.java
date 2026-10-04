package org.streamrune.runtime;

import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionErrorStrategy;
import org.streamrune.core.types.ProjectionName;

/**
 * Registration of a projection with its name, error handling strategy and delivery mode.
 *
 * @param name unique name for offset tracking
 * @param projection the projection to run
 * @param errorStrategy how failures are handled
 * @param deliveryMode what this projection promises: {@code TRANSACTIONAL_LOCAL} / {@code
 *     EXTERNAL_EFFECT} are handed the processor's transaction-scoped repository and are checked
 *     against it at {@code build()}; {@code AT_LEAST_ONCE_IDEMPOTENT} is handed {@code null} under
 *     every processor. Required; no registration path supplies a default.
 */
public record ProjectionRegistration(
    ProjectionName name,
    Projection projection,
    ProjectionErrorStrategy errorStrategy,
    ProjectionDeliveryMode deliveryMode) {

  /** Creates a registration with the default HALT error strategy. */
  public ProjectionRegistration(String name, Projection projection, ProjectionDeliveryMode mode) {
    this(ProjectionName.of(name), projection, ProjectionErrorStrategy.HALT, mode);
  }

  /** Creates a registration with an explicit error strategy. */
  public ProjectionRegistration(
      String name,
      Projection projection,
      ProjectionErrorStrategy errorStrategy,
      ProjectionDeliveryMode mode) {
    this(ProjectionName.of(name), projection, errorStrategy, mode);
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
