package org.streamrune.micronaut;

import io.micronaut.context.BeanProvider;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
import java.util.List;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;

/**
 * Declares every {@link ProjectionIndex} bean on the application's {@link ProjectionRepository}
 * bean — the module's {@code JdbcProjectionRepository} or one the application defines — on the
 * context's {@link StartupEvent}, in the startup validators' tier, below {@link
 * StreamRuneLifecycle}, so before any projection runner starts and before the first request. A
 * refused key, an index that cannot be created, or index beans without a repository bean fail the
 * start. The Spring and Quarkus integrations declare at the same point of their startup.
 */
@Singleton
public class ProjectionIndexDeclarer implements ApplicationEventListener<StartupEvent>, Ordered {

  private final BeanProvider<ProjectionRepository> repositories;
  private final List<ProjectionIndex> projectionIndexes;

  /**
   * Creates the declarer.
   *
   * @param repositories the projection repository bean, resolved only when index beans exist
   * @param projectionIndexes the index beans
   */
  public ProjectionIndexDeclarer(
      BeanProvider<ProjectionRepository> repositories, List<ProjectionIndex> projectionIndexes) {
    this.repositories = repositories;
    this.projectionIndexes = List.copyOf(projectionIndexes);
  }

  // The validators' tier (StreamRuneConfigValidator and the others), strictly below
  // StreamRuneLifecycle (1100): Ordered, a LOWER value runs first.
  @Override
  public int getOrder() {
    return 1000;
  }

  /**
   * Declares the index beans.
   *
   * @param event the Micronaut context startup event
   * @throws IllegalStateException if index beans exist and no repository bean does
   */
  @Override
  public void onApplicationEvent(StartupEvent event) {
    if (projectionIndexes.isEmpty()) {
      return;
    }
    if (!repositories.isPresent()) {
      throw new IllegalStateException(
          projectionIndexes.size()
              + " ProjectionIndex bean(s) but no ProjectionRepository bean to declare them on:"
              + " define one, or a DataSource for the module's JdbcProjectionRepository");
    }
    ProjectionRepository repository = repositories.get();
    projectionIndexes.forEach(repository::declareIndex);
  }
}
