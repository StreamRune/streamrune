package org.streamrune.micronaut;

import io.micronaut.context.BeanProvider;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import io.micronaut.core.order.Ordered;
import jakarta.inject.Singleton;
import java.util.List;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.integration.ProjectionRoutes;

/**
 * Declares every {@link ProjectionIndex} bean on the store its projection is routed to — the routes
 * the projection runners use ({@link ProjectionRoutes}, computed once by {@code
 * ProjectionFactory#projectionRoutes}) — on the context's {@link StartupEvent}, in the startup
 * validators' tier, below {@link StreamRuneLifecycle}, so before any projection runner starts and
 * before the first request. An index whose projection is routed to no store (the subscription
 * checkpoint, an INLINE projection, no registration, or projection discovery off) goes to the
 * application's {@link ProjectionRepository} bean — the repository of the store named {@code
 * postgres} ({@code StreamRuneMicronautModule#projectionRepository}) or one the application
 * defines. A refused key, an index that cannot be created, or such an index without a repository
 * bean fail the start. The Spring and Quarkus integrations declare at the same point of their
 * startup.
 */
@Singleton
public class ProjectionIndexDeclarer implements ApplicationEventListener<StartupEvent>, Ordered {

  private final BeanProvider<ProjectionRepository> repositories;
  private final List<ProjectionIndex> projectionIndexes;
  private final BeanProvider<ProjectionRoutes> routes;

  /**
   * Creates the declarer.
   *
   * @param repositories the projection repository bean, resolved only for an index routed to no
   *     store
   * @param projectionIndexes the index beans
   * @param routes the routes of the projection runners' registrations, resolved only when index
   *     beans exist; absent when projection discovery is off
   */
  public ProjectionIndexDeclarer(
      BeanProvider<ProjectionRepository> repositories,
      List<ProjectionIndex> projectionIndexes,
      BeanProvider<ProjectionRoutes> routes) {
    this.repositories = repositories;
    this.projectionIndexes = List.copyOf(projectionIndexes);
    this.routes = routes;
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
   * @throws IllegalStateException if an index is routed to no store and no repository bean exists,
   *     or the routes cannot be computed
   */
  @Override
  public void onApplicationEvent(StartupEvent event) {
    if (projectionIndexes.isEmpty()) {
      return;
    }
    ProjectionRoutes routed = routes.isPresent() ? routes.get() : ProjectionRoutes.none();
    for (ProjectionIndex index : projectionIndexes) {
      routed
          .indexRepository(
              index,
              () -> {
                if (!repositories.isPresent()) {
                  throw new IllegalStateException(
                      "ProjectionIndex bean for projection '"
                          + LogSanitizer.sanitizeForLog(index.projection().value())
                          + "', routed to no read-model store, but no ProjectionRepository bean"
                          + " to declare it on: define one, or a DataSource for the default"
                          + " read-model store");
                }
                return repositories.get();
              })
          .declareIndex(index);
    }
  }
}
