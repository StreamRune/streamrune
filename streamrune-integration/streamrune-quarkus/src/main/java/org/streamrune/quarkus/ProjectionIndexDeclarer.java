package org.streamrune.quarkus;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.interceptor.Interceptor;
import java.util.List;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.integration.ProjectionRoutes;

/**
 * Declares every {@link ProjectionIndex} bean on the store its projection is routed to — the routes
 * the projection runners use ({@link ProjectionRoutes}, computed once by {@code
 * ProjectionProducer#projectionRoutes}) — on the startup event, at {@link
 * Interceptor.Priority#LIBRARY_BEFORE} with the startup validators, so before any projection runner
 * starts and before the first request. An index whose projection is routed to no store (the
 * subscription checkpoint, an INLINE projection, no registration, or projection discovery off) goes
 * to the application's {@link ProjectionRepository} bean — the repository of the store named {@code
 * postgres} ({@code StreamRuneProducers#projectionRepository}) or one the application produces. A
 * refused key, an index that cannot be created, or such an index without a single repository bean
 * fail the start. The Spring and Micronaut integrations declare at the same point of their startup.
 */
@ApplicationScoped
public class ProjectionIndexDeclarer {

  /** For CDI. */
  public ProjectionIndexDeclarer() {}

  /**
   * Declares the index beans. {@code @Priority} sits on the event parameter (Arc ignores a
   * method-level one for observer ordering); {@code public} for the cross-classloader reason given
   * on {@link StreamRuneConfigValidator#validate}.
   *
   * @param event the Quarkus startup event
   * @param repositories the projection repository bean, resolved only for an index routed to no
   *     store
   * @param projectionIndexes the index beans
   * @param routes the routes of the projection runners' registrations, resolved only when index
   *     beans exist; unsatisfied when the application has no routes bean
   * @throws IllegalStateException if an index is routed to no store and no single repository bean
   *     exists, or the routes cannot be computed
   * @hidden
   */
  @org.streamrune.core.Internal
  public void declare(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent event,
      Instance<ProjectionRepository> repositories,
      Instance<ProjectionIndex> projectionIndexes,
      Instance<ProjectionRoutes> routes) {
    List<ProjectionIndex> indexes = projectionIndexes.stream().toList();
    if (indexes.isEmpty()) {
      return;
    }
    ProjectionRoutes routed = routes.isResolvable() ? routes.get() : ProjectionRoutes.none();
    for (ProjectionIndex index : indexes) {
      routed
          .indexRepository(
              index,
              () -> {
                if (!repositories.isResolvable()) {
                  throw new IllegalStateException(
                      "ProjectionIndex bean for projection '"
                          + LogSanitizer.sanitizeForLog(index.projection().value())
                          + "', routed to no read-model store, but "
                          + (repositories.isAmbiguous() ? "several" : "no")
                          + " ProjectionRepository bean(s) to declare it on: define exactly one,"
                          + " or a DataSource for the default read-model store");
                }
                return repositories.get();
              })
          .declareIndex(index);
    }
  }
}
