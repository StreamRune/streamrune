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

/**
 * Declares every {@link ProjectionIndex} bean on the application's {@link ProjectionRepository}
 * bean — the framework's {@code JdbcProjectionRepository} or one the application produces — on the
 * startup event, at {@link Interceptor.Priority#LIBRARY_BEFORE} with the startup validators, so
 * before any projection runner starts and before the first request. A refused key, an index that
 * cannot be created, or index beans without a repository bean fail the start. The Spring and
 * Micronaut integrations declare at the same point of their startup.
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
   * @param repositories the projection repository bean, resolved only when index beans exist
   * @param projectionIndexes the index beans
   * @throws IllegalStateException if index beans exist and no single repository bean does
   */
  public void declare(
      @Observes @Priority(Interceptor.Priority.LIBRARY_BEFORE) StartupEvent event,
      Instance<ProjectionRepository> repositories,
      Instance<ProjectionIndex> projectionIndexes) {
    List<ProjectionIndex> indexes = projectionIndexes.stream().toList();
    if (indexes.isEmpty()) {
      return;
    }
    if (!repositories.isResolvable()) {
      throw new IllegalStateException(
          indexes.size()
              + " ProjectionIndex bean(s) but "
              + (repositories.isAmbiguous() ? "several" : "no")
              + " ProjectionRepository bean(s) to declare them on: define exactly one, or a"
              + " DataSource for the framework's JdbcProjectionRepository");
    }
    ProjectionRepository repository = repositories.get();
    indexes.forEach(repository::declareIndex);
  }
}
