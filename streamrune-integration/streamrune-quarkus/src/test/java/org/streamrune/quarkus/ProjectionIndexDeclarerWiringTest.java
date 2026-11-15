package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Sort;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * Arc hands every {@link ProjectionIndex} bean to {@link ProjectionIndexDeclarer}, which declares
 * it on the application's own {@link ProjectionRepository} bean on the startup event: nothing reads
 * the repository before the assertion.
 */
class ProjectionIndexDeclarerWiringTest {

  public static final ProjectionIndex BY_CUSTOMER =
      ProjectionIndex.forFilter(ProjectionName.of("orders"), "customerId", Sort.by("createdAt"));
  public static final ProjectionIndex NESTED =
      ProjectionIndex.forSort(ProjectionName.of("orders"), Sort.by("customer.id"));

  /** The application's own repository. */
  @ApplicationScoped
  public static class ApplicationRepository {
    @Produces
    @Singleton
    public ProjectionRepository repository() {
      return new InMemoryProjectionRepository();
    }
  }

  /** One index bean the repository accepts. */
  @ApplicationScoped
  public static class AcceptedIndex {
    @Produces
    @Singleton
    public ProjectionIndex byCustomer() {
      return BY_CUSTOMER;
    }
  }

  /** One index bean the repository refuses. */
  @ApplicationScoped
  public static class RefusedIndex {
    @Produces
    @Singleton
    public ProjectionIndex nested() {
      return NESTED;
    }
  }

  private static void start(RealArcTestContainer arc) {
    arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
  }

  @Test
  void anIndexBeanIsDeclaredOnTheApplicationsRepositoryAtStartup() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                ProjectionIndexDeclarer.class,
                ApplicationRepository.class,
                AcceptedIndex.class,
                ProjectionRepository.class,
                ProjectionIndex.class))) {
      start(arc);

      var repository =
          (InMemoryProjectionRepository) arc.container().instance(ProjectionRepository.class).get();
      assertEquals(List.of(BY_CUSTOMER), repository.declaredIndexes());
    }
  }

  @Test
  void anIndexBeanTheRepositoryRefusesFailsStartup() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                ProjectionIndexDeclarer.class,
                ApplicationRepository.class,
                RefusedIndex.class,
                ProjectionRepository.class,
                ProjectionIndex.class))) {
      var failure = assertThrows(RuntimeException.class, () -> start(arc));

      assertTrue(causes(failure).contains("no nested path"), causes(failure));
    }
  }

  @Test
  void indexBeansWithoutARepositoryFailStartup() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                ProjectionIndexDeclarer.class,
                AcceptedIndex.class,
                ProjectionRepository.class,
                ProjectionIndex.class))) {
      var failure = assertThrows(RuntimeException.class, () -> start(arc));

      assertTrue(causes(failure).contains("no ProjectionRepository"), causes(failure));
    }
  }

  private static String causes(Throwable failure) {
    var messages = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append(" | ");
    }
    return messages.toString();
  }
}
