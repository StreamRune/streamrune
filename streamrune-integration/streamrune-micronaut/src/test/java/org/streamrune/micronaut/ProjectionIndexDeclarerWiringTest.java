package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.Sort;
import org.streamrune.core.projection.ProjectionIndex;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * The container hands every {@link ProjectionIndex} bean to {@link ProjectionIndexDeclarer}, which
 * declares it on the application's own {@link ProjectionRepository} bean at startup, before any
 * request: nothing in the test reads the repository before the assertion.
 */
class ProjectionIndexDeclarerWiringTest {

  static final ProjectionIndex BY_CUSTOMER =
      ProjectionIndex.forFilter(ProjectionName.of("orders"), "customerId", Sort.by("createdAt"));
  static final ProjectionIndex NESTED =
      ProjectionIndex.forSort(ProjectionName.of("orders"), Sort.by("customer.id"));

  @Factory
  @Requires(property = "spec.name", pattern = "index-.*")
  static class Fixtures {
    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore(SimpleEventTypeRegistry.builder().build());
    }

    @Singleton
    @Requires(property = "spec.name", pattern = "index-(declared|refused)")
    ProjectionRepository projectionRepository() {
      return new InMemoryProjectionRepository();
    }

    @Singleton
    @Requires(property = "spec.name", pattern = "index-(declared|orphan)")
    ProjectionIndex byCustomer() {
      return BY_CUSTOMER;
    }

    @Singleton
    @Requires(property = "spec.name", value = "index-refused")
    ProjectionIndex nested() {
      return NESTED;
    }
  }

  private static ApplicationContext run(String spec) {
    return ApplicationContext.run(
        Map.of("spec.name", spec, "streamrune.runner-lifecycle-enabled", "false"));
  }

  @Test
  void anIndexBeanIsDeclaredOnTheApplicationsRepositoryAtStartup() {
    try (var ctx = run("index-declared")) {
      var repository = (InMemoryProjectionRepository) ctx.getBean(ProjectionRepository.class);

      assertEquals(List.of(BY_CUSTOMER), repository.declaredIndexes());
    }
  }

  @Test
  void anIndexBeanTheRepositoryRefusesFailsStartup() {
    var failure = assertThrows(RuntimeException.class, () -> run("index-refused").close());

    assertTrue(causes(failure).contains("no nested path"), causes(failure));
  }

  @Test
  void indexBeansWithoutARepositoryFailStartup() {
    var failure = assertThrows(RuntimeException.class, () -> run("index-orphan").close());

    assertTrue(causes(failure).contains("no ProjectionRepository bean"), causes(failure));
  }

  private static String causes(Throwable failure) {
    var messages = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      messages.append(t.getMessage()).append(" | ");
    }
    return messages.toString();
  }
}
