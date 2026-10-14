package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The integration's event store fences a saga's forward commands against the rows of the
 * integration's saga store: both are built over the application's one {@code DataSource} bean, so
 * the identities a {@code SagaRunner} compares at {@code build()} are equal, and the command bus
 * reports the same one.
 */
@Testcontainers
class MicronautSagaForwardFenceWiringTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_fence");

  static volatile PGSimpleDataSource applicationDataSource;

  @BeforeAll
  static void dataSource() {
    var dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    applicationDataSource = dataSource;
  }

  /** What an application supplies: the {@code DataSource}. Every store is the integration's. */
  @Factory
  @Requires(property = "spec.name", value = "saga-forward-fence-wiring")
  static class Infrastructure {
    @Singleton
    DataSource dataSource() {
      return applicationDataSource;
    }
  }

  @Test
  void theEventStoreFencesAgainstTheSagaStoresRows() {
    try (var ctx =
        ApplicationContext.run(
            Map.of(
                "spec.name",
                "saga-forward-fence-wiring",
                "streamrune.saga.compensation-retry-enabled",
                "false"))) {
      EventStore eventStore = ctx.getBean(EventStore.class);
      SagaStore sagaStore = ctx.getBean(SagaStore.class);

      assertThat(eventStore.sagaFenceStorageIdentity()).isNotNull();
      assertThat(eventStore.sagaFenceStorageIdentity()).isEqualTo(sagaStore.storageIdentity());
      assertThat(ctx.getBean(CommandBus.class).sagaFenceStorageIdentity())
          .isEqualTo(sagaStore.storageIdentity());
    }
  }
}
