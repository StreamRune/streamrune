package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventStore;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The integration's event store fences a saga's forward commands against the rows of the
 * integration's saga store, in a real Arc container over the {@code DataSource} a Quarkus
 * application injects: an Agroal pool behind the client proxy of an application-scoped bean. A
 * {@code SagaRunner} compares the two identities at {@code build()}, so each injection point must
 * hand both stores the same object.
 */
@Testcontainers
class QuarkusSagaForwardFenceWiringTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_fence");

  /** The pool the infrastructure producer hands to the container under test. */
  static volatile AgroalDataSource applicationPool;

  @BeforeAll
  static void openPool() throws Exception {
    var config =
        new AgroalDataSourceConfigurationSupplier()
            .connectionPoolConfiguration(
                pool ->
                    pool.maxSize(4)
                        .acquisitionTimeout(Duration.ofSeconds(5))
                        .connectionFactoryConfiguration(
                            factory ->
                                factory
                                    .jdbcUrl(PG.getJdbcUrl())
                                    .autoCommit(true)
                                    .principal(new NamePrincipal(PG.getUsername()))
                                    .credential(new SimplePassword(PG.getPassword()))));
    applicationPool = AgroalDataSource.from(config.get());
  }

  @AfterAll
  static void closePool() {
    if (applicationPool != null) {
      applicationPool.close();
    }
  }

  /**
   * What a Quarkus application with quarkus-agroal supplies: the datasource as an
   * application-scoped bean (a client proxy is injected wherever a {@code DataSource} is asked
   * for), and the bound configuration.
   */
  @ApplicationScoped
  public static class AgroalInfrastructure {
    @Produces
    @ApplicationScoped
    public AgroalDataSource dataSource() {
      return applicationPool;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return SagaTypeOwnershipContainerWiringTest.properties(Map.of());
    }
  }

  @Test
  void theEventStoreFencesAgainstTheSagaStoresRows() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(List.of(StreamRuneProducers.class, AgroalInfrastructure.class))) {
      EventStore eventStore = arc.container().select(EventStore.class).get();
      SagaStore sagaStore = arc.container().select(SagaStore.class).get();
      CommandBus commandBus = arc.container().select(CommandBus.class).get();

      assertThat(eventStore.sagaFenceStorageIdentity()).isNotNull();
      assertThat(eventStore.sagaFenceStorageIdentity()).isEqualTo(sagaStore.storageIdentity());
      assertThat(commandBus.sagaFenceStorageIdentity()).isEqualTo(sagaStore.storageIdentity());
    }
  }
}
