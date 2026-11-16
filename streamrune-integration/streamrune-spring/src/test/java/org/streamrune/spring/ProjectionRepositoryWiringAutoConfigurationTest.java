package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.util.ReflectionTestUtils;
import org.streamrune.core.EventStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.core.projection.ReadModelStore;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Proves the default read-model store's repository — the auto-configured {@code
 * ProjectionRepository} bean — receives a crypto-aware {@code ObjectMapper} (one with {@code
 * CryptoShreddingModule} registered) whenever a {@link CryptoEngine} bean is present — regression
 * coverage for the GDPR defect where the framework built the projection repository via the
 * crypto-blind constructor, silently persisting {@code @Encrypted} read-model fields as plaintext
 * PII outside GDPR forget scope. Mirrors {@link SagaStoreWiringAutoConfigurationTest}.
 *
 * <p>Creating the default store reads the server version and migrates the read-model schema, so the
 * contexts run on a real PostgreSQL.
 */
@Testcontainers
class ProjectionRepositoryWiringAutoConfigurationTest {

  private static final String CRYPTO_SHREDDING_MODULE_ID = "CryptoShreddingModule";

  @Container
  static final PostgreSQLContainer PG =
      new PostgreSQLContainer(PostgresTestImage.NAME).withDatabaseName("streamrune_repo_wiring");

  private static DataSource dataSource() {
    var dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    return dataSource;
  }

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, ProjectionRepositoryWiringAutoConfigurationTest::dataSource)
          .withBean(EventStore.class, () -> mock(EventStore.class));

  @Test
  void projectionIndexBeansAreDeclaredOnTheRepository() {
    runner
        .withBean(
            org.streamrune.core.projection.ProjectionIndex.class,
            () ->
                org.streamrune.core.projection.ProjectionIndex.forFilter(
                    org.streamrune.core.types.ProjectionName.of("orders"),
                    "customerId",
                    org.streamrune.core.Sort.by(
                        "createdAt", org.streamrune.core.SortDirection.DESC)))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(indexDefinitions("orders_view"))
                  .anyMatch(
                      definition ->
                          definition.contains("orders_customerid_createdat_desc_id_ix_")
                              && definition.contains("ON public.orders_view"));
            });
  }

  /**
   * The application defines its own repository: the auto-configured one backs off, and the index
   * beans are declared on the application's bean at startup, before anything reads.
   */
  @Test
  void projectionIndexBeansAreDeclaredOnAnApplicationDefinedRepositoryAtStartup() {
    var index =
        org.streamrune.core.projection.ProjectionIndex.forSort(
            org.streamrune.core.types.ProjectionName.of("orders"),
            org.streamrune.core.Sort.by("createdAt"));
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .withBean(
            org.streamrune.core.projection.ProjectionRepository.class,
            org.streamrune.test.InMemoryProjectionRepository::new)
        .withBean(org.streamrune.core.projection.ProjectionIndex.class, () -> index)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var repository =
                  (org.streamrune.test.InMemoryProjectionRepository)
                      ctx.getBean(org.streamrune.core.projection.ProjectionRepository.class);
              assertThat(repository.declaredIndexes()).containsExactly(index);
            });
  }

  @Test
  void projectionIndexBeansWithoutARepositoryFailStartup() {
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .withBean(
            org.streamrune.core.projection.ProjectionIndex.class,
            () ->
                org.streamrune.core.projection.ProjectionIndex.forSort(
                    org.streamrune.core.types.ProjectionName.of("orders"),
                    org.streamrune.core.Sort.by("createdAt")))
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("no ProjectionRepository bean");
            });
  }

  @Test
  void aProjectionIndexBeanTheRepositoryRefusesFailsStartup() {
    runner
        .withBean(
            org.streamrune.core.projection.ProjectionIndex.class,
            () ->
                org.streamrune.core.projection.ProjectionIndex.forSort(
                    org.streamrune.core.types.ProjectionName.of("orders"),
                    org.streamrune.core.Sort.by("customer.id")))
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure()).hasStackTraceContaining("no nested path");
            });
  }

  @Test
  void cryptoEngineBeanIsWiredIntoProjectionRepositoryMapper() {
    runner
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .run(
            ctx -> {
              var repo = ctx.getBean(ProjectionRepository.class);
              assertThat(repo)
                  .isSameAs(ctx.getBean("postgres", ReadModelStore.class).repository())
                  .isInstanceOf(JdbcProjectionRepository.class);
              var mapper =
                  (com.fasterxml.jackson.databind.ObjectMapper)
                      ReflectionTestUtils.getField(repo, "objectMapper");
              assertThat(mapper.getRegisteredModuleIds()).contains(CRYPTO_SHREDDING_MODULE_ID);
            });
  }

  @Test
  void noCryptoEngineBeanLeavesProjectionRepositoryMapperCryptoBlind() {
    runner.run(
        ctx -> {
          var repo = ctx.getBean(ProjectionRepository.class);
          assertThat(repo)
              .isSameAs(ctx.getBean("postgres", ReadModelStore.class).repository())
              .isInstanceOf(JdbcProjectionRepository.class);
          var mapper =
              (com.fasterxml.jackson.databind.ObjectMapper)
                  ReflectionTestUtils.getField(repo, "objectMapper");
          assertThat(mapper.getRegisteredModuleIds()).doesNotContain(CRYPTO_SHREDDING_MODULE_ID);
        });
  }

  @Test
  void ambiguousCryptoEnginesFailStartupInsteadOfPersistingPlaintext() {
    runner
        .withUserConfiguration(TwoCryptoEnginesConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("PLAINTEXT")
                  .hasStackTraceContaining("@Primary");
            });
  }

  @Test
  void primaryCryptoEngineResolvesAmbiguityForProjectionRepository() {
    runner
        .withUserConfiguration(PrimaryCryptoEngineConfig.class)
        .run(
            ctx -> {
              var repo = ctx.getBean(ProjectionRepository.class);
              assertThat(repo)
                  .isSameAs(ctx.getBean("postgres", ReadModelStore.class).repository())
                  .isInstanceOf(JdbcProjectionRepository.class);
              var mapper =
                  (com.fasterxml.jackson.databind.ObjectMapper)
                      ReflectionTestUtils.getField(repo, "objectMapper");
              assertThat(mapper.getRegisteredModuleIds()).contains(CRYPTO_SHREDDING_MODULE_ID);
            });
  }

  @Configuration
  static class TwoCryptoEnginesConfig {
    @Bean
    CryptoEngine engineA() {
      return new InMemoryCryptoEngine();
    }

    @Bean
    CryptoEngine engineB() {
      return new InMemoryCryptoEngine();
    }
  }

  @Configuration
  static class PrimaryCryptoEngineConfig {
    @Bean
    @Primary
    CryptoEngine primaryEngine() {
      return new InMemoryCryptoEngine();
    }

    @Bean
    CryptoEngine secondaryEngine() {
      return new InMemoryCryptoEngine();
    }
  }

  /** The definitions of the indexes on {@code table}, from {@code pg_indexes}. */
  private static List<String> indexDefinitions(String table) throws java.sql.SQLException {
    try (var conn = dataSource().getConnection();
        var ps = conn.prepareStatement("SELECT indexdef FROM pg_indexes WHERE tablename = ?")) {
      ps.setString(1, table);
      try (var rs = ps.executeQuery()) {
        var definitions = new java.util.ArrayList<String>();
        while (rs.next()) {
          definitions.add(rs.getString(1));
        }
        return definitions;
      }
    }
  }
}
