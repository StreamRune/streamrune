package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.test.util.ReflectionTestUtils;
import org.streamrune.core.EventStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * Proves the auto-configured {@link JdbcProjectionRepository} bean receives a crypto-aware {@code
 * ObjectMapper} (one with {@code CryptoShreddingModule} registered) whenever a {@link CryptoEngine}
 * bean is present — regression coverage for the GDPR defect where the framework built the
 * projection repository via the crypto-blind constructor, silently persisting {@code @Encrypted}
 * read-model fields as plaintext PII outside GDPR forget scope. Mirrors {@link
 * SagaStoreWiringAutoConfigurationTest}.
 */
class ProjectionRepositoryWiringAutoConfigurationTest {

  private static final String CRYPTO_SHREDDING_MODULE_ID = "CryptoShreddingModule";

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class));

  @Test
  void projectionIndexBeansAreDeclaredOnTheRepository() {
    List<String> statements = new CopyOnWriteArrayList<>();
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS)
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> recordingDataSource(statements))
        .withBean(EventStore.class, () -> mock(EventStore.class))
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
              assertThat(statements)
                  .anyMatch(
                      sql ->
                          sql.startsWith(
                                  "CREATE INDEX IF NOT EXISTS orders_customerid_createdat_desc_id_ix_")
                              && sql.endsWith(
                                  " ON orders_view ((data->>'customerId') COLLATE \"C\","
                                      + " (data->>'createdAt') COLLATE \"C\" DESC,"
                                      + " id COLLATE \"C\")"));
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
              var repo = ctx.getBean(JdbcProjectionRepository.class);
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
          var repo = ctx.getBean(JdbcProjectionRepository.class);
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
              var repo = ctx.getBean(JdbcProjectionRepository.class);
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

  /**
   * A DataSource whose connections record every statement run through {@code createStatement()} and
   * answer every query with no rows (so a declared index is found missing and created).
   */
  private static DataSource recordingDataSource(List<String> statements) {
    try {
      var dataSource = mock(DataSource.class);
      var connection = mock(java.sql.Connection.class);
      var statement = mock(java.sql.Statement.class);
      var prepared = mock(java.sql.PreparedStatement.class);
      when(dataSource.getConnection()).thenReturn(connection);
      when(connection.getAutoCommit()).thenReturn(true);
      when(connection.createStatement()).thenReturn(statement);
      when(statement.execute(org.mockito.ArgumentMatchers.anyString()))
          .thenAnswer(
              invocation -> {
                statements.add(invocation.getArgument(0));
                return false;
              });
      when(connection.prepareStatement(org.mockito.ArgumentMatchers.anyString()))
          .thenReturn(prepared);
      when(prepared.executeQuery()).thenReturn(mock(java.sql.ResultSet.class));
      return dataSource;
    } catch (java.sql.SQLException e) {
      throw new IllegalStateException(e);
    }
  }
}
