package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class));

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
}
