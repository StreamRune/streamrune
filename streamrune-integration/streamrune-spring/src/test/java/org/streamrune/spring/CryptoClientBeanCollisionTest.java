package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.net.http.HttpClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.crypto.CryptoEngine;
import software.amazon.awssdk.services.kms.KmsClient;

/**
 * Enabling a Vault/AWS crypto backend must not break an application that owns a bean of one of the
 * ubiquitous helper types those backends need ({@link HttpClient}, {@link KmsClient}).
 *
 * <p>The framework used to register those clients as plain, unqualified beans of exactly those
 * types, so an app with its own {@code HttpClient} bean got a {@code
 * NoUniqueBeanDefinitionException} naming {@code java.net.http.HttpClient} — with nothing pointing
 * at StreamRune — the moment it set the documented one-line enable property. Every application
 * injection point of that type broke too.
 */
class CryptoClientBeanCollisionTest {

  private static final HttpClient APP_HTTP_CLIENT = HttpClient.newHttpClient();
  private static final KmsClient APP_KMS_CLIENT = mock(KmsClient.class);

  @Test
  void vaultBackend_bootsAlongsideAnApplicationOwnedHttpClientBean() {
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                StreamRuneAutoConfiguration.class,
                CryptoPropertiesConfiguration.class,
                VaultCryptoEngineConfiguration.class))
        .withUserConfiguration(
            StubEventStoreConfig.class, StubDataSourceConfig.class, AppHttpClientConfig.class)
        .withPropertyValues(
            "streamrune.crypto.vault.enabled=true",
            "streamrune.crypto.vault.token=s.test",
            "streamrune.crypto.vault.address=http://localhost:8200")
        .run(
            ctx -> {
              assertThat(ctx)
                  .as("an app-owned HttpClient bean must not break the Vault crypto wiring")
                  .hasNotFailed();
              assertThat(ctx).hasSingleBean(CryptoEngine.class);
              // The app's own bean stays plainly resolvable by type — the framework's client must
              // not participate in that resolution at all.
              assertThat(ctx.getBean(HttpClient.class)).isSameAs(APP_HTTP_CLIENT);
            });
  }

  @Test
  void awsBackend_bootsAlongsideAnApplicationOwnedKmsClientBean() {
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                StreamRuneAutoConfiguration.class,
                CryptoPropertiesConfiguration.class,
                AwsKmsCryptoEngineConfiguration.class))
        .withUserConfiguration(
            StubEventStoreConfig.class, StubDataSourceConfig.class, AppKmsClientConfig.class)
        .withPropertyValues(
            "streamrune.crypto.aws.enabled=true",
            "streamrune.crypto.aws.region=eu-central-1",
            "streamrune.crypto.aws.kms-key-id=alias/test")
        .run(
            ctx -> {
              assertThat(ctx)
                  .as("an app-owned KmsClient bean must not break the AWS-KMS crypto wiring")
                  .hasNotFailed();
              assertThat(ctx).hasSingleBean(CryptoEngine.class);
              assertThat(ctx.getBean(KmsClient.class)).isSameAs(APP_KMS_CLIENT);
            });
  }

  @Test
  void vaultEngineUsesTheFrameworkClient_notTheApplicationOne() {
    // The framework must never route Vault traffic (bearer token included) over an arbitrary
    // application HttpClient with unknown proxy/TLS/timeout settings — which is why the fix is
    // namespacing, not @ConditionalOnMissingBean.
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                StreamRuneAutoConfiguration.class,
                CryptoPropertiesConfiguration.class,
                VaultCryptoEngineConfiguration.class))
        .withUserConfiguration(
            StubEventStoreConfig.class, StubDataSourceConfig.class, AppHttpClientConfig.class)
        .withPropertyValues(
            "streamrune.crypto.vault.enabled=true",
            "streamrune.crypto.vault.token=s.test",
            "streamrune.crypto.vault.address=http://localhost:8200")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var managed = ctx.getBean(VaultCryptoEngineConfiguration.VaultHttpClient.class);
              assertThat(managed.client())
                  .as("the engine's client is the framework's own, never the application's")
                  .isNotSameAs(APP_HTTP_CLIENT);
            });
  }

  @Test
  void frameworkClientsAreStillClosedOnShutdown() {
    // Container-managed closing must survive the holder namespacing: the holder is AutoCloseable,
    // so Spring's
    // inferred destroy method still closes the underlying client when the context shuts down.
    var closed = new java.util.concurrent.atomic.AtomicReference<HttpClient>();
    new ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                StreamRuneAutoConfiguration.class,
                CryptoPropertiesConfiguration.class,
                VaultCryptoEngineConfiguration.class))
        .withUserConfiguration(StubEventStoreConfig.class, StubDataSourceConfig.class)
        .withPropertyValues(
            "streamrune.crypto.vault.enabled=true",
            "streamrune.crypto.vault.token=s.test",
            "streamrune.crypto.vault.address=http://localhost:8200")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              closed.set(
                  ctx.getBean(VaultCryptoEngineConfiguration.VaultHttpClient.class).client());
              assertThat(closed.get().isTerminated()).isFalse();
            });
    // The runner closes the context when run() returns.
    assertThat(closed.get().isTerminated())
        .as("the container must still close the framework-managed HttpClient")
        .isTrue();
  }

  @Configuration
  static class StubEventStoreConfig {
    @Bean
    EventStoreFactory eventStoreFactory() {
      return () -> mock(EventStore.class);
    }
  }

  @Configuration
  static class StubDataSourceConfig {
    @Bean
    javax.sql.DataSource dataSource() {
      return mock(javax.sql.DataSource.class);
    }
  }

  /** A perfectly ordinary application bean — an outbound REST client. */
  @Configuration
  static class AppHttpClientConfig {
    @Bean
    HttpClient httpClient() {
      return APP_HTTP_CLIENT;
    }
  }

  /** An app that already uses AWS KMS for its own purposes. */
  @Configuration
  static class AppKmsClientConfig {
    @Bean
    KmsClient kmsClient() {
      return APP_KMS_CLIENT;
    }
  }
}
