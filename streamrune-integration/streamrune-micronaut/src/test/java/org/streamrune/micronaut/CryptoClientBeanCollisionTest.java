package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.net.http.HttpClient;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import software.amazon.awssdk.services.kms.KmsClient;

/**
 * The crypto factory must not register beans of ubiquitous types an application is likely to
 * declare itself.
 *
 * <p>The factory used to declare plain {@code @Singleton HttpClient} / {@code @Singleton KmsClient}
 * beans, so an application that owns either — an outbound REST client, or its own KMS usage — got a
 * {@code NonUniqueBeanException} at boot the moment the operator enabled the documented crypto
 * property, both for the framework's own engine wiring and for every application injection point of
 * those types.
 */
class CryptoClientBeanCollisionTest {

  private static final Map<String, Object> VAULT_ENABLED =
      Map.of(
          "spec.name", "crypto-client-collision",
          "streamrune.crypto.vault.enabled", "true",
          "streamrune.crypto.vault.address", "http://localhost:8200",
          "streamrune.crypto.vault.token", "s.test");

  private static final Map<String, Object> AWS_ENABLED =
      Map.of(
          "spec.name", "crypto-client-collision",
          "streamrune.crypto.aws.enabled", "true",
          "streamrune.crypto.aws.region", "eu-central-1",
          "streamrune.crypto.aws.kms-key-id", "alias/test");

  @Test
  void applicationHttpClientStaysResolvable_whenVaultBackendIsEnabled() {
    try (var ctx = ApplicationContext.run(VAULT_ENABLED)) {
      assertSame(
          ApplicationClientFixture.APP_HTTP_CLIENT,
          ctx.getBean(HttpClient.class),
          "an app-owned HttpClient must stay plainly resolvable with the Vault backend enabled");
    }
  }

  @Test
  void applicationKmsClientStaysResolvable_whenAwsBackendIsEnabled() {
    try (var ctx = ApplicationContext.run(AWS_ENABLED)) {
      assertSame(
          ApplicationClientFixture.APP_KMS_CLIENT,
          ctx.getBean(KmsClient.class),
          "an app-owned KmsClient must stay plainly resolvable with the AWS backend enabled");
    }
  }

  @Test
  void vaultEngineBuilds_andUsesTheFrameworkClientNotTheApplicationOne() {
    // The framework must use its OWN configured client: routing Vault traffic (bearer token
    // included) over an application client with unknown proxy/TLS/timeout settings is exactly what
    // an "app bean wins" fix (@Secondary / @ConditionalOnMissingBean) would have caused.
    try (var ctx = ApplicationContext.run(VAULT_ENABLED)) {
      assertNotNull(
          ctx.getBean(org.streamrune.core.crypto.CryptoEngine.class),
          "the Vault engine must still build with an app-owned HttpClient bean present");
      var managed = ctx.getBean(StreamRuneCryptoFactory.StreamRuneVaultHttpClient.class);
      assertNotSame(
          ApplicationClientFixture.APP_HTTP_CLIENT,
          managed.client(),
          "the engine's client is the framework's own, never the application's");
    }
  }

  @Test
  void holderCloseClosesTheWrappedClient() {
    // The holder types exist ONLY so the container can still close the
    // underlying client without registering a bean of the ubiquitous type. Nothing pinned
    // that close-through here — Spring asserts HttpClient#isTerminated and Quarkus verifies
    // client.close(), but on Micronaut a holder whose close() did nothing would have passed every
    // existing assertion while silently leaking a selector thread + connection pool per shutdown.
    var httpClient = mock(HttpClient.class);
    new StreamRuneCryptoFactory.StreamRuneVaultHttpClient(httpClient).close();
    verify(httpClient).close();

    var kmsClient = mock(KmsClient.class);
    new StreamRuneCryptoFactory.StreamRuneKmsClient(kmsClient).close();
    verify(kmsClient).close();
  }

  @Test
  void containerShutdownActuallyClosesTheFrameworkVaultClient() {
    // End-to-end companion to the unit assertion above: @Bean(preDestroy = "close") must really
    // fire on context shutdown (Micronaut does NOT auto-close @Factory-produced AutoCloseable
    // beans), so the close-through is exercised through the container, not just called directly.
    HttpClient managed;
    try (var ctx = ApplicationContext.run(VAULT_ENABLED)) {
      managed = ctx.getBean(StreamRuneCryptoFactory.StreamRuneVaultHttpClient.class).client();
      assertFalse(managed.isTerminated(), "the framework client is live while the context is up");
    }
    assertTrue(
        managed.isTerminated(),
        "the container must close the framework-managed HttpClient on shutdown");
  }

  /** An ordinary application: an outbound HTTP client, its own KMS client, and a DataSource. */
  @Factory
  @Requires(property = "spec.name", value = "crypto-client-collision")
  static class ApplicationClientFixture {

    static final HttpClient APP_HTTP_CLIENT = HttpClient.newHttpClient();
    static final KmsClient APP_KMS_CLIENT = mock(KmsClient.class);

    @Singleton
    HttpClient httpClient() {
      return APP_HTTP_CLIENT;
    }

    @Singleton
    KmsClient kmsClient() {
      return APP_KMS_CLIENT;
    }

    @Singleton
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    // The startup validators resolve the command bus; an EventStore of its own lets the bus be
    // built over the mock DataSource, which the framework's event store would query for its schema.
    @Singleton
    EventStore eventStore() {
      return mock(EventStore.class);
    }
  }
}
