package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.kms.KmsClient;

/**
 * The crypto producers must not register beans of ubiquitous types an application is likely to
 * produce itself.
 *
 * <p>Boots a <strong>real Arc container</strong> (the genuine build-time {@code BeanProcessor}, see
 * {@link RealArcTestContainer}) over the real {@link StreamRuneCryptoProducers} backend producers
 * plus an application that produces its own {@link HttpClient} and {@link KmsClient}. When the
 * framework produced those exact types, Arc's build-time validation failed the whole deployment
 * with an {@code AmbiguousResolutionException} — the Quarkus flavour of the Spring {@code
 * NoUniqueBeanDefinitionException}, and a build failure rather than a startup failure.
 */
class CryptoClientBeanCollisionTest {

  @Test
  void cryptoProducersBootAlongsideApplicationOwnedHttpAndKmsClients() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneCryptoProducers.VaultCryptoProducer.class,
                StreamRuneCryptoProducers.AwsKmsCryptoProducer.class,
                CryptoInfrastructure.class,
                ApplicationClients.class))) {

      // The application's own beans stay plainly resolvable — the framework never competes for
      // these types.
      var httpClient = arc.container().instance(HttpClient.class);
      assertTrue(httpClient.isAvailable(), "the application's own HttpClient must still resolve");
      assertSame(ApplicationClients.APP_HTTP_CLIENT, httpClient.get());

      var kmsClient = arc.container().instance(KmsClient.class);
      assertTrue(kmsClient.isAvailable(), "the application's own KmsClient must still resolve");
      assertSame(ApplicationClients.APP_KMS_CLIENT, kmsClient.get());
    }
  }

  @Test
  void frameworkClientsAreOwnBeansOfStreamRuneOwnedTypes() throws Exception {
    // The framework must use its OWN configured client, never the application's — routing Vault
    // traffic (bearer token included) over an app client with unknown proxy/TLS/timeout settings is
    // exactly what @DefaultBean-style "let the app override it" would have done.
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneCryptoProducers.VaultCryptoProducer.class,
                StreamRuneCryptoProducers.AwsKmsCryptoProducer.class,
                CryptoInfrastructure.class,
                ApplicationClients.class))) {
      var managed =
          arc.container().instance(StreamRuneCryptoProducers.StreamRuneVaultHttpClient.class);
      assertTrue(managed.isAvailable(), "the framework's own namespaced client bean must resolve");
      assertNotSame(ApplicationClients.APP_HTTP_CLIENT, managed.get().client());
    }
  }

  @Test
  void cryptoProducersBootWithoutAnyApplicationClients() throws Exception {
    // Guards the other direction: namespacing must not make the framework's own wiring unsatisfied.
    assertDoesNotThrow(
        () -> {
          try (var arc =
              RealArcTestContainer.boot(
                  List.of(
                      StreamRuneCryptoProducers.VaultCryptoProducer.class,
                      StreamRuneCryptoProducers.AwsKmsCryptoProducer.class,
                      CryptoInfrastructure.class))) {
            assertTrue(
                arc.container()
                    .instance(StreamRuneCryptoProducers.StreamRuneVaultHttpClient.class)
                    .isAvailable());
            assertTrue(
                arc.container()
                    .instance(StreamRuneCryptoProducers.StreamRuneKmsClient.class)
                    .isAvailable());
          }
        });
  }

  /**
   * The two beans a real Quarkus application supplies that raw Arc cannot: the Agroal {@code
   * DataSource} and the bound {@code @ConfigMapping} properties.
   */
  @ApplicationScoped
  public static class CryptoInfrastructure {

    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusCryptoProperties properties() {
      SmallRyeConfig config =
          new SmallRyeConfigBuilder()
              .withMapping(StreamRuneQuarkusCryptoProperties.class)
              .withConverter(
                  Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
              // The KMS client producer validates the region up front (fail-fast), so a deployment
              // that resolves that bean needs one bound.
              .withDefaultValues(Map.of("streamrune.crypto.aws.region", "eu-central-1"))
              .build();
      return config.getConfigMapping(StreamRuneQuarkusCryptoProperties.class);
    }
  }

  /** An ordinary application: an outbound HTTP client and its own KMS client. */
  @ApplicationScoped
  public static class ApplicationClients {

    static final HttpClient APP_HTTP_CLIENT = HttpClient.newHttpClient();
    static final KmsClient APP_KMS_CLIENT = mock(KmsClient.class);

    @Produces
    @Singleton
    public HttpClient httpClient() {
      return APP_HTTP_CLIENT;
    }

    @Produces
    @Singleton
    public KmsClient kmsClient() {
      return APP_KMS_CLIENT;
    }
  }
}
