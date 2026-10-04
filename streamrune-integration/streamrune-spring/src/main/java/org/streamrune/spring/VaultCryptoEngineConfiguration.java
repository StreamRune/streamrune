package org.streamrune.spring;

import java.net.http.HttpClient;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.postgres.JdbcForgottenSubjectStore;
import org.streamrune.vault.VaultCryptoEngine;

@Configuration
@ConditionalOnClass(CryptoEngine.class)
@ConditionalOnProperty(prefix = "streamrune.crypto.vault", name = "enabled", havingValue = "true")
public class VaultCryptoEngineConfiguration {

  /**
   * The framework's own Vault HTTP client, wrapped in a StreamRune-owned holder type.
   *
   * <p>This client is a bean so the container closes it on shutdown instead of leaking a selector
   * thread and connection pool. Registering it as a plain {@code @Bean java.net.http.HttpClient}
   * made every application that owns an {@code HttpClient} bean — an utterly ordinary thing for an
   * outbound REST client — fail context refresh with a {@code NoUniqueBeanDefinitionException}
   * naming {@code java.net.http.HttpClient} and nothing pointing at StreamRune, the moment the
   * operator set {@code streamrune.crypto.vault.enabled=true}; every application injection point of
   * that type broke with it.
   *
   * <p>Holding the client inside a type only StreamRune declares keeps the shutdown-close property
   * while removing the framework from by-type resolution of the ubiquitous type entirely: no {@code
   * HttpClient} bean is registered at all, so nothing can collide and no application bean can be
   * selected here by accident. {@code @ConditionalOnMissingBean} would be the wrong fix in the
   * other direction — it would silently route Vault traffic, bearer token included, over an
   * arbitrary application client with unknown proxy/TLS/timeout settings.
   *
   * <p>The holder is {@link AutoCloseable}, so Spring's inferred destroy method closes the client
   * (inference, never an explicit {@code destroyMethod}).
   */
  @Bean
  VaultHttpClient streamRuneVaultHttpClient() {
    return new VaultHttpClient(HttpClient.newHttpClient());
  }

  /**
   * StreamRune-owned holder for the Vault {@link HttpClient}. Package-private: it is wiring, not
   * API.
   */
  record VaultHttpClient(HttpClient client) implements AutoCloseable {
    @Override
    public void close() {
      client.close();
    }
  }

  /**
   * Builds the Vault-backed engine. Vault erasure is terminal only if a persistent {@link
   * org.streamrune.crypto.ForgottenSubjectStore} is wired (Vault's transit engine re-creates a
   * deleted key on the next encrypt). When a {@link DataSource} is present, a durable {@link
   * JdbcForgottenSubjectStore} is auto-wired so a forgotten subject can never be re-encrypted; when
   * no DataSource is available, {@code build()} fails closed (no silent non-terminal erasure).
   * Requires the crypto Flyway migration ({@code forgotten_subjects} table) on that DataSource's
   * database.
   */
  // Inferred destroy method (not explicit destroyMethod = "close"). Cache enabled → the bean
  // is an AutoCloseable CachedCryptoEngine and is still closed on shutdown; cache.enabled=false →
  // the bare VaultCryptoEngine has no close() and inference skips it (an explicit
  // destroyMethod = "close" would fail bean creation with "Invalid destruction signature").
  @Bean
  CryptoEngine vaultCryptoEngine(
      VaultHttpClient vaultHttpClient,
      @Nullable DataSource dataSource,
      StreamRuneCryptoProperties props) {
    String token = props.vault().token();
    if (token == null || token.isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.vault.token must be set when streamrune.crypto.vault.enabled is true");
    }
    var vaultProps = props.vault();
    var vaultBuilder =
        VaultCryptoEngine.builder()
            .httpClient(vaultHttpClient.client())
            .vaultAddress(vaultProps.address())
            .token(token)
            .engineMount(vaultProps.engineMount());
    if (dataSource != null) {
      vaultBuilder.forgottenSubjectStore(new JdbcForgottenSubjectStore(dataSource));
    }
    // No DataSource → no durable tombstone store → build() fails closed
    // rather than silently shipping non-terminal Vault erasure.
    var delegate = vaultBuilder.build();

    var cacheProps = props.cache();
    // Honor streamrune.crypto.cache.enabled=false (parity with Quarkus) — return the bare
    // engine so no decrypted PII is retained in heap (the deliberate no-cache GDPR posture).
    if (!cacheProps.enabled()) {
      return delegate;
    }
    // Wire the LISTEN/NOTIFY forget channel when a DataSource is present so a
    // deleteKey on any replica promptly evicts the forgotten subject's cached plaintext on peers;
    // else NOOP + a startup WARN naming the write-TTL ceiling as the only cross-replica bound.
    return CachedCryptoEngine.builder()
        .delegate(delegate)
        .maximumSize(cacheProps.maximumSize())
        .expireAfterWrite(cacheProps.expireAfterWrite())
        .expireAfterAccess(cacheProps.expireAfterAccess())
        .forgetSignal(
            SpringCryptoForgetSignals.resolve(dataSource, cacheProps.expireAfterWrite(), "vault"))
        .build();
  }
}
