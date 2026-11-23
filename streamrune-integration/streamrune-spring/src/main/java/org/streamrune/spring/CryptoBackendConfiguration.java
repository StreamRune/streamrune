package org.streamrune.spring;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.streamrune.core.backend.StreamRuneBackend;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.postgres.PostgresCryptoBackend;
import org.streamrune.integration.BackendSelection;
import org.streamrune.postgres.PostgresBackend;

/**
 * The crypto half of the storage backend, {@value #CRYPTO_BACKEND}: the {@link CryptoBackend} the
 * crypto engines take their forgotten-subject store, forget signal and key-store engine from.
 * Imported by each of the four crypto engine configurations, so it exists only when a crypto engine
 * is enabled, and once however many are.
 *
 * <p>It follows {@code streamrune.backend}, read with the relaxed binding {@link
 * StreamRuneProperties} is bound with: {@link PostgresCryptoBackend} for {@code postgres}. The
 * selection is resolved first, so an unknown value, or {@code sqlite} without the module {@code
 * org.streamrune:streamrune-sqlite} on the classpath, fails the start with {@link
 * BackendSelection}'s message, as the {@code streamRuneBackend} bean does.
 */
@Configuration(proxyBeanMethods = false)
class CryptoBackendConfiguration {

  /** The bean name of the crypto backend, {@value}. */
  static final String CRYPTO_BACKEND = "streamRuneCryptoBackend";

  /**
   * The crypto backend of the backend {@code streamrune.backend} names.
   *
   * @param environment the environment {@code streamrune.backend} is read from
   * @return the crypto backend
   * @throws IllegalStateException if the property names no backend, the selected backend cannot be
   *     loaded, or it has no crypto backend in this integration
   */
  @Bean(CRYPTO_BACKEND)
  CryptoBackend streamRuneCryptoBackend(Environment environment) {
    String raw = Binder.get(environment).bind(BackendSelection.PROPERTY, String.class).orElse(null);
    StreamRuneBackend backend =
        BackendSelection.resolve(BackendSelection.parse(raw), PostgresBackend::new);
    if (backend instanceof PostgresBackend) {
      return new PostgresCryptoBackend();
    }
    throw new IllegalStateException(
        BackendSelection.PROPERTY
            + "="
            + backend.name()
            + " has no crypto backend in the Spring integration; only postgres has one");
  }
}
