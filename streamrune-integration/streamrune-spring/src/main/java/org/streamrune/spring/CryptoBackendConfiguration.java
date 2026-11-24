package org.streamrune.spring;

import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.streamrune.crypto.CryptoBackend;
import org.streamrune.crypto.postgres.PostgresCryptoBackend;
import org.streamrune.integration.BackendSelection;
import org.streamrune.integration.BackendStartup;

/**
 * The crypto half of the storage backend, {@value #CRYPTO_BACKEND}: the {@link CryptoBackend} the
 * crypto engines take their forgotten-subject store, forget signal and key-store engine from.
 * Imported by each of the five crypto engine configurations, so it exists only when a crypto engine
 * is enabled, and once however many are.
 *
 * <p>It follows {@code streamrune.backend}, read with the relaxed binding {@link
 * StreamRuneProperties} is bound with: {@link PostgresCryptoBackend} for {@code postgres}, and for
 * {@code sqlite} {@code org.streamrune.crypto.sqlite.SqliteCryptoBackend}, loaded by name through
 * {@link BackendSelection#resolveCrypto}. An unknown value fails the start with {@link
 * BackendSelection}'s message; {@code sqlite} without the module {@code
 * org.streamrune:streamrune-sqlite-crypto} fails naming that module.
 *
 * <p>The key-store engine must be the selected backend's: {@code
 * streamrune.crypto.postgres.enabled=true} with {@code streamrune.backend=sqlite}, or {@code
 * streamrune.crypto.sqlite.enabled=true} with {@code postgres}, fails the start here.
 */
@Configuration(proxyBeanMethods = false)
class CryptoBackendConfiguration {

  /** The bean name of the crypto backend, {@value}. */
  static final String CRYPTO_BACKEND = "streamRuneCryptoBackend";

  /**
   * The crypto backend of the backend {@code streamrune.backend} names.
   *
   * @param environment the environment {@code streamrune.backend} and the key-store switches are
   *     read from
   * @return the crypto backend
   * @throws IllegalStateException if the property names no backend, the key-store engine of another
   *     backend is enabled, or the selected crypto backend cannot be loaded
   */
  @Bean(CRYPTO_BACKEND)
  CryptoBackend streamRuneCryptoBackend(Environment environment) {
    Binder binder = Binder.get(environment);
    String backend =
        BackendSelection.parse(binder.bind(BackendSelection.PROPERTY, String.class).orElse(null));
    BackendStartup.requireKeyStoreEngineOfBackend(
        backend,
        binder.bind(BackendStartup.POSTGRES_KEY_STORE_PROPERTY, Boolean.class).orElse(false),
        binder.bind(BackendStartup.SQLITE_KEY_STORE_PROPERTY, Boolean.class).orElse(false));
    return BackendSelection.resolveCrypto(
        backend, CryptoBackend.class, CryptoBackend::name, PostgresCryptoBackend::new);
  }
}
