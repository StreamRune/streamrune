package org.streamrune.spring;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.lang.Nullable;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoBackend;

/**
 * The SQLite key-store engine, enabled by {@code streamrune.crypto.sqlite.enabled=true} with {@code
 * streamrune.backend=sqlite}: per-subject keys and tombstones in the application's SQLite database,
 * built by the {@link CryptoBackend} ({@code SqliteCryptoBackend}, of the module {@code
 * org.streamrune:streamrune-sqlite-crypto}). SQLite has no key-table name property: the engine uses
 * its default table. The integration names no SQLite type, so this module needs no dependency on
 * the SQLite modules; the application adds them.
 *
 * <p>With the cache on, the engine is wrapped in a {@link CachedCryptoEngine} with the backend's
 * forget signal, which on SQLite is in-process: one instance has no peer to notify.
 */
@Configuration
@ConditionalOnClass(CryptoEngine.class)
@Import(CryptoBackendConfiguration.class)
@ConditionalOnProperty(prefix = "streamrune.crypto.sqlite", name = "enabled", havingValue = "true")
public class SqliteCryptoEngineConfiguration {

  /** Creates the configuration. */
  public SqliteCryptoEngineConfiguration() {}

  // Inferred destroy method, as for the Postgres engine: the cached engine is AutoCloseable, the
  // bare engine has no close().
  @Bean
  CryptoEngine sqliteCryptoEngine(
      DataSource dataSource,
      StreamRuneCryptoProperties props,
      @Nullable StreamRuneMetrics metrics,
      CryptoBackend cryptoBackend) {
    var delegate =
        cryptoBackend.keyStoreEngine(
            dataSource,
            new CryptoBackend.KeyStoreEngineSettings(
                null, metrics != null ? metrics : StreamRuneMetrics.NOOP));
    var cacheProps = props.cache();
    if (!cacheProps.enabled()) {
      return delegate;
    }
    return CachedCryptoEngine.builder()
        .delegate(delegate)
        .maximumSize(cacheProps.maximumSize())
        .expireAfterWrite(cacheProps.expireAfterWrite())
        .expireAfterAccess(cacheProps.expireAfterAccess())
        .forgetSignal(cryptoBackend.forgetSignal(dataSource))
        .build();
  }
}
