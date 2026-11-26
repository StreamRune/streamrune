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

@Configuration
@ConditionalOnClass(CryptoEngine.class)
@Import(CryptoBackendConfiguration.class)
@ConditionalOnProperty(
    prefix = "streamrune.crypto.postgres",
    name = "enabled",
    havingValue = "true")
public class PostgresCryptoEngineConfiguration {

  // Inferred destroy method (not explicit destroyMethod = "close"). Cache enabled → the bean
  // is an AutoCloseable CachedCryptoEngine and is still closed on shutdown; cache.enabled=false →
  // the bare PostgresCryptoEngine has no close() and inference skips it (an explicit
  // destroyMethod = "close" would fail bean creation with "Invalid destruction signature").
  @Bean
  CryptoEngine postgresCryptoEngine(
      DataSource dataSource,
      StreamRuneCryptoProperties props,
      @Nullable StreamRuneMetrics metrics,
      CryptoBackend cryptoBackend) {
    // The backend's key-store engine. Honor the configured key-table name (validated as a SQL
    // identifier by the backend). Default keeps the migration-created "encryption_keys" table.
    var delegate =
        cryptoBackend.keyStoreEngine(
            dataSource,
            new CryptoBackend.KeyStoreEngineSettings(
                props.postgres().tableName(),
                // Counts discarded encryptions and retried decryptions; NOOP without a bean.
                metrics != null ? metrics : StreamRuneMetrics.NOOP));

    var cacheProps = props.cache();
    // Honor streamrune.crypto.cache.enabled=false (parity with Quarkus) — return the bare
    // engine so no decrypted PII is retained in heap (the deliberate no-cache GDPR posture).
    if (!cacheProps.enabled()) {
      return delegate;
    }
    // A DataSource is present, so wire the backend's forget channel (LISTEN/NOTIFY on Postgres) —
    // a deleteKey on any replica promptly evicts the forgotten subject's cached plaintext on every
    // peer. The CachedCryptoEngine is AutoCloseable and closed on context shutdown (inferred
    // destroy method), which stops the listener and releases its dedicated connection.
    return CachedCryptoEngine.builder()
        .delegate(delegate)
        .maximumSize(cacheProps.maximumSize())
        .expireAfterWrite(cacheProps.expireAfterWrite())
        .expireAfterAccess(cacheProps.expireAfterAccess())
        .forgetSignal(cryptoBackend.forgetSignal(dataSource))
        .build();
  }
}
