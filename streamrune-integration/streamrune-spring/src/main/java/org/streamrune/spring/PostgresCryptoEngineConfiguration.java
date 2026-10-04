package org.streamrune.spring;

import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoEngine;
import org.streamrune.crypto.postgres.PostgresCryptoForgetSignal;

@Configuration
@ConditionalOnClass(CryptoEngine.class)
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
  CryptoEngine postgresCryptoEngine(DataSource dataSource, StreamRuneCryptoProperties props) {
    // Honor the configured key-table name (validated as a SQL identifier inside the
    // engine builder). Default keeps the migration-created "encryption_keys" table.
    var delegate =
        PostgresCryptoEngine.builder()
            .dataSource(dataSource)
            .tableName(props.postgres().tableName())
            .build();

    var cacheProps = props.cache();
    // Honor streamrune.crypto.cache.enabled=false (parity with Quarkus) — return the bare
    // engine so no decrypted PII is retained in heap (the deliberate no-cache GDPR posture).
    if (!cacheProps.enabled()) {
      return delegate;
    }
    // A Postgres DataSource is present, so wire the LISTEN/NOTIFY forget channel — a
    // deleteKey on any replica promptly evicts the forgotten subject's cached plaintext on every
    // peer. The CachedCryptoEngine is AutoCloseable and closed on context shutdown (inferred
    // destroy method), which stops the listener and releases its dedicated connection.
    return CachedCryptoEngine.builder()
        .delegate(delegate)
        .maximumSize(cacheProps.maximumSize())
        .expireAfterWrite(cacheProps.expireAfterWrite())
        .expireAfterAccess(cacheProps.expireAfterAccess())
        .forgetSignal(new PostgresCryptoForgetSignal(dataSource))
        .build();
  }
}
