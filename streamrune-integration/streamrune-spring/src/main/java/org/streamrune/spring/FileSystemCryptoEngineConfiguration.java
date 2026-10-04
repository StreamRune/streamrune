package org.streamrune.spring;

import java.nio.file.Path;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.filesystem.FileSystemCryptoEngine;

@Configuration
@ConditionalOnClass(CryptoEngine.class)
@ConditionalOnProperty(
    prefix = "streamrune.crypto.filesystem",
    name = "enabled",
    havingValue = "true")
public class FileSystemCryptoEngineConfiguration {

  // Inferred destroy method (not an explicit destroyMethod = "close"). When the cache is
  // enabled the bean is an AutoCloseable CachedCryptoEngine, so Spring still closes it on shutdown;
  // when cache.enabled=false the bare FileSystemCryptoEngine has no close() and inference simply
  // skips it — an explicit destroyMethod = "close" would fail bean creation with "Invalid
  // destruction signature".
  @Bean
  CryptoEngine fileSystemCryptoEngine(
      @Nullable DataSource dataSource, StreamRuneCryptoProperties props) {
    String keyDirectory = props.filesystem().keyDirectory();
    if (keyDirectory == null || keyDirectory.isBlank()) {
      throw new IllegalStateException(
          "streamrune.crypto.filesystem.key-directory must be set when streamrune.crypto.filesystem.enabled is true");
    }
    var delegate = FileSystemCryptoEngine.builder().keyDirectory(Path.of(keyDirectory)).build();

    var cacheProps = props.cache();
    // Honor streamrune.crypto.cache.enabled=false (parity with Quarkus) — return the bare
    // engine so no decrypted PII is retained in heap (the deliberate no-cache GDPR posture).
    if (!cacheProps.enabled()) {
      return delegate;
    }
    // Even the filesystem backend (whose own key tombstone is node-local)
    // needs cross-replica cache eviction on a shared read model. Wire the LISTEN/NOTIFY forget
    // channel when a DataSource is present; else NOOP + a startup WARN naming the write-TTL
    // ceiling.
    return CachedCryptoEngine.builder()
        .delegate(delegate)
        .maximumSize(cacheProps.maximumSize())
        .expireAfterWrite(cacheProps.expireAfterWrite())
        .expireAfterAccess(cacheProps.expireAfterAccess())
        .forgetSignal(
            SpringCryptoForgetSignals.resolve(
                dataSource, cacheProps.expireAfterWrite(), "filesystem"))
        .build();
  }
}
