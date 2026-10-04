package org.streamrune.spring;

import java.time.Duration;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.crypto.postgres.PostgresCryptoForgetSignal;

/**
 * Resolves the cross-replica {@link CryptoForgetSignal} for the non-Postgres crypto backends
 * (AWS-KMS, Vault, FileSystem).
 *
 * <p>{@code CachedCryptoEngine} serves decrypt cache hits without consulting the delegate or the
 * tombstone, so after a GDPR erasure a peer replica keeps serving the forgotten subject's cached
 * plaintext until the cache write-TTL elapses — unless a {@link CryptoForgetSignal} promptly evicts
 * it. The Postgres crypto backend wires the {@link PostgresCryptoForgetSignal} (LISTEN/NOTIFY)
 * channel; the other three left it {@link CryptoForgetSignal#NOOP}, a silent cross-backend
 * divergence. This helper closes that gap: a {@code PostgreSQL}-native deployment almost always has
 * a {@link DataSource}, so when one is present we wire the same LISTEN/NOTIFY channel; when none
 * is, we fall back to NOOP but log a startup WARN naming the write-TTL ceiling as the only
 * cross-replica erasure bound, so the weaker guarantee is never silent.
 */
final class SpringCryptoForgetSignals {

  private static final Logger log = LoggerFactory.getLogger(SpringCryptoForgetSignals.class);

  private SpringCryptoForgetSignals() {}

  /**
   * @param dataSource the application {@link DataSource}, or {@code null} if none is available
   * @param configuredWriteTtl the configured cache {@code expireAfterWrite}, or {@code null} to use
   *     the {@link CachedCryptoEngine#DEFAULT_EXPIRE_AFTER_WRITE default}
   * @param backend the crypto backend name, for the WARN message
   * @return a {@link PostgresCryptoForgetSignal} when a DataSource is present, else {@link
   *     CryptoForgetSignal#NOOP} (with a startup WARN)
   */
  static CryptoForgetSignal resolve(
      @Nullable DataSource dataSource, @Nullable Duration configuredWriteTtl, String backend) {
    if (dataSource != null) {
      return new PostgresCryptoForgetSignal(dataSource);
    }
    Duration writeTtl =
        configuredWriteTtl != null
            ? configuredWriteTtl
            : CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE;
    log.warn(
        "No DataSource available to wire the cross-replica crypto-forget channel for the {} crypto"
            + " backend; after a GDPR erasure, peer replicas will keep serving a forgotten subject's"
            + " cached plaintext for up to the cache write-TTL ceiling ({}) — the only cross-replica"
            + " erasure bound. Provide a Postgres DataSource for prompt (LISTEN/NOTIFY) cross-replica"
            + " eviction.",
        backend,
        writeTtl);
    return CryptoForgetSignal.NOOP;
  }
}
