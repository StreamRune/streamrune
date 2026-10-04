package org.streamrune.crypto;

import java.util.function.Consumer;

/**
 * Cross-instance cache-invalidation channel for {@link CachedCryptoEngine}, so a GDPR erasure
 * (crypto-shredding) on one replica promptly evicts the forgotten subject's decrypted plaintext
 * from every other replica's local cache rather than leaving it readable until the cache TTL
 * elapses.
 *
 * <p><b>Why this exists.</b> {@link CachedCryptoEngine} serves decrypt cache hits without
 * consulting the delegate. When replica A processes {@code deleteKey(X)} it deletes the durable key
 * and invalidates <em>only its own</em> cache; peer replicas that previously cached {@code
 * decrypt(X)} keep serving that now-forgotten plaintext until their own entries expire. This
 * channel closes that gap: {@code deleteKey} publishes an opaque per-subject token, and every
 * {@code CachedCryptoEngine} subscribed to the same channel evicts the matching entries on receipt.
 *
 * <p><b>Opaque tokens, never PII.</b> The published {@code token} is a stable, non-reversible hash
 * of the subject id (computed by {@code CachedCryptoEngine}), never the raw subject id — a subject
 * id may itself be PII (an email, a username), and the token traverses a shared transport (e.g. a
 * Postgres {@code NOTIFY} payload that the server may log). An implementation must treat the token
 * as an opaque string and relay it verbatim; it must not attempt to interpret, log-in-cleartext, or
 * transform it.
 *
 * <p><b>Best-effort, not a correctness boundary.</b> Prompt cross-instance eviction is an
 * optimization layered on top of {@code CachedCryptoEngine}'s mandatory {@code expireAfterWrite}
 * ceiling, which is the <em>guaranteed</em> bounded-staleness window even if this channel drops,
 * coalesces, or never delivers a signal. Implementations should therefore favor liveness over
 * exactly-once delivery.
 *
 * <p>The {@link #NOOP} implementation does nothing; it is the default wiring when no cross-instance
 * transport (e.g. a Postgres {@code DataSource}) is available, in which case peers converge only
 * via the write-TTL ceiling.
 */
public interface CryptoForgetSignal extends AutoCloseable {

  /**
   * Publishes an opaque per-subject token to every peer subscribed to the same channel, signalling
   * that the subject was forgotten and its cached plaintext must be evicted. Best-effort: a failure
   * to publish must never fail the originating {@code deleteKey} (the durable key deletion already
   * succeeded, and peers are still bounded by the write-TTL ceiling).
   *
   * @param token an opaque, non-PII token identifying the forgotten subject; never null
   */
  void publish(String token);

  /**
   * Registers a listener invoked with the opaque token of every forgotten subject observed on the
   * channel (including, harmlessly, tokens this instance itself published). {@code
   * CachedCryptoEngine} passes a listener that evicts the matching cache entries.
   *
   * @param listener receives each published token; never null
   */
  void subscribe(Consumer<String> listener);

  @Override
  default void close() {}

  /**
   * No-op channel: no cross-instance transport is available, so peers converge via the TTL only.
   */
  CryptoForgetSignal NOOP =
      new CryptoForgetSignal() {
        @Override
        public void publish(String token) {}

        @Override
        public void subscribe(Consumer<String> listener) {}

        @Override
        public String toString() {
          return "CryptoForgetSignal.NOOP";
        }
      };
}
