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
 * <p><b>A replica that cannot hear drops what it cached once it can.</b> A forget published while
 * this replica's listening side is not established — before it first takes effect, or while its
 * connection is lost — never reaches it. An implementation with a listening side therefore runs
 * every subscriber's {@code onListening} callback each time it starts hearing, after it can hear
 * and before it delivers the first token; {@code CachedCryptoEngine} evicts its whole plaintext
 * cache there, so nothing cached while the replica could not hear outlives that moment. {@link
 * #isListening()} says whether the replica hears right now.
 *
 * <p><b>Best-effort delivery; the TTL bounds what it misses.</b> Prompt cross-instance eviction is
 * layered on top of {@code CachedCryptoEngine}'s mandatory {@code expireAfterWrite} ceiling, which
 * bounds staleness for every case the channel does not cover: a replica whose listening side never
 * takes effect, and a forget delivered late to a replica that hears. Implementations should
 * therefore favor liveness over exactly-once delivery.
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
   * Registers a subscriber: {@code onForget} receives the opaque token of every forgotten subject
   * observed on the channel (including, harmlessly, tokens this instance itself published), and
   * {@code onListening} runs each time the channel starts hearing peers' forgets — the first time
   * and after every reconnect — before any token heard from then on is delivered. Forgets published
   * while the channel could not hear are not delivered, so a subscriber that holds state a forget
   * must reach drops all of it in {@code onListening}. {@code CachedCryptoEngine} passes a listener
   * that evicts the matching cache entries and a callback that evicts the whole cache.
   *
   * @param onForget receives each published token; never null
   * @param onListening runs each time the channel starts hearing; never null
   */
  void subscribe(Consumer<String> onForget, Runnable onListening);

  /**
   * Registers a listener for forgotten subjects' tokens alone: {@link #subscribe(Consumer,
   * Runnable)} with nothing to do when the channel starts hearing.
   *
   * @param onForget receives each published token; never null
   */
  default void subscribe(Consumer<String> onForget) {
    subscribe(onForget, () -> {});
  }

  /**
   * Whether this channel has a listening side on this replica that a subscriber started: {@code
   * true} from the first {@link #subscribe(Consumer, Runnable)} until {@link #close()}, also while
   * that side cannot hear. {@code false} for a channel without one, such as {@link #NOOP}. Health
   * reporting covers a channel for which this is {@code true}, and only such a channel: an
   * implementation with a listening side must override this method and {@link #isListening()}, or
   * the framework health indicators never report it.
   *
   * @return whether a listening side was started and not closed
   */
  default boolean isSubscribed() {
    return false;
  }

  /**
   * Whether this replica hears peers' forgets on this channel right now: the listening side is
   * established and its subscribers have run {@code onListening} since it was. {@code false} before
   * the first subscribe, while it connects or reconnects, when it cannot be opened, after {@link
   * #close()}, and always for a channel without a listening side.
   *
   * @return whether forgets published now reach this replica
   */
  default boolean isListening() {
    return false;
  }

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
        public void subscribe(Consumer<String> onForget, Runnable onListening) {}

        @Override
        public String toString() {
          return "CryptoForgetSignal.NOOP";
        }
      };
}
