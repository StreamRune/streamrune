package org.streamrune.aws;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import javax.crypto.SecretKey;
import org.streamrune.core.types.SubjectId;

/**
 * The bounded, in-process cache of plaintext data keys behind {@link AwsKmsCryptoEngine}.
 *
 * <p>Two least-recently-used maps, each holding at most {@code maximumSize} entries: the data key
 * each subject currently encrypts under (with its wrapped form and the number of values it has
 * encrypted), and the data keys unwrapped for decryption, keyed by subject <em>and</em> wrapped
 * form, so a wrapped key is only ever looked up for the subject it was presented with. Every entry
 * expires {@code expireAfterWriteNanos} after it was stored, however often it is used.
 *
 * <p>The cache is never what decides whether a subject's values can be read: the engine reads the
 * subject's tombstone before every lookup. Dropping a forgotten subject's keys only stops them from
 * staying in heap. {@link #evict(SubjectId)} advances an epoch before it removes anything, and a
 * store carries the epoch its caller read before it read the tombstone: a store whose epoch has
 * moved is dropped, so a key fetched while a forget was in progress is not cached after that
 * forget's eviction.
 *
 * <p>All methods synchronize on the cache. None of them calls out of the process.
 */
final class DataKeyCache {

  /** A data key the engine encrypts under, with its wrapped form and its remaining budget. */
  static final class EncryptionKey {
    private final SecretKey key;
    private final byte[] wrapped;
    private final long storedAt;
    private long encryptions;

    EncryptionKey(SecretKey key, byte[] wrapped, long storedAt) {
      this.key = key;
      this.wrapped = wrapped;
      this.storedAt = storedAt;
    }

    SecretKey key() {
      return key;
    }

    byte[] wrapped() {
      return wrapped;
    }
  }

  private record UnwrapKey(SubjectId subject, byte[] wrapped) {
    @Override
    public boolean equals(Object o) {
      return o instanceof UnwrapKey(SubjectId s, byte[] w)
          && subject.equals(s)
          && Arrays.equals(wrapped, w);
    }

    @Override
    public int hashCode() {
      return 31 * subject.hashCode() + Arrays.hashCode(wrapped);
    }
  }

  private record UnwrappedKey(SecretKey key, long storedAt) {}

  private final int maximumSize;
  private final long expireAfterWriteNanos;
  private final long maxEncryptionsPerKey;
  private final LongSupplier nanoClock;
  private final Map<SubjectId, EncryptionKey> encryptionKeys;
  private final Map<UnwrapKey, UnwrappedKey> unwrappedKeys;
  private long epoch;

  DataKeyCache(
      int maximumSize,
      long expireAfterWriteNanos,
      long maxEncryptionsPerKey,
      LongSupplier nanoClock) {
    this.maximumSize = maximumSize;
    this.expireAfterWriteNanos = expireAfterWriteNanos;
    this.maxEncryptionsPerKey = maxEncryptionsPerKey;
    this.nanoClock = nanoClock;
    this.encryptionKeys = lru(maximumSize);
    this.unwrappedKeys = lru(maximumSize);
  }

  private static <K, V> Map<K, V> lru(int maximumSize) {
    return new LinkedHashMap<>(16, 0.75f, true) {
      @Override
      protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        return size() > maximumSize;
      }
    };
  }

  /** The epoch to pass to a later store; read it before the tombstone. */
  synchronized long epoch() {
    return epoch;
  }

  /** The current time of the cache's clock, for a new {@link EncryptionKey}. */
  long now() {
    return nanoClock.getAsLong();
  }

  private boolean expired(long storedAt) {
    return nanoClock.getAsLong() - storedAt >= expireAfterWriteNanos;
  }

  /**
   * The subject's cached data key with one encryption taken from its budget, or null when there is
   * none, it expired, or its budget is spent (the entry is then dropped).
   */
  synchronized EncryptionKey takeEncryptionKey(SubjectId subject) {
    EncryptionKey cached = encryptionKeys.get(subject);
    if (cached == null) {
      return null;
    }
    if (expired(cached.storedAt) || cached.encryptions >= maxEncryptionsPerKey) {
      encryptionKeys.remove(subject);
      return null;
    }
    cached.encryptions++;
    return cached;
  }

  /**
   * Caches a freshly generated data key that has encrypted one value, for encryption and for
   * decryption, unless a forget evicted anything since {@code epochSeen}.
   */
  synchronized void storeEncryptionKey(SubjectId subject, EncryptionKey key, long epochSeen) {
    if (maximumSize == 0 || epochSeen != epoch) {
      return;
    }
    key.encryptions = 1;
    encryptionKeys.put(subject, key);
    unwrappedKeys.put(new UnwrapKey(subject, key.wrapped), new UnwrappedKey(key.key, key.storedAt));
  }

  /** The data key unwrapped earlier for this subject and wrapped form, or null. */
  synchronized SecretKey unwrappedKey(SubjectId subject, byte[] wrapped) {
    UnwrapKey id = new UnwrapKey(subject, wrapped);
    UnwrappedKey cached = unwrappedKeys.get(id);
    if (cached == null) {
      return null;
    }
    if (expired(cached.storedAt)) {
      unwrappedKeys.remove(id);
      return null;
    }
    return cached.key;
  }

  /** Caches an unwrapped data key unless a forget evicted anything since {@code epochSeen}. */
  synchronized void storeUnwrappedKey(
      SubjectId subject, byte[] wrapped, SecretKey key, long epochSeen) {
    if (maximumSize == 0 || epochSeen != epoch) {
      return;
    }
    unwrappedKeys.put(new UnwrapKey(subject, wrapped.clone()), new UnwrappedKey(key, now()));
  }

  /** Drops every data key of the subject; a store that read an earlier epoch is dropped too. */
  synchronized void evict(SubjectId subject) {
    epoch++;
    encryptionKeys.remove(subject);
    unwrappedKeys.keySet().removeIf(id -> id.subject.equals(subject));
  }
}
