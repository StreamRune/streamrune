package org.streamrune.crypto;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.types.SubjectId;

/**
 * Caching decorator for a {@link CryptoEngine}. Caches decrypt results so repeated reads of the
 * same ciphertext (e.g. during event replay) avoid round-trips to slow key backends.
 *
 * <p>Cache entries are keyed on the subject id <em>plus</em> a SHA-256 hash of the ciphertext, so
 * two different ciphertexts for the same subject never share a cached plaintext. {@link
 * #deleteKey(SubjectId)} invalidates every cached entry for the subject before delegating, so
 * crypto-shredding takes effect immediately on this instance.
 *
 * <p><b>Terminal erasure passes through.</b> This decorator only caches <em>decrypt</em> results;
 * {@code encrypt} is delegated verbatim, so a {@link
 * org.streamrune.core.crypto.SubjectForgottenException} from a tombstone-enforcing backend
 * propagates unchanged and is never cached or swallowed. {@link #reinstate(SubjectId)} delegates
 * too.
 *
 * <p>Plaintexts are defensively copied on both store and read — a caller mutating a returned array
 * can never corrupt what subsequent callers receive.
 *
 * <p><b>PII retention — a hard write-TTL ceiling is ALWAYS enforced.</b> Cached entries hold
 * decrypted plaintext in heap until evicted. An {@code expireAfterWrite} ceiling always applies:
 * either the explicit {@link Builder#expireAfterWrite(Duration)} or, when none is set, the
 * mandatory {@link #DEFAULT_EXPIRE_AFTER_WRITE}. Crucially it is enforced <em>even when {@link
 * Builder#expireAfterAccess(Duration)} is configured</em> — an access-only cache would let a
 * frequently-read entry live forever, so a forgotten subject's plaintext could linger unbounded on
 * a peer replica that keeps reading it. The write ceiling guarantees <b>no</b> entry outlives a
 * bounded window since it was cached, regardless of access, so cross-replica GDPR erasure is
 * fleet-effective within at most that window. {@code expireAfterAccess}, when set, only evicts
 * <em>sooner</em>; it can never raise the ceiling.
 *
 * <p><b>Prompt cross-replica erasure.</b> The write-TTL ceiling bounds staleness, but a {@link
 * CryptoForgetSignal} (default {@link CryptoForgetSignal#NOOP}) makes erasure propagate
 * <em>promptly</em>: {@link #deleteKey(SubjectId)} publishes the forgotten subject's opaque token,
 * and every {@code CachedCryptoEngine} subscribed to the same channel evicts the subject's cached
 * plaintext on receipt. The two mechanisms compose — the signal handles the common case in
 * (typically) sub-second time; the write-TTL ceiling is the guaranteed bound if the signal drops or
 * is unavailable. Wire a real channel (e.g. the Postgres {@code LISTEN/NOTIFY} implementation) via
 * {@link Builder#forgetSignal(CryptoForgetSignal)} where a shared transport is available.
 *
 * <p><b>Decrypt/deleteKey race (crypto-shredding must not resurrect plaintext).</b> The cache-miss
 * path in {@link #decrypt} is check-cache -&gt; call delegate -&gt; populate cache; without
 * coordination, a {@link #deleteKey(SubjectId)} racing between the delegate call and the cache
 * populate could let a just-decrypted, now-forgotten plaintext be cached right after the
 * invalidation swept past it. A per-subject <b>striped lock</b> closes this fully: the cache-MISS
 * path of {@code decrypt} (its delegate call and cache populate) and {@code deleteKey} (its
 * invalidate and delegate) hold the same stripe for a given subject, so they are mutually exclusive
 * — the two can never interleave, and a forgotten subject's plaintext can never be cached after its
 * erasure. Cache <em>hits</em> take no lock (the hot path stays fast). Locks are a fixed-size array
 * (no unbounded growth); two subjects sharing a stripe by hash collision only serialize harmlessly.
 * As a bonus, concurrent cache-miss decrypts of the same ciphertext collapse to a single delegate
 * call.
 */
public final class CachedCryptoEngine implements CryptoEngine, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(CachedCryptoEngine.class);

  /**
   * Mandatory default {@code expireAfterWrite} ceiling, applied whenever the builder is not given
   * an explicit {@link Builder#expireAfterWrite(Duration)} — including when only {@link
   * Builder#expireAfterAccess(Duration)} is configured. This is the guaranteed bounded-staleness
   * window: no cached plaintext outlives it since it was written, so a forgotten subject's PII
   * cannot linger unbounded on a peer replica by mere oversight (or by an access-only config).
   */
  public static final Duration DEFAULT_EXPIRE_AFTER_WRITE = Duration.ofMinutes(5);

  /** Number of lock stripes. Bounds lock memory; each subject maps to one stripe by hash. */
  private static final int LOCK_STRIPES = 64;

  private final CryptoEngine delegate;
  private final Cache<CacheKey, byte[]> cache;
  private final CryptoForgetSignal forgetSignal;

  /**
   * Per-subject striped locks (see the class javadoc). {@link #decrypt}'s cache-miss path and
   * {@link #deleteKey} for the same subject take the same stripe, making the delegate-call +
   * cache-populate atomic with respect to a concurrent invalidate + delegate.
   */
  private final ReentrantLock[] stripes;

  /** Cache key: subject id plus SHA-256 hex of the ciphertext being decrypted. */
  private record CacheKey(SubjectId subjectId, String ciphertextHash) {}

  private CachedCryptoEngine(Builder builder) {
    this.delegate = builder.delegate;
    Caffeine<Object, Object> b = Caffeine.newBuilder().maximumSize(builder.maximumSize);
    // A hard write-TTL ceiling is ALWAYS enforced — either the explicit value or the mandatory
    // default — so no cached plaintext can outlive a bounded window since it was written. This
    // holds even when expireAfterAccess is set: an access-only cache would let a frequently-read
    // entry live forever, leaving a forgotten subject's plaintext readable indefinitely on a peer
    // replica. expireAfterAccess, when set, only evicts SOONER; it can never raise this
    // ceiling.
    Duration writeTtlCeiling =
        builder.expireAfterWrite != null ? builder.expireAfterWrite : DEFAULT_EXPIRE_AFTER_WRITE;
    b.expireAfterWrite(writeTtlCeiling);
    if (builder.expireAfterAccess != null) {
      b.expireAfterAccess(builder.expireAfterAccess);
    }
    // A test-only ticker seam: lets tests drive expiry deterministically (no wall-clock sleeps) so
    // the write-TTL ceiling can be asserted to actually take effect. Null in production.
    if (builder.ticker != null) {
      b.ticker(builder.ticker);
    }
    this.cache = b.build();
    this.stripes = new ReentrantLock[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      this.stripes[i] = new ReentrantLock();
    }
    this.forgetSignal = builder.forgetSignal;
    // Subscribe to cross-instance forget signals: when a peer replica crypto-shreds a subject,
    // evict that subject's cached plaintext here too, promptly (rather than waiting out the
    // write-TTL). The NOOP channel never invokes the listener, so a default engine starts no
    // background listener.
    this.forgetSignal.subscribe(this::invalidateByToken);
  }

  public static Builder builder() {
    return new Builder();
  }

  private ReentrantLock stripeFor(SubjectId subjectId) {
    return stripes[Math.floorMod(subjectId.hashCode(), stripes.length)];
  }

  @Override
  public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
    return delegate.encrypt(subjectId, plaintext);
  }

  @Override
  public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
    if (subjectId == null) throw new IllegalArgumentException("subjectId must not be null");
    if (ciphertext == null) throw new IllegalArgumentException("ciphertext must not be null");
    CacheKey key = new CacheKey(subjectId, sha256Hex(ciphertext));
    byte[] cached = cache.getIfPresent(key);
    if (cached != null) {
      // Fast path: a cache hit needs no lock.
      return cached.clone();
    }
    // Cache miss: take the subject's stripe so the delegate call + cache populate below are atomic
    // with respect to a concurrent deleteKey (which takes the same stripe). A deleteKey for this
    // subject blocks until this completes, then its invalidate removes whatever we populate — a
    // forgotten subject's plaintext is never left in the cache.
    ReentrantLock lock = stripeFor(subjectId);
    lock.lock();
    try {
      // Double-check: another thread may have populated the cache while we waited for the lock.
      byte[] recheck = cache.getIfPresent(key);
      if (recheck != null) {
        return recheck.clone();
      }
      byte[] result = delegate.decrypt(subjectId, ciphertext);
      // Store a private copy: callers receive distinct arrays and cannot mutate the cached one.
      cache.put(key, result.clone());
      return result;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void deleteKey(SubjectId subjectId) {
    // Take the subject's stripe so this invalidate + delegate is mutually exclusive with a
    // concurrent decrypt cache-miss for the same subject — the decrypt cannot populate the cache
    // after we invalidate, and we cannot invalidate between its delegate call and its populate.
    ReentrantLock lock = stripeFor(subjectId);
    lock.lock();
    try {
      // Invalidate every cached plaintext for this subject, regardless of ciphertext.
      cache.asMap().keySet().removeIf(key -> key.subjectId().equals(subjectId));
      delegate.deleteKey(subjectId);
    } finally {
      lock.unlock();
    }
    // After the local erasure has durably committed on the delegate, tell peer replicas
    // to evict this subject's cached plaintext too. Published OUTSIDE the stripe lock so a
    // slow/blocking transport never pins the lock, and best-effort so a publish failure can never
    // fail the erasure (the durable delete already succeeded; peers are still bounded by the
    // write-TTL ceiling).
    publishForget(subjectId);
  }

  private void publishForget(SubjectId subjectId) {
    if (forgetSignal == CryptoForgetSignal.NOOP) {
      return;
    }
    try {
      forgetSignal.publish(subjectToken(subjectId));
    } catch (RuntimeException e) {
      log.warn(
          "Failed to publish a cross-instance crypto-forget signal after deleteKey; the local"
              + " erasure succeeded and peer replicas will still converge within the cache write-TTL"
              + " ceiling, but prompt cross-replica eviction did not fire",
          e);
    }
  }

  /**
   * Evicts every cached entry whose subject matches the opaque token published by a peer replica's
   * {@link #deleteKey}. Lock-free: this is a pure removal on the concurrent cache map. A decrypt
   * for the same subject that is in flight at this exact instant could re-cache it — but that
   * residual window is itself bounded by the write-TTL ceiling, so cross-replica staleness stays
   * bounded.
   */
  private void invalidateByToken(String token) {
    if (token == null) {
      return;
    }
    cache.asMap().keySet().removeIf(key -> subjectToken(key.subjectId()).equals(token));
  }

  /**
   * Closes the underlying {@link CryptoForgetSignal} (e.g. stops the Postgres {@code LISTEN}
   * listener and releases its dedicated connection). Idempotent for the NOOP channel. The delegate
   * engine is not closed here — its lifecycle is owned by whoever built it.
   */
  @Override
  public void close() {
    try {
      forgetSignal.close();
    } catch (Exception e) {
      log.warn("Failed to close the crypto-forget signal", e);
    }
  }

  @Override
  public void reinstate(SubjectId subjectId) {
    // Reinstating only affects future encrypts on the delegate; there is nothing cached to touch.
    delegate.reinstate(subjectId);
  }

  @Override
  public boolean isKeyAvailable(SubjectId subjectId) {
    // A cached decrypt result proves the key existed; the scan is bounded by maximumSize.
    for (CacheKey key : cache.asMap().keySet()) {
      if (key.subjectId().equals(subjectId)) {
        return true;
      }
    }
    return delegate.isKeyAvailable(subjectId);
  }

  @Override
  public java.util.Set<String> requiredCryptoTables() {
    // This decorator adds no schema of its own; the delegate defines the requirement.
    return delegate.requiredCryptoTables();
  }

  private static String sha256Hex(byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (NoSuchAlgorithmException e) {
      throw new CryptoOperationException("SHA-256 hashing failed", e);
    }
  }

  /**
   * The opaque, non-reversible token that identifies a subject on the {@link CryptoForgetSignal}
   * channel: the SHA-256 hex of the subject id's UTF-8 bytes. The raw subject id may itself be PII,
   * and the token crosses a shared transport (a Postgres {@code NOTIFY} payload the server may
   * log), so it must never be published verbatim. Both the publish side ({@link #deleteKey}) and
   * the match side ({@link #invalidateByToken}) compute the token the same way, so peers agree
   * without ever exchanging the raw id.
   */
  private static String subjectToken(SubjectId subjectId) {
    return sha256Hex(subjectId.value().getBytes(StandardCharsets.UTF_8));
  }

  public static final class Builder {
    private CryptoEngine delegate;
    private int maximumSize = 10_000;
    private Duration expireAfterWrite;
    private Duration expireAfterAccess;
    private Ticker ticker;
    private CryptoForgetSignal forgetSignal = CryptoForgetSignal.NOOP;

    public Builder delegate(CryptoEngine delegate) {
      this.delegate = delegate;
      return this;
    }

    /**
     * Cross-instance cache-invalidation channel. When set to a real transport (e.g. the Postgres
     * {@code LISTEN/NOTIFY} implementation), a {@code deleteKey} on any replica promptly evicts the
     * forgotten subject's cached plaintext on every peer subscribed to the same channel, rather
     * than leaving it readable until the write-TTL ceiling elapses. Defaults to {@link
     * CryptoForgetSignal#NOOP} (no transport available — peers converge via the TTL ceiling only).
     */
    public Builder forgetSignal(CryptoForgetSignal forgetSignal) {
      this.forgetSignal = forgetSignal == null ? CryptoForgetSignal.NOOP : forgetSignal;
      return this;
    }

    /**
     * Test-only seam: supplies the Caffeine {@link Ticker} that drives cache expiry, so tests can
     * advance time deterministically instead of sleeping on the wall clock. Package-private on
     * purpose — production callers must not depend on injecting a clock; leave it unset and
     * Caffeine uses {@link System#nanoTime()}.
     */
    Builder ticker(Ticker ticker) {
      this.ticker = ticker;
      return this;
    }

    public Builder maximumSize(int maximumSize) {
      this.maximumSize = maximumSize;
      return this;
    }

    public Builder expireAfterWrite(Duration duration) {
      this.expireAfterWrite = duration;
      return this;
    }

    public Builder expireAfterAccess(Duration duration) {
      this.expireAfterAccess = duration;
      return this;
    }

    public CachedCryptoEngine build() {
      if (delegate == null) throw new IllegalStateException("delegate is required");
      if (maximumSize < 0) throw new IllegalArgumentException("maximumSize must be non-negative");
      return new CachedCryptoEngine(this);
    }
  }
}
