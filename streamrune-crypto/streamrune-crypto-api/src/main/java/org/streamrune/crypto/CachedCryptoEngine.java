package org.streamrune.crypto;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
 * <p><b>What was cached while the replica could not hear is dropped once it can.</b> A forget a
 * peer publishes while this replica's channel is not listening — before its first {@code LISTEN}
 * takes effect, or while its connection is lost — never arrives here. Each time the channel starts
 * hearing, after it can hear and before it delivers anything, it tells this engine, and the engine
 * evicts its whole plaintext cache: every subject is read from the key store again, and a subject
 * forgotten in that window finds its key gone. Until then the cache keeps serving, bounded by the
 * write-TTL ceiling.
 *
 * <p><b>An eviction is not undone by a decrypt in flight.</b> Every eviction a signal triggers
 * advances an eviction counter before it removes anything: the counter of the forgotten subject's
 * eviction stripe for one subject's forget, and one counter that every decrypt reads for the
 * eviction of everything. A cache-miss decrypt reads the counters that apply to its subject before
 * it calls the delegate and stores its result only if they have not moved since; the check and the
 * store are one step that an advance cannot interleave with. For a decrypt that read the counter
 * before an eviction advanced it, either its step comes before that advance, so its entry was
 * stored before the eviction removed entries and is removed by it, or its step comes after, sees
 * the counter moved and stores nothing — so no thread ever finds the entry. A decrypt that read the
 * counter after the advance read the key after the forget that caused the eviction had deleted it
 * (a peer publishes a forget after its key deletion committed, and a channel starts hearing before
 * the engine evicts), so it finds the key gone. A decrypt that read the key just before a peer's
 * forget therefore cannot leave the plaintext cached after the eviction that forget caused. A
 * forget costs only the in-flight results of its own stripe — one in 64 subjects at random — not of
 * every subject, so a stream of forgets (a bulk erasure) does not keep the cache cold for the
 * subjects nobody forgot.
 *
 * <p>That argument assumes two things of the delegate, which the shipped engines (PostgreSQL,
 * filesystem, Vault, AWS KMS) meet: it holds no cache of key material of its own, and each decrypt
 * reads the key, or the subject's tombstone, afresh and sees every deletion committed before the
 * read — a new statement under {@code READ COMMITTED}, a file read, a remote call. A delegate that
 * caches keys, or whose reads join an application transaction at {@code REPEATABLE READ} or {@code
 * SERIALIZABLE} that began before the forget, can still find the key after the eviction and cache
 * the plaintext again; the write-TTL ceiling then bounds it.
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

  /**
   * Number of eviction stripes: the subjects a peer's forget can cost an in-flight decrypt are one
   * stripe's share of them.
   */
  private static final int EVICTION_STRIPES = 64;

  private final CryptoEngine delegate;
  private final Cache<CacheKey, byte[]> cache;
  private final CryptoForgetSignal forgetSignal;

  /**
   * Per-subject striped locks (see the class javadoc). {@link #decrypt}'s cache-miss path and
   * {@link #deleteKey} for the same subject take the same stripe, making the delegate-call +
   * cache-populate atomic with respect to a concurrent invalidate + delegate.
   */
  private final ReentrantLock[] stripes;

  /**
   * Advanced before a {@link CryptoForgetSignal}'s eviction of the subjects of one stripe (see
   * {@link #evictionStripe(SubjectId)}), indexed by stripe; see the class javadoc.
   */
  private final AtomicLong[] stripeEvictions = new AtomicLong[EVICTION_STRIPES];

  /** Advanced before a {@link CryptoForgetSignal}'s eviction of every subject. */
  private final AtomicLong allEvictions = new AtomicLong();

  /**
   * Makes a decrypt's check of the eviction counters and its store of the result one step with
   * respect to an advance of a counter: a decrypt holds the read side across both, an advance takes
   * the write side. Neither holds it across anything slow — the delegate call is outside it.
   */
  private final ReentrantReadWriteLock evictionGate = new ReentrantReadWriteLock();

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
    // A test-only seam, like the ticker: lets a test see an entry that was stored and removed
    // again. Notifications run on the removing thread so the test reads them without waiting.
    if (builder.removalListener != null) {
      b.removalListener(builder.removalListener).executor(Runnable::run);
    }
    this.cache = b.build();
    this.stripes = new ReentrantLock[LOCK_STRIPES];
    for (int i = 0; i < LOCK_STRIPES; i++) {
      this.stripes[i] = new ReentrantLock();
    }
    for (int i = 0; i < EVICTION_STRIPES; i++) {
      this.stripeEvictions[i] = new AtomicLong();
    }
    this.forgetSignal = builder.forgetSignal;
    // Subscribe to cross-instance forget signals: when a peer replica crypto-shreds a subject,
    // evict that subject's cached plaintext here too, promptly (rather than waiting out the
    // write-TTL), and each time the channel starts hearing, evict everything cached while it could
    // not. The NOOP channel never invokes either, so a default engine starts no background
    // listener.
    this.forgetSignal.subscribe(this::invalidateByToken, this::invalidateAllCached);
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
      AtomicLong stripeEvictionCounter = stripeEvictions[evictionStripe(subjectId)];
      long stripeEvictionsBefore = stripeEvictionCounter.get();
      long allEvictionsBefore = allEvictions.get();
      byte[] result = delegate.decrypt(subjectId, ciphertext);
      // Store a private copy: callers receive distinct arrays and cannot mutate the cached one.
      byte[] stored = result.clone();
      // The check and the store are one step with respect to an eviction's advance of its counter:
      // an eviction that advanced after the key was read either finds the stored entry and removes
      // it, or is seen here and nothing is stored — no thread ever reads an entry the eviction
      // should have removed (see the class javadoc).
      evictionGate.readLock().lock();
      try {
        if (stripeEvictionCounter.get() == stripeEvictionsBefore
            && allEvictions.get() == allEvictionsBefore) {
          cache.put(key, stored);
        }
      } finally {
        evictionGate.readLock().unlock();
      }
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
   * {@link #deleteKey}. Takes no stripe lock: this is a pure removal on the concurrent cache map,
   * after the subject's eviction counter advanced, so a decrypt in flight does not leave the
   * subject cached behind it.
   */
  private void invalidateByToken(String token) {
    if (token == null) {
      return;
    }
    advance(stripeEvictions[evictionStripe(token)]);
    cache.asMap().keySet().removeIf(key -> subjectToken(key.subjectId()).equals(token));
  }

  /**
   * Evicts every cached plaintext: the channel started hearing, and a forget published before that
   * did not reach this replica. The eviction counter advances first, as for one subject.
   */
  private void invalidateAllCached() {
    advance(allEvictions);
    cache.invalidateAll();
  }

  /** Advances an eviction counter, never between a decrypt's check of it and its store. */
  private void advance(AtomicLong counter) {
    evictionGate.writeLock().lock();
    try {
      counter.incrementAndGet();
    } finally {
      evictionGate.writeLock().unlock();
    }
  }

  /**
   * The eviction stripe of a subject: the stripe of its token, so a peer's forget, which carries
   * only the token, advances the counter of the stripe its subject's decrypts read. Package-private
   * for tests.
   */
  static int evictionStripe(SubjectId subjectId) {
    return evictionStripe(subjectToken(subjectId));
  }

  private static int evictionStripe(String token) {
    return Math.floorMod(token.hashCode(), EVICTION_STRIPES);
  }

  /**
   * The channel this engine publishes its forgets on and hears peers' forgets from; {@link
   * CryptoForgetSignal#NOOP} when none was configured. Health reporting reads whether it listens.
   *
   * @return the configured forget channel
   */
  public CryptoForgetSignal forgetSignal() {
    return forgetSignal;
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
    private RemovalListener<Object, Object> removalListener;
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

    /**
     * Test-only seam: receives every removal from the cache, so a test can tell an entry that was
     * never stored from one stored and removed again. Package-private, like {@link
     * #ticker(Ticker)}.
     */
    Builder removalListener(RemovalListener<Object, Object> removalListener) {
      this.removalListener = removalListener;
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
