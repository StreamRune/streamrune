package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;

class CachedCryptoEngineTest {

  private CryptoEngine delegate;
  private CachedCryptoEngine engine;

  @BeforeEach
  void setUp() {
    delegate = mock(CryptoEngine.class);
    engine = CachedCryptoEngine.builder().delegate(delegate).build();
  }

  @Test
  void decryptReturnsCachedValueOnHit() {
    byte[] ciphertext = "cipher".getBytes();
    byte[] plaintext = "secret".getBytes();
    when(delegate.decrypt(SubjectId.of("subject-1"), ciphertext)).thenReturn(plaintext);

    byte[] first = engine.decrypt(SubjectId.of("subject-1"), ciphertext);
    byte[] second = engine.decrypt(SubjectId.of("subject-1"), ciphertext);

    assertEquals("secret", new String(first));
    assertEquals("secret", new String(second));
    verify(delegate, times(1)).decrypt(SubjectId.of("subject-1"), ciphertext);
  }

  @Test
  void decryptFetchesFromDelegateOnMiss() {
    byte[] ciphertext = "cipher".getBytes();
    byte[] plaintext = "secret".getBytes();
    when(delegate.decrypt(SubjectId.of("subject-2"), ciphertext)).thenReturn(plaintext);

    byte[] result = engine.decrypt(SubjectId.of("subject-2"), ciphertext);

    assertEquals("secret", new String(result));
    verify(delegate).decrypt(SubjectId.of("subject-2"), ciphertext);
  }

  @Test
  void decryptDistinguishesDifferentCiphertextsForSameSubject() {
    byte[] ciphertext1 = "cipher-one".getBytes();
    byte[] ciphertext2 = "cipher-two".getBytes();
    when(delegate.decrypt(SubjectId.of("subject-1"), ciphertext1))
        .thenReturn("plain-one".getBytes());
    when(delegate.decrypt(SubjectId.of("subject-1"), ciphertext2))
        .thenReturn("plain-two".getBytes());

    // Prime the cache with the first ciphertext, then decrypt a different one.
    assertEquals("plain-one", new String(engine.decrypt(SubjectId.of("subject-1"), ciphertext1)));
    assertEquals("plain-two", new String(engine.decrypt(SubjectId.of("subject-1"), ciphertext2)));

    // Both must stay correct on cache hits.
    assertEquals("plain-one", new String(engine.decrypt(SubjectId.of("subject-1"), ciphertext1)));
    assertEquals("plain-two", new String(engine.decrypt(SubjectId.of("subject-1"), ciphertext2)));
    verify(delegate, times(1)).decrypt(SubjectId.of("subject-1"), ciphertext1);
    verify(delegate, times(1)).decrypt(SubjectId.of("subject-1"), ciphertext2);
  }

  @Test
  void isKeyAvailableChecksCacheFirst() {
    byte[] ciphertext = "cipher".getBytes();
    when(delegate.decrypt(SubjectId.of("subject-3"), ciphertext)).thenReturn("value".getBytes());
    engine.decrypt(SubjectId.of("subject-3"), ciphertext);

    assertTrue(engine.isKeyAvailable(SubjectId.of("subject-3")));
    verify(delegate, never()).isKeyAvailable(any(SubjectId.class));
  }

  @Test
  void isKeyAvailableFallsBackToDelegateOnMiss() {
    when(delegate.isKeyAvailable(SubjectId.of("subject-4"))).thenReturn(true);

    assertTrue(engine.isKeyAvailable(SubjectId.of("subject-4")));

    verify(delegate).isKeyAvailable(SubjectId.of("subject-4"));
  }

  @Test
  void encryptBypassesCache() {
    byte[] result = "encrypted".getBytes();
    when(delegate.encrypt(SubjectId.of("subject-5"), "plain".getBytes())).thenReturn(result);

    byte[] actual = engine.encrypt(SubjectId.of("subject-5"), "plain".getBytes());

    assertEquals("encrypted", new String(actual));
    verify(delegate).encrypt(SubjectId.of("subject-5"), "plain".getBytes());
  }

  @Test
  void deleteKeyEvictsAllCachedEntriesForSubject() {
    byte[] ciphertext1 = "cipher-one".getBytes();
    byte[] ciphertext2 = "cipher-two".getBytes();
    when(delegate.decrypt(SubjectId.of("subject-6"), ciphertext1))
        .thenReturn("plain-one".getBytes());
    when(delegate.decrypt(SubjectId.of("subject-6"), ciphertext2))
        .thenReturn("plain-two".getBytes());
    engine.decrypt(SubjectId.of("subject-6"), ciphertext1);
    engine.decrypt(SubjectId.of("subject-6"), ciphertext2);

    engine.deleteKey(SubjectId.of("subject-6"));

    verify(delegate).deleteKey(SubjectId.of("subject-6"));
    // Both cached plaintexts must be gone — the next decrypts hit the delegate again.
    engine.decrypt(SubjectId.of("subject-6"), ciphertext1);
    engine.decrypt(SubjectId.of("subject-6"), ciphertext2);
    verify(delegate, times(2)).decrypt(SubjectId.of("subject-6"), ciphertext1);
    verify(delegate, times(2)).decrypt(SubjectId.of("subject-6"), ciphertext2);
  }

  @Test
  void deleteKeyLeavesOtherSubjectsCached() {
    byte[] ciphertext = "cipher".getBytes();
    when(delegate.decrypt(SubjectId.of("subject-a"), ciphertext)).thenReturn("plain-a".getBytes());
    when(delegate.decrypt(SubjectId.of("subject-b"), ciphertext)).thenReturn("plain-b".getBytes());
    engine.decrypt(SubjectId.of("subject-a"), ciphertext);
    engine.decrypt(SubjectId.of("subject-b"), ciphertext);

    engine.deleteKey(SubjectId.of("subject-a"));

    // subject-b's entry survives — decrypt is served from cache.
    assertEquals("plain-b", new String(engine.decrypt(SubjectId.of("subject-b"), ciphertext)));
    verify(delegate, times(1)).decrypt(SubjectId.of("subject-b"), ciphertext);
  }

  @Test
  void decryptRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.decrypt(null, "cipher".getBytes()));
  }

  @Test
  void decryptRejectsNullCiphertext() {
    assertThrows(
        IllegalArgumentException.class, () -> engine.decrypt(SubjectId.of("subject"), null));
  }

  @Test
  void builderDefaults() {
    var engine = CachedCryptoEngine.builder().delegate(delegate).build();

    assertNotNull(engine);
  }

  @Test
  void builderSetsMaximumSize() {
    when(delegate.isKeyAvailable(any(SubjectId.class))).thenReturn(false);
    var engine = CachedCryptoEngine.builder().delegate(delegate).maximumSize(500).build();
    assertNotNull(engine);
    // Verify the configured maximumSize is respected — engine should still work
    assertFalse(engine.isKeyAvailable(SubjectId.of("subject")));
  }

  @Test
  void builderSetsExpireAfterWrite() {
    when(delegate.isKeyAvailable(any(SubjectId.class))).thenReturn(false);
    var engine =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .expireAfterWrite(Duration.ofMinutes(10))
            .build();
    assertNotNull(engine);
    assertFalse(engine.isKeyAvailable(SubjectId.of("subject")));
  }

  @Test
  void builderSetsExpireAfterAccess() {
    var engine =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .expireAfterAccess(Duration.ofMinutes(2))
            .build();
    assertNotNull(engine);
  }

  @Test
  void builderRejectsNegativeMaximumSize() {
    assertThrows(
        IllegalArgumentException.class,
        () -> CachedCryptoEngine.builder().delegate(delegate).maximumSize(-1).build());
  }

  @Test
  void deleteKeyWhenNotInCache() {
    engine.deleteKey(SubjectId.of("unknown-subject"));
    verify(delegate).deleteKey(SubjectId.of("unknown-subject"));
  }

  @Test
  void encryptPropagatesSubjectForgottenWithoutCaching() {
    // A tombstone-enforcing delegate refuses to re-encrypt a forgotten subject. The cache must let
    // that terminal-erasure signal pass through unchanged, never swallowing or caching around it.
    when(delegate.encrypt(SubjectId.of("forgotten"), "pii".getBytes()))
        .thenThrow(new SubjectForgottenException("forgotten"));

    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("forgotten"), "pii".getBytes()));
    verify(delegate).encrypt(SubjectId.of("forgotten"), "pii".getBytes());
  }

  @Test
  void reinstateDelegates() {
    engine.reinstate(SubjectId.of("comeback"));
    verify(delegate).reinstate(SubjectId.of("comeback"));
  }

  @Test
  void concurrentDeleteKeyDuringDecryptNeverServesForgottenPlaintextAfterward() throws Exception {
    // Defect 8: under the per-subject striped lock, decrypt's cache-miss (delegate + populate) and
    // deleteKey (invalidate + delegate) are mutually exclusive, so a forgotten subject's plaintext
    // can never be left in the cache. We force the worst-case interleave N times WITHOUT
    // deadlocking: the first decrypt's delegate blocks on a latch WHILE HOLDING the stripe lock; a
    // second thread starts deleteKey, which blocks on the same stripe (expected); the test thread
    // then releases decrypt (it populates the cache and unlocks) WITHOUT first waiting for
    // deleteKey
    // — so deleteKey acquires the lock afterward and invalidates exactly the entry decrypt just
    // populated. A follow-up decrypt must then go back to the delegate (call count 2), proving no
    // stale entry survived. (The old test blocked decrypt until deleteKey fully completed, which
    // would deadlock now that deleteKey must wait for decrypt's lock.)
    int iterations = 200;
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      for (int i = 0; i < iterations; i++) {
        int iteration = i;
        SubjectId subject = SubjectId.of("race-subject-" + iteration);
        byte[] ciphertext = ("cipher-" + iteration).getBytes();
        byte[] plaintext = ("secret-" + iteration).getBytes();

        CountDownLatch decryptEnteredDelegate = new CountDownLatch(1);
        CountDownLatch releaseDecrypt = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicInteger decryptCalls =
            new java.util.concurrent.atomic.AtomicInteger();
        CryptoEngine racingDelegate =
            new CryptoEngine() {
              @Override
              public byte[] encrypt(SubjectId s, byte[] p) {
                throw new UnsupportedOperationException();
              }

              @Override
              public byte[] decrypt(SubjectId s, byte[] c) {
                int call = decryptCalls.incrementAndGet();
                if (call == 1) {
                  // The first decrypt is inside the delegate holding the stripe lock. Signal that,
                  // then block until the test releases us — long enough for deleteKey to reach and
                  // block on the same stripe.
                  decryptEnteredDelegate.countDown();
                  try {
                    assertTrue(
                        releaseDecrypt.await(5, TimeUnit.SECONDS),
                        "decrypt was not released in time (iteration " + iteration + ")");
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                  }
                }
                return plaintext.clone();
              }

              @Override
              public void deleteKey(SubjectId s) {}

              @Override
              public boolean isKeyAvailable(SubjectId s) {
                return false;
              }
            };
        CachedCryptoEngine racingEngine =
            CachedCryptoEngine.builder().delegate(racingDelegate).build();

        var decryptTask = executor.submit(() -> racingEngine.decrypt(subject, ciphertext));
        // Wait until decrypt is inside the delegate (holding the stripe lock), then start deleteKey
        // — it will block on the stripe until decrypt finishes.
        assertTrue(
            decryptEnteredDelegate.await(5, TimeUnit.SECONDS),
            "decrypt did not enter the delegate in time (iteration " + iteration + ")");
        var deleteTask =
            executor.submit(
                () -> {
                  racingEngine.deleteKey(subject);
                  return null;
                });
        // Give deleteKey a moment to reach and block on the stripe, then release decrypt so it
        // populates the cache and unlocks; deleteKey then invalidates that entry.
        Thread.sleep(2);
        releaseDecrypt.countDown();

        byte[] decryptResult = decryptTask.get(10, TimeUnit.SECONDS);
        deleteTask.get(10, TimeUnit.SECONDS);
        assertArrayEquals(plaintext, decryptResult, "the in-flight decrypt itself still succeeds");

        // Once deleteKey has completed, the entry the in-flight decrypt populated must have been
        // invalidated. A follow-up decrypt must go back to the delegate (call count increments to
        // 2) instead of being served from a leftover cache entry (which would leave it at 1).
        racingEngine.decrypt(subject, ciphertext);
        assertEquals(
            2,
            decryptCalls.get(),
            "a decrypt after deleteKey completed must not be served from a stale cache entry"
                + " (iteration "
                + i
                + ")");
      }
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void builderWithoutExplicitTtlStillEnforcesADefaultTtl() {
    // Defect 8: a default-built engine (no expireAfterWrite/expireAfterAccess configured) must
    // still bound how long decrypted PII plaintext stays cached in heap — defense-in-depth for
    // GDPR retention, even when the caller forgot to configure a TTL explicitly.
    //
    // This test is NON-VACUOUS: it drives a deterministic Caffeine ticker (no wall-clock sleeps)
    // and asserts a cached entry is still present just before DEFAULT_EXPIRE_AFTER_WRITE elapses
    // and gone just after — so it FAILS if the mandatory-default-TTL wiring is removed from
    // CachedCryptoEngine (the `else if (expireAfterAccess == null) b.expireAfterWrite(default)`
    // branch), which would leave the default-built cache with no expiry and keep serving the entry.
    assertTrue(
        CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE.compareTo(Duration.ZERO) > 0,
        "a mandatory default TTL must be a positive duration");

    long defaultTtlNanos = CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE.toNanos();
    var ticker = new FakeTicker();

    SubjectId subject = SubjectId.of("ttl-subject");
    byte[] ciphertext = "cipher".getBytes();
    when(delegate.decrypt(subject, ciphertext)).thenReturn("secret".getBytes());

    // Default-built engine: only the mandatory default TTL bounds retention.
    var defaultEngine = CachedCryptoEngine.builder().delegate(delegate).ticker(ticker).build();
    // Control engine with an explicit TTL much longer than the default: it must still hold the
    // entry after the default window elapses, proving the expiry above is the DEFAULT, not some
    // unconditional eviction.
    CryptoEngine controlDelegate = mock(CryptoEngine.class);
    when(controlDelegate.decrypt(subject, ciphertext)).thenReturn("secret".getBytes());
    var controlEngine =
        CachedCryptoEngine.builder()
            .delegate(controlDelegate)
            .expireAfterWrite(CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE.multipliedBy(100))
            .ticker(ticker)
            .build();

    // t0: populate both caches (one delegate call each).
    assertEquals("secret", new String(defaultEngine.decrypt(subject, ciphertext)));
    assertEquals("secret", new String(controlEngine.decrypt(subject, ciphertext)));
    verify(delegate, times(1)).decrypt(subject, ciphertext);
    verify(controlDelegate, times(1)).decrypt(subject, ciphertext);

    // Just before the default TTL elapses: still served from cache (no new delegate call).
    ticker.advance(defaultTtlNanos - 1);
    assertEquals("secret", new String(defaultEngine.decrypt(subject, ciphertext)));
    verify(delegate, times(1)).decrypt(subject, ciphertext);

    // Just after the default TTL elapses: the default-built entry has expired, so decrypt must go
    // back to the delegate (call count 2). If the mandatory default were removed, the entry would
    // still be cached here and this would stay at 1 — the assertion would fail.
    ticker.advance(2);
    assertEquals("secret", new String(defaultEngine.decrypt(subject, ciphertext)));
    verify(delegate, times(2)).decrypt(subject, ciphertext);

    // The control engine (explicit 100x TTL) still serves from cache at the same instant — proving
    // the default-built eviction was driven by the DEFAULT TTL, not by ticker advancement alone.
    assertEquals("secret", new String(controlEngine.decrypt(subject, ciphertext)));
    verify(controlDelegate, times(1)).decrypt(subject, ciphertext);
  }

  @Test
  void writeTtlCeilingBoundsRetentionEvenWithExpireAfterAccessOnly() {
    // An expireAfterAccess-only config must NOT let a continuously-accessed entry live
    // forever. A hard expireAfterWrite ceiling (DEFAULT_EXPIRE_AFTER_WRITE) is enforced regardless,
    // bounding how long a forgotten subject's decrypted plaintext can linger on a peer replica.
    //
    // Fail-first: before the ceiling was made unconditional, an access-only cache had NO write
    // bound
    // (the old `else if (expireAfterAccess == null)` branch skipped the default), so a
    // continuously-read entry survived past the default window forever and kept being served
    // (delegate call count would stay 1 at every step below).
    long ceilingNanos = CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE.toNanos();
    var ticker = new FakeTicker();
    SubjectId subject = SubjectId.of("access-only-subject");
    byte[] ciphertext = "cipher".getBytes();
    when(delegate.decrypt(subject, ciphertext)).thenReturn("secret".getBytes());

    // Access TTL far LONGER than the write ceiling, and NO explicit expireAfterWrite.
    var accessOnlyEngine =
        CachedCryptoEngine.builder()
            .delegate(delegate)
            .expireAfterAccess(CachedCryptoEngine.DEFAULT_EXPIRE_AFTER_WRITE.multipliedBy(100))
            .ticker(ticker)
            .build();

    // t0: populate the cache (one delegate call).
    assertEquals("secret", new String(accessOnlyEngine.decrypt(subject, ciphertext)));
    verify(delegate, times(1)).decrypt(subject, ciphertext);

    // Just before the write ceiling: keep reading it (so the long access-TTL never lapses). Still
    // served from cache — a read refreshes the access timer but NOT the write timer.
    ticker.advance(ceilingNanos - 1);
    assertEquals("secret", new String(accessOnlyEngine.decrypt(subject, ciphertext)));
    verify(delegate, times(1)).decrypt(subject, ciphertext);

    // Just after the write ceiling elapses (measured from the write at t0, NOT the recent access):
    // the entry must be evicted despite the fresh access, so decrypt goes back to the delegate.
    ticker.advance(2);
    assertEquals("secret", new String(accessOnlyEngine.decrypt(subject, ciphertext)));
    verify(delegate, times(2)).decrypt(subject, ciphertext);
  }

  @Test
  void deleteKeyOnOnePeerInvalidatesCachedPlaintextOnAnotherPeerViaForgetSignal() {
    // Two engines (two replicas) over one shared delegate share a
    // forget channel. B has cached decrypt(X); when A processes the GDPR erasure deleteKey(X), the
    // signal must promptly evict X from B's cache so B stops serving the forgotten plaintext
    // WITHOUT
    // waiting out the write-TTL.
    //
    // Fail-first: without the CryptoForgetSignal wiring, A's deleteKey invalidates only A's local
    // cache; B is never notified and keeps serving the cached plaintext (B's delegate call count
    // stays 1), which is exactly the fleet-ineffective-erasure defect.
    SubjectId subject = SubjectId.of("cross-replica-subject");
    byte[] ciphertext = "cipher".getBytes();

    CryptoEngine delegateA = mock(CryptoEngine.class);
    CryptoEngine delegateB = mock(CryptoEngine.class);
    when(delegateB.decrypt(subject, ciphertext)).thenReturn("alice@example.com".getBytes());

    TestForgetSignal channel = new TestForgetSignal();
    var engineA = CachedCryptoEngine.builder().delegate(delegateA).forgetSignal(channel).build();
    var engineB = CachedCryptoEngine.builder().delegate(delegateB).forgetSignal(channel).build();

    // B caches the plaintext for subject X.
    assertEquals("alice@example.com", new String(engineB.decrypt(subject, ciphertext)));
    verify(delegateB, times(1)).decrypt(subject, ciphertext);

    // A processes the erasure — its published forget signal must reach B and evict X.
    engineA.deleteKey(subject);
    verify(delegateA).deleteKey(subject);

    // B must no longer serve the cached plaintext: the next decrypt goes back to B's delegate.
    engineB.decrypt(subject, ciphertext);
    verify(delegateB, times(2)).decrypt(subject, ciphertext);
  }

  @Test
  void forgetSignalOnlyEvictsTheForgottenSubjectNotOthers() {
    // The cross-instance eviction must be scoped to the forgotten subject's opaque token — a forget
    // for one subject must never evict an unrelated subject's cached plaintext on a peer.
    byte[] ciphertext = "cipher".getBytes();
    CryptoEngine delegateA = mock(CryptoEngine.class);
    CryptoEngine delegateB = mock(CryptoEngine.class);
    when(delegateB.decrypt(SubjectId.of("keep-me"), ciphertext)).thenReturn("kept".getBytes());

    TestForgetSignal channel = new TestForgetSignal();
    var engineA = CachedCryptoEngine.builder().delegate(delegateA).forgetSignal(channel).build();
    var engineB = CachedCryptoEngine.builder().delegate(delegateB).forgetSignal(channel).build();

    engineB.decrypt(SubjectId.of("keep-me"), ciphertext);

    // A forgets a DIFFERENT subject.
    engineA.deleteKey(SubjectId.of("forget-me"));

    // B's unrelated entry survives — still served from cache (no extra delegate call).
    assertEquals("kept", new String(engineB.decrypt(SubjectId.of("keep-me"), ciphertext)));
    verify(delegateB, times(1)).decrypt(SubjectId.of("keep-me"), ciphertext);
  }

  /**
   * In-memory {@link CryptoForgetSignal} standing in for a shared cross-replica channel: {@code
   * publish} synchronously fans the token out to every subscribed listener, so both engines wired
   * to the same instance see each other's forget signals — modelling the Postgres LISTEN/NOTIFY
   * channel without a database.
   */
  private static final class TestForgetSignal implements CryptoForgetSignal {
    private final java.util.List<java.util.function.Consumer<String>> listeners =
        new java.util.concurrent.CopyOnWriteArrayList<>();

    @Override
    public void publish(String token) {
      for (var listener : listeners) {
        listener.accept(token);
      }
    }

    @Override
    public void subscribe(java.util.function.Consumer<String> listener) {
      listeners.add(listener);
    }
  }

  /** Deterministic Caffeine ticker driven by the test (no wall-clock dependency). */
  private static final class FakeTicker implements com.github.benmanes.caffeine.cache.Ticker {
    private final java.util.concurrent.atomic.AtomicLong nanos =
        new java.util.concurrent.atomic.AtomicLong();

    void advance(long deltaNanos) {
      nanos.addAndGet(deltaNanos);
    }

    @Override
    public long read() {
      return nanos.get();
    }
  }

  @Test
  void mutatingReturnedPlaintextNeverCorruptsLaterReads() {
    byte[] ciphertext = "cipher".getBytes();
    when(delegate.decrypt(SubjectId.of("subject-mut"), ciphertext)).thenReturn("secret".getBytes());

    byte[] first = engine.decrypt(SubjectId.of("subject-mut"), ciphertext);
    java.util.Arrays.fill(first, (byte) 0); // hostile/careless caller wipes its copy

    byte[] second = engine.decrypt(SubjectId.of("subject-mut"), ciphertext); // served from cache
    assertEquals("secret", new String(second), "cache must hold its own copy");

    java.util.Arrays.fill(second, (byte) 0);
    assertEquals("secret", new String(engine.decrypt(SubjectId.of("subject-mut"), ciphertext)));
    verify(delegate, times(1)).decrypt(SubjectId.of("subject-mut"), ciphertext);
  }
}
