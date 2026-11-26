package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.types.SubjectId;

/**
 * A cache-miss decrypt waits a bounded time for its stripe: while a {@code deleteKey} of a subject
 * of the same stripe holds it (for example waiting for a database lock the decrypting thread
 * holds), the decrypt goes to the delegate without the stripe after the bound and caches nothing,
 * so it can neither stall the erasure nor leave plaintext behind it.
 */
class CachedCryptoEngineStripeWaitTest {

  private static final byte[] CIPHERTEXT = "cipher".getBytes();
  private static final byte[] PLAINTEXT = "secret".getBytes();

  /** A delegate whose deleteKey blocks until released, counting its decrypts. */
  private static final class BlockingDelegate implements CryptoEngine {
    final CountDownLatch insideDeleteKey = new CountDownLatch(1);
    final CountDownLatch releaseDeleteKey = new CountDownLatch(1);
    final AtomicInteger decrypts = new AtomicInteger();

    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      decrypts.incrementAndGet();
      return PLAINTEXT.clone();
    }

    @Override
    public void deleteKey(SubjectId subjectId) {
      insideDeleteKey.countDown();
      try {
        releaseDeleteKey.await(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }
  }

  /** A subject of the same stripe as {@code subject}, other than it. */
  private static SubjectId sameStripeAs(SubjectId subject) {
    int stripe = Math.floorMod(subject.hashCode(), 64);
    for (int i = 0; ; i++) {
      SubjectId candidate = SubjectId.of("neighbour-" + i);
      if (Math.floorMod(candidate.hashCode(), 64) == stripe && !candidate.equals(subject)) {
        return candidate;
      }
    }
  }

  @Test
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void aMissWhoseStripeAnErasureHoldsDecryptsUncachedAfterTheBound() throws Exception {
    var delegate = new BlockingDelegate();
    var engine = CachedCryptoEngine.builder().delegate(delegate).build();
    SubjectId erased = SubjectId.of("erased");
    SubjectId reader = sameStripeAs(erased);
    Thread erasure = Thread.ofPlatform().start(() -> engine.deleteKey(erased));
    assertTrue(delegate.insideDeleteKey.await(10, TimeUnit.SECONDS));

    long start = System.nanoTime();
    byte[] first = engine.decrypt(reader, CIPHERTEXT);
    long waitedMillis = (System.nanoTime() - start) / 1_000_000;
    byte[] second = engine.decrypt(reader, CIPHERTEXT);

    assertArrayEquals(PLAINTEXT, first);
    assertArrayEquals(PLAINTEXT, second);
    assertTrue(
        waitedMillis >= CachedCryptoEngine.MISS_STRIPE_WAIT_MILLIS - 20 && waitedMillis < 5_000,
        "waited " + waitedMillis + " ms");
    assertEquals(2, delegate.decrypts.get(), "nothing was cached while the stripe was held");

    delegate.releaseDeleteKey.countDown();
    erasure.join();
    engine.decrypt(reader, CIPHERTEXT);
    engine.decrypt(reader, CIPHERTEXT);
    assertEquals(3, delegate.decrypts.get(), "with the stripe free a miss caches again");
  }

  @Test
  @Timeout(value = 30, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  void anInterruptedWaitDecryptsUncachedAndKeepsTheInterrupt() throws Exception {
    var delegate = new BlockingDelegate();
    var engine = CachedCryptoEngine.builder().delegate(delegate).build();
    SubjectId erased = SubjectId.of("erased");
    SubjectId reader = sameStripeAs(erased);
    Thread erasure = Thread.ofPlatform().start(() -> engine.deleteKey(erased));
    assertTrue(delegate.insideDeleteKey.await(10, TimeUnit.SECONDS));
    try {
      Thread.currentThread().interrupt();
      assertArrayEquals(PLAINTEXT, engine.decrypt(reader, CIPHERTEXT));
      assertTrue(Thread.interrupted(), "the interrupt status is kept");
      engine.decrypt(reader, CIPHERTEXT);
      assertTrue(delegate.decrypts.get() >= 2, "the interrupted miss cached nothing");
    } finally {
      delegate.releaseDeleteKey.countDown();
      erasure.join();
    }
  }
}
