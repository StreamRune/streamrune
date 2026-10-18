package org.streamrune.runtime;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.streamrune.core.subscription.SubscriptionLeadership;

/**
 * A {@link SubscriptionLeadership} a test drives by hand. A name the test granted is handed out by
 * {@link #tryAcquire}; from then on {@link #current} reports it held until {@link #resign} or
 * {@link #revoke}. Every call is counted per name.
 */
class RecordingLeadership implements SubscriptionLeadership {

  private static final Lease LEASE = new Lease(1L);

  private record Block(CountDownLatch entered, CountDownLatch release) {}

  private final Set<String> grantable = ConcurrentHashMap.newKeySet();
  private final Set<String> held = ConcurrentHashMap.newKeySet();
  private final Map<String, AtomicInteger> tryAcquires = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> resigns = new ConcurrentHashMap<>();
  private final Map<String, AtomicInteger> currents = new ConcurrentHashMap<>();
  private final AtomicReference<Block> block = new AtomicReference<>();
  private final AtomicInteger resignsOnAnInterruptedThread = new AtomicInteger();

  /** The next {@link #tryAcquire} of {@code name} succeeds, and so does every later one. */
  void grant(String name) {
    grantable.add(name);
  }

  /** The lease of {@code name} is lost: not held here, and not handed out. */
  void revoke(String name) {
    grantable.remove(name);
    held.remove(name);
  }

  /**
   * The next {@link #tryAcquire}, of any name, counts {@code entered} down and waits for {@code
   * release} before it answers.
   */
  void blockInsideTryAcquire(CountDownLatch entered, CountDownLatch release) {
    block.set(new Block(entered, release));
  }

  int tryAcquires(String name) {
    return count(tryAcquires, name);
  }

  int resigns(String name) {
    return count(resigns, name);
  }

  int currents(String name) {
    return count(currents, name);
  }

  /**
   * Resigns made by a thread whose interrupt flag was set: on a virtual thread such a resign's
   * statement fails, and the lease is left to expire at its time to live.
   */
  int resignsOnAnInterruptedThread() {
    return resignsOnAnInterruptedThread.get();
  }

  @Override
  public Optional<Lease> tryAcquire(String consumerName) {
    increment(tryAcquires, consumerName);
    Block blocked = block.getAndSet(null);
    if (blocked != null) {
      blocked.entered().countDown();
      try {
        if (!blocked.release().await(20, TimeUnit.SECONDS)) {
          throw new AssertionError("the test never released the blocked tryAcquire");
        }
      } catch (InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        return Optional.empty();
      }
    }
    if (!grantable.contains(consumerName)) {
      return Optional.empty();
    }
    held.add(consumerName);
    return Optional.of(LEASE);
  }

  @Override
  public Optional<Lease> current(String consumerName) {
    increment(currents, consumerName);
    return held.contains(consumerName) ? Optional.of(LEASE) : Optional.empty();
  }

  @Override
  public void resign(String consumerName) {
    increment(resigns, consumerName);
    if (Thread.currentThread().isInterrupted()) {
      resignsOnAnInterruptedThread.incrementAndGet();
    }
    held.remove(consumerName);
  }

  @Override
  public void close() {
    held.clear();
  }

  private static void increment(Map<String, AtomicInteger> counts, String name) {
    counts.computeIfAbsent(name, n -> new AtomicInteger()).incrementAndGet();
  }

  private static int count(Map<String, AtomicInteger> counts, String name) {
    AtomicInteger count = counts.get(name);
    return count == null ? 0 : count.get();
  }
}
