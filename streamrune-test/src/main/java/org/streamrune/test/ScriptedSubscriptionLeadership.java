package org.streamrune.test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import org.streamrune.core.subscription.SubscriptionLeadership;

/**
 * A {@link SubscriptionLeadership} the test scripts: it hands out a lease only after {@link
 * #grant()}, takes every lease away on {@link #revoke()}, and counts what the code under test asked
 * for. Use it to drive a projection runner, a subscription or a saga runner through a failover
 * without a database:
 *
 * <pre>{@code
 * var leadership = new ScriptedSubscriptionLeadership().grant();
 * // ... build the runner with .leadership(leadership) and start it: it leads ...
 * leadership.revoke();   // another replica took the lease: the runner stands by
 * leadership.grant();    // the lease is free again: the runner takes it, with a higher epoch
 * assertEquals(2, leadership.acquisitions());
 * }</pre>
 *
 * <p>It starts denying: {@link #tryAcquire} answers empty for every name until {@link #grant()}.
 * Once granted, a name is acquired with a fresh {@link Lease#epoch() epoch}, strictly greater than
 * every epoch handed out before, and a {@code tryAcquire} of a name this process holds renews it
 * with the same lease. {@link #deny()} stops handing out leases and leaves the held ones held; a
 * held lease is renewed under it as well. {@link #revoke()} drops every held lease — the next
 * {@link #current} of each name reports none — and denies, as when another replica takes over.
 * {@link #resign} and {@link #close()} release what this process holds. The scripting calls apply
 * to every consumer name.
 *
 * <p>Thread-safe: runners call it from their own threads.
 */
public class ScriptedSubscriptionLeadership implements SubscriptionLeadership {

  private final ReentrantLock lock = new ReentrantLock();
  private final Map<String, Lease> held = new HashMap<>();
  private boolean granting;
  private long lastEpoch;
  private int attempts;
  private int acquisitions;
  private int resignations;

  /** Creates a leadership that denies every lease until {@link #grant()}. */
  public ScriptedSubscriptionLeadership() {}

  /**
   * From now on {@link #tryAcquire} hands out a lease to every name it is asked for.
   *
   * @return this leadership
   */
  public ScriptedSubscriptionLeadership grant() {
    lock.lock();
    try {
      granting = true;
      return this;
    } finally {
      lock.unlock();
    }
  }

  /**
   * From now on {@link #tryAcquire} hands out no new lease; the leases this process holds stay held
   * and are renewed.
   *
   * @return this leadership
   */
  public ScriptedSubscriptionLeadership deny() {
    lock.lock();
    try {
      granting = false;
      return this;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Every lease this process holds is lost — the next {@link #current} of each name reports none —
   * and no new lease is handed out until {@link #grant()}.
   *
   * @return this leadership
   */
  public ScriptedSubscriptionLeadership revoke() {
    lock.lock();
    try {
      held.clear();
      granting = false;
      return this;
    } finally {
      lock.unlock();
    }
  }

  /**
   * The {@link #tryAcquire} calls made so far, whatever they answered.
   *
   * @return the number of calls
   */
  public int attempts() {
    lock.lock();
    try {
      return attempts;
    } finally {
      lock.unlock();
    }
  }

  /**
   * The {@link #tryAcquire} calls answered with a lease so far, renewals included.
   *
   * @return the number of calls
   */
  public int acquisitions() {
    lock.lock();
    try {
      return acquisitions;
    } finally {
      lock.unlock();
    }
  }

  /**
   * The {@link #resign} calls made so far, for a held name or not.
   *
   * @return the number of calls
   */
  public int resignations() {
    lock.lock();
    try {
      return resignations;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<Lease> tryAcquire(String consumerName) {
    Objects.requireNonNull(consumerName, "consumerName is required");
    lock.lock();
    try {
      attempts++;
      Lease lease = held.get(consumerName);
      if (lease == null && granting) {
        lease = new Lease(++lastEpoch);
        held.put(consumerName, lease);
      }
      if (lease == null) {
        return Optional.empty();
      }
      acquisitions++;
      return Optional.of(lease);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Optional<Lease> current(String consumerName) {
    Objects.requireNonNull(consumerName, "consumerName is required");
    lock.lock();
    try {
      return Optional.ofNullable(held.get(consumerName));
    } finally {
      lock.unlock();
    }
  }

  @Override
  public void resign(String consumerName) {
    Objects.requireNonNull(consumerName, "consumerName is required");
    lock.lock();
    try {
      resignations++;
      held.remove(consumerName);
    } finally {
      lock.unlock();
    }
  }

  /** Releases every lease this process holds; the scripting and the counts stay. */
  @Override
  public void close() {
    lock.lock();
    try {
      held.clear();
    } finally {
      lock.unlock();
    }
  }
}
