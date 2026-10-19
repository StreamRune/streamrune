package org.streamrune.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionLeadership;

/**
 * A named lease over a {@link SubscriptionLeadership}, shared by the components of one process that
 * must lead together: a saga type's subscription, its timeout runner and its compensation-retry
 * sweeper ({@link SagaRunner#lease()}).
 *
 * <p>One component is the <b>anchor</b>: the subscription, through {@link #anchor()}. It alone
 * acquires and resigns the lease. The others are <b>followers</b>: they act while {@link
 * #isLeadingHere()} — an anchor is open in this process and the lease is held — and never acquire
 * or resign. A follower's stop therefore never drops the lease, and the lease is resigned exactly
 * once, when the anchor closes. A process with no anchor never leads, however many followers it
 * runs; a follower there acts nowhere.
 *
 * <p>One anchor at a time per lease object: {@link #anchor()} refuses a second one while the first
 * is open. Leadership decides nothing about correctness: the saga row's compare-and-swap, the
 * command inbox and the forward fence hold during every handover overlap, with or without a lease.
 *
 * <p>A lease over {@link SubscriptionLeadership#NOOP}, and {@link #ungated()}, is <em>ungated</em>:
 * it leads everywhere, has no anchor and issues no statement.
 *
 * <p>The anchor writes one {@code INFO} line each time this process takes the lease ({@code ... now
 * leads}) and each time a check finds it gone ({@code ... no longer leads}), naming the lease; a
 * standby's polls, a leader's steady polls and a deliberate {@linkplain Anchor#close() close} write
 * none. Logger: {@code org.streamrune.runtime.SubscriptionLease}.
 */
public final class SubscriptionLease {

  private static final Logger log = LoggerFactory.getLogger(SubscriptionLease.class);

  private static final SubscriptionLease UNGATED =
      new SubscriptionLease(SubscriptionLeadership.NOOP, "ungated", System::nanoTime);

  private final SubscriptionLeadership leadership;
  private final String name;
  private final boolean gated;
  private final LongSupplier nanoTime;
  // Guards openAnchor, everAnchored and each anchor's closed flag; never held across a call to the
  // leadership.
  private final ReentrantLock lock = new ReentrantLock();
  private volatile Anchor openAnchor;
  private volatile boolean everAnchored;

  private SubscriptionLease(SubscriptionLeadership leadership, String name, LongSupplier nanoTime) {
    this.leadership = leadership;
    this.name = name;
    this.gated = leadership != SubscriptionLeadership.NOOP;
    this.nanoTime = nanoTime;
  }

  /**
   * A lease named {@code name} over {@code leadership}; ungated when {@code leadership} is {@code
   * NOOP}.
   */
  public static SubscriptionLease of(SubscriptionLeadership leadership, String name) {
    return of(leadership, name, System::nanoTime);
  }

  /**
   * As {@link #of(SubscriptionLeadership, String)}, with the clock of the attempt throttle; for
   * tests.
   */
  static SubscriptionLease of(
      SubscriptionLeadership leadership, String name, LongSupplier nanoTime) {
    Objects.requireNonNull(leadership, "leadership is required");
    Objects.requireNonNull(name, "name is required");
    if (name.isBlank()) {
      throw new IllegalArgumentException("name must not be blank");
    }
    return new SubscriptionLease(
        leadership, name, Objects.requireNonNull(nanoTime, "nanoTime is required"));
  }

  /** The lease of a subscription that takes none: it delivers on every replica. */
  public static SubscriptionLease ungated() {
    return UNGATED;
  }

  /** The consumer name the lease is held under. */
  public String name() {
    return name;
  }

  /**
   * Whether this lease takes a real leadership: {@code false} over {@link
   * SubscriptionLeadership#NOOP}.
   */
  public boolean isGated() {
    return gated;
  }

  /**
   * Opens the one anchor of this lease in this process.
   *
   * @throws IllegalStateException when the lease is ungated, or while another anchor of it is open
   */
  public Anchor anchor() {
    if (!gated) {
      throw new IllegalStateException(
          "The lease '"
              + name
              + "' is ungated and has no anchor: a subscription without a lease"
              + " delivers without one");
    }
    lock.lock();
    try {
      if (openAnchor != null) {
        throw new IllegalStateException(
            "The lease '"
                + name
                + "' already has a delivering subscription in this process: build"
                + " one subscription per lease (one per saga type for a saga runner's lease)");
      }
      Anchor anchor = new Anchor();
      openAnchor = anchor;
      everAnchored = true;
      return anchor;
    } finally {
      lock.unlock();
    }
  }

  /**
   * Whether this process leads: always for an ungated lease; for a gated one, while an anchor is
   * open and the leadership reports the lease held. A local read: no statement.
   */
  public boolean isLeadingHere() {
    if (!gated) {
      return true;
    }
    return openAnchor != null && leadership.current(name).isPresent();
  }

  /**
   * {@link LeaseRole#UNGATED}, or {@link LeaseRole#LEADING} while {@link #isLeadingHere()}, else
   * {@link LeaseRole#STANDBY}.
   */
  public LeaseRole role() {
    if (!gated) {
      return LeaseRole.UNGATED;
    }
    return isLeadingHere() ? LeaseRole.LEADING : LeaseRole.STANDBY;
  }

  /** Whether an anchor of this lease has been opened in this process since the lease was built. */
  public boolean everAnchored() {
    return everAnchored;
  }

  /**
   * The anchor of a gated lease: the one component that acquires and resigns it. {@link #lead} and
   * {@link #stillLeads} are called by one thread at a time (the subscription's poll serialisation);
   * {@link #close} by any thread.
   */
  public final class Anchor implements AutoCloseable {
    private volatile boolean closed;
    // The role this anchor last reported in the log: true from the line "now leads" until the line
    // "no longer leads" (or its close). Written by the polling thread, reset by close().
    private volatile boolean leading;
    private boolean attempted;
    private long lastAttemptNanos;

    private Anchor() {}

    /**
     * Whether this process leads now, acquiring the lease when it does not: {@code true} at once
     * while the lease is held (no statement). Otherwise one {@code tryAcquire} — unless the
     * previous attempt of this anchor is younger than {@code minAttemptInterval}, then {@code
     * false} with no statement. A grant that arrives after {@link #close()} ran is given back.
     * {@code false} after {@link #close()}.
     */
    public boolean lead(Duration minAttemptInterval) {
      Objects.requireNonNull(minAttemptInterval, "minAttemptInterval is required");
      if (closed) {
        return false;
      }
      if (leadership.current(name).isPresent()) {
        tookLease();
        return true;
      }
      lostLease();
      long now = nanoTime.getAsLong();
      if (attempted && now - lastAttemptNanos < minAttemptInterval.toNanos()) {
        return false;
      }
      attempted = true;
      lastAttemptNanos = now;
      if (leadership.tryAcquire(name).isEmpty()) {
        return false;
      }
      lock.lock();
      try {
        if (!closed) {
          tookLease();
          return true;
        }
        if (openAnchor != null) {
          return false; // a newer anchor of this lease owns the name now: leave the grant to it
        }
      } finally {
        lock.unlock();
      }
      leadership.resign(name); // close() ran while the acquire was in flight
      return false;
    }

    /** Whether this anchor is open and the lease is still held; a local read, no statement. */
    public boolean stillLeads() {
      if (closed) {
        return false;
      }
      if (leadership.current(name).isPresent()) {
        return true;
      }
      lostLease();
      return false;
    }

    /** The one line per STANDBY to LEADING change. */
    private void tookLease() {
      if (!leading && !closed) {
        leading = true;
        log.info("Lease '{}': this process now leads it", name);
      }
    }

    /** The one line per LEADING to STANDBY change, found by a check; none for a close. */
    private void lostLease() {
      if (leading) {
        leading = false;
        log.info(
            "Lease '{}': this process no longer leads it (the lease is gone; it stands by until it"
                + " takes it again)",
            name);
      }
    }

    /** Closes this anchor and resigns the lease; idempotent. The resign is no-throw by contract. */
    @Override
    public void close() {
      lock.lock();
      try {
        if (closed) {
          return;
        }
        closed = true;
        leading = false;
        if (openAnchor == this) {
          openAnchor = null;
        }
      } finally {
        lock.unlock();
      }
      leadership.resign(name);
    }

    /** Whether {@link #close()} has run; for tests. */
    boolean isClosed() {
      return closed;
    }
  }
}
