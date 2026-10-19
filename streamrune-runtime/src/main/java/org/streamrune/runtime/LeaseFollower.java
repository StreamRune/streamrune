package org.streamrune.runtime;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;
import org.slf4j.Logger;

/**
 * The follower side of a {@link SubscriptionLease}, for a saga's timeout runner and its
 * compensation-retry sweeper: whether the component may act on this cycle, and the two lines and
 * the health reading for a lease no subscription of this process ever anchored.
 *
 * <p>The {@code INFO} line is written on the second poll in a row that finds no anchor, one poll
 * interval of the follower's own after the first: a subscription that starts a moment after the
 * follower (the usual order in an application whose timeout runner starts first) opens its anchor
 * before that and costs no line. The {@code WARN} and the {@code DEGRADED} reading come once the
 * grace has passed, unchanged.
 */
final class LeaseFollower {

  /**
   * How long after {@code start()} a missing anchor is expected (a subscription that starts later):
   * the default lease TTL.
   */
  static final Duration DEFAULT_ANCHOR_GRACE = Duration.ofSeconds(15);

  private final SubscriptionLease lease;
  private final Logger log;
  private final String component; // "timeout runner" | "compensation-retry sweeper"
  private final String sagaType;
  private final String duty; // "deadlines are claimed" | "compensation retries are run"
  private final Duration anchorGrace;
  private final LongSupplier nanoTime;
  private final AtomicBoolean infoLogged = new AtomicBoolean();
  private final AtomicBoolean warnLogged = new AtomicBoolean();
  // Counts every poll since the start: before an anchor first opens each poll is unanchored, and
  // the branch that counts is never entered again once one has opened (everAnchored is permanent).
  private final AtomicInteger unanchoredPolls = new AtomicInteger();
  private volatile long startedAtNanos;
  private volatile boolean unanchoredPastGrace;

  LeaseFollower(
      SubscriptionLease lease,
      Logger log,
      String component,
      String sagaType,
      String duty,
      Duration anchorGrace,
      LongSupplier nanoTime) {
    this.lease = Objects.requireNonNull(lease, "lease is required");
    this.log = Objects.requireNonNull(log, "log is required");
    this.component = Objects.requireNonNull(component, "component is required");
    this.sagaType = Objects.requireNonNull(sagaType, "sagaType is required");
    this.duty = Objects.requireNonNull(duty, "duty is required");
    this.anchorGrace = Objects.requireNonNull(anchorGrace, "anchorGrace is required");
    if (anchorGrace.isNegative()) {
      throw new IllegalArgumentException("anchorGrace must not be negative, was " + anchorGrace);
    }
    this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime is required");
    this.startedAtNanos =
        nanoTime.getAsLong(); // a pollOnce() driven without start() counts from here
  }

  /** A new start: the grace counts from now, and each line may be written once again. */
  void started() {
    startedAtNanos = nanoTime.getAsLong();
    unanchoredPolls.set(0);
    infoLogged.set(false);
    warnLogged.set(false);
    unanchoredPastGrace = false;
  }

  /**
   * Whether the component acts on this cycle: always for an ungated lease, else while this process
   * leads.
   */
  boolean mayAct() {
    if (lease.isLeadingHere()) {
      unanchoredPastGrace = false;
      return true;
    }
    if (lease.everAnchored()) {
      unanchoredPastGrace = false; // a standby, or an anchor that left: nothing to say
      return false;
    }
    long elapsed = nanoTime.getAsLong() - startedAtNanos;
    if (elapsed < anchorGrace.toNanos()) {
      // The second unanchored poll since the start, not the first: see the class documentation.
      if (unanchoredPolls.incrementAndGet() >= 2 && infoLogged.compareAndSet(false, true)) {
        log.info(
            "Saga type {}: no subscription of this saga type is anchored in this process; {} by"
                + " the process that delivers its events",
            sagaType,
            duty);
      }
    } else {
      unanchoredPastGrace = true;
      if (warnLogged.compareAndSet(false, true)) {
        log.warn(
            "Saga type {}: no subscription of this saga type has anchored its lease '{}' in this"
                + " process since this {} started {} ago, so this {} acts for it only where its"
                + " events are delivered. Build the saga subscription with .sagaRunner(runner); a"
                + " process meant to run the {} without the saga's subscription builds it from a"
                + " runner without leadership (a timeout runner: stepLock(new SagaStepLock())).",
            sagaType,
            lease.name(),
            component,
            Duration.ofNanos(elapsed),
            component,
            component);
      }
    }
    return false;
  }

  /** Whether the latest {@link #mayAct()} found no anchor ever opened, past the grace. */
  boolean unanchoredPastGrace() {
    return unanchoredPastGrace;
  }
}
