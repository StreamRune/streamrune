package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.crypto.CachedCryptoEngine;
import org.streamrune.crypto.CryptoForgetSignal;
import org.streamrune.runtime.health.RelayComponentNames;

/**
 * Aggregates the liveness/degradation of the background relay threads — the outbox relay ({@link
 * OutboxPoller}), the command dead-letter retry runner ({@link DeadLetterRetryRunner}), the saga
 * drivers, the four retention sweepers ({@link RetentionSweeper}), the feed of the Server-Sent
 * Events endpoint ({@link SseEventFeed}) and the listening side of the crypto-shredding forget
 * channel ({@link CryptoForgetSignal}) — into a single health signal.
 *
 * <p>Each relay keeps its own thread alive, logs a failed cycle at ERROR and exposes {@code
 * consecutiveFailures()}/{@code isRunning()}; this contributor is what reads them. The framework
 * health indicators fold it in beside the DataSource and the subscription health, so {@code
 * /health} shows a relay stalled in backoff and one whose thread has died outright (an {@link
 * Error} escaping the poll loop leaves {@code started=true} with the thread dead). A metrics gauge
 * sampled by the loop cannot report the loop's own death, so this health contributor is the piece
 * that catches a wholly dead relay.
 *
 * <p>Status per component:
 *
 * <ul>
 *   <li><b>DOWN</b> — started but the poll thread is no longer alive/running (died unexpectedly).
 *   <li><b>DEGRADED</b> — running but with one or more consecutive poll-cycle failures (in
 *       backoff-retry: store/broker unreachable); or, for a saga timeout runner, running while a
 *       saga's deadline is not enforced because a step of that saga has not returned; or, for a
 *       saga timeout runner or compensation-retry sweeper that follows its runner's lease, running
 *       past the anchor grace in a process where no subscription ever anchored that lease; or, for
 *       the crypto forget channel, subscribed while this replica does not hear peers' forgets.
 *   <li><b>UP</b> — running with no consecutive failures, or not started (nothing to be unhealthy
 *       about).
 * </ul>
 *
 * <p>Components are registered optionally: a deployment with no outbox or no DLQ retry runner
 * simply reports on whatever is present, and an empty contributor is {@link Status#UP}. The
 * framework health indicators fold {@link #overallStatus()} into the overall health and expose
 * {@link #components()} as details.
 */
public class BackgroundRelayHealthContributor {

  /** Aggregate health of a background relay. */
  public enum Status {
    UP,
    DEGRADED,
    DOWN
  }

  /**
   * Health of a single background relay component.
   *
   * @param name a stable component name (e.g. {@code outbox-relay}, {@code dead-letter-retry})
   * @param status the component's computed status
   * @param started whether the component was started and not yet closed
   * @param alive whether the component's poll thread is currently alive/running
   * @param consecutiveFailures consecutive poll-cycle failures ({@code 0} when healthy)
   */
  public record ComponentHealth(
      String name, Status status, boolean started, boolean alive, int consecutiveFailures) {}

  /** Minimal liveness view of a background relay, so component status is unit-testable. */
  interface RelayStatusSource {
    String name();

    boolean started();

    boolean alive();

    int consecutiveFailures();

    /**
     * Whether the relay, alive and with no failed cycle, is leaving work undone that it reports
     * itself: a saga timeout runner's deadline it does not enforce because a step of that saga has
     * not returned, and a saga timeout runner's or compensation-retry sweeper's lease that no
     * subscription of this process anchored ({@code leaseUnanchored()}).
     */
    default boolean leavingWorkUndone() {
      return false;
    }

    /** The component's status from its readings; see the class javadoc. */
    default Status status(boolean started, boolean alive, int consecutiveFailures) {
      Status status = statusFor(started, alive, consecutiveFailures);
      return status == Status.UP && started && leavingWorkUndone() ? Status.DEGRADED : status;
    }
  }

  private final List<RelayStatusSource> sources = new CopyOnWriteArrayList<>();

  /** The forget channels registered so far, in order; guarded by itself. */
  private final List<CryptoForgetSignal> cryptoForgetSignals = new ArrayList<>();

  /** Registers the outbox relay to be reported on. A {@code null} is ignored. */
  public void registerOutboxPoller(OutboxPoller outboxPoller) {
    if (outboxPoller == null) {
      return;
    }
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return RelayComponentNames.OUTBOX_RELAY;
          }

          @Override
          public boolean started() {
            return outboxPoller.isStarted();
          }

          @Override
          public boolean alive() {
            return outboxPoller.isAlive();
          }

          @Override
          public int consecutiveFailures() {
            return outboxPoller.consecutiveFailures();
          }
        });
  }

  /** Registers the command DLQ retry runner to be reported on. A {@code null} is ignored. */
  public void registerDlqRetryRunner(DeadLetterRetryRunner dlqRetryRunner) {
    if (dlqRetryRunner == null) {
      return;
    }
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return RelayComponentNames.DEAD_LETTER_RETRY;
          }

          @Override
          public boolean started() {
            return dlqRetryRunner.isStarted();
          }

          @Override
          public boolean alive() {
            return dlqRetryRunner.isRunning();
          }

          @Override
          public int consecutiveFailures() {
            return dlqRetryRunner.consecutiveFailures();
          }
        });
  }

  /**
   * Registers a {@link SagaCompensationRetrySweeper} to be reported on. A {@code null} is ignored.
   * The component name is {@code saga-compensation-retry:<saga type>} — or, {@code
   * saga-compensation-retry:<saga type>:sampling-only} when the sweeper was built with {@code
   * compensationRetryEnabled(false)}, so the component name itself does not imply an active
   * re-drive driver for one that only samples the saga backlog gauges. The saga type is {@link
   * SagaCompensationRetrySweeper#sagaTypeName()}, the saga state's fully-qualified class name:
   * keyed by the simple name, two saga-state classes sharing it across packages produced one name
   * twice, and the framework health indicators — which key each component's details by {@code
   * "relay." + name} — showed only one of them, so one saga's dead driver could hide. A dead
   * sweeper thread ({@code isStarted() && !isAlive()}) is reported DOWN — the exact
   * operator-blindness this contributor was built to prevent, now applied to the business-critical
   * saga drivers that strand refunds/stock when they die. A started, alive sweeper is reported
   * DEGRADED while {@link SagaCompensationRetrySweeper#leaseUnanchored()}: it follows its runner's
   * lease and no subscription of this process has anchored it, so it re-drives nothing here.
   */
  public void registerSagaCompensationRetrySweeper(SagaCompensationRetrySweeper<?> sweeper) {
    if (sweeper == null) {
      return;
    }
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return RelayComponentNames.sagaCompensationRetry(sweeper.sagaTypeName())
                + (sweeper.isCompensationRetryEnabled()
                    ? ""
                    : RelayComponentNames.SAMPLING_ONLY_SUFFIX);
          }

          @Override
          public boolean started() {
            return sweeper.isStarted();
          }

          @Override
          public boolean alive() {
            return sweeper.isAlive();
          }

          @Override
          public int consecutiveFailures() {
            return sweeper.consecutiveFailures();
          }

          @Override
          public boolean leavingWorkUndone() {
            return sweeper.leaseUnanchored();
          }
        });
  }

  /**
   * Registers a {@link SagaTimeoutRunner} to be reported on. A {@code null} is ignored. The
   * component name is {@code saga-timeout:<saga type>}, the saga type being {@link
   * SagaTimeoutRunner#sagaTypeName()} — the saga state's fully-qualified class name, unique where
   * the simple name is not. A dead timeout-runner thread ({@code isStarted() && !isAlive()}) is
   * reported DOWN, so a timed-out saga that never fires its compensation shows in the health check.
   * A started, alive runner is reported DEGRADED while {@link
   * SagaTimeoutRunner#claimsDeferredPastTheWait()} is above zero: its latest poll waited for a step
   * of a saga past its deadline, the step was still in flight, and the claim is not made underneath
   * it — that saga's deadline is not enforced in this process until the step returns. That happens
   * only for a runner whose forward fence is off, or a timeout runner built with a step lock alone:
   * with the fence on the claim is made once the wait has passed. It is reported DEGRADED too while
   * {@link SagaTimeoutRunner#leaseUnanchored()}: it follows its runner's lease and no subscription
   * of this process has anchored it, so it claims no deadline here.
   */
  public void registerSagaTimeoutRunner(SagaTimeoutRunner<?> runner) {
    if (runner == null) {
      return;
    }
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return RelayComponentNames.sagaTimeout(runner.sagaTypeName());
          }

          @Override
          public boolean started() {
            return runner.isStarted();
          }

          @Override
          public boolean alive() {
            return runner.isAlive();
          }

          @Override
          public int consecutiveFailures() {
            return runner.consecutiveFailures();
          }

          @Override
          public boolean leavingWorkUndone() {
            return runner.claimsDeferredPastTheWait() > 0 || runner.leaseUnanchored();
          }
        });
  }

  /**
   * Registers a retention sweeper to be reported on. A {@code null} is ignored. The component name
   * is the sweeper's own {@link RetentionSweeper#name()} (e.g. {@code outbox-retention-sweeper}).
   * Until this hook existed the four retention sweepers exposed no liveness at all and nothing
   * registered them, so a sweeper thread killed by an {@link Error} (which {@link
   * ResilientPollLoop} cannot retry) was invisible: its table grew unbounded and the gauge it fed —
   * {@code streamrune.saga.faulted_backlog} is sampled only by the saga dead-letter sweeper — froze
   * at its last value while {@code /health} stayed UP. A sweeper whose retention is disabled never
   * starts a thread and reports UP; a started sweeper whose thread died reports DOWN.
   */
  public void registerRetentionSweeper(RetentionSweeper sweeper) {
    if (sweeper == null) {
      return;
    }
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return sweeper.name();
          }

          @Override
          public boolean started() {
            return sweeper.isStarted();
          }

          @Override
          public boolean alive() {
            return sweeper.isAlive();
          }

          @Override
          public int consecutiveFailures() {
            return sweeper.consecutiveFailures();
          }
        });
  }

  /**
   * Registers the feed of the Server-Sent Events endpoint to be reported on. A {@code null} is
   * ignored. The component name is {@code sse-event-feed}. A feed whose polling thread has died
   * ({@code isStarted() && !isRunning()}) is reported DOWN: the endpoint keeps admitting clients
   * and writing keepalives, and no event reaches them. A feed whose reads fail is DEGRADED.
   */
  public void registerSseEventFeed(SseEventFeed feed) {
    if (feed == null) {
      return;
    }
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return RelayComponentNames.SSE_EVENT_FEED;
          }

          @Override
          public boolean started() {
            return feed.isStarted();
          }

          @Override
          public boolean alive() {
            return feed.isRunning();
          }

          @Override
          public int consecutiveFailures() {
            return feed.consecutiveFailures();
          }
        });
  }

  /**
   * Registers the forget channel of {@code engine} to be reported on, when {@code engine} is a
   * {@link CachedCryptoEngine} whose channel has a listening side that was started ({@link
   * CryptoForgetSignal#isSubscribed()}); otherwise — a {@code null}, an engine without a plaintext
   * cache, a channel without a listening side such as {@link CryptoForgetSignal#NOOP} — nothing is
   * registered. See {@link #registerCryptoForgetSignal(CryptoForgetSignal)}.
   */
  public void registerCryptoEngine(CryptoEngine engine) {
    if (engine instanceof CachedCryptoEngine cached) {
      registerCryptoForgetSignal(cached.forgetSignal());
    }
  }

  /**
   * Registers a crypto forget channel to be reported on as {@code crypto-forget-listen}, when its
   * listening side was started ({@link CryptoForgetSignal#isSubscribed()}); a {@code null} or a
   * channel without one is ignored. The component reads {@code DEGRADED} while the channel is
   * subscribed and does not hear peers' forgets ({@link CryptoForgetSignal#isListening()} is {@code
   * false}): before its listening side first takes effect, while it reconnects, or when it cannot
   * be opened at all. Meanwhile the replica serves cached plaintext of subjects a peer may have
   * forgotten; once it hears again the engine drops that cache. It is never {@code DOWN}: the
   * replica keeps serving, bounded by the cache's write-TTL. {@code alive} reads whether it
   * listens; {@code consecutiveFailures} is always {@code 0}.
   *
   * <p>A channel is reported once: registering the same instance again — the integration's
   * registrar for an engine and the application's own call for its channel — changes nothing. A
   * second, different channel is reported as {@code crypto-forget-listen-2}, a third as {@code
   * crypto-forget-listen-3}, and so on, so that no component hides another under the same name in
   * the health details.
   */
  public void registerCryptoForgetSignal(CryptoForgetSignal signal) {
    if (signal == null || !signal.isSubscribed()) {
      return;
    }
    // Registered under the lock too, so that the components list in the order they are numbered.
    synchronized (cryptoForgetSignals) {
      for (CryptoForgetSignal registered : cryptoForgetSignals) {
        if (registered == signal) {
          return;
        }
      }
      cryptoForgetSignals.add(signal);
      String name =
          cryptoForgetSignals.size() == 1
              ? RelayComponentNames.CRYPTO_FORGET_LISTEN
              : RelayComponentNames.CRYPTO_FORGET_LISTEN + "-" + cryptoForgetSignals.size();
      registerCryptoForgetComponent(name, signal);
    }
  }

  private void registerCryptoForgetComponent(String name, CryptoForgetSignal signal) {
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return name;
          }

          @Override
          public boolean started() {
            return signal.isSubscribed();
          }

          @Override
          public boolean alive() {
            return signal.isListening();
          }

          @Override
          public int consecutiveFailures() {
            return 0;
          }

          @Override
          public Status status(boolean started, boolean alive, int consecutiveFailures) {
            return started && !alive ? Status.DEGRADED : Status.UP;
          }
        });
  }

  /** Package-private registration hook for tests and the typed register* methods above. */
  void register(RelayStatusSource source) {
    sources.add(source);
  }

  /** Per-component health for every registered background relay. */
  public List<ComponentHealth> components() {
    var components = new ArrayList<ComponentHealth>(sources.size());
    for (RelayStatusSource s : sources) {
      boolean started = s.started();
      boolean alive = s.alive();
      int failures = s.consecutiveFailures();
      Status status = s.status(started, alive, failures);
      components.add(new ComponentHealth(s.name(), status, started, alive, failures));
    }
    return components;
  }

  /**
   * Maps a component's raw liveness into a {@link Status}: DOWN if started-but-not-alive (dead poll
   * thread), DEGRADED if started-and-alive with consecutive failures, else UP.
   */
  static Status statusFor(boolean started, boolean alive, int consecutiveFailures) {
    if (started && !alive) {
      return Status.DOWN;
    }
    if (started && consecutiveFailures > 0) {
      return Status.DEGRADED;
    }
    return Status.UP;
  }

  /**
   * Worst status across all registered components: {@link Status#DOWN} if any is down, else {@link
   * Status#DEGRADED} if any is degraded, else {@link Status#UP} (including when nothing is
   * registered).
   */
  public Status overallStatus() {
    Status worst = Status.UP;
    for (ComponentHealth c : components()) {
      if (c.status() == Status.DOWN) {
        return Status.DOWN;
      }
      if (c.status() == Status.DEGRADED) {
        worst = Status.DEGRADED;
      }
    }
    return worst;
  }
}
