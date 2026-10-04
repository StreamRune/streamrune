package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Aggregates the liveness/degradation of the background relay threads — the outbox relay ({@link
 * OutboxPoller}), the command dead-letter retry runner ({@link DeadLetterRetryRunner}), the saga
 * drivers and the four retention sweepers ({@link RetentionSweeper}) — into a single health signal.
 *
 * <p>Before this, both runners kept their virtual thread alive and logged cycle failures at ERROR,
 * while {@code consecutiveFailures()}/{@code isRunning()} were exposed but consumed by nothing: the
 * framework health indicators aggregated only the DataSource and subscription health, so {@code
 * /health} reported UP even while a relay was stalled in backoff or its thread had died outright
 * (an {@link Error} escaping the poll loop leaves {@code started=true} but the thread dead). A
 * metrics gauge sampled by the loop cannot report the loop's own death, so this health contributor
 * is the piece that catches a wholly dead relay.
 *
 * <p>Status per component:
 *
 * <ul>
 *   <li><b>DOWN</b> — started but the poll thread is no longer alive/running (died unexpectedly).
 *   <li><b>DEGRADED</b> — running but with one or more consecutive poll-cycle failures (in
 *       backoff-retry: store/broker unreachable).
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
  }

  private final List<RelayStatusSource> sources = new CopyOnWriteArrayList<>();

  /** Registers the outbox relay to be reported on. A {@code null} is ignored. */
  public void registerOutboxPoller(OutboxPoller outboxPoller) {
    if (outboxPoller == null) {
      return;
    }
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return "outbox-relay";
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
            return "dead-letter-retry";
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
   * saga drivers that strand refunds/stock when they die.
   */
  public void registerSagaCompensationRetrySweeper(SagaCompensationRetrySweeper<?> sweeper) {
    if (sweeper == null) {
      return;
    }
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return "saga-compensation-retry:"
                + sweeper.sagaTypeName()
                + (sweeper.isCompensationRetryEnabled() ? "" : ":sampling-only");
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
   * Registers a {@link SagaTimeoutRunner} to be reported on. A {@code null} is ignored. The
   * component name is {@code saga-timeout:<saga type>}, the saga type being {@link
   * SagaTimeoutRunner#sagaTypeName()} — the saga state's fully-qualified class name, unique where
   * the simple name is not. A dead timeout-runner thread ({@code isStarted() && !isAlive()}) is
   * reported DOWN, so a timed-out saga that never fires its compensation is no longer silent.
   */
  public void registerSagaTimeoutRunner(SagaTimeoutRunner<?> runner) {
    if (runner == null) {
      return;
    }
    register(
        new RelayStatusSource() {
          @Override
          public String name() {
            return "saga-timeout:" + runner.sagaTypeName();
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
      components.add(
          new ComponentHealth(
              s.name(), statusFor(started, alive, failures), started, alive, failures));
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
