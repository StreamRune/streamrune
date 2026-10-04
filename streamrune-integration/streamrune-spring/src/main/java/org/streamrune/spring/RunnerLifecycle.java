package org.streamrune.spring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Adapts a StreamRune background runner (projection runner, outbox poller, dead-letter retry
 * runner) to Spring's {@link SmartLifecycle} so the runner is started when the application context
 * refreshes and stopped when it closes.
 *
 * <p>Without this adapter the auto-configured runners are created but never started, so
 * projections, the outbox relay, and dead-letter retries silently do nothing.
 *
 * <p>Stop ordering: all {@code SmartLifecycle} beans are stopped before singleton destruction, so
 * runners always shut down before the {@code DataSource} they poll is closed. The {@link #PHASE}
 * additionally places runners before the web server in the start order, which means they stop
 * <em>after</em> graceful web shutdown completes — in-flight HTTP work can still observe projection
 * progress during shutdown.
 */
public final class RunnerLifecycle implements SmartLifecycle {

  /**
   * Runs before Spring Boot's web server lifecycles (graceful shutdown uses {@code DEFAULT_PHASE -
   * 1024}, start/stop uses {@code DEFAULT_PHASE}): runners start before HTTP traffic is accepted
   * and stop after it has drained.
   */
  public static final int PHASE = SmartLifecycle.DEFAULT_PHASE - 2048;

  private static final Logger logger = LoggerFactory.getLogger(RunnerLifecycle.class);

  private final String runnerName;
  private final Runnable startAction;
  private final Runnable stopAction;
  private volatile boolean running;

  /**
   * Creates a lifecycle adapter.
   *
   * @param runnerName human-readable runner name used in log messages (required)
   * @param startAction invoked once on context refresh (required)
   * @param stopAction invoked once on context close, before bean destruction (required)
   */
  public RunnerLifecycle(String runnerName, Runnable startAction, Runnable stopAction) {
    if (runnerName == null || runnerName.isBlank()) {
      throw new IllegalArgumentException("runnerName is required");
    }
    if (startAction == null) {
      throw new IllegalArgumentException("startAction is required");
    }
    if (stopAction == null) {
      throw new IllegalArgumentException("stopAction is required");
    }
    this.runnerName = runnerName;
    this.startAction = startAction;
    this.stopAction = stopAction;
  }

  @Override
  public void start() {
    if (running) {
      return;
    }
    logger.info("Starting {}", runnerName);
    startAction.run();
    running = true;
  }

  @Override
  public void stop() {
    if (!running) {
      return;
    }
    logger.info("Stopping {}", runnerName);
    running = false;
    stopAction.run();
  }

  @Override
  public boolean isRunning() {
    return running;
  }

  @Override
  public int getPhase() {
    return PHASE;
  }
}
