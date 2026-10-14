package org.streamrune.runtime;

import java.util.Collection;

/**
 * Supplies a deployment's default for the forward fence ({@link
 * SagaRunner.Builder#forwardFence(boolean)}) to application-constructed {@link SagaRunner} beans.
 *
 * <p>The framework never builds a {@code SagaRunner}: applications do, and the integrations only
 * discover the beans at startup. A runner whose builder named no value is fenced from its build;
 * {@link #applyDefault} lets an integration's startup hook switch such a runner to the configured
 * default afterwards. A value the builder named is never changed.
 *
 * <p><b>Late application is safe.</b> {@link SagaRunner.Builder#build()} checks, for every runner
 * that did not switch the fence off, that its bus can fence against its saga rows, so a runner that
 * named nothing starts fenced over a bus that can fence. A default applied later can only move it
 * from the fenced value to the configured one; the runner reads the value on each forward command,
 * so the next dispatch after the hook uses it.
 *
 * <p>Lives in {@code org.streamrune.runtime} for the same reason {@link SagaEventPathRetention}
 * does: it needs {@link SagaRunner}'s package-private state, and the integration modules call a
 * small public entry point.
 */
public final class SagaForwardFenceDefault {

  private SagaForwardFenceDefault() {}

  /**
   * Applies {@code enabled} to every runner whose builder named no forward fence.
   *
   * @param sagaRunners the discovered saga runners; {@code null}/empty is a no-op
   * @param enabled the configured default
   * @return how many runners took the default (for the caller's startup log)
   */
  public static int applyDefault(Collection<? extends SagaRunner<?>> sagaRunners, boolean enabled) {
    if (sagaRunners == null || sagaRunners.isEmpty()) {
      return 0;
    }
    int applied = 0;
    for (SagaRunner<?> runner : sagaRunners) {
      if (runner != null && runner.applyForwardFenceDefault(enabled)) {
        applied++;
      }
    }
    return applied;
  }

  /**
   * The fence {@code sagaRunner} applies to its next forward command.
   *
   * @param sagaRunner the runner to inspect (required)
   */
  public static boolean effectiveOf(SagaRunner<?> sagaRunner) {
    return sagaRunner.forwardFence();
  }
}
