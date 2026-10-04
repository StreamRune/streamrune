package org.streamrune.runtime;

import java.time.Duration;
import java.util.Collection;

/**
 * Supplies {@code streamrune.inbox.retention-max-age} to the <b>event path's</b> key-age guard on
 * application-constructed {@link SagaRunner} beans.
 *
 * <p><b>Why this exists.</b> The framework never builds a {@code SagaRunner}: applications do, and
 * the three auto-configurations only discover the beans. Every OTHER consumer of the inbox
 * retention window is framework-built and therefore configured automatically — {@code
 * SagaCompensationRetrySweeper} by each integration's {@code SagaCompensationRetryLifecycle} — or
 * documented as the application's job on its own builder ({@code SagaTimeoutRunner}, {@code
 * SagaDeadLetterReplayer}). The event path had neither: a deployment that set the property got the
 * guard on the sweeper and nowhere near the event path, which is the one re-driver with <b>no
 * leadership gate</b> — it runs on every replica and is the likeliest of the four to fire. A fix
 * that does not reach the default wiring is not a fix, so the auto-configurations now push the
 * property in here.
 *
 * <p><b>The application still wins.</b> {@link #applyDefault} only touches a runner whose builder
 * never named {@link SagaRunner.Builder#inboxRetentionMaxAge(Duration)}, so an explicit value —
 * including a deliberate zero/negative "leave this guard off" — is never overwritten.
 *
 * <p><b>Late application is safe.</b> The window is read on every ForwardStep resume through a
 * {@code volatile} field, and an unset window leaves the guard inert (the behaviour without a
 * retention window). A delivery that raced the startup hook therefore takes the OLD decision, never
 * a wrong new one, and every subsequent delivery sees the configured window.
 *
 * <p>Lives in {@code org.streamrune.runtime} for the same reason {@link SagaStateCryptoValidation}
 * does: it needs {@link SagaRunner}'s package-private state, and the three integration modules call
 * a small public entry point.
 */
public final class SagaEventPathRetention {

  private SagaEventPathRetention() {}

  /**
   * Applies {@code inboxRetentionMaxAge} as the event-path key-age bound on every runner that did
   * not set one itself.
   *
   * @param sagaRunners the discovered saga runners; {@code null}/empty is a no-op
   * @param inboxRetentionMaxAge the configured {@code streamrune.inbox.retention-max-age}; {@code
   *     null}, zero or negative means the inbox sweeper is disabled and the guard stays inert
   * @return how many runners took the default (for the caller's startup log)
   */
  public static int applyDefault(
      Collection<? extends SagaRunner<?>> sagaRunners, Duration inboxRetentionMaxAge) {
    if (sagaRunners == null || sagaRunners.isEmpty()) {
      return 0;
    }
    int applied = 0;
    for (SagaRunner<?> runner : sagaRunners) {
      if (runner != null && runner.applyInboxRetentionMaxAgeDefault(inboxRetentionMaxAge)) {
        applied++;
      }
    }
    return applied;
  }

  /**
   * The window {@code runner}'s event path will actually enforce, or {@code null} when the key-age
   * guard is inert on it. Zero/negative inputs are normalized to {@code null} here, exactly as the
   * three out-of-band re-drivers normalize theirs.
   *
   * @param sagaRunner the runner to inspect (required)
   */
  public static Duration effectiveWindowOf(SagaRunner<?> sagaRunner) {
    return sagaRunner.inboxRetentionMaxAge();
  }
}
