package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Startup guard: a saga type has one {@link SagaRunner} over a saga store.
 *
 * <p><b>Why this exists.</b> A saga's type is its state class ({@code SagaType.fromClass}). The
 * saga store keeps rows by saga type and saga id, and a runner routes the reply to a saga's command
 * by the saga type and saga id the command bus wrote into the reply ({@code
 * EventMetadata.sagaOrigin()}). Two runners built over the same state class and the same store are
 * therefore one saga type with two sets of logic: both load and write the same rows, each resolves
 * the replies to the other's commands to that row, and whichever subscription is ahead applies its
 * own {@code evolve} to it — the other then skips the event as a redelivery. No routing method can
 * tell the two apart, because the reply does not reach one.
 *
 * <p>The framework never builds a {@code SagaRunner}; applications do, and the three integration
 * modules discover the beans. They call {@link #requireOneRunnerPerSagaType} at startup, before a
 * subscription delivers an event. A hand-wired application calls it itself, or keeps the rule: a
 * saga state class belongs to one saga.
 *
 * <p>Two runners of one saga type over <em>different</em> store instances are not refused: they
 * share no row. The check compares store instances, so two wrappers around one underlying store are
 * beyond it.
 *
 * <p>Lives in {@code org.streamrune.runtime} for the same reason {@link SagaStateCryptoValidation}
 * does: it reads {@link SagaRunner}'s package-private state.
 */
public final class SagaTypeOwnership {

  private SagaTypeOwnership() {}

  /**
   * Refuses two runners that drive the same saga type over the same {@code SagaStore} instance. The
   * same runner listed twice is one runner.
   *
   * @param sagaRunners the discovered saga runners; {@code null}/empty is a no-op, {@code null}
   *     elements are skipped
   * @throws IllegalStateException for the first such pair, naming the saga type and what each of
   *     the two runners was built from
   */
  public static void requireOneRunnerPerSagaType(Collection<? extends SagaRunner<?>> sagaRunners) {
    if (sagaRunners == null || sagaRunners.isEmpty()) {
      return;
    }
    List<SagaRunner<?>> accepted = new ArrayList<>();
    for (SagaRunner<?> runner : sagaRunners) {
      if (runner == null || accepted.contains(runner)) {
        continue;
      }
      for (SagaRunner<?> earlier : accepted) {
        if (earlier.sagaType().equals(runner.sagaType())
            && earlier.sagaStore() == runner.sagaStore()) {
          throw new IllegalStateException(
              "Two SagaRunner instances drive the saga type "
                  + runner.sagaType().value()
                  + " over the same SagaStore: one is built from "
                  + earlier.sagaLogicName()
                  + ", the other from "
                  + runner.sagaLogicName()
                  + ". A saga state class belongs to one saga: both runners load and write the"
                  + " same saga rows, and each takes the replies to the other's commands for its"
                  + " own, because a reply names its saga by the state class and the saga id."
                  + " Give each saga a state class of its own, or register one runner for it.");
        }
      }
      accepted.add(runner);
    }
  }
}
