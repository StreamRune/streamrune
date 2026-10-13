package org.streamrune.runtime;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.streamrune.core.saga.SagaStore;

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
 * modules discover the beans. They call {@link #requireOneRunnerPerSagaType} at startup, in the
 * step that validates the configuration, ahead of the runners, sweepers and subscriptions the
 * integration starts itself; the application does not start. A subscription that an application
 * starts on its own while its beans are created is already polling when the refusal stops the
 * startup. A hand-wired application calls the method itself, or keeps the rule: a saga state class
 * belongs to one saga. Two runners of one saga type also have two step locks ({@link
 * SagaStepLock}): the steps of one are not excluded from the other's, and a timeout runner built
 * from one of them does not wait for the other's deliveries.
 *
 * <p><b>The same store.</b> Two runners share their rows when their stores are one instance, or
 * when the two instances say they keep the same rows ({@link SagaStore#storageIdentity()}): two
 * {@code PostgresSagaStore}s over one {@code DataSource} are one store, whatever mapper each has.
 * Two runners of one saga type over stores with <em>different</em> identities are not refused. The
 * identity is what a store answers without I/O, so it has a limit: two {@code DataSource} objects
 * configured for one database, and a wrapper that does not forward the identity of the store it
 * wraps, are two stores to this check although their runners share rows.
 *
 * <p>Lives in {@code org.streamrune.runtime} for the same reason {@link SagaStateCryptoValidation}
 * does: it reads {@link SagaRunner}'s package-private state.
 */
public final class SagaTypeOwnership {

  private SagaTypeOwnership() {}

  /**
   * Refuses two runners that drive the same saga type over the same {@code SagaStore}: one
   * instance, or two with equal {@link SagaStore#storageIdentity() storage identities}. The same
   * runner listed twice is one runner.
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
            && keepTheSameRows(earlier.sagaStore(), runner.sagaStore())) {
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

  /** Whether two stores are one instance, or say they keep the same rows. */
  static boolean keepTheSameRows(SagaStore first, SagaStore second) {
    return first == second || storageOf(first).equals(storageOf(second));
  }

  /** A store's storage identity; the store itself for one that answers {@code null}. */
  private static Object storageOf(SagaStore store) {
    Object identity = store.storageIdentity();
    return identity != null ? identity : store;
  }
}
