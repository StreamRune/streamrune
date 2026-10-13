package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaStepLockFixture.onAnotherThread;
import static org.streamrune.runtime.SagaStepLockFixture.outcomeOf;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.RepetitionInfo;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.Command;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.runtime.SagaStepLockFixture.Advanced;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.Reserved;
import org.streamrune.runtime.SagaStepLockFixture.Ship;
import org.streamrune.runtime.SagaStepLockFixture.Shipped;

/**
 * One process under load: several threads deliver every event of many sagas (so each event is
 * delivered more than once, at the same time), forward commands fail from time to time with a
 * retry-later failure and are delivered again, and the timeout runner polls without pause against a
 * clock that puts every saga past its deadline. Whatever the interleaving:
 *
 * <ul>
 *   <li>no forward command of a saga is stored after that saga's first compensation command;
 *   <li>no delivery loses a compare-and-swap on the saga row;
 *   <li>each compensation command of a saga is stored once;
 *   <li>when everything has returned, no saga has an entry in the step lock.
 * </ul>
 *
 * <p>The repetition number seeds every choice the test makes: the order in which each thread walks
 * the sagas, where a thread yields before a command, which forward commands fail. The scheduler's
 * part of the interleaving is not seeded, which is why the property is asserted over ten seeds and
 * many sagas and not over one schedule.
 */
class SagaStepLockStressTest {

  private static final int SAGAS = 40;
  private static final int DELIVERING_THREADS = 6;

  /** The seeded choices of the calling thread; set by each thread the test starts. */
  private static final ThreadLocal<Random> CHOICES = new ThreadLocal<>();

  @RepeatedTest(10)
  @Timeout(value = 120, unit = TimeUnit.SECONDS)
  void noForwardCommandOfASagaIsStoredAfterThatSagasFirstCompensationCommand(
      RepetitionInfo repetition) {
    long seed = repetition.getCurrentRepetition();
    try (var p = new SagaStepLockFixture()) {
      Map<String, List<EventEnvelope>> eventsOfOrder = new LinkedHashMap<>();
      for (int saga = 1; saga <= SAGAS; saga++) {
        String order = String.valueOf(saga);
        eventsOfOrder.put(
            order,
            List.of(
                p.append(new Opened(order)),
                p.append(new Advanced(order)),
                p.append(new Advanced(order))));
      }
      p.bus.beforeEveryCommand = SagaStepLockStressTest::yieldAndSometimesFail;

      var delivering = new AtomicBoolean(true);
      CompletableFuture<Void> timeouts =
          onAnotherThread(
              () -> {
                CHOICES.set(new Random(seed * 31));
                while (delivering.get()) {
                  p.timeOut();
                  Thread.yield();
                }
              });
      List<CompletableFuture<Void>> deliverers = new ArrayList<>();
      for (int thread = 0; thread < DELIVERING_THREADS; thread++) {
        long threadSeed = seed * 1_000 + thread;
        deliverers.add(
            onAnotherThread(
                () -> {
                  Random choices = new Random(threadSeed);
                  CHOICES.set(choices);
                  List<String> orders = new ArrayList<>(eventsOfOrder.keySet());
                  Collections.shuffle(orders, choices);
                  for (String order : orders) {
                    for (EventEnvelope event : eventsOfOrder.get(order)) {
                      deliverUntilItReturns(p, event);
                    }
                  }
                }));
      }
      for (CompletableFuture<Void> deliverer : deliverers) {
        assertThat(outcomeOf(deliverer)).as("a delivering thread").isNull();
      }
      delivering.set(false);
      assertThat(outcomeOf(timeouts)).as("the timeout thread").isNull();
      p.bus.beforeEveryCommand = null;
      // Whatever is still forward is past its deadline, and an episode two paths were driving at
      // once may need one more resume: poll until every saga is compensated.
      for (int poll = 0; poll < 10 && !allCompensated(p, eventsOfOrder.keySet()); poll++) {
        p.timeOut();
      }

      List<DomainEvent> stored =
          p.eventStore.readGlobalStream(GlobalOffset.initial(), 100_000).stream()
              .map(e -> (DomainEvent) e.event())
              .toList();
      for (String order : eventsOfOrder.keySet()) {
        List<String> ofOrder = p.storedOrder(order);
        int firstCompensation =
            Math.min(indexOrEnd(ofOrder, "Released"), indexOrEnd(ofOrder, "Refunded"));
        assertThat(ofOrder.subList(firstCompensation, ofOrder.size()))
            .as("seed %d, order %s: stored %s", seed, order, ofOrder)
            .containsExactlyInAnyOrder("Released", "Refunded");
        assertThat(p.row(order).status())
            .as("seed %d, order %s", seed, order)
            .isEqualTo(SagaStatus.COMPENSATED);
      }
      assertThat(stored)
          .as("the run stored forward events")
          .anyMatch(e -> e instanceof Reserved || e instanceof Shipped);
      assertThat(p.lock.sagasInFlight()).as("seed %d: sagas left in the step lock", seed).isZero();
    }
  }

  private static boolean allCompensated(SagaStepLockFixture p, Iterable<String> orders) {
    for (String order : orders) {
      if (p.row(order).status() != SagaStatus.COMPENSATED) {
        return false;
      }
    }
    return true;
  }

  private static int indexOrEnd(List<String> events, String name) {
    int index = events.indexOf(name);
    return index < 0 ? events.size() : index;
  }

  /**
   * What a subscription does with a delivery whose forward command fails retry-later: it delivers
   * the event again. Nothing else may leave the listener here. In particular no delivery loses a
   * compare-and-swap: with every step and every claim of the process under one lock, no write lands
   * between a step's read of the row and its own write.
   */
  private static void deliverUntilItReturns(SagaStepLockFixture p, EventEnvelope event) {
    for (int attempt = 0; attempt < 10_000; attempt++) {
      try {
        p.deliver(event);
        return;
      } catch (RetryLater redeliver) {
        Thread.yield();
      }
    }
    throw new AssertionError("a delivery never returned: offset " + event.globalOffset().value());
  }

  /** A retry-later failure of a forward command: a connection reset, by its cause. */
  private static final class RetryLater extends IllegalStateException {
    RetryLater() {
      super("connection reset", new SQLException("connection reset"));
    }
  }

  /**
   * Before every command: yield a seeded number of times, so that a step is in flight for a while
   * between its read of the row and its command; and fail one forward command in eight with a
   * retry-later failure.
   */
  private static void yieldAndSometimesFail(Command command) {
    Random choices = CHOICES.get();
    if (choices == null) {
      return;
    }
    for (int yields = choices.nextInt(8); yields > 0; yields--) {
      Thread.yield();
    }
    boolean forward = command instanceof Reserve || command instanceof Ship;
    if (forward && choices.nextInt(8) == 0) {
      throw new RetryLater();
    }
  }
}
