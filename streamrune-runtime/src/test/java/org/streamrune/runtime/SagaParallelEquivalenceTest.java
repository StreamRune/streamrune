package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.streamrune.runtime.SagaParallelFixtures.orchestrator;
import static org.streamrune.runtime.SagaParallelFixtures.page;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.Command;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.runtime.SagaParallelFixtures.Delegating;
import org.streamrune.runtime.SagaParallelFixtures.EndState;
import org.streamrune.runtime.SagaParallelFixtures.GatedBus;
import org.streamrune.runtime.SagaParallelFixtures.ParState;
import org.streamrune.runtime.SagaParallelFixtures.Replied;
import org.streamrune.runtime.SagaParallelFixtures.Started;
import org.streamrune.runtime.SagaParallelFixtures.Step;
import org.streamrune.runtime.SagaParallelFixtures.Undo;
import org.streamrune.test.ForwardingCommandBus;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * A runner that steps the sagas of a page in parallel ends where a serial runner ends: the same
 * rows, the same commands for each saga in the same order, the same dead letters. The events are
 * drawn once in one global offset order, and what happens to an event is a function of its offset,
 * so no run depends on the order in which the sagas of a page happen to be stepped.
 */
class SagaParallelEquivalenceTest {

  private static final int PAGE_SIZE = 25;
  private static final int MAX_DELIVERIES_PER_PAGE = 20;

  private enum Behaviour {
    SUCCEEDS,
    REJECTED,
    RETRY_LATER_ONCE,
    POISON
  }

  @Timeout(value = 2, unit = TimeUnit.MINUTES)
  @ParameterizedTest
  @ValueSource(longs = {1, 2, 3, 5, 8, 13, 21, 34, 55, 89})
  void aParallelRunEndsWhereTheSerialRunEnds(long seed) {
    Scenario scenario = Scenario.random(seed, /* sagas */ 40, /* events */ 300);
    Outcome serial = scenario.runToQuiescence(/* parallelism */ 1);
    Outcome parallel = scenario.runToQuiescence(/* parallelism */ 8);

    // the run did something worth comparing
    assertThat(serial.end().rowsBySaga()).hasSizeGreaterThan(10);
    assertThat(serial.end().commandsBySaga().values().stream().mapToInt(List::size).sum())
        .isGreaterThan(60);
    assertThat(serial.end().deadLettersBySaga()).isNotEmpty();
    assertThat(serial.end().commandsBySaga().values().stream().flatMap(List::stream))
        .anyMatch(c -> c instanceof Undo);
    // and the parallel run was parallel, the serial one was not
    assertThat(serial.dispatchThreads()).isEqualTo(1);
    assertThat(parallel.dispatchThreads()).isGreaterThan(1);

    // status, state, version, last_applied_offset, genesis_applied of every row
    assertThat(parallel.end().rowsBySaga()).isEqualTo(serial.end().rowsBySaga());
    // the commands executed for each saga, in order
    assertThat(parallel.end().commandsBySaga()).isEqualTo(serial.end().commandsBySaga());
    // (offset, error type) of each dead letter, by saga
    assertThat(parallel.end().deadLettersBySaga()).isEqualTo(serial.end().deadLettersBySaga());
    assertThat(parallel.end().inboxKeys()).isEqualTo(serial.end().inboxKeys());
  }

  /** What a run left behind, and how many threads dispatched its commands. */
  private record Outcome(EndState end, long dispatchThreads) {}

  private static final class Scenario {
    private final long seed;
    private final List<DomainEvent> events;
    private final Map<Long, Behaviour> behaviours;

    private Scenario(long seed, List<DomainEvent> events, Map<Long, Behaviour> behaviours) {
      this.seed = seed;
      this.events = events;
      this.behaviours = behaviours;
    }

    /**
     * Per saga a start event and 0-8 correlated events, interleaved across the sagas in one global
     * offset order (a saga's start precedes its correlated events); {@code events} caps the count.
     */
    static Scenario random(long seed, int sagas, int events) {
      Random random = new Random(seed);
      List<Integer> remaining = new ArrayList<>(); // correlated events still to draw, by saga
      Set<Integer> started = new HashSet<>();
      List<Integer> open = new ArrayList<>();
      for (int s = 0; s < sagas; s++) {
        remaining.add(random.nextInt(9));
        open.add(s);
      }
      List<DomainEvent> drawn = new ArrayList<>();
      while (!open.isEmpty() && drawn.size() < events) {
        int s = open.get(random.nextInt(open.size()));
        String id = "saga-" + s;
        if (started.add(s)) {
          drawn.add(new Started(id));
        } else if (remaining.get(s) > 0) {
          remaining.set(s, remaining.get(s) - 1);
          drawn.add(new Replied(id));
        }
        if (started.contains(s) && remaining.get(s) == 0) {
          open.remove(Integer.valueOf(s));
        }
      }
      Map<Long, Behaviour> behaviours = new HashMap<>();
      for (int i = 0; i < drawn.size(); i++) {
        int roll = random.nextInt(100);
        Behaviour behaviour =
            roll < 72
                ? Behaviour.SUCCEEDS
                : roll < 85
                    ? Behaviour.REJECTED
                    : roll < 96 ? Behaviour.RETRY_LATER_ONCE : Behaviour.POISON;
        behaviours.put(i + 1L, behaviour);
      }
      return new Scenario(seed, List.copyOf(drawn), Map.copyOf(behaviours));
    }

    Outcome runToQuiescence(int parallelism) {
      InMemorySagaStore rows = new InMemorySagaStore();
      InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(rows);
      GatedBus gated = new GatedBus();
      Set<Long> retried = ConcurrentHashMap.newKeySet();
      ForwardingCommandBus bus =
          new ForwardingCommandBus(gated) {
            @Override
            public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
              if (command instanceof Step step) {
                Behaviour behaviour = behaviours.get(step.triggerOffset());
                if (behaviour == Behaviour.REJECTED) {
                  throw new DomainException("rejected at " + step.triggerOffset());
                }
                if (behaviour == Behaviour.RETRY_LATER_ONCE && retried.add(step.triggerOffset())) {
                  throw new CircuitBreakerOpenException("later at " + step.triggerOffset());
                }
              }
              if (parallelism > 1) {
                pause(command);
              }
              return super.execute(command, key);
            }
          };
      var runner =
          SagaRunner.<ParState>builder()
              .orchestrator(poisoning())
              .sagaStore(rows)
              .commandBus(bus)
              .sagaDeadLetterStore(deadLetters)
              .forwardFence(false)
              .parallelism(parallelism)
              .build();
      var listener = runner.asEventListener();
      for (int first = 0; first < events.size(); first += PAGE_SIZE) {
        int last = Math.min(first + PAGE_SIZE, events.size());
        List<EventEnvelope> page =
            page(first + 1L, events.subList(first, last).toArray(DomainEvent[]::new));
        deliverUntilAcknowledged(listener, page);
      }
      return new Outcome(
          EndState.of(rows, deadLetters, gated), gated.threads().stream().distinct().count());
    }

    /** A seeded 0-2 ms, a function of the command, so a rerun of a seed waits the same. */
    private void pause(Command command) {
      long h = seed * 31 + SagaParallelFixtures.sagaIdOf(command).hashCode();
      if (command instanceof Step step) {
        h = h * 31 + step.triggerOffset();
      }
      try {
        Thread.sleep(new Random(h).nextInt(3));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    private Delegating poisoning() {
      return new Delegating(orchestrator()) {
        @Override
        public ParState evolve(ParState state, EventEnvelope event) {
          if (behaviours.get(event.globalOffset().value()) == Behaviour.POISON) {
            throw new IllegalStateException("poison at " + event.globalOffset().value());
          }
          return super.evolve(state, event);
        }
      };
    }

    private static void deliverUntilAcknowledged(EventListener listener, List<EventEnvelope> page) {
      CircuitBreakerOpenException last = null;
      for (int attempt = 0; attempt < MAX_DELIVERIES_PER_PAGE; attempt++) {
        try {
          listener.onEvents(page);
          return;
        } catch (CircuitBreakerOpenException e) {
          last = e;
        }
      }
      throw new AssertionError(
          "a page was not acknowledged after " + MAX_DELIVERIES_PER_PAGE + " deliveries", last);
    }
  }
}
