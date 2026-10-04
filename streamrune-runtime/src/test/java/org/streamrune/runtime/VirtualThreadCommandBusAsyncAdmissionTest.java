package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.CommandBus.CommandResult;
import org.streamrune.core.CommandBusOverloadedException;
import org.streamrune.core.Decider;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.VirtualThreadCommandBusTest.CounterCommand;
import org.streamrune.runtime.VirtualThreadCommandBusTest.CounterEvent;
import org.streamrune.runtime.VirtualThreadCommandBusTest.CounterState;
import org.streamrune.runtime.VirtualThreadCommandBusTest.InMemoryEventStore;

/**
 * The {@code executeAsync} admission budget frees a command's slot <b>before</b> the command's
 * future becomes observable as complete, on every path a command can end by, and exactly once.
 *
 * <p>Every test runs with a budget of one, so a slot that is still held when the caller observes
 * completion turns a resubmission into a {@link CommandBusOverloadedException}. The race is made
 * deterministic, not provoked by timing: a stage chained on the future while the command is
 * provably still running (its decider is parked on a gate) runs synchronously on the command's own
 * thread, inside the future's completion — the earliest point at which any caller can observe it.
 */
class VirtualThreadCommandBusAsyncAdmissionTest {

  private static final AggregateType TYPE = AggregateType.of("counter");

  private static final long TIMEOUT_SECONDS = 5;

  private final InMemoryEventStore store = new InMemoryEventStore();
  private final ScriptedDecider decider = new ScriptedDecider();
  // Every virtual thread a command ran on, so a test can join them and observe the state after
  // each thread's last statement — the point where a second release of one slot would land.
  private final List<Thread> commandThreads = new CopyOnWriteArrayList<>();

  @Test
  void aStageChainedOnASucceedingCommand_resubmits_andIsAdmitted() throws Exception {
    try (var bus = busWithBudgetOfOne(UnaryOperator.identity())) {
      var gate = decider.gate("first");
      var first = bus.executeAsync(new CounterCommand.Inc("first"));
      gate.awaitEntered();

      // Registered while "first" is parked in its decider, so the stage runs inside the future's
      // completion on the command's own thread.
      var chained = first.thenCompose(r -> bus.executeAsync(new CounterCommand.Inc("second")));
      gate.open();

      var result =
          assertDoesNotThrow(
              () -> chained.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
              "the slot of a command whose future completed must already be free");
      assertEquals(1, result.events().size());
      assertEquals(1, store.eventsFor("counter:first").size());
      assertEquals(1, store.eventsFor("counter:second").size());

      assertBudgetIsExactlyOne(bus);
    }
  }

  static Stream<Arguments> commandFailures() {
    return Stream.of(
        Arguments.of(new IllegalStateException("decider refused")),
        Arguments.of(new ScriptedError("decider died")));
  }

  @ParameterizedTest
  @MethodSource("commandFailures")
  void aStageChainedOnAFailingCommand_resubmits_andIsAdmitted(Throwable failure) throws Exception {
    try (var bus = busWithBudgetOfOne(UnaryOperator.identity())) {
      var gate = decider.gate("first");
      decider.failWith("first", failure);
      var first = bus.executeAsync(new CounterCommand.Inc("first"));
      gate.awaitEntered();

      var chained =
          first.exceptionallyCompose(e -> bus.executeAsync(new CounterCommand.Inc("second")));
      gate.open();

      var ex =
          assertThrows(
              ExecutionException.class, () -> first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertSame(failure, ex.getCause(), "the decider's throwable fails the future as-is");
      var result =
          assertDoesNotThrow(
              () -> chained.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
              "the slot of a command whose future failed must already be free");
      assertEquals(1, result.events().size());
      assertTrue(store.eventsFor("counter:first").isEmpty());
      assertEquals(1, store.eventsFor("counter:second").size());

      assertBudgetIsExactlyOne(bus);
    }
  }

  @Test
  void aCancelledFuture_keepsTheSlotUntilTheCommandFinishes_thenFreesItOnce() throws Exception {
    try (var bus = busWithBudgetOfOne(UnaryOperator.identity())) {
      var gate = decider.gate("first");
      var first = bus.executeAsync(new CounterCommand.Inc("first"));
      gate.awaitEntered();

      // AsyncCommandBus tells callers not to cancel, because cancelling does not stop the command.
      // A caller that cancels anyway (or settles the future through a timeout of its own) must not
      // corrupt the budget: the command runs on and keeps its slot until it finishes.
      assertTrue(first.cancel(true));
      assertOverloaded(bus.executeAsync(new CounterCommand.Inc("refused")));

      gate.open();
      awaitCommandThreadsTerminated();
      assertEquals(
          1, store.eventsFor("counter:first").size(), "the cancelled command still committed");
      assertTrue(store.eventsFor("counter:refused").isEmpty());

      assertBudgetIsExactlyOne(bus);
    }
  }

  @Test
  void aWrapperWhoseRunnableThrowsBeforeTheCommand_failsTheFuture_andFreesTheSlotFirst()
      throws Exception {
    var wrapperFailure = new IllegalStateException("wrapper runnable failed");
    var wraps = new AtomicInteger();
    UnaryOperator<Runnable> failFirstOnly =
        task ->
            wraps.getAndIncrement() == 0
                ? () -> {
                  throw wrapperFailure;
                }
                : task;
    try (var bus = busWithBudgetOfOne(failFirstOnly)) {
      var first = bus.executeAsync(new CounterCommand.Inc("first"));
      var chained =
          first.exceptionallyCompose(e -> bus.executeAsync(new CounterCommand.Inc("second")));

      var ex =
          assertThrows(
              ExecutionException.class,
              () -> first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
              "a wrapper that never ran the command must not leave the caller waiting forever");
      assertSame(wrapperFailure, ex.getCause());
      assertDoesNotThrow(() -> chained.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertTrue(store.eventsFor("counter:first").isEmpty(), "the command never ran");
      assertEquals(1, store.eventsFor("counter:second").size());

      assertBudgetIsExactlyOne(bus);
    }
  }

  @Test
  void aWrapperThatThrowsAtSubmission_failsTheFuture_andFreesTheSlotOnce() throws Exception {
    var wraps = new AtomicInteger();
    UnaryOperator<Runnable> refuseFirstOnly =
        task -> {
          if (wraps.getAndIncrement() == 0) {
            throw new IllegalStateException("wrapper refused");
          }
          return task;
        };
    try (var bus = busWithBudgetOfOne(refuseFirstOnly)) {
      var first = bus.executeAsync(new CounterCommand.Inc("first"));

      var ex =
          assertThrows(
              ExecutionException.class, () -> first.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
      assertInstanceOf(IllegalStateException.class, ex.getCause());

      assertBudgetIsExactlyOne(bus);
    }
  }

  // === Helpers ===

  /**
   * Proves the budget holds exactly one free slot once every command thread has terminated: one
   * held command is admitted (no slot leaked) and a second is refused while it is held (no slot
   * released twice).
   */
  private void assertBudgetIsExactlyOne(VirtualThreadCommandBus bus) throws Exception {
    awaitCommandThreadsTerminated();
    var gate = decider.gate("probe-held");
    var held = bus.executeAsync(new CounterCommand.Inc("probe-held"));
    gate.awaitEntered();
    assertOverloaded(bus.executeAsync(new CounterCommand.Inc("probe-overflow")));
    gate.open();
    assertEquals(1, held.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).events().size());
    assertTrue(store.eventsFor("counter:probe-overflow").isEmpty());
  }

  private static void assertOverloaded(CompletableFuture<CommandResult> future) {
    var ex =
        assertThrows(ExecutionException.class, () -> future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
    assertInstanceOf(CommandBusOverloadedException.class, ex.getCause());
  }

  private void awaitCommandThreadsTerminated() throws InterruptedException {
    for (Thread thread : commandThreads) {
      assertTrue(
          thread.join(Duration.ofSeconds(TIMEOUT_SECONDS)),
          "command thread " + thread + " did not terminate");
    }
  }

  private VirtualThreadCommandBus busWithBudgetOfOne(UnaryOperator<Runnable> wrapper) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        .maxInFlightAsyncCommands(1)
        .asyncTaskWrapper(
            task -> {
              Runnable wrapped = wrapper.apply(task);
              return () -> {
                commandThreads.add(Thread.currentThread());
                wrapped.run();
              };
            })
        .register(
            TYPE,
            CounterCommand.class,
            cmd ->
                switch (cmd) {
                  case CounterCommand.Inc c -> AggregateId.of(c.counterId());
                },
            decider)
        .build();
  }

  /** An {@link Error} the decider throws, distinct from anything the bus raises itself. */
  static final class ScriptedError extends Error {
    ScriptedError(String message) {
      super(message);
    }
  }

  /** One gated command: the test awaits its arrival in the decider and then lets it continue. */
  record Gate(CountDownLatch entered, CountDownLatch release) {
    void awaitEntered() throws InterruptedException {
      assertTrue(entered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS), "command never reached decide");
    }

    void open() {
      release.countDown();
    }
  }

  /** A decider whose behaviour per counter id — park on a gate, then fail — the test scripts. */
  static final class ScriptedDecider
      implements Decider<CounterCommand, CounterState, CounterEvent> {
    private final Map<String, Gate> gates = new ConcurrentHashMap<>();
    private final Map<String, Throwable> failures = new ConcurrentHashMap<>();

    Gate gate(String counterId) {
      var gate = new Gate(new CountDownLatch(1), new CountDownLatch(1));
      gates.put(counterId, gate);
      return gate;
    }

    void failWith(String counterId, Throwable failure) {
      failures.put(counterId, failure);
    }

    @Override
    public CounterState initialState() {
      return new CounterState();
    }

    @Override
    public List<CounterEvent> decide(CounterCommand command, CounterState state) {
      String id = ((CounterCommand.Inc) command).counterId();
      Gate gate = gates.get(id);
      if (gate != null) {
        gate.entered().countDown();
        try {
          if (!gate.release().await(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
            throw new IllegalStateException("gate for " + id + " was never opened");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      Throwable failure = failures.get(id);
      if (failure instanceof RuntimeException runtime) {
        throw runtime;
      }
      if (failure instanceof Error error) {
        throw error;
      }
      return List.of(new CounterEvent.Incremented());
    }

    @Override
    public CounterState evolve(CounterState state, CounterEvent event) {
      return new CounterState(state.count() + 1);
    }
  }
}
