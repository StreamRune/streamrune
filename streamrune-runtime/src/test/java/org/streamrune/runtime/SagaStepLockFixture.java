package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.OptimisticLockException;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore.AppliedEvent;
import org.streamrune.core.subscription.SubscriptionLeadership;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * One process for the step-lock tests: a {@link SagaRunner}, the {@link SagaTimeoutRunner}, the
 * compensation-retry sweeper and the dead-letter replayer built from it, the real executor, the
 * real command bus and the in-memory stores. Any number of order sagas: the saga of order {@code n}
 * is {@code order-saga-n}, and every command and stored event carries its order id.
 *
 * <p>A test places a second path at a chosen point of a step with a hook: right before a chosen
 * command reaches the bus, or right before a claim reaches the store. Threads are coordinated with
 * latches and with the lock's own count of waiting threads; nothing waits on a clock.
 */
final class SagaStepLockFixture implements AutoCloseable {

  record Opened(String orderId) implements DomainEvent {}

  /** A correlated event whose forward command is {@link Ship}; the saga stays RUNNING. */
  record Advanced(String orderId) implements DomainEvent {}

  /** A correlated event that completes the saga and issues no command. */
  record Completed(String orderId) implements DomainEvent {}

  record Reserve(String orderId) implements Command {}

  record Ship(String orderId) implements Command {}

  record Release(String orderId) implements Command {}

  record Refund(String orderId) implements Command {}

  record Reserved(String orderId) implements DomainEvent {}

  record Shipped(String orderId) implements DomainEvent {}

  record Released(String orderId) implements DomainEvent {}

  record Refunded(String orderId) implements DomainEvent {}

  record Untouched() implements AggregateState {}

  record State(SagaStatus status, String orderId) implements SagaState {}

  static final SagaType TYPE = SagaType.fromClass(State.class);
  static final Duration TIMEOUT = Duration.ofMinutes(5);

  /** The bound of every wait a test makes for another thread. */
  static final long PATIENCE_SECONDS = 20;

  static SagaId sagaOf(String orderId) {
    return SagaId.of("order-saga-" + orderId);
  }

  /** The death of the process: an {@link Error}, which no saga code catches. */
  static final class ProcessDeath extends Error {
    ProcessDeath() {
      super("the process dies here", null, false, false);
    }
  }

  /** Stores one event per command; {@code failure}, while set, makes {@code decide} throw. */
  static final class OneEventDecider<C extends Command>
      implements Decider<C, Untouched, DomainEvent> {
    private final Function<C, DomainEvent> eventOf;
    volatile Supplier<RuntimeException> failure;

    OneEventDecider(Function<C, DomainEvent> eventOf) {
      this.eventOf = eventOf;
    }

    @Override
    public Untouched initialState() {
      return new Untouched();
    }

    @Override
    public List<DomainEvent> decide(C command, Untouched state) {
      Supplier<RuntimeException> failing = failure;
      if (failing != null) {
        throw failing.get();
      }
      return List.of(eventOf.apply(command));
    }

    @Override
    public Untouched evolve(Untouched state, DomainEvent event) {
      return state;
    }
  }

  /**
   * {@link Opened} issues {@link Reserve} (and {@link Ship} with {@code startAlsoShips}), {@link
   * Advanced} issues {@link Ship}, {@link Completed} completes the saga. Compensation is a release
   * and a refund.
   */
  static final class OrderSaga implements SagaDecider<State> {
    volatile boolean startAlsoShips;

    /** While set, {@code evolve} throws: the event is poison on its next evolve. */
    volatile boolean evolveThrows;

    @Override
    public Class<State> stateType() {
      return State.class;
    }

    @Override
    public State initialState(SagaId sagaId) {
      return new State(SagaStatus.STARTED, sagaId.value().substring("order-saga-".length()));
    }

    @Override
    public State evolve(State state, EventEnvelope event) {
      if (evolveThrows) {
        throw new IllegalStateException("evolve fails on purpose");
      }
      return new State(
          event.event() instanceof Completed ? SagaStatus.COMPLETED : SagaStatus.RUNNING,
          state.orderId());
    }

    @Override
    public List<SagaCommand> handle(State state, EventEnvelope event) {
      String order = state.orderId();
      SagaCommand reserve = SagaCommand.of(new Reserve(order), new AggregateId(order));
      SagaCommand ship = SagaCommand.of(new Ship(order), new AggregateId(order));
      return switch (event.event()) {
        case Opened opened -> startAlsoShips ? List.of(reserve, ship) : List.of(reserve);
        case Advanced advanced -> List.of(ship);
        default -> List.of();
      };
    }

    @Override
    public List<SagaCommand> compensate(State state, Throwable failure, SagaCommand failedCommand) {
      String order = state.orderId();
      return List.of(
          SagaCommand.of(new Release(order), new AggregateId(order)),
          SagaCommand.of(new Refund(order), new AggregateId(order)));
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(TIMEOUT);
    }
  }

  /**
   * Runs the real bus. A hook, when set, runs once, on the dispatching thread, right before the
   * first command of the chosen class; {@code beforeEveryCommand}, when set, runs before each one.
   */
  static final class HookedBus implements CommandBus {
    private record Hook(Class<? extends Command> before, Runnable action) {}

    private final CommandBus delegate;
    private final AtomicReference<Hook> hook = new AtomicReference<>();
    volatile Consumer<Command> beforeEveryCommand;

    HookedBus(CommandBus delegate) {
      this.delegate = delegate;
    }

    void runOnceBefore(Class<? extends Command> command, Runnable action) {
      hook.set(new Hook(command, action));
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return delegate.execute(command);
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      Hook armed = hook.get();
      if (armed != null && armed.before().isInstance(command) && hook.compareAndSet(armed, null)) {
        armed.action().run();
      }
      Consumer<Command> every = beforeEveryCommand;
      if (every != null) {
        every.accept(command);
      }
      return delegate.execute(command, key);
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return delegate.supportsIdempotentExecution();
    }

    @Override
    public Object sagaFenceStorageIdentity() {
      return delegate.sagaFenceStorageIdentity();
    }
  }

  /**
   * A forwarding store with hooks that run once — right before the next claim is written, right
   * before the next event write — a switch that fails one coming read, and a record of how many ids
   * each timeout query asked for.
   */
  static final class HookedSagaStore extends ForwardingSagaStore {
    private final AtomicReference<Runnable> beforeClaim = new AtomicReference<>();
    private final AtomicReference<Runnable> beforeEventWrite = new AtomicReference<>();
    private final AtomicInteger readsUntilAFailure = new AtomicInteger();
    private final AtomicInteger eventWritesToLose = new AtomicInteger();
    private final AtomicInteger eventWriteAttempts = new AtomicInteger();
    private final AtomicInteger genesisCreatesToLose = new AtomicInteger();

    /** The {@code limit} of every {@code findTimedOut} call, in call order. */
    final List<Integer> timeoutQueryLimits = new CopyOnWriteArrayList<>();

    HookedSagaStore(InMemorySagaStore delegate) {
      super(delegate);
    }

    /** Runs once, on the writing thread, after the step's read and before its event write. */
    void runOnceBeforeTheEventWrite(Runnable action) {
      beforeEventWrite.set(action);
    }

    /**
     * Makes the next {@code n} event writes lose their compare-and-swap: each throws before it
     * reaches the delegate, so the row is untouched.
     */
    void conflictOnTheNextEventWrites(int n) {
      eventWritesToLose.set(n);
    }

    /**
     * Makes the next genesis create lose its compare-and-swap without writing a row: the saga was
     * created and removed by other writers before the loser read it again.
     */
    void loseTheNextGenesisCreate() {
      genesisCreatesToLose.set(1);
    }

    @Override
    public void createGenesisPending(
        SagaId id,
        SagaType type,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending,
        EventId episodeCause) {
      if (genesisCreatesToLose.getAndSet(0) > 0) {
        throw new OptimisticLockException("Saga already exists (test)");
      }
      super.createGenesisPending(id, type, state, status, deadLetterPending, episodeCause);
    }

    /** How many event writes reached this store, the ones that lost included. */
    int eventWriteAttempts() {
      return eventWriteAttempts.get();
    }

    /** Makes the {@code n}-th read of a row from here on throw; the reads around it succeed. */
    void failTheReadNumber(int n) {
      readsUntilAFailure.set(n);
    }

    @Override
    public void applyEvent(
        SagaId id,
        SagaType type,
        SagaState state,
        SagaStatus status,
        long version,
        AppliedEvent applied) {
      Runnable once = beforeEventWrite.getAndSet(null);
      if (once != null) {
        once.run();
      }
      eventWriteAttempts.incrementAndGet();
      if (eventWritesToLose.getAndUpdate(left -> left > 0 ? left - 1 : 0) > 0) {
        throw new OptimisticLockException("Stale view of saga (test)");
      }
      super.applyEvent(id, type, state, status, version, applied);
    }

    @Override
    public <S extends SagaState> Optional<LoadedSaga<S>> load(
        SagaId id, SagaType type, Class<S> stateType) {
      if (readsUntilAFailure.get() > 0 && readsUntilAFailure.decrementAndGet() == 0) {
        throw new IllegalStateException("the saga store is unreachable");
      }
      return super.load(id, type, stateType);
    }

    @Override
    public List<SagaId> findTimedOut(SagaType type, Instant cutoff, int limit) {
      timeoutQueryLimits.add(limit);
      return super.findTimedOut(type, cutoff, limit);
    }

    /** Runs once, on the claiming thread, after the claim's read and before its write. */
    void runOnceBeforeTheClaimIsWritten(Runnable action) {
      beforeClaim.set(action);
    }

    @Override
    public void claimCompensating(
        SagaId id, SagaType type, SagaState state, long version, EventId episodeCause) {
      Runnable once = beforeClaim.getAndSet(null);
      if (once != null) {
        once.run();
      }
      super.claimCompensating(id, type, state, version, episodeCause);
    }
  }

  final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
  final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  final InMemorySagaStore rows = new InMemorySagaStore(clock);
  final InMemoryEventStore eventStore =
      new InMemoryEventStore().withCommandInbox(inbox).withSagaStore(rows);
  final HookedSagaStore sagaStore = new HookedSagaStore(rows);
  final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(rows);
  final OneEventDecider<Reserve> reserve = new OneEventDecider<>(c -> new Reserved(c.orderId()));
  final OneEventDecider<Ship> ship = new OneEventDecider<>(c -> new Shipped(c.orderId()));
  final OneEventDecider<Release> release = new OneEventDecider<>(c -> new Released(c.orderId()));
  final OneEventDecider<Refund> refund = new OneEventDecider<>(c -> new Refunded(c.orderId()));
  final OrderSaga saga = new OrderSaga();

  /** The saga type of every deferred timeout claim the timeout runner counted. */
  final List<String> deferredClaimSamples = new CopyOnWriteArrayList<>();

  /** The saga type of every step the runner ran again after a lost compare-and-swap. */
  final List<String> deliveryRetriedSamples = new CopyOnWriteArrayList<>();

  final StreamRuneMetrics metrics =
      new StreamRuneMetrics() {
        @Override
        public void recordSagaTimeoutClaimDeferred(String sagaType) {
          deferredClaimSamples.add(sagaType);
        }

        @Override
        public void recordSagaDeliveryRetried(String sagaType) {
          deliveryRetriedSamples.add(sagaType);
        }
      };
  private final AtomicInteger triggerIds = new AtomicInteger();
  final VirtualThreadCommandBus realBus =
      VirtualThreadCommandBus.builder()
          .eventStore(eventStore)
          .locker(new LocalStripedLocker(64))
          .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
          .objectMapper(new ObjectMapper())
          .commandInbox(inbox)
          .register(
              AggregateType.of("reservation"),
              Reserve.class,
              c -> AggregateId.of(c.orderId()),
              reserve)
          .register(
              AggregateType.of("shipment"), Ship.class, c -> AggregateId.of(c.orderId()), ship)
          .register(
              AggregateType.of("release"), Release.class, c -> AggregateId.of(c.orderId()), release)
          .register(
              AggregateType.of("refund"), Refund.class, c -> AggregateId.of(c.orderId()), refund)
          .build();
  final HookedBus bus = new HookedBus(realBus);

  /** The lock of this process: the runner's, shared by everything built from the runner. */
  final SagaStepLock lock;

  final SagaRunner<State> runner;
  final SagaTimeoutRunner<State> timeoutRunner;
  final SagaCompensationRetrySweeper<State> sweeper;
  final SagaDeadLetterReplayer replayer;

  SagaStepLockFixture() {
    this(new SagaStepLock(), SagaTimeoutRunner.DEFAULT_CLAIM_WAIT);
  }

  /** A process whose runner fences its forward commands, or not. */
  SagaStepLockFixture(boolean forwardFence) {
    this(new SagaStepLock(), SagaTimeoutRunner.DEFAULT_CLAIM_WAIT, forwardFence);
  }

  /**
   * @param lock the step lock of the runner (a test of the step bound passes a short one)
   * @param claimWait how long the timeout runner's claim waits once it waits
   */
  SagaStepLockFixture(SagaStepLock lock, Duration claimWait) {
    this(lock, claimWait, true);
  }

  /**
   * @param lock the step lock of the runner (a test of the step bound passes a short one)
   * @param claimWait how long the timeout runner's claim waits once it waits
   * @param forwardFence whether the runner fences its forward commands at their commit
   */
  SagaStepLockFixture(SagaStepLock lock, Duration claimWait, boolean forwardFence) {
    this.lock = lock;
    runner =
        SagaRunner.<State>builder()
            .decider(saga)
            .startWhen(e -> e.event() instanceof Opened)
            .extractSagaId(e -> sagaOf(((Opened) e.event()).orderId()))
            .correlateByEventData(
                (streamId, e) ->
                    switch (e) {
                      case Advanced advanced -> Optional.of(sagaOf(advanced.orderId()));
                      case Completed completed -> Optional.of(sagaOf(completed.orderId()));
                      default -> Optional.empty();
                    })
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(deadLetters)
            .clock(clock)
            .stepLock(lock)
            .metrics(metrics)
            .forwardFence(forwardFence)
            .build();
    timeoutRunner =
        SagaTimeoutRunner.<State>builder()
            .decider(saga)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaType(TYPE)
            .clock(clock)
            .sagaRunner(runner)
            .claimWait(claimWait)
            .batchSize(1_000)
            .metrics(metrics)
            .build();
    sweeper =
        SagaCompensationRetrySweeper.<State>builder()
            .sagaRunner(runner)
            .sagaStore(sagaStore)
            .leadership(SubscriptionLeadership.NOOP)
            .retryInterval(Duration.ofSeconds(30))
            .giveUpAfter(Duration.ofHours(1))
            .clock(clock)
            .build();
    replayer =
        SagaDeadLetterReplayer.builder()
            .sagaDeadLetterStore(deadLetters)
            .sagaStore(sagaStore)
            .eventStore(eventStore)
            .sagaRunner(runner)
            .clock(clock)
            .build();
  }

  /** Appends a trigger event as a request stores it. */
  EventEnvelope append(DomainEvent event) {
    StreamId streamId =
        StreamId.of(AggregateType.of("trigger"), AggregateId.of("t-" + UUID.randomUUID()));
    var appended =
        eventStore.append(
            streamId,
            List.of(
                new EventEnvelope(
                    GlobalOffset.initial(),
                    streamId,
                    Version.initial(),
                    EventType.fromClass(event.getClass()),
                    event,
                    new EventMetadata(
                        EventId.of("evt-" + triggerIds.incrementAndGet()),
                        CommandId.of("cmd-" + UUID.randomUUID()),
                        null,
                        null,
                        CorrelationId.of("request-" + UUID.randomUUID()),
                        null,
                        null,
                        Instant.now()))),
            Version.initial());
    return eventStore
        .readGlobalStream(GlobalOffset.of(appended.globalOffsets().get(0).value() - 1), 1)
        .get(0);
  }

  void deliver(EventEnvelope event) {
    runner.asEventListener().onEvents(List.of(event));
  }

  /** Every saga passes its deadline and the timeout runner polls once. */
  void timeOut() {
    clock.advance(TIMEOUT.plusSeconds(1));
    timeoutRunner.pollOnce();
  }

  LoadedSaga<State> row(String orderId) {
    return rows.load(sagaOf(orderId), TYPE, State.class).orElseThrow();
  }

  /** Waits until the order's saga has a row: its start step has written the genesis-pending row. */
  void awaitRow(String orderId) {
    awaitUntil(
        "the saga of order " + orderId + " has a row",
        () -> rows.load(sagaOf(orderId), TYPE, State.class).isPresent());
  }

  /** The stored forward and compensation events of one order, in global order. */
  List<String> storedOrder(String orderId) {
    return eventStore.readGlobalStream(GlobalOffset.initial(), 100_000).stream()
        .map(e -> (DomainEvent) e.event())
        .filter(e -> orderId.equals(orderOf(e)))
        .map(e -> e.getClass().getSimpleName())
        .toList();
  }

  /** The order id of a stored forward or compensation event; {@code null} for any other event. */
  static String orderOf(DomainEvent event) {
    return switch (event) {
      case Reserved e -> e.orderId();
      case Shipped e -> e.orderId();
      case Released e -> e.orderId();
      case Refunded e -> e.orderId();
      default -> null;
    };
  }

  // ---- threads ----

  /** Runs {@code body} on a new platform thread; the future carries what it threw. */
  static CompletableFuture<Void> onAnotherThread(Runnable body) {
    CompletableFuture<Void> done = new CompletableFuture<>();
    Thread.ofPlatform()
        .start(
            () -> {
              try {
                body.run();
                done.complete(null);
              } catch (Throwable thrown) {
                done.completeExceptionally(thrown);
              }
            });
    return done;
  }

  /** What the thread threw, or {@code null} when it returned normally. */
  static Throwable outcomeOf(CompletableFuture<Void> thread) {
    try {
      thread.get(PATIENCE_SECONDS, TimeUnit.SECONDS);
      return null;
    } catch (ExecutionException failed) {
      return failed.getCause();
    } catch (TimeoutException | InterruptedException stuck) {
      throw new AssertionError("the other thread did not finish", stuck);
    }
  }

  static void await(CountDownLatch latch) {
    try {
      assertThat(latch.await(PATIENCE_SECONDS, TimeUnit.SECONDS))
          .as("the other thread reached its point")
          .isTrue();
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new AssertionError("interrupted while waiting for the other thread", interrupted);
    }
  }

  /** Parks the calling thread on {@code latch} without a bound of its own turning into a pass. */
  static void park(CountDownLatch latch) {
    await(latch);
  }

  /** Spins until {@code condition} holds; a condition of another thread's progress, not a delay. */
  static void awaitUntil(String what, BooleanSupplier condition) {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(PATIENCE_SECONDS);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("never happened: " + what);
      }
      Thread.onSpinWait();
    }
  }

  /** Waits until {@code threads} threads wait for the step lock of the order's saga. */
  void awaitWaitingFor(String orderId, int threads) {
    awaitUntil(
        threads + " thread(s) waiting for the step lock of order " + orderId,
        () -> lock.waitingFor(TYPE, sagaOf(orderId)) == threads);
  }

  @Override
  public void close() {
    realBus.close();
  }
}
