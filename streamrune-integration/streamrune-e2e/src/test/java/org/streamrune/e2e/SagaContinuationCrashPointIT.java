package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.DomainException;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.ReservedCorrelationIdException;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaDecider;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.EventListener;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.postgres.PostgresCommandInbox;
import org.streamrune.postgres.PostgresDeadLetterQueue;
import org.streamrune.postgres.PostgresEventStore;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.postgres.PostgresSagaDeadLetterStore;
import org.streamrune.postgres.PostgresSagaStore;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaCompensationRetrySweeper;
import org.streamrune.runtime.SagaDeadLetterReplayer;
import org.streamrune.runtime.SagaDeadLetterReplayer.ReplayOutcome;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.SagaTimeoutRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * One saga step with a listener's continuation, on PostgreSQL, interrupted after each durable step
 * and resumed on each path that can resume it.
 *
 * <p><b>The loop.</b> A request stores {@code OrderPlaced}. The saga starts and dispatches {@code
 * ReserveStock}; its reply {@code StockReserved} is stored with the saga's origin at hop 0. A
 * listener reacts to that reply inside {@link StreamRuneContext#continuing(EventEnvelope)} with
 * {@code CapturePayment}; its event {@code PaymentCaptured} carries the origin at hop 1 and the
 * reply's id as causation id. The saga receives it by that origin and dispatches its next command,
 * {@code ShipOrder}; {@code OrderShipped} completes it. When {@code ShipOrder} is rejected, or the
 * saga's deadline passes, the saga compensates with {@code ReleaseStock} and {@code RefundPayment}.
 *
 * <p><b>What every case asserts.</b> For every stored event of the order: that it is stored once,
 * its saga origin and hop, its correlation id (the reserved {@code streamrune:saga:<sagaId>} for
 * everything the saga caused), and its causation id; besides the saga's final status, the event
 * that opened its compensation episode, the idempotency keys in the command inbox, and that the
 * command dead letter queue and the saga dead-letter store are empty. A case compares that outcome
 * with the outcome of the same scenario run without an interruption. Where an interruption changes
 * the outcome by design — the saga's deadline passes before the forward loop of its start has
 * committed, a stored reply is altered — the case states the outcome it expects instead.
 *
 * <p><b>The interruptions.</b> A process death is an {@link Error} thrown at a fixed point: by the
 * command bus before or after a command of one type commits, or by a delivery loop after its
 * listener returned and before its checkpoint is saved. After a death every in-memory object — the
 * command bus, the saga runner, the listener, the delivery loops, the stores — is built again over
 * the same database, and delivery restarts from the stored checkpoints.
 *
 * <ul>
 *   <li><i>The forward loop:</i> a death after the saga row is inserted, after each command of the
 *       saga and of the listener commits, after each write of the saga row, and before each
 *       checkpoint; and a failed dispatch of the listener, which is dead-lettered nowhere.
 *   <li><i>Compensation opened by an event</i> (a rejected {@code ShipOrder}): a death after the
 *       claim, after each compensation command, and after the terminal write; resumed by the
 *       redelivery of the event, by a later event of the saga, by the compensation-retry sweeper
 *       and by the timeout runner. The compensation events carry the id of the event that opened
 *       the episode on every path.
 *   <li><i>Compensation opened by the timeout runner:</i> the same deaths, on the runner's thread;
 *       resumed by the timeout runner, by the sweeper and by a later event of the saga. The
 *       compensation events carry no causation id on every path, also when an event resumes the
 *       episode.
 *   <li><i>A start event whose {@code evolve} yields {@code COMPENSATING}:</i> a death after the
 *       insert, after each forward command, after the write that enters the episode, and a forward
 *       command that fails with a failure worth retrying; then the redelivery of the start event,
 *       the sweeper, a later event of the saga, or the timeout runner at the saga's deadline acts
 *       first. And the same start with a forward command the domain rejects: a death after the
 *       claim, after each compensation command and after the terminal write, resumed on each path.
 *   <li><i>The saga dead-letter replayer:</i> a reply the saga could not read and the reply held
 *       behind it are fed to the saga their origin names, also across a death inside the feed; so
 *       is a reply that was quarantined before it could be routed, whose entry names no saga.
 *   <li><i>Routing:</i> a reply whose stored correlation id names another saga reaches the saga its
 *       origin names; a reply whose origin is removed reaches no saga, whatever its correlation id.
 * </ul>
 *
 * <p><b>How the cases are driven.</b> Events are delivered by a delivery loop driven by hand, one
 * event at a time: it reads the event after the checkpoint stored in {@code projection_offset},
 * hands it to the listener and saves the checkpoint — what a subscription's poll does — so a case
 * decides which loop moves next, and no case waits for a poll thread. One case runs the whole loop
 * on two started {@link PollingEventSubscription}s and compares what they store with the same
 * outcome. The compensation-retry sweeper and the timeout runner are run through their public
 * lifecycle: started with a short interval, awaited on the saga row with a bound, and stopped.
 * Their clock runs ten minutes ahead of the database's, so a row is due on their first cycle
 * whatever the clocks of the test JVM and of the database container say to each other, and the
 * saga's five-minute deadline has passed.
 */
@Timeout(600)
class SagaContinuationCrashPointIT extends E2ETestBase {

  // ---- events ----

  /** An event of one order. */
  interface OrderEvent extends DomainEvent {
    String orderId();
  }

  /** The start event. A cancelled order starts a saga that is compensating from its first step. */
  record OrderPlaced(String orderId, boolean cancelled) implements OrderEvent {}

  record StockReserved(String orderId) implements OrderEvent {}

  record PaymentCaptured(String orderId) implements OrderEvent {}

  record OrderShipped(String orderId) implements OrderEvent {}

  record StockReleased(String orderId) implements OrderEvent {}

  record PaymentRefunded(String orderId) implements OrderEvent {}

  /** An event a request stores for a running saga; it is routed by its own data. */
  record CustomerPinged(String orderId) implements OrderEvent {}

  // ---- commands ----

  /** A command about one order. */
  interface OrderCommand extends Command {
    String orderId();
  }

  public record ReserveStock(String orderId) implements OrderCommand {}

  public record CapturePayment(String orderId) implements OrderCommand {}

  public record ShipOrder(String orderId) implements OrderCommand {}

  public record ReleaseStock(String orderId) implements OrderCommand {}

  public record RefundPayment(String orderId) implements OrderCommand {}

  record Untouched() implements AggregateState {}

  /** What goes wrong in one process: a command's decider that fails, a saga that cannot evolve. */
  static final class Faults {
    private final Map<Class<?>, Supplier<RuntimeException>> failures = new ConcurrentHashMap<>();
    private final Map<Class<?>, AtomicInteger> timesLeft = new ConcurrentHashMap<>();
    private volatile Class<? extends DomainEvent> unreadable;
    private volatile Class<? extends DomainEvent> unroutable;

    static Faults none() {
      return new Faults();
    }

    /** The decider of {@code command} fails {@code times} times with {@code failure}. */
    Faults failing(
        Class<? extends Command> command, int times, Supplier<RuntimeException> failure) {
      failures.put(command, failure);
      timesLeft.put(command, new AtomicInteger(times));
      return this;
    }

    /** The domain rejects {@code command}, every time. */
    Faults rejecting(Class<? extends Command> command) {
      return failing(
          command, Integer.MAX_VALUE, () -> new DomainException("the order cannot be shipped"));
    }

    /** The saga's {@code evolve} throws on {@code event}. */
    Faults unableToRead(Class<? extends DomainEvent> event) {
      unreadable = event;
      return this;
    }

    /** The saga's start predicate, the first step of routing, throws on {@code event}. */
    Faults unableToRoute(Class<? extends DomainEvent> event) {
      unroutable = event;
      return this;
    }

    void beforeRouting(DomainEvent event) {
      Class<? extends DomainEvent> poison = unroutable;
      if (poison != null && poison.isInstance(event)) {
        throw new IllegalStateException(
            "the saga cannot route " + event.getClass().getSimpleName());
      }
    }

    void beforeDeciding(Command command) {
      AtomicInteger left = timesLeft.get(command.getClass());
      if (left != null && left.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
        throw failures.get(command.getClass()).get();
      }
    }

    void beforeEvolving(DomainEvent event) {
      Class<? extends DomainEvent> poison = unreadable;
      if (poison != null && poison.isInstance(event)) {
        throw new IllegalStateException("the saga cannot read " + event.getClass().getSimpleName());
      }
    }
  }

  /** Stores one event per command, unless the process's faults say otherwise. */
  static final class OneEvent<C extends Command> implements Decider<C, Untouched, DomainEvent> {
    private final Function<C, DomainEvent> eventOf;
    private final Faults faults;

    OneEvent(Function<C, DomainEvent> eventOf, Faults faults) {
      this.eventOf = eventOf;
      this.faults = faults;
    }

    @Override
    public Untouched initialState() {
      return new Untouched();
    }

    @Override
    public List<DomainEvent> decide(C command, Untouched state) {
      faults.beforeDeciding(command);
      return List.of(eventOf.apply(command));
    }

    @Override
    public Untouched evolve(Untouched state, DomainEvent event) {
      return state;
    }
  }

  // ---- the saga ----

  record Fulfillment(SagaStatus status, String orderId, boolean reserved, boolean paid)
      implements SagaState {}

  private static final SagaType TYPE = SagaType.fromClass(Fulfillment.class);

  static SagaId sagaIdOf(String orderId) {
    return SagaId.of("fulfillment-" + orderId);
  }

  static final class FulfillmentSaga implements SagaDecider<Fulfillment> {
    private final Faults faults;

    FulfillmentSaga(Faults faults) {
      this.faults = faults;
    }

    @Override
    public Class<Fulfillment> stateType() {
      return Fulfillment.class;
    }

    @Override
    public Fulfillment initialState(SagaId sagaId) {
      return new Fulfillment(SagaStatus.STARTED, null, false, false);
    }

    @Override
    public Fulfillment evolve(Fulfillment state, EventEnvelope event) {
      faults.beforeEvolving(event.event());
      if (event.event() instanceof OrderPlaced placed) {
        return new Fulfillment(
            placed.cancelled() ? SagaStatus.COMPENSATING : SagaStatus.RUNNING,
            placed.orderId(),
            false,
            false);
      }
      if (event.event() instanceof StockReserved) {
        return new Fulfillment(state.status(), state.orderId(), true, state.paid());
      }
      if (event.event() instanceof PaymentCaptured) {
        return new Fulfillment(state.status(), state.orderId(), state.reserved(), true);
      }
      if (event.event() instanceof OrderShipped) {
        return new Fulfillment(
            SagaStatus.COMPLETED, state.orderId(), state.reserved(), state.paid());
      }
      return state;
    }

    @Override
    public List<SagaCommand> handle(Fulfillment state, EventEnvelope event) {
      String orderId = state.orderId();
      if (event.event() instanceof OrderPlaced placed) {
        // A cancelled order still issues its forward commands: the framework dispatches them
        // before the saga enters its compensation episode.
        return placed.cancelled()
            ? List.of(
                SagaCommand.of(new ReserveStock(orderId), AggregateId.of(orderId)),
                SagaCommand.of(new ShipOrder(orderId), AggregateId.of(orderId)))
            : List.of(SagaCommand.of(new ReserveStock(orderId), AggregateId.of(orderId)));
      }
      if (event.event() instanceof PaymentCaptured && state.status() == SagaStatus.RUNNING) {
        return List.of(SagaCommand.of(new ShipOrder(orderId), AggregateId.of(orderId)));
      }
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        Fulfillment state, Throwable failure, SagaCommand failedCommand) {
      String orderId = state.orderId();
      return List.of(
          SagaCommand.of(new ReleaseStock(orderId), AggregateId.of(orderId)),
          SagaCommand.of(new RefundPayment(orderId), AggregateId.of(orderId)));
    }

    @Override
    public Optional<Duration> timeout() {
      return Optional.of(Duration.ofMinutes(5));
    }
  }

  // ---- a process ----

  /** The process dies here: an {@link Error}, so no catch block of the runtime sees it. */
  static final class ProcessDeath extends Error {
    private static final long serialVersionUID = 1L;

    ProcessDeath(String where) {
      super("the process dies " + where, null, false, false);
    }
  }

  enum When {
    BEFORE,
    AFTER
  }

  /**
   * The command bus of one process. It records the command and key of every keyed dispatch, and
   * lets the process die once, before or after a command of one type commits.
   */
  static final class DyingBus implements CommandBus {
    private final CommandBus delegate;
    private final List<String> presented;
    private final AtomicBoolean died = new AtomicBoolean();
    private volatile When when;
    private volatile Class<? extends Command> dyingAt;

    DyingBus(CommandBus delegate, List<String> presented) {
      this.delegate = delegate;
      this.presented = presented;
    }

    void dieOnce(When when, Class<? extends Command> command) {
      this.dyingAt = command;
      this.when = when;
    }

    boolean hasDied() {
      return died.get();
    }

    private void dieIf(When point, Command command) {
      Class<? extends Command> at = dyingAt;
      if (at != null && when == point && at.isInstance(command)) {
        dyingAt = null;
        died.set(true);
        throw new ProcessDeath(point.name().toLowerCase() + " " + at.getSimpleName() + " commits");
      }
    }

    @Override
    public <C extends Command> CommandResult execute(C command) {
      return delegate.execute(command);
    }

    @Override
    public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
      presented.add(command.getClass().getSimpleName() + " " + key.value());
      dieIf(When.BEFORE, command);
      CommandResult result = delegate.execute(command, key);
      dieIf(When.AFTER, command);
      return result;
    }

    @Override
    public boolean supportsIdempotentExecution() {
      return delegate.supportsIdempotentExecution();
    }
  }

  /**
   * A subscription's delivery loop, driven one event at a time: read the event after the stored
   * checkpoint, hand it to the listener, save the checkpoint. A listener that fails, or a process
   * that dies, leaves the checkpoint where it was.
   */
  static final class DeliveryLoop {
    private final ProjectionName name;
    private final PostgresEventStore store;
    private final PostgresOffsetStore offsets;
    private final EventListener listener;
    private Class<? extends DomainEvent> dyingBeforeTheCheckpointOf;

    DeliveryLoop(
        String name,
        PostgresEventStore store,
        PostgresOffsetStore offsets,
        EventListener listener) {
      this.name = ProjectionName.of(name);
      this.store = store;
      this.offsets = offsets;
      this.listener = listener;
    }

    /** The process dies once, after the listener returned for {@code event}, before the save. */
    void dieBeforeTheCheckpointOf(Class<? extends DomainEvent> event) {
      dyingBeforeTheCheckpointOf = event;
    }

    /** Delivers the next event; {@code false} at the head of the stream. */
    boolean deliverNext() {
      List<EventEnvelope> next = store.readGlobalStream(offsets.getLastOffset(name), 1);
      if (next.isEmpty()) {
        return false;
      }
      EventEnvelope event = next.getFirst();
      listener.onEvents(next);
      Class<? extends DomainEvent> at = dyingBeforeTheCheckpointOf;
      if (at != null && at.isInstance(event.event())) {
        dyingBeforeTheCheckpointOf = null;
        throw new ProcessDeath("before the checkpoint of " + at.getSimpleName());
      }
      offsets.saveOffset(name, event.globalOffset());
      return true;
    }
  }

  private static final String SAGA_SUBSCRIPTION = "fulfillment-saga";
  private static final String LISTENER_SUBSCRIPTION = "payment-process-manager";

  /** The clock of the sweeper and the timeout runner: ten minutes after the database's. */
  private static final Clock TEN_MINUTES_LATER =
      Clock.offset(Clock.systemUTC(), Duration.ofMinutes(10));

  private static final Duration BOUND = Duration.ofSeconds(60);

  private static final EventTypeRegistry REGISTRY =
      new EventTypeRegistry() {
        private final Map<EventType, Class<?>> types =
            Stream.of(
                    OrderPlaced.class,
                    StockReserved.class,
                    PaymentCaptured.class,
                    OrderShipped.class,
                    StockReleased.class,
                    PaymentRefunded.class,
                    CustomerPinged.class)
                .collect(
                    java.util.stream.Collectors.toMap(EventType::fromClass, Function.identity()));

        @Override
        public Class<?> resolveEventType(EventType eventType) {
          Class<?> type = types.get(eventType);
          if (type == null) {
            throw new IllegalArgumentException("Unknown event type: " + eventType);
          }
          return type;
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          throw new IllegalArgumentException("No aggregate snapshots in this test: " + stateType);
        }
      };

  private static ObjectMapper mapper() {
    return new ObjectMapper().registerModule(new JavaTimeModule());
  }

  /** Counts the cycles a sweeper has begun: it samples its backlog first thing in every cycle. */
  static final class CycleCounter implements StreamRuneMetrics {
    final AtomicInteger begun = new AtomicInteger();

    @Override
    public void recordSagaCompensatingBacklog(String sagaType, long count) {
      begun.incrementAndGet();
    }
  }

  /** One process: every in-memory object of the loop, built over the database. */
  final class Node implements AutoCloseable {
    final ObjectMapper mapper = mapper();
    final PostgresCommandInbox inbox = new PostgresCommandInbox(dataSource);
    final PostgresEventStore store =
        new PostgresEventStore(dataSource, mapper, REGISTRY, null, null, null, null, inbox);
    final PostgresSagaStore sagaStore = new PostgresSagaStore(dataSource, mapper);
    final PostgresSagaDeadLetterStore sagaDeadLetters = new PostgresSagaDeadLetterStore(dataSource);
    final PostgresOffsetStore offsets = new PostgresOffsetStore(dataSource);
    final VirtualThreadCommandBus realBus;
    final DyingBus bus;
    final FulfillmentSaga saga;
    final SagaRunner<Fulfillment> runner;
    final SagaDeadLetterReplayer replayer;
    final DeliveryLoop sagaLoop;

    /** {@code null} in a process that runs the saga without the listener. */
    final DeliveryLoop listenerLoop;

    Node(Faults faults, boolean withListener) {
      realBus =
          VirtualThreadCommandBus.builder()
              .eventStore(store)
              .locker(new LocalStripedLocker(16))
              .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
              .objectMapper(mapper)
              .commandInbox(inbox)
              .deadLetterQueue(new PostgresDeadLetterQueue(dataSource))
              .register(
                  AggregateType.of("stock"),
                  ReserveStock.class,
                  c -> AggregateId.of(c.orderId()),
                  new OneEvent<ReserveStock>(c -> new StockReserved(c.orderId()), faults))
              .register(
                  AggregateType.of("payment"),
                  CapturePayment.class,
                  c -> AggregateId.of(c.orderId()),
                  new OneEvent<CapturePayment>(c -> new PaymentCaptured(c.orderId()), faults))
              .register(
                  AggregateType.of("shipment"),
                  ShipOrder.class,
                  c -> AggregateId.of(c.orderId()),
                  new OneEvent<ShipOrder>(c -> new OrderShipped(c.orderId()), faults))
              .register(
                  AggregateType.of("release"),
                  ReleaseStock.class,
                  c -> AggregateId.of(c.orderId()),
                  new OneEvent<ReleaseStock>(c -> new StockReleased(c.orderId()), faults))
              .register(
                  AggregateType.of("refund"),
                  RefundPayment.class,
                  c -> AggregateId.of(c.orderId()),
                  new OneEvent<RefundPayment>(c -> new PaymentRefunded(c.orderId()), faults))
              .build();
      bus = new DyingBus(realBus, presented);
      saga = new FulfillmentSaga(faults);
      runner =
          SagaRunner.<Fulfillment>builder()
              .decider(saga)
              .startWhen(
                  event -> {
                    faults.beforeRouting(event.event());
                    return event.event() instanceof OrderPlaced;
                  })
              .extractSagaId(event -> sagaIdOf(((OrderPlaced) event.event()).orderId()))
              .correlateByEventData(
                  (streamId, event) ->
                      event instanceof CustomerPinged pinged
                          ? Optional.of(sagaIdOf(pinged.orderId()))
                          : Optional.empty())
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaDeadLetterStore(sagaDeadLetters)
              .build();
      replayer =
          SagaDeadLetterReplayer.builder()
              .sagaDeadLetterStore(sagaDeadLetters)
              .sagaStore(sagaStore)
              .eventStore(store)
              .sagaRunner(runner)
              .build();
      sagaLoop = new DeliveryLoop(SAGA_SUBSCRIPTION, store, offsets, runner.asEventListener());
      listenerLoop =
          withListener
              ? new DeliveryLoop(LISTENER_SUBSCRIPTION, store, offsets, processManager())
              : null;
    }

    /**
     * The listener of the loop, a process manager: it reacts to the saga's reply {@code
     * StockReserved} with a keyed {@code CapturePayment} inside the continuation of that reply.
     */
    EventListener processManager() {
      return batch -> {
        for (EventEnvelope event : batch) {
          if (event.event() instanceof StockReserved reserved) {
            StreamRuneContext.continuing(event)
                .run(
                    () ->
                        bus.execute(
                            new CapturePayment(reserved.orderId()),
                            IdempotencyKey.of("capture:" + event.metadata().eventId().value())));
          }
        }
      };
    }

    /** Delivers until every loop is at the head: the saga's next event, then the listener's. */
    void drain() {
      boolean progressed;
      do {
        progressed = sagaLoop.deliverNext();
        if (listenerLoop != null) {
          progressed |= listenerLoop.deliverNext();
        }
      } while (progressed);
    }

    /** Hands {@code event} to the saga runner outside its delivery loop. */
    void deliverToTheSaga(EventEnvelope event) {
      runner.asEventListener().onEvents(List.of(event));
    }

    private SagaCompensationRetrySweeper<Fulfillment> sweeper(StreamRuneMetrics metrics) {
      return SagaCompensationRetrySweeper.<Fulfillment>builder()
          .sagaRunner(runner)
          .sagaStore(sagaStore)
          .retryInterval(Duration.ofMillis(20))
          .giveUpAfter(Duration.ofHours(1))
          .clock(TEN_MINUTES_LATER)
          .metrics(metrics)
          .build();
    }

    /** Runs the compensation-retry sweeper until {@code done} holds. */
    void runTheSweeperUntil(BooleanSupplier done) {
      SagaCompensationRetrySweeper<Fulfillment> sweeper = sweeper(new CycleCounter());
      sweeper.start();
      try {
        await().atMost(BOUND).pollInterval(Duration.ofMillis(10)).until(done::getAsBoolean);
      } finally {
        sweeper.close();
      }
    }

    /** Runs the compensation-retry sweeper for {@code cycles} complete cycles. */
    void runTheSweeperFor(int cycles) {
      CycleCounter counter = new CycleCounter();
      SagaCompensationRetrySweeper<Fulfillment> sweeper = sweeper(counter);
      sweeper.start();
      try {
        // A cycle is complete once the next one has begun.
        await()
            .atMost(BOUND)
            .pollInterval(Duration.ofMillis(10))
            .until(() -> counter.begun.get() > cycles);
      } finally {
        sweeper.close();
      }
    }

    /** Runs the timeout runner until {@code done} holds or the process has died. */
    void runTheTimeoutRunnerUntil(BooleanSupplier done) {
      SagaTimeoutRunner<Fulfillment> timeouts =
          SagaTimeoutRunner.<Fulfillment>builder()
              .decider(saga)
              .sagaStore(sagaStore)
              .commandBus(bus)
              .sagaType(TYPE)
              .pollInterval(Duration.ofMillis(20))
              .clock(TEN_MINUTES_LATER)
              .build();
      timeouts.start();
      try {
        // The death is an uncaught Error on the runner's thread. It ends that thread, as it ends
        // the process; the wait must not take it for a failure of its own.
        await()
            .dontCatchUncaughtExceptions()
            .atMost(BOUND)
            .pollInterval(Duration.ofMillis(10))
            .until(() -> done.getAsBoolean() || bus.hasDied());
      } finally {
        timeouts.stop();
      }
    }

    @Override
    public void close() {
      realBus.close();
    }
  }

  // ---- what the database holds ----

  /** One stored event, with the ids of the run replaced by what they name. */
  record Stored(String event, String origin, String correlation, String causedBy) {}

  /**
   * Everything a run leaves behind for one order: its stored events in no particular order, the
   * saga's status, the event that opened its compensation episode, the keys in the command inbox,
   * and the sizes of the two dead-letter stores.
   */
  record Outcome(
      SagaStatus status,
      List<Stored> events,
      String episode,
      List<String> inboxKeys,
      long deadLetteredCommands,
      long sagaDeadLetters) {

    /** This outcome with one more stored event. */
    Outcome and(Stored event) {
      List<Stored> more = new ArrayList<>(events);
      more.add(event);
      more.sort(Comparator.comparing(Stored::toString));
      return new Outcome(status, more, episode, inboxKeys, deadLetteredCommands, sagaDeadLetters);
    }
  }

  private static final String NO_ORIGIN = "none";
  private static final String NO_CAUSE = "nothing";
  private static final String THE_REQUESTS = "the request's";
  private static final String THE_SAGAS = "the saga's";

  private static final Stored PLACED = new Stored("OrderPlaced", NO_ORIGIN, THE_REQUESTS, NO_CAUSE);
  private static final Stored PINGED =
      new Stored("CustomerPinged", NO_ORIGIN, THE_REQUESTS, NO_CAUSE);

  /** An event the saga caused: its origin at {@code hop}, the saga's reserved correlation id. */
  private static Stored causedByTheSaga(String event, int hop, String causedBy) {
    return new Stored(event, "this saga, hop " + hop, THE_SAGAS, causedBy);
  }

  private static List<Stored> sorted(Stored... events) {
    return Stream.of(events).sorted(Comparator.comparing(Stored::toString)).toList();
  }

  private final List<String> presented = new CopyOnWriteArrayList<>();
  private PostgresEventStore probe;
  private PostgresSagaStore sagaProbe;

  @BeforeEach
  void probes() throws Exception {
    probe = new PostgresEventStore(dataSource, mapper(), REGISTRY, null, null, null, null, null);
    sagaProbe = new PostgresSagaStore(dataSource, mapper());
    fresh();
  }

  /** An empty database: every case of a factory method starts from one. */
  private void fresh() throws Exception {
    cleanDatabase();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_dead_letters");
      stmt.execute("DELETE FROM saga_state");
      stmt.execute("DELETE FROM command_inbox");
      stmt.execute("DELETE FROM dead_letter_queue");
    }
    presented.clear();
  }

  private static String newOrder() {
    return "o-" + UUID.randomUUID().toString().substring(0, 8);
  }

  /** Appends {@code event} as a request stores it: no origin, a client's correlation id. */
  private EventEnvelope appendFromARequest(OrderEvent event) {
    StreamId stream =
        StreamId.of(AggregateType.of("request"), AggregateId.of("r-" + UUID.randomUUID()));
    EventEnvelope envelope =
        new EventEnvelope(
            GlobalOffset.initial(),
            stream,
            new Version(1),
            EventType.fromClass(event.getClass()),
            event,
            new EventMetadata(
                EventId.of("evt-" + UUID.randomUUID()),
                CommandId.of("cmd-" + UUID.randomUUID()),
                null,
                null,
                CorrelationId.of("request-" + event.orderId()),
                null,
                null,
                Instant.now()));
    long offset =
        probe
            .append(stream, List.of(envelope), Version.initial())
            .globalOffsets()
            .getFirst()
            .value();
    return probe.readGlobalStream(GlobalOffset.of(offset - 1), 1).getFirst();
  }

  /** Places an order and returns its id. */
  private String place(boolean cancelled) {
    String orderId = newOrder();
    appendFromARequest(new OrderPlaced(orderId, cancelled));
    return orderId;
  }

  private List<EventEnvelope> everything() {
    return probe.readGlobalStream(GlobalOffset.initial(), 10_000);
  }

  private List<EventEnvelope> eventsOf(String orderId) {
    return everything().stream()
        .filter(e -> ((OrderEvent) e.event()).orderId().equals(orderId))
        .toList();
  }

  /** The one stored event of {@code type} of the order. */
  private EventEnvelope the(String orderId, Class<? extends DomainEvent> type) {
    List<EventEnvelope> found =
        eventsOf(orderId).stream().filter(e -> type.isInstance(e.event())).toList();
    assertThat(found).as("%s of %s is stored once", type.getSimpleName(), orderId).hasSize(1);
    return found.getFirst();
  }

  /** The simple names of the order's stored events, in global order. */
  private List<String> names(String orderId) {
    return eventsOf(orderId).stream().map(e -> e.event().getClass().getSimpleName()).toList();
  }

  private Optional<LoadedSaga<Fulfillment>> row(String orderId) {
    return sagaProbe.load(sagaIdOf(orderId), TYPE, Fulfillment.class);
  }

  private SagaStatus status(String orderId) {
    return row(orderId).map(LoadedSaga::status).orElse(null);
  }

  private boolean ended(String orderId) {
    SagaStatus status = status(orderId);
    return status != null && status.isTerminal();
  }

  private List<String> column(String sql) {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql)) {
      List<String> values = new ArrayList<>();
      while (rs.next()) {
        values.add(rs.getString(1));
      }
      return values;
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private long count(String table) {
    return Long.parseLong(column("SELECT COUNT(*) FROM " + table).getFirst());
  }

  private void update(String sql, Object... parameters) {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(sql)) {
      for (int i = 0; i < parameters.length; i++) {
        ps.setObject(i + 1, parameters[i]);
      }
      assertThat(ps.executeUpdate()).as(sql).isEqualTo(1);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * The keys of the order in the command inbox, each named by what it is: the saga's forward key by
   * the event that triggered the command, its compensation key by its position, the listener's key
   * by the event it reacted to. A compensation key must carry the episode version of the saga row.
   */
  private List<String> inboxKeys(String orderId) {
    String ofTheSaga = "saga:" + sagaIdOf(orderId).value() + ":";
    Map<String, String> nameByOffset = new HashMap<>();
    Map<String, EventEnvelope> byId = new HashMap<>();
    for (EventEnvelope event : everything()) {
      nameByOffset.put(
          Long.toString(event.globalOffset().value()), event.event().getClass().getSimpleName());
      byId.put(event.metadata().eventId().value(), event);
    }
    List<String> named = new ArrayList<>();
    for (String key : column("SELECT idempotency_key FROM command_inbox")) {
      if (key.startsWith(ofTheSaga + "episode:")) {
        String[] parts = key.substring(ofTheSaga.length()).split(":");
        assertThat(Long.parseLong(parts[1]))
            .as("the compensation key %s carries the episode version of the row", key)
            .isEqualTo(row(orderId).orElseThrow().episodeVersion());
        named.add("compensation command " + parts[3]);
      } else if (key.startsWith(ofTheSaga)) {
        String[] parts = key.substring(ofTheSaga.length()).split(":");
        named.add("forward command " + parts[1] + " for " + nameByOffset.get(parts[0]));
      } else if (key.startsWith("capture:")) {
        EventEnvelope cause = byId.get(key.substring("capture:".length()));
        if (cause != null && ((OrderEvent) cause.event()).orderId().equals(orderId)) {
          named.add("the listener's command for " + cause.event().getClass().getSimpleName());
        }
      }
    }
    return named.stream().sorted().toList();
  }

  private Outcome outcome(String orderId) {
    SagaId sagaId = sagaIdOf(orderId);
    Map<String, String> nameById = new HashMap<>();
    for (EventEnvelope event : everything()) {
      nameById.put(event.metadata().eventId().value(), event.event().getClass().getSimpleName());
    }
    List<Stored> events = new ArrayList<>();
    for (EventEnvelope event : eventsOf(orderId)) {
      EventMetadata metadata = event.metadata();
      SagaOrigin origin = metadata.sagaOrigin();
      String originText;
      if (origin == null) {
        originText = NO_ORIGIN;
      } else if (origin.sagaType().equals(TYPE) && origin.sagaId().equals(sagaId)) {
        originText = "this saga, hop " + origin.hop();
      } else {
        originText = "another saga: " + origin;
      }
      String correlation = metadata.correlationId().value();
      if (correlation.equals("streamrune:saga:" + sagaId.value())) {
        correlation = THE_SAGAS;
      } else if (correlation.equals("request-" + orderId)) {
        correlation = THE_REQUESTS;
      }
      String causedBy =
          metadata.causationId() == null
              ? NO_CAUSE
              : nameById.getOrDefault(metadata.causationId().value(), "an event not in the store");
      events.add(
          new Stored(event.event().getClass().getSimpleName(), originText, correlation, causedBy));
    }
    events.sort(Comparator.comparing(Stored::toString));
    Optional<LoadedSaga<Fulfillment>> row = row(orderId);
    String episode;
    if (row.isEmpty() || row.get().episodeVersion() == null) {
      episode = "none";
    } else if (row.get().episodeCauseEventId() == null) {
      episode = "opened by no event";
    } else {
      episode = "opened by " + nameById.get(row.get().episodeCauseEventId().value());
    }
    return new Outcome(
        row.map(LoadedSaga::status).orElse(null),
        List.copyOf(events),
        episode,
        inboxKeys(orderId),
        count("dead_letter_queue"),
        count("saga_dead_letters"));
  }

  /**
   * Every compensation command any process of the case presented to the bus carried the key the
   * saga row's episode stamp gives: the call that opened the episode and every resume alike.
   */
  private void assertEveryCompensationKeyIsTheRowsStamp(String orderId) {
    LoadedSaga<Fulfillment> row = row(orderId).orElseThrow();
    String scope =
        "saga:" + sagaIdOf(orderId).value() + ":episode:" + row.episodeVersion() + ":comp:";
    List<String> compensations =
        presented.stream()
            .filter(key -> key.startsWith("ReleaseStock ") || key.startsWith("RefundPayment "))
            .toList();
    assertThat(compensations)
        .isNotEmpty()
        .containsOnly("ReleaseStock " + scope + 0, "RefundPayment " + scope + 1);
  }

  /** The stamp of the order's compensation episode: version, claim time, opening event. */
  private List<Object> stamp(String orderId) {
    LoadedSaga<Fulfillment> row = row(orderId).orElseThrow();
    List<Object> stamp = new ArrayList<>();
    stamp.add(row.episodeVersion());
    stamp.add(row.episodeClaimedAt());
    stamp.add(row.episodeCauseEventId());
    return stamp;
  }

  // ==== The forward loop ====

  private static final List<Stored> THE_LOOP =
      sorted(
          PLACED,
          causedByTheSaga("StockReserved", 0, "OrderPlaced"),
          causedByTheSaga("PaymentCaptured", 1, "StockReserved"),
          causedByTheSaga("OrderShipped", 0, "PaymentCaptured"));

  private static final String FORWARD_OF_THE_START = "forward command 0 for OrderPlaced";
  private static final String FORWARD_OF_THE_PAYMENT = "forward command 0 for PaymentCaptured";
  private static final String CAPTURE = "the listener's command for StockReserved";

  /** The loop run without an interruption, by the hand-driven delivery loops. */
  private Outcome theLoopUninterrupted() throws Exception {
    fresh();
    String order = place(false);
    try (Node node = new Node(Faults.none(), true)) {
      node.drain();
    }
    Outcome reference = outcome(order);
    assertThat(reference)
        .isEqualTo(
            new Outcome(
                SagaStatus.COMPLETED,
                THE_LOOP,
                "none",
                List.of(FORWARD_OF_THE_START, FORWARD_OF_THE_PAYMENT, CAPTURE),
                0,
                0));
    return reference;
  }

  /** A point of the forward loop where the process dies, and what it leaves in the database. */
  private record Death(String at, Consumer<Node> arm, List<String> events, List<String> keys) {}

  @TestFactory
  Stream<DynamicTest> theLoop_convergesWhenTheProcessDies() throws Exception {
    Outcome reference = theLoopUninterrupted();
    List<String> placed = List.of("OrderPlaced");
    List<String> reserved = List.of("OrderPlaced", "StockReserved");
    List<String> captured = List.of("OrderPlaced", "StockReserved", "PaymentCaptured");
    List<String> shipped =
        List.of("OrderPlaced", "StockReserved", "PaymentCaptured", "OrderShipped");
    List<String> afterTheStart = List.of(FORWARD_OF_THE_START);
    List<String> afterTheCapture = List.of(FORWARD_OF_THE_START, CAPTURE);
    List<String> afterTheShipment = List.of(FORWARD_OF_THE_START, FORWARD_OF_THE_PAYMENT, CAPTURE);
    List<Death> deaths =
        List.of(
            new Death(
                "after the saga row is inserted, before the saga's command commits",
                node -> node.bus.dieOnce(When.BEFORE, ReserveStock.class),
                placed,
                List.of()),
            new Death(
                "after the reply and its key commit, before the saga row advances",
                node -> node.bus.dieOnce(When.AFTER, ReserveStock.class),
                reserved,
                afterTheStart),
            new Death(
                "after the saga row advances, before the checkpoint of the start event",
                node -> node.sagaLoop.dieBeforeTheCheckpointOf(OrderPlaced.class),
                reserved,
                afterTheStart),
            new Death(
                "after the saga applies the reply, before its checkpoint",
                node -> node.sagaLoop.dieBeforeTheCheckpointOf(StockReserved.class),
                reserved,
                afterTheStart),
            new Death(
                "in the listener, before its command commits",
                node -> node.bus.dieOnce(When.BEFORE, CapturePayment.class),
                reserved,
                afterTheStart),
            new Death(
                "in the listener, after its command and key commit",
                node -> node.bus.dieOnce(When.AFTER, CapturePayment.class),
                captured,
                afterTheCapture),
            new Death(
                "in the listener, after it returned, before its checkpoint",
                node -> node.listenerLoop.dieBeforeTheCheckpointOf(StockReserved.class),
                captured,
                afterTheCapture),
            new Death(
                "after the saga reads the listener's event, before its next command commits",
                node -> node.bus.dieOnce(When.BEFORE, ShipOrder.class),
                captured,
                afterTheCapture),
            new Death(
                "after the saga's next command commits, before the saga row advances",
                node -> node.bus.dieOnce(When.AFTER, ShipOrder.class),
                shipped,
                afterTheShipment),
            new Death(
                "after the saga row advances, before the checkpoint of the listener's event",
                node -> node.sagaLoop.dieBeforeTheCheckpointOf(PaymentCaptured.class),
                shipped,
                afterTheShipment),
            new Death(
                "after the saga completes, before its last checkpoint",
                node -> node.sagaLoop.dieBeforeTheCheckpointOf(OrderShipped.class),
                shipped,
                afterTheShipment));
    return deaths.stream()
        .map(
            death ->
                dynamicTest(
                    death.at(),
                    () -> {
                      fresh();
                      String order = place(false);
                      try (Node first = new Node(Faults.none(), true)) {
                        death.arm().accept(first);
                        assertThatThrownBy(first::drain).isInstanceOf(ProcessDeath.class);
                      }
                      assertThat(names(order)).as("stored at the death").isEqualTo(death.events());
                      assertThat(inboxKeys(order))
                          .as("the keys recorded at the death")
                          .isEqualTo(death.keys().stream().sorted().toList());

                      try (Node second = new Node(Faults.none(), true)) {
                        second.drain();
                        assertThat(outcome(order)).isEqualTo(reference);

                        // Both loops are at the head: nothing is delivered, nothing is stored.
                        assertThat(second.sagaLoop.deliverNext()).isFalse();
                        assertThat(second.listenerLoop.deliverNext()).isFalse();
                      }
                      assertThat(outcome(order)).isEqualTo(reference);
                    }));
  }

  /** What the saga row holds at the two deaths inside the start event's step. */
  @Test
  void aDeathInsideTheStartStep_leavesTheGenesisPendingRow_andTheRedeliveryCommitsIt()
      throws Exception {
    String order = place(false);
    try (Node first = new Node(Faults.none(), true)) {
      first.bus.dieOnce(When.AFTER, ReserveStock.class);
      assertThatThrownBy(first::drain).isInstanceOf(ProcessDeath.class);
    }
    LoadedSaga<Fulfillment> pending = row(order).orElseThrow();
    assertThat(pending.genesisApplied()).as("the start step has not committed").isFalse();
    assertThat(pending.version()).isEqualTo(1L);
    assertThat(pending.lastAppliedOffset()).isNull();
    EventEnvelope reply = the(order, StockReserved.class);

    try (Node second = new Node(Faults.none(), true)) {
      assertThat(second.sagaLoop.deliverNext()).as("the start event is delivered again").isTrue();

      assertThat(the(order, StockReserved.class).metadata().eventId())
          .as("the saga's command is answered from the command inbox: the same stored reply")
          .isEqualTo(reply.metadata().eventId());
      LoadedSaga<Fulfillment> committed = row(order).orElseThrow();
      assertThat(committed.genesisApplied()).isTrue();
      assertThat(committed.version()).isEqualTo(2L);
      assertThat(committed.lastAppliedOffset())
          .isEqualTo(the(order, OrderPlaced.class).globalOffset().value());
    }
  }

  /**
   * The listener's dispatch fails with a failure a later attempt cures. Nothing is published to the
   * command dead letter queue, the failure leaves the listener, its checkpoint stays, and the next
   * delivery of the reply stores the listener's event once.
   */
  @Test
  void theListenersDispatchFails_nothingIsDeadLettered_andTheRedeliveryStoresItsEventOnce()
      throws Exception {
    Outcome reference = theLoopUninterrupted();
    fresh();
    String order = place(false);
    Faults paymentProviderDownOnce =
        Faults.none()
            .failing(
                CapturePayment.class,
                1,
                () -> new IllegalStateException("payment provider unreachable"));
    try (Node node = new Node(paymentProviderDownOnce, true)) {
      assertThatThrownBy(node::drain)
          .isInstanceOf(IllegalStateException.class)
          .hasMessage("payment provider unreachable");

      assertThat(count("dead_letter_queue")).isZero();
      assertThat(names(order)).containsExactly("OrderPlaced", "StockReserved");
      assertThat(node.offsets.getLastOffset(ProjectionName.of(LISTENER_SUBSCRIPTION)).value())
          .as("the listener's checkpoint stays before the reply")
          .isLessThan(the(order, StockReserved.class).globalOffset().value());

      node.drain();
    }
    assertThat(outcome(order)).isEqualTo(reference);
  }

  /**
   * The whole loop on two started polling subscriptions, one for the saga runner and one for the
   * listener: what they store equals what the hand-driven delivery loops store.
   */
  @Test
  void theLoop_onStartedPollingSubscriptions_storesWhatTheHandDrivenLoopsStore() throws Exception {
    Outcome reference = theLoopUninterrupted();
    fresh();
    try (Node node = new Node(Faults.none(), true);
        PollingEventSubscription sagaSubscription =
            new PollingEventSubscription(
                SubscriptionName.of(SAGA_SUBSCRIPTION),
                node.store,
                node.offsets,
                node.runner.asEventListener(),
                fastConfig(),
                50);
        PollingEventSubscription listenerSubscription =
            new PollingEventSubscription(
                SubscriptionName.of(LISTENER_SUBSCRIPTION),
                node.store,
                node.offsets,
                node.processManager(),
                fastConfig(),
                50)) {
      sagaSubscription.start();
      listenerSubscription.start();

      String order = place(false);

      await().atMost(BOUND).until(() -> status(order) == SagaStatus.COMPLETED);
      assertThat(outcome(order)).isEqualTo(reference);
    }
  }

  // ==== Compensation opened by an event ====

  private static final List<Stored> REJECTED_SHIPMENT =
      sorted(
          PLACED,
          causedByTheSaga("StockReserved", 0, "OrderPlaced"),
          causedByTheSaga("PaymentCaptured", 1, "StockReserved"),
          causedByTheSaga("StockReleased", 0, "PaymentCaptured"),
          causedByTheSaga("PaymentRefunded", 0, "PaymentCaptured"));

  private static final List<String> BOTH_COMPENSATIONS =
      List.of("compensation command 0", "compensation command 1");

  private static Faults shipmentRejected() {
    return Faults.none().rejecting(ShipOrder.class);
  }

  /** A path that resumes a claimed compensation episode in a process built after the death. */
  private record Resume(String by, ResumeStep step) {}

  private interface ResumeStep {
    void run(Node node, String order);
  }

  private Resume byTheSweeper() {
    return new Resume(
        "the compensation-retry sweeper",
        (node, order) -> node.runTheSweeperUntil(() -> ended(order)));
  }

  private Resume byTheTimeoutRunner() {
    return new Resume(
        "the timeout runner", (node, order) -> node.runTheTimeoutRunnerUntil(() -> ended(order)));
  }

  @TestFactory
  Stream<DynamicTest> aCompensationOpenedByAnEvent_convergesOnEveryResumePath() throws Exception {
    fresh();
    String uninterrupted = place(false);
    try (Node node = new Node(shipmentRejected(), true)) {
      node.drain();
    }
    Outcome reference = outcome(uninterrupted);
    assertThat(reference)
        .isEqualTo(
            new Outcome(
                SagaStatus.COMPENSATED,
                REJECTED_SHIPMENT,
                "opened by PaymentCaptured",
                Stream.concat(BOTH_COMPENSATIONS.stream(), Stream.of(FORWARD_OF_THE_START, CAPTURE))
                    .sorted()
                    .toList(),
                0,
                0));
    assertEveryCompensationKeyIsTheRowsStamp(uninterrupted);

    Map<String, Consumer<Node>> deaths = new java.util.LinkedHashMap<>();
    deaths.put(
        "after the claim, before the first compensation command commits",
        node -> node.bus.dieOnce(When.BEFORE, ReleaseStock.class));
    deaths.put(
        "after the first compensation command commits",
        node -> node.bus.dieOnce(When.AFTER, ReleaseStock.class));
    deaths.put(
        "after the last compensation command commits, before the terminal write",
        node -> node.bus.dieOnce(When.AFTER, RefundPayment.class));
    deaths.put(
        "after the terminal write, before the checkpoint",
        node -> node.sagaLoop.dieBeforeTheCheckpointOf(PaymentCaptured.class));
    List<Resume> resumes =
        List.of(
            new Resume("the redelivery of the event", (node, order) -> node.drain()),
            new Resume(
                "a later event of the saga",
                (node, order) ->
                    node.deliverToTheSaga(appendFromARequest(new CustomerPinged(order)))),
            byTheSweeper(),
            byTheTimeoutRunner());

    List<DynamicTest> cases = new ArrayList<>();
    deaths.forEach(
        (at, arm) ->
            resumes.forEach(
                resume ->
                    cases.add(
                        dynamicTest(
                            "dies " + at + "; resumed by " + resume.by(),
                            () -> {
                              fresh();
                              String order = place(false);
                              try (Node first = new Node(shipmentRejected(), true)) {
                                arm.accept(first);
                                assertThatThrownBy(first::drain).isInstanceOf(ProcessDeath.class);
                              }
                              EventId opening =
                                  the(order, PaymentCaptured.class).metadata().eventId();
                              LoadedSaga<Fulfillment> claimed = row(order).orElseThrow();
                              assertThat(claimed.episodeVersion())
                                  .as("the claim stamped the episode")
                                  .isNotNull();
                              assertThat(claimed.episodeCauseEventId()).isEqualTo(opening);
                              List<Object> stampAtTheDeath = stamp(order);

                              try (Node second = new Node(shipmentRejected(), true)) {
                                resume.step().run(second, order);
                                assertThat(status(order)).isEqualTo(SagaStatus.COMPENSATED);
                                second.drain();
                              }

                              boolean pinged = names(order).contains("CustomerPinged");
                              assertThat(outcome(order))
                                  .isEqualTo(pinged ? reference.and(PINGED) : reference);
                              assertThat(stamp(order))
                                  .as("no resume rewrites the episode stamp")
                                  .isEqualTo(stampAtTheDeath);
                              assertEveryCompensationKeyIsTheRowsStamp(order);
                            }))));
    return cases.stream();
  }

  // ==== Compensation opened by the timeout runner ====

  private static final List<Stored> DEADLINE_PASSED =
      sorted(
          PLACED,
          causedByTheSaga("StockReserved", 0, "OrderPlaced"),
          causedByTheSaga("StockReleased", 0, NO_CAUSE),
          causedByTheSaga("PaymentRefunded", 0, NO_CAUSE),
          // The listener comes up after the saga ended: its continuation is stored all the same.
          causedByTheSaga("PaymentCaptured", 1, "StockReserved"));

  /** Places an order and runs the start step only: the saga waits for a payment nobody captures. */
  private String placeAndStart(Node node) {
    String order = place(false);
    assertThat(node.sagaLoop.deliverNext()).isTrue();
    assertThat(status(order)).isEqualTo(SagaStatus.RUNNING);
    return order;
  }

  @TestFactory
  Stream<DynamicTest> aCompensationOpenedByTheTimeoutRunner_convergesOnEveryResumePath()
      throws Exception {
    fresh();
    String uninterrupted;
    try (Node node = new Node(Faults.none(), true)) {
      uninterrupted = placeAndStart(node);
      String order = uninterrupted;
      node.runTheTimeoutRunnerUntil(() -> ended(order));
      node.drain();
    }
    Outcome reference = outcome(uninterrupted);
    assertThat(reference)
        .isEqualTo(
            new Outcome(
                SagaStatus.COMPENSATED,
                DEADLINE_PASSED,
                "opened by no event",
                Stream.concat(BOTH_COMPENSATIONS.stream(), Stream.of(FORWARD_OF_THE_START, CAPTURE))
                    .sorted()
                    .toList(),
                0,
                0));
    assertEveryCompensationKeyIsTheRowsStamp(uninterrupted);

    Map<String, Consumer<Node>> deaths = new java.util.LinkedHashMap<>();
    deaths.put(
        "after the claim, before the first compensation command commits",
        node -> node.bus.dieOnce(When.BEFORE, ReleaseStock.class));
    deaths.put(
        "after the first compensation command commits",
        node -> node.bus.dieOnce(When.AFTER, ReleaseStock.class));
    deaths.put(
        "after the last compensation command commits, before the terminal write",
        node -> node.bus.dieOnce(When.AFTER, RefundPayment.class));
    List<Resume> resumes =
        List.of(
            new Resume(
                "the timeout runner, which picks the saga again",
                (node, order) -> node.runTheTimeoutRunnerUntil(() -> ended(order))),
            byTheSweeper(),
            // The saga's own reply is the next event of its delivery loop.
            new Resume(
                "a later event of the saga",
                (node, order) -> assertThat(node.sagaLoop.deliverNext()).isTrue()));

    List<DynamicTest> cases = new ArrayList<>();
    deaths.forEach(
        (at, arm) ->
            resumes.forEach(
                resume ->
                    cases.add(
                        dynamicTest(
                            "the timeout runner dies " + at + "; resumed by " + resume.by(),
                            () -> {
                              fresh();
                              String order;
                              try (Node first = new Node(Faults.none(), true)) {
                                order = placeAndStart(first);
                                arm.accept(first);
                                first.runTheTimeoutRunnerUntil(() -> false);
                                assertThat(first.bus.hasDied()).isTrue();
                              }
                              LoadedSaga<Fulfillment> claimed = row(order).orElseThrow();
                              assertThat(claimed.status()).isEqualTo(SagaStatus.COMPENSATING);
                              assertThat(claimed.episodeVersion()).isNotNull();
                              assertThat(claimed.episodeCauseEventId())
                                  .as("a timeout claim names no event")
                                  .isNull();
                              List<Object> stampAtTheDeath = stamp(order);

                              try (Node second = new Node(Faults.none(), true)) {
                                resume.step().run(second, order);
                                assertThat(status(order)).isEqualTo(SagaStatus.COMPENSATED);
                                second.drain();
                              }

                              assertThat(outcome(order)).isEqualTo(reference);
                              assertThat(stamp(order))
                                  .as("no resume rewrites the episode stamp")
                                  .isEqualTo(stampAtTheDeath);
                              assertEveryCompensationKeyIsTheRowsStamp(order);
                            }))));
    return cases.stream();
  }

  // ==== A start event whose evolve yields COMPENSATING ====

  private static final List<Stored> CANCELLED_ORDER =
      sorted(
          PLACED,
          causedByTheSaga("StockReserved", 0, "OrderPlaced"),
          causedByTheSaga("OrderShipped", 0, "OrderPlaced"),
          causedByTheSaga("StockReleased", 0, "OrderPlaced"),
          causedByTheSaga("PaymentRefunded", 0, "OrderPlaced"));

  /** How the forward loop of the start event is interrupted, and the process that resumes it. */
  private record Interruption(
      String at, Supplier<Faults> faults, Consumer<Node> arm, boolean episodeEntered) {}

  /** The row while the forward loop of the start event has not committed. */
  private void assertForwardRowOfTheStart(String order) {
    LoadedSaga<Fulfillment> row = row(order).orElseThrow();
    assertThat(row.status())
        .as("a forward status until the forward loop ends")
        .isEqualTo(SagaStatus.STARTED);
    assertThat(row.state().status()).as("the evolved state").isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.genesisApplied()).isFalse();
    assertThat(row.version()).isEqualTo(1L);
    assertThat(row.episodeVersion()).as("no episode yet").isNull();
    assertThat(row.episodeClaimedAt()).isNull();
    assertThat(row.episodeCauseEventId()).isNull();
  }

  /** The row once the write after the forward loop has landed, and nothing has compensated. */
  private void assertEpisodeEnteredByTheStart(String order) {
    LoadedSaga<Fulfillment> row = row(order).orElseThrow();
    assertThat(row.status()).isEqualTo(SagaStatus.COMPENSATING);
    assertThat(row.genesisApplied()).isTrue();
    assertThat(row.episodeVersion())
        .as("stamped by the write that landed COMPENSATING")
        .isEqualTo(row.version());
    assertThat(row.episodeClaimedAt()).isNotNull();
    assertThat(row.episodeCauseEventId())
        .isEqualTo(the(order, OrderPlaced.class).metadata().eventId());
    assertThat(names(order))
        .as("every forward command ran; nothing is compensated yet")
        .filteredOn(name -> !name.equals("CustomerPinged"))
        .containsExactly("OrderPlaced", "StockReserved", "OrderShipped");
  }

  @TestFactory
  Stream<DynamicTest> aStartThatEvolvesToCompensating_convergesWhateverActsFirst()
      throws Exception {
    fresh();
    String uninterrupted = place(true);
    try (Node node = new Node(Faults.none(), false)) {
      assertThat(node.sagaLoop.deliverNext()).isTrue();
      assertEpisodeEnteredByTheStart(uninterrupted);
      node.drain();
    }
    Outcome reference = outcome(uninterrupted);
    assertThat(reference)
        .isEqualTo(
            new Outcome(
                SagaStatus.COMPENSATED,
                CANCELLED_ORDER,
                "opened by OrderPlaced",
                Stream.concat(
                        BOTH_COMPENSATIONS.stream(),
                        Stream.of(FORWARD_OF_THE_START, "forward command 1 for OrderPlaced"))
                    .sorted()
                    .toList(),
                0,
                0));

    List<Interruption> interruptions =
        List.of(
            new Interruption(
                "the process dies after the row is inserted, before a forward command commits",
                Faults::none,
                node -> node.bus.dieOnce(When.BEFORE, ReserveStock.class),
                false),
            new Interruption(
                "the process dies after the first forward command commits",
                Faults::none,
                node -> node.bus.dieOnce(When.AFTER, ReserveStock.class),
                false),
            new Interruption(
                "the process dies after the last forward command commits, before the write that"
                    + " enters the episode",
                Faults::none,
                node -> node.bus.dieOnce(When.AFTER, ShipOrder.class),
                false),
            new Interruption(
                "a forward command fails with a failure worth retrying",
                () ->
                    Faults.none()
                        .failing(
                            ShipOrder.class,
                            1,
                            () ->
                                new IllegalStateException(
                                    "warehouse unreachable",
                                    new SQLException("connection refused"))),
                node -> {},
                false),
            new Interruption(
                "the process dies after the write that enters the episode, before the checkpoint",
                Faults::none,
                node -> node.sagaLoop.dieBeforeTheCheckpointOf(OrderPlaced.class),
                true));

    List<DynamicTest> cases = new ArrayList<>();
    for (Interruption interruption : interruptions) {
      cases.add(
          dynamicTest(
              interruption.at() + "; the start event is delivered again first",
              () -> {
                String order = interrupted(interruption);
                try (Node second = new Node(Faults.none(), false)) {
                  assertThat(second.sagaLoop.deliverNext()).isTrue();
                  if (interruption.episodeEntered()) {
                    assertThat(status(order))
                        .as("a start event over a compensating row resumes its episode")
                        .isEqualTo(SagaStatus.COMPENSATED);
                  } else {
                    assertEpisodeEnteredByTheStart(order);
                  }
                  second.drain();
                }
                assertConvergedOnTheCancelledOrder(order, reference);
              }));
      cases.add(
          dynamicTest(
              interruption.at() + "; the compensation-retry sweeper runs first",
              () -> {
                String order = interrupted(interruption);
                try (Node second = new Node(Faults.none(), false)) {
                  if (!interruption.episodeEntered()) {
                    List<String> storedAtTheInterruption = names(order);
                    second.runTheSweeperFor(2);
                    assertForwardRowOfTheStart(order);
                    assertThat(names(order))
                        .as("the sweeper sees no compensation episode while the forward loop runs")
                        .isEqualTo(storedAtTheInterruption);
                    assertThat(second.sagaLoop.deliverNext()).isTrue();
                    assertEpisodeEnteredByTheStart(order);
                  }
                  second.runTheSweeperUntil(() -> ended(order));
                  assertThat(status(order)).isEqualTo(SagaStatus.COMPENSATED);
                  second.drain();
                }
                assertConvergedOnTheCancelledOrder(order, reference);
              }));
      cases.add(
          dynamicTest(
              interruption.at() + "; a later event of the saga arrives first",
              () -> {
                String order = interrupted(interruption);
                try (Node second = new Node(Faults.none(), false)) {
                  EventEnvelope pinged = appendFromARequest(new CustomerPinged(order));
                  second.deliverToTheSaga(pinged);
                  if (interruption.episodeEntered()) {
                    assertThat(status(order))
                        .as("an event for a compensating saga resumes its episode")
                        .isEqualTo(SagaStatus.COMPENSATED);
                  } else {
                    assertThat(
                            column(
                                "SELECT error_message FROM saga_dead_letters WHERE event_offset = "
                                    + pinged.globalOffset().value()))
                        .as("held until the start step has committed")
                        .singleElement()
                        .asString()
                        .contains("GENESIS_PENDING");
                    assertThat(row(order).orElseThrow().status()).isEqualTo(SagaStatus.STARTED);
                    assertThat(row(order).orElseThrow().genesisApplied()).isFalse();

                    assertThat(second.sagaLoop.deliverNext()).isTrue();
                    assertEpisodeEnteredByTheStart(order);

                    assertThat(second.replayer.replayAll(sagaIdOf(order)))
                        .as("the held event is fed, and resumes the episode")
                        .containsExactly(ReplayOutcome.REPLAYED);
                    assertThat(status(order)).isEqualTo(SagaStatus.COMPENSATED);
                  }
                  second.drain();
                }
                assertConvergedOnTheCancelledOrder(order, reference.and(PINGED));
              }));
      cases.add(
          dynamicTest(
              interruption.at() + "; the saga's deadline passes first",
              () -> {
                String order = interrupted(interruption);
                List<String> storedAtTheInterruption = names(order);
                try (Node second = new Node(Faults.none(), false)) {
                  second.runTheTimeoutRunnerUntil(() -> ended(order));
                  assertThat(status(order)).isEqualTo(SagaStatus.COMPENSATED);
                  second.drain();
                }
                if (interruption.episodeEntered()) {
                  // The timeout runner picked a claimed episode: it resumed the start event's.
                  assertConvergedOnTheCancelledOrder(order, reference);
                  return;
                }
                // The timeout runner claimed a start whose forward loop had not committed: the
                // saga's deadline bounds it. The episode is the runner's own, opened by no event,
                // and the forward commands that had not run are not dispatched afterwards.
                List<Stored> expected = new ArrayList<>();
                expected.add(PLACED);
                for (String forward :
                    storedAtTheInterruption.subList(1, storedAtTheInterruption.size())) {
                  expected.add(causedByTheSaga(forward, 0, "OrderPlaced"));
                }
                expected.add(causedByTheSaga("StockReleased", 0, NO_CAUSE));
                expected.add(causedByTheSaga("PaymentRefunded", 0, NO_CAUSE));
                Outcome claimedAtTheDeadline = outcome(order);
                assertThat(claimedAtTheDeadline.status()).isEqualTo(SagaStatus.COMPENSATED);
                assertThat(claimedAtTheDeadline.episode()).isEqualTo("opened by no event");
                assertThat(claimedAtTheDeadline.events())
                    .isEqualTo(
                        expected.stream().sorted(Comparator.comparing(Stored::toString)).toList());
                List<String> keys = new ArrayList<>(BOTH_COMPENSATIONS);
                for (int i = 0; i < storedAtTheInterruption.size() - 1; i++) {
                  keys.add("forward command " + i + " for OrderPlaced");
                }
                assertThat(claimedAtTheDeadline.inboxKeys())
                    .isEqualTo(keys.stream().sorted().toList());
                assertThat(claimedAtTheDeadline.deadLetteredCommands()).isZero();
                assertThat(claimedAtTheDeadline.sagaDeadLetters()).isZero();
                assertEveryCompensationKeyIsTheRowsStamp(order);
              }));
    }
    return cases.stream();
  }

  /** Places a cancelled order and interrupts the forward loop of its start event. */
  private String interrupted(Interruption interruption) throws Exception {
    fresh();
    String order = place(true);
    try (Node first = new Node(interruption.faults().get(), false)) {
      interruption.arm().accept(first);
      assertThatThrownBy(first.sagaLoop::deliverNext)
          .isInstanceOfAny(ProcessDeath.class, IllegalStateException.class);
    }
    if (interruption.episodeEntered()) {
      assertEpisodeEnteredByTheStart(order);
    } else {
      assertForwardRowOfTheStart(order);
    }
    return order;
  }

  private void assertConvergedOnTheCancelledOrder(String order, Outcome expected) {
    assertThat(outcome(order)).isEqualTo(expected);
    assertEveryCompensationKeyIsTheRowsStamp(order);
    assertThat(row(order).orElseThrow().episodeCauseEventId())
        .isEqualTo(the(order, OrderPlaced.class).metadata().eventId());
    long lastForward =
        Math.max(
            the(order, StockReserved.class).globalOffset().value(),
            the(order, OrderShipped.class).globalOffset().value());
    assertThat(the(order, StockReleased.class).globalOffset().value())
        .as("every forward effect is stored before its compensation")
        .isGreaterThan(lastForward);
    assertThat(the(order, PaymentRefunded.class).globalOffset().value()).isGreaterThan(lastForward);
  }

  /**
   * The cancelled order again, and its second forward command is rejected: the claim that follows
   * is the write that stamps the episode, with the start event as its cause, over a row whose start
   * step never commits. The executor reads that stamp back from the row, so the delivery that
   * opened the episode and every path that resumes it present the same compensation keys.
   */
  @TestFactory
  Stream<DynamicTest> aStartThatEvolvesToCompensating_whoseForwardCommandIsRejected_converges()
      throws Exception {
    fresh();
    String uninterrupted = place(true);
    try (Node node = new Node(shipmentRejected(), false)) {
      node.drain();
    }
    Outcome reference = outcome(uninterrupted);
    assertThat(reference)
        .isEqualTo(
            new Outcome(
                SagaStatus.COMPENSATED,
                sorted(
                    PLACED,
                    causedByTheSaga("StockReserved", 0, "OrderPlaced"),
                    causedByTheSaga("StockReleased", 0, "OrderPlaced"),
                    causedByTheSaga("PaymentRefunded", 0, "OrderPlaced")),
                "opened by OrderPlaced",
                Stream.concat(BOTH_COMPENSATIONS.stream(), Stream.of(FORWARD_OF_THE_START))
                    .sorted()
                    .toList(),
                0,
                0));
    assertEveryCompensationKeyIsTheRowsStamp(uninterrupted);

    Map<String, Consumer<Node>> deaths = new java.util.LinkedHashMap<>();
    deaths.put(
        "after the claim, before the first compensation command commits",
        node -> node.bus.dieOnce(When.BEFORE, ReleaseStock.class));
    deaths.put(
        "after the first compensation command commits",
        node -> node.bus.dieOnce(When.AFTER, ReleaseStock.class));
    deaths.put(
        "after the last compensation command commits, before the terminal write",
        node -> node.bus.dieOnce(When.AFTER, RefundPayment.class));
    deaths.put(
        "after the terminal write, before the checkpoint",
        node -> node.sagaLoop.dieBeforeTheCheckpointOf(OrderPlaced.class));
    List<Resume> resumes =
        List.of(
            new Resume("the redelivery of the start event", (node, order) -> node.drain()),
            new Resume(
                "a later event of the saga",
                (node, order) ->
                    node.deliverToTheSaga(appendFromARequest(new CustomerPinged(order)))),
            byTheSweeper(),
            byTheTimeoutRunner());

    List<DynamicTest> cases = new ArrayList<>();
    deaths.forEach(
        (at, arm) ->
            resumes.forEach(
                resume ->
                    cases.add(
                        dynamicTest(
                            "dies " + at + "; resumed by " + resume.by(),
                            () -> {
                              fresh();
                              String order = place(true);
                              try (Node first = new Node(shipmentRejected(), false)) {
                                arm.accept(first);
                                assertThatThrownBy(first::drain).isInstanceOf(ProcessDeath.class);
                              }
                              LoadedSaga<Fulfillment> claimed = row(order).orElseThrow();
                              assertThat(claimed.genesisApplied())
                                  .as("the start step never commits: the claim took the row over")
                                  .isFalse();
                              assertThat(claimed.episodeVersion())
                                  .as("the claim over the inserted row stamped the episode")
                                  .isEqualTo(2L);
                              assertThat(claimed.episodeCauseEventId())
                                  .isEqualTo(the(order, OrderPlaced.class).metadata().eventId());
                              List<Object> stampAtTheDeath = stamp(order);

                              try (Node second = new Node(shipmentRejected(), false)) {
                                resume.step().run(second, order);
                                assertThat(status(order)).isEqualTo(SagaStatus.COMPENSATED);
                                second.drain();
                              }

                              boolean pinged = names(order).contains("CustomerPinged");
                              assertThat(outcome(order))
                                  .isEqualTo(pinged ? reference.and(PINGED) : reference);
                              assertThat(stamp(order))
                                  .as("no resume rewrites the episode stamp")
                                  .isEqualTo(stampAtTheDeath);
                              assertEveryCompensationKeyIsTheRowsStamp(order);
                            }))));
    return cases.stream();
  }

  // ==== The saga dead-letter replayer ====

  /**
   * The saga cannot read its first reply: the reply is quarantined and the saga faults. The
   * listener's event arrives meanwhile and is held behind the fault. Both entries are stored as
   * (saga id, event offset); the replayer reads each event from the store and feeds it to the saga
   * its origin names.
   *
   * @return the order
   */
  private String aQuarantinedReplyAndTheReplyHeldBehindIt() {
    String order = place(false);
    try (Node first = new Node(Faults.none().unableToRead(StockReserved.class), true)) {
      first.drain();
    }
    assertThat(status(order)).isEqualTo(SagaStatus.FAULTED);
    assertThat(names(order)).containsExactly("OrderPlaced", "StockReserved", "PaymentCaptured");
    assertThat(
            column(
                "SELECT event_offset FROM saga_dead_letters WHERE saga_id = '"
                    + sagaIdOf(order).value()
                    + "' ORDER BY event_offset"))
        .containsExactly(
            Long.toString(the(order, StockReserved.class).globalOffset().value()),
            Long.toString(the(order, PaymentCaptured.class).globalOffset().value()));
    return order;
  }

  @Test
  void aQuarantinedReplyAndTheReplyHeldBehindIt_areFedToTheSagaTheirOriginNames() throws Exception {
    Outcome reference = theLoopUninterrupted();
    fresh();
    String order = aQuarantinedReplyAndTheReplyHeldBehindIt();

    try (Node second = new Node(Faults.none(), true)) {
      assertThat(second.replayer.replayAll(sagaIdOf(order)))
          .containsExactly(ReplayOutcome.REPLAYED, ReplayOutcome.REPLAYED);

      assertThat(names(order))
          .as("the feed of the listener's event dispatched the saga's next command")
          .containsExactly("OrderPlaced", "StockReserved", "PaymentCaptured", "OrderShipped");
      assertThat(status(order)).isEqualTo(SagaStatus.RUNNING);

      second.drain();
    }
    assertThat(outcome(order)).isEqualTo(reference);
  }

  /**
   * The process dies inside the feed, after the saga's next command committed. Its reply arrives
   * live while the saga still owns an entry and is held behind it; a second drain feeds both. The
   * command of the feed is dispatched under the same forward key by both drains and is stored once.
   */
  @Test
  void aDeathInsideTheReplayFeed_isResumedByTheNextDrain_underTheSameForwardKey() throws Exception {
    Outcome reference = theLoopUninterrupted();
    fresh();
    String order = aQuarantinedReplyAndTheReplyHeldBehindIt();

    try (Node second = new Node(Faults.none(), true)) {
      second.bus.dieOnce(When.AFTER, ShipOrder.class);
      assertThatThrownBy(() -> second.replayer.replayAll(sagaIdOf(order)))
          .isInstanceOf(ProcessDeath.class);
    }
    assertThat(names(order))
        .containsExactly("OrderPlaced", "StockReserved", "PaymentCaptured", "OrderShipped");
    assertThat(count("saga_dead_letters")).as("the entry of the interrupted feed stays").isOne();

    try (Node third = new Node(Faults.none(), true)) {
      third.drain();
      assertThat(count("saga_dead_letters"))
          .as("the live reply is held behind the entry the saga still owns")
          .isEqualTo(2);
      assertThat(status(order)).isEqualTo(SagaStatus.RUNNING);

      assertThat(third.replayer.replayAll(sagaIdOf(order)))
          .containsExactly(ReplayOutcome.REPLAYED, ReplayOutcome.REPLAYED);
      third.drain();
    }
    assertThat(outcome(order)).isEqualTo(reference);
    String forwardKey =
        "ShipOrder saga:"
            + sagaIdOf(order).value()
            + ":"
            + the(order, PaymentCaptured.class).globalOffset().value()
            + ":0";
    assertThat(presented.stream().filter(key -> key.startsWith("ShipOrder ")))
        .as("both feeds presented the forward key of the listener's event")
        .containsExactly(forwardKey, forwardKey);
  }

  /**
   * The saga's router throws on the listener's event, so the event is quarantined before it is
   * routed: its entry names no saga. Replayed, the event is read from the store and fed to the saga
   * its origin names — nothing else on the entry or in the event's data names one.
   */
  @Test
  void aReplyQuarantinedBeforeItWasRouted_isFedToTheSagaItsOriginNames() throws Exception {
    Outcome reference = theLoopUninterrupted();
    fresh();
    String order = place(false);
    try (Node first = new Node(Faults.none().unableToRoute(PaymentCaptured.class), true)) {
      first.drain();
    }
    EventEnvelope unrouted = the(order, PaymentCaptured.class);
    assertThat(
            column(
                "SELECT saga_id FROM saga_dead_letters WHERE event_offset = "
                    + unrouted.globalOffset().value()))
        .as("one entry, and it names no saga")
        .containsExactly((String) null);
    LoadedSaga<Fulfillment> waiting = row(order).orElseThrow();
    assertThat(waiting.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(waiting.state().paid()).isFalse();

    try (Node second = new Node(Faults.none(), true)) {
      assertThat(second.replayer.replay(null, unrouted.globalOffset()))
          .isEqualTo(ReplayOutcome.REPLAYED);

      assertThat(row(order).orElseThrow().state().paid()).isTrue();
      assertThat(names(order))
          .as("the feed dispatched the saga's next command")
          .containsExactly("OrderPlaced", "StockReserved", "PaymentCaptured", "OrderShipped");

      second.drain();
    }
    assertThat(outcome(order)).isEqualTo(reference);
  }

  // ==== Routing reads the origin, never the correlation id ====

  /**
   * Two sagas wait for the reply to their own {@code ReserveStock}. The stored correlation id of
   * one reply is rewritten to the reserved correlation id of the other saga; its origin is intact.
   * The reply reaches the saga its origin names, and the saga its correlation id names is not
   * touched.
   */
  @Test
  void aReplyWhoseCorrelationIdNamesAnotherSaga_reachesTheSagaItsOriginNames() throws Exception {
    Outcome reference = theLoopUninterrupted();
    fresh();
    String other = place(false);
    String order = place(false);
    try (Node node = new Node(Faults.none(), true)) {
      assertThat(node.sagaLoop.deliverNext()).isTrue();
      assertThat(node.sagaLoop.deliverNext()).isTrue();
      EventEnvelope reply = the(order, StockReserved.class);
      String theOthers = "streamrune:saga:" + sagaIdOf(other).value();
      update(
          "UPDATE event_stream SET metadata = jsonb_set(metadata, '{correlationId}', to_jsonb(?))"
              + " WHERE global_offset = ?",
          theOthers,
          reply.globalOffset().value());
      EventEnvelope altered = the(order, StockReserved.class);
      assertThat(altered.metadata().correlationId()).isEqualTo(CorrelationId.of(theOthers));
      assertThat(altered.metadata().sagaOrigin())
          .isEqualTo(new SagaOrigin(TYPE, sagaIdOf(order), 0));

      // The other saga's own reply is the next event of the loop; the altered one follows.
      assertThat(node.sagaLoop.deliverNext()).isTrue();
      LoadedSaga<Fulfillment> theOtherBefore = row(other).orElseThrow();
      assertThat(theOtherBefore.lastAppliedOffset())
          .isEqualTo(the(other, StockReserved.class).globalOffset().value());
      assertThat(row(order).orElseThrow().state().reserved()).isFalse();

      assertThat(node.sagaLoop.deliverNext()).isTrue();

      LoadedSaga<Fulfillment> routed = row(order).orElseThrow();
      assertThat(routed.state().reserved()).as("the origin decides").isTrue();
      assertThat(routed.lastAppliedOffset()).isEqualTo(altered.globalOffset().value());
      assertThat(row(other).orElseThrow())
          .as("the saga the correlation id names is not touched")
          .isEqualTo(theOtherBefore);

      node.drain();
    }
    assertThat(outcome(other)).isEqualTo(reference);
    Outcome withTheAlteredReply = outcome(order);
    assertThat(withTheAlteredReply.status()).isEqualTo(SagaStatus.COMPLETED);
    assertThat(withTheAlteredReply.events())
        .as(
            "the altered reply is stored as altered; its continuation carries the correlation id"
                + " of the origin again")
        .isEqualTo(
            sorted(
                PLACED,
                new Stored(
                    "StockReserved",
                    "this saga, hop 0",
                    "streamrune:saga:" + sagaIdOf(other).value(),
                    "OrderPlaced"),
                causedByTheSaga("PaymentCaptured", 1, "StockReserved"),
                causedByTheSaga("OrderShipped", 0, "PaymentCaptured")));
    assertThat(withTheAlteredReply.inboxKeys()).isEqualTo(reference.inboxKeys());
  }

  /**
   * The reverse: the origin of a stored reply is removed and its reserved correlation id is intact.
   * The reply reaches no saga, a listener cannot continue it, and the saga ends through its
   * timeout.
   */
  @Test
  void aReplyWhoseOriginIsRemoved_reachesNoSaga_whateverItsCorrelationId() throws Exception {
    String order = place(false);
    try (Node node = new Node(Faults.none(), true)) {
      assertThat(node.sagaLoop.deliverNext()).isTrue();
      EventEnvelope reply = the(order, StockReserved.class);
      update(
          "UPDATE event_stream SET metadata = metadata - 'sagaOrigin' WHERE global_offset = ?",
          reply.globalOffset().value());
      EventEnvelope stripped = the(order, StockReserved.class);
      assertThat(stripped.metadata().sagaOrigin()).isNull();
      assertThat(stripped.metadata().correlationId())
          .isEqualTo(CorrelationId.of("streamrune:saga:" + sagaIdOf(order).value()));
      LoadedSaga<Fulfillment> before = row(order).orElseThrow();

      assertThat(node.sagaLoop.deliverNext()).as("the reply is delivered").isTrue();

      assertThat(row(order).orElseThrow())
          .as("a correlation id routes nothing: the saga row is as it was")
          .isEqualTo(before);
      assertThat(count("saga_dead_letters")).isZero();

      // The listener: the start event, then the reply. A reserved correlation id without an
      // origin is metadata no command bus writes, and the bus refuses the continuation.
      assertThat(node.listenerLoop.deliverNext()).isTrue();
      assertThatThrownBy(node.listenerLoop::deliverNext)
          .isInstanceOf(ReservedCorrelationIdException.class);
      assertThat(names(order)).containsExactly("OrderPlaced", "StockReserved");
      assertThat(count("dead_letter_queue")).isZero();

      node.runTheTimeoutRunnerUntil(() -> ended(order));
    }
    Outcome ended = outcome(order);
    assertThat(ended.status()).isEqualTo(SagaStatus.COMPENSATED);
    assertThat(ended.episode()).isEqualTo("opened by no event");
    assertThat(ended.events())
        .isEqualTo(
            sorted(
                PLACED,
                new Stored("StockReserved", NO_ORIGIN, THE_SAGAS, "OrderPlaced"),
                causedByTheSaga("StockReleased", 0, NO_CAUSE),
                causedByTheSaga("PaymentRefunded", 0, NO_CAUSE)));
  }
}
