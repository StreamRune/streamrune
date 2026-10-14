package org.streamrune.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaStepLockFixture.Advanced;
import org.streamrune.runtime.SagaStepLockFixture.Completed;
import org.streamrune.runtime.SagaStepLockFixture.HookedBus;
import org.streamrune.runtime.SagaStepLockFixture.OneEventDecider;
import org.streamrune.runtime.SagaStepLockFixture.Opened;
import org.streamrune.runtime.SagaStepLockFixture.OrderSaga;
import org.streamrune.runtime.SagaStepLockFixture.Refund;
import org.streamrune.runtime.SagaStepLockFixture.Refunded;
import org.streamrune.runtime.SagaStepLockFixture.Release;
import org.streamrune.runtime.SagaStepLockFixture.Released;
import org.streamrune.runtime.SagaStepLockFixture.Reserve;
import org.streamrune.runtime.SagaStepLockFixture.Reserved;
import org.streamrune.runtime.SagaStepLockFixture.Ship;
import org.streamrune.runtime.SagaStepLockFixture.Shipped;
import org.streamrune.runtime.SagaStepLockFixture.State;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;
import org.streamrune.test.MutableClock;

/**
 * Two processes over one database, in one JVM: the event store, its command inbox, the saga rows
 * and the dead-letter store are shared, and each node has a bus of its own (its own aggregate
 * locks), a runner with its own step lock, a timeout runner built from that runner, a dead-letter
 * replayer and its own metrics. The step locks of the two nodes know nothing of each other — which
 * is what two processes are — so only the event store's forward fence keeps a forward command of
 * one node from being stored after a compensation the other node ran.
 *
 * <p>The saga is the step-lock tests' order saga: the start event issues {@link Reserve} and {@link
 * Ship}, {@link Advanced} issues {@link Ship}, {@link Completed} completes the saga, and the
 * compensation is a release and a refund. A test places the other node's work at a chosen point of
 * a step with a hook on the stepping node's bus; the hook runs on the stepping thread, so nothing
 * waits on another thread or a clock.
 */
final class SagaForwardFenceTwoNodeFixture implements AutoCloseable {

  final MutableClock clock = MutableClock.startingAt(Instant.parse("2026-01-01T00:00:00Z"));
  final InMemoryCommandInbox inbox = new InMemoryCommandInbox();
  final InMemorySagaStore rows = new InMemorySagaStore(clock);
  final InMemoryEventStore eventStore =
      new InMemoryEventStore().withCommandInbox(inbox).withSagaStore(rows);
  final InMemorySagaDeadLetterStore deadLetters = new InMemorySagaDeadLetterStore(rows);

  final Node a;
  final Node b;

  private final AtomicInteger triggerIds = new AtomicInteger();

  /** One process: its bus, its runner (and step lock), its timeout runner and replayer. */
  final class Node implements AutoCloseable {
    final OneEventDecider<Reserve> reserve = new OneEventDecider<>(c -> new Reserved(c.orderId()));
    final OneEventDecider<Ship> ship = new OneEventDecider<>(c -> new Shipped(c.orderId()));
    final OneEventDecider<Release> release = new OneEventDecider<>(c -> new Released(c.orderId()));
    final OneEventDecider<Refund> refund = new OneEventDecider<>(c -> new Refunded(c.orderId()));
    final OrderSaga saga = new OrderSaga();

    /** The saga type of every forward command this node's steps saw refused at the commit. */
    final List<String> fencedSamples = new CopyOnWriteArrayList<>();

    /** The saga type of every compensation episode this node found opened under its step. */
    final List<String> underStepSamples = new CopyOnWriteArrayList<>();

    /** Every command that reached this node's bus, in order. */
    final List<Class<?>> dispatched = new CopyOnWriteArrayList<>();

    final StreamRuneMetrics metrics =
        new StreamRuneMetrics() {
          @Override
          public void recordSagaForwardCommandFenced(String sagaType) {
            fencedSamples.add(sagaType);
          }

          @Override
          public void recordSagaCompensationUnderForwardStep(String sagaType) {
            underStepSamples.add(sagaType);
          }
        };

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
                AggregateType.of("release"),
                Release.class,
                c -> AggregateId.of(c.orderId()),
                release)
            .register(
                AggregateType.of("refund"), Refund.class, c -> AggregateId.of(c.orderId()), refund)
            .build();
    final HookedBus bus = new HookedBus(realBus);
    final SagaRunner<State> runner;
    final SagaTimeoutRunner<State> timeoutRunner;
    final SagaDeadLetterReplayer replayer;

    Node(boolean forwardFence) {
      saga.startAlsoShips = true;
      bus.beforeEveryCommand = command -> dispatched.add(command.getClass());
      runner =
          SagaRunner.<State>builder()
              .decider(saga)
              .startWhen(e -> e.event() instanceof Opened)
              .extractSagaId(e -> SagaStepLockFixture.sagaOf(((Opened) e.event()).orderId()))
              .correlateByEventData(
                  (streamId, e) ->
                      switch (e) {
                        case Advanced advanced ->
                            Optional.of(SagaStepLockFixture.sagaOf(advanced.orderId()));
                        case Completed completed ->
                            Optional.of(SagaStepLockFixture.sagaOf(completed.orderId()));
                        default -> Optional.empty();
                      })
              .sagaStore(rows)
              .commandBus(bus)
              .sagaDeadLetterStore(deadLetters)
              .clock(clock)
              .metrics(metrics)
              .forwardFence(forwardFence)
              .build();
      timeoutRunner =
          SagaTimeoutRunner.<State>builder()
              .decider(saga)
              .sagaStore(rows)
              .commandBus(bus)
              .sagaType(SagaStepLockFixture.TYPE)
              .clock(clock)
              .sagaRunner(runner)
              .batchSize(1_000)
              .metrics(metrics)
              .build();
      replayer =
          SagaDeadLetterReplayer.builder()
              .sagaDeadLetterStore(deadLetters)
              .sagaStore(rows)
              .eventStore(eventStore)
              .sagaRunner(runner)
              .clock(clock)
              .build();
    }

    void deliver(EventEnvelope event) {
      runner.asEventListener().onEvents(List.of(event));
    }

    /** The saga's deadline passes and this node's timeout runner polls once. */
    void timeOut() {
      clock.advance(SagaStepLockFixture.TIMEOUT.plusSeconds(1));
      timeoutRunner.pollOnce();
    }

    @Override
    public void close() {
      realBus.close();
    }
  }

  /** Two nodes whose runners both fence their forward commands, or both do not. */
  SagaForwardFenceTwoNodeFixture(boolean forwardFence) {
    a = new Node(forwardFence);
    b = new Node(forwardFence);
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

  LoadedSaga<State> row(String orderId) {
    return rows.load(SagaStepLockFixture.sagaOf(orderId), SagaStepLockFixture.TYPE, State.class)
        .orElseThrow();
  }

  /** The stored forward and compensation events of one order, in global order. */
  List<String> storedOrder(String orderId) {
    return eventStore.readGlobalStream(GlobalOffset.initial(), 100_000).stream()
        .map(e -> (DomainEvent) e.event())
        .filter(e -> orderId.equals(SagaStepLockFixture.orderOf(e)))
        .map(e -> e.getClass().getSimpleName())
        .toList();
  }

  /** A forward command of the saga and the episode command it was stored after. */
  record LateForwardCommand(IdempotencyKey forward, IdempotencyKey compensation) {}

  /**
   * The rule of the invariant every scenario asserts, read from the shared command inbox: the
   * forward commands of the order's saga (key {@code saga:<id>:<offset>:<index>}) whose last stored
   * event lies after the first stored event of one of its compensation commands (key {@code
   * saga:<id>:episode:<version>:comp:<index>}). The keys are looked up for every offset of the
   * stream and every index and episode version a scenario can produce.
   */
  List<LateForwardCommand> forwardCommandsStoredAfterACompensation(String orderId) {
    SagaId sagaId = SagaStepLockFixture.sagaOf(orderId);
    List<CommandInbox.InboxResult> forward = new ArrayList<>();
    List<CommandInbox.InboxResult> compensation = new ArrayList<>();
    for (EventEnvelope event : eventStore.readGlobalStream(GlobalOffset.initial(), 100_000)) {
      for (int index = 0; index < 4; index++) {
        inbox
            .find(SagaCommandDispatch.forwardKey(sagaId, event.globalOffset().value(), index))
            .ifPresent(forward::add);
      }
    }
    for (long episode = 1; episode <= 20; episode++) {
      for (int index = 0; index < 4; index++) {
        inbox
            .find(SagaCommandDispatch.episodeCompensationKey(sagaId, episode, index))
            .ifPresent(compensation::add);
      }
    }
    List<LateForwardCommand> late = new ArrayList<>();
    for (var f : forward) {
      if (f.globalOffsets().isEmpty()) {
        continue;
      }
      long lastOfForward = f.globalOffsets().getLast().value();
      for (var c : compensation) {
        if (!c.globalOffsets().isEmpty() && lastOfForward > c.globalOffsets().getFirst().value()) {
          late.add(new LateForwardCommand(f.key(), c.key()));
        }
      }
    }
    return late;
  }

  @Override
  public void close() {
    a.close();
    b.close();
  }
}
