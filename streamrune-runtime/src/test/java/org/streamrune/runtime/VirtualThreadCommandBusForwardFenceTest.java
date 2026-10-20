package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.saga.SagaForwardFencedException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * The bus hands the event store the saga origin of a saga's forward command — for that command's
 * own key only — and treats the store's refusal as a final answer: not retried, not dead-lettered,
 * not counted by the circuit breaker, audited as refused.
 */
class VirtualThreadCommandBusForwardFenceTest {

  private static final AggregateType TYPE = AggregateType.of("shipment");
  private static final SagaOrigin ORIGIN =
      SagaOrigin.dispatchedBy(SagaType.of("orders.Fulfillment"), SagaId.of("saga-1"));

  record Ship(String shipmentId, boolean emitsEvent) implements Command {}

  record Shipped(String shipmentId) implements DomainEvent {}

  record ShipmentState() implements AggregateState {}

  record FulfillmentState(SagaStatus status) implements SagaState {}

  static final class ShipDecider implements Decider<Ship, ShipmentState, Shipped> {
    final AtomicInteger decideCalls = new AtomicInteger();

    @Override
    public ShipmentState initialState() {
      return new ShipmentState();
    }

    @Override
    public List<Shipped> decide(Ship command, ShipmentState state) {
      decideCalls.incrementAndGet();
      return command.emitsEvent() ? List.of(new Shipped(command.shipmentId())) : List.of();
    }

    @Override
    public ShipmentState evolve(ShipmentState state, Shipped event) {
      return state;
    }
  }

  /** Records the saga origin of every keyed append; stores through an in-memory store. */
  static final class RecordingEventStore implements EventStore {
    final InMemoryEventStore inner =
        new InMemoryEventStore().withCommandInbox(new InMemoryCommandInbox());
    final List<SagaOrigin> forwardOf = Collections.synchronizedList(new ArrayList<>());
    final Object rows = new Object();

    @Override
    public IdempotentAppendResult appendWithKey(
        StreamId streamId,
        List<EventEnvelope> events,
        Version expectedVersion,
        IdempotencyKey idempotencyKey,
        String commandType,
        SagaOrigin forwardOf) {
      this.forwardOf.add(forwardOf);
      return inner.appendWithKey(streamId, events, expectedVersion, idempotencyKey, commandType);
    }

    @Override
    public Object sagaFenceStorageIdentity() {
      return rows;
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      return inner.load(streamId);
    }

    @Override
    public AppendResult append(
        StreamId streamId, List<EventEnvelope> events, Version expectedVersion) {
      return inner.append(streamId, events, expectedVersion);
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      inner.saveSnapshot(streamId, version, state);
    }

    @Override
    public boolean saveSnapshot(
        StreamId streamId,
        Version version,
        AggregateState state,
        int snapshotVersion,
        long snapshotPurgeGeneration) {
      throw new UnsupportedOperationException("this test store keeps no snapshots");
    }

    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      return inner.readGlobalStream(afterOffset, maxCount);
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      return inner.readStream(streamId, afterVersion, maxCount);
    }
  }

  private static VirtualThreadCommandBus.Builder bus(EventStore store, ShipDecider decider) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        .retryPolicy(new RetryPolicy(3, Duration.ofMillis(1), 1.0))
        .objectMapper(new ObjectMapper())
        .commandInbox(new InMemoryCommandInbox())
        .register(TYPE, Ship.class, cmd -> AggregateId.of(cmd.shipmentId()), decider);
  }

  private static StreamRuneContext.SagaForward forward(IdempotencyKey key) {
    return new StreamRuneContext.SagaForward(ORIGIN, key);
  }

  @Test
  void theMarkerReachesTheStoreForItsOwnKeyOnly() {
    var store = new RecordingEventStore();
    var bus = bus(store, new ShipDecider()).build();
    var own = IdempotencyKey.of("saga:saga-1:cmd:0");
    var other = IdempotencyKey.of("nested-1");

    ScopedValue.where(StreamRuneContext.SAGA_FORWARD, forward(own))
        .run(
            () -> {
              bus.execute(new Ship("s-1", true), own);
              bus.execute(new Ship("s-2", true), other);
            });

    assertEquals(2, store.forwardOf.size());
    assertSame(ORIGIN, store.forwardOf.get(0), "the forward command's own append is fenced");
    assertNull(store.forwardOf.get(1), "a command under another key is not");
  }

  @Test
  void aKeyedZeroEventCommandCarriesTheMarker() {
    var store = new RecordingEventStore();
    var bus = bus(store, new ShipDecider()).build();
    var key = IdempotencyKey.of("saga:saga-1:cmd:1");

    ScopedValue.where(StreamRuneContext.SAGA_FORWARD, forward(key))
        .run(() -> bus.execute(new Ship("s-3", false), key));

    assertEquals(List.of(ORIGIN), store.forwardOf);
  }

  @Test
  void noMarkerNoOrigin() {
    var store = new RecordingEventStore();
    var bus = bus(store, new ShipDecider()).build();

    bus.execute(new Ship("s-4", true), IdempotencyKey.of("plain-1"));

    assertEquals(1, store.forwardOf.size());
    assertNull(store.forwardOf.getFirst());
  }

  @Test
  void theBusReportsItsEventStoresFenceIdentity() {
    var store = new RecordingEventStore();
    var decider = new ShipDecider();

    assertSame(store.rows, bus(store, decider).build().sagaFenceStorageIdentity());
    var facade =
        StreamRune.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .register(TYPE, Ship.class, cmd -> AggregateId.of(cmd.shipmentId()), decider)
            .build();
    assertSame(store.rows, facade.sagaFenceStorageIdentity(), "the facade asks its bus");
    assertNull(
        bus(new InMemoryEventStore(), decider).build().sagaFenceStorageIdentity(),
        "a store that cannot fence");
  }

  /**
   * A hundred forward commands of a saga whose compensation episode is open, through the real bus:
   * each is refused with the store's exception as it is, the breaker stays closed and lets the next
   * command through, nothing is dead-lettered, the decider runs once per command (no retry), and
   * the audit records each as a failure whose message says it was refused.
   */
  @Test
  void aFencedCommandIsNeitherRetriedNorDeadLetteredNorCountedByTheBreaker_andIsAuditedAsRefused() {
    var rows = new InMemorySagaStore();
    rows.create(
        ORIGIN.sagaId(),
        ORIGIN.sagaType(),
        new FulfillmentState(SagaStatus.RUNNING),
        SagaStatus.RUNNING);
    rows.claimCompensating(
        ORIGIN.sagaId(), ORIGIN.sagaType(), new FulfillmentState(SagaStatus.COMPENSATING), 1L);
    var inbox = new InMemoryCommandInbox();
    var store = new InMemoryEventStore().withCommandInbox(inbox).withSagaStore(rows);
    var breaker = new CircuitBreakerCommandInterceptor(5, Duration.ofSeconds(30));
    List<AuditEntry> audit = Collections.synchronizedList(new ArrayList<>());
    AuditStore auditStore = audit::add;
    var dlq = new InMemoryDeadLetterQueue();
    var decider = new ShipDecider();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(store)
            .locker(new LocalStripedLocker(16))
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(1), 1.0))
            .objectMapper(new ObjectMapper())
            .commandInbox(inbox)
            .deadLetterQueue(dlq)
            .interceptors(breaker, new AuditCommandInterceptor(auditStore))
            .register(TYPE, Ship.class, cmd -> AggregateId.of(cmd.shipmentId()), decider)
            .build();

    for (int i = 0; i < 100; i++) {
      var key = IdempotencyKey.of("saga:saga-1:cmd:" + i);
      var command = new Ship("s-" + i, true);
      var refusal =
          assertThrows(
              SagaForwardFencedException.class,
              () ->
                  ScopedValue.where(StreamRuneContext.SAGA_FORWARD, forward(key))
                      .where(
                          StreamRuneContext.CAUSE,
                          new StreamRuneContext.DispatchCause(null, ORIGIN))
                      .call(() -> bus.execute(command, key)));
      assertEquals(ORIGIN.sagaId(), refusal.sagaId());
    }

    assertEquals("CLOSED", breaker.circuitState(), "a refusal says nothing about infrastructure");
    bus.execute(new Ship("unfenced", true), IdempotencyKey.of("plain-after"));
    assertEquals(101, decider.decideCalls.get(), "one decision per command: nothing retried");
    assertTrue(dlq.all().isEmpty(), "nothing dead-lettered");
    List<AuditEntry> failures =
        audit.stream().filter(e -> e.outcome() == AuditOutcome.FAILURE).toList();
    assertEquals(100, failures.size());
    assertTrue(
        failures.stream()
            .allMatch(e -> e.errorMessage() != null && e.errorMessage().contains("refused")),
        "the audit row says the command was refused: " + failures.getFirst().errorMessage());
  }
}
