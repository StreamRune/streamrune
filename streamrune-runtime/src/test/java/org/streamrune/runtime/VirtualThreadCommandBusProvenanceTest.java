package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.CommandBusProvenanceContract;
import org.streamrune.test.InMemoryEventStore;

/** Runs the provenance contract against {@link VirtualThreadCommandBus}. */
class VirtualThreadCommandBusProvenanceTest extends CommandBusProvenanceContract {

  @Override
  protected CommandBus newBus(EventStore eventStore) {
    return bus(eventStore);
  }

  private static VirtualThreadCommandBus bus(EventStore eventStore) {
    return VirtualThreadCommandBus.builder()
        .eventStore(eventStore)
        .locker(new LocalStripedLocker(16))
        .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
        .objectMapper(new ObjectMapper())
        .register(AGGREGATE_TYPE, Mark.class, CommandBusProvenanceContract::aggregateIdOf, DECIDER)
        .build();
  }

  @Test
  void executeAsync_carriesTheCauseOntoItsThread() {
    var cause = new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), ORIGIN);
    var context =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("order-saga-42"), Instant.now(), Map.of());

    // Bound alone, and bound beside a context and the saga flag as saga dispatch binds it.
    assertStoredUnder("async-1", ScopedValue.where(StreamRuneContext.CAUSE, cause), cause);
    assertStoredUnder(
        "async-2",
        ScopedValue.where(StreamRuneContext.CURRENT, context)
            .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
            .where(StreamRuneContext.CAUSE, cause),
        cause);
  }

  private static void assertStoredUnder(
      String aggregateId, ScopedValue.Carrier bindings, StreamRuneContext.DispatchCause cause) {
    InMemoryEventStore eventStore = new InMemoryEventStore();
    try (VirtualThreadCommandBus bus = bus(eventStore)) {
      // The bindings end when call() returns; join() then waits for a command that runs on the
      // bus's own virtual thread, where only what executeAsync carried over is bound.
      CompletableFuture<CommandBus.CommandResult> future =
          bindings.call(() -> bus.executeAsync(new Mark(aggregateId)));
      future.join();
    }

    List<EventEnvelope> stored =
        eventStore.load(StreamId.of(AGGREGATE_TYPE, AggregateId.of(aggregateId))).events();
    assertEquals(2, stored.size());
    for (EventEnvelope envelope : stored) {
      assertEquals(cause.sagaOrigin(), envelope.metadata().sagaOrigin());
      assertEquals(cause.causationId(), envelope.metadata().causationId());
    }
  }
}
