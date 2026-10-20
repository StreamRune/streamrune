package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.GlobalOffset;

/** Runs the provenance contract against {@link InMemoryCommandBus}. */
class InMemoryCommandBusProvenanceTest extends CommandBusProvenanceContract {

  @Override
  protected CommandBus newBus(EventStore eventStore) {
    return bus(eventStore);
  }

  private static InMemoryCommandBus bus(EventStore eventStore) {
    return InMemoryCommandBus.builder()
        .eventStore(eventStore)
        .register(
            new InMemoryCommandBus.DeciderRegistration<>(
                AGGREGATE_TYPE, Mark.class, CommandBusProvenanceContract::aggregateIdOf, DECIDER))
        .build();
  }

  @Test
  void theEnvelopesTheBusReturnsCarryTheOriginToo() {
    var cause = new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), ORIGIN);

    try (InMemoryCommandBus bus = bus(new InMemoryEventStore())) {
      CommandBus.CommandResult result =
          ScopedValue.where(StreamRuneContext.CAUSE, cause)
              .call(() -> bus.execute(new Mark("returned-1")));

      assertEquals(2, result.envelopes().size());
      for (EventEnvelope envelope : result.envelopes()) {
        assertEquals(ORIGIN, envelope.metadata().sagaOrigin());
        assertEquals(CausationId.of("evt-1"), envelope.metadata().causationId());
      }
    }
  }

  /**
   * The contract leaves open what a bus does with a context bound to {@code null}. This bus fails
   * where it reads the context, as {@code VirtualThreadCommandBus} does, and stores nothing.
   */
  @Test
  void aContextBoundToNull_executesAsNoContext() {
    InMemoryEventStore eventStore = new InMemoryEventStore();
    InMemoryCommandBus bus = bus(eventStore);

    ScopedValue.where(StreamRuneContext.CURRENT, (StreamRuneContext.RequestContext) null)
        .run(() -> bus.execute(new Mark("null-context-1")));

    List<EventEnvelope> stored = eventStore.readGlobalStream(GlobalOffset.initial(), 10);
    assertEquals(2, stored.size());
    assertEquals(
        stored.get(0).metadata().correlationId(), stored.get(1).metadata().correlationId());
  }
}
