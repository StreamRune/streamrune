package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.types.CausationId;

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
}
