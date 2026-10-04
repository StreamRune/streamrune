package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.test.InMemoryEventStore;

/**
 * Pins the {@link VirtualThreadCommandBus.Builder} wiring guards: a mis-wiring is refused where it
 * is written, with a message naming it, instead of surfacing later as an unrelated failure on the
 * first command.
 */
class VirtualThreadCommandBusBuilderGuardsTest {

  private static final AggregateType TYPE = AggregateType.of("ping");

  record Ping(String id) implements Command {}

  record Pinged() implements DomainEvent {}

  record PingState() implements AggregateState {}

  static final class PingDecider implements Decider<Ping, PingState, Pinged> {
    @Override
    public PingState initialState() {
      return new PingState();
    }

    @Override
    public List<Pinged> decide(Ping command, PingState state) {
      return List.of(new Pinged());
    }

    @Override
    public PingState evolve(PingState state, Pinged event) {
      return state;
    }
  }

  @Test
  void nullLockTimeout_isRefusedAtTheBuilder_notAtTheFirstCommand() {
    var builder = VirtualThreadCommandBus.builder().eventStore(new InMemoryEventStore());

    NullPointerException refused =
        assertThrows(NullPointerException.class, () -> builder.lockTimeout(null));
    assertTrue(refused.getMessage().contains("lockTimeout"), refused.getMessage());
  }

  @Test
  void aRegistrationWithoutAnAggregateType_isRefusedAtTheBuilder() {
    var builder = VirtualThreadCommandBus.builder().eventStore(new InMemoryEventStore());

    IllegalArgumentException refused =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder.register(
                    null, Ping.class, cmd -> AggregateId.of(cmd.id()), new PingDecider()));
    assertEquals("aggregateType is required", refused.getMessage());
  }

  @Test
  void secondDeciderForTheSameCommandType_isRefused_insteadOfSilentlyReplacingTheFirst() {
    var builder =
        VirtualThreadCommandBus.builder()
            .eventStore(new InMemoryEventStore())
            .register(TYPE, Ping.class, cmd -> AggregateId.of(cmd.id()), new PingDecider());

    IllegalArgumentException refused =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder.register(
                    TYPE, Ping.class, cmd -> AggregateId.of(cmd.id()), new PingDecider()));
    assertTrue(refused.getMessage().contains(Ping.class.getName()), refused.getMessage());
  }

  @Test
  void omittedLocker_defaultsToTheInProcessStripedLocker() {
    assertDoesNotThrow(
        () ->
            VirtualThreadCommandBus.builder()
                .eventStore(new InMemoryEventStore())
                .register(TYPE, Ping.class, cmd -> AggregateId.of(cmd.id()), new PingDecider())
                .build()
                .execute(new Ping("p-1")));
  }
}
