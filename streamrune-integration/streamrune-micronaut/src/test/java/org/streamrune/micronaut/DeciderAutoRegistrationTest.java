package org.streamrune.micronaut;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;

class DeciderAutoRegistrationTest {

  sealed interface TestCommand extends Command permits TestCommand.DoIt {
    record DoIt(String id) implements TestCommand {}
  }

  record TestState() implements AggregateState {}

  sealed interface TestEvent extends DomainEvent permits TestEvent.ItHappened {
    record ItHappened() implements TestEvent {}
  }

  static class TestDecider implements Decider<TestCommand, TestState, TestEvent> {
    @Override
    public TestState initialState() {
      return new TestState();
    }

    @Override
    public List<TestEvent> decide(TestCommand command, TestState state) {
      return List.of(new TestEvent.ItHappened());
    }

    @Override
    public TestState evolve(TestState state, TestEvent event) {
      return state;
    }
  }

  @Test
  void deciderRegistrationCanBeUsedWithCommandBus() {
    var eventStore = mock(EventStore.class);
    var registration =
        new DeciderRegistration<>(
            AggregateType.of("test"),
            TestCommand.class,
            cmd ->
                switch (cmd) {
                  case TestCommand.DoIt d -> AggregateId.of(d.id());
                },
            new TestDecider());

    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore)
            .register(
                registration.aggregateType(),
                registration.commandType(),
                registration.idExtractor(),
                registration.decider())
            .build();

    assertThat(bus).isNotNull();
  }
}
