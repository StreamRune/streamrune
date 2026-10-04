package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;

class DeciderAutoRegistrationTest {

  private static final AggregateType TEST = AggregateType.of("test");

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
    var registration = registration();

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

  @Test
  void twoRegistrationBeansForOneCommandType_failTheContext_withTheRegistrationMessage() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean("first", DeciderRegistration.class, () -> registration())
        .withBean("second", DeciderRegistration.class, () -> registration())
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .rootCause()
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("command type " + TestCommand.class.getName())
                  .hasMessageContaining("is already registered (aggregate type 'test')");
            });
  }

  // Five command roots: three sibling roots under one type (the QUICKSTART shape), two other types.
  record PlaceIt(String id) implements Command {}

  record ConfirmIt(String id) implements Command {}

  record CancelIt(String id) implements Command {}

  record StockIt(String id) implements Command {}

  /** One event per command; one instance per registration (one instance never spans two types). */
  static final class OneEventDecider<C extends Command>
      implements Decider<C, TestState, TestEvent> {
    @Override
    public TestState initialState() {
      return new TestState();
    }

    @Override
    public List<TestEvent> decide(C command, TestState state) {
      return List.of(new TestEvent.ItHappened());
    }

    @Override
    public TestState evolve(TestState state, TestEvent event) {
      return state;
    }
  }

  @Test
  void fiveDistinctRegistrations_boot() {
    var order = AggregateType.of("order");
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean("test", DeciderRegistration.class, () -> registration())
        .withBean(
            "place",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    order,
                    PlaceIt.class,
                    c -> AggregateId.of(c.id()),
                    new OneEventDecider<PlaceIt>()))
        .withBean(
            "confirm",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    order,
                    ConfirmIt.class,
                    c -> AggregateId.of(c.id()),
                    new OneEventDecider<ConfirmIt>()))
        .withBean(
            "cancel",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    order,
                    CancelIt.class,
                    c -> AggregateId.of(c.id()),
                    new OneEventDecider<CancelIt>()))
        .withBean(
            "stock",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    AggregateType.of("inventory"),
                    StockIt.class,
                    c -> AggregateId.of(c.id()),
                    new OneEventDecider<StockIt>()))
        .run(ctx -> assertThat(ctx).hasNotFailed().hasSingleBean(VirtualThreadCommandBus.class));
  }

  private static DeciderRegistration<TestCommand, TestState, TestEvent> registration() {
    return new DeciderRegistration<>(
        TEST,
        TestCommand.class,
        cmd ->
            switch (cmd) {
              case TestCommand.DoIt d -> AggregateId.of(d.id());
            },
        new TestDecider());
  }
}
