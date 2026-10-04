package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.RequireRole;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;

class StreamRuneAuthorizationValidatorTest {

  @RequireRole("ADMIN")
  record GuardedCommand(String id) implements Command {}

  record PlainCommand(String id) implements Command {}

  record TestState() implements AggregateState {}

  record TestEvent() implements DomainEvent {}

  private static <C extends Command> DeciderRegistration<C, TestState, TestEvent> registrationFor(
      Class<C> commandType) {
    return new DeciderRegistration<>(
        AggregateType.of("test"),
        commandType,
        cmd -> AggregateId.of("agg-1"),
        new Decider<>() {
          @Override
          public TestState initialState() {
            return new TestState();
          }

          @Override
          public List<TestEvent> decide(C command, TestState state) {
            return List.of();
          }

          @Override
          public TestState evolve(TestState state, TestEvent event) {
            return state;
          }
        });
  }

  private static VirtualThreadCommandBus.Builder busRegistering(Class<? extends Command> type) {
    DeciderRegistration<? extends Command, TestState, TestEvent> registration =
        registrationFor(type);
    return register(
        VirtualThreadCommandBus.builder().eventStore(mock(EventStore.class)), registration);
  }

  private static <C extends Command> VirtualThreadCommandBus.Builder register(
      VirtualThreadCommandBus.Builder builder, DeciderRegistration<C, TestState, TestEvent> reg) {
    return builder.register(
        reg.aggregateType(), reg.commandType(), reg.idExtractor(), reg.decider());
  }

  /** The command buses as the validator meets them: each resolved (built) on iteration. */
  @SafeVarargs
  @SuppressWarnings("unchecked")
  private static Instance<VirtualThreadCommandBus> buses(
      java.util.function.Supplier<VirtualThreadCommandBus>... buses) {
    Instance<VirtualThreadCommandBus> instance = mock(Instance.class);
    when(instance.iterator())
        .thenAnswer(
            invocation -> Arrays.stream(buses).map(java.util.function.Supplier::get).iterator());
    return instance;
  }

  /**
   * The application produces its own bus and registers the guarded command through the builder, so
   * no {@link DeciderRegistration} bean exists, and wires no enforcing interceptor. Nothing injects
   * the bus during startup; the validator resolves it, so the startup is refused.
   */
  @Test
  void anApplicationBusRegisteringAnAnnotatedCommandWithoutEnforcementRefusesStartup()
      throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneAuthorizationValidator.class, ApplicationGuardedBus.class))) {
      Throwable failure =
          org.assertj.core.api.Assertions.catchThrowable(
              () ->
                  arc.container()
                      .beanManager()
                      .getEvent()
                      .select(StartupEvent.class)
                      .fire(new StartupEvent()));

      assertNotNull(failure, "startup must be refused");
      Throwable root = failure;
      while (root.getCause() != null && root.getCause() != root) {
        root = root.getCause();
      }
      assertInstanceOf(IllegalStateException.class, root, String.valueOf(failure));
      assertTrue(root.getMessage().contains("UNGUARDED"), root.getMessage());
      assertTrue(root.getMessage().contains(GuardedCommand.class.getName()), root.getMessage());
    }
  }

  /** The application's own bus: the guarded command registered through the builder only. */
  @ApplicationScoped
  public static class ApplicationGuardedBus {
    @Produces
    @Singleton
    public VirtualThreadCommandBus commandBus() {
      DeciderRegistration<GuardedCommand, TestState, TestEvent> guarded =
          registrationFor(GuardedCommand.class);
      return VirtualThreadCommandBus.builder()
          .eventStore(mock(EventStore.class))
          .register(
              guarded.aggregateType(),
              guarded.commandType(),
              guarded.idExtractor(),
              guarded.decider())
          .build();
    }
  }

  @Test
  void startup_passes_whenNoAnnotatedCommands() {
    var validator =
        new StreamRuneAuthorizationValidator(
            buses(() -> busRegistering(PlainCommand.class).build()));

    assertDoesNotThrow(() -> validator.validate(new StartupEvent()));
  }

  @Test
  void startup_throws_whenResolvingABusThatRunsAnAnnotatedCommandUnguarded() {
    var validator =
        new StreamRuneAuthorizationValidator(
            buses(() -> busRegistering(GuardedCommand.class).build()));

    IllegalStateException ex =
        assertThrows(IllegalStateException.class, () -> validator.validate(new StartupEvent()));
    assertTrue(ex.getMessage().contains(GuardedCommand.class.getName()));
    assertTrue(ex.getMessage().contains("UNGUARDED"));
  }

  @Test
  void startup_passes_whenTheBusRunsTheEnforcingInterceptor() {
    var enforcement = new AnnotationAuthorizationInterceptor(userId -> UserAuthority.EMPTY);
    var validator =
        new StreamRuneAuthorizationValidator(
            buses(
                () -> busRegistering(PlainCommand.class).build(),
                () -> busRegistering(GuardedCommand.class).interceptors(enforcement).build()));

    assertDoesNotThrow(() -> validator.validate(new StartupEvent()));
  }

  @Test
  void startup_passes_withoutAnyCommandBus() {
    var validator = new StreamRuneAuthorizationValidator(buses());

    assertDoesNotThrow(() -> validator.validate(new StartupEvent()));
  }
}
