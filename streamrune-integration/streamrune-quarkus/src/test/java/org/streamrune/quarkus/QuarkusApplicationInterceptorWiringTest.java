package org.streamrune.quarkus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.RequireRole;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditOutcome;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.AuditCommandInterceptor;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryEventStore;

/**
 * An application's own {@link CommandInterceptor} beans must JOIN the framework's interceptor
 * chain, never replace it — driven through a real Arc container ({@link RealArcTestContainer}) over
 * the real {@link StreamRuneProducers}, so the collection the command bus and the startup
 * validators build from goes through real Arc bean resolution.
 *
 * <p>The framework's interceptor producers are {@code @DefaultBean}. Arc resolves an {@code
 * Instance<T>} iteration (and an {@code @All List<T>}) through the same ambiguity resolution as a
 * single-valued injection point, which drops every {@code @DefaultBean} candidate as soon as one
 * non-default candidate of the collected type exists. Collected through an {@code Instance}, the
 * first application interceptor bean — a maintenance gate, a rate limiter — therefore evicted the
 * authorization, audit and circuit-breaker interceptors wholesale, and the identity validator,
 * iterating the same stripped collection, let startup proceed without them.
 *
 * <p>The chain is observed by behaviour, not by inspecting a field: two application gates are
 * positioned by {@code @Priority} on either side of the framework's authorization interceptor
 * (audit 2000 &lt; outer 2500 &lt; authorization 3000 &lt; inner 3500 &lt; circuit breaker 5000),
 * and a denied command, a failed command and a command refused by the open breaker each leave a
 * distinct trace in the gates and the audit store.
 *
 * <p>Every chain test runs twice: once with Arc's build-time removal of unused beans switched off,
 * as the other container tests boot, and once with it switched on, as every Quarkus application
 * builds by default. Only the bus producer collects the interceptors, through the {@code
 * BeanManager}, a lookup the removal cannot see; only an injection point matching every interceptor
 * bean keeps the framework's interceptor producers and the application's interceptor beans in the
 * application at all. The startup validators read the chain back from each {@code
 * VirtualThreadCommandBus} bean, and their {@code Instance<VirtualThreadCommandBus>} keeps the bus.
 */
class QuarkusApplicationInterceptorWiringTest {

  /** How the deployment is built. */
  enum Removal {
    /** Every bean is kept — how the other container tests boot. */
    OFF,
    /** Arc removes unused beans — the Quarkus default, {@code quarkus.arc.remove-unused-beans}. */
    ON
  }

  @BeforeEach
  void resetRecorders() {
    OuterGate.EVENTS.clear();
    InnerGate.EVENTS.clear();
    RecordingAuditStoreConfig.SAVED.clear();
    ReplacingAuditInterceptorConfig.APPLICATION_STORE.clear();
    WidgetRegistrationConfig.DECIDED.set(0);
  }

  @ParameterizedTest
  @EnumSource(Removal.class)
  void anApplicationInterceptorBeanJoinsTheFrameworkChainInCanonicalOrder(Removal removal)
      throws Exception {
    try (var arc =
        bootWithCommandResource(
            removal,
            List.of(
                StreamRuneProducers.class,
                Infrastructure.class,
                InMemoryEventStoreConfig.class,
                RecordingAuditStoreConfig.class,
                SelectivePolicyConfig.class,
                WidgetRegistrationConfig.class,
                OuterGate.class,
                InnerGate.class))) {
      VirtualThreadCommandBus bus = arc.container().instance(VirtualThreadCommandBus.class).get();

      // 1. Denied by the framework's authorization interceptor: the audit interceptor (outside
      //    authorization) records the failure, the outer gate saw before() and onError(), and the
      //    inner gate — positioned after authorization — never ran.
      assertThrows(AuthorizationException.class, () -> bus.execute(new WidgetCommand("denied")));
      assertThat(RecordingAuditStoreConfig.SAVED)
          .as("the framework audit interceptor must still be in the chain")
          .extracting(AuditEntry::outcome)
          .containsExactly(AuditOutcome.FAILURE);
      assertThat(OuterGate.EVENTS).containsExactly("before", "onError");
      assertThat(InnerGate.EVENTS)
          .as("the framework authorization interceptor must run between the two gates")
          .isEmpty();
      assertThat(WidgetRegistrationConfig.DECIDED).hasValue(0);

      // 2. A failing decider trips the framework's circuit breaker (threshold 1).
      OuterGate.EVENTS.clear();
      InnerGate.EVENTS.clear();
      assertThrows(RuntimeException.class, () -> bus.execute(new WidgetCommand("boom")));
      assertThat(WidgetRegistrationConfig.DECIDED).hasValue(1);
      assertThat(InnerGate.EVENTS).containsExactly("before", "onError");

      // 3. The open breaker — innermost of the gates — refuses the next command before the
      //    decider: the inner gate's before() completed, so it is notified of the refusal.
      OuterGate.EVENTS.clear();
      InnerGate.EVENTS.clear();
      assertThrows(CircuitBreakerOpenException.class, () -> bus.execute(new WidgetCommand("w-3")));
      assertThat(WidgetRegistrationConfig.DECIDED).hasValue(1);
      assertThat(InnerGate.EVENTS)
          .as("the framework circuit breaker must still be in the chain, inside the inner gate")
          .containsExactly("before", "onError");
      assertThat(OuterGate.EVENTS).containsExactly("before", "onError");
    }
  }

  @ParameterizedTest
  @EnumSource(Removal.class)
  void theFrameworkInterceptorsRunWithoutAnyApplicationInterceptor(Removal removal)
      throws Exception {
    // The framework's interceptor producers are reached only through the BeanManager collection;
    // nothing else in an application injects them. They must survive the build on their own.
    try (var arc =
        bootWithCommandResource(
            removal,
            List.of(
                StreamRuneProducers.class,
                Infrastructure.class,
                InMemoryEventStoreConfig.class,
                RecordingAuditStoreConfig.class,
                SelectivePolicyConfig.class,
                WidgetRegistrationConfig.class))) {
      CommandBus bus = arc.container().instance(CommandResource.class).get().bus;

      assertThrows(
          AuthorizationException.class,
          () -> bus.execute(new WidgetCommand("denied")),
          "the framework authorization interceptor must consult the policy");
      assertThat(WidgetRegistrationConfig.DECIDED).hasValue(0);
      assertThat(RecordingAuditStoreConfig.SAVED)
          .as("the framework audit interceptor must record the denial")
          .extracting(AuditEntry::outcome)
          .containsExactly(AuditOutcome.FAILURE);

      assertThrows(RuntimeException.class, () -> bus.execute(new WidgetCommand("boom")));
      assertThrows(
          CircuitBreakerOpenException.class,
          () -> bus.execute(new WidgetCommand("w-3")),
          "the framework circuit breaker must open after the failure");
      assertThat(WidgetRegistrationConfig.DECIDED).hasValue(1);
    }
  }

  @ParameterizedTest
  @EnumSource(Removal.class)
  void anApplicationBeanOfAFrameworkInterceptorTypeReplacesThatFrameworkInterceptor(Removal removal)
      throws Exception {
    // Parity with Spring's @ConditionalOnMissingBean and Micronaut's @Requires(missingBeans):
    // an application AuditCommandInterceptor bean stands in for the framework's, so a command is
    // audited exactly once — into the application interceptor's store, not the AuditStore bean the
    // framework producer would have wrapped.
    try (var arc =
        bootWithCommandResource(
            removal,
            List.of(
                StreamRuneProducers.class,
                Infrastructure.class,
                InMemoryEventStoreConfig.class,
                RecordingAuditStoreConfig.class,
                ReplacingAuditInterceptorConfig.class,
                WidgetRegistrationConfig.class))) {
      VirtualThreadCommandBus bus = arc.container().instance(VirtualThreadCommandBus.class).get();
      bus.execute(new WidgetCommand("w-1"));
      assertThat(ReplacingAuditInterceptorConfig.APPLICATION_STORE)
          .extracting(AuditEntry::outcome)
          .containsExactly(AuditOutcome.SUCCESS);
      assertThat(RecordingAuditStoreConfig.SAVED)
          .as("the framework audit interceptor must not be registered beside the application's")
          .isEmpty();
    }
  }

  @ParameterizedTest
  @EnumSource(Removal.class)
  void theIdentityValidatorStillSeesTheAuthorizationInterceptorBesideAnApplicationGate(
      Removal removal) throws Exception {
    // A CommandAuthorizationPolicy in the anonymous identity mode refuses startup. An application
    // gate must not hide the authorization interceptor from the validator.
    List<Class<?>> beans =
        List.of(
            StreamRuneProducers.class,
            StreamRuneRequestIdentityValidator.class,
            StreamRuneRequestContextHolder.class,
            Infrastructure.class,
            InMemoryEventStoreConfig.class,
            AllowAllPolicyConfig.class,
            OuterGate.class);
    // No resource injects the command bus here: the validator must keep what it checks on its own.
    List<Class<?>> restProviders = List.of(StreamRuneRequestFilter.class);
    try (var arc =
        removal == Removal.ON
            ? RealArcTestContainer.bootRemovingUnusedBeans(beans, restProviders, Set.of())
            : RealArcTestContainer.bootWithRestProviders(beans, restProviders)) {
      IllegalStateException refused = assertThrows(IllegalStateException.class, () -> startUp(arc));
      assertThat(refused).hasMessageContaining("AuthorizationCommandInterceptor");
    }
  }

  @ParameterizedTest
  @EnumSource(Removal.class)
  void theAuthorizationValidatorStillSeesTheAnnotationInterceptorBesideAnApplicationGate(
      Removal removal) throws Exception {
    // A @RequireRole command with a UserRoleResolver bean is enforced by the framework's
    // AnnotationAuthorizationInterceptor; an application gate must not hide that interceptor from
    // the validator, which would refuse a correctly configured application.
    List<Class<?>> beans =
        List.of(
            StreamRuneProducers.class,
            StreamRuneAuthorizationValidator.class,
            Infrastructure.class,
            InMemoryEventStoreConfig.class,
            RoleResolverConfig.class,
            GuardedRegistrationConfig.class,
            OuterGate.class);
    // No resource injects the command bus here: the validator must keep what it checks on its own.
    try (var arc =
        removal == Removal.ON
            ? RealArcTestContainer.bootRemovingUnusedBeans(beans, List.of(), Set.of())
            : RealArcTestContainer.boot(beans)) {
      assertDoesNotThrow(() -> startUp(arc));
    }
  }

  // ---- harness ----------------------------------------------------------------------------------

  /**
   * Boots {@code beans} plus {@link CommandResource}, the class through which the application uses
   * the command bus; with removal on, that resource is what keeps the bus in the application.
   */
  private static RealArcTestContainer bootWithCommandResource(Removal removal, List<Class<?>> beans)
      throws Exception {
    if (removal == Removal.ON) {
      return RealArcTestContainer.bootRemovingUnusedBeans(
          beans, List.of(), Set.of(CommandResource.class));
    }
    List<Class<?>> withResource = new ArrayList<>(beans);
    withResource.add(CommandResource.class);
    return RealArcTestContainer.boot(withResource);
  }

  /** A real, synchronous {@code Event#fire} of the Quarkus startup event. */
  private static void startUp(RealArcTestContainer arc) {
    arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
  }

  // ---- the domain -------------------------------------------------------------------------------

  record WidgetCommand(String id) implements Command {}

  record WidgetState() implements AggregateState {}

  sealed interface WidgetEvent extends DomainEvent {
    record Created() implements WidgetEvent {}
  }

  @RequireRole("ADMIN")
  record GuardedCommand(String id) implements Command {}

  /** Decides every command except {@code boom}, which fails like an infrastructure fault. */
  static final class WidgetDecider implements Decider<WidgetCommand, WidgetState, WidgetEvent> {
    @Override
    public WidgetState initialState() {
      return new WidgetState();
    }

    @Override
    public List<WidgetEvent> decide(WidgetCommand command, WidgetState state) {
      WidgetRegistrationConfig.DECIDED.incrementAndGet();
      if ("boom".equals(command.id())) {
        throw new IllegalStateException("downstream unavailable");
      }
      return List.of(new WidgetEvent.Created());
    }

    @Override
    public WidgetState evolve(WidgetState state, WidgetEvent event) {
      return state;
    }
  }

  // ---- the beans a real Quarkus application supplies ------------------------------------------

  /**
   * Stands in for a REST resource: Quarkus REST keeps resource classes, and this one injects the
   * command bus as an application endpoint would.
   */
  @Singleton
  public static class CommandResource {
    final CommandBus bus;

    @Inject
    public CommandResource(CommandBus bus) {
      this.bus = bus;
    }
  }

  /**
   * The Agroal {@code DataSource} and the bound {@code @ConfigMapping}: a circuit breaker that
   * opens on the first counted failure and no retries, so the chain is observable in three
   * commands.
   */
  @ApplicationScoped
  public static class Infrastructure {
    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return TestProperties.of(
          Map.of(
              "streamrune.circuit-breaker-failure-threshold", "1",
              "streamrune.retry-max-attempts", "1"));
    }
  }

  /** Replaces the Postgres-backed default so commands execute against an in-memory store. */
  @ApplicationScoped
  public static class InMemoryEventStoreConfig {
    @Produces
    @Singleton
    public EventStore eventStore() {
      return new InMemoryEventStore();
    }
  }

  /** The {@link AuditStore} bean for which the framework produces its audit interceptor. */
  @ApplicationScoped
  public static class RecordingAuditStoreConfig {
    static final List<AuditEntry> SAVED = new CopyOnWriteArrayList<>();

    @Produces
    @Singleton
    public AuditStore auditStore() {
      return SAVED::add;
    }
  }

  /** Denies the command {@code denied}, permits every other. */
  @ApplicationScoped
  public static class SelectivePolicyConfig {
    @Produces
    @Singleton
    public CommandAuthorizationPolicy commandAuthorizationPolicy() {
      return (userId, command) -> {
        if (command instanceof WidgetCommand widget && "denied".equals(widget.id())) {
          throw new AuthorizationException("denied");
        }
      };
    }
  }

  @ApplicationScoped
  public static class AllowAllPolicyConfig {
    @Produces
    @Singleton
    public CommandAuthorizationPolicy commandAuthorizationPolicy() {
      return CommandAuthorizationPolicy.allowAll();
    }
  }

  @ApplicationScoped
  public static class RoleResolverConfig {
    @Produces
    @Singleton
    public UserRoleResolver userRoleResolver() {
      return userId -> new UserAuthority(Set.of("ADMIN"), Set.of());
    }
  }

  @ApplicationScoped
  public static class WidgetRegistrationConfig {
    static final AtomicInteger DECIDED = new AtomicInteger();

    @Produces
    @Singleton
    public DeciderRegistration<WidgetCommand, WidgetState, WidgetEvent> widgetRegistration() {
      return new DeciderRegistration<>(
          AggregateType.of("widget"),
          WidgetCommand.class,
          command -> AggregateId.of(command.id()),
          new WidgetDecider());
    }
  }

  @ApplicationScoped
  public static class GuardedRegistrationConfig {
    @Produces
    @Singleton
    public DeciderRegistration<GuardedCommand, WidgetState, WidgetEvent> guardedRegistration() {
      return new DeciderRegistration<>(
          AggregateType.of("widget"),
          GuardedCommand.class,
          command -> AggregateId.of(command.id()),
          new Decider<GuardedCommand, WidgetState, WidgetEvent>() {
            @Override
            public WidgetState initialState() {
              return new WidgetState();
            }

            @Override
            public List<WidgetEvent> decide(GuardedCommand command, WidgetState state) {
              return List.of();
            }

            @Override
            public WidgetState evolve(WidgetState state, WidgetEvent event) {
              return state;
            }
          });
    }
  }

  // ---- the application's interceptors ----------------------------------------------------------

  /** An application gate positioned between the audit and the authorization interceptors. */
  @ApplicationScoped
  @Priority(2500)
  public static class OuterGate implements CommandInterceptor {
    static final List<String> EVENTS = new CopyOnWriteArrayList<>();

    @Override
    public boolean before(CommandContext ctx) {
      EVENTS.add("before");
      return true;
    }

    @Override
    public void after(CommandContext ctx) {
      EVENTS.add("after");
    }

    @Override
    public void onError(CommandContext ctx, Throwable error) {
      EVENTS.add("onError");
    }
  }

  /** An application gate positioned between the authorization interceptor and the breaker. */
  @Singleton
  @Priority(3500)
  public static class InnerGate implements CommandInterceptor {
    static final List<String> EVENTS = new CopyOnWriteArrayList<>();

    @Override
    public boolean before(CommandContext ctx) {
      EVENTS.add("before");
      return true;
    }

    @Override
    public void after(CommandContext ctx) {
      EVENTS.add("after");
    }

    @Override
    public void onError(CommandContext ctx, Throwable error) {
      EVENTS.add("onError");
    }
  }

  /** An application {@link AuditCommandInterceptor} writing to its own store, not the bean's. */
  @ApplicationScoped
  public static class ReplacingAuditInterceptorConfig {
    static final List<AuditEntry> APPLICATION_STORE = new CopyOnWriteArrayList<>();

    @Produces
    @Singleton
    public AuditCommandInterceptor applicationAuditInterceptor() {
      return new AuditCommandInterceptor(APPLICATION_STORE::add);
    }
  }
}
