package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.enterprise.inject.spi.BeanManager;
import jakarta.enterprise.inject.spi.ObserverMethod;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import jakarta.interceptor.Interceptor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.RequireRole;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.ScheduledProjectionRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemorySagaDeadLetterStore;
import org.streamrune.test.InMemorySagaStore;

/**
 * {@link StreamRuneLifecycle#onStart} and all three startup validators ({@link
 * StreamRuneConfigValidator}, {@link StreamRuneAuthorizationValidator}, {@link
 * SagaStateCryptoValidator}) were plain {@code @Observes StartupEvent} with NO {@code @Priority}
 * anywhere in the module, so CDI/Arc observer ordering among them was UNSPECIFIED — the runners'
 * first poll (immediate, {@code ResilientPollLoop}) could persist data before a fail-closed
 * validator ever ran. Spring already enforces this ordering structurally ({@code
 * SmartInitializingSingleton} always completes before any {@code SmartLifecycle} starts); Quarkus
 * needs an explicit {@code @Priority} band since CDI has no such phase split for one event type.
 *
 * <p>Two complementary checks:
 *
 * <ol>
 *   <li>{@link #everyValidatorOutranksTheLifecycleStarter()} — a reflective pin on the exact
 *       priority values, fast and with no container.
 *   <li>{@link #aFailingValidatorPreventsAnyRunnerFromStarting()} — proves the CONSEQUENCE using
 *       the real {@code validate}/{@code onStart} method bodies, invoked in
 *       ascending-{@code @Priority} order with the first-exception-aborts-the-rest semantics CDI's
 *       synchronous {@code Event#fire} uses for observers of one event. This module cannot boot a
 *       real {@code @Observes} dispatch for these two classes through {@code RealArcTestContainer}:
 *       both are {@code @ApplicationScoped} with a non-default (constructor-injected) constructor,
 *       and a client proxy for that needs the build-time bytecode transformer the {@code
 *       io.quarkus} Gradle plugin supplies — which this raw-Arc harness deliberately does not have
 *       (see {@code StreamRuneConfigValidatorTest}'s {@code SagasDisabledInfrastructure} javadoc
 *       for the same constraint, worked around the same way: construct the real objects directly).
 * </ol>
 */
class StreamRuneLifecycleOrderingTest {

  /**
   * Arc computes observer priority in {@code io.quarkus.arc.processor.ObserverInfo.create} from
   * {@code Annotations.getParameterAnnotations(...)} on the EVENT PARAMETER only — a method-level
   * {@code @Priority} compiles (CDI 4 allows {@code @Priority} on a method for producer/alternative
   * prioritization) but is silently ignored for observer ordering. This helper therefore reads the
   * annotation off {@code StartupEvent event}'s parameter, and asserts the method itself carries
   * none, so a future regression that puts {@code @Priority} back on the method fails here instead
   * of passing while the container ignores it.
   */
  private static int priorityOf(Class<?> type, String methodName) throws NoSuchMethodException {
    Method m = type.getDeclaredMethod(methodName, StartupEvent.class);
    assertNull(
        m.getAnnotation(Priority.class),
        () ->
            type.getSimpleName()
                + "."
                + methodName
                + " must not carry @Priority on the METHOD — Arc's observer-priority resolution"
                + " reads it from the StartupEvent PARAMETER only, and a method-level annotation is"
                + " silently ignored");
    Priority p = m.getParameters()[0].getAnnotation(Priority.class);
    assertNotNull(
        p,
        () ->
            type.getSimpleName()
                + "."
                + methodName
                + " must declare @Priority on the StartupEvent PARAMETER so its ordering relative"
                + " to the other StartupEvent observers is not left to CDI's unspecified default");
    return p.value();
  }

  @Test
  void everyValidatorOutranksTheLifecycleStarter() throws Exception {
    int configValidator = priorityOf(StreamRuneConfigValidator.class, "validate");
    int authValidator = priorityOf(StreamRuneAuthorizationValidator.class, "validate");
    int cryptoValidator = priorityOf(SagaStateCryptoValidator.class, "validate");
    int lifecycleStart = priorityOf(StreamRuneLifecycle.class, "onStart");

    assertTrue(
        configValidator < lifecycleStart,
        "StreamRuneConfigValidator must run before StreamRuneLifecycle.onStart");
    assertTrue(
        authValidator < lifecycleStart,
        "StreamRuneAuthorizationValidator must run before StreamRuneLifecycle.onStart");
    assertTrue(
        cryptoValidator < lifecycleStart,
        "SagaStateCryptoValidator must run before StreamRuneLifecycle.onStart");
    // The request-identity startup rule is a fail-closed validator like the others.
    int identityValidator = priorityOf(StreamRuneRequestIdentityValidator.class, "validate");
    assertTrue(
        identityValidator < lifecycleStart,
        "StreamRuneRequestIdentityValidator must run before StreamRuneLifecycle.onStart");
  }

  @RequireRole("ADMIN")
  record GuardedCommand(String id) implements Command {}

  record FixtureState() implements AggregateState {}

  record FixtureEvent() implements DomainEvent {}

  private static DeciderRegistration<GuardedCommand, FixtureState, FixtureEvent>
      guardedRegistration() {
    return new DeciderRegistration<>(
        AggregateType.of("fixture"),
        GuardedCommand.class,
        cmd -> AggregateId.of("fixture-1"),
        new Decider<GuardedCommand, FixtureState, FixtureEvent>() {
          @Override
          public FixtureState initialState() {
            return new FixtureState();
          }

          @Override
          public List<FixtureEvent> decide(GuardedCommand command, FixtureState state) {
            return List.of();
          }

          @Override
          public FixtureState evolve(FixtureState state, FixtureEvent event) {
            return state;
          }
        });
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> satisfied(T value) {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(false);
    when(inst.isResolvable()).thenReturn(true);
    when(inst.get()).thenReturn(value);
    return inst;
  }

  @SuppressWarnings("unchecked")
  private static <T> Instance<T> unsatisfied() {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(true);
    when(inst.isResolvable()).thenReturn(false);
    return inst;
  }

  /**
   * The command bus as the validator meets it: resolving the bean builds the bus, and its builder
   * refuses the guarded command because no {@code AnnotationAuthorizationInterceptor} is among its
   * interceptors.
   */
  @SuppressWarnings("unchecked")
  private static Instance<VirtualThreadCommandBus> aBusItsBuilderRefuses() {
    Instance<VirtualThreadCommandBus> inst = mock(Instance.class);
    when(inst.iterator())
        .thenAnswer(
            invocation -> {
              var guarded = guardedRegistration();
              return List.of(
                      VirtualThreadCommandBus.builder()
                          .eventStore(mock(org.streamrune.core.EventStore.class))
                          .register(
                              guarded.aggregateType(),
                              guarded.commandType(),
                              guarded.idExtractor(),
                              guarded.decider())
                          .build())
                  .iterator();
            });
    return inst;
  }

  /**
   * Invokes {@code target.methodName(new StartupEvent())} via reflection, unwrapping {@link
   * InvocationTargetException} so callers see the same exception a direct call would throw —
   * mirrors how a real CDI {@code Event#fire} propagates an observer's exception to the firer.
   */
  private static void invokeObserver(Object target, String methodName) throws Throwable {
    Method m = target.getClass().getDeclaredMethod(methodName, StartupEvent.class);
    m.setAccessible(true);
    try {
      m.invoke(target, new StartupEvent());
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Test
  void aFailingValidatorPreventsAnyRunnerFromStarting() throws Exception {
    // A @RequireRole-guarded command on a bus with NO AnnotationAuthorizationInterceptor — the bus
    // is refused the moment the validator resolves it.
    var authValidator = new StreamRuneAuthorizationValidator(aBusItsBuilderRefuses());

    MultiProjectionRunner runner = mock(MultiProjectionRunner.class);
    var lifecycle =
        new StreamRuneLifecycle(
            satisfied(runner),
            this.<ScheduledProjectionRunner>unsatisfied(),
            this.<org.streamrune.runtime.OutboxPoller>unsatisfied(),
            this.<org.streamrune.runtime.OutboxRetentionSweeper>unsatisfied(),
            this.<org.streamrune.runtime.InboxRetentionSweeper>unsatisfied(),
            this.<org.streamrune.runtime.SagaDeadLetterRetentionSweeper>unsatisfied(),
            this.<org.streamrune.runtime.DeadLetterRetentionSweeper>unsatisfied(),
            this.<org.streamrune.runtime.DeadLetterRetryRunner>unsatisfied(),
            this.<org.streamrune.core.subscription.SubscriptionLeadership>unsatisfied(),
            this.<VirtualThreadCommandBus>unsatisfied(),
            mock(jakarta.enterprise.inject.spi.BeanManager.class));

    // Simulates CDI's synchronous Event#fire: observers run in ascending @Priority order, and an
    // exception from one aborts delivery to the rest.
    record PriorityObserver(int priority, Object target, String method) {}
    var observers = new ArrayList<PriorityObserver>();
    observers.add(
        new PriorityObserver(
            priorityOf(StreamRuneAuthorizationValidator.class, "validate"),
            authValidator,
            "validate"));
    observers.add(
        new PriorityObserver(
            priorityOf(StreamRuneLifecycle.class, "onStart"), lifecycle, "onStart"));
    observers.sort(Comparator.comparingInt(PriorityObserver::priority));

    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> {
              for (var observer : observers) {
                try {
                  invokeObserver(observer.target(), observer.method());
                } catch (IllegalStateException e) {
                  throw e;
                } catch (Throwable e) {
                  throw new AssertionError(e);
                }
              }
            },
            "a guarded command with no enforcement interceptor must fail startup");
    assertTrue(thrown.getMessage().contains("UNGUARDED"));

    // The failing validator must run BEFORE StreamRuneLifecycle.onStart, so the runner it would
    // otherwise have started must never see start().
    verify(runner, never()).start();
  }

  // ── REAL Arc container pins ───────────────────────────────────────────────────────────
  //
  // The two tests above prove the claim only against a hand-rolled simulation of CDI dispatch —
  // exactly the gap these tests close (the simulation reads @Priority off the method and sorts by
  // it, so it stayed green while method-level @Priority was silently ignored by the real
  // container). These two tests boot a REAL Arc container (RealArcTestContainer) with the actual
  // bean classes and either (1) ask Arc itself what priority it resolved for each observer, or
  // (2) fire a real StartupEvent and observe the consequence — no in-test dispatch simulation.
  //
  // Indexing these @ApplicationScoped, constructor-injected classes directly as Arc bean classes
  // (rather than resolving their dependencies through a producer and calling `new` by hand, the
  // workaround StreamRuneConfigValidatorTest/the two tests above use) turns out to work fine here:
  // RealArcTestContainer sets `setTransformUnproxyableClasses(true)`, and
  // SagaEventPathRetentionContainerWiringTest already proves the pattern end to end for another
  // @ApplicationScoped/@Inject-constructor class in this same module
  // (SagaEventPathRetentionConfigurer), including firing a real StartupEvent through
  // `beanManager().getEvent().select(StartupEvent.class).fire(...)`. The "this raw-Arc harness
  // cannot proxy these classes" premise in StreamRuneConfigValidatorTest's/this class's own
  // javadoc (above) is therefore stale for this purpose; corrected by these tests.

  /** Binds the real {@code @ConfigMapping} through SmallRye with Quarkus's Duration converter. */
  private static StreamRuneQuarkusProperties bind(Map<String, String> overrides) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withConverter(
                Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
            .withDefaultValues(overrides)
            .build();
    return config.getConfigMapping(StreamRuneQuarkusProperties.class);
  }

  /** Just enough config for the four beans to deploy; no validator fails. */
  @ApplicationScoped
  public static class DefaultConfigInfrastructure {
    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of());
    }
  }

  @Test
  void arcResolvesObserverPriorityFromTheEventParameter_notTheMethod() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneConfigValidator.class,
                StreamRuneAuthorizationValidator.class,
                SagaStateCryptoValidator.class,
                StreamRuneLifecycle.class,
                SagaCompensationRetryLifecycle.class,
                // SagaEventPathRetentionConfigurer.apply had no
                // @Priority, so it ran deterministically AFTER the two 1100 runner starters below —
                // its event-path key-age guard landed too late for a SagaRunner bean.
                SagaEventPathRetentionConfigurer.class,
                DefaultConfigInfrastructure.class))) {
      BeanManager beanManager = arc.container().beanManager();
      // CDI observer resolution — no dispatch, no side effects — against a real deployment's
      // ObserverMethod metadata, i.e. exactly what io.quarkus.arc.processor.ObserverInfo.create
      // computed at build time.
      Set<ObserverMethod<? super StartupEvent>> observers =
          beanManager.resolveObserverMethods(new StartupEvent());

      Map<Class<?>, Integer> priorityByBeanClass = new HashMap<>();
      for (ObserverMethod<? super StartupEvent> observer : observers) {
        priorityByBeanClass.put(observer.getBeanClass(), observer.getPriority());
      }

      Integer configValidatorPriority = priorityByBeanClass.get(StreamRuneConfigValidator.class);
      Integer authValidatorPriority =
          priorityByBeanClass.get(StreamRuneAuthorizationValidator.class);
      Integer cryptoValidatorPriority = priorityByBeanClass.get(SagaStateCryptoValidator.class);
      Integer lifecycleStartPriority = priorityByBeanClass.get(StreamRuneLifecycle.class);
      // SagaCompensationRetryLifecycle is a second runner-starting startup listener, left
      // out of the startup-validator ordering fix.
      Integer sagaSweeperLifecycleStartPriority =
          priorityByBeanClass.get(SagaCompensationRetryLifecycle.class);
      // Must resolve BEFORE both 1100 runner starters, mirroring
      // the validators above — before this fix it had no @Priority at all (CDI default 2500).
      Integer sagaEventPathRetentionConfigurerPriority =
          priorityByBeanClass.get(SagaEventPathRetentionConfigurer.class);

      assertEquals(
          Interceptor.Priority.LIBRARY_BEFORE,
          configValidatorPriority,
          "Arc must resolve StreamRuneConfigValidator.validate at LIBRARY_BEFORE — a value it can"
              + " only see on the event parameter, never the method");
      assertEquals(Interceptor.Priority.LIBRARY_BEFORE, authValidatorPriority);
      assertEquals(Interceptor.Priority.LIBRARY_BEFORE, cryptoValidatorPriority);
      assertEquals(
          Interceptor.Priority.LIBRARY_BEFORE + 100,
          lifecycleStartPriority,
          "Arc must resolve StreamRuneLifecycle.onStart at LIBRARY_BEFORE + 100");
      assertEquals(
          Interceptor.Priority.LIBRARY_BEFORE + 100,
          sagaSweeperLifecycleStartPriority,
          "Arc must resolve SagaCompensationRetryLifecycle.onStart at LIBRARY_BEFORE + 100 too");
      assertEquals(
          Interceptor.Priority.LIBRARY_BEFORE,
          sagaEventPathRetentionConfigurerPriority,
          "Arc must resolve SagaEventPathRetentionConfigurer.apply at LIBRARY_BEFORE — before this"
              + " fix it had no @Priority at all and ran deterministically AFTER both 1100"
              + " runner starters, so the event-path key-age guard applied too late for a SagaRunner"
              + " bean");

      assertTrue(configValidatorPriority < lifecycleStartPriority);
      assertTrue(authValidatorPriority < lifecycleStartPriority);
      assertTrue(cryptoValidatorPriority < lifecycleStartPriority);
      assertTrue(configValidatorPriority < sagaSweeperLifecycleStartPriority);
      assertTrue(authValidatorPriority < sagaSweeperLifecycleStartPriority);
      assertTrue(cryptoValidatorPriority < sagaSweeperLifecycleStartPriority);
      assertTrue(
          sagaEventPathRetentionConfigurerPriority < lifecycleStartPriority,
          "the retention configurer must apply the key-age guard before StreamRuneLifecycle starts"
              + " any runner");
      assertTrue(
          sagaEventPathRetentionConfigurerPriority < sagaSweeperLifecycleStartPriority,
          "the retention configurer must apply the key-age guard before the saga compensation-retry"
              + " sweeper starts");
    }
  }

  /** streamrune.snapshot-every-n-events=0 fails StreamRuneConfigValidator's very first check. */
  @ApplicationScoped
  public static class FailingConfigInfrastructure {
    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of("streamrune.snapshot-every-n-events", "0"));
    }

    @Produces
    @Named(ProjectionProducer.FRAMEWORK_MULTI_RUNNER)
    @Singleton
    public MultiProjectionRunner frameworkMultiProjectionRunner() {
      return mock(MultiProjectionRunner.class);
    }

    // A real SagaStore + SagaRunner so SagaCompensationRetryLifecycle.startSweepers() would
    // reach its actual sweeper-build loop if it ran — otherwise "sweepers() is empty" would hold
    // trivially regardless of whether the ordering fix works, since startSweepers() also
    // short-circuits on an empty/null saga store (see the early guards in that method).
    @Produces
    @Singleton
    public SagaStore sagaStore() {
      return new InMemorySagaStore();
    }

    @Produces
    @Singleton
    public SagaRunner<SagaOrderingFixtureState> sagaOrderingFixtureRunner(SagaStore store) {
      return SagaRunner.<SagaOrderingFixtureState>builder()
          .orchestrator(new SagaOrderingFixtureOrchestrator())
          .sagaStore(store)
          .commandBus(NOOP_SAGA_BUS)
          .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(store))
          .build();
    }
  }

  record SagaOrderingFixtureState(SagaStatus status, String id) implements SagaState {}

  static final class SagaOrderingFixtureOrchestrator
      implements SagaOrchestrator<SagaOrderingFixtureState> {
    @Override
    public Class<SagaOrderingFixtureState> stateType() {
      return SagaOrderingFixtureState.class;
    }

    @Override
    public SagaOrderingFixtureState initialState(SagaId sagaId) {
      return new SagaOrderingFixtureState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      return SagaId.of("x");
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.empty();
    }

    @Override
    public SagaOrderingFixtureState evolve(SagaOrderingFixtureState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(SagaOrderingFixtureState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        SagaOrderingFixtureState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  static final CommandBus NOOP_SAGA_BUS =
      new CommandBus() {
        @Override
        public boolean supportsIdempotentExecution() {
          return true;
        }

        @Override
        public <C extends Command> CommandResult execute(C command) {
          throw new UnsupportedOperationException();
        }

        @Override
        public <C extends Command> CommandResult execute(C command, IdempotencyKey key) {
          throw new UnsupportedOperationException();
        }
      };

  @Test
  void aFailingValidatorPreventsAnyRunnerFromStarting_realArcContainer() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneConfigValidator.class,
                StreamRuneAuthorizationValidator.class,
                SagaStateCryptoValidator.class,
                StreamRuneLifecycle.class,
                SagaCompensationRetryLifecycle.class,
                FailingConfigInfrastructure.class))) {
      MultiProjectionRunner runner =
          arc.container()
              .select(
                  MultiProjectionRunner.class,
                  NamedLiteral.of(ProjectionProducer.FRAMEWORK_MULTI_RUNNER))
              .get();
      SagaCompensationRetryLifecycle sagaSweeperLifecycle =
          arc.container().select(SagaCompensationRetryLifecycle.class).get();

      // A real Event#fire — CDI's synchronous, ascending-@Priority, first-exception-aborts-the-
      // rest dispatch — not the in-test simulation the two tests above use.
      IllegalStateException thrown =
          assertThrows(
              IllegalStateException.class,
              () ->
                  arc.container()
                      .beanManager()
                      .getEvent()
                      .select(StartupEvent.class)
                      .fire(new StartupEvent()),
              "streamrune.snapshot-every-n-events=0 must fail boot");
      assertTrue(thrown.getMessage().contains("snapshot-every-n-events"));

      // StreamRuneConfigValidator (LIBRARY_BEFORE) must run and abort delivery BEFORE
      // StreamRuneLifecycle.onStart / SagaCompensationRetryLifecycle.onStart (both
      // LIBRARY_BEFORE + 100) ever see the event.
      verify(runner, never()).start();
      // With a real SagaRunner + SagaStore wired (so startSweepers() would reach its build
      // loop if invoked), no sweeper was built at all.
      assertTrue(
          sagaSweeperLifecycle.sweepers().isEmpty(),
          "SagaCompensationRetryLifecycle.onStart must never run when a validator fails boot"
              + " — a re-drive of a COMPENSATING saga must never start before"
              + " SagaStateCryptoValidator/StreamRuneAuthorizationValidator have run");
    }
  }
}
