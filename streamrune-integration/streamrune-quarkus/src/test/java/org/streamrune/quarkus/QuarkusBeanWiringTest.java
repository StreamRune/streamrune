package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import io.quarkus.arc.DefaultBean;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.enterprise.inject.literal.NamedLiteral;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.QueryBus;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.SagaDeadLetterRetentionSweeper;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Container-level DI tests for the Quarkus integration — the Quarkus mirror of {@code
 * MicronautBeanWiringTest}. Until this existed, <em>every</em> Quarkus test {@code new}-ed a {@link
 * StreamRuneProducers} and passed Mockito mocks, so real Arc CDI resolution — {@code @DefaultBean}
 * uniqueness, an application bean overriding a framework default, and {@code @ConfigMapping}
 * binding — was NEVER exercised. That is the exact defect class that shipped a Micronaut DI bug
 * past every review (the {@code commandInbox} interface-vs-concrete return-type regression that
 * {@code MicronautBeanWiringTest} now guards).
 *
 * <p><strong>Why not {@code @QuarkusTest} / {@code QuarkusUnitTest}?</strong> Both bootstrap by
 * loading a Quarkus <em>application model</em>: {@code @QuarkusTest}'s {@code
 * QuarkusGradleModelFactory} opens a nested Gradle connection that needs the {@code io.quarkus}
 * Gradle plugin applied (this module deliberately does not — it is a producers <em>library</em>,
 * not an application, and applying the app plugin would add {@code quarkusBuild}/native tasks and
 * an application-entrypoint expectation); {@code QuarkusUnitTest}'s {@code
 * BootstrapAppModelFactory} fails with "Unable to locate the maven project on the filesystem" in
 * this Gradle build, even with {@code setFlatClassPath(true)}. Both were verified to fail here. So
 * this test boots a <strong>real Arc container</strong> the way the Quarkus plugin's augmentation
 * step would — via {@link RealArcTestContainer}, which runs the genuine {@code BeanProcessor} over
 * a Jandex index and boots a real {@code ArcContainerImpl}. Bean lookups below go through real Arc
 * resolution, not Mockito.
 *
 * <p>The two {@code @DefaultBean} beans under test are produced by delegating to the <em>real</em>
 * {@link StreamRuneProducers} methods, so the framework's actual production code runs inside the
 * real container. Config binding is asserted against the real {@link StreamRuneQuarkusProperties}
 * {@code @ConfigMapping} via SmallRye Config — the same interface, defaults, and keys the producers
 * consume at runtime — because raw Arc does not itself wire the SmallRye config extension.
 */
class QuarkusBeanWiringTest {

  private static final StreamRuneProducers PRODUCERS = new StreamRuneProducers();

  /**
   * A framework-side producer set: the real {@code @DefaultBean} idiom, delegating to the actual
   * {@link StreamRuneProducers} so real framework code produces the bean inside the real container.
   */
  @ApplicationScoped
  public static class FrameworkDefaults {

    @Produces
    @Singleton
    @DefaultBean
    public EventTypeRegistry eventTypeRegistry() {
      return PRODUCERS.eventTypeRegistry();
    }

    @Produces
    @Singleton
    @DefaultBean
    public QueryBus queryBus() {
      // The real default query bus the producers fall back to when no metrics/cache/audit beans
      // exist (see StreamRuneProducers#simpleQueryBus / #queryBus with unsatisfied Instances) — a
      // dependency-free default, so it boots in the standalone container.
      return new org.streamrune.runtime.SimpleQueryBus();
    }
  }

  /** An application-defined bean of the same type that must WIN over the framework @DefaultBean. */
  @ApplicationScoped
  public static class ApplicationOverride {

    static final EventTypeRegistry APP_REGISTRY = SimpleEventTypeRegistry.builder().build();

    @Produces
    @Singleton
    public EventTypeRegistry appEventTypeRegistry() {
      return APP_REGISTRY;
    }
  }

  @Test
  void frameworkDefaultBeansResolveUniquely() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(FrameworkDefaults.class, EventTypeRegistry.class, QueryBus.class))) {
      var registry = arc.container().instance(EventTypeRegistry.class);
      assertTrue(
          registry.isAvailable(),
          "the framework @DefaultBean EventTypeRegistry must resolve (unambiguously)");
      assertInstanceOf(EventTypeRegistry.class, registry.get());

      var queryBus = arc.container().instance(QueryBus.class);
      assertTrue(queryBus.isAvailable(), "the framework @DefaultBean QueryBus must resolve");
    }
  }

  @Test
  void applicationBeanWinsOverFrameworkDefault() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                FrameworkDefaults.class,
                ApplicationOverride.class,
                EventTypeRegistry.class,
                QueryBus.class))) {
      var registry = arc.container().instance(EventTypeRegistry.class);
      assertTrue(registry.isAvailable(), "EventTypeRegistry must resolve without ambiguity");
      assertSame(
          ApplicationOverride.APP_REGISTRY,
          registry.get(),
          "an application-defined EventTypeRegistry must WIN over the framework @DefaultBean");
      assertNotSame(
          PRODUCERS.eventTypeRegistry().getClass(),
          registry.get(),
          "the framework default instance must not be the one resolved when an app bean exists");
    }
  }

  // ── the saga dead-letter retention sweeper in the default configuration ───────────

  /**
   * The only two beans a real Quarkus application supplies that raw Arc cannot: the {@code
   * DataSource} (Agroal) and the bound {@code @ConfigMapping} properties (the SmallRye config
   * extension). Everything else in the tests below comes from the <strong>real</strong> {@link
   * StreamRuneProducers} class, which is indexed into the bean archive so Arc resolves the genuine
   * producer signatures — the declared return types are exactly what is under test.
   */
  @ApplicationScoped
  public static class ApplicationInfrastructure {

    static final DataSource DATA_SOURCE = mock(DataSource.class);

    @Produces
    @Singleton
    public DataSource dataSource() {
      return DATA_SOURCE;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of());
    }
  }

  /**
   * Overrides the framework's Postgres-backed {@link StreamRuneProducers#eventStore} (which calls
   * {@code EventStoreFactory.create()} — real schema I/O against {@link
   * ApplicationInfrastructure#DATA_SOURCE}, a bare Mockito mock with no stubbed connection) with a
   * mock. A non-{@code @DefaultBean} producer always wins over the framework's {@code @DefaultBean}
   * one, so this is a faithful "application already has its own EventStore" shape. Used only by
   * tests that must fully CONSTRUCT a {@link VirtualThreadCommandBus} (proving unambiguous
   * resolution of a bean type it supplies), not merely resolve the factory.
   */
  @ApplicationScoped
  public static class ApplicationEventStoreOverride {
    @Produces
    @Singleton
    public org.streamrune.core.EventStore eventStore() {
      return mock(org.streamrune.core.EventStore.class);
    }
  }

  // Five command roots for the registration tests: three sibling roots under one type, two others.
  record PlaceIt(String id) implements Command {}

  record ConfirmIt(String id) implements Command {}

  record CancelIt(String id) implements Command {}

  record StockIt(String id) implements Command {}

  record ProbeIt(String id) implements Command {}

  record WiringState() implements AggregateState {}

  record ItHappened() implements DomainEvent {}

  /** One event per command; one instance per registration (one instance never spans two types). */
  static final class OneEventDecider<C extends Command>
      implements Decider<C, WiringState, ItHappened> {
    @Override
    public WiringState initialState() {
      return new WiringState();
    }

    @Override
    public List<ItHappened> decide(C command, WiringState state) {
      return List.of(new ItHappened());
    }

    @Override
    public WiringState evolve(WiringState state, ItHappened event) {
      return state;
    }
  }

  private static <C extends Command> DeciderRegistration<C, WiringState, ItHappened> registration(
      String type, Class<C> commandType, Function<C, String> id) {
    return new DeciderRegistration<>(
        AggregateType.of(type),
        commandType,
        command -> AggregateId.of(id.apply(command)),
        new OneEventDecider<C>());
  }

  /** Two registration beans for one command type, both under the type {@code dup}. */
  @ApplicationScoped
  public static class DuplicateRegistrations {
    @Produces
    @Singleton
    public DeciderRegistration<PlaceIt, WiringState, ItHappened> first() {
      return registration("dup", PlaceIt.class, PlaceIt::id);
    }

    @Produces
    @Singleton
    public DeciderRegistration<PlaceIt, WiringState, ItHappened> second() {
      return registration("dup", PlaceIt.class, PlaceIt::id);
    }
  }

  /** Five distinct registration beans: three sibling roots under {@code order}, two other types. */
  @ApplicationScoped
  public static class FiveRegistrations {
    @Produces
    @Singleton
    public DeciderRegistration<PlaceIt, WiringState, ItHappened> place() {
      return registration("order", PlaceIt.class, PlaceIt::id);
    }

    @Produces
    @Singleton
    public DeciderRegistration<ConfirmIt, WiringState, ItHappened> confirm() {
      return registration("order", ConfirmIt.class, ConfirmIt::id);
    }

    @Produces
    @Singleton
    public DeciderRegistration<CancelIt, WiringState, ItHappened> cancel() {
      return registration("order", CancelIt.class, CancelIt::id);
    }

    @Produces
    @Singleton
    public DeciderRegistration<StockIt, WiringState, ItHappened> stock() {
      return registration("inventory", StockIt.class, StockIt::id);
    }

    @Produces
    @Singleton
    public DeciderRegistration<ProbeIt, WiringState, ItHappened> probe() {
      return registration("test", ProbeIt.class, ProbeIt::id);
    }
  }

  @Test
  void twoRegistrationsOfOneCommandType_failTheBusInTheRealContainer() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ApplicationInfrastructure.class,
                ApplicationEventStoreOverride.class,
                DuplicateRegistrations.class))) {
      var failure =
          assertThrows(
              RuntimeException.class,
              () -> arc.container().instance(VirtualThreadCommandBus.class).get());
      Throwable root = failure;
      while (root.getCause() != null) {
        root = root.getCause();
      }
      assertInstanceOf(IllegalArgumentException.class, root);
      assertTrue(
          root.getMessage().contains("is already registered (aggregate type 'dup')"),
          root.getMessage());
    }
  }

  @Test
  void fiveDistinctRegistrations_resolveTheBusInTheRealContainer() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ApplicationInfrastructure.class,
                ApplicationEventStoreOverride.class,
                FiveRegistrations.class))) {
      var bus = arc.container().instance(VirtualThreadCommandBus.class);
      assertTrue(bus.isAvailable());
      assertNotNull(bus.get());
    }
  }

  /** An application-supplied saga dead-letter store, declared on the interface as a user would. */
  @ApplicationScoped
  public static class ApplicationSagaDeadLetterStore {

    static final SagaDeadLetterStore USER_STORE = mock(SagaDeadLetterStore.class);

    @Produces
    @Singleton
    public SagaDeadLetterStore userSagaDeadLetterStore() {
      return USER_STORE;
    }
  }

  @Test
  void sagaDeadLetterRetentionSweeperResolvesInTheDefaultConfiguration() throws Exception {
    // The sagaDeadLetterStore producer declared the SagaDeadLetterStore INTERFACE as its return
    // type while sagaDeadLetterRetentionSweeper injects Instance<PostgresSagaDeadLetterStore>. CDI
    // bean types are the closure of the producer's DECLARED return type, so that Instance was
    // permanently unsatisfied against the framework's own default bean and the producer returned
    // null with no log: the sweeper was silently absent in every default deployment and
    // saga_dead_letters grew without bound (a GDPR storage-limitation violation). The
    // boot validator only checks the STORE by interface type, so it passed while the sweeper was
    // missing.
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, ApplicationInfrastructure.class))) {
      var concreteStore =
          arc.container().instance(org.streamrune.postgres.PostgresSagaDeadLetterStore.class);
      assertTrue(
          concreteStore.isAvailable(),
          "the default store must be resolvable by its concrete type — that is what the sweeper's"
              + " Instance<PostgresSagaDeadLetterStore> injection point asks for");

      var interfaceStore = arc.container().instance(SagaDeadLetterStore.class);
      assertTrue(
          interfaceStore.isAvailable(),
          "the concrete store must still satisfy interface-typed consumers");
      assertInstanceOf(
          org.streamrune.postgres.PostgresSagaDeadLetterStore.class, interfaceStore.get());

      var sweeper = arc.container().instance(SagaDeadLetterRetentionSweeper.class);
      assertTrue(
          sweeper.isAvailable(),
          "the SagaDeadLetterRetentionSweeper bean must exist in the default configuration");
      assertNotNull(
          sweeper.get(),
          "the sweeper producer must not return null in the default configuration — without it"
              + " saga_dead_letters is never pruned");
      // Every produced sweeper is registered for relay liveness health.
      var relayHealth =
          arc.container()
              .instance(org.streamrune.runtime.BackgroundRelayHealthContributor.class)
              .get();
      assertTrue(
          relayHealth.components().stream()
              .anyMatch(c -> c.name().equals("saga-dead-letter-retention-sweeper")),
          "the produced saga dead-letter retention sweeper must be registered with the relay-health"
              + " contributor so a dead sweep thread reports DOWN");

      // StreamRuneLifecycle starts the sweeper through exactly this named lookup.
      var namedSweeper =
          arc.container()
              .instance(
                  SagaDeadLetterRetentionSweeper.class,
                  NamedLiteral.of(
                      StreamRuneProducers.FRAMEWORK_SAGA_DEAD_LETTER_RETENTION_SWEEPER));
      assertNotNull(
          namedSweeper.get(),
          "the framework sweeper must resolve under the name StreamRuneLifecycle injects");
    }
  }

  @Test
  void applicationSagaDeadLetterStoreWinsInterfaceResolution() throws Exception {
    // Override semantics stay on the INTERFACE: @DefaultBean makes the framework producer lose
    // interface-typed resolution to a user-supplied SagaDeadLetterStore even though the default now
    // declares the concrete PostgresSagaDeadLetterStore return type. (Arc's @DefaultBean resolves
    // ambiguity rather than removing the bean, so the framework's own Postgres store — and the
    // sweeper that prunes the table the framework created — stay registered.)
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ApplicationInfrastructure.class,
                ApplicationSagaDeadLetterStore.class))) {
      var store = arc.container().instance(SagaDeadLetterStore.class);
      assertTrue(store.isAvailable(), "SagaDeadLetterStore must resolve without ambiguity");
      assertSame(
          ApplicationSagaDeadLetterStore.USER_STORE,
          store.get(),
          "an application-supplied SagaDeadLetterStore must WIN over the framework @DefaultBean");
    }
  }

  // ── the command dead-letter retention sweeper follows the queue bean, not the retry switch ──

  /** {@link ApplicationInfrastructure} with the automatic dead-letter retry switched off. */
  @ApplicationScoped
  public static class ApplicationInfrastructureWithRetryDisabled {

    @Produces
    @Singleton
    public DataSource dataSource() {
      return ApplicationInfrastructure.DATA_SOURCE;
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bind(Map.of("streamrune.dead-letter.enabled", "false"));
    }
  }

  /** An application-supplied command dead-letter queue — the framework registers none itself. */
  @ApplicationScoped
  public static class ApplicationDeadLetterQueue {

    @Produces
    @Singleton
    public org.streamrune.core.DeadLetterQueue deadLetterQueue() {
      return mock(org.streamrune.core.DeadLetterQueue.class);
    }
  }

  @Test
  void deadLetterRetentionSweeperSurvivesDisablingTheAutomaticRetryRunner() throws Exception {
    // streamrune.dead-letter.enabled switches the automatic retry runner only. The command bus
    // still dead-letters into the queue bean, so the sweeper that bounds how long those payloads
    // stay queryable must still be produced, health-registered and resolvable under the name
    // StreamRuneLifecycle injects.
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ApplicationInfrastructureWithRetryDisabled.class,
                ApplicationDeadLetterQueue.class))) {
      var sweeper =
          arc.container()
              .instance(
                  org.streamrune.runtime.DeadLetterRetentionSweeper.class,
                  NamedLiteral.of(StreamRuneProducers.FRAMEWORK_DEAD_LETTER_RETENTION_SWEEPER));
      assertNotNull(
          sweeper.get(),
          "the dead-letter retention sweeper must be produced while a DeadLetterQueue bean exists,"
              + " whatever streamrune.dead-letter.enabled says");
      var relayHealth =
          arc.container()
              .instance(org.streamrune.runtime.BackgroundRelayHealthContributor.class)
              .get();
      assertTrue(
          relayHealth.components().stream()
              .anyMatch(c -> c.name().equals("dead-letter-retention-sweeper")),
          "and its liveness must still be reported through the relay health");
      assertEquals(
          null,
          arc.container()
              .instance(
                  org.streamrune.runtime.DeadLetterRetryRunner.class,
                  NamedLiteral.of(StreamRuneProducers.FRAMEWORK_DEAD_LETTER_RETRY_RUNNER))
              .get(),
          "while the automatic retry runner stays off");
    }
  }

  // ── the advertised CommandInbox override must not become a runtime trap ────────────

  /** An application-supplied command inbox, declared on the interface as the javadoc invites. */
  @ApplicationScoped
  public static class ApplicationCommandInbox {

    @Produces
    @Singleton
    public org.streamrune.core.CommandInbox userCommandInbox() {
      return new org.streamrune.test.InMemoryCommandInbox();
    }
  }

  /**
   * The command bus injects {@code Instance<CommandInbox>} and the event-store factory injects the
   * concrete {@code Instance<PostgresCommandInbox>} (deliberately concrete), so a user-supplied
   * {@code CommandInbox} wins the bus's interface-typed point while the framework's
   * {@code @DefaultBean} still satisfies the factory's concrete one — Arc's {@code @DefaultBean}
   * resolves ambiguity rather than removing the bean. The store would then keep claiming
   * idempotency keys in the framework's own {@code command_inbox} table while the bus pre-checks
   * the user's inbox: the override silently ignored on the authoritative dedup path, or (where the
   * default is absent) every keyed execution dying inside {@code appendWithKey}. It must fail fast
   * at wiring, and only a real container reproduces the split resolution.
   */
  @Test
  void customCommandInboxWithTheFrameworkEventStoreFactoryFailsFast() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ApplicationInfrastructure.class,
                ApplicationCommandInbox.class))) {
      var failure =
          org.junit.jupiter.api.Assertions.assertThrows(
              RuntimeException.class,
              () -> arc.container().instance(org.streamrune.core.EventStoreFactory.class).get(),
              "a custom CommandInbox without a custom EventStoreFactory must fail at wiring");
      String message = causeChainMessages(failure);
      assertTrue(
          message.contains("command-inbox wiring"),
          "the failure must name the inbox wiring invariant, got: " + message);
      assertTrue(
          message.contains("appendWithKey"),
          "the failure must explain the appendWithKey contract, got: " + message);
    }
  }

  @Test
  void defaultCommandInboxWiringBuildsTheFactory() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, ApplicationInfrastructure.class))) {
      assertNotNull(
          arc.container().instance(org.streamrune.core.EventStoreFactory.class).get(),
          "the default configuration (framework inbox on both sides) must still build");
    }
  }

  private static String causeChainMessages(Throwable t) {
    var messages = new StringBuilder();
    for (Throwable current = t; current != null; current = current.getCause()) {
      messages.append(current.getMessage()).append('\n');
    }
    return messages.toString();
  }

  // ── QueryBus and AsyncCommandBus must resolve unambiguously ─────

  /**
   * {@link StreamRuneProducers#simpleQueryBus} and {@link StreamRuneProducers#queryBus} were BOTH
   * {@code @DefaultBean} and both carried the {@link QueryBus} CDI bean type ({@code SimpleQueryBus
   * implements QueryBus}), so a real Arc container could never resolve a plain {@code @Inject
   * QueryBus} injection point: {@code Beans.resolveAmbiguity} strips every {@code @DefaultBean}
   * candidate first, finds the remaining set empty, falls back to comparing {@code @DefaultBean}
   * priorities, finds both producers at the default priority 0, and reports the injection point
   * AMBIGUOUS — a real Quarkus application's BUILD fails the moment it injects the documented
   * primary read-side API. Every prior Quarkus test either called the producer method directly (no
   * CDI) or, like {@link #frameworkDefaultBeansResolveUniquely} above, registered a hand-written
   * stand-in producer set with a single {@code QueryBus} producer — neither exercises the real
   * {@link StreamRuneProducers} class's actual bean-type shape. This test does: it boots the real
   * producer class and resolves {@code QueryBus} through real Arc.
   */
  @Test
  void queryBusResolvesUniquelyThroughTheRealProducerSet() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, ApplicationInfrastructure.class))) {
      var queryBus = arc.container().instance(QueryBus.class);
      assertNotNull(
          queryBus.get(),
          "QueryBus must resolve unambiguously through the real producer set — the composite"
              + " queryBus() producer, not the building blocks (SimpleQueryBus/CachingQueryBus),"
              + " must be the sole QueryBus-typed candidate");
    }
  }

  /**
   * {@link StreamRuneProducers#virtualThreadCommandBus} (return type {@code
   * VirtualThreadCommandBus}, which {@code implements CommandBus, AsyncCommandBus}) and the
   * redundant {@code asyncCommandBus} producer (return type {@code AsyncCommandBus}) were BOTH
   * {@code @DefaultBean} and both carried the {@link org.streamrune.core.AsyncCommandBus} CDI bean
   * type, so a real Arc container could never resolve a plain {@code @Inject AsyncCommandBus}
   * injection point — the identical ambiguity as {@link
   * #queryBusResolvesUniquelyThroughTheRealProducerSet}, one redundant producer method away.
   */
  @Test
  void asyncCommandBusResolvesUniquelyThroughTheRealProducerSet() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ApplicationInfrastructure.class,
                ApplicationEventStoreOverride.class))) {
      var asyncBus = arc.container().instance(org.streamrune.core.AsyncCommandBus.class);
      assertNotNull(
          asyncBus.get(),
          "AsyncCommandBus must resolve unambiguously through the real producer set — the"
              + " VirtualThreadCommandBus producer alone must supply this bean type");
    }
  }

  // ── @ConfigMapping binding (the real StreamRuneQuarkusProperties interface) ──────────────────

  private static StreamRuneQuarkusProperties bind(Map<String, String> overrides) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            // Register the same Quarkus Duration converter the runtime uses, so short forms like
            // "11s" bind exactly as they do in a Quarkus app (raw SmallRye only accepts ISO-8601).
            .withConverter(
                Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
            .withDefaultValues(overrides)
            .build();
    return config.getConfigMapping(StreamRuneQuarkusProperties.class);
  }

  @Test
  void configBindsDefaultsFromWithDefault() {
    var props = bind(Map.of());
    assertEquals(100, props.snapshotEveryNEvents(), "snapshot-every-n-events default");
    assertEquals(3, props.retryMaxAttempts(), "retry-max-attempts default");
    assertEquals(Duration.ofSeconds(5), props.lockTimeout(), "lock-timeout default");
    assertEquals(1024, props.stripeCount(), "stripe-count default");
    assertTrue(props.saga().enabled(), "saga.enabled defaults to true");
    assertEquals(false, props.outbox().enabled(), "outbox.enabled defaults to false");
    assertTrue(
        props.eventStore().schema().autoInitialize(),
        "event-store.schema.auto-initialize defaults to true");
  }

  @Test
  void configBindsOverriddenKeys_includingNestedGroups() {
    var props =
        bind(
            Map.of(
                "streamrune.snapshot-every-n-events", "42",
                "streamrune.lock-timeout", "11s",
                "streamrune.saga.enabled", "false",
                "streamrune.outbox.enabled", "true",
                "streamrune.outbox.batch-size", "7",
                "streamrune.event-store.schema.auto-initialize", "false"));
    assertEquals(42, props.snapshotEveryNEvents());
    assertEquals(Duration.ofSeconds(11), props.lockTimeout());
    assertEquals(false, props.saga().enabled(), "nested streamrune.saga.enabled must bind");
    assertTrue(props.outbox().enabled(), "nested streamrune.outbox.enabled must bind");
    assertEquals(7, props.outbox().batchSize(), "nested streamrune.outbox.batch-size must bind");
    assertEquals(
        false,
        props.eventStore().schema().autoInitialize(),
        "doubly-nested streamrune.event-store.schema.auto-initialize must bind");
    assertEquals(3, props.retryMaxAttempts(), "unset keys keep their @WithDefault");
  }
}
