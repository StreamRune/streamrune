package org.streamrune.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.AggregateLocker;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.Decider;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.SnapshotPolicy;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRunner;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.upcasting.EventUpcaster;

/**
 * Top-level facade for the StreamRune CQRS/ES framework. Provides a fluent builder API for
 * registering deciders and configuring the runtime.
 */
public final class StreamRune implements CommandBus, AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(StreamRune.class);

  private final VirtualThreadCommandBus commandBus;
  private final List<ProjectionRegistration> projections;

  // Exactly one of these two is non-null, matching
  // resolveProjectionRunners()'s mutual exclusion of projectionRunner(...) /
  // projectionRunnerFactory(...). projectionRunner is the caller-supplied SHARED instance used for
  // every registration (the caller's responsibility to supply one capable of multiplexing distinct
  // projection names if there is more than one registration). projectionRunnersByRegistration is
  // populated ONLY for the factory form — one FRESH instance per registerProjection call, same
  // order/size as `projections` — so a one-projection-per-instance runner (the only shipped
  // continuous runner, ContinuousProjectionRunner) works correctly with more than one registration
  // through the factory: before this, the factory was invoked once and startProjections() called
  // run() on that single instance for every registration, so registration #2 (and any later one)
  // always hit IllegalStateException("Runner already running") and never ran.
  private final ProjectionRunner projectionRunner;
  private final List<ProjectionRunner> projectionRunnersByRegistration;

  // Threads spawned by startProjections(), one per registration — populated there,
  // interrupted and joined by stopProjections(). A plain ArrayList is not safe for the
  // add-during-start / iterate-during-stop split across calls; CopyOnWriteArrayList is cheap here
  // (writes are rare — one burst per startProjections() call — and stop's iteration must not race
  // a concurrent start on another thread).
  private final List<Thread> projectionThreads = new CopyOnWriteArrayList<>();

  // The EventStoreFactory passed to Builder#eventStoreFactory(EventStoreFactory), when it
  // is itself AutoCloseable (a factory that owns resources, such as a pool it built) — retained
  // ONLY for that overload; see ConfiguredEventStoreFactory's javadoc for why a factory constructed
  // inside a settings-lambda cannot be retained here. Null when not applicable.
  private final AutoCloseable eventStoreFactory;

  private StreamRune(
      VirtualThreadCommandBus commandBus,
      List<ProjectionRegistration> projections,
      ProjectionRunner projectionRunner,
      List<ProjectionRunner> projectionRunnersByRegistration,
      AutoCloseable eventStoreFactory) {
    this.commandBus = commandBus;
    this.projections = List.copyOf(projections);
    this.projectionRunner = projectionRunner;
    this.projectionRunnersByRegistration =
        projectionRunnersByRegistration == null
            ? null
            : List.copyOf(projectionRunnersByRegistration);
    this.eventStoreFactory = eventStoreFactory;
  }

  /** Creates a new builder for configuring StreamRune. */
  public static Builder builder() {
    return new Builder();
  }

  @Override
  public <C extends Command> CommandResult execute(C command) {
    return commandBus.execute(command);
  }

  @Override
  public <C extends Command> CommandResult execute(C command, IdempotencyKey idempotencyKey) {
    return commandBus.execute(command, idempotencyKey);
  }

  @Override
  public boolean supportsIdempotentExecution() {
    return commandBus.supportsIdempotentExecution();
  }

  /**
   * Returns the underlying command bus for direct access if needed.
   *
   * <p>Most users should use {@link #execute(Command)} directly on this facade. This accessor is
   * useful for advanced use cases like registering interceptors after the facade is built, or
   * accessing bus-specific operations.
   *
   * @return the underlying command bus
   */
  public CommandBus commandBus() {
    return commandBus;
  }

  /**
   * Starts every registered projection, each on its own virtual thread, and returns without waiting
   * for any of them to complete.
   *
   * <p>A live/continuous {@link ProjectionRunner} (e.g. {@link ContinuousProjectionRunner}) never
   * returns from {@link ProjectionRunner#run} while running — calling it in a synchronous loop, as
   * this method used to, meant the SECOND registration's {@code run} call was never even issued
   * until the first one stopped, which for a live runner is never. Mirrors {@link
   * MultiProjectionRunner#start()}'s per-registration virtual thread.
   *
   * <p>Every registered projection's {@code run} call is issued exactly once per call to this
   * method, regardless of whether earlier ones block.
   *
   * <p><b>Per-registration runners.</b> When built via {@link Builder#projectionRunnerFactory},
   * each registration already has its OWN runner instance (one {@code create(...)} call per {@link
   * Builder#registerProjection} — see {@link Builder#projectionRunnerFactory}), so a
   * one-projection-per-instance runner such as {@link ContinuousProjectionRunner} works correctly
   * with any number of registrations. When built via the single-instance {@link
   * Builder#projectionRunner}, every registration still shares that ONE caller-supplied instance —
   * whether it can itself correctly service more than one concurrent {@code run} call for different
   * projection names is then that implementation's responsibility, not this method's; a plain
   * {@link ContinuousProjectionRunner} passed this way still fails every registration past the
   * first fast with an {@link IllegalStateException}, logged here rather than thrown.
   *
   * <p><b>No health surface.</b> The facade exposes no liveness accessor for these threads. A
   * {@code run} call that throws (any {@link Throwable}) is logged at ERROR ({@code "Projection
   * '<name>' failed"}) and its thread ends; nothing restarts it and nothing else reports it, and a
   * {@code run} call that returns ends its thread without a log line. To alert on a stopped
   * projection, observe the runner itself — the {@link ProjectionRunnerFactory} is application
   * code, so it can keep each instance it builds (e.g. {@link ContinuousProjectionRunner#state()}
   * and {@link ContinuousProjectionRunner#lastError()}) or wire a health contributor into it.
   *
   * @see #stopProjections()
   */
  public void startProjections() {
    if (projectionRunner != null) {
      for (var reg : projections) {
        spawnProjectionThread(reg, projectionRunner);
      }
    } else if (projectionRunnersByRegistration != null) {
      for (int i = 0; i < projections.size(); i++) {
        spawnProjectionThread(projections.get(i), projectionRunnersByRegistration.get(i));
      }
    }
  }

  private void spawnProjectionThread(ProjectionRegistration reg, ProjectionRunner runner) {
    // stopProjections() closes every runner, idle ones included, and a ContinuousProjectionRunner
    // holds a stop that finds it idle for its next run(). That stop was meant for the run that had
    // already ended (or never started), so drop it before starting this one: a stop issued after
    // this point is still held for the new run.
    if (runner instanceof ContinuousProjectionRunner continuous) {
      continuous.discardHeldStop();
    }
    Thread thread =
        Thread.ofVirtual()
            .name("streamrune-projection-" + reg.name().value())
            .start(
                () -> {
                  try {
                    runner.run(reg.name(), reg.projection(), reg.deliveryMode());
                  } catch (Throwable e) {
                    // Mirrors MultiProjectionRunner#start(): catch Throwable, not just Exception,
                    // so an Error unwinding the virtual thread is still logged rather than
                    // silently killing it with no trace.
                    logger.error("Projection '{}' failed", reg.name().value(), e);
                  }
                });
    projectionThreads.add(thread);
  }

  /**
   * Stops all projections: closes every managed {@link ProjectionRunner} that is {@link
   * AutoCloseable} (the shared instance, or every distinct per-registration instance when built via
   * {@link Builder#projectionRunnerFactory}), then interrupts and joins every thread {@link
   * #startProjections()} spawned — up to {@link MultiProjectionRunner#DEFAULT_STOP_TIMEOUT} per
   * thread — so this method does not return while a projection thread it started is still alive on
   * a cooperative runner. A thread that does not exit within the timeout (e.g. a stuck projection)
   * is logged and left running; this method still returns. Closing is exception-isolated (a failure
   * closing one runner does not skip the others, and threads are still joined afterward even if a
   * close failed) — the first failure is thrown, every later one attached to it as a suppressed
   * exception.
   */
  public void stopProjections() {
    RuntimeException failure = closeProjectionRunners();
    joinProjectionThreads();
    if (failure != null) {
      throw failure;
    }
  }

  private RuntimeException closeProjectionRunners() {
    if (projectionRunner instanceof AutoCloseable closeable) {
      try {
        closeable.close();
      } catch (Exception e) {
        return new RuntimeException("Failed to stop projections", e);
      }
      return null;
    }
    if (projectionRunnersByRegistration == null) {
      return null;
    }
    RuntimeException failure = null;
    for (ProjectionRunner runner : projectionRunnersByRegistration) {
      if (runner instanceof AutoCloseable closeable) {
        try {
          closeable.close();
        } catch (Exception e) {
          if (failure == null) {
            failure = new RuntimeException("Failed to stop projections", e);
          } else {
            failure.addSuppressed(e);
          }
        }
      }
    }
    return failure;
  }

  private void joinProjectionThreads() {
    long timeoutMillis = MultiProjectionRunner.DEFAULT_STOP_TIMEOUT.toMillis();
    for (Thread thread : projectionThreads) {
      // The runner's own close() (above) already signals a cooperative AutoCloseable runner, but
      // this facade owns these threads regardless of what the runner supports, so interrupt
      // unconditionally too — wakes a plain Thread.sleep/wait-based runner that isn't
      // AutoCloseable, and is a harmless no-op on a thread that has already exited.
      thread.interrupt();
      try {
        thread.join(timeoutMillis);
      } catch (InterruptedException _) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted while waiting for a projection thread to stop");
      }
      if (thread.isAlive()) {
        logger.error(
            "A projection thread did not stop within {} ms — it may still be processing",
            timeoutMillis);
      }
    }
    projectionThreads.clear();
  }

  /**
   * Stops projections, closes the command bus, and — when the facade retains one (see {@link
   * ConfiguredEventStoreFactory}'s javadoc for when it does not) — closes the {@link AutoCloseable}
   * event store factory passed to {@link Builder#eventStoreFactory(EventStoreFactory)} or produced
   * by {@link Builder#eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider)}.
   *
   * <p>The steps are exception-isolated: a failure in one must not skip the others or leave a later
   * resource open behind a "closed" facade. The event store factory is closed LAST, after the
   * command bus has drained its in-flight commands — closing its connection pool first could pull
   * it out from under a command still in flight. The first failure is thrown; every later failure
   * is attached to it as a suppressed exception.
   *
   * <p><b>What this does NOT close.</b> An {@link EventStore} passed directly via {@link
   * Builder#eventStore(EventStore)} — the interface carries no close/lifecycle contract — and a
   * factory constructed INSIDE a {@link Builder#eventStoreFactory(ConfiguredEventStoreFactory)}
   * lambda, which is invisible to the facade (the lambda returns only the constructed {@link
   * EventStore}). See {@link ConfiguredEventStoreFactory}'s javadoc for the pattern that does get
   * automatic cleanup.
   */
  @Override
  public void close() {
    RuntimeException failure = null;
    try {
      stopProjections();
    } catch (RuntimeException e) {
      failure = e;
    }
    try {
      commandBus.close();
    } catch (RuntimeException e) {
      if (failure == null) {
        failure = e;
      } else {
        failure.addSuppressed(e);
      }
    }
    if (eventStoreFactory != null) {
      try {
        eventStoreFactory.close();
      } catch (Exception e) {
        RuntimeException wrapped =
            e instanceof RuntimeException re
                ? re
                : new RuntimeException("Failed to close event store factory", e);
        if (failure == null) {
          failure = wrapped;
        } else {
          failure.addSuppressed(wrapped);
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }

  /**
   * Event-store settings collected by {@link Builder}: the event/state type registry populated via
   * {@link Builder#registerEventType(String, Class)} / {@link Builder#registerStateType(String,
   * Class)}, the crypto engine from {@link Builder#cryptoEngine(CryptoEngine)} and the upcasters
   * from {@link Builder#upcasters(List)}. Handed to a {@link ConfiguredEventStoreFactory} so the
   * factory can apply them when constructing the store.
   *
   * @param typeRegistry the registry of event and state types (never null, possibly empty)
   * @param cryptoEngine the configured crypto engine, or {@code null} if none was configured
   * @param upcasters the configured upcasters (never null, possibly empty)
   */
  public record EventStoreSettings(
      EventTypeRegistry typeRegistry, CryptoEngine cryptoEngine, List<EventUpcaster> upcasters) {

    public EventStoreSettings {
      if (typeRegistry == null) {
        throw new IllegalArgumentException("typeRegistry is required");
      }
      upcasters = upcasters == null ? List.of() : List.copyOf(upcasters);
    }
  }

  /**
   * Factory that constructs the event store from the builder's collected {@link
   * EventStoreSettings}. This is the seam through which {@link Builder#cryptoEngine(CryptoEngine)},
   * {@link Builder#upcasters(List)} and the type registrations reach the store.
   *
   * <p><b>Resource ownership.</b> This interface returns only the constructed {@link EventStore},
   * so a factory object created INSIDE the lambda below is invisible to the facade: it is NOT
   * retained, and {@link StreamRune#close()} does NOT close it. If the factory owns resources (a
   * connection pool it built, for instance), closing it is the caller's responsibility. {@code
   * PostgresEventStoreFactory} owns none: its store borrows from the application's {@code
   * DataSource}, which the application closes.
   *
   * <p>When the type registry, crypto engine or upcasters must be accumulated through {@link
   * Builder#registerEventType(String, Class)} / {@link Builder#registerStateType(String, Class)} /
   * {@link Builder#cryptoEngine(CryptoEngine)} / {@link Builder#upcasters(List)} spread across the
   * fluent chain AND the constructed factory owns resources that need closing, use {@link
   * Builder#eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider)} instead — its lambda
   * returns the {@link EventStoreFactory} itself (not yet {@code .create()}d), which the facade
   * DOES retain and close automatically, exactly like the plain {@link
   * Builder#eventStoreFactory(EventStoreFactory)} form below:
   *
   * <pre>{@code
   * StreamRune.builder()
   *     .eventStoreFactoryProvider(settings ->
   *         new PostgresEventStoreFactory(dataSource, settings.typeRegistry())
   *             .cryptoEngine(settings.cryptoEngine())
   *             .upcasters(settings.upcasters()))       // no .create() — the FACTORY is returned
   *     .cryptoEngine(engine)
   *     .upcasters(List.of(new OrderCreatedV1ToV2Upcaster()))
   *     .registerEventType("OrderCreated", OrderCreated.class)
   *     .build();                     // app.close() closes the factory if it is AutoCloseable
   * }</pre>
   *
   * <p>This interface ({@code ConfiguredEventStoreFactory}, returning the constructed {@link
   * EventStore} directly) remains available for when the store genuinely has nothing to close (an
   * in-memory or otherwise non-{@link AutoCloseable} factory), or the caller wants to build and
   * close the factory entirely outside the facade for some other reason. When settings are not
   * needed at all, build and configure the factory OUTSIDE the builder chain — its own fluent
   * setters cover the crypto engine, upcasters and type registry directly — and pass it to {@link
   * Builder#eventStoreFactory(EventStoreFactory)}:
   *
   * <pre>{@code
   * var factory =
   *     new PostgresEventStoreFactory(dataSource, typeRegistry)
   *         .cryptoEngine(engine)
   *         .upcasters(List.of(new OrderCreatedV1ToV2Upcaster()));
   * StreamRune app =
   *     StreamRune.builder()
   *         .eventStoreFactory(factory) // retained; closed by app.close() if AutoCloseable
   *         .register(...)
   *         .build();
   * }</pre>
   */
  @FunctionalInterface
  public interface ConfiguredEventStoreFactory {

    /**
     * Creates the event store, applying the given settings.
     *
     * @param settings the settings collected by the builder
     * @return the event store (must not be null)
     */
    EventStore create(EventStoreSettings settings);
  }

  /**
   * Provides an {@link EventStoreFactory} built from the builder's collected {@link
   * EventStoreSettings} — the settings-threaded counterpart of {@link
   * Builder#eventStoreFactory(EventStoreFactory)} that DOES get automatic cleanup, unlike {@link
   * ConfiguredEventStoreFactory} (see its javadoc). The lambda returns the {@link
   * EventStoreFactory} itself rather than an already-constructed {@link EventStore} — the facade
   * calls {@link EventStoreFactory#create()} on it, retains the factory, and — when it is {@link
   * AutoCloseable} (a factory that owns resources, such as a pool it built) — closes it from {@link
   * StreamRune#close()}, in the same retained field and at the same point in the close sequence as
   * the plain {@link Builder#eventStoreFactory(EventStoreFactory)} form.
   */
  @FunctionalInterface
  public interface ConfiguredEventStoreFactoryProvider {

    /**
     * Creates the event store factory, applying the given settings. The facade calls {@link
     * EventStoreFactory#create()} on the result itself — do not call it here.
     *
     * @param settings the settings collected by the builder
     * @return the event store factory (must not be null)
     */
    EventStoreFactory create(EventStoreSettings settings);
  }

  /**
   * Factory that constructs the projection runner from the built event store and the {@link
   * Builder#subscriptionConfig(SubscriptionConfig)}. This is the seam through which the
   * subscription config reaches the subscription the runner creates, and the only way to reference
   * the event store when it is itself created by a factory inside {@link Builder#build()}.
   */
  @FunctionalInterface
  public interface ProjectionRunnerFactory {

    /**
     * Creates the projection runner.
     *
     * @param eventStore the event store the facade was built with
     * @param subscriptionConfig the configured subscription config (defaults to {@link
     *     SubscriptionConfig#DEFAULT})
     * @return the projection runner (must not be null)
     */
    ProjectionRunner create(EventStore eventStore, SubscriptionConfig subscriptionConfig);
  }

  /** Fluent builder for the StreamRune facade. */
  public static final class Builder {

    private final VirtualThreadCommandBus.Builder busBuilder = VirtualThreadCommandBus.builder();
    private final SimpleEventTypeRegistry.Builder typeRegistryBuilder =
        SimpleEventTypeRegistry.builder();
    private final List<ProjectionRegistration> projections = new ArrayList<>();
    private ProjectionRunner projectionRunner;
    private ProjectionRunnerFactory projectionRunnerFactory;
    private SubscriptionConfig subscriptionConfig = SubscriptionConfig.DEFAULT;
    private boolean subscriptionConfigSet;
    private CryptoEngine cryptoEngine;
    private List<EventUpcaster> upcasters;
    private boolean hasTypeRegistrations;
    private EventStoreFactory eventStoreFactory;
    private ConfiguredEventStoreFactory configuredEventStoreFactory;
    private ConfiguredEventStoreFactoryProvider configuredEventStoreFactoryProvider;

    Builder() {}

    // ==================== Event Store Configuration ====================

    /** Sets the event store for persistence. */
    public Builder eventStore(EventStore eventStore) {
      busBuilder.eventStore(eventStore);
      return this;
    }

    /** Alias for eventStore() for fluent API consistency. */
    public Builder withEventStore(EventStore eventStore) {
      return eventStore(eventStore);
    }

    /**
     * Sets a custom event store factory. The factory receives no settings from this builder; if you
     * configure {@link #cryptoEngine(CryptoEngine)}, {@link #upcasters(List)} or register
     * event/state types, use {@link #eventStoreFactory(ConfiguredEventStoreFactory)} instead so
     * those settings reach the store.
     */
    public Builder eventStoreFactory(EventStoreFactory factory) {
      this.eventStoreFactory = factory;
      return this;
    }

    /**
     * Sets an event store factory that receives the builder's {@link EventStoreSettings} (type
     * registry, crypto engine, upcasters) so it can apply them when constructing the store. The
     * constructed {@link EventStore} is not retained by the facade — see {@link
     * ConfiguredEventStoreFactory}'s javadoc for why, and for {@link
     * #eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider)} when automatic cleanup is
     * needed too.
     */
    public Builder eventStoreFactory(ConfiguredEventStoreFactory factory) {
      this.configuredEventStoreFactory = factory;
      return this;
    }

    /**
     * Sets a PROVIDER of the event store factory, receiving the builder's {@link
     * EventStoreSettings} (type registry, crypto engine, upcasters) so it can apply them when
     * building the factory. Unlike {@link #eventStoreFactory(ConfiguredEventStoreFactory)}, whose
     * lambda returns the constructed {@link EventStore} directly, this lambda returns the {@link
     * EventStoreFactory} itself — the facade calls {@link EventStoreFactory#create()} on it,
     * retains the factory, and closes it from {@link StreamRune#close()} when it is {@link
     * AutoCloseable}, exactly like {@link #eventStoreFactory(EventStoreFactory)}. This is the
     * primary way to combine builder-accumulated settings with a factory, and the one that cleans
     * up a factory owning resources (a pool it built, for instance). See {@link
     * ConfiguredEventStoreFactoryProvider}'s javadoc for an example.
     */
    public Builder eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider provider) {
      this.configuredEventStoreFactoryProvider = provider;
      return this;
    }

    /**
     * Registers an event type for serialization. Honored only when the event store is created via
     * {@link #eventStoreFactory(ConfiguredEventStoreFactory)} or {@link
     * #eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider)} — {@link #build()} throws
     * otherwise, because the facade cannot inject the registry into a pre-built store.
     */
    public Builder registerEventType(String name, Class<?> eventClass) {
      typeRegistryBuilder.registerEvent(name, eventClass);
      hasTypeRegistrations = true;
      return this;
    }

    /**
     * Registers a state type for snapshots. Honored only when the event store is created via {@link
     * #eventStoreFactory(ConfiguredEventStoreFactory)} or {@link
     * #eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider)} — {@link #build()} throws
     * otherwise, because the facade cannot inject the registry into a pre-built store.
     */
    public Builder registerStateType(String name, Class<?> stateClass) {
      typeRegistryBuilder.registerState(name, stateClass);
      hasTypeRegistrations = true;
      return this;
    }

    // ==================== Encryption & Upcasting ====================

    /**
     * Sets the crypto engine for encryption. Honored only when the event store is created via
     * {@link #eventStoreFactory(ConfiguredEventStoreFactory)} or {@link
     * #eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider)} — {@link #build()} throws
     * otherwise, because the facade cannot retrofit encryption onto a pre-built store.
     */
    public Builder cryptoEngine(CryptoEngine cryptoEngine) {
      this.cryptoEngine = cryptoEngine;
      return this;
    }

    /**
     * Sets event upcasters for schema evolution. Honored only when the event store is created via
     * {@link #eventStoreFactory(ConfiguredEventStoreFactory)} or {@link
     * #eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider)} — {@link #build()} throws
     * otherwise, because the facade cannot retrofit upcasting onto a pre-built store.
     */
    public Builder upcasters(List<EventUpcaster> upcasters) {
      this.upcasters = upcasters;
      return this;
    }

    // ==================== Command Processing ====================

    /** Sets the aggregate locker for concurrency control. */
    public Builder locker(AggregateLocker locker) {
      busBuilder.locker(locker);
      return this;
    }

    /** Sets the retry policy for optimistic lock conflicts. */
    public Builder retryPolicy(RetryPolicy retryPolicy) {
      busBuilder.retryPolicy(retryPolicy);
      return this;
    }

    /** Sets the snapshot policy. */
    public Builder snapshotPolicy(SnapshotPolicy snapshotPolicy) {
      busBuilder.snapshotPolicy(snapshotPolicy);
      return this;
    }

    /** Sets the lock timeout for aggregate locking. Defaults to 5 seconds. */
    public Builder lockTimeout(Duration lockTimeout) {
      busBuilder.lockTimeout(lockTimeout);
      return this;
    }

    /** Sets the stripe count for the default striped locker. Defaults to 1024. */
    public Builder stripeCount(int stripeCount) {
      busBuilder.stripeCount(stripeCount);
      return this;
    }

    // ==================== Decider Registration ====================

    /**
     * Registers a decider for a command type under an aggregate type: the type names the streams
     * the decider's commands write to ({@code <type>:<id>}).
     *
     * @throws IllegalArgumentException at this call, before anything is stored or created, if
     *     {@code aggregateType} is outside {@link AggregateType#SYNTAX}, {@code commandType} is
     *     already registered, it overlaps a registered command type under a different aggregate
     *     type, {@code decider} is already registered under a different aggregate type, or any
     *     argument is null
     */
    public <C extends Command> Builder register(
        AggregateType aggregateType,
        Class<C> commandType,
        Function<C, AggregateId> idExtractor,
        Decider<C, ?, ?> decider) {
      busBuilder.register(aggregateType, commandType, idExtractor, decider);
      return this;
    }

    /** Alias for register() for clearer intent. */
    public <C extends Command> Builder registerDecider(
        AggregateType aggregateType,
        Class<C> commandType,
        Function<C, AggregateId> idExtractor,
        Decider<C, ?, ?> decider) {
      return register(aggregateType, commandType, idExtractor, decider);
    }

    // ==================== Projection Configuration ====================

    /**
     * Registers a projection to run via {@link #startProjections()}, with the delivery guarantee it
     * declares. The runner the factory builds validates the mode against its processor at {@code
     * run()}.
     */
    public Builder registerProjection(
        String name, Projection projection, ProjectionDeliveryMode mode) {
      if (mode == null) {
        throw new IllegalArgumentException(
            "deliveryMode is required for projection '" + LogSanitizer.sanitizeForLog(name) + "'");
      }
      projections.add(new ProjectionRegistration(ProjectionName.of(name), projection, mode));
      return this;
    }

    /** Sets the projection runner to use. Mutually exclusive with projectionRunnerFactory(). */
    public Builder projectionRunner(ProjectionRunner runner) {
      this.projectionRunner = runner;
      return this;
    }

    /**
     * Sets a factory that creates the projection runner from the built event store and the
     * configured {@link #subscriptionConfig(SubscriptionConfig)}. Mutually exclusive with {@link
     * #projectionRunner(ProjectionRunner)}. Example:
     *
     * <pre>{@code
     * .projectionRunnerFactory((store, config) ->
     *     ContinuousProjectionRunner.builder()
     *         .eventStore(store)
     *         .offsetStore(offsetStore)
     *         .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
     *         .subscriptionConfig(config)
     *         .build())
     * }</pre>
     *
     * <p><b>One instance PER registration.</b> The factory is invoked once for EACH {@link
     * #registerProjection}-ed projection, and {@link #startProjections()} pairs each registration
     * with its own runner instance, each on its own thread — so a plain {@link
     * ContinuousProjectionRunner}, as in the example above (which owns exactly one projection's
     * lifecycle per instance), works correctly with any number of registrations: each gets a fresh
     * instance rather than being asked to multiplex a shared one. {@link #stopProjections()} closes
     * every {@link AutoCloseable} instance the factory produced. Supply a {@link ProjectionRunner}
     * capable of multiplexing distinct projection names itself only if you want to share ONE
     * instance across registrations yourself — via {@link #projectionRunner(ProjectionRunner)}, not
     * this method.
     */
    public Builder projectionRunnerFactory(ProjectionRunnerFactory factory) {
      this.projectionRunnerFactory = factory;
      return this;
    }

    /**
     * Sets the subscription config for live projections. Honored only when the projection runner is
     * created via {@link #projectionRunnerFactory(ProjectionRunnerFactory)} — {@link #build()}
     * throws otherwise, because the facade cannot inject the config into a pre-built runner.
     */
    public Builder subscriptionConfig(SubscriptionConfig config) {
      this.subscriptionConfig = config;
      this.subscriptionConfigSet = true;
      return this;
    }

    /**
     * Sets the Jackson ObjectMapper that serializes a command when the bus dead-letters it.
     * Required when deadLetterQueue is configured. A command type that can be dead-lettered must
     * round-trip through it, because the dead-letter retry runner rebuilds the command from that
     * JSON on replay (a {@code record} does by default; see {@link Command}).
     */
    public Builder objectMapper(ObjectMapper objectMapper) {
      busBuilder.objectMapper(objectMapper);
      return this;
    }

    /**
     * Sets the dead letter queue for failed commands. When a command exhausts all retry attempts,
     * it is published here before the exception propagates.
     */
    public Builder deadLetterQueue(DeadLetterQueue deadLetterQueue) {
      busBuilder.deadLetterQueue(deadLetterQueue);
      return this;
    }

    /**
     * Sets the metrics collector. If not set, metrics are not recorded. Use {@code
     * org.streamrune.integration.MicrometerStreamRuneMetrics} for Micrometer-backed metrics.
     */
    public Builder metrics(StreamRuneMetrics metrics) {
      busBuilder.metrics(metrics);
      return this;
    }

    /**
     * Adds command interceptors for cross-cutting concerns (audit logging, rate limiting, etc.).
     * Repeated calls accumulate: interceptors from every call (either overload) run in the order
     * they were added — a later call never silently replaces earlier ones.
     */
    public Builder interceptors(List<CommandInterceptor> interceptors) {
      busBuilder.interceptors(interceptors);
      return this;
    }

    /**
     * Adds command interceptors. Convenience varargs overload; repeated calls accumulate — see
     * {@link #interceptors(List)}.
     */
    public Builder interceptors(CommandInterceptor... interceptors) {
      busBuilder.interceptors(List.of(interceptors));
      return this;
    }

    /**
     * Sets a wrapper applied to tasks spawned by {@code executeAsync} on the underlying command
     * bus. Invoked on the submitting thread, so it can capture thread-bound caller context (e.g.
     * the current OpenTelemetry context) that does not cross {@code Thread.start()}. See {@link
     * VirtualThreadCommandBus.Builder#asyncTaskWrapper(UnaryOperator)}.
     */
    public Builder asyncTaskWrapper(UnaryOperator<Runnable> asyncTaskWrapper) {
      busBuilder.asyncTaskWrapper(asyncTaskWrapper);
      return this;
    }

    /**
     * Builds the StreamRune instance.
     *
     * @throws IllegalStateException if no event store source is configured, if more than one is
     *     configured, if a configured setting (cryptoEngine, upcasters, registerEventType /
     *     registerStateType, subscriptionConfig) cannot be honored, or if registerProjection(...)
     *     was called but no projectionRunner(...) / projectionRunnerFactory(...) is configured to
     *     run it — the builder fails loudly instead of silently discarding it; or if a registered
     *     command type declares {@code @RequireRole}/{@code @RequirePermission} and no {@link
     *     AnnotationAuthorizationInterceptor} is among the interceptors, which is refused before
     *     any event store is created
     */
    public StreamRune build() {
      busBuilder.requireAuthorizationEnforcement();
      ResolvedEventStore resolvedEventStore = resolveEventStore();
      EventStore eventStore = resolvedEventStore.store();
      busBuilder.eventStore(eventStore);
      ResolvedProjectionRunners runners = resolveProjectionRunners(eventStore);
      return new StreamRune(
          busBuilder.build(),
          List.copyOf(projections),
          runners.shared(),
          runners.perRegistration(),
          resolvedEventStore.retainedFactory());
    }

    /**
     * {@code retainedFactory} is the {@link AutoCloseable} {@link EventStoreFactory} {@link
     * StreamRune#close()} must close — from the plain {@link #eventStoreFactory(EventStoreFactory)}
     * form, or the one {@link #eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider)}
     * produced. {@code null} for a pre-built {@link #eventStore(EventStore)} or the settings-lambda
     * {@link #eventStoreFactory(ConfiguredEventStoreFactory)} form, which the facade never sees a
     * factory object for at all (see that interface's javadoc).
     */
    private record ResolvedEventStore(EventStore store, AutoCloseable retainedFactory) {}

    private ResolvedEventStore resolveEventStore() {
      int sources = 0;
      if (busBuilder.eventStore() != null) {
        sources++;
      }
      if (eventStoreFactory != null) {
        sources++;
      }
      if (configuredEventStoreFactory != null) {
        sources++;
      }
      if (configuredEventStoreFactoryProvider != null) {
        sources++;
      }
      if (sources == 0) {
        throw new IllegalStateException(
            "Either eventStoreFactory or eventStore must be configured. "
                + "Use eventStoreFactory() with a PostgresEventStoreFactory or InMemoryEventStoreFactory.");
      }
      if (sources > 1) {
        throw new IllegalStateException(
            "Exactly one event store source must be configured, but multiple were set. "
                + "Configure only one of eventStore(...), eventStoreFactory(EventStoreFactory), "
                + "eventStoreFactory(ConfiguredEventStoreFactory) or "
                + "eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider).");
      }

      if (configuredEventStoreFactoryProvider != null) {
        // Unlike ConfiguredEventStoreFactory below, this form's lambda returns the FACTORY,
        // not the store — call create() on it ourselves, and retain the factory (closed exactly
        // like the plain eventStoreFactory(EventStoreFactory) form) so builder-accumulated
        // settings and automatic cleanup are no longer mutually exclusive.
        var settings = new EventStoreSettings(typeRegistryBuilder.build(), cryptoEngine, upcasters);
        EventStoreFactory factory = configuredEventStoreFactoryProvider.create(settings);
        if (factory == null) {
          throw new IllegalStateException("ConfiguredEventStoreFactoryProvider returned null");
        }
        EventStore store = factory.create();
        if (store == null) {
          throw new IllegalStateException("EventStoreFactory returned null");
        }
        AutoCloseable retained = factory instanceof AutoCloseable closeable ? closeable : null;
        return new ResolvedEventStore(store, retained);
      }

      if (configuredEventStoreFactory != null) {
        var settings = new EventStoreSettings(typeRegistryBuilder.build(), cryptoEngine, upcasters);
        EventStore store = configuredEventStoreFactory.create(settings);
        if (store == null) {
          throw new IllegalStateException("ConfiguredEventStoreFactory returned null");
        }
        return new ResolvedEventStore(store, null);
      }

      // Pre-built store or no-settings factory: the facade cannot thread these settings into the
      // store, so refuse to discard them silently.
      requireNotConfigured(
          cryptoEngine != null,
          "cryptoEngine",
          "configure the engine on the store itself (e.g. PostgresEventStoreFactory.cryptoEngine(...))");
      requireNotConfigured(
          upcasters != null,
          "upcasters",
          "configure the upcasters on the store itself (e.g. PostgresEventStoreFactory.upcasters(...))");
      requireNotConfigured(
          hasTypeRegistrations,
          "registerEventType/registerStateType",
          "pass the registry to the store itself (e.g. new PostgresEventStoreFactory(dataSource, typeRegistry))");

      if (eventStoreFactory != null) {
        // Retain the plain EventStoreFactory (not the settings-threaded
        // ConfiguredEventStoreFactory — see its javadoc) for close() to release, when it is itself
        // AutoCloseable (a factory that owns resources, such as a pool it built).
        AutoCloseable retained =
            eventStoreFactory instanceof AutoCloseable closeable ? closeable : null;
        return new ResolvedEventStore(eventStoreFactory.create(), retained);
      }
      return new ResolvedEventStore(busBuilder.eventStore(), null);
    }

    /**
     * {@code shared} is set for the single-instance {@link #projectionRunner} form (used for every
     * registration — the caller's own responsibility to supply a multiplexing-capable runner for
     * more than one registration); {@code perRegistration} is set for the {@link
     * #projectionRunnerFactory} form (one distinct instance per registration, same order/size as
     * {@code projections}). Exactly one of the two is non-null, mirroring {@link
     * #resolveProjectionRunners}'s mutual exclusion.
     */
    private record ResolvedProjectionRunners(
        ProjectionRunner shared, List<ProjectionRunner> perRegistration) {}

    private ResolvedProjectionRunners resolveProjectionRunners(EventStore eventStore) {
      if (projectionRunner != null && projectionRunnerFactory != null) {
        throw new IllegalStateException(
            "Only one of projectionRunner(...) and projectionRunnerFactory(...) may be configured.");
      }
      if (projectionRunnerFactory != null) {
        // One factory call per registration, so a one-projection-per-instance runner (e.g.
        // ContinuousProjectionRunner) gets its OWN instance per registered projection instead of
        // being asked to multiplex — the previous behavior (factory invoked once, that single
        // instance reused for every registration) meant registration #2 (and any later one)
        // always hit that runner's own "already running" guard and never ran. At least ONE call
        // regardless of registration count, so build() still eagerly validates the factory
        // (null-check below) and threads eventStore/subscriptionConfig into it exactly as before
        // even with zero registrations — that lone result is simply never paired with a
        // registration to run, and is still closed like any other on stopProjections()/close().
        int runnerCount = Math.max(1, projections.size());
        List<ProjectionRunner> perRegistration = new ArrayList<>(runnerCount);
        for (int i = 0; i < runnerCount; i++) {
          ProjectionRunner runner = projectionRunnerFactory.create(eventStore, subscriptionConfig);
          if (runner == null) {
            throw new IllegalStateException("ProjectionRunnerFactory returned null");
          }
          perRegistration.add(runner);
        }
        return new ResolvedProjectionRunners(null, perRegistration);
      }
      if (subscriptionConfigSet) {
        throw new IllegalStateException(
            "subscriptionConfig was configured but cannot be honored: the facade only threads it "
                + "into projection runners it constructs. Use projectionRunnerFactory(...) — it "
                + "receives the config — or configure the runner passed to projectionRunner(...) "
                + "directly (e.g. ContinuousProjectionRunner.builder().subscriptionConfig(...)).");
      }
      if (projectionRunner == null && !projections.isEmpty()) {
        throw new IllegalStateException(
            "registerProjection(...) registered "
                + projections.size()
                + " projection(s), but no projection runner is configured: startProjections() "
                + "would silently do nothing and the registered projection(s) would never run, "
                + "leaving their read models permanently stale. Configure projectionRunner(...) "
                + "or projectionRunnerFactory(...).");
      }
      return new ResolvedProjectionRunners(projectionRunner, null);
    }

    private static void requireNotConfigured(boolean configured, String setting, String guidance) {
      if (configured) {
        throw new IllegalStateException(
            setting
                + " was configured but cannot be honored: the facade only threads it into event "
                + "stores it constructs. Use eventStoreFactory(ConfiguredEventStoreFactory) and "
                + "apply the setting from EventStoreSettings when constructing the store, or "
                + guidance
                + ".");
      }
    }
  }

  private record ProjectionRegistration(
      ProjectionName name, Projection projection, ProjectionDeliveryMode deliveryMode) {}
}
