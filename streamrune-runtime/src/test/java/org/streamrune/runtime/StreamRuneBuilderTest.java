package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;
import static org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRunner;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.upcasting.EventUpcaster;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryEventStoreFactory;
import org.streamrune.test.InMemoryOffsetStore;

/** Tests for enhanced StreamRune.Builder API */
class StreamRuneBuilderTest {

  private static final AggregateType TYPE = AggregateType.of("widget");

  @Test
  void shouldCreateWithEventStore() {
    var store = new InMemoryEventStore();

    var streamRune = StreamRune.builder().eventStore(store).build();

    assertInstanceOf(CommandBus.class, streamRune);
  }

  record Paint(String widgetId) implements Command {}

  record Painted() implements DomainEvent {}

  record WidgetState() implements AggregateState {}

  static final class WidgetDecider implements Decider<Paint, WidgetState, Painted> {
    @Override
    public WidgetState initialState() {
      return new WidgetState();
    }

    @Override
    public List<Painted> decide(Paint command, WidgetState state) {
      return List.of(new Painted());
    }

    @Override
    public WidgetState evolve(WidgetState state, Painted event) {
      return state;
    }
  }

  @Test
  void register_refusesADuplicateCommandType_atTheCall_beforeAnyEventStoreIsCreated() {
    var storeCreated = new AtomicReference<Boolean>(false);
    var builder =
        StreamRune.builder()
            .eventStoreFactory(
                settings -> {
                  storeCreated.set(true);
                  return new InMemoryEventStore();
                })
            .register(TYPE, Paint.class, c -> AggregateId.of(c.widgetId()), new WidgetDecider());

    var refused =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                builder.register(
                    TYPE, Paint.class, c -> AggregateId.of(c.widgetId()), new WidgetDecider()));
    assertTrue(refused.getMessage().contains(Paint.class.getName()), refused.getMessage());
    assertFalse(storeCreated.get(), "a refused registration creates no event store");
  }

  // ==================== Settings threading (configured event store factory) ====================

  @Test
  void shouldThreadEventTypeRegistrationsIntoConfiguredFactory() {
    var received = new AtomicReference<StreamRune.EventStoreSettings>();

    var streamRune =
        StreamRune.builder()
            .eventStoreFactory(
                settings -> {
                  received.set(settings);
                  return new InMemoryEventStore();
                })
            .registerEventType("TestEvent", TestEvent.class)
            .registerStateType("TestState", TestState.class)
            .build();

    assertInstanceOf(CommandBus.class, streamRune);
    assertNotNull(received.get(), "configured factory should have been invoked");
    assertEquals(
        TestEvent.class,
        received.get().typeRegistry().resolveEventType(new EventType("TestEvent")));
    assertEquals(TestState.class, received.get().typeRegistry().resolveStateType("TestState"));
  }

  @Test
  void shouldThreadCryptoEngineIntoConfiguredFactory() {
    var engine = new TestCryptoEngine();
    var received = new AtomicReference<StreamRune.EventStoreSettings>();

    var streamRune =
        StreamRune.builder()
            .eventStoreFactory(
                settings -> {
                  received.set(settings);
                  return new InMemoryEventStore();
                })
            .cryptoEngine(engine)
            .build();

    assertInstanceOf(CommandBus.class, streamRune);
    assertSame(engine, received.get().cryptoEngine());
  }

  @Test
  void shouldThreadUpcastersIntoConfiguredFactory() {
    var upcaster = new TestUpcaster();
    var received = new AtomicReference<StreamRune.EventStoreSettings>();

    var streamRune =
        StreamRune.builder()
            .eventStoreFactory(
                settings -> {
                  received.set(settings);
                  return new InMemoryEventStore();
                })
            .upcasters(List.of(upcaster))
            .build();

    assertInstanceOf(CommandBus.class, streamRune);
    assertEquals(List.of(upcaster), received.get().upcasters());
  }

  @Test
  void shouldPassNullCryptoAndEmptyUpcastersWhenUnconfigured() {
    var received = new AtomicReference<StreamRune.EventStoreSettings>();

    StreamRune.builder()
        .eventStoreFactory(
            settings -> {
              received.set(settings);
              return new InMemoryEventStore();
            })
        .build();

    assertNull(received.get().cryptoEngine());
    assertEquals(List.of(), received.get().upcasters());
  }

  @Test
  void shouldThrowWhenConfiguredFactoryReturnsNull() {
    var builder =
        StreamRune.builder().eventStoreFactory((StreamRune.ConfiguredEventStoreFactory) s -> null);

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("null"), ex.getMessage());
  }

  // ==================== Settings that cannot be honored must fail loudly ====================

  @Test
  void shouldThrowWhenCryptoEngineSetWithPrebuiltStore() {
    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .cryptoEngine(new TestCryptoEngine());

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("cryptoEngine"), ex.getMessage());
  }

  @Test
  void shouldThrowWhenCryptoEngineSetWithPlainEventStoreFactory() {
    var builder =
        StreamRune.builder()
            .eventStoreFactory(InMemoryEventStoreFactory.builder().build())
            .cryptoEngine(new TestCryptoEngine());

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("cryptoEngine"), ex.getMessage());
  }

  @Test
  void shouldThrowWhenUpcastersSetWithPrebuiltStore() {
    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .upcasters(List.of(new TestUpcaster()));

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("upcasters"), ex.getMessage());
  }

  @Test
  void shouldThrowWhenEventTypeRegisteredWithPrebuiltStore() {
    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .registerEventType("TestEvent", TestEvent.class);

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("registerEventType"), ex.getMessage());
  }

  @Test
  void shouldThrowWhenStateTypeRegisteredWithPrebuiltStore() {
    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .registerStateType("TestState", TestState.class);

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("registerStateType"), ex.getMessage());
  }

  @Test
  void shouldThrowWhenSubscriptionConfigSetWithoutProjectionRunnerFactory() {
    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .subscriptionConfig(SubscriptionConfig.pollingOnly(Duration.ofMillis(100)));

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("subscriptionConfig"), ex.getMessage());
  }

  // ==================== Subscription config threading (projection runner factory) ============

  @Test
  void shouldThreadSubscriptionConfigIntoProjectionRunnerFactory() {
    var store = new InMemoryEventStore();
    var config = new SubscriptionConfig(false, Duration.ofMillis(250), Duration.ofMillis(25));
    var receivedStore = new AtomicReference<EventStore>();
    var receivedConfig = new AtomicReference<SubscriptionConfig>();

    var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .subscriptionConfig(config)
            .projectionRunnerFactory(
                (eventStore, subscriptionConfig) -> {
                  receivedStore.set(eventStore);
                  receivedConfig.set(subscriptionConfig);
                  return new NoOpProjectionRunner();
                })
            .build();

    assertInstanceOf(CommandBus.class, streamRune);
    assertSame(store, receivedStore.get());
    assertSame(config, receivedConfig.get());
  }

  @Test
  void shouldPassDefaultSubscriptionConfigToProjectionRunnerFactoryWhenUnset() {
    var receivedConfig = new AtomicReference<SubscriptionConfig>();

    StreamRune.builder()
        .eventStore(new InMemoryEventStore())
        .projectionRunnerFactory(
            (eventStore, subscriptionConfig) -> {
              receivedConfig.set(subscriptionConfig);
              return new NoOpProjectionRunner();
            })
        .build();

    assertSame(SubscriptionConfig.DEFAULT, receivedConfig.get());
  }

  @Test
  void shouldRunProjectionsWithRunnerFromFactory() throws InterruptedException {
    // startProjections() now runs each registration on its own thread, so the assertion
    // below must wait for it rather than checking a flag set by a not-yet-scheduled thread.
    var started = new CountDownLatch(1);
    ProjectionRunner runner =
        new ProjectionRunner() {
          @Override
          public void run(ProjectionName name, Projection projection, ProjectionDeliveryMode mode) {
            started.countDown();
          }

          @Override
          public void reset(ProjectionName projectionName) {}
        };

    var streamRune =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .projectionRunnerFactory((eventStore, subscriptionConfig) -> runner)
            .registerProjection(
                "TestProjection", (Projection) batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    streamRune.startProjections();
    assertTrue(
        started.await(5, TimeUnit.SECONDS),
        "Factory-built projection runner should have been invoked");
    streamRune.close();
  }

  @Test
  void registerProjectionRefusesANullMode() {
    var builder = StreamRune.builder().eventStore(new InMemoryEventStore());
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> builder.registerProjection("orders", (Projection) batch -> {}, null));
    assertEquals("deliveryMode is required for projection 'orders'", ex.getMessage());
  }

  @Test
  void startProjectionsPassesEachRegistrationsModeToItsRunner() throws InterruptedException {
    var seenModes = new java.util.concurrent.ConcurrentHashMap<String, ProjectionDeliveryMode>();
    var seenProjections = new java.util.concurrent.ConcurrentHashMap<String, Projection>();
    var both = new CountDownLatch(2);
    Projection atLeastOnce = batch -> {};
    Projection transactional = batch -> {};
    ProjectionRunner recording =
        new ProjectionRunner() {
          @Override
          public void run(ProjectionName name, Projection projection, ProjectionDeliveryMode mode) {
            seenModes.put(name.value(), mode);
            seenProjections.put(name.value(), projection);
            both.countDown();
          }

          @Override
          public void reset(ProjectionName projectionName) {}
        };

    var streamRune =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .projectionRunnerFactory((eventStore, subscriptionConfig) -> recording)
            .registerProjection("at-least-once", atLeastOnce, AT_LEAST_ONCE_IDEMPOTENT)
            .registerProjection(
                "transactional", transactional, ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
            .build();
    try {
      streamRune.startProjections();
      assertTrue(both.await(5, TimeUnit.SECONDS), "both registrations must reach their runner");
      assertEquals(AT_LEAST_ONCE_IDEMPOTENT, seenModes.get("at-least-once"));
      assertEquals(ProjectionDeliveryMode.TRANSACTIONAL_LOCAL, seenModes.get("transactional"));
      assertSame(atLeastOnce, seenProjections.get("at-least-once"));
      assertSame(transactional, seenProjections.get("transactional"));
    } finally {
      streamRune.close();
    }
  }

  @Test
  void shouldThrowWhenProjectionRunnerFactoryReturnsNull() {
    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .projectionRunnerFactory((eventStore, subscriptionConfig) -> null);

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("null"), ex.getMessage());
  }

  @Test
  void shouldThrowWhenBothProjectionRunnerAndFactoryConfigured() {
    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .projectionRunner(new NoOpProjectionRunner())
            .projectionRunnerFactory(
                (eventStore, subscriptionConfig) -> new NoOpProjectionRunner());

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("projectionRunner"), ex.getMessage());
  }

  // ==================== Event store source validation ====================

  @Test
  void shouldThrowWhenMultipleEventStoreSourcesConfigured() {
    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .eventStoreFactory(InMemoryEventStoreFactory.builder().build());

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("one"), ex.getMessage());
  }

  @Test
  void shouldThrowWhenEventStoreAndConfiguredFactoryConfigured() {
    var builder =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .eventStoreFactory(settings -> new InMemoryEventStore());

    assertThrows(IllegalStateException.class, builder::build);
  }

  // ==================== Existing behavior ====================

  @Test
  void shouldSetSnapshotPolicy() {
    var store = new InMemoryEventStore();

    var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .snapshotPolicy(org.streamrune.core.SnapshotPolicy.never())
            .build();

    assertInstanceOf(CommandBus.class, streamRune);
  }

  @Test
  void shouldSetRetryPolicy() {
    var store = new InMemoryEventStore();

    var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .retryPolicy(
                new org.streamrune.core.RetryPolicy(3, java.time.Duration.ofMillis(100), 2.0))
            .build();

    assertInstanceOf(CommandBus.class, streamRune);
  }

  @Test
  void shouldSetLockTimeout() {
    var store = new InMemoryEventStore();

    var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .lockTimeout(java.time.Duration.ofSeconds(10))
            .build();

    assertInstanceOf(CommandBus.class, streamRune);
  }

  @Test
  void shouldSetStripeCount() {
    var store = new InMemoryEventStore();

    var streamRune = StreamRune.builder().eventStore(store).stripeCount(512).build();

    assertInstanceOf(CommandBus.class, streamRune);
  }

  @Test
  void shouldChainBuilderMethods() {
    var streamRune =
        StreamRune.builder()
            .eventStoreFactory(settings -> new InMemoryEventStore())
            .registerEventType("TestEvent", TestEvent.class)
            .registerStateType("TestState", TestState.class)
            .cryptoEngine(new TestCryptoEngine())
            .upcasters(List.of(new TestUpcaster()))
            .snapshotPolicy(org.streamrune.core.SnapshotPolicy.everyNEvents(100))
            .retryPolicy(
                new org.streamrune.core.RetryPolicy(3, java.time.Duration.ofMillis(100), 2.0))
            .lockTimeout(java.time.Duration.ofSeconds(10))
            .stripeCount(512)
            .build();

    assertInstanceOf(CommandBus.class, streamRune);
  }

  @Test
  void shouldThrowWhenNoEventStoreConfigured() {
    assertThrows(IllegalStateException.class, () -> StreamRune.builder().build());
  }

  @Test
  void shouldReturnCommandBusAccessor() {
    var store = new InMemoryEventStore();

    var streamRune = StreamRune.builder().eventStore(store).build();

    CommandBus bus = streamRune.commandBus();
    assertNotNull(bus);
    assertInstanceOf(org.streamrune.runtime.VirtualThreadCommandBus.class, bus);
  }

  @Test
  void shouldBuildWithEventStoreFactory() {
    // Covers the plain (no-settings) EventStoreFactory branch in build()
    var factory = InMemoryEventStoreFactory.builder().build();

    var streamRune = StreamRune.builder().eventStoreFactory(factory).build();

    assertInstanceOf(CommandBus.class, streamRune);
  }

  @Test
  void shouldStartAndStopProjections() throws InterruptedException {
    var store = new InMemoryEventStore();
    // startProjections() now runs each registration on its own thread, so the assertion
    // below must wait for it rather than checking a flag set by a not-yet-scheduled thread.
    var started = new CountDownLatch(1);

    ProjectionRunner runner =
        new ProjectionRunner() {
          @Override
          public void run(ProjectionName name, Projection projection, ProjectionDeliveryMode mode) {
            started.countDown();
          }

          @Override
          public void reset(ProjectionName projectionName) {}
        };

    var streamRune =
        StreamRune.builder()
            .eventStore(store)
            .projectionRunner(runner)
            .registerProjection(
                "TestProjection", (Projection) batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    streamRune.startProjections();
    assertTrue(started.await(5, TimeUnit.SECONDS), "Projection runner should have been invoked");

    streamRune.close(); // tests stopProjections() too
  }

  @Test
  void shouldThrowWhenProjectionRegisteredWithoutRunner() {
    // build() used to succeed and startProjections() would silently do nothing when a
    // projection was registered but no runner was configured — permanently-stale read models with
    // zero error signal. build() must now fail loudly instead.
    var store = new InMemoryEventStore();

    var builder =
        StreamRune.builder()
            .eventStore(store)
            .registerProjection("noop", (Projection) batch -> {}, AT_LEAST_ONCE_IDEMPOTENT);

    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("registerProjection"), ex.getMessage());
    assertTrue(ex.getMessage().contains("projectionRunner"), ex.getMessage());
  }

  // ==================== Projection thread lifecycle ====================

  @Test
  void startProjectionsShouldStartEveryRegistrationEvenWhenOneBlocksForever() throws Exception {
    var started1 = new CountDownLatch(1);
    var started2 = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    ProjectionRunner blockingRunner =
        new ProjectionRunner() {
          @Override
          public void run(ProjectionName name, Projection projection, ProjectionDeliveryMode mode) {
            if (name.value().equals("proj1")) {
              started1.countDown();
            } else {
              started2.countDown();
            }
            // Simulates a live/continuous runner (e.g. ContinuousProjectionRunner) that never
            // returns from run() while running. Bounded so a regression here fails the test
            // instead of hanging the build.
            try {
              release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
          }

          @Override
          public void reset(ProjectionName name) {}
        };

    var streamRune =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .projectionRunner(blockingRunner)
            .registerProjection("proj1", (Projection) batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .registerProjection("proj2", (Projection) batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    // Drive startProjections() from its own thread too: a regression to the old synchronous loop
    // would block THIS call forever on proj1, and this test must still fail cleanly rather than
    // hang the build.
    var caller = new Thread(streamRune::startProjections);
    caller.setDaemon(true);
    caller.start();

    try {
      assertTrue(started1.await(5, TimeUnit.SECONDS), "first registration should start");
      assertTrue(
          started2.await(5, TimeUnit.SECONDS),
          "second registration should ALSO start promptly, not be starved by the first blocking");
    } finally {
      release.countDown();
      caller.join(5_000);
      streamRune.close();
    }
  }

  // ==================== One runner per registration, factory form ====================

  record PerRegistrationTestEvent(String payload) implements org.streamrune.core.DomainEvent {}

  private static org.streamrune.core.EventMetadata registrationMetadata() {
    return new org.streamrune.core.EventMetadata(
        org.streamrune.core.IdGenerator.generateEventId(),
        org.streamrune.core.IdGenerator.generateCommandId(),
        null,
        null,
        org.streamrune.core.types.CorrelationId.of("corr-1"),
        null,
        null,
        java.time.Instant.now());
  }

  private static org.streamrune.core.EventEnvelope registrationEnvelope(
      org.streamrune.core.types.StreamId streamId, long version) {
    return new org.streamrune.core.EventEnvelope(
        org.streamrune.core.types.GlobalOffset.of(1),
        streamId,
        new org.streamrune.core.types.Version(version),
        new EventType("PerRegistrationTestEvent"),
        new PerRegistrationTestEvent("payload-" + version),
        registrationMetadata());
  }

  @Test
  void projectionRunnerFactory_createsOneRunnerPerRegistration_bothReceiveTheDispatch()
      throws Exception {
    // resolveProjectionRunner invoked the factory ONCE and
    // startProjections() called run() on that single shared instance for EVERY registration. A
    // counting factory here proves the fix directly: exactly one factory call per
    // registerProjection
    // call, each producing a distinct runner, and every distinct runner's run() is actually
    // invoked.
    var factoryCalls = new java.util.concurrent.atomic.AtomicInteger();
    var latch1 = new CountDownLatch(1);
    var latch2 = new CountDownLatch(1);

    var streamRune =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .projectionRunnerFactory(
                (eventStore, subscriptionConfig) -> {
                  factoryCalls.incrementAndGet();
                  return new ProjectionRunner() {
                    @Override
                    public void run(
                        ProjectionName name, Projection projection, ProjectionDeliveryMode mode) {
                      (name.value().equals("proj1") ? latch1 : latch2).countDown();
                    }

                    @Override
                    public void reset(ProjectionName name) {}
                  };
                })
            .registerProjection("proj1", (Projection) batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .registerProjection("proj2", (Projection) batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    try {
      streamRune.startProjections();
      assertTrue(latch1.await(5, TimeUnit.SECONDS), "proj1's own runner must have been invoked");
      assertTrue(latch2.await(5, TimeUnit.SECONDS), "proj2's own runner must have been invoked");
      assertEquals(
          2, factoryCalls.get(), "the factory must be invoked once per registerProjection call");
    } finally {
      streamRune.close();
    }
  }

  @Test
  void projectionRunnerFactory_withRealContinuousProjectionRunner_bothRegistrationsProcessEvents()
      throws Exception {
    // The only shipped continuous runner, ContinuousProjectionRunner, is one-projection-per-
    // instance (running.compareAndSet(false, true) else IllegalStateException("Runner already
    // running")) — exactly the builder javadoc's own projectionRunnerFactory example. Before the
    // fix, pairing it with two registrations meant registration #2 threw immediately inside
    // startProjections()'s per-thread catch block: an ERROR log, and read model #2 permanently
    // stale. Real runner (one per registration, post-fix), real InMemoryEventStore: both
    // registrations must actually process an appended event, and nothing may log at ERROR.
    var eventStore = new InMemoryEventStore();
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(StreamRune.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);

    var proj1Batches =
        new java.util.concurrent.CopyOnWriteArrayList<org.streamrune.core.EventEnvelope>();
    var proj2Batches =
        new java.util.concurrent.CopyOnWriteArrayList<org.streamrune.core.EventEnvelope>();
    var fastConfig = new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5));

    var streamRune =
        StreamRune.builder()
            .eventStore(eventStore)
            .projectionRunnerFactory(
                (store, subscriptionConfig) ->
                    ContinuousProjectionRunner.builder()
                        .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                        .eventStore(store)
                        .offsetStore(new InMemoryOffsetStore())
                        .fetchSize(100)
                        .batchSize(50)
                        .subscriptionConfig(subscriptionConfig)
                        .build())
            .subscriptionConfig(fastConfig)
            .registerProjection(
                "proj1", (Projection) proj1Batches::addAll, AT_LEAST_ONCE_IDEMPOTENT)
            .registerProjection(
                "proj2", (Projection) proj2Batches::addAll, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    try {
      streamRune.startProjections();

      var streamId = org.streamrune.core.types.StreamId.of(TYPE, AggregateId.of("w52-stream"));
      eventStore.append(
          streamId,
          List.of(registrationEnvelope(streamId, 1)),
          org.streamrune.core.types.Version.initial());

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while ((proj1Batches.isEmpty() || proj2Batches.isEmpty()) && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertFalse(proj1Batches.isEmpty(), "proj1's OWN runner must have processed the event");
      assertFalse(proj2Batches.isEmpty(), "proj2's OWN runner must have processed the event");
    } finally {
      streamRune.close();
      logger.detachAppender(appender);
      appender.stop();
    }

    assertTrue(
        appender.list.stream().noneMatch(e -> e.getLevel() == ch.qos.logback.classic.Level.ERROR),
        "neither registration may fail — a per-instance runner must never see"
            + " IllegalStateException(\"Runner already running\")");
  }

  @Test
  void startProjections_afterAStopThatFoundTheRunnerIdle_stillProcesses() throws Exception {
    // stopProjections() closes every runner, idle ones included, and a ContinuousProjectionRunner
    // holds a stop that finds it idle for its next run(). Without startProjections() dropping
    // that stop, a stop before the first start, or a stop + start to recover a run that had ended
    // on its own, left the restarted run() returning at once with nothing processed.
    var eventStore = new InMemoryEventStore();
    var batches =
        new java.util.concurrent.CopyOnWriteArrayList<org.streamrune.core.EventEnvelope>();
    var streamRune =
        StreamRune.builder()
            .eventStore(eventStore)
            .projectionRunnerFactory(
                (store, subscriptionConfig) ->
                    ContinuousProjectionRunner.builder()
                        .atomicProcessor(AtomicBatchProcessor.nonAtomicAtLeastOnce())
                        .eventStore(store)
                        .offsetStore(new InMemoryOffsetStore())
                        .subscriptionConfig(subscriptionConfig)
                        .build())
            .subscriptionConfig(
                new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5)))
            .registerProjection("held_stop", (Projection) batches::addAll, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    var streamId = org.streamrune.core.types.StreamId.of(TYPE, AggregateId.of("held-stop-stream"));
    eventStore.append(
        streamId,
        List.of(registrationEnvelope(streamId, 1)),
        org.streamrune.core.types.Version.initial());

    try {
      streamRune.stopProjections(); // finds the runner idle
      streamRune.startProjections();

      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (batches.isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertFalse(batches.isEmpty(), "the started run must process, not consume a stale stop");
    } finally {
      streamRune.close();
    }
  }

  @Test
  void stopProjectionsShouldInterruptAndJoinProjectionThreads() throws Exception {
    var started = new CountDownLatch(1);
    var interrupted = new CountDownLatch(1);
    ProjectionRunner runner =
        new ProjectionRunner() {
          @Override
          public void run(ProjectionName name, Projection projection, ProjectionDeliveryMode mode) {
            started.countDown();
            try {
              Thread.sleep(Duration.ofSeconds(10));
            } catch (InterruptedException _) {
              interrupted.countDown();
              Thread.currentThread().interrupt();
            }
          }

          @Override
          public void reset(ProjectionName name) {}
        };

    var streamRune =
        StreamRune.builder()
            .eventStore(new InMemoryEventStore())
            .projectionRunner(runner)
            .registerProjection("p", (Projection) batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();

    streamRune.startProjections();
    assertTrue(started.await(5, TimeUnit.SECONDS), "projection thread should start");

    streamRune.stopProjections(); // must return promptly, not after the 10s sleep completes

    assertTrue(
        interrupted.await(2, TimeUnit.SECONDS),
        "stopProjections() must interrupt the thread it spawned, not merely wait for it");
  }

  // ==================== Event store factory lifecycle ====================

  @Test
  void closeShouldCloseAnAutoCloseableEventStoreFactory() {
    var factory = new RecordingCloseableEventStoreFactory();

    var streamRune = StreamRune.builder().eventStoreFactory(factory).build();
    assertFalse(factory.closed, "factory must not be closed before close()");

    streamRune.close();

    assertTrue(factory.closed, "StreamRune.close() must close an AutoCloseable EventStoreFactory");
  }

  @Test
  void closeShouldCloseEventStoreFactoryAfterTheCommandBusDrains() {
    var order = new java.util.ArrayList<String>();
    var factory =
        new RecordingCloseableEventStoreFactory() {
          @Override
          public void close() {
            order.add("factory");
            super.close();
          }
        };
    // `order` used to receive ONLY "factory" — nothing else in the facade ever wrote to
    // it — so assertEquals(List.of("factory"), order) held for ANY relative ordering and pinned
    // nothing. The claim that actually matters is "the factory closes strictly after the
    // projection threads are joined" (stopProjections() runs before commandBus.close() and the
    // factory close in StreamRune#close()) — a projection reading through the event store must
    // never see its connection pool torn down out from under it mid-read. A stub AutoCloseable
    // ProjectionRunner recording "projections" on close() pins that end to end.
    var streamRune =
        StreamRune.builder()
            .eventStoreFactory(factory)
            .projectionRunner(new RecordingCloseableProjectionRunner(order))
            .registerProjection("p", (Projection) batch -> {}, AT_LEAST_ONCE_IDEMPOTENT)
            .build();
    streamRune.startProjections();

    streamRune.close();

    assertEquals(List.of("projections", "factory"), order);
    // The bus itself has no externally observable "closed" flag on this facade, but a second
    // close() must still be safe (idempotent underlying bus close) now the factory is also closed.
    assertDoesNotThrow(streamRune::close);
  }

  @Test
  void closeShouldNotFailWhenEventStoreFactoryIsNotAutoCloseable() {
    // InMemoryEventStoreFactory does not implement AutoCloseable — close() must simply skip it.
    var streamRune =
        StreamRune.builder().eventStoreFactory(InMemoryEventStoreFactory.builder().build()).build();

    assertDoesNotThrow(streamRune::close);
  }

  @Test
  void closeShouldNotCloseAFactoryReachedOnlyThroughConfiguredEventStoreFactory() {
    // Documents the current, structural limitation described on ConfiguredEventStoreFactory's
    // javadoc: a factory constructed INSIDE the settings lambda is invisible to the facade (the
    // lambda returns only the EventStore), so it is the caller's responsibility to close it.
    var factory = new RecordingCloseableEventStoreFactory();
    var streamRune = StreamRune.builder().eventStoreFactory(settings -> factory.create()).build();

    streamRune.close();

    assertFalse(
        factory.closed,
        "a factory reachable only from inside a ConfiguredEventStoreFactory lambda is not "
            + "retained by the facade and must not be closed by it");
  }

  @Test
  void closeShouldCloseAFactoryProducedByASettingsThreadedProvider() {
    // eventStoreFactoryProvider(ConfiguredEventStoreFactoryProvider) is the
    // settings-threaded overload that DOES get automatic cleanup — unlike
    // eventStoreFactory(ConfiguredEventStoreFactory) just above, whose lambda returns the
    // constructed EventStore (invisible to the facade). It is a DIFFERENTLY NAMED method (not a
    // third eventStoreFactory(...) overload) precisely so it never collides with that lambda form:
    // both take a single EventStoreSettings argument and return an unrelated functional-interface
    // type, and javac's overload resolution does not look inside an implicitly-typed lambda body
    // to disambiguate — a same-named third overload would have made every existing
    // .eventStoreFactory(settings -> ...) call site in this file (and
    // StreamRuneSettingsThreadingTest)
    // ambiguous and fail to compile. The lambda here returns the FACTORY itself;
    // builder-accumulated
    // settings (registerEventType) and automatic factory cleanup are no longer mutually exclusive.
    var factory = new RecordingCloseableEventStoreFactory();
    var receivedSettings = new AtomicReference<StreamRune.EventStoreSettings>();

    var streamRune =
        StreamRune.builder()
            .eventStoreFactoryProvider(
                settings -> {
                  receivedSettings.set(settings);
                  return factory;
                })
            .registerEventType("TestEvent", TestEvent.class)
            .build();

    assertFalse(factory.closed, "factory must not be closed before close()");
    assertNotNull(receivedSettings.get(), "the provider must receive the builder's settings");
    assertEquals(
        TestEvent.class,
        receivedSettings.get().typeRegistry().resolveEventType(new EventType("TestEvent")),
        "registerEventType must reach the provider exactly as it reaches "
            + "eventStoreFactory(ConfiguredEventStoreFactory)");

    streamRune.close();

    assertTrue(
        factory.closed,
        "StreamRune.close() must close the AutoCloseable EventStoreFactory a "
            + "ConfiguredEventStoreFactoryProvider produced, exactly like the plain "
            + "eventStoreFactory(EventStoreFactory) form");
  }

  static class NoOpProjectionRunner implements ProjectionRunner {
    @Override
    public void run(ProjectionName name, Projection projection, ProjectionDeliveryMode mode) {}

    @Override
    public void reset(ProjectionName projectionName) {}
  }

  /** Records "projections" into the shared order list when closed. */
  static class RecordingCloseableProjectionRunner implements ProjectionRunner, AutoCloseable {
    private final List<String> order;

    RecordingCloseableProjectionRunner(List<String> order) {
      this.order = order;
    }

    @Override
    public void run(ProjectionName name, Projection projection, ProjectionDeliveryMode mode) {}

    @Override
    public void reset(ProjectionName name) {}

    @Override
    public void close() {
      order.add("projections");
    }
  }

  static class TestUpcaster implements EventUpcaster {
    @Override
    public org.streamrune.core.types.EventType eventType() {
      return new org.streamrune.core.types.EventType("Test");
    }

    @Override
    public int currentVersion() {
      return 1;
    }

    @Override
    public Map<String, Object> upcast(Map<String, Object> data, int fromVersion) {
      return data;
    }
  }

  static class TestCryptoEngine implements CryptoEngine {
    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      // Simple XOR for testing - not secure, just for test verification
      byte[] result = new byte[plaintext.length];
      for (int i = 0; i < plaintext.length; i++) {
        result[i] = (byte) (plaintext[i] ^ 0x42);
      }
      return result;
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      // XOR is symmetric
      return encrypt(subjectId, ciphertext);
    }

    @Override
    public void deleteKey(SubjectId subjectId) {
      // no-op
    }

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return true;
    }
  }

  static class RecordingCloseableEventStoreFactory implements EventStoreFactory, AutoCloseable {
    boolean closed;

    @Override
    public EventStore create() {
      return new InMemoryEventStore();
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  record TestEvent(String payload) {}

  record TestState(String data) {}
}
