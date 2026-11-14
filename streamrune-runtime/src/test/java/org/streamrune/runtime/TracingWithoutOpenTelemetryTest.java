package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.RecordComponent;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code opentelemetry-api} is an optional dependency of this module. With {@code
 * StreamRuneTracing.NOOP}, the default of every builder, an application without it on its classpath
 * builds a command bus and wraps listeners and projections without loading an OpenTelemetry class;
 * with a {@code StreamRuneTracing} of its own the three emitters trace without one too. The test
 * runs that in a class loader that sees this module's test runtime classpath minus every
 * OpenTelemetry jar.
 */
class TracingWithoutOpenTelemetryTest {

  private static URLClassLoader loaderWithoutOpenTelemetry() throws MalformedURLException {
    List<URL> urls = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      if (!entry.isBlank() && !entry.contains("opentelemetry")) {
        urls.add(new File(entry).toURI().toURL());
      }
    }
    return new URLClassLoader(urls.toArray(URL[]::new), ClassLoader.getPlatformClassLoader());
  }

  @Test
  void noopTracing_needsNoOpenTelemetryClass() throws Exception {
    try (URLClassLoader loader = loaderWithoutOpenTelemetry()) {
      assertThatThrownBy(() -> Class.forName("io.opentelemetry.api.OpenTelemetry", false, loader))
          .isInstanceOf(ClassNotFoundException.class);

      Class<?> tracingType = load(loader, "org.streamrune.core.tracing.StreamRuneTracing");
      Object noop = tracingType.getField("NOOP").get(null);

      // The emitters initialize, so their verification links no OpenTelemetry type either.
      load(loader, "org.streamrune.runtime.TracingCommandInterceptor");
      Class<?> listenerDecorator = load(loader, "org.streamrune.runtime.TracingEventListener");
      Class<?> projectionDecorator =
          load(loader, "org.streamrune.runtime.TracingProjectionDecorator");

      Class<?> listenerType = load(loader, "org.streamrune.core.subscription.EventListener");
      Class<?> subscriptionName = load(loader, "org.streamrune.core.types.SubscriptionName");
      Object listener = proxy(loader, listenerType);
      Object name = subscriptionName.getMethod("of", String.class).invoke(null, "s");
      Method tracedListener =
          listenerDecorator.getMethod("traced", listenerType, tracingType, subscriptionName);
      assertThat(tracedListener.invoke(null, listener, noop, name)).isSameAs(listener);

      Class<?> projectionType = load(loader, "org.streamrune.core.projection.Projection");
      Class<?> projectionName = load(loader, "org.streamrune.core.types.ProjectionName");
      Object projection = proxy(loader, projectionType);
      Object pName = projectionName.getMethod("of", String.class).invoke(null, "p");
      Method tracedProjection =
          projectionDecorator.getMethod("traced", projectionName, projectionType, tracingType);
      assertThat(tracedProjection.invoke(null, pName, projection, noop)).isSameAs(projection);

      Class<?> busType = load(loader, "org.streamrune.runtime.VirtualThreadCommandBus");
      Class<?> storeType = load(loader, "org.streamrune.core.EventStore");
      Object store =
          load(loader, "org.streamrune.test.InMemoryEventStore").getConstructor().newInstance();
      Object builder = busType.getMethod("builder").invoke(null);
      Class<?> builderType = builder.getClass();
      builderType.getMethod("eventStore", storeType).invoke(builder, store);
      builderType.getMethod("tracing", tracingType).invoke(builder, noop);
      try (AutoCloseable bus = (AutoCloseable) builderType.getMethod("build").invoke(builder)) {
        assertThat((List<?>) busType.getMethod("interceptors").invoke(bus)).isEmpty();
      }
    }
  }

  @Test
  void aTracingOfItsOwn_tracesThroughTheEmittersWithoutOpenTelemetry() throws Exception {
    try (URLClassLoader loader = loaderWithoutOpenTelemetry()) {
      Class<?> tracingType = load(loader, "org.streamrune.core.tracing.StreamRuneTracing");
      Object noopScope =
          load(loader, "org.streamrune.core.tracing.TraceScope").getField("NOOP").get(null);
      List<String> started = new ArrayList<>();
      Object tracing =
          Proxy.newProxyInstance(
              loader,
              new Class<?>[] {tracingType},
              (self, method, args) ->
                  switch (method.getName()) {
                    case "enabled" -> true;
                    case "start" -> {
                      started.add((String) args[1]);
                      yield noopScope;
                    }
                    case "hashCode" -> System.identityHashCode(self);
                    case "equals" -> self == args[0];
                    default -> "tracing";
                  });

      Class<?> listenerType = load(loader, "org.streamrune.core.subscription.EventListener");
      Class<?> subscriptionName = load(loader, "org.streamrune.core.types.SubscriptionName");
      Object listener =
          load(loader, "org.streamrune.runtime.TracingEventListener")
              .getMethod("traced", listenerType, tracingType, subscriptionName)
              .invoke(
                  null,
                  proxy(loader, listenerType),
                  tracing,
                  subscriptionName.getMethod("of", String.class).invoke(null, "s"));
      listenerType.getMethod("onEvents", List.class).invoke(listener, List.of());

      Class<?> projectionType = load(loader, "org.streamrune.core.projection.Projection");
      Class<?> projectionName = load(loader, "org.streamrune.core.types.ProjectionName");
      Object projection =
          load(loader, "org.streamrune.runtime.TracingProjectionDecorator")
              .getMethod("traced", projectionName, projectionType, tracingType)
              .invoke(
                  null,
                  projectionName.getMethod("of", String.class).invoke(null, "p"),
                  proxy(loader, projectionType),
                  tracing);
      projectionType.getMethod("process", List.class).invoke(projection, List.of());

      Class<?> interceptorType = load(loader, "org.streamrune.core.CommandInterceptor");
      Class<?> contextType = load(loader, "org.streamrune.core.CommandInterceptor$CommandContext");
      Class<?> commandIdType = load(loader, "org.streamrune.core.types.CommandId");
      Constructor<?> canonical =
          contextType.getDeclaredConstructor(
              Arrays.stream(contextType.getRecordComponents())
                  .map(RecordComponent::getType)
                  .toArray(Class<?>[]::new));
      Object context =
          canonical.newInstance(
              proxy(loader, load(loader, "org.streamrune.core.Command")),
              "Switch",
              commandIdType.getMethod("of", String.class).invoke(null, "cmd_1"),
              null,
              null,
              null,
              Instant.now());
      // The bus builder creates the interceptor, as an application's code would: reflection over
      // the interceptor's constructors would resolve the OpenTelemetry one too.
      Class<?> busType = load(loader, "org.streamrune.runtime.VirtualThreadCommandBus");
      Object builder = busType.getMethod("builder").invoke(null);
      builder
          .getClass()
          .getMethod("eventStore", load(loader, "org.streamrune.core.EventStore"))
          .invoke(
              builder,
              load(loader, "org.streamrune.test.InMemoryEventStore")
                  .getConstructor()
                  .newInstance());
      builder.getClass().getMethod("tracing", tracingType).invoke(builder, tracing);
      Object interceptor;
      try (AutoCloseable bus =
          (AutoCloseable) builder.getClass().getMethod("build").invoke(builder)) {
        interceptor = ((List<?>) busType.getMethod("interceptors").invoke(bus)).getFirst();
      }
      interceptorType.getMethod("before", contextType).invoke(interceptor, context);
      interceptorType.getMethod("after", contextType).invoke(interceptor, context);

      assertThat(started)
          .containsExactly("subscription/s/deliver", "projection/p/process", "command/Switch");
    }
  }

  /**
   * The subscription and projection runner builders call the {@code traced(...)} factories; with
   * {@code NOOP} a polling subscription delivers and a continuous runner processes without loading
   * an OpenTelemetry class.
   */
  @Test
  void noopTracing_subscriptionAndRunnerBuildersNeedNoOpenTelemetryClass() throws Exception {
    try (URLClassLoader loader = loaderWithoutOpenTelemetry()) {
      Class<?> tracingType = load(loader, "org.streamrune.core.tracing.StreamRuneTracing");
      Object noop = tracingType.getField("NOOP").get(null);
      Class<?> storeType = load(loader, "org.streamrune.core.EventStore");
      Class<?> offsetStoreType = load(loader, "org.streamrune.core.projection.OffsetStore");
      Object store =
          load(loader, "org.streamrune.test.InMemoryEventStore").getConstructor().newInstance();

      Class<?> listenerType = load(loader, "org.streamrune.core.subscription.EventListener");
      Class<?> subscriptionName = load(loader, "org.streamrune.core.types.SubscriptionName");
      Class<?> pollingType = load(loader, "org.streamrune.runtime.PollingEventSubscription");
      Object polling = pollingType.getMethod("builder").invoke(null);
      Class<?> pollingBuilder = polling.getClass();
      pollingBuilder
          .getMethod("subscriptionName", subscriptionName)
          .invoke(polling, subscriptionName.getMethod("of", String.class).invoke(null, "s"));
      pollingBuilder.getMethod("eventStore", storeType).invoke(polling, store);
      pollingBuilder
          .getMethod("offsetStore", offsetStoreType)
          .invoke(polling, newOffsetStore(loader));
      pollingBuilder
          .getMethod("listener", listenerType)
          .invoke(polling, proxy(loader, listenerType));
      pollingBuilder.getMethod("tracing", tracingType).invoke(polling, noop);
      Object subscription = pollingBuilder.getMethod("build").invoke(polling);
      Method pollOnce = pollingType.getDeclaredMethod("pollOnce");
      pollOnce.setAccessible(true);
      assertThat(pollOnce.invoke(subscription)).isEqualTo(0);

      Class<?> runnerType = load(loader, "org.streamrune.runtime.ContinuousProjectionRunner");
      Class<?> processorType = load(loader, "org.streamrune.core.projection.AtomicBatchProcessor");
      Object runnerBuilder = runnerType.getMethod("builder").invoke(null);
      Class<?> runnerBuilderType = runnerBuilder.getClass();
      runnerBuilderType.getMethod("eventStore", storeType).invoke(runnerBuilder, store);
      runnerBuilderType
          .getMethod("offsetStore", offsetStoreType)
          .invoke(runnerBuilder, newOffsetStore(loader));
      runnerBuilderType
          .getMethod("atomicProcessor", processorType)
          .invoke(runnerBuilder, processorType.getMethod("nonAtomicAtLeastOnce").invoke(null));
      runnerBuilderType.getMethod("tracing", tracingType).invoke(runnerBuilder, noop);
      Object runner = runnerBuilderType.getMethod("build").invoke(runnerBuilder);

      Class<?> projectionType = load(loader, "org.streamrune.core.projection.Projection");
      Class<?> projectionName = load(loader, "org.streamrune.core.types.ProjectionName");
      Class<?> modeType = load(loader, "org.streamrune.core.projection.ProjectionDeliveryMode");
      Method run = runnerType.getMethod("run", projectionName, projectionType, modeType);
      Object name = projectionName.getMethod("of", String.class).invoke(null, "p");
      Object projection = proxy(loader, projectionType);
      Object mode = modeType.getField("AT_LEAST_ONCE_IDEMPOTENT").get(null);
      AtomicReference<Throwable> failure = new AtomicReference<>();
      Thread thread =
          Thread.ofVirtual()
              .start(
                  () -> {
                    try {
                      run.invoke(runner, name, projection, mode);
                    } catch (ReflectiveOperationException e) {
                      failure.set(e.getCause() != null ? e.getCause() : e);
                    }
                  });
      // The empty store has nothing to catch up on: the run reaches its live phase and waits.
      Method state = runnerType.getMethod("state");
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!"LIVE".equals(String.valueOf(state.invoke(runner)))
          && thread.isAlive()
          && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertThat(String.valueOf(state.invoke(runner))).isEqualTo("LIVE");
      ((AutoCloseable) runner).close();
      thread.join(TimeUnit.SECONDS.toMillis(5));
      assertThat(failure.get()).isNull();
      assertThat(thread.isAlive()).isFalse();
    }
  }

  /**
   * Code that creates the three emitters through their {@code StreamRuneTracing} constructors and
   * factories compiles without {@code opentelemetry-api}: no signature javac reads to resolve them
   * names an OpenTelemetry type. The control source shows that the compile classpath holds no
   * OpenTelemetry type.
   */
  @Test
  void theEmittersCompileWithoutOpenTelemetry(@TempDir Path dir) throws IOException {
    String classpath = classpathWithoutOpenTelemetry();
    String emitters =
        """
        import org.streamrune.core.tracing.StreamRuneTracing;
        import org.streamrune.core.types.ProjectionName;
        import org.streamrune.core.types.SubscriptionName;
        import org.streamrune.runtime.TracingCommandInterceptor;
        import org.streamrune.runtime.TracingEventListener;
        import org.streamrune.runtime.TracingProjectionDecorator;
        import org.streamrune.runtime.VirtualThreadCommandBus;

        class Emitters {
          static void create(StreamRuneTracing tracing) {
            new TracingCommandInterceptor(tracing);
            new TracingEventListener(events -> {}, tracing, SubscriptionName.of("s"));
            new TracingProjectionDecorator(ProjectionName.of("p"), batch -> {}, tracing);
            TracingEventListener.traced(events -> {}, tracing, SubscriptionName.of("s"));
            TracingProjectionDecorator.traced(ProjectionName.of("p"), batch -> {}, tracing);
            VirtualThreadCommandBus.builder().tracing(tracing);
          }
        }
        """;
    String control =
        """
        class Control {
          io.opentelemetry.api.OpenTelemetry openTelemetry;
        }
        """;

    assertThat(compile(dir.resolve("emitters"), "Emitters", emitters, classpath)).isEmpty();
    assertThat(compile(dir.resolve("control"), "Control", control, classpath))
        .contains("io.opentelemetry.api");
  }

  /**
   * The emitters of the event store, the locker, the outbox poller and the saga runners compile and
   * run without {@code opentelemetry-api}: a source that calls their {@code tracing(...)} and
   * {@code withTracing(...)} methods compiles against the classpath without any OpenTelemetry jar,
   * and run in a class loader without one it starts no span with {@code NOOP} and every span with a
   * tracing of its own.
   */
  @Test
  void theNewEmittersCompileAndRunWithoutOpenTelemetry(@TempDir Path dir) throws Exception {
    String source =
        """
        package org.streamrune.runtime;

        import java.time.Duration;
        import java.util.List;
        import java.util.Map;
        import java.util.Optional;
        import java.util.concurrent.CopyOnWriteArrayList;
        import org.streamrune.core.DomainEvent;
        import org.streamrune.core.EventEnvelope;
        import org.streamrune.core.EventMetadata;
        import org.streamrune.core.RetryPolicy;
        import org.streamrune.core.SimpleEventTypeRegistry;
        import org.streamrune.core.outbox.OutboxEntry;
        import org.streamrune.core.outbox.OutboxEntryId;
        import org.streamrune.core.outbox.OutboxOrderingMode;
        import org.streamrune.core.saga.SagaCommand;
        import org.streamrune.core.saga.SagaDecider;
        import org.streamrune.core.saga.SagaId;
        import org.streamrune.core.saga.SagaState;
        import org.streamrune.core.saga.SagaStatus;
        import org.streamrune.core.tracing.SpanAttributes;
        import org.streamrune.core.tracing.SpanKind;
        import org.streamrune.core.tracing.SpanBaggage;
        import org.streamrune.core.tracing.SpanLink;
        import org.streamrune.core.tracing.StreamRuneTracing;
        import org.streamrune.core.tracing.TraceScope;
        import org.streamrune.core.tracing.TracingArea;
        import org.streamrune.core.types.AggregateId;
        import org.streamrune.core.types.AggregateType;
        import org.streamrune.core.types.CommandId;
        import org.streamrune.core.types.CorrelationId;
        import org.streamrune.core.types.EventId;
        import org.streamrune.core.types.EventType;
        import org.streamrune.core.types.GlobalOffset;
        import org.streamrune.core.types.StreamId;
        import org.streamrune.core.types.Version;
        import org.streamrune.test.InMemoryEventStore;
        import org.streamrune.test.InMemoryOutboxStore;
        import org.streamrune.test.SagaFixture;

        public class NewEmitters {
          public record Switched(String to) implements DomainEvent {}

          public record LampState(int on) implements org.streamrune.core.AggregateState {}

          public record OrderState(SagaStatus status) implements SagaState {}

          public record CancelOrder() implements org.streamrune.core.Command {}

          public static final class OrderSaga implements SagaDecider<OrderState> {
            public Class<OrderState> stateType() { return OrderState.class; }
            public OrderState initialState(SagaId id) { return new OrderState(SagaStatus.STARTED); }
            public OrderState evolve(OrderState s, EventEnvelope e) {
              return new OrderState(SagaStatus.RUNNING);
            }
            public List<SagaCommand> handle(OrderState s, EventEnvelope e) { return List.of(); }
            public List<SagaCommand> compensate(OrderState s, Throwable f, SagaCommand c) {
              return List.of(SagaCommand.of(new CancelOrder(), AggregateId.of("o-1")));
            }
            public Optional<Duration> timeout() { return Optional.of(Duration.ofMinutes(1)); }
          }

          public static List<String> run(boolean traced) throws Exception {
            List<String> started = new CopyOnWriteArrayList<>();
            StreamRuneTracing tracing =
                !traced
                    ? StreamRuneTracing.NOOP
                    : new StreamRuneTracing() {
                      public boolean enabled(TracingArea area) { return true; }
                      public TraceScope start(TracingArea area, String name, SpanKind kind,
                          SpanAttributes attributes, SpanLink link, SpanBaggage baggage) {
                        started.add(name);
                        return TraceScope.NOOP;
                      }
                    };
            StreamId stream = StreamId.of(AggregateType.of("lamp"), AggregateId.of("l-1"));
            new LocalStripedLocker(1).traced(tracing)
                .acquireLock(stream, Duration.ofSeconds(1)).close();

            InMemoryEventStore store =
                InMemoryEventStore.serializing(
                        SimpleEventTypeRegistry.builder().registerEvent("Switched", Switched.class)
                            .registerState("LampState", LampState.class).build(),
                        null)
                    .withTracing(tracing);
            EventMetadata metadata = new EventMetadata(EventId.of("e-1"), CommandId.of("c-1"),
                null, CorrelationId.of("r-1"), null, null, java.time.Instant.now(), Map.of());
            store.append(stream, List.of(new EventEnvelope(GlobalOffset.initial(), stream,
                Version.initial(), EventType.of("Switched"), new Switched("on"), metadata)),
                Version.initial());
            store.saveSnapshot(stream, new Version(1), new LampState(1), 1, 0);
            store.load(stream, 1);
            store.readGlobalStream(GlobalOffset.initial(), 10);

            InMemoryOutboxStore outbox = new InMemoryOutboxStore(java.time.Clock.systemUTC(),
                InMemoryOutboxStore.DEFAULT_CLAIM_LEASE, OutboxOrderingMode.AVAILABILITY_FIRST);
            outbox.save(OutboxEntry.pending(OutboxEntryId.of("o-1"), "{}", "Switched"));
            OutboxPoller.builder().outboxStore(outbox).publisher(entry -> {})
                .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
                .pollInterval(Duration.ofSeconds(60)).tracing(tracing).build().pollCycle();

            SagaFixture<OrderState> saga =
                SagaFixture.of(OrderState.class, b -> b.decider(new OrderSaga())
                        .startWhen(e -> e.event() instanceof Switched)
                        .extractSagaId(e -> SagaId.of("saga-1"))
                        .tracing(tracing))
                    .withTimeouts(b -> b.decider(new OrderSaga()).tracing(tracing));
            saga.when(stream, new Switched("on")).advance(Duration.ofMinutes(2)).runTimeouts();
            return started;
          }
        }
        """;
    Path classes = dir.resolve("emitters").resolve("classes");
    assertThat(
            compile(
                dir.resolve("emitters"), "NewEmitters", source, classpathWithoutOpenTelemetry()))
        .isEmpty();

    List<URL> urls = new ArrayList<>();
    urls.add(classes.toUri().toURL());
    try (URLClassLoader base = loaderWithoutOpenTelemetry()) {
      urls.addAll(List.of(base.getURLs()));
    }
    try (URLClassLoader loader =
        new URLClassLoader(urls.toArray(URL[]::new), ClassLoader.getPlatformClassLoader())) {
      assertThatThrownBy(() -> Class.forName("io.opentelemetry.api.OpenTelemetry", false, loader))
          .isInstanceOf(ClassNotFoundException.class);
      Method run =
          load(loader, "org.streamrune.runtime.NewEmitters").getMethod("run", boolean.class);

      assertThat((List<?>) run.invoke(null, false)).isEmpty();
      String sagaType = "org.streamrune.runtime.NewEmitters$OrderState";
      List<String> traced =
          ((List<?>) run.invoke(null, true)).stream().map(String::valueOf).toList();
      assertThat(traced)
          .contains(
              "lock/acquire",
              "eventstore/append",
              "eventstore/append/serialize",
              "eventstore/load",
              "eventstore/snapshot/save",
              "eventstore/snapshot/load",
              "eventstore/load/deserialize",
              "eventstore/read",
              "outbox/relay",
              "outbox/publish",
              "saga/" + sagaType + "/step",
              "saga/" + sagaType + "/timeout",
              "saga/" + sagaType + "/compensate");
    }
  }

  /** Compiles one source file and returns javac's error messages, empty when it compiles. */
  private static String compile(Path dir, String className, String source, String classpath)
      throws IOException {
    Path sources = Files.createDirectories(dir.resolve("src"));
    Path classes = Files.createDirectories(dir.resolve("classes"));
    Path file = Files.writeString(sources.resolve(className + ".java"), source);
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
    try (StandardJavaFileManager files =
        compiler.getStandardFileManager(diagnostics, null, StandardCharsets.UTF_8)) {
      boolean compiled =
          compiler
              .getTask(
                  null,
                  files,
                  diagnostics,
                  List.of("-proc:none", "-classpath", classpath, "-d", classes.toString()),
                  null,
                  files.getJavaFileObjects(file))
              .call();
      if (compiled) {
        return "";
      }
    }
    StringBuilder errors = new StringBuilder();
    for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
      if (diagnostic.getKind() == Diagnostic.Kind.ERROR) {
        errors.append(diagnostic.getMessage(Locale.ROOT)).append('\n');
      }
    }
    return errors.isEmpty() ? "compilation failed without an error message" : errors.toString();
  }

  private static String classpathWithoutOpenTelemetry() {
    List<String> entries = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      if (!entry.isBlank() && !entry.contains("opentelemetry")) {
        entries.add(entry);
      }
    }
    return String.join(File.pathSeparator, entries);
  }

  private static Object newOffsetStore(ClassLoader loader) throws ReflectiveOperationException {
    return load(loader, "org.streamrune.test.InMemoryOffsetStore").getConstructor().newInstance();
  }

  private static Class<?> load(ClassLoader loader, String name) throws ClassNotFoundException {
    return Class.forName(name, true, loader);
  }

  private static Object proxy(ClassLoader loader, Class<?> type) {
    return Proxy.newProxyInstance(
        loader,
        new Class<?>[] {type},
        (self, method, args) ->
            switch (method.getName()) {
              case "hashCode" -> System.identityHashCode(self);
              case "equals" -> self == args[0];
              case "toString" -> type.getSimpleName();
              case "writesThroughRepository" -> false;
              case "writeTarget" -> Optional.empty();
              default -> null;
            });
  }
}
