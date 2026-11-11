package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.lang.reflect.RecordComponent;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

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
      load(loader, "org.streamrune.runtime.OpenTelemetryCommandInterceptor");
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
              default -> null;
            });
  }
}
