package org.streamrune.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.EventStore;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxStore;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * What every forwarding decorator of the kit does, checked over every method of its interface by
 * reflection, so a method added to the interface later cannot fall through to a default: the
 * decorator declares it, forwards it to the same method of the delegate with the same arguments and
 * returns what the delegate returned, and consults its fault schedule under the method's name.
 *
 * <p>The delegate is a {@link Proxy} that records each call: a proxy receives the interface's
 * default methods too, so a decorator that left one to its default would forward another method
 * instead, and the check names it.
 */
final class ForwardingDecoratorChecks {

  private ForwardingDecoratorChecks() {}

  /** A call the recording delegate received. */
  record Received(Method method, Object[] args) {}

  /** The instance methods of {@code type}, defaults included, in a stable order. */
  static List<Method> methodsOf(Class<?> type) {
    return Arrays.stream(type.getMethods())
        .filter(method -> !Modifier.isStatic(method.getModifiers()))
        .sorted(Comparator.comparing(Method::toGenericString))
        .toList();
  }

  /** Every method of {@code type} is declared by {@code decorator} itself. */
  static void assertEveryMethodOverridden(Class<?> type, Class<?> decorator, int expectedMethods) {
    List<Method> methods = methodsOf(type);
    assertEquals(
        expectedMethods,
        methods.size(),
        () -> type.getSimpleName() + " has these methods: " + methods);
    List<String> inherited = new ArrayList<>();
    for (Method method : methods) {
      try {
        Method declared = decorator.getDeclaredMethod(method.getName(), method.getParameterTypes());
        assertTrue(Modifier.isPublic(declared.getModifiers()), declared + " must be public");
      } catch (NoSuchMethodException missing) {
        inherited.add(method.toGenericString());
      }
    }
    if (!inherited.isEmpty()) {
      fail(decorator.getSimpleName() + " leaves these methods to a default: " + inherited);
    }
  }

  /**
   * Every method forwards to the same method of the delegate with the same arguments and returns
   * what the delegate returned; each method is called twice, with two sets of arguments and
   * answers.
   */
  static <T> void assertEveryMethodForwards(
      Class<T> type, BiFunction<T, FaultSchedule, T> decorate) {
    for (Method method : methodsOf(type)) {
      for (int variant = 0; variant < 2; variant++) {
        List<Received> received = new ArrayList<>();
        Object answer = sample(method.getReturnType(), 0, variant);
        T delegate = recording(type, received, answer);
        T decorator = decorate.apply(delegate, FaultSchedule.none());
        Object[] args = argumentsOf(method, variant);

        Object result = invoke(method, decorator, args);

        assertEquals(1, received.size(), () -> method + " made " + received + " on the delegate");
        assertEquals(method, received.getFirst().method(), () -> method + " forwarded another");
        assertArrayEquals(
            args, received.getFirst().args(), () -> method + " changed its arguments");
        assertEquals(answer, result, () -> method + " did not return the delegate's answer");
      }
    }
  }

  /**
   * Every method consults the schedule under its own name: {@code failOnce} fails it before the
   * delegate is called, {@code crashAfter} after, and both calls are counted.
   */
  static <T> void assertEveryMethodConsultsTheSchedule(
      Class<T> type, BiFunction<T, FaultSchedule, T> decorate) {
    for (Method method : methodsOf(type)) {
      String name = method.getName();
      Object[] args = argumentsOf(method, 0);

      List<Received> before = new ArrayList<>();
      FaultSchedule failing = FaultSchedule.none();
      T failingDecorator =
          decorate.apply(recording(type, before, sample(method.getReturnType(), 0, 0)), failing);
      failing.failOnce(name);
      assertInjected(method, () -> invoke(method, failingDecorator, args));
      assertTrue(before.isEmpty(), () -> method + " reached the delegate: " + before);
      assertEquals(1, failing.calls(name), () -> method + " was not counted");

      List<Received> after = new ArrayList<>();
      FaultSchedule crashing = FaultSchedule.none();
      T crashingDecorator =
          decorate.apply(recording(type, after, sample(method.getReturnType(), 0, 0)), crashing);
      crashing.crashAfter(name);
      assertInjected(method, () -> invoke(method, crashingDecorator, args));
      assertEquals(1, after.size(), () -> method + " did not reach the delegate first");
      assertEquals(1, crashing.calls(name), () -> method + " was not counted");
    }
  }

  private static void assertInjected(Method method, Runnable call) {
    try {
      call.run();
    } catch (InjectedFaultException expected) {
      return;
    }
    fail(method + " did not throw the injected fault");
  }

  private static Object invoke(Method method, Object target, Object[] args) {
    try {
      return method.invoke(target, args);
    } catch (InvocationTargetException e) {
      if (e.getCause() instanceof RuntimeException runtime) {
        throw runtime;
      }
      throw new AssertionError(e.getCause());
    } catch (IllegalAccessException e) {
      throw new AssertionError(e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T> T recording(Class<T> type, List<Received> received, Object answer) {
    return (T)
        Proxy.newProxyInstance(
            type.getClassLoader(),
            new Class<?>[] {type},
            (proxy, method, args) -> {
              if (method.getDeclaringClass() == Object.class) {
                return switch (method.getName()) {
                  case "equals" -> proxy == args[0];
                  case "hashCode" -> System.identityHashCode(proxy);
                  default -> "recording " + type.getSimpleName();
                };
              }
              received.add(new Received(method, args == null ? new Object[0] : args));
              return answer;
            });
  }

  private static Object[] argumentsOf(Method method, int variant) {
    Class<?>[] types = method.getParameterTypes();
    Object[] args = new Object[types.length];
    for (int i = 0; i < types.length; i++) {
      args[i] = sample(types[i], i + 1, variant);
    }
    return args;
  }

  /**
   * A value of {@code type} that differs by position and by variant, so a decorator that swaps two
   * arguments or answers with a constant is caught; {@code null} for a type with no sample (it is
   * forwarded as it is).
   */
  static Object sample(Class<?> type, int position, int variant) {
    int n = 10 * position + variant + 1;
    if (type == void.class) {
      return null;
    }
    if (type == boolean.class || type == Boolean.class) {
      return (position + variant) % 2 == 0;
    }
    if (type == int.class || type == Integer.class) {
      return n;
    }
    if (type == long.class || type == Long.class) {
      return (long) n;
    }
    if (type == String.class) {
      return "value-" + n;
    }
    if (type == Object.class) {
      return "identity-" + n;
    }
    if (type.isEnum()) {
      Object[] constants = type.getEnumConstants();
      return constants[n % constants.length];
    }
    if (type == List.class || type == Collection.class) {
      return List.of("element-" + n);
    }
    if (type == Optional.class) {
      return Optional.of("present-" + n);
    }
    if (type == Class.class) {
      return variant == 0 ? String.class : Integer.class;
    }
    if (type == Instant.class) {
      return Instant.ofEpochSecond(1_000_000L + n);
    }
    if (type == Duration.class) {
      return Duration.ofMillis(n);
    }
    if (type == GlobalOffset.class) {
      return GlobalOffset.of(n);
    }
    if (type == Version.class) {
      return new Version(n);
    }
    if (type == StreamId.class) {
      return StreamId.of(AggregateType.of("type" + n), AggregateId.of("id-" + n));
    }
    if (type == IdempotencyKey.class) {
      return IdempotencyKey.of("key-" + n);
    }
    if (type == SagaId.class) {
      return SagaId.of("saga-" + n);
    }
    if (type == SagaType.class) {
      return SagaType.of("saga.Type" + n);
    }
    if (type == SagaOrigin.class) {
      return SagaOrigin.dispatchedBy(SagaType.of("saga.Origin" + n), SagaId.of("origin-" + n));
    }
    if (type == EventId.class) {
      return EventId.of("event-" + n);
    }
    if (type == CommandId.class) {
      return CommandId.of("command-" + n);
    }
    if (type == ProjectionName.class) {
      return ProjectionName.of("projection_" + n);
    }
    if (type == OutboxEntryId.class) {
      return OutboxEntryId.of("outbox-" + n);
    }
    if (type == SagaStore.AppliedEvent.class) {
      return SagaStore.AppliedEvent.live(GlobalOffset.of(n));
    }
    if (type == AggregateHistory.class) {
      return new AggregateHistory(null, List.of(), new Version(n), Version.initial());
    }
    if (type == EventStore.AppendResult.class) {
      return new EventStore.AppendResult(List.of(GlobalOffset.of(n)), new Version(n));
    }
    if (type == EventStore.IdempotentAppendResult.class) {
      return new EventStore.IdempotentAppendResult(List.of(), new Version(n), variant == 0);
    }
    if (type == OutboxStore.BlockageSample.class) {
      return new OutboxStore.BlockageSample(n, Instant.ofEpochSecond(n), n + 1L);
    }
    if (type.isInterface()) {
      return Proxy.newProxyInstance(
          type.getClassLoader(),
          new Class<?>[] {type},
          (proxy, method, args) ->
              switch (method.getName()) {
                case "equals" -> proxy == args[0];
                case "hashCode" -> System.identityHashCode(proxy);
                case "toString" -> type.getSimpleName() + "-" + n;
                default -> throw new UnsupportedOperationException(method.toString());
              });
    }
    return null;
  }

  /** The sample values of the check are what they say they are. */
  static void assertTheSamplesHaveTheirTypes() {
    for (Class<?> type :
        List.of(
            AggregateHistory.class,
            EventStore.AppendResult.class,
            EventStore.IdempotentAppendResult.class,
            OutboxStore.BlockageSample.class,
            SagaStore.AppliedEvent.class,
            StreamId.class)) {
      assertInstanceOf(type, sample(type, 1, 0));
    }
  }
}
