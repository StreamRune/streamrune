package org.streamrune.runtime;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.core.CommandBus;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.saga.SagaDeadLetterStore;
import org.streamrune.core.saga.SagaStore;

/**
 * The crash decorators of {@link CrashingStores} declare an override of every data method of the
 * SPI they wrap. A method the kit's forwarding class forwards and the decorator does not override
 * would reach the store with no crash hook, so a node that crashed would still write through it; a
 * method added to an SPI fails here until its decorator hooks it. The methods a decorator leaves to
 * the kit are named, and must still exist on the SPI.
 */
class CrashingStoresHooksTest {

  @Test
  void theSagaStoreDecoratorHooksEveryDataMethod() {
    assertHooked(
        CrashingStores.NodeSagaStore.class,
        SagaStore.class,
        Set.of("storageIdentity", "connectionFailureClassifier"));
  }

  @Test
  void theDeadLetterStoreDecoratorHooksEveryDataMethod() {
    assertHooked(CrashingStores.NodeDeadLetters.class, SagaDeadLetterStore.class, Set.of());
  }

  @Test
  void theOffsetStoreDecoratorHooksEveryDataMethod() {
    assertHooked(CrashingStores.NodeOffsets.class, OffsetStore.class, Set.of());
  }

  @Test
  void theBusDecoratorHooksEveryDataMethod() {
    assertHooked(
        CrashingStores.NodeBus.class,
        CommandBus.class,
        Set.of("supportsIdempotentExecution", "sagaFenceStorageIdentity"));
  }

  private static void assertHooked(Class<?> decorator, Class<?> spi, Set<String> notHooked) {
    List<String> missing = new ArrayList<>();
    for (Method method : spi.getMethods()) {
      if (Modifier.isStatic(method.getModifiers()) || notHooked.contains(method.getName())) {
        continue;
      }
      try {
        decorator.getDeclaredMethod(method.getName(), method.getParameterTypes());
      } catch (NoSuchMethodException unhooked) {
        missing.add(method.toGenericString());
      }
    }
    assertThat(missing).as(decorator.getSimpleName() + " leaves unhooked").isEmpty();
    assertThat(notHooked)
        .as("every method left to the kit is a method of " + spi.getSimpleName())
        .allMatch(name -> Arrays.stream(spi.getMethods()).anyMatch(m -> m.getName().equals(name)));
  }
}
