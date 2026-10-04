package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/**
 * Guards the fix for the runner-lifecycle-disabled leak: when {@code
 * streamrune.runner-lifecycle-enabled=false} the {@link StreamRuneLifecycle} bean is not created,
 * so nothing closes the background runners/sweepers or the {@code SubscriptionLeadership} held
 * connection on shutdown. Micronaut does <em>not</em> auto-close {@code @Factory}-produced {@link
 * AutoCloseable} singletons (unlike class-defined beans), so every such factory method must declare
 * {@code @Bean(preDestroy = "close")} as a shutdown safety net.
 */
class FactoryBeanDisposalTest {

  @Test
  void everyManagedBackgroundFactoryMethodDeclaresPreDestroyClose() {
    // The command bus must close on shutdown so in-flight commands drain gracefully (parity
    // with
    // Spring/Quarkus). Micronaut does not auto-close @Factory-produced AutoCloseable beans without
    // an explicit preDestroy, so without this the graceful-drain guarantee is silently lost.
    assertPreDestroyClose(StreamRuneMicronautModule.class, "virtualThreadCommandBus");
    assertPreDestroyClose(StreamRuneMicronautModule.class, "subscriptionLeadership");
    assertPreDestroyClose(StreamRuneMicronautModule.class, "outboxPoller");
    assertPreDestroyClose(StreamRuneMicronautModule.class, "outboxRetentionSweeper");
    assertPreDestroyClose(StreamRuneMicronautModule.class, "inboxRetentionSweeper");
    assertPreDestroyClose(StreamRuneMicronautModule.class, "sagaDeadLetterRetentionSweeper");
    assertPreDestroyClose(StreamRuneMicronautModule.class, "deadLetterRetentionSweeper");
    assertPreDestroyClose(StreamRuneMicronautModule.class, "deadLetterRetryRunner");
    assertPreDestroyClose(ProjectionFactory.class, "multiProjectionRunner");
    assertPreDestroyClose(ProjectionFactory.class, "scheduledProjectionRunner");
    // sseEventPublisher() produces an AutoCloseable SseEventPublisher with no
    // preDestroy, so its per-subscriber virtual-thread workers (and their queued decrypted
    // events) survive every context teardown — the same "framework-owned AutoCloseable without
    // preDestroy leaks" shape this test already guards for every OTHER factory method above.
    assertPreDestroyClose(StreamRuneMicronautModule.class, "sseEventPublisher");
  }

  private static void assertPreDestroyClose(Class<?> factory, String methodName) {
    Method method =
        Arrays.stream(factory.getDeclaredMethods())
            .filter(m -> m.getName().equals(methodName))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no factory method " + methodName));
    Bean bean = method.getAnnotation(Bean.class);
    assertNotNull(
        bean, () -> factory.getSimpleName() + "." + methodName + " must be @Bean(preDestroy=...)");
    assertEquals(
        "close",
        bean.preDestroy(),
        () -> factory.getSimpleName() + "." + methodName + " must declare preDestroy=\"close\"");
  }

  /**
   * Documents the framework behavior the fix relies on: an {@code AutoCloseable} produced by a
   * {@code @Factory} method is closed on context shutdown <em>only</em> when
   * {@code @Bean(preDestroy = "close")} is declared — without it Micronaut leaks the bean.
   */
  @Test
  void micronautClosesFactoryAutoCloseableOnlyWithPreDestroy() {
    Probe.WITHOUT_PREDESTROY.set(false);
    Probe.WITH_PREDESTROY.set(false);
    try (ApplicationContext ctx =
        ApplicationContext.run(Map.of("spec.name", "FactoryBeanDisposalTest"))) {
      assertNotNull(ctx.getBean(Probe.Unmanaged.class));
      assertNotNull(ctx.getBean(Probe.Managed.class));
    }
    assertFalse(
        Probe.WITHOUT_PREDESTROY.get(),
        "Micronaut must NOT auto-close a @Factory AutoCloseable without @Bean(preDestroy)");
    assertTrue(
        Probe.WITH_PREDESTROY.get(),
        "@Bean(preDestroy=\"close\") must close the @Factory AutoCloseable on shutdown");
  }

  static final class Probe {
    static final AtomicBoolean WITHOUT_PREDESTROY = new AtomicBoolean(false);
    static final AtomicBoolean WITH_PREDESTROY = new AtomicBoolean(false);

    static final class Unmanaged implements AutoCloseable {
      @Override
      public void close() {
        WITHOUT_PREDESTROY.set(true);
      }
    }

    static final class Managed implements AutoCloseable {
      @Override
      public void close() {
        WITH_PREDESTROY.set(true);
      }
    }

    @Factory
    static class ProbeFactory {
      @Singleton
      Unmanaged unmanaged() {
        return new Unmanaged();
      }

      @Singleton
      @Bean(preDestroy = "close")
      Managed managed() {
        return new Managed();
      }
    }
  }
}
