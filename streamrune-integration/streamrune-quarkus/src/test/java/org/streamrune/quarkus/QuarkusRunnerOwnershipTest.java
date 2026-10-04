package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import io.quarkus.arc.DefaultBean;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Named;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.runtime.OutboxPoller;

/**
 * Runner ownership on a real Arc container: an application that declares its own (unqualified)
 * runner bean owns it, so the framework neither creates nor starts its {@code @DefaultBean} runner
 * of that type and does not start the application's either — the contract Spring
 * ({@code @ConditionalOnMissingBean} plus a lifecycle bound to the framework bean name) and
 * Micronaut ({@code @Requires(missingBeans = ...)}) already keep.
 *
 * <p>Arc suppresses a {@code @DefaultBean} only where it would be AMBIGUOUS with another bean. The
 * lifecycle's injection point carries the framework {@code @Named} qualifier, which the
 * application's bean lacks, so before this check Arc resolved the framework producer anyway: the
 * demo's Quarkus native binary ran two dead-letter retry runners, the framework's (started, with
 * the framework policy) and the application's (never started). The framework-style producers below
 * repeat the annotations of the real ones, which {@link
 * #theFixtureCarriesTheRealProducersAnnotations} pins.
 */
class QuarkusRunnerOwnershipTest {

  static volatile DeadLetterRetryRunner frameworkRetryRunner;
  static volatile OutboxPoller frameworkPoller;
  static volatile DeadLetterRetryRunner applicationRetryRunner;
  static volatile OutboxPoller applicationPoller;
  static volatile MultiProjectionRunner frameworkProjectionRunner;
  static volatile MultiProjectionRunner applicationProjectionRunner;
  static final AtomicInteger frameworkProjectionRunnersProduced = new AtomicInteger();
  static final AtomicInteger frameworkRetryRunnersProduced = new AtomicInteger();
  static final AtomicInteger frameworkPollersProduced = new AtomicInteger();

  @BeforeEach
  void freshRunners() {
    frameworkRetryRunner = mock(DeadLetterRetryRunner.class);
    frameworkPoller = mock(OutboxPoller.class);
    applicationRetryRunner = mock(DeadLetterRetryRunner.class);
    applicationPoller = mock(OutboxPoller.class);
    frameworkProjectionRunner = mock(MultiProjectionRunner.class);
    applicationProjectionRunner = mock(MultiProjectionRunner.class);
    frameworkRetryRunnersProduced.set(0);
    frameworkPollersProduced.set(0);
    frameworkProjectionRunnersProduced.set(0);
  }

  /** The framework's producer idiom: {@code @Produces @DefaultBean @Named(FRAMEWORK_...)}. */
  @ApplicationScoped
  public static class FrameworkStyleRunners {

    @Produces
    @DefaultBean
    @Named(StreamRuneProducers.FRAMEWORK_DEAD_LETTER_RETRY_RUNNER)
    public DeadLetterRetryRunner deadLetterRetryRunner() {
      frameworkRetryRunnersProduced.incrementAndGet();
      return frameworkRetryRunner;
    }

    @Produces
    @DefaultBean
    @Named(StreamRuneProducers.FRAMEWORK_OUTBOX_POLLER)
    public OutboxPoller outboxPoller() {
      frameworkPollersProduced.incrementAndGet();
      return frameworkPoller;
    }

    @Produces
    @DefaultBean
    @Named(ProjectionProducer.FRAMEWORK_MULTI_RUNNER)
    public MultiProjectionRunner multiProjectionRunner() {
      frameworkProjectionRunnersProduced.incrementAndGet();
      return frameworkProjectionRunner;
    }
  }

  /** What an application (the demo's Quarkus app) declares: plain, unqualified producers. */
  @ApplicationScoped
  public static class ApplicationRunners {

    @Produces
    public DeadLetterRetryRunner applicationDeadLetterRetryRunner() {
      return applicationRetryRunner;
    }

    @Produces
    public OutboxPoller applicationOutboxPoller() {
      return applicationPoller;
    }

    @Produces
    public MultiProjectionRunner applicationMultiProjectionRunner() {
      return applicationProjectionRunner;
    }
  }

  @Test
  void anApplicationRunnerSuppressesTheFrameworkRunner_whichIsNeitherCreatedNorStarted()
      throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneLifecycle.class,
                FrameworkStyleRunners.class,
                ApplicationRunners.class))) {
      startUp(arc);

      assertEquals(
          0,
          frameworkRetryRunnersProduced.get(),
          "the framework's DeadLetterRetryRunner must not even be created");
      assertEquals(
          0, frameworkPollersProduced.get(), "the framework's OutboxPoller must not be created");
      assertEquals(
          0,
          frameworkProjectionRunnersProduced.get(),
          "a second projection runner over the same projections would apply every event twice");
      verify(applicationRetryRunner, never()).start();
      verify(applicationPoller, never()).start();
      verify(applicationProjectionRunner, never()).start();
      var lifecycle = arc.container().instance(StreamRuneLifecycle.class).get();
      assertNull(lifecycle.deadLetterRetryRunner());
      assertNull(lifecycle.outboxPoller());
      // The application's runner is still the one its own injection points get.
      assertSame(
          applicationRetryRunner, arc.container().instance(DeadLetterRetryRunner.class).get());
    }
  }

  @Test
  void withoutAnApplicationRunnerTheFrameworkRunnerIsCreatedAndStarted() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneLifecycle.class, FrameworkStyleRunners.class))) {
      startUp(arc);

      assertEquals(1, frameworkRetryRunnersProduced.get());
      assertEquals(1, frameworkPollersProduced.get());
      assertEquals(1, frameworkProjectionRunnersProduced.get());
      verify(frameworkRetryRunner).start();
      verify(frameworkPoller).start();
      verify(frameworkProjectionRunner).start();
      var lifecycle = arc.container().instance(StreamRuneLifecycle.class).get();
      assertSame(frameworkRetryRunner, lifecycle.deadLetterRetryRunner());
      assertSame(frameworkPoller, lifecycle.outboxPoller());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"deadLetterRetryRunner", "outboxPoller", "multiProjectionRunner"})
  void theFixtureCarriesTheRealProducersAnnotations(String producer) {
    Method real =
        declared(
            producer.equals("multiProjectionRunner")
                ? ProjectionProducer.class
                : StreamRuneProducers.class,
            producer);
    Method fixture = declared(FrameworkStyleRunners.class, producer);
    assertNotNull(real.getAnnotation(Produces.class));
    assertNotNull(real.getAnnotation(DefaultBean.class));
    assertEquals(
        fixture.getAnnotation(Named.class).value(), real.getAnnotation(Named.class).value());
    assertEquals(fixture.getReturnType(), real.getReturnType());
  }

  private static Method declared(Class<?> type, String name) {
    return Arrays.stream(type.getDeclaredMethods())
        .filter(m -> m.getName().equals(name))
        .findFirst()
        .orElseThrow();
  }

  private static void startUp(RealArcTestContainer arc) {
    arc.container().beanManager().getEvent().select(StartupEvent.class).fire(new StartupEvent());
  }
}
