package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import jakarta.interceptor.AroundInvoke;
import jakarta.interceptor.Interceptor;
import jakarta.interceptor.InterceptorBinding;
import jakarta.interceptor.InvocationContext;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.runtime.MultiProjectionRunner;

/**
 * A projection that carries a class-level interceptor binding is registered as the generated Arc
 * subclass, and that subclass overrides every method of the bean, the inherited default methods of
 * {@link Projection} included. What the delivery policy learns from such a bean therefore cannot
 * depend on which methods its class declares: whether a projection writes through the handed
 * repository is a declaration ({@link Projection#writesThroughRepository()}), not something read
 * back from the concrete class.
 *
 * <p>Boots a REAL Arc container ({@link RealArcTestContainer}): the generated subclass exists only
 * there. A {@code BaseProjection} subclass is not part of this: its two-argument {@code process} is
 * {@code final}, which a real Quarkus build makes interceptable by rewriting the class at
 * augmentation, a step this in-process harness does not run.
 */
class QuarkusInterceptedProjectionWiringTest {

  /** An application interceptor binding, standing in for {@code @Transactional}, {@code @Timed}. */
  @InterceptorBinding
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  public @interface Observed {}

  @Observed
  @Interceptor
  @Priority(Interceptor.Priority.APPLICATION)
  public static class ObservedInterceptor {
    @AroundInvoke
    public Object around(InvocationContext context) throws Exception {
      return context.proceed();
    }
  }

  /**
   * Implements only {@code process(List)}: the inherited two-argument {@code process} ignores the
   * repository, so the read-model writes would commit outside the checkpoint's transaction.
   */
  @ProjectionConfig(
      name = "intercepted_plain",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  @Observed
  @Singleton
  public static class InterceptedPlainProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Overrides the two-argument {@code process} and says so. */
  @ProjectionConfig(
      name = "intercepted_declared",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  @Observed
  @Singleton
  public static class InterceptedDeclaredProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}

    @Override
    public void process(List<EventEnvelope> batch, ProjectionRepository repository) {}

    @Override
    public boolean writesThroughRepository() {
      return true;
    }
  }

  /** MicroProfile Config for the producer, which raw Arc does not wire. */
  @ApplicationScoped
  public static class ConfigBeans {
    @Produces
    @Singleton
    public org.eclipse.microprofile.config.Config config() {
      return new io.smallrye.config.SmallRyeConfigBuilder().addDefaultSources().build();
    }
  }

  private static RealArcTestContainer boot(Class<?> projection) throws Exception {
    return RealArcTestContainer.boot(
        List.of(
            StreamRuneProducers.class,
            ProjectionProducer.class,
            QuarkusBeanWiringTest.ApplicationInfrastructure.class,
            QuarkusBeanWiringTest.ApplicationEventStoreOverride.class,
            ConfigBeans.class,
            Observed.class,
            ObservedInterceptor.class,
            projection));
  }

  private static MultiProjectionRunner runnerOf(RealArcTestContainer arc) {
    return arc.container()
        .instance(
            MultiProjectionRunner.class,
            jakarta.enterprise.inject.literal.NamedLiteral.of(
                ProjectionProducer.FRAMEWORK_MULTI_RUNNER))
        .get();
  }

  @Test
  void anInterceptedPlainProjection_isTheGeneratedSubclass_andDoesNotClaimToWriteThrough()
      throws Exception {
    try (var arc = boot(InterceptedPlainProjection.class)) {
      Projection bean = arc.container().instance(InterceptedPlainProjection.class).get();

      assertInstanceOf(
          io.quarkus.arc.Subclass.class,
          bean,
          "the premise: the contextual instance is the generated interceptor subclass");
      assertFalse(
          bean.getClass().getMethod("process", List.class, ProjectionRepository.class).isDefault(),
          "the premise: the subclass overrides the inherited default two-argument process");
      assertFalse(
          bean.writesThroughRepository(),
          "a projection that implements only process(List) does not write through the repository,"
              + " whatever the generated subclass declares");
    }
  }

  @Test
  void anInterceptedPlainTransactionalLocalProjection_isRefusedAtBoot() throws Exception {
    try (var arc = boot(InterceptedPlainProjection.class)) {
      Throwable failure = assertThrows(RuntimeException.class, () -> runnerOf(arc));

      assertTrue(
          messages(failure).contains("projection 'intercepted_plain' declares TRANSACTIONAL_LOCAL")
              && messages(failure).contains("does not write through the handed repository"),
          () -> "expected the write-through refusal, got: " + messages(failure));
    }
  }

  @Test
  void anInterceptedProjectionThatDeclaresWriteThrough_boots() throws Exception {
    try (var arc = boot(InterceptedDeclaredProjection.class)) {
      assertInstanceOf(MultiProjectionRunner.class, runnerOf(arc));
    }
  }

  private static String messages(Throwable failure) {
    StringBuilder all = new StringBuilder();
    for (Throwable t = failure; t != null; t = t.getCause()) {
      all.append(t.getMessage()).append(" | ");
    }
    return all.toString();
  }
}
