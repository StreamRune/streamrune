package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.aop.Around;
import io.micronaut.aop.InterceptorBean;
import io.micronaut.aop.MethodInterceptor;
import io.micronaut.aop.MethodInvocationContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.micronaut.inject.qualifiers.Qualifiers;
import jakarta.inject.Singleton;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.runtime.MultiProjectionRunner;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;
import org.streamrune.test.InMemoryProjectionRepository;

/**
 * A projection that carries a class-level around advice is registered as the generated {@code
 * $Intercepted} subclass, and that subclass overrides every method of the bean, the inherited
 * default methods of {@link Projection} included. What the delivery policy learns from such a bean
 * therefore cannot depend on which methods its class declares: whether a projection writes through
 * the handed repository is a declaration ({@link Projection#writesThroughRepository()}), not
 * something read back from the concrete class.
 *
 * <p>Boots real application contexts: the generated subclass exists only there.
 */
class MicronautInterceptedProjectionWiringTest {

  /** An application around advice, standing in for {@code @Transactional}, {@code @Timed}. */
  @Around
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE, ElementType.METHOD})
  @interface Observed {}

  @InterceptorBean(Observed.class)
  static class ObservedInterceptor implements MethodInterceptor<Object, Object> {
    @Override
    public Object intercept(MethodInvocationContext<Object, Object> context) {
      return context.proceed();
    }
  }

  @Factory
  @Requires(property = "spec.name", pattern = "intercepted-.*")
  static class Fixtures {
    @Singleton
    EventStore eventStore() {
      return new InMemoryEventStore();
    }

    @Singleton
    OffsetStore offsetStore() {
      return new InMemoryOffsetStore();
    }

    // The only processor candidate, and a fencing one: no DataSource bean, so the module produces
    // no JdbcProjectionRepository of its own.
    @Singleton
    AtomicBatchProcessor processor() {
      return new InMemoryProjectionRepository();
    }
  }

  /**
   * Implements only {@code process(List)}: the inherited two-argument {@code process} ignores the
   * repository, so the read-model writes would commit outside the checkpoint's transaction.
   */
  @Singleton
  @Observed
  @Requires(property = "spec.name", value = "intercepted-plain")
  @ProjectionConfig(
      name = "intercepted_plain",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class InterceptedPlainProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Extends {@link BaseProjection}: its two-argument {@code process} routes writes. */
  @Singleton
  @Observed
  @Requires(property = "spec.name", value = "intercepted-base")
  @ProjectionConfig(
      name = "intercepted_base",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class InterceptedBaseProjection extends BaseProjection {
    InterceptedBaseProjection() {
      super(null, "intercepted_base");
    }

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** Overrides the two-argument {@code process} and says so. */
  @Singleton
  @Observed
  @Requires(property = "spec.name", value = "intercepted-declared")
  @ProjectionConfig(
      name = "intercepted_declared",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  static class InterceptedDeclaredProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}

    @Override
    public void process(List<EventEnvelope> batch, ProjectionRepository repository) {}

    @Override
    public boolean writesThroughRepository() {
      return true;
    }
  }

  private static ApplicationContext run(String spec) {
    return ApplicationContext.run(
        Map.of("spec.name", spec, "streamrune.runner-lifecycle-enabled", "false"));
  }

  private static MultiProjectionRunner runnerOf(ApplicationContext ctx) {
    return ctx.getBean(
        MultiProjectionRunner.class, Qualifiers.byName(ProjectionFactory.FRAMEWORK_MULTI_RUNNER));
  }

  @Test
  void anInterceptedPlainProjection_isTheGeneratedSubclass_andDoesNotClaimToWriteThrough()
      throws Exception {
    try (var ctx = run("intercepted-plain")) {
      Projection bean = ctx.getBean(InterceptedPlainProjection.class);

      assertTrue(
          bean.getClass().getName().endsWith("$Intercepted"),
          "the premise: the bean is the generated advice subclass, was "
              + bean.getClass().getName());
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
  void anInterceptedPlainTransactionalLocalProjection_isRefusedAtBoot() {
    var failure =
        assertThrows(
            RuntimeException.class,
            () -> {
              try (var ctx = run("intercepted-plain")) {
                runnerOf(ctx);
              }
            });

    String messages = messages(failure);
    assertTrue(
        messages.contains("projection 'intercepted_plain' declares TRANSACTIONAL_LOCAL")
            && messages.contains("does not write through the handed repository"),
        () -> "expected the write-through refusal, got: " + messages);
  }

  @Test
  void anInterceptedBaseProjection_reportsWriteThrough_andBoots() {
    try (var ctx = run("intercepted-base")) {
      Projection bean = ctx.getBean(InterceptedBaseProjection.class);

      assertTrue(bean.getClass().getName().endsWith("$Intercepted"));
      assertTrue(bean.writesThroughRepository());
      assertInstanceOf(MultiProjectionRunner.class, runnerOf(ctx));
    }
  }

  @Test
  void anInterceptedProjectionThatDeclaresWriteThrough_boots() {
    try (var ctx = run("intercepted-declared")) {
      assertInstanceOf(MultiProjectionRunner.class, runnerOf(ctx));
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
