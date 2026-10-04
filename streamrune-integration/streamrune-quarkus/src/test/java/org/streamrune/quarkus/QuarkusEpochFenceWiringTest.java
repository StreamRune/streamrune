package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.AtomicBatchProcessor;
import org.streamrune.core.projection.BaseProjection;
import org.streamrune.core.projection.Projection;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.projection.ProjectionRepository;
import org.streamrune.postgres.JdbcProjectionRepository;
import org.streamrune.runtime.MultiProjectionRunner;

/**
 * Container-level wiring of the projection epoch fence.
 *
 * <p>No test proved the epoch fence is actually armed in an auto-configured Quarkus application.
 * Every existing fencing test either constructs {@link JdbcProjectionRepository} directly (proving
 * the processor fences, not that any container ever supplies one), or drives {@link
 * ProjectionProducer} with hand-built mocks via {@code ProjectionProducerTest} (proving a parameter
 * is forwarded, not that a container produces a non-null one).
 *
 * <p>Micronaut's equivalent gap was closed by {@code
 * MicronautBeanWiringTest#frameworkProjectionRunner_getsAFencingProcessor_whenDataSourcePresent};
 * Spring's by {@code
 * EventStoreWiringAutoConfigurationTest#jdbcProjectionRepositoryIsWiredAsAtomicBatchProcessorOnProjectionRunner}.
 * Quarkus was the one integration with no container-driven equivalent.
 *
 * <p>Boots a REAL Arc container (the wiring defect can only be observed this way — see {@link
 * RealArcTestContainer}), registers a real CONTINUOUS {@code @ProjectionConfig} bean, and asserts
 * the framework-assembled {@link MultiProjectionRunner}'s {@code atomicProcessor} field is the
 * auto-configured {@link JdbcProjectionRepository} — never {@link
 * AtomicBatchProcessor#nonAtomicAtLeastOnce()}, which would leave the leadership epoch fence and
 * overlap guard silently inert.
 *
 * <p>The second test boots a {@code TRANSACTIONAL_LOCAL} projection over an application-scoped
 * repository, so the projection and the runner both hold Arc's client proxy, never the instance
 * behind it: the write-target identity check must see through the proxy on both sides.
 */
class QuarkusEpochFenceWiringTest {

  // Public: RealArcTestContainer's generated producer bean classes live in an isolated
  // URLClassLoader, so a package-private nested class here is inaccessible cross-classloader
  // (IllegalAccessError) — matches the visibility QuarkusBeanWiringTest's own fixtures use.
  // This fixture exists so the wiring under test (a fencing AtomicBatchProcessor is selected when
  // leadership is on) has SOME projection to attach to a real MultiProjectionRunner.
  @ProjectionConfig(
      name = "fenced_orders",
      deliveryMode = ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  public static class FencedProjection implements Projection {
    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /** A TRANSACTIONAL_LOCAL read model over the application-scoped repository's client proxy. */
  @ProjectionConfig(
      name = "proxied_orders",
      deliveryMode = ProjectionDeliveryMode.TRANSACTIONAL_LOCAL)
  public static class ProxiedOrders extends BaseProjection {
    public ProxiedOrders(ProjectionRepository repository) {
      super(repository, "proxied_orders");
    }

    @Override
    public void process(List<EventEnvelope> batch) {}
  }

  /**
   * The bean type of the application-scoped repository. {@link JdbcProjectionRepository} is {@code
   * final}: a Quarkus build removes that modifier at augmentation so Arc can subclass it into a
   * client proxy, but this in-process harness cannot rewrite a class the test class loader has
   * already loaded. Declaring the bean by an interface over the two contracts the runner and the
   * projection use gives Arc an interface to proxy, with a real {@code JdbcProjectionRepository}
   * behind it.
   */
  public interface TransactionalRepository extends ProjectionRepository, AtomicBatchProcessor {}

  /**
   * The application's own repository, {@code @ApplicationScoped}: every injection point and every
   * {@code Instance} lookup receives Arc's client proxy, which forwards to a contextual instance
   * that forwards to one {@link JdbcProjectionRepository}.
   */
  @ApplicationScoped
  public static class ApplicationScopedRepository {
    @Produces
    @ApplicationScoped
    public TransactionalRepository repo(DataSource dataSource) {
      var jdbc = new JdbcProjectionRepository(dataSource);
      return (TransactionalRepository)
          Proxy.newProxyInstance(
              TransactionalRepository.class.getClassLoader(),
              new Class<?>[] {TransactionalRepository.class},
              (proxy, method, args) -> {
                try {
                  return method.invoke(jdbc, args);
                } catch (InvocationTargetException e) {
                  throw e.getCause();
                }
              });
    }

    @Produces
    @Singleton
    public ProxiedOrders proxiedOrders(TransactionalRepository repository) {
      return new ProxiedOrders(repository);
    }
  }

  /**
   * Registers {@link FencedProjection} as a real CDI bean discoverable via {@code
   * Instance<Projection>}.
   */
  @ApplicationScoped
  public static class ProjectionBeans {
    @Produces
    @Singleton
    public FencedProjection fencedProjection() {
      return new FencedProjection();
    }

    // ProjectionProducer's methods take a raw MicroProfile Config (auto-discovery kill switch,
    // per-projection enabled/error-strategy overrides) — raw Arc does not itself wire the SmallRye
    // config extension, so this test supplies one directly, matching QuarkusBeanWiringTest#bind.
    @Produces
    @Singleton
    public org.eclipse.microprofile.config.Config config() {
      return new io.smallrye.config.SmallRyeConfigBuilder().addDefaultSources().build();
    }
  }

  @Test
  void frameworkProjectionRunner_getsAFencingProcessor_whenDataSourcePresent() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ProjectionProducer.class,
                QuarkusBeanWiringTest.ApplicationInfrastructure.class,
                QuarkusBeanWiringTest.ApplicationEventStoreOverride.class,
                ProjectionBeans.class,
                FencedProjection.class))) {
      var runnerInstance =
          arc.container()
              .instance(
                  MultiProjectionRunner.class,
                  jakarta.enterprise.inject.literal.NamedLiteral.of(
                      ProjectionProducer.FRAMEWORK_MULTI_RUNNER));
      assertTrue(
          runnerInstance.isAvailable(),
          "the framework-assembled MultiProjectionRunner must exist once a CONTINUOUS projection"
              + " bean is registered");
      MultiProjectionRunner runner = runnerInstance.get();

      Field field = MultiProjectionRunner.class.getDeclaredField("atomicProcessor");
      field.setAccessible(true);
      Object processor = field.get(runner);

      // Leadership is on, so the fencing-capable bean is selected for the epoch, although every
      // registration is at-least-once.
      assertFalse(
          processor instanceof AtomicBatchProcessor.NonAtomicAtLeastOnce,
          "the framework-assembled runner must not run on the nonatomic processor while leadership"
              + " is on — the epoch fence and first-offset overlap guard are inert there");
      assertInstanceOf(JdbcProjectionRepository.class, processor);
    }
  }

  @Test
  void transactionalProjection_overTheApplicationScopedRepositoryProxy_verifiesAndBoots()
      throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(
                StreamRuneProducers.class,
                ProjectionProducer.class,
                QuarkusBeanWiringTest.ApplicationInfrastructure.class,
                QuarkusBeanWiringTest.ApplicationEventStoreOverride.class,
                ProjectionBeans.class,
                ApplicationScopedRepository.class,
                ProxiedOrders.class))) {
      var runnerInstance =
          arc.container()
              .instance(
                  MultiProjectionRunner.class,
                  jakarta.enterprise.inject.literal.NamedLiteral.of(
                      ProjectionProducer.FRAMEWORK_MULTI_RUNNER));
      assertTrue(
          runnerInstance.isAvailable(),
          "the TRANSACTIONAL_LOCAL projection over the client proxy must pass the write-target"
              + " identity check and boot");
      MultiProjectionRunner runner = runnerInstance.get();

      Field field = MultiProjectionRunner.class.getDeclaredField("atomicProcessor");
      field.setAccessible(true);
      Object processor = field.get(runner);

      assertInstanceOf(io.quarkus.arc.ClientProxy.class, processor);
      assertSame(
          arc.container().instance(TransactionalRepository.class).get(),
          processor,
          "the runner's processor is the application-scoped bean's client proxy");
    }
  }
}
