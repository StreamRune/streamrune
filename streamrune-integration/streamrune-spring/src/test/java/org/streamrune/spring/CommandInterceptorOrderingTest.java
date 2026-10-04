package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.CommandAuthorizationPolicy;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.ProjectionConfig;
import org.streamrune.core.projection.Projection;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.AuditCommandInterceptor;
import org.streamrune.runtime.AuthorizationCommandInterceptor;
import org.streamrune.runtime.BeanValidationInterceptor;
import org.streamrune.runtime.CircuitBreakerCommandInterceptor;
import org.streamrune.runtime.InlineProjectionInterceptor;
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;

/**
 * Proves the auto-registered command interceptor chain is deterministic: every framework
 * interceptor carries an explicit {@code @Order} (the {@code ORDER_*} constants on {@link
 * StreamRuneAutoConfiguration}), so {@code before()} runs OpenTelemetry → Audit → Authorization →
 * AnnotationAuthorization → BeanValidation → CircuitBreaker → InlineProjection, and {@code
 * after()}/{@code onError()} run in reverse. Without explicit ordering, e.g. the circuit breaker
 * could count validation rejections as failures, or audit could miss rejected commands.
 */
class CommandInterceptorOrderingTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(
              AutoConfigurations.of(StreamRuneAutoConfiguration.class, ProjectionAutoConfig.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
          .withBean(
              io.opentelemetry.api.OpenTelemetry.class, io.opentelemetry.api.OpenTelemetry::noop)
          .withBean(CommandAuthorizationPolicy.class, () -> mock(CommandAuthorizationPolicy.class))
          .withBean("orderingInlineProjection", Projection.class, InlineTestProjection::new);

  @Test
  void frameworkInterceptorsHaveDeterministicOrder() {
    runner.run(
        ctx -> {
          List<Class<?>> chain =
              ctx.getBeanProvider(CommandInterceptor.class)
                  .orderedStream()
                  .<Class<?>>map(Object::getClass)
                  .toList();
          assertThat(chain)
              .containsExactly(
                  OpenTelemetryCommandInterceptor.class,
                  AuditCommandInterceptor.class,
                  AuthorizationCommandInterceptor.class,
                  AnnotationAuthorizationInterceptor.class,
                  BeanValidationInterceptor.class,
                  CircuitBreakerCommandInterceptor.class,
                  InlineProjectionInterceptor.class);
        });
  }

  @Test
  void unorderedUserInterceptorsRunInnermost() {
    var userInterceptor = new CommandInterceptor() {};
    runner
        .withBean("userInterceptor", CommandInterceptor.class, () -> userInterceptor)
        .run(
            ctx -> {
              List<CommandInterceptor> chain =
                  ctx.getBeanProvider(CommandInterceptor.class).orderedStream().toList();
              assertThat(chain.getLast()).isSameAs(userInterceptor);
            });
  }

  @ProjectionConfig(
      name = "ordering-inline",
      mode = ProjectionConfig.Mode.INLINE,
      deliveryMode = org.streamrune.core.projection.ProjectionDeliveryMode.AT_LEAST_ONCE_IDEMPOTENT)
  static class InlineTestProjection implements Projection {
    @Override
    public void process(List<org.streamrune.core.EventEnvelope> batch) {}
  }

  // ── a user-supplied AuditCommandInterceptor must keep its canonical slot ────────

  record OrderingProbeCommand(String id) implements org.streamrune.core.Command {}

  record OrderingProbeState() implements org.streamrune.core.AggregateState {}

  static final class OrderingProbeDecider
      implements org.streamrune.core.Decider<
          OrderingProbeCommand, OrderingProbeState, org.streamrune.core.DomainEvent> {
    @Override
    public OrderingProbeState initialState() {
      return new OrderingProbeState();
    }

    @Override
    public List<org.streamrune.core.DomainEvent> decide(
        OrderingProbeCommand command, OrderingProbeState state) {
      return List.of();
    }

    @Override
    public OrderingProbeState evolve(
        OrderingProbeState state, org.streamrune.core.DomainEvent event) {
      return state;
    }
  }

  @org.springframework.context.annotation.Configuration
  static class OrderingProbeDeciderConfig {
    @org.springframework.context.annotation.Bean
    org.streamrune.runtime.DeciderRegistration<
            OrderingProbeCommand, OrderingProbeState, org.streamrune.core.DomainEvent>
        orderingProbeRegistration() {
      return new org.streamrune.runtime.DeciderRegistration<>(
          org.streamrune.core.types.AggregateType.of("test"),
          OrderingProbeCommand.class,
          cmd -> org.streamrune.core.types.AggregateId.of(cmd.id()),
          new OrderingProbeDecider());
    }
  }

  /**
   * {@link StreamRuneAutoConfiguration#auditCommandInterceptor} is
   * {@code @ConditionalOnMissingBean(AuditCommandInterceptor.class)} — an application that defines
   * its own {@code AuditCommandInterceptor} (e.g. to redirect audit writes to a custom store — the
   * use case the Micronaut sibling's javadoc names verbatim) makes the framework bean back off.
   * Until this fix, Spring then built the interceptor chain straight from {@code
   * interceptorProvider.orderedStream()} with NO normalization: an un-annotated bean sorts at
   * {@code Ordered.LOWEST_PRECEDENCE}, i.e. AFTER {@code AuthorizationCommandInterceptor} (@Order
   * 3000) — the opposite of the canonical ORDER_AUDIT (2000, outside authorization) slot every
   * other framework interceptor gets automatically. A denied command then throws inside
   * authorization's {@code before()}; {@code VirtualThreadCommandBus} only delivers {@code
   * onError()} to interceptors whose {@code before()} actually ran, so the user's audit
   * interceptor's {@code before()} never having a chance to run first meant the denial went
   * UNAUDITED. Quarkus and Micronaut both run {@code CommandInterceptorOrdering.sorted(...)}, which
   * keys the canonical slot by exact class regardless of annotations, so the identical override IS
   * audited on those two (see Quarkus's {@code
   * QuarkusInterceptorOrderingTest#policyDeniedCommandIsAudited_provingAuditSortsOutsideAuthorization}).
   * This is the Spring mirror — through a REAL {@link ApplicationContextRunner} context and a REAL
   * command execution, not a hand-built bus, so it actually exercises Spring's
   * {@code @ConditionalOnMissingBean} back-off plus whatever (or, before this fix, nothing) orders
   * the resulting chain.
   */
  @Test
  void userSuppliedAuditInterceptorStillAuditsAPolicyDenial() {
    var saved =
        new java.util.concurrent.CopyOnWriteArrayList<org.streamrune.core.audit.AuditEntry>();
    org.streamrune.core.audit.AuditStore auditStore = saved::add;

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(OrderingProbeDeciderConfig.class)
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withBean(EventStore.class, () -> mock(EventStore.class))
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(
            CommandAuthorizationPolicy.class, () -> CommandAuthorizationPolicy.denyAll("denied"))
        // The user's own AuditCommandInterceptor bean, with NO @Order — exactly the documented
        // override path. It must back off the framework's auditCommandInterceptor
        // (@ConditionalOnMissingBean(AuditCommandInterceptor.class)) and still land at the
        // canonical ORDER_AUDIT slot (outside authorization), not at Ordered.LOWEST_PRECEDENCE.
        .withBean(AuditCommandInterceptor.class, () -> new AuditCommandInterceptor(auditStore))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBeanProvider(AuditCommandInterceptor.class).stream().count())
                  .as(
                      "the user bean must be the ONLY AuditCommandInterceptor — the framework"
                          + " default must have backed off")
                  .isEqualTo(1);

              var bus = ctx.getBean(org.streamrune.runtime.VirtualThreadCommandBus.class);
              org.junit.jupiter.api.Assertions.assertThrows(
                  org.streamrune.core.AuthorizationException.class,
                  () -> bus.execute(new OrderingProbeCommand("ip-1")));

              assertThat(saved)
                  .as(
                      "a policy-denied command must be audited as FAILURE by the user's OWN"
                          + " AuditCommandInterceptor — proving it sorted OUTSIDE (before) the"
                          + " AuthorizationCommandInterceptor, not innermost")
                  .anyMatch(e -> e.outcome() == org.streamrune.core.audit.AuditOutcome.FAILURE);
            });
  }
}
