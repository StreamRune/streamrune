package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.core.context.SecurityContextHolder;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.RequireRole;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.UserId;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Behavioural startup contract for the Spring {@link StreamRuneAuthorizationValidator}, mirroring
 * the cross-framework template in {@code
 * org.streamrune.quarkus.StreamRuneAuthorizationValidatorTest} and {@code
 * org.streamrune.micronaut.StreamRuneAuthorizationValidatorTest}.
 *
 * <p>The rule is the command bus's own: {@code VirtualThreadCommandBus.Builder.build()} refuses a
 * bus whose registered commands are annotated while its chain has no enforcing interceptor, and the
 * Spring validator — a {@link org.springframework.beans.factory.SmartInitializingSingleton} — runs
 * it again over every bus bean from {@code afterSingletonsInstantiated()}, creating a bus nothing
 * has created yet. The idiomatic way to exercise the real path is to drive a full {@link
 * ApplicationContextRunner} refresh and assert on the startup outcome. These tests therefore prove
 * fail-closed by <em>behaviour</em> (context refresh fails / succeeds), not merely by validator
 * bean presence, for the auto-configured bus and for a bus the application builds itself.
 */
class StreamRuneAuthorizationValidatorTest {

  /** Mirrors the {@code GuardedCommand} fixture used by the Quarkus/Micronaut template. */
  @RequireRole("ADMIN")
  record GuardedCommand(String id) implements Command {}

  record PlainCommand(String id) implements Command {}

  record TestState() implements AggregateState {}

  record TestEvent() implements DomainEvent {}

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(
              EventStoreFactory.class, () -> (EventStoreFactory) () -> mock(EventStore.class));

  /**
   * (a) Context refresh must FAIL when a registered command declares {@code @RequireRole} but no
   * {@link AnnotationAuthorizationInterceptor} is wired. Spring Security is filtered off the
   * classpath so the auto-config registers no default {@link UserRoleResolver} — and therefore no
   * enforcement interceptor — leaving the annotated command unguarded. The failure must name the
   * enforcement requirement (the {@code UNGUARDED} message and the offending command type); it is
   * raised while the auto-configured bus is built.
   */
  @Test
  void startup_fails_whenAnnotatedCommandWithoutEnforcement() {
    runner
        .withClassLoader(new FilteredClassLoader(SecurityContextHolder.class))
        .withUserConfiguration(GuardedDeciderConfig.class)
        .run(ctx -> assertRefusedAsUnguarded(ctx));
  }

  /**
   * (b) The inverse: context starts cleanly when enforcement IS configured. An explicit {@link
   * UserRoleResolver} bean causes the auto-config to register an {@link
   * AnnotationAuthorizationInterceptor}, which satisfies the validator. Asserting the interceptor
   * is actually present proves enforcement is real rather than the check having been skipped.
   */
  @Test
  void startup_succeeds_whenAnnotatedCommandAndEnforcementPresent() {
    runner
        .withClassLoader(new FilteredClassLoader(SecurityContextHolder.class))
        .withUserConfiguration(GuardedDeciderConfig.class, StubResolverConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(AnnotationAuthorizationInterceptor.class);
              assertThat(ctx).hasSingleBean(StreamRuneAuthorizationValidator.class);
            });
  }

  /**
   * (b') The on-classpath default path: with Spring Security present (as it is on the test
   * classpath) the auto-config wires {@link SpringSecurityUserRoleResolver} and the enforcement
   * interceptor, so an annotated command starts cleanly without any user-supplied resolver.
   */
  @Test
  void startup_succeeds_whenAnnotatedCommandAndSpringSecurityDefaultEnforcement() {
    runner
        .withUserConfiguration(GuardedDeciderConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(SpringSecurityUserRoleResolver.class);
              assertThat(ctx).hasSingleBean(AnnotationAuthorizationInterceptor.class);
            });
  }

  /**
   * (c) A command carrying NO authorization annotations needs no enforcement: context starts
   * cleanly even with Spring Security filtered off and no resolver/interceptor present.
   */
  @Test
  void startup_succeeds_whenNoAnnotatedCommands() {
    runner
        .withClassLoader(new FilteredClassLoader(SecurityContextHolder.class))
        .withUserConfiguration(PlainDeciderConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).doesNotHaveBean(AnnotationAuthorizationInterceptor.class);
              assertThat(ctx).hasSingleBean(StreamRuneAuthorizationValidator.class);
            });
  }

  /**
   * (d) The application builds its own bus and registers the guarded command through the builder,
   * so no {@link DeciderRegistration} bean exists, and wires no enforcing interceptor: startup is
   * refused all the same, naming the unguarded command.
   */
  @Test
  void startup_fails_whenAHandBuiltBusRegistersAnAnnotatedCommandWithoutEnforcement() {
    runner
        .withClassLoader(new FilteredClassLoader(SecurityContextHolder.class))
        .withUserConfiguration(HandBuiltBusConfig.class)
        .run(ctx -> assertRefusedAsUnguarded(ctx));
  }

  /**
   * (e) An {@link AnnotationAuthorizationInterceptor} bean exists (the application's {@link
   * UserRoleResolver} registers one), but the hand-built bus leaves it out of its chain: the bus
   * does not enforce the annotation, so startup is refused.
   */
  @Test
  void startup_fails_whenTheEnforcingInterceptorBeanIsLeftOutOfAHandBuiltBus() {
    runner
        .withClassLoader(new FilteredClassLoader(SecurityContextHolder.class))
        .withUserConfiguration(HandBuiltBusConfig.class, StubResolverConfig.class)
        .run(ctx -> assertRefusedAsUnguarded(ctx));
  }

  /**
   * (f) With lazy initialization nothing creates the bus during refresh; the validator resolves
   * every command bus itself, so the refusal still happens at startup rather than on the first
   * command.
   */
  @Test
  void startup_fails_whenALazilyCreatedHandBuiltBusLacksEnforcement() {
    runner
        .withClassLoader(new FilteredClassLoader(SecurityContextHolder.class))
        .withInitializer(
            context ->
                context.addBeanFactoryPostProcessor(
                    new LazyInitializationBeanFactoryPostProcessor()))
        .withUserConfiguration(HandBuiltBusConfig.class)
        .run(ctx -> assertRefusedAsUnguarded(ctx));
  }

  /**
   * (g) A hand-built bus whose chain carries an enforcing interceptor built inline (not a bean)
   * starts: what counts is the chain the bus runs, not the interceptor beans of the context.
   */
  @Test
  void startup_succeeds_whenAHandBuiltBusCarriesAnInlineEnforcingInterceptor() {
    runner
        .withClassLoader(new FilteredClassLoader(SecurityContextHolder.class))
        .withUserConfiguration(HandBuiltEnforcedBusConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).doesNotHaveBean(AnnotationAuthorizationInterceptor.class);
            });
  }

  private static void assertRefusedAsUnguarded(AssertableApplicationContext ctx) {
    assertThat(ctx).hasFailed();
    assertThat(rootCause(ctx.getStartupFailure()))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("UNGUARDED")
        .hasMessageContaining("AnnotationAuthorizationInterceptor")
        .hasMessageContaining(GuardedCommand.class.getName());
  }

  private static Throwable rootCause(Throwable failure) {
    Throwable root = failure;
    while (root.getCause() != null && root.getCause() != root) {
      root = root.getCause();
    }
    return root;
  }

  private static VirtualThreadCommandBus.Builder handBuiltBus() {
    DeciderRegistration<GuardedCommand, TestState, TestEvent> guarded =
        registrationFor(GuardedCommand.class);
    return VirtualThreadCommandBus.builder()
        .eventStore(mock(EventStore.class))
        .register(
            guarded.aggregateType(),
            guarded.commandType(),
            guarded.idExtractor(),
            guarded.decider());
  }

  private static <C extends Command> DeciderRegistration<C, TestState, TestEvent> registrationFor(
      Class<C> commandType) {
    return new DeciderRegistration<>(
        AggregateType.of("test"),
        commandType,
        cmd -> AggregateId.of("agg-1"),
        new Decider<>() {
          @Override
          public TestState initialState() {
            return new TestState();
          }

          @Override
          public List<TestEvent> decide(C command, TestState state) {
            return List.of();
          }

          @Override
          public TestState evolve(TestState state, TestEvent event) {
            return state;
          }
        });
  }

  @Configuration
  static class GuardedDeciderConfig {
    @Bean
    DeciderRegistration<GuardedCommand, TestState, TestEvent> guardedRegistration() {
      return registrationFor(GuardedCommand.class);
    }
  }

  @Configuration
  static class PlainDeciderConfig {
    @Bean
    DeciderRegistration<PlainCommand, TestState, TestEvent> plainRegistration() {
      return registrationFor(PlainCommand.class);
    }
  }

  /** The application's own bus: the guarded command registered through the builder only. */
  @Configuration(proxyBeanMethods = false)
  static class HandBuiltBusConfig {
    @Bean
    VirtualThreadCommandBus commandBus() {
      return handBuiltBus().build();
    }
  }

  /** The application's own bus, enforcing through an interceptor that is not a bean. */
  @Configuration(proxyBeanMethods = false)
  static class HandBuiltEnforcedBusConfig {
    @Bean
    VirtualThreadCommandBus commandBus() {
      return handBuiltBus()
          .interceptors(new AnnotationAuthorizationInterceptor(userId -> UserAuthority.EMPTY))
          .build();
    }
  }

  @Configuration
  static class StubResolverConfig {
    @Bean
    UserRoleResolver userRoleResolver() {
      return (UserId userId) -> UserAuthority.EMPTY;
    }
  }
}
