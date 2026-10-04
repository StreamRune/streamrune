package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.runtime.BeanValidationInterceptor;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * The Bean Validation <strong>API</strong> on the classpath with <strong>no provider</strong> must
 * degrade to validation-disabled, not abort context refresh.
 *
 * <p>{@code @ConditionalOnClass(jakarta.validation.Validator.class)} can only see the API jar, and
 * {@code streamrune.validation-enabled} defaults to true, so the {@code beanValidationInterceptor}
 * factory's fallback — {@code Validation.buildDefaultValidatorFactory()} — was the default
 * execution path for every app that pulls jakarta.validation-api transitively without a provider.
 * It threw {@code NoProviderFoundException}, failing context initialization: the application did
 * not boot, from a dependency graph that boots fine without StreamRune. The Quarkus sibling
 * degrades gracefully for exactly this case; Micronaut is safe because micronaut-validation is a
 * hard implementation dependency.
 *
 * <p><strong>This class runs in its own test JVM</strong> ({@code noValidationProviderTest} in
 * {@code build.gradle.kts}), whose classpath has hibernate-validator removed. It is excluded from
 * the main {@code test} task, where the provider IS present. A classloader-based harness cannot
 * substitute: jakarta.validation's bootstrap falls back to resolving providers through its own
 * classloader when the thread-context classloader yields none, so a {@code FilteredClassLoader}
 * cannot hide a provider that is on the JVM classpath.
 */
class BeanValidationNoProviderTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore);

  @Test
  void thisJvmHasTheValidationApiButNoProvider() {
    // Guards the harness itself: if a provider ever leaks back onto this task's classpath the two
    // tests below would silently stop covering the degradation path.
    assertThat(jakarta.validation.Validator.class).isNotNull();
    assertThatExceptionOfType(jakarta.validation.NoProviderFoundException.class)
        .isThrownBy(jakarta.validation.Validation::buildDefaultValidatorFactory);
  }

  @Test
  void validationApiWithoutProvider_contextStartsWithCommandValidationDisabled() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          // The factory returns null, so Spring registers a NullBean under the name: the definition
          // is still listed by type, but no instance is resolvable and Spring's ObjectProvider
          // streams filter NullBeans out — which is what the command bus consumes.
          assertThat(ctx.getBeanProvider(BeanValidationInterceptor.class).getIfAvailable())
              .isNull();
          assertThat(
                  ctx.getBeanProvider(CommandInterceptor.class)
                      .orderedStream()
                      .map(Object::getClass))
              .doesNotContain(BeanValidationInterceptor.class);
          // the rest of the framework still wires up — only command validation is skipped
          assertThat(ctx).hasSingleBean(VirtualThreadCommandBus.class);
          assertThat(ctx).hasSingleBean(EventStore.class);
        });
  }

  @Test
  void validatorBeanSuppliedByTheApplication_stillWiresTheInterceptor() {
    // The degradation must be limited to the no-provider fallback: an application that supplies its
    // own Validator bean (spring-boot-starter-validation, or a hand-built one) keeps command
    // validation, because the ObjectProvider resolves before the fallback runs.
    runner
        .withBean(
            jakarta.validation.Validator.class, () -> mock(jakarta.validation.Validator.class))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(BeanValidationInterceptor.class);
            });
  }
}
