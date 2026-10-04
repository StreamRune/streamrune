package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Boots the auto-configuration on a classpath that <strong>genuinely lacks</strong> the optional
 * dependencies {@code streamrune-spring} declares {@code compileOnly} — the servlet API, Spring
 * Boot's health (actuator) module, Bean Validation, and Spring MVC. This is the WebFlux app, the
 * {@code spring-boot-starter-jersey} app, and a headless HA worker.
 *
 * <p><strong>Why a separate test JVM.</strong> {@code FilteredClassLoader} cannot express genuine
 * absence: it carries no URLs of its own and delegates every non-filtered name to the parent
 * classloader, so a framework class such as {@link ScopedValueFilter} is still <em>defined</em> by
 * the parent — whose classpath still contains {@code servlet-api} — and its direct superinterface
 * {@code jakarta.servlet.Filter} therefore links successfully. The earlier regression test does
 * exactly that and passed throughout, while a real WebFlux deployment failed context refresh with
 * {@code IllegalStateException: Failed to introspect Class [StreamRuneAutoConfiguration]} wrapping
 * a NoClassDefFoundError: {@code Class#getDeclaredMethods} resolves every declared method's return
 * and parameter types, so a single {@code @Bean} method mentioning an optional type breaks the
 * whole configuration class <em>before</em> any {@code @ConditionalOnClass} /
 * {@code @ConditionalOnWebApplication} gate is consulted — those gates only suppress bean
 * REGISTRATION.
 *
 * <p>This class runs under the {@code optionalDependencyAbsentTest} Gradle task, which strips those
 * jars from the test runtime classpath (the same pattern as {@code noValidationProviderTest}). It
 * is excluded from the normal {@code test} task, where all of them are present.
 */
class OptionalDependencyAbsentTest {

  /** Types the auto-configuration may reference only from a nested, class-gated configuration. */
  private static final String[] ABSENT_TYPES = {
    "jakarta.servlet.Filter",
    "org.springframework.boot.health.contributor.HealthIndicator",
    "jakarta.validation.Validator",
    "org.springframework.web.servlet.mvc.method.annotation.SseEmitter",
  };

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withUserConfiguration(StubInfrastructure.class);

  @Test
  void preconditionTheOptionalJarsAreReallyAbsent() {
    // Guards the Gradle classpath filter: if a jar sneaks back in, every assertion below would pass
    // vacuously and the regression would ship again.
    for (String type : ABSENT_TYPES) {
      assertThatThrownBy(() -> Class.forName(type))
          .as("%s must NOT be on this test JVM's classpath", type)
          .isInstanceOf(ClassNotFoundException.class);
    }
  }

  @Test
  void contextStartsWithoutServletActuatorAndValidationOnTheClasspath() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          // The servlet/actuator/validation beans are absent, as their conditions intend...
          assertThat(ctx).doesNotHaveBean("scopedValueFilter");
          assertThat(ctx).doesNotHaveBean("scopedValueFilterRegistration");
          assertThat(ctx).doesNotHaveBean("streamRuneHealthIndicator");
          assertThat(ctx).doesNotHaveBean("beanValidationInterceptor");
          // ...while the rest of the framework wires up normally.
          assertThat(ctx).hasSingleBean(VirtualThreadCommandBus.class);
          assertThat(ctx).hasSingleBean(EventStore.class);
          assertThat(ctx).hasSingleBean(StreamRuneConfigValidator.class);
        });
  }

  /**
   * {@code streamrune.sse.enabled=true} on a classpath without Spring MVC — a {@code
   * spring-boot-starter-jersey} app, or a worker replica sharing the web app's {@code
   * application.yml} — must still boot. {@link SseController} was gated only on the property and on
   * the (unconditionally auto-configured) {@code SseEventPublisher}, so Spring created the bean and
   * introspected its class for lifecycle/autowire metadata and the inferred {@code close()} destroy
   * method: {@code stream()}'s return type {@code
   * org.springframework.web.servlet.mvc.method.annotation.SseEmitter} was unresolvable →
   * NoClassDefFoundError → "Failed to introspect Class [org.streamrune.spring.SseController]".
   * Quarkus and Micronaut cannot hit this — their SSE controller can only exist with its HTTP stack
   * present.
   */
  @Test
  void contextStartsWithSseEnabledButNoSpringMvcOnTheClasspath() {
    runner
        .withPropertyValues("streamrune.sse.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).doesNotHaveBean("streamRuneSseController");
              assertThat(ctx).doesNotHaveBean("sseDenyAllAuthorizer");
              // The publisher itself is stack-independent and stays available to application code.
              assertThat(ctx).hasSingleBean(org.streamrune.runtime.SseEventPublisher.class);
            });
  }

  @Configuration
  static class StubInfrastructure {

    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Bean
    EventStoreFactory eventStoreFactory() {
      return SpringTestMocks.eventStoreFactoryReturningMockStore();
    }
  }
}
