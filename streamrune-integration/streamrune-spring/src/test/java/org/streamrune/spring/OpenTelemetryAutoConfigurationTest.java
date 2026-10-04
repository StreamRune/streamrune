package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.testing.junit5.OpenTelemetryExtension;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.runtime.OpenTelemetryCommandInterceptor;

class OpenTelemetryAutoConfigurationTest {

  @RegisterExtension
  static final OpenTelemetryExtension otelTesting = OpenTelemetryExtension.create();

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withUserConfiguration(StubDataSourceConfig.class, StubEventStoreConfig.class);

  @Test
  void shouldRegisterCommandInterceptorWhenOpenTelemetryBeanPresent() {
    runner
        .withBean(OpenTelemetry.class, otelTesting::getOpenTelemetry)
        .run(ctx -> assertThat(ctx).hasSingleBean(OpenTelemetryCommandInterceptor.class));
  }

  @Test
  void shouldNotRegisterCommandInterceptorWhenOpenTelemetryBeanAbsent() {
    runner.run(ctx -> assertThat(ctx).doesNotHaveBean(OpenTelemetryCommandInterceptor.class));
  }

  @Test
  void shouldNotOverrideUserProvidedInterceptor() {
    runner
        .withBean(OpenTelemetry.class, otelTesting::getOpenTelemetry)
        .withBean(
            "openTelemetryCommandInterceptor",
            OpenTelemetryCommandInterceptor.class,
            () -> new OpenTelemetryCommandInterceptor(otelTesting.getOpenTelemetry()))
        .run(ctx -> assertThat(ctx).hasSingleBean(OpenTelemetryCommandInterceptor.class));
  }

  @Test
  void shouldBootWhenOpenTelemetryNotOnClasspath() {
    // Opentelemetry-api is compileOnly (classpath-optional). The OTel @Bean lives in a
    // nested
    // @Configuration gated by @ConditionalOnClass, so Spring never introspects its OTel-typed
    // method
    // signature when the OTel API is absent. Hide io.opentelemetry.api.OpenTelemetry from the
    // context
    // classloader and assert the context still boots (pre-fix: a NoClassDefFoundError on context
    // init as Spring reflected on the OTel-typed @Bean method), with no interceptor registered.
    runner
        .withClassLoader(new FilteredClassLoader(OpenTelemetry.class))
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @Configuration
  static class StubDataSourceConfig {
    @Bean
    DataSource dataSource() {
      return mock(DataSource.class);
    }
  }

  @Configuration
  static class StubEventStoreConfig {
    @Bean
    EventStoreFactory eventStoreFactory() {
      return () -> mock(EventStore.class);
    }
  }
}
