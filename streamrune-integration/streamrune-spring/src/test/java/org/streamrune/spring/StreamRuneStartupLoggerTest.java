package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.crypto.CryptoEngine;

class StreamRuneStartupLoggerTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withUserConfiguration(StubDataSourceConfig.class, StubEventStoreConfig.class);

  @Test
  void startupLoggerBeanRegisteredByDefault() {
    runner.run(ctx -> assertThat(ctx).hasSingleBean(StreamRuneStartupLogger.class));
  }

  @Test
  void startupLoggerNotRegisteredWhenDisabled() {
    runner
        .withPropertyValues("streamrune.startup-log=false")
        .run(ctx -> assertThat(ctx).doesNotHaveBean(StreamRuneStartupLogger.class));
  }

  /**
   * The crypto probe counts CryptoEngine bean definitions instead of fetching the bean. {@code
   * getBean(CryptoEngine.class)} threw on two engines (and instantiated the bean just to log a
   * line), and the swallowed exception reported "Crypto: none" for an application that has
   * encryption configured.
   */
  @Test
  void reportsCryptoActiveWhenMoreThanOneCryptoEngineIsDefined() {
    try (var ctx = new org.springframework.context.support.GenericApplicationContext()) {
      ctx.registerBean("engineA", CryptoEngine.class, () -> mock(CryptoEngine.class));
      ctx.registerBean("engineB", CryptoEngine.class, () -> mock(CryptoEngine.class));
      ctx.refresh();

      assertThat(startupLinesFor(ctx)).singleElement().asString().contains("Crypto: active");
    }
  }

  @Test
  void reportsCryptoNoneWhenNoCryptoEngineIsDefined() {
    try (var ctx = new org.springframework.context.support.GenericApplicationContext()) {
      ctx.refresh();

      assertThat(startupLinesFor(ctx)).singleElement().asString().contains("Crypto: none");
    }
  }

  private static List<String> startupLinesFor(ApplicationContext ctx) {
    var logger =
        (ch.qos.logback.classic.Logger)
            org.slf4j.LoggerFactory.getLogger(StreamRuneStartupLogger.class);
    var originalLevel = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    try {
      new StreamRuneStartupLogger(ctx).afterSingletonsInstantiated();
    } finally {
      logger.detachAppender(appender);
      appender.stop();
      logger.setLevel(originalLevel);
    }
    return appender.list.stream()
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();
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
