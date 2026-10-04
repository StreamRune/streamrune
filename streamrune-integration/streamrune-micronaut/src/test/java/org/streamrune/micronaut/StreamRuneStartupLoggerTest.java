package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.event.StartupEvent;
import java.util.Map;
import org.junit.jupiter.api.Test;

class StreamRuneStartupLoggerTest {

  @Test
  void startupLoggerBeanRegisteredByDefault() {
    try (var ctx = ApplicationContext.run()) {
      assertTrue(ctx.containsBean(StreamRuneStartupLogger.class));
    }
  }

  @Test
  void startupLoggerNotRegisteredWhenDisabled() {
    try (var ctx = ApplicationContext.run(Map.of("streamrune.startup-log", "false"))) {
      assertFalse(ctx.containsBean(StreamRuneStartupLogger.class));
    }
  }

  @Test
  void logsSummaryWithoutInstantiatingBeans() {
    try (var ctx = ApplicationContext.run()) {
      var logger = ctx.getBean(StreamRuneStartupLogger.class);
      assertDoesNotThrow(() -> logger.onApplicationEvent(new StartupEvent(ctx)));
    }
  }
}
