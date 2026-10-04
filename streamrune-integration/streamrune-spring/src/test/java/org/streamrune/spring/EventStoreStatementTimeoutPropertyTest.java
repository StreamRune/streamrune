package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.postgres.PostgresEventStoreFactory;

/**
 * {@code streamrune.event-store.statement-timeout} reaches the auto-configured {@link
 * PostgresEventStoreFactory}. {@link EventStore} is overridden so {@code factory.create()} never
 * touches the mock DataSource.
 */
class EventStoreStatementTimeoutPropertyTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class));

  @Test
  void theBoundDefaultsToThirtySeconds() {
    runner.run(
        ctx -> {
          assertThat(boundOf(ctx.getBean(EventStoreFactory.class)))
              .isEqualTo(Duration.ofSeconds(30));
          assertThat(ctx.getBean(StreamRuneProperties.class).eventStore().statementTimeout())
              .isEqualTo(Duration.ofSeconds(30));
        });
  }

  @Test
  void theConfiguredBoundReachesTheFactory() {
    runner
        .withPropertyValues("streamrune.event-store.statement-timeout=2m")
        .run(
            ctx ->
                assertThat(boundOf(ctx.getBean(EventStoreFactory.class)))
                    .isEqualTo(Duration.ofMinutes(2)));
  }

  @Test
  void zeroSetsNoBound() {
    runner
        .withPropertyValues("streamrune.event-store.statement-timeout=0")
        .run(
            ctx ->
                assertThat(boundOf(ctx.getBean(EventStoreFactory.class))).isEqualTo(Duration.ZERO));
  }

  @Test
  void aNegativeOrSubMillisecondBoundFailsTheBootNamingTheProperty() {
    for (String value : new String[] {"-1s", "500us"}) {
      runner
          .withPropertyValues("streamrune.event-store.statement-timeout=" + value)
          .run(
              ctx ->
                  assertThat(ctx)
                      .hasFailed()
                      .getFailure()
                      .rootCause()
                      .isInstanceOf(IllegalArgumentException.class)
                      .hasMessageContaining("statementTimeout"));
      runner
          .withPropertyValues("streamrune.event-store.statement-timeout=" + value)
          .run(
              ctx ->
                  assertThat(ctx.getStartupFailure())
                      .hasStackTraceContaining("streamrune.event-store.statement-timeout"));
    }
  }

  private static Object boundOf(EventStoreFactory factory) {
    assertThat(factory).isInstanceOf(PostgresEventStoreFactory.class);
    return ReflectionTestUtils.getField(factory, "statementTimeout");
  }
}
