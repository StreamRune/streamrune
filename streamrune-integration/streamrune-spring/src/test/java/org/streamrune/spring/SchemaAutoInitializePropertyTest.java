package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.test.util.ReflectionTestUtils;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.postgres.PostgresEventStoreFactory;

/**
 * Proves that {@code streamrune.event-store.schema.auto-initialize} is honoured by the
 * auto-configured {@link PostgresEventStoreFactory}.
 *
 * <p>The real {@link PostgresEventStoreFactory} bean is built (no EventStoreFactory override) so
 * the field value can be inspected. {@link EventStore} IS overridden so {@code factory.create()}
 * never runs and Flyway / schema validation never touch the mock DataSource.
 */
class SchemaAutoInitializePropertyTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class));

  @Test
  void autoInitializeTrueByDefault() {
    runner.run(
        ctx -> {
          var factory = ctx.getBean(EventStoreFactory.class);
          assertThat(factory).isInstanceOf(PostgresEventStoreFactory.class);
          assertThat(ReflectionTestUtils.getField(factory, "autoInitializeSchema"))
              .as("autoInitializeSchema should default to true")
              .isEqualTo(true);
          assertThat(ctx.getBean(StreamRuneProperties.class).eventStore().schema().autoInitialize())
              .as("StreamRuneProperties should reflect default true")
              .isTrue();
        });
  }

  @Test
  void autoInitializeFalseWhenPropertySetToFalse() {
    runner
        .withPropertyValues("streamrune.event-store.schema.auto-initialize=false")
        .run(
            ctx -> {
              var factory = ctx.getBean(EventStoreFactory.class);
              assertThat(factory).isInstanceOf(PostgresEventStoreFactory.class);
              assertThat(ReflectionTestUtils.getField(factory, "autoInitializeSchema"))
                  .as(
                      "autoInitializeSchema should be false when"
                          + " streamrune.event-store.schema.auto-initialize=false")
                  .isEqualTo(false);
              assertThat(
                      ctx.getBean(StreamRuneProperties.class)
                          .eventStore()
                          .schema()
                          .autoInitialize())
                  .as("StreamRuneProperties should reflect false")
                  .isFalse();
            });
  }

  @Test
  void autoInitializeTrueWhenPropertyExplicitlySetToTrue() {
    runner
        .withPropertyValues("streamrune.event-store.schema.auto-initialize=true")
        .run(
            ctx -> {
              var factory = ctx.getBean(EventStoreFactory.class);
              assertThat(factory).isInstanceOf(PostgresEventStoreFactory.class);
              assertThat(ReflectionTestUtils.getField(factory, "autoInitializeSchema"))
                  .as(
                      "autoInitializeSchema should be true when"
                          + " streamrune.event-store.schema.auto-initialize=true")
                  .isEqualTo(true);
              assertThat(
                      ctx.getBean(StreamRuneProperties.class)
                          .eventStore()
                          .schema()
                          .autoInitialize())
                  .as("StreamRuneProperties should reflect explicit true")
                  .isTrue();
            });
  }
}
