package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.postgres.PostgresEventStoreFactory;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * The auto-configured event store reads and writes through the application's {@link DataSource}
 * bean as given: the framework puts no pool of its own between them, so there is nothing of the
 * framework's to close with the context and the DataSource stays the application's to manage.
 */
@Testcontainers
class AutoConfiguredEventStoreDataSourceTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME).withDatabaseName("streamrune_ds_it");

  static PGSimpleDataSource dataSource;
  static final EventTypeRegistry TYPE_REGISTRY = SimpleEventTypeRegistry.builder().build();

  @BeforeAll
  static void initDataSource() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
  }

  @Test
  void theStoreKeepsWorkingOnTheApplicationDataSourceAfterTheContextCloses() {
    var streamId = StreamId.of(AggregateType.of("probe"), AggregateId.of("datasource-probe"));
    var storeFromClosedContext = new AtomicReference<EventStore>();

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(DataSource.class, () -> dataSource)
        .withBean(EventTypeRegistry.class, () -> TYPE_REGISTRY)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(EventStoreFactory.class))
                  .isInstanceOf(PostgresEventStoreFactory.class)
                  .isNotInstanceOf(AutoCloseable.class);
              EventStore store = ctx.getBean(EventStore.class);
              assertThat(store.load(streamId).events()).isEmpty();
              storeFromClosedContext.set(store);
            });

    // The context is closed. The store held nothing of its own, so it still reads through the
    // application's DataSource, which the application — not the framework — closes.
    assertThat(storeFromClosedContext.get().load(streamId).events()).isEmpty();
  }
}
