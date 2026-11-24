package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.SimpleEventTypeRegistry;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.test.EventStoreFixture;

/**
 * An append inside an application transaction on the same SQLite file is not supported: the stores
 * never join an application's transaction. A Spring {@code @Transactional} method that has written
 * through the same DataSource holds SQLite's write lock, so the append waits busy_timeout and fails
 * with a message naming that cause; the application's own write commits when its method returns.
 */
class SpringSqliteApplicationTransactionTest {

  private static final StreamId STREAM = StreamId.of(AggregateType.of("ping"), AggregateId.of("s"));

  record Ping(int n) implements DomainEvent {}

  @TempDir Path dir;

  /** The application: a transaction manager on the DataSource and a transactional service. */
  @Configuration(proxyBeanMethods = false)
  @EnableTransactionManagement
  static class Application {
    @Bean
    PlatformTransactionManager transactionManager(DataSource dataSource) {
      return new DataSourceTransactionManager(dataSource);
    }

    @Bean
    EventTypeRegistry eventTypeRegistry() {
      return SimpleEventTypeRegistry.builder().registerEvent(Ping.class).build();
    }

    @Bean
    OrderService orderService(DataSource dataSource, EventStore eventStore) {
      return new OrderService(new JdbcTemplate(dataSource), eventStore);
    }
  }

  /** Writes a row of its own, then appends an event, in one {@code @Transactional} method. */
  public static class OrderService {
    private final JdbcTemplate jdbc;
    private final EventStore eventStore;

    public OrderService(JdbcTemplate jdbc, EventStore eventStore) {
      this.jdbc = jdbc;
      this.eventStore = eventStore;
    }

    /** The application's write, then the append, inside the application's transaction. */
    @Transactional
    public void placeOrder() {
      jdbc.update("INSERT INTO app_order (id) VALUES (1)");
      eventStore.append(
          STREAM,
          List.of(
              EventStoreFixture.event(STREAM, new Version(1), new EventType("Ping"), new Ping(1))),
          new Version(0));
    }
  }

  @Test
  void anAppendInsideATransactionalMethodOnTheSameFileFailsNamingTheCause() {
    DataSource dataSource = SqliteDataSources.file(dir.resolve("app.db"), 200);
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS, "streamrune.backend=sqlite")
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(Application.class)
        .withBean(DataSource.class, () -> dataSource)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              new JdbcTemplate(dataSource).execute("CREATE TABLE app_order (id INTEGER)");

              assertThatThrownBy(() -> ctx.getBean(OrderService.class).placeOrder())
                  .hasMessageContaining(
                      "SQLite's write lock was not granted within the connection's busy_timeout")
                  .hasMessageContaining(
                      "an application transaction on the same database file holds the write lock");

              // The application's transaction rolled back on the exception, and the append stored
              // nothing.
              assertThat(
                      new JdbcTemplate(dataSource)
                          .queryForObject("SELECT count(*) FROM app_order", Integer.class))
                  .isZero();
              assertThat(ctx.getBean(EventStore.class).load(STREAM).events()).isEmpty();
            });
  }

  @Test
  void theSameAppendOutsideTheApplicationTransactionIsStored() {
    DataSource dataSource = SqliteDataSources.file(dir.resolve("app.db"), 200);
    new ApplicationContextRunner()
        .withPropertyValues(SpringTestMocks.NO_SNAPSHOTS, "streamrune.backend=sqlite")
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withUserConfiguration(Application.class)
        .withBean(DataSource.class, () -> dataSource)
        .run(
            ctx -> {
              ctx.getBean(EventStore.class)
                  .append(
                      STREAM,
                      List.of(
                          EventStoreFixture.event(
                              STREAM, new Version(1), new EventType("Ping"), new Ping(1))),
                      new Version(0));
              assertThat(ctx.getBean(EventStore.class).load(STREAM).events()).hasSize(1);
            });
  }
}
