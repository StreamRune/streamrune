package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.streamrune.core.Cacheable;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.Query;
import org.streamrune.core.QueryAuthorizer;
import org.streamrune.core.QueryBus;
import org.streamrune.runtime.CacheInvalidator;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.SimpleQueryBus;

/** Tests for query-cache auto-configuration in {@link StreamRuneAutoConfiguration}. */
class SpringQueryCacheAutoConfigTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withUserConfiguration(StubEventStoreConfig.class);

  @Test
  void queryCache_disabled_byDefault_noCachingBus() {
    runner.run(
        ctx -> {
          assertThat(ctx).doesNotHaveBean(CachingQueryBus.class);
        });
  }

  @Test
  void queryCache_enabled_providesCachingBus() {
    runner
        .withPropertyValues("streamrune.query-cache.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasBean("cachingQueryBus");
              // Primary composed QueryBus delegates to CachingQueryBus when caching is enabled
              assertThat(ctx.getBean("queryBus", QueryBus.class))
                  .isInstanceOf(CachingQueryBus.class);
            });
  }

  @Test
  void queryCache_enabled_providesCacheInvalidator() {
    runner
        .withPropertyValues("streamrune.query-cache.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(CacheInvalidator.class);
            });
  }

  @Test
  void queryCache_disabled_noCacheInvalidator() {
    runner.run(
        ctx -> {
          assertThat(ctx).doesNotHaveBean(CacheInvalidator.class);
        });
  }

  @Test
  void queryCache_userOverride_suppressesAutoConfig() {
    runner
        .withPropertyValues("streamrune.query-cache.enabled=true")
        .withUserConfiguration(UserCachingQueryBusConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasBean("cachingQueryBus");
              // user-provided CachingQueryBus is the only CachingQueryBus bean
              assertThat(ctx.getBeansOfType(CachingQueryBus.class)).containsKey("cachingQueryBus");
              // composed queryBus delegates to the user-provided CachingQueryBus
              assertThat(ctx.getBean("queryBus", QueryBus.class))
                  .isSameAs(ctx.getBean("cachingQueryBus"));
            });
  }

  @Test
  void queryCache_userInvalidatorOverride_suppressesAutoConfig() {
    runner
        .withPropertyValues("streamrune.query-cache.enabled=true")
        .withUserConfiguration(UserCacheInvalidatorConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasSingleBean(CacheInvalidator.class);
              assertThat(ctx.getBean(CacheInvalidator.class))
                  .isSameAs(ctx.getBean("userCacheInvalidator"));
            });
  }

  @Cacheable(scope = Cacheable.Scope.GLOBAL)
  record TestCachedQuery(String id) implements Query<String> {}

  static final class RecordingAuthorizer implements QueryAuthorizer {
    final AtomicInteger calls = new AtomicInteger();

    @Override
    public void authorize(Query<?> query, org.streamrune.core.types.UserId user) {
      calls.incrementAndGet();
    }
  }

  @Configuration
  static class RecordingAuthorizerConfig {
    @Bean
    RecordingAuthorizer queryAuthorizer() {
      return new RecordingAuthorizer();
    }
  }

  /**
   * An application-supplied {@link QueryAuthorizer} bean is auto-detected and wired into the
   * auto-configured {@link CachingQueryBus}, consulted on EVERY dispatch — including a cache HIT,
   * which never calls the delegate.
   */
  @Test
  void queryCache_enabled_authorizerBeanPresent_consultedOnAHit() {
    runner
        .withPropertyValues("streamrune.query-cache.enabled=true")
        .withUserConfiguration(RecordingAuthorizerConfig.class)
        .run(
            ctx -> {
              CachingQueryBus bus = ctx.getBean("cachingQueryBus", CachingQueryBus.class);
              bus.register(TestCachedQuery.class, q -> "answer");

              bus.dispatch(new TestCachedQuery("q1")); // MISS
              bus.dispatch(new TestCachedQuery("q1")); // HIT

              RecordingAuthorizer authorizer = ctx.getBean(RecordingAuthorizer.class);
              assertThat(authorizer.calls.get())
                  .as("the authorizer must run on the hit too, not just the miss")
                  .isEqualTo(2);
            });
  }

  @Configuration
  static class TwoAuthorizersConfig {
    @Bean
    QueryAuthorizer authorizerOne() {
      return new RecordingAuthorizer();
    }

    @Bean
    QueryAuthorizer authorizerTwo() {
      return new RecordingAuthorizer();
    }
  }

  /**
   * Sibling-parity check: Spring's {@code ObjectProvider.ifAvailable} already throws {@code
   * NoUniqueBeanDefinitionException} on an ambiguous {@link QueryAuthorizer} — ambiguity fails the
   * context, unlike the Quarkus bug that was fixed (which silently built an unauthorized caching
   * bus instead). Pinning here so a future refactor cannot regress Spring onto the same fail-open
   * behavior without a test noticing.
   */
  @Test
  void queryCache_enabled_ambiguousAuthorizer_contextFails() {
    runner
        .withPropertyValues("streamrune.query-cache.enabled=true")
        .withUserConfiguration(TwoAuthorizersConfig.class)
        .run(ctx -> assertThat(ctx).hasFailed());
  }

  @Test
  void queryCache_disabled_simpleQueryBusStillPresent() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasBean("simpleQueryBus");
          assertThat(ctx).hasBean("queryBus");
          // composed queryBus delegates to SimpleQueryBus when caching is disabled
          assertThat(ctx.getBean("queryBus", QueryBus.class)).isInstanceOf(SimpleQueryBus.class);
        });
  }

  // -------------------------------------------------------------------------
  // Stub configurations
  // -------------------------------------------------------------------------

  @Configuration
  static class StubEventStoreConfig {
    @Bean
    EventStoreFactory eventStoreFactory() {
      return () -> mock(EventStore.class);
    }
  }

  @Configuration
  static class UserCachingQueryBusConfig {
    @Bean
    CachingQueryBus cachingQueryBus() {
      return CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    }
  }

  @Configuration
  static class UserCacheInvalidatorConfig {
    @Bean
    CacheInvalidator userCacheInvalidator() {
      return batch -> {};
    }
  }
}
