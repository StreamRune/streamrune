package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * Proves that the auto-configured {@link org.streamrune.integration.MicrometerStreamRuneMetrics} is
 * wired into the {@link VirtualThreadCommandBus} — dispatching a command must actually record
 * metrics.
 */
class MetricsWiringAutoConfigurationTest {

  sealed interface TestCommand extends Command permits TestCommand.DoIt {
    record DoIt(String id) implements TestCommand {}
  }

  record TestState() implements AggregateState {}

  sealed interface TestEvent extends DomainEvent permits TestEvent.ItHappened {
    record ItHappened() implements TestEvent {}
  }

  record EchoQuery(String value) implements org.streamrune.core.Query<String> {}

  // GLOBAL scope: this test asserts the cache HIT/MISS meters are wired, not user partitioning, and
  // it dispatches with no request context bound — a USER-scoped query would bypass the cache
  // entirely and never record a hit.
  @org.streamrune.core.Cacheable(
      ttlSeconds = 60,
      maxEntries = 10,
      scope = org.streamrune.core.Cacheable.Scope.GLOBAL)
  record CachedEchoQuery(String value) implements org.streamrune.core.Query<String> {}

  static class TestDecider implements Decider<TestCommand, TestState, TestEvent> {
    @Override
    public TestState initialState() {
      return new TestState();
    }

    @Override
    public List<TestEvent> decide(TestCommand command, TestState state) {
      return List.of(new TestEvent.ItHappened());
    }

    @Override
    public TestState evolve(TestState state, TestEvent event) {
      return state;
    }
  }

  private static EventStore stubbedEventStore() {
    EventStore eventStore = mock(EventStore.class);
    when(eventStore.load(any())).thenReturn(AggregateHistory.empty());
    when(eventStore.load(any(), anyInt())).thenReturn(AggregateHistory.empty());
    when(eventStore.append(any(), anyList(), any()))
        .thenReturn(new EventStore.AppendResult(List.of(GlobalOffset.of(1)), new Version(1)));
    return eventStore;
  }

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          // Mocked AuditStore so the audit interceptor does not hit the mock DataSource
          .withBean(
              org.streamrune.core.audit.AuditStore.class,
              () -> mock(org.streamrune.core.audit.AuditStore.class))
          .withBean(EventStore.class, MetricsWiringAutoConfigurationTest::stubbedEventStore)
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
          .withBean(MeterRegistry.class, SimpleMeterRegistry::new)
          .withBean(
              DeciderRegistration.class,
              () ->
                  new DeciderRegistration<>(
                      AggregateType.of("test"),
                      TestCommand.class,
                      cmd ->
                          switch (cmd) {
                            case TestCommand.DoIt d -> AggregateId.of(d.id());
                          },
                      new TestDecider()));

  @Test
  void commandDispatchRecordsMicrometerMetrics() {
    runner.run(
        ctx -> {
          var bus = ctx.getBean(VirtualThreadCommandBus.class);
          bus.execute(new TestCommand.DoIt("agg-1"));

          var registry = ctx.getBean(MeterRegistry.class);
          // commands.dispatched and events.appended carry a tag key on every series
          // (command.type / event.type, "unknown" on the no-arg path the bus uses), so they are
          // located with find(...) — the bare registry.counter(name) lookup would auto-vivify a
          // second, tag-less shape of the family and read 0.0.
          assertThat(registry.find("streamrune.commands.dispatched").counter().count())
              .isEqualTo(1.0);
          assertThat(registry.counter("streamrune.commands.succeeded").count()).isEqualTo(1.0);
          assertThat(registry.find("streamrune.events.appended").counter().count()).isEqualTo(1.0);
        });
  }

  @Test
  void metricsPrefixPropertyIsAppliedToMeterNames() {
    runner
        .withPropertyValues("streamrune.metrics.prefix=myapp.es")
        .run(
            ctx -> {
              var bus = ctx.getBean(VirtualThreadCommandBus.class);
              bus.execute(new TestCommand.DoIt("agg-prefixed"));

              var registry = ctx.getBean(MeterRegistry.class);
              assertThat(registry.find("myapp.es.commands.dispatched").counter().count())
                  .isEqualTo(1.0);
              assertThat(registry.find("streamrune.commands.dispatched").counter()).isNull();
            });
  }

  @Test
  void queryDispatchRecordsMicrometerMetrics() {
    // recordQueryDispatched(queryType) now tags the series with query.type instead of
    // silently discarding it into a bare aggregate counter (queries have no separate untagged SPI
    // entry point to fall back to, unlike commands/events) — assert on the query.type=EchoQuery
    // series SimpleQueryBus actually drives, not a bare lookup that no longer receives any sample.
    runner.run(
        ctx -> {
          org.streamrune.runtime.SimpleQueryBus bus =
              ctx.getBean(org.streamrune.runtime.SimpleQueryBus.class);
          bus.register(
              EchoQuery.class,
              (org.streamrune.core.QueryHandler<EchoQuery, String>) q -> q.value());
          String result = bus.dispatch(new EchoQuery("hi"));
          assertThat(result).isEqualTo("hi");

          var registry = ctx.getBean(MeterRegistry.class);
          assertThat(
                  registry
                      .find("streamrune.queries.dispatched")
                      .tag(org.streamrune.core.metrics.MetricNames.TAG_QUERY_TYPE, "EchoQuery")
                      .counter()
                      .count())
              .isEqualTo(1.0);
          assertThat(
                  registry
                      .find("streamrune.queries.duration")
                      .tag(org.streamrune.core.metrics.MetricNames.TAG_QUERY_TYPE, "EchoQuery")
                      .timer()
                      .count())
              .isEqualTo(1L);
        });
  }

  @Test
  void cachingQueryBusRecordsCacheMetrics() {
    // Companion case: same query.type-tagged contract for the cache hit/miss series.
    runner
        .withPropertyValues("streamrune.query-cache.enabled=true")
        .run(
            ctx -> {
              org.streamrune.runtime.CachingQueryBus bus =
                  ctx.getBean(org.streamrune.runtime.CachingQueryBus.class);
              bus.register(
                  CachedEchoQuery.class,
                  (org.streamrune.core.QueryHandler<CachedEchoQuery, String>) q -> q.value());
              bus.dispatch(new CachedEchoQuery("a")); // miss
              bus.dispatch(new CachedEchoQuery("a")); // hit

              var registry = ctx.getBean(MeterRegistry.class);
              assertThat(
                      registry
                          .find("streamrune.queries.cache.misses")
                          .tag(
                              org.streamrune.core.metrics.MetricNames.TAG_QUERY_TYPE,
                              "CachedEchoQuery")
                          .counter()
                          .count())
                  .isEqualTo(1.0);
              assertThat(
                      registry
                          .find("streamrune.queries.cache.hits")
                          .tag(
                              org.streamrune.core.metrics.MetricNames.TAG_QUERY_TYPE,
                              "CachedEchoQuery")
                          .counter()
                          .count())
                  .isEqualTo(1.0);
            });
  }

  @Test
  void userDefinedStreamRuneMetricsBeanWinsOverMicrometerAutoConfig() {
    var recorded = new java.util.concurrent.atomic.AtomicInteger();
    org.streamrune.core.StreamRuneMetrics custom =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordCommandDispatched() {
            recorded.incrementAndGet();
          }
        };
    runner
        .withBean(org.streamrune.core.StreamRuneMetrics.class, () -> custom)
        .run(
            ctx -> {
              assertThat(ctx)
                  .doesNotHaveBean(org.streamrune.integration.MicrometerStreamRuneMetrics.class);
              var bus = ctx.getBean(VirtualThreadCommandBus.class);
              bus.execute(new TestCommand.DoIt("agg-2"));
              assertThat(recorded.get()).isEqualTo(1);
            });
  }
}
