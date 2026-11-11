package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.SubscriptionName;
import org.streamrune.core.types.Version;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.InMemoryOffsetStore;

/**
 * The hybrid subscription's {@code tracing(...)}: each delivery of the polling subscription inside
 * is traced as {@code subscription/{name}/deliver}. Polling only, so no database is needed.
 */
class HybridEventSubscriptionTracingTest {

  record Seen() implements DomainEvent {}

  @Test
  void eachDelivery_isTracedAsTheSubscription() throws Exception {
    StreamId stream = StreamId.of(AggregateType.of("lamp"), AggregateId.of("l-1"));
    var store = new InMemoryEventStore();
    store.append(stream, List.of(envelope(stream)), Version.initial());
    var tracing = new RecordingTracing();
    var received = new CopyOnWriteArrayList<EventEnvelope>();

    var sub =
        HybridEventSubscription.builder()
            .subscriptionName(SubscriptionName.of("lamps"))
            .dataSource(mock(DataSource.class))
            .eventStore(store)
            .offsetStore(new InMemoryOffsetStore())
            .listener(received::addAll)
            .config(SubscriptionConfig.pollingOnly(Duration.ofMillis(20)))
            .tracing(tracing)
            .build();
    sub.start();
    try (sub) {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (received.isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
    }

    assertThat(received).hasSize(1);
    assertThat(tracing.started).containsExactly("subscription/lamps/deliver");
  }

  private static EventEnvelope envelope(StreamId stream) {
    return new EventEnvelope(
        GlobalOffset.of(1),
        stream,
        new Version(1),
        new EventType("Seen"),
        new Seen(),
        new EventMetadata(
            EventId.of("evt_1"),
            CommandId.of("cmd_1"),
            null,
            null,
            CorrelationId.of("corr"),
            null,
            null,
            Instant.now()));
  }
}
