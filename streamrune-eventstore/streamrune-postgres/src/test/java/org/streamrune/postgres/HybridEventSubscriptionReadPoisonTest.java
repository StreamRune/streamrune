package org.streamrune.postgres;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateHistory;
import org.streamrune.core.AggregateState;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.ReadPoisonAware;

/**
 * The documented factory/Hybrid production path must be poison-bounded. A {@link
 * HybridEventSubscription} built with a live read-poison bound must stop itself terminally on a
 * deterministic read-poison and surface it via {@link ReadPoisonAware#readPoisonError()}, so the
 * owning projection runner HALTs instead of the resilient poll loop retrying an unreadable event
 * forever (which previously wedged the projection because the bound was never propagated here).
 */
class HybridEventSubscriptionReadPoisonTest {

  @Test
  void livePoisonWithBoundStopsSubscriptionAndSurfacesReadPoison() throws Exception {
    // A deterministic read/deserialization poison (UncheckedIOException → POISON per
    // ReadPoisonClassifier) thrown on every read: it can never be read on retry.
    EventStore poisonStore = new PoisonReadEventStore();
    OffsetStore offsetStore = new AtInitialOffsetStore();

    // The LISTEN/NOTIFY push path is irrelevant to this test — mock a DataSource whose connection
    // attempts fail, so the notification listener just retries (harmless) while the polling path
    // (the delivery owner) hits the poison.
    DataSource ds = mock(DataSource.class);
    when(ds.getConnection()).thenThrow(new SQLException("no db in this unit test"));

    var hybrid =
        HybridEventSubscription.builder()
            .subscriptionName("hybrid-read-poison")
            .dataSource(ds)
            .eventStore(poisonStore)
            .offsetStore(offsetStore)
            .listener(events -> {})
            .config(new SubscriptionConfig(false, Duration.ofMillis(20), Duration.ofMillis(5)))
            .fetchSize(10)
            .readPoisonBound(2)
            .build();

    assertInstanceOf(
        ReadPoisonAware.class, hybrid, "HybridEventSubscription must be ReadPoisonAware");

    hybrid.start();
    try {
      // After the bound (2) consecutive poison reads at the same checkpoint, the internal polling
      // subscription stops terminally, so the Hybrid reports not-running.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (hybrid.isRunning() && System.nanoTime() < deadline) {
        Thread.sleep(20);
      }
      assertFalse(
          hybrid.isRunning(),
          "a bounded live read-poison must stop the Hybrid subscription, not wedge forever");
      assertNotNull(
          hybrid.readPoisonError(),
          "the Hybrid subscription must surface the terminal read-poison via readPoisonError()");
    } finally {
      hybrid.close();
    }
  }

  /** Event store whose every global read fails with a deterministic read-poison. */
  private static final class PoisonReadEventStore implements EventStore {
    @Override
    public List<EventEnvelope> readGlobalStream(GlobalOffset afterOffset, int maxCount) {
      throw new UncheckedIOException(new IOException("corrupt/schema-incompatible payload"));
    }

    @Override
    public AggregateHistory load(StreamId streamId) {
      throw new UnsupportedOperationException();
    }

    @Override
    public AppendResult append(StreamId streamId, List<EventEnvelope> events, Version expected) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void saveSnapshot(StreamId streamId, Version version, AggregateState state) {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<EventEnvelope> readStream(StreamId streamId, Version afterVersion, int maxCount) {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Minimal offset store pinned at the initial offset (reads always fail, so it never advances).
   */
  private static final class AtInitialOffsetStore implements OffsetStore {
    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      return GlobalOffset.initial();
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      // no-op: the poison read never advances the checkpoint
    }
  }
}
