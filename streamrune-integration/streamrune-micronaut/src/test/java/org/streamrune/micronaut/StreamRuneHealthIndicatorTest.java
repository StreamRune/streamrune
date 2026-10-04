package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.HealthResult;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.runtime.SubscriptionHealthContributor;
import reactor.core.publisher.Mono;

class StreamRuneHealthIndicatorTest {

  @Test
  void shouldReturnUpWhenDatabaseIsAvailable() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    var indicator = new StreamRuneHealthIndicator(ds, null);
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.UP, result.getStatus());
  }

  @Test
  void shouldReturnDownWhenConnectionValidationTimesOut() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(false);

    var indicator = new StreamRuneHealthIndicator(ds, null);
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.DOWN, result.getStatus());
  }

  @Test
  void shouldReturnDownWhenDatabaseFails() throws SQLException {
    DataSource ds = mock(DataSource.class);
    when(ds.getConnection()).thenThrow(new SQLException("Refused"));

    var indicator = new StreamRuneHealthIndicator(ds, null);
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.DOWN, result.getStatus());
    assertNotNull(result.getDetails(), "Expected error details");
  }

  @Test
  void shouldIncludeEventStoreDetailsWhenEventStorePresent() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenReturn(GlobalOffset.of(42L));

    var indicator = new StreamRuneHealthIndicator(ds, null, eventStore);
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.UP, result.getStatus());
    assertNotNull(result.getDetails(), "Expected event store details");
    @SuppressWarnings("unchecked")
    Map<String, Object> details = (Map<String, Object>) result.getDetails();
    assertEquals(42L, details.get("eventStore.lastGlobalOffset"));
  }

  @Test
  void shouldHandleEmptyEventStream() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenReturn(GlobalOffset.of(0L));

    var indicator = new StreamRuneHealthIndicator(ds, null, eventStore);
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.UP, result.getStatus());
    assertNotNull(result.getDetails());
    @SuppressWarnings("unchecked")
    Map<String, Object> details = (Map<String, Object>) result.getDetails();
    assertEquals(0L, details.get("eventStore.lastGlobalOffset"));
  }

  @Test
  void shouldHandleEventStoreError() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenThrow(new RuntimeException("store error"));

    var indicator = new StreamRuneHealthIndicator(ds, null, eventStore);
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.UP, result.getStatus());
    assertNotNull(result.getDetails());
    @SuppressWarnings("unchecked")
    Map<String, Object> details = (Map<String, Object>) result.getDetails();
    assertTrue(details.containsKey("eventStore.error"));
  }

  @Test
  void shouldFallBackToZeroWhenLastGlobalOffsetUnsupported() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenThrow(new UnsupportedOperationException("no head"));

    var indicator = new StreamRuneHealthIndicator(ds, null, eventStore);
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.UP, result.getStatus());
    assertNotNull(result.getDetails());
    @SuppressWarnings("unchecked")
    Map<String, Object> details = (Map<String, Object>) result.getDetails();
    assertEquals(0L, details.get("eventStore.lastGlobalOffset"));
  }

  @Test
  void shouldReportSubscriptionHealthUp() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    SubscriptionHealthContributor contributor = mock(SubscriptionHealthContributor.class);
    var sub =
        new org.streamrune.core.subscription.SubscriptionHealth(
            "orders",
            org.streamrune.core.subscription.SubscriptionLifecycleState.RUNNING,
            5L,
            0,
            org.streamrune.core.subscription.SubscriptionHealth.Status.UP);
    when(contributor.health()).thenReturn(List.of(sub));
    when(contributor.overallStatus())
        .thenReturn(org.streamrune.core.subscription.SubscriptionHealth.Status.UP);

    var indicator = new StreamRuneHealthIndicator(ds, contributor);
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.UP, result.getStatus());
    assertNotNull(result.getDetails());
    @SuppressWarnings("unchecked")
    Map<String, Object> details = (Map<String, Object>) result.getDetails();
    assertTrue(details.containsKey("subscription.orders"));
  }

  @Test
  void shouldReportSubscriptionHealthDown() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    SubscriptionHealthContributor contributor = mock(SubscriptionHealthContributor.class);
    var sub =
        new org.streamrune.core.subscription.SubscriptionHealth(
            "orders",
            org.streamrune.core.subscription.SubscriptionLifecycleState.STOPPED,
            0L,
            0,
            org.streamrune.core.subscription.SubscriptionHealth.Status.DOWN);
    when(contributor.health()).thenReturn(List.of(sub));
    when(contributor.overallStatus())
        .thenReturn(org.streamrune.core.subscription.SubscriptionHealth.Status.DOWN);

    var indicator = new StreamRuneHealthIndicator(ds, contributor);
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.DOWN, result.getStatus());
  }

  private static DataSource validDataSource() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);
    return ds;
  }

  /**
   * A real contributor over a store whose head is {@code head} and a shared checkpoint at {@code
   * checkpoint} for the {@code orders} subscription, which reports RUNNING.
   */
  private static SubscriptionHealthContributor contributorWithOrdersAt(long head, long checkpoint) {
    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenReturn(GlobalOffset.of(head));
    OffsetStore offsetStore = mock(OffsetStore.class);
    when(offsetStore.getLastOffset(ProjectionName.of("orders")))
        .thenReturn(GlobalOffset.of(checkpoint));
    SubscriptionLifecycle orders = mock(SubscriptionLifecycle.class);
    when(orders.state()).thenReturn(SubscriptionLifecycleState.RUNNING);
    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore);
    contributor.register("orders", orders);
    return contributor;
  }

  @Test
  @SuppressWarnings("unchecked")
  void aSubscriptionFarBehindTheHeadKeepsTheCheckUp_withItsLagInTheDetails() throws SQLException {
    // Micronaut's /health/readiness runs every indicator not annotated @Liveness, this one
    // included. A projection replaying the store from offset 0 (new, or reset for a rebuild) is
    // behind on every replica alike, because lag is measured against the shared checkpoint — a
    // lag-driven DOWN would take every replica out of service at once. Lag is reported as a
    // DEGRADED subscription detail (and through the lag gauge) instead.
    var indicator =
        new StreamRuneHealthIndicator(validDataSource(), contributorWithOrdersAt(50_000, 0));
    HealthResult result = Mono.from(indicator.getResult()).block();

    assertNotNull(result);
    assertEquals(HealthStatus.UP, result.getStatus());
    var details = (Map<String, Object>) result.getDetails();
    var orders = (Map<String, Object>) details.get("subscription.orders");
    assertEquals(50_000L, orders.get("lag"));
    assertEquals("DEGRADED", orders.get("status"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void aTerminallyHaltedSubscriptionTurnsTheCheckDown() throws SQLException {
    var contributor = contributorWithOrdersAt(10, 10);
    contributor.markTerminalError("orders");

    HealthResult result =
        Mono.from(new StreamRuneHealthIndicator(validDataSource(), contributor).getResult())
            .block();

    assertNotNull(result);
    assertEquals(HealthStatus.DOWN, result.getStatus());
    var details = (Map<String, Object>) result.getDetails();
    assertEquals("DOWN", ((Map<String, Object>) details.get("subscription.orders")).get("status"));
  }

  @Test
  void theOverallStatusFollowsTheSubscriptionDetailsItReports() throws SQLException {
    // The contributor computes health afresh on every call; the indicator reads it once, so it can
    // never be UP while its own details show a DOWN subscription (or the reverse).
    SubscriptionHealthContributor contributor = mock(SubscriptionHealthContributor.class);
    when(contributor.health())
        .thenReturn(
            List.of(
                new org.streamrune.core.subscription.SubscriptionHealth(
                    "orders",
                    SubscriptionLifecycleState.STOPPED,
                    0L,
                    0,
                    org.streamrune.core.subscription.SubscriptionHealth.Status.DOWN)));
    when(contributor.overallStatus())
        .thenReturn(org.streamrune.core.subscription.SubscriptionHealth.Status.UP);

    HealthResult result =
        Mono.from(new StreamRuneHealthIndicator(validDataSource(), contributor).getResult())
            .block();

    assertNotNull(result);
    assertEquals(HealthStatus.DOWN, result.getStatus());
    verify(contributor, times(1)).health();
  }
}
