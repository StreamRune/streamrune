package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

import jakarta.enterprise.inject.Instance;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.eclipse.microprofile.health.HealthCheckResponse;
import org.junit.jupiter.api.Test;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SubscriptionHealthContributor;

class StreamRuneHealthCheckTest {

  @SuppressWarnings("unchecked")
  private Instance<SubscriptionHealthContributor> noHealthContributor() {
    Instance<SubscriptionHealthContributor> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    return instance;
  }

  @SuppressWarnings("unchecked")
  private Instance<EventStore> noEventStore() {
    Instance<EventStore> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    return instance;
  }

  @SuppressWarnings("unchecked")
  private Instance<BackgroundRelayHealthContributor> noRelay() {
    Instance<BackgroundRelayHealthContributor> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    return instance;
  }

  @SuppressWarnings("unchecked")
  private Instance<DataSource> dataSource(DataSource ds) {
    Instance<DataSource> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(false);
    when(instance.get()).thenReturn(ds);
    return instance;
  }

  @SuppressWarnings("unchecked")
  private Instance<DataSource> noDataSource() {
    Instance<DataSource> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(true);
    return instance;
  }

  @Test
  void shouldReturnUpAndSkipDatabaseCheckWhenNoDataSourceBean() {
    var check =
        new StreamRuneHealthCheck(noDataSource(), noHealthContributor(), noEventStore(), noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
    assertTrue(response.getData().isPresent());
    assertTrue(response.getData().get().containsKey("database"));
  }

  @Test
  void shouldReturnDownWhenDataSourceThrowsRuntimeException() throws SQLException {
    // e.g. an Agroal datasource bean that exists but was never activated by configuration
    DataSource ds = mock(DataSource.class);
    when(ds.getConnection()).thenThrow(new IllegalStateException("Datasource not activated"));

    var check =
        new StreamRuneHealthCheck(dataSource(ds), noHealthContributor(), noEventStore(), noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.DOWN, response.getStatus());
    assertTrue(response.getData().isPresent());
    assertTrue(response.getData().get().containsKey("error"));
  }

  @Test
  void shouldReturnUpWhenDatabaseIsAvailable() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    var check =
        new StreamRuneHealthCheck(dataSource(ds), noHealthContributor(), noEventStore(), noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
  }

  @Test
  void shouldReturnDownWhenDatabaseFails() throws SQLException {
    DataSource ds = mock(DataSource.class);
    when(ds.getConnection()).thenThrow(new SQLException("Timeout"));

    var check =
        new StreamRuneHealthCheck(dataSource(ds), noHealthContributor(), noEventStore(), noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.DOWN, response.getStatus());
    assertTrue(response.getData().isPresent());
    assertTrue(response.getData().get().containsKey("error"));
  }

  @Test
  void shouldReturnDownWhenConnectionIsInvalid() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(false);

    var check =
        new StreamRuneHealthCheck(dataSource(ds), noHealthContributor(), noEventStore(), noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.DOWN, response.getStatus());
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldIncludeEventStoreDetailsWhenEventStorePresent() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenReturn(GlobalOffset.of(42L));

    Instance<EventStore> eventStoreInstance = mock(Instance.class);
    when(eventStoreInstance.isUnsatisfied()).thenReturn(false);
    when(eventStoreInstance.get()).thenReturn(eventStore);

    var check =
        new StreamRuneHealthCheck(
            dataSource(ds), noHealthContributor(), eventStoreInstance, noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
    assertTrue(response.getData().isPresent());
    assertEquals(42L, response.getData().get().get("eventStore.lastGlobalOffset"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldHandleEmptyEventStream() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenReturn(GlobalOffset.of(0L));

    Instance<EventStore> eventStoreInstance = mock(Instance.class);
    when(eventStoreInstance.isUnsatisfied()).thenReturn(false);
    when(eventStoreInstance.get()).thenReturn(eventStore);

    var check =
        new StreamRuneHealthCheck(
            dataSource(ds), noHealthContributor(), eventStoreInstance, noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
    assertTrue(response.getData().isPresent());
    assertEquals(0L, response.getData().get().get("eventStore.lastGlobalOffset"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldHandleEventStoreError() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenThrow(new RuntimeException("store down"));

    Instance<EventStore> eventStoreInstance = mock(Instance.class);
    when(eventStoreInstance.isUnsatisfied()).thenReturn(false);
    when(eventStoreInstance.get()).thenReturn(eventStore);

    var check =
        new StreamRuneHealthCheck(
            dataSource(ds), noHealthContributor(), eventStoreInstance, noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
    assertTrue(response.getData().isPresent());
    assertTrue(response.getData().get().containsKey("eventStore.error"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void shouldFallBackToZeroWhenLastGlobalOffsetUnsupported() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);

    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenThrow(new UnsupportedOperationException("no head"));

    Instance<EventStore> eventStoreInstance = mock(Instance.class);
    when(eventStoreInstance.isUnsatisfied()).thenReturn(false);
    when(eventStoreInstance.get()).thenReturn(eventStore);

    var check =
        new StreamRuneHealthCheck(
            dataSource(ds), noHealthContributor(), eventStoreInstance, noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
    assertTrue(response.getData().isPresent());
    assertEquals(0L, response.getData().get().get("eventStore.lastGlobalOffset"));
  }

  @Test
  @SuppressWarnings("unchecked")
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

    Instance<SubscriptionHealthContributor> healthInstance = mock(Instance.class);
    when(healthInstance.isUnsatisfied()).thenReturn(false);
    when(healthInstance.get()).thenReturn(contributor);

    var check =
        new StreamRuneHealthCheck(dataSource(ds), healthInstance, noEventStore(), noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
    assertTrue(response.getData().isPresent());
    assertTrue(response.getData().get().containsKey("subscription.orders.state"));
    assertTrue(response.getData().get().containsKey("subscription.orders.lag"));
  }

  @Test
  @SuppressWarnings("unchecked")
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

    Instance<SubscriptionHealthContributor> healthInstance = mock(Instance.class);
    when(healthInstance.isUnsatisfied()).thenReturn(false);
    when(healthInstance.get()).thenReturn(contributor);

    var check =
        new StreamRuneHealthCheck(dataSource(ds), healthInstance, noEventStore(), noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.DOWN, response.getStatus());
  }

  @Test
  @SuppressWarnings("unchecked")
  void theOverallStatusFollowsTheSubscriptionDetailsItReports() throws SQLException {
    // The contributor computes health afresh on every call: a subscription that goes DOWN between
    // two calls must not yield an UP check whose own details show a DOWN subscription.
    SubscriptionHealthContributor contributor = mock(SubscriptionHealthContributor.class);
    var down =
        new org.streamrune.core.subscription.SubscriptionHealth(
            "orders",
            org.streamrune.core.subscription.SubscriptionLifecycleState.STOPPED,
            0L,
            0,
            org.streamrune.core.subscription.SubscriptionHealth.Status.DOWN);
    when(contributor.health()).thenReturn(List.of(down));
    when(contributor.overallStatus())
        .thenReturn(org.streamrune.core.subscription.SubscriptionHealth.Status.UP);
    Instance<SubscriptionHealthContributor> healthInstance = mock(Instance.class);
    when(healthInstance.isUnsatisfied()).thenReturn(false);
    when(healthInstance.get()).thenReturn(contributor);

    var check =
        new StreamRuneHealthCheck(
            dataSource(validDataSource()), healthInstance, noEventStore(), noRelay());
    HealthCheckResponse response = check.call();

    assertEquals("DOWN", response.getData().get().get("subscription.orders.status"));
    assertEquals(HealthCheckResponse.Status.DOWN, response.getStatus());
  }

  @Test
  @SuppressWarnings("unchecked")
  void theOverallStatusFollowsTheRelayDetailsItReports() throws SQLException {
    // Same single-snapshot rule for the background relays: a relay thread that dies between two
    // calls must not yield an UP check whose own details show a DOWN relay.
    var contributor =
        new BackgroundRelayHealthContributor() {
          @Override
          public Status overallStatus() {
            return Status.UP;
          }

          @Override
          public List<ComponentHealth> components() {
            return List.of(new ComponentHealth("outbox-relay", Status.DOWN, true, false, 0));
          }
        };
    Instance<BackgroundRelayHealthContributor> relay = mock(Instance.class);
    when(relay.isUnsatisfied()).thenReturn(false);
    when(relay.get()).thenReturn(contributor);

    var check =
        new StreamRuneHealthCheck(
            dataSource(validDataSource()), noHealthContributor(), noEventStore(), relay);
    HealthCheckResponse response = check.call();

    assertEquals("DOWN", response.getData().get().get("relay.outbox-relay.status"));
    assertEquals(HealthCheckResponse.Status.DOWN, response.getStatus());
  }

  private static DataSource validDataSource() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);
    return ds;
  }

  @SuppressWarnings("unchecked")
  private Instance<BackgroundRelayHealthContributor> relayInstance(
      BackgroundRelayHealthContributor.Status status) {
    var contributor =
        new BackgroundRelayHealthContributor() {
          @Override
          public Status overallStatus() {
            return status;
          }

          @Override
          public List<ComponentHealth> components() {
            return List.of(
                new ComponentHealth("outbox-relay", status, true, status != Status.DOWN, 0));
          }
        };
    Instance<BackgroundRelayHealthContributor> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(false);
    when(instance.get()).thenReturn(contributor);
    return instance;
  }

  /**
   * A real contributor over a store whose head is {@code head} and a shared checkpoint at {@code
   * checkpoint} for the {@code orders} subscription, which reports RUNNING.
   */
  @SuppressWarnings("unchecked")
  private static Instance<SubscriptionHealthContributor> contributorWithOrdersAt(
      SubscriptionHealthContributor[] out, long head, long checkpoint) {
    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenReturn(GlobalOffset.of(head));
    OffsetStore offsetStore = mock(OffsetStore.class);
    when(offsetStore.getLastOffset(ProjectionName.of("orders")))
        .thenReturn(GlobalOffset.of(checkpoint));
    SubscriptionLifecycle orders = mock(SubscriptionLifecycle.class);
    when(orders.state()).thenReturn(SubscriptionLifecycleState.RUNNING);
    var contributor = new SubscriptionHealthContributor(eventStore, offsetStore);
    contributor.register("orders", orders);
    out[0] = contributor;
    Instance<SubscriptionHealthContributor> instance = mock(Instance.class);
    when(instance.isUnsatisfied()).thenReturn(false);
    when(instance.get()).thenReturn(contributor);
    return instance;
  }

  @Test
  void aSubscriptionFarBehindTheHeadKeepsTheReadinessCheckUp_withItsLagInTheData()
      throws SQLException {
    // This check is @Readiness. A projection replaying the store from offset 0 (new, or reset for
    // a rebuild) is behind on every replica alike, because lag is measured against the shared
    // checkpoint — a lag-driven DOWN would take every replica out of service at once. Lag is
    // reported as a DEGRADED subscription in the data (and through the lag gauge) instead.
    var holder = new SubscriptionHealthContributor[1];
    var check =
        new StreamRuneHealthCheck(
            dataSource(validDataSource()),
            contributorWithOrdersAt(holder, 50_000, 0),
            noEventStore(),
            noRelay());
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
    assertEquals(50_000L, response.getData().get().get("subscription.orders.lag"));
    assertEquals("DEGRADED", response.getData().get().get("subscription.orders.status"));
  }

  @Test
  void aTerminallyHaltedSubscriptionTurnsTheReadinessCheckDown() throws SQLException {
    var holder = new SubscriptionHealthContributor[1];
    var instance = contributorWithOrdersAt(holder, 10, 10);
    holder[0].markTerminalError("orders");

    HealthCheckResponse response =
        new StreamRuneHealthCheck(
                dataSource(validDataSource()), instance, noEventStore(), noRelay())
            .call();

    assertEquals(HealthCheckResponse.Status.DOWN, response.getStatus());
    assertEquals("DOWN", response.getData().get().get("subscription.orders.status"));
  }

  @Test
  void shouldReturnDownWhenBackgroundRelayIsDown() throws SQLException {
    // A started relay whose poll thread died flips /q/health/ready to DOWN with details.
    var check =
        new StreamRuneHealthCheck(
            dataSource(validDataSource()),
            noHealthContributor(),
            noEventStore(),
            relayInstance(BackgroundRelayHealthContributor.Status.DOWN));
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.DOWN, response.getStatus());
    assertTrue(response.getData().isPresent());
    assertTrue(response.getData().get().containsKey("relay.outbox-relay.status"));
  }

  @Test
  void shouldStayUpAndReportRelayDetailsWhenBackgroundRelayHealthy() throws SQLException {
    var check =
        new StreamRuneHealthCheck(
            dataSource(validDataSource()),
            noHealthContributor(),
            noEventStore(),
            relayInstance(BackgroundRelayHealthContributor.Status.UP));
    HealthCheckResponse response = check.call();

    assertEquals(HealthCheckResponse.Status.UP, response.getStatus());
    assertTrue(response.getData().get().containsKey("relay.outbox-relay.started"));
  }
}
