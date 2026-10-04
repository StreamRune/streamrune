package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.runtime.BackgroundRelayHealthContributor;
import org.streamrune.runtime.SubscriptionHealthContributor;

class StreamRuneHealthIndicatorTest {

  private static DataSource validDataSource() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(true);
    return ds;
  }

  /** Stubs the head probe to return one row with the given offset and timestamp. */
  private static void stubLatestEvent(DataSource ds, long offset, OffsetDateTime createdAt)
      throws SQLException {
    Connection conn = ds.getConnection();
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(conn.prepareStatement(StreamRuneHealthIndicator.LATEST_EVENT_QUERY)).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(true);
    when(rs.getLong("global_offset")).thenReturn(offset);
    when(rs.getObject("created_at", OffsetDateTime.class)).thenReturn(createdAt);
  }

  /** Stubs the head probe to return an empty result (no events appended yet). */
  private static void stubEmptyEventStream(DataSource ds) throws SQLException {
    Connection conn = ds.getConnection();
    PreparedStatement ps = mock(PreparedStatement.class);
    ResultSet rs = mock(ResultSet.class);
    when(conn.prepareStatement(anyString())).thenReturn(ps);
    when(ps.executeQuery()).thenReturn(rs);
    when(rs.next()).thenReturn(false);
  }

  @Test
  void shouldReturnUpWhenDatabaseIsAvailable() throws SQLException {
    DataSource ds = validDataSource();
    stubEmptyEventStream(ds);

    var indicator = new StreamRuneHealthIndicator(ds);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
  }

  @Test
  void shouldReturnDownWhenDatabaseIsUnavailable() throws SQLException {
    DataSource ds = mock(DataSource.class);
    when(ds.getConnection()).thenThrow(new SQLException("Connection refused"));

    var indicator = new StreamRuneHealthIndicator(ds);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    assertThat(health.getDetails()).containsKey("error");
  }

  @Test
  void shouldReturnDownWhenConnectionIsInvalid() throws SQLException {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(false);

    var indicator = new StreamRuneHealthIndicator(ds);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
  }

  @Test
  void shouldReportLatestGlobalOffsetAndTimestamp() throws SQLException {
    DataSource ds = validDataSource();
    stubLatestEvent(ds, 42L, OffsetDateTime.of(2026, 4, 23, 10, 0, 0, 0, ZoneOffset.UTC));

    var indicator = new StreamRuneHealthIndicator(ds);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsEntry("eventStore.lastGlobalOffset", 42L);
    assertThat(health.getDetails())
        .containsEntry("eventStore.lastEventTimestamp", "2026-04-23T10:00:00Z");
  }

  @Test
  void shouldReportZeroOffsetWhenEventStreamIsEmpty() throws SQLException {
    DataSource ds = validDataSource();
    stubEmptyEventStream(ds);

    var indicator = new StreamRuneHealthIndicator(ds);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsEntry("eventStore.lastGlobalOffset", 0L);
  }

  @Test
  void shouldStayUpAndReportErrorWhenHeadProbeFails() throws SQLException {
    DataSource ds = validDataSource();
    when(ds.getConnection().prepareStatement(anyString()))
        .thenThrow(new SQLException("relation \"event_stream\" does not exist"));

    var indicator = new StreamRuneHealthIndicator(ds);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails())
        .containsEntry("eventStore.error", "relation \"event_stream\" does not exist");
  }

  @Test
  void shouldIncludeSubscriptionHealthWhenContributorPresent() throws SQLException {
    DataSource ds = validDataSource();
    stubEmptyEventStream(ds);

    var contributor = mock(SubscriptionHealthContributor.class);
    when(contributor.health())
        .thenReturn(
            List.of(
                new SubscriptionHealth(
                    "orders",
                    SubscriptionLifecycleState.RUNNING,
                    5,
                    0,
                    SubscriptionHealth.Status.UP)));
    when(contributor.overallStatus()).thenReturn(SubscriptionHealth.Status.UP);

    var indicator = new StreamRuneHealthIndicator(ds, contributor);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsKey("subscription.orders");
  }

  @Test
  void shouldReturnDownWhenSubscriptionIsDown() throws SQLException {
    DataSource ds = validDataSource();
    stubEmptyEventStream(ds);

    var contributor = mock(SubscriptionHealthContributor.class);
    when(contributor.health())
        .thenReturn(
            List.of(
                new SubscriptionHealth(
                    "orders",
                    SubscriptionLifecycleState.STOPPED,
                    0,
                    0,
                    SubscriptionHealth.Status.DOWN)));
    when(contributor.overallStatus()).thenReturn(SubscriptionHealth.Status.DOWN);

    var indicator = new StreamRuneHealthIndicator(ds, contributor);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    // Event store details from the head probe are retained alongside the DOWN status
    assertThat(health.getDetails()).containsKey("eventStore.lastGlobalOffset");
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
  void aSubscriptionFarBehindTheHeadKeepsTheCheckUp_withItsLagInTheDetails() throws SQLException {
    // A projection replaying the store from offset 0 (new, or reset for a rebuild) is behind on
    // every replica alike, because lag is measured against the shared checkpoint. The check is the
    // one a readiness probe consults, so lag must not turn it DOWN: it is reported as a DEGRADED
    // detail (and through the streamrune.subscriptions.lag gauge) instead.
    DataSource ds = validDataSource();
    stubEmptyEventStream(ds);

    var indicator = new StreamRuneHealthIndicator(ds, contributorWithOrdersAt(50_000, 0));
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails().get("subscription.orders"))
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("lag", 50_000L)
        .containsEntry("status", "DEGRADED");
  }

  @Test
  void aTerminallyHaltedSubscriptionTurnsTheCheckDown() throws SQLException {
    DataSource ds = validDataSource();
    stubEmptyEventStream(ds);
    var contributor = contributorWithOrdersAt(10, 10);
    contributor.markTerminalError("orders");

    Health health = new StreamRuneHealthIndicator(ds, contributor).health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    assertThat(health.getDetails().get("subscription.orders"))
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("status", "DOWN");
  }

  @Test
  void theOverallStatusFollowsTheSubscriptionDetailsItReports() throws SQLException {
    // The contributor computes health afresh on every call; the check reads it once, so it can
    // never be UP while its own details show a DOWN subscription (or the reverse).
    DataSource ds = validDataSource();
    stubEmptyEventStream(ds);
    var contributor = mock(SubscriptionHealthContributor.class);
    when(contributor.health())
        .thenReturn(
            List.of(
                new SubscriptionHealth(
                    "orders",
                    SubscriptionLifecycleState.STOPPED,
                    0,
                    0,
                    SubscriptionHealth.Status.DOWN)));
    when(contributor.overallStatus()).thenReturn(SubscriptionHealth.Status.UP);

    Health health = new StreamRuneHealthIndicator(ds, contributor).health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    verify(contributor, times(1)).health();
  }

  /** A relay-health contributor stub reporting a fixed overall status and one component. */
  private static BackgroundRelayHealthContributor relayContributor(
      BackgroundRelayHealthContributor.Status status) {
    return new BackgroundRelayHealthContributor() {
      @Override
      public BackgroundRelayHealthContributor.Status overallStatus() {
        return status;
      }

      @Override
      public List<ComponentHealth> components() {
        boolean alive = status != BackgroundRelayHealthContributor.Status.DOWN;
        return List.of(new ComponentHealth("outbox-relay", status, true, alive, 0));
      }
    };
  }

  @Test
  void shouldReturnDownWhenBackgroundRelayIsDown() throws SQLException {
    // A started relay whose poll thread died must flip /health to DOWN and expose
    // details.
    DataSource ds = validDataSource();
    stubEmptyEventStream(ds);

    var indicator =
        new StreamRuneHealthIndicator(
            ds, null, relayContributor(BackgroundRelayHealthContributor.Status.DOWN));
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    assertThat(health.getDetails()).containsKey("relay.outbox-relay");
  }

  @Test
  void shouldStayUpAndReportRelayDetailsWhenBackgroundRelayIsHealthy() throws SQLException {
    DataSource ds = validDataSource();
    stubEmptyEventStream(ds);

    var indicator =
        new StreamRuneHealthIndicator(
            ds, null, relayContributor(BackgroundRelayHealthContributor.Status.UP));
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsKey("relay.outbox-relay");
  }
}
