package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;
import org.streamrune.core.EventStore;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionHealth;
import org.streamrune.core.subscription.SubscriptionLifecycle;
import org.streamrune.core.subscription.SubscriptionLifecycleState;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;
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

  /** An event store whose head is {@code offset}. */
  private static EventStore eventStoreAt(long offset) {
    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenReturn(GlobalOffset.of(offset));
    return eventStore;
  }

  @Test
  void shouldReturnUpWhenDatabaseIsAvailable() throws Exception {
    DataSource ds = validDataSource();

    var indicator = new StreamRuneHealthIndicator(ds);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
  }

  @Test
  void shouldReturnDownWhenDatabaseIsUnavailable() throws Exception {
    DataSource ds = mock(DataSource.class);
    when(ds.getConnection()).thenThrow(new SQLException("Connection refused"));

    var indicator = new StreamRuneHealthIndicator(ds);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    assertThat(health.getDetails()).containsKey("error");
  }

  @Test
  void shouldReturnDownWhenConnectionIsInvalid() throws Exception {
    DataSource ds = mock(DataSource.class);
    Connection conn = mock(Connection.class);
    when(ds.getConnection()).thenReturn(conn);
    when(conn.isValid(anyInt())).thenReturn(false);

    var indicator = new StreamRuneHealthIndicator(ds);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
  }

  @Test
  void shouldReportTheHeadOfTheEventStore() throws Exception {
    DataSource ds = validDataSource();

    var indicator = new StreamRuneHealthIndicator(ds, null, eventStoreAt(42L), null);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsEntry("eventStore.lastGlobalOffset", 42L);
    assertThat(health.getDetails()).doesNotContainKey("eventStore.error");
  }

  /** The head is read through the event store alone: the indicator runs no SQL of its own. */
  @Test
  void shouldReadTheHeadThroughTheEventStoreAndRunNoStatement() throws Exception {
    DataSource ds = validDataSource();
    Connection conn = ds.getConnection();

    new StreamRuneHealthIndicator(ds, null, eventStoreAt(7L), null).health();

    verify(conn, never()).prepareStatement(anyString());
    verify(conn, never()).createStatement();
  }

  @Test
  void shouldReportZeroOffsetWhenEventStreamIsEmpty() throws Exception {
    DataSource ds = validDataSource();

    var indicator = new StreamRuneHealthIndicator(ds, null, eventStoreAt(0L), null);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsEntry("eventStore.lastGlobalOffset", 0L);
  }

  @Test
  void shouldReportZeroOffsetForAStoreWithoutAGlobalHead() throws Exception {
    DataSource ds = validDataSource();
    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenThrow(new UnsupportedOperationException("no head"));

    Health health = new StreamRuneHealthIndicator(ds, null, eventStore, null).health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsEntry("eventStore.lastGlobalOffset", 0L);
    assertThat(health.getDetails()).doesNotContainKey("eventStore.error");
  }

  @Test
  void shouldReportNoEventStoreDetailWithoutAnEventStore() throws Exception {
    DataSource ds = validDataSource();

    Health health = new StreamRuneHealthIndicator(ds).health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails())
        .doesNotContainKey("eventStore.lastGlobalOffset")
        .doesNotContainKey("eventStore.error");
  }

  @Test
  void shouldStayUpAndReportErrorWhenHeadProbeFails() throws Exception {
    DataSource ds = validDataSource();
    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset())
        .thenThrow(new IllegalStateException("relation \"event_stream\" does not exist"));

    var indicator = new StreamRuneHealthIndicator(ds, null, eventStore, null);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails())
        .containsEntry("eventStore.error", "relation \"event_stream\" does not exist");
  }

  @Test
  void shouldNameTheExceptionWhenTheHeadProbeFailsWithoutAMessage() throws Exception {
    DataSource ds = validDataSource();
    EventStore eventStore = mock(EventStore.class);
    when(eventStore.lastGlobalOffset()).thenThrow(new IllegalStateException());

    Health health = new StreamRuneHealthIndicator(ds, null, eventStore, null).health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails())
        .containsEntry("eventStore.error", IllegalStateException.class.getName());
  }

  @Test
  void shouldIncludeSubscriptionHealthWhenContributorPresent() throws Exception {
    DataSource ds = validDataSource();

    var contributor = mock(SubscriptionHealthContributor.class);
    when(contributor.health())
        .thenReturn(
            List.of(
                new SubscriptionHealth(
                    SubscriptionName.of("orders"),
                    SubscriptionLifecycleState.RUNNING,
                    5,
                    0,
                    SubscriptionHealth.Status.UP,
                    LeaseRole.NOT_REPORTED)));
    when(contributor.overallStatus()).thenReturn(SubscriptionHealth.Status.UP);

    var indicator = new StreamRuneHealthIndicator(ds, contributor);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsKey("subscription.orders");
  }

  @Test
  void rendersTheRoleOfEachSubscription() throws Exception {
    DataSource ds = validDataSource();
    var contributor = mock(SubscriptionHealthContributor.class);
    when(contributor.health())
        .thenReturn(
            List.of(
                new SubscriptionHealth(
                    SubscriptionName.of("orders-saga"),
                    SubscriptionLifecycleState.RUNNING,
                    5,
                    0,
                    SubscriptionHealth.Status.UP,
                    LeaseRole.LEADING),
                new SubscriptionHealth(
                    SubscriptionName.of("orders"),
                    SubscriptionLifecycleState.RUNNING,
                    5,
                    0,
                    SubscriptionHealth.Status.UP,
                    LeaseRole.NOT_REPORTED)));
    when(contributor.overallStatus()).thenReturn(SubscriptionHealth.Status.UP);

    Health health = new StreamRuneHealthIndicator(ds, contributor).health();

    assertThat(health.getDetails().get("subscription.orders-saga"))
        .asInstanceOf(InstanceOfAssertFactories.MAP)
        .containsEntry("role", "LEADING");
    assertThat(health.getDetails().get("subscription.orders"))
        .asInstanceOf(InstanceOfAssertFactories.MAP)
        .containsEntry("role", "NOT_REPORTED");
  }

  @Test
  void rendersTheStoreOfAProjection_andNoneForAnEntryWithout() throws Exception {
    DataSource ds = validDataSource();
    var contributor = mock(SubscriptionHealthContributor.class);
    when(contributor.health())
        .thenReturn(
            List.of(
                new SubscriptionHealth(
                    SubscriptionName.of("order-summary"),
                    SubscriptionLifecycleState.RUNNING,
                    0,
                    0,
                    SubscriptionHealth.Status.UP,
                    LeaseRole.LEADING,
                    "postgres"),
                new SubscriptionHealth(
                    SubscriptionName.of("listener"),
                    SubscriptionLifecycleState.RUNNING,
                    0,
                    0,
                    SubscriptionHealth.Status.UP,
                    LeaseRole.NOT_REPORTED)));

    Health health = new StreamRuneHealthIndicator(ds, contributor).health();

    assertThat(health.getDetails().get("subscription.order-summary"))
        .asInstanceOf(InstanceOfAssertFactories.MAP)
        .containsEntry("store", "postgres");
    assertThat(health.getDetails().get("subscription.listener"))
        .asInstanceOf(InstanceOfAssertFactories.MAP)
        .doesNotContainKey("store");
  }

  @Test
  void shouldReturnDownWhenSubscriptionIsDown() throws Exception {
    DataSource ds = validDataSource();

    var contributor = mock(SubscriptionHealthContributor.class);
    when(contributor.health())
        .thenReturn(
            List.of(
                new SubscriptionHealth(
                    SubscriptionName.of("orders"),
                    SubscriptionLifecycleState.STOPPED,
                    0,
                    0,
                    SubscriptionHealth.Status.DOWN,
                    LeaseRole.NOT_REPORTED)));
    when(contributor.overallStatus()).thenReturn(SubscriptionHealth.Status.DOWN);

    var indicator = new StreamRuneHealthIndicator(ds, contributor, eventStoreAt(3L), null);
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    // The head of the event store is retained alongside the DOWN status
    assertThat(health.getDetails()).containsEntry("eventStore.lastGlobalOffset", 3L);
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
    contributor.register(SubscriptionName.of("orders"), orders);
    return contributor;
  }

  @Test
  void aSubscriptionFarBehindTheHeadKeepsTheCheckUp_withItsLagInTheDetails() throws Exception {
    // A projection replaying the store from offset 0 (new, or reset for a rebuild) is behind on
    // every replica alike, because lag is measured against the shared checkpoint. The check is the
    // one a readiness probe consults, so lag must not turn it DOWN: it is reported as a DEGRADED
    // detail (and through the streamrune.subscriptions.lag gauge) instead.
    DataSource ds = validDataSource();

    var indicator = new StreamRuneHealthIndicator(ds, contributorWithOrdersAt(50_000, 0));
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails().get("subscription.orders"))
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("lag", 50_000L)
        .containsEntry("status", "DEGRADED");
  }

  @Test
  void aTerminallyHaltedSubscriptionTurnsTheCheckDown() throws Exception {
    DataSource ds = validDataSource();
    var contributor = contributorWithOrdersAt(10, 10);
    contributor.markTerminalError(SubscriptionName.of("orders"));

    Health health = new StreamRuneHealthIndicator(ds, contributor).health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    assertThat(health.getDetails().get("subscription.orders"))
        .asInstanceOf(org.assertj.core.api.InstanceOfAssertFactories.MAP)
        .containsEntry("status", "DOWN");
  }

  @Test
  void theOverallStatusFollowsTheSubscriptionDetailsItReports() throws Exception {
    // The contributor computes health afresh on every call; the check reads it once, so it can
    // never be UP while its own details show a DOWN subscription (or the reverse).
    DataSource ds = validDataSource();
    var contributor = mock(SubscriptionHealthContributor.class);
    when(contributor.health())
        .thenReturn(
            List.of(
                new SubscriptionHealth(
                    SubscriptionName.of("orders"),
                    SubscriptionLifecycleState.STOPPED,
                    0,
                    0,
                    SubscriptionHealth.Status.DOWN,
                    LeaseRole.NOT_REPORTED)));
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
  void shouldReturnDownWhenBackgroundRelayIsDown() throws Exception {
    // A started relay whose poll thread died must flip /health to DOWN and expose
    // details.
    DataSource ds = validDataSource();

    var indicator =
        new StreamRuneHealthIndicator(
            ds, null, null, relayContributor(BackgroundRelayHealthContributor.Status.DOWN));
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.DOWN);
    assertThat(health.getDetails()).containsKey("relay.outbox-relay");
  }

  @Test
  void shouldStayUpAndReportRelayDetailsWhenBackgroundRelayIsHealthy() throws Exception {
    DataSource ds = validDataSource();

    var indicator =
        new StreamRuneHealthIndicator(
            ds, null, null, relayContributor(BackgroundRelayHealthContributor.Status.UP));
    Health health = indicator.health();

    assertThat(health.getStatus()).isEqualTo(Status.UP);
    assertThat(health.getDetails()).containsKey("relay.outbox-relay");
  }
}
