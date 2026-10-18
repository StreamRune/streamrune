package org.streamrune.e2e;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.projection.OffsetStore;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.subscription.LeaseRole;
import org.streamrune.core.subscription.SubscriptionConfig;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.postgres.LeaseBasedLeadership;
import org.streamrune.postgres.PostgresOffsetStore;
import org.streamrune.runtime.PollingEventSubscription;
import org.streamrune.runtime.SagaCompensationRetrySweeper;

/**
 * Processes of one saga type delivered under a lease ({@code SagaRunner.Builder.leadership}): each
 * {@link LeasedNode} runs the saga's subscription built with {@code sagaRunner(runner)}, its
 * timeout runner and its compensation-retry sweeper, and reaches the lease table through a {@link
 * SilentPeerProxy} of its own, so a test can cut one process off its leadership — as a crash or a
 * partition does — while its other connections keep working.
 */
abstract class SagaLeadershipNodes extends SagaForwardFenceNodes {

  static final Duration LEASE_TTL = Duration.ofSeconds(2);
  static final Duration POLLING = Duration.ofMillis(200);
  static final String SUBSCRIPTION = "saga-leadership";

  private final List<LeasedNode> leasedNodes = new ArrayList<>();

  /** Counts the checkpoint reads and saves of one subscription. */
  static final class CountingOffsetStore implements OffsetStore {
    private final OffsetStore delegate;
    final AtomicInteger reads = new AtomicInteger();
    final AtomicInteger saves = new AtomicInteger();

    CountingOffsetStore(OffsetStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public GlobalOffset getLastOffset(ProjectionName projectionName) {
      reads.incrementAndGet();
      return delegate.getLastOffset(projectionName);
    }

    @Override
    public void saveOffset(ProjectionName projectionName, GlobalOffset offset) {
      saves.incrementAndGet();
      delegate.saveOffset(projectionName, offset);
    }
  }

  /**
   * One process of a leased saga type: its leadership runs through a SilentPeerProxy of its own.
   */
  final class LeasedNode implements AutoCloseable {
    final SilentPeerProxy leadershipProxy;
    final LeaseBasedLeadership leadership;
    final Node node;
    final CountingOffsetStore offsets;
    final PollingEventSubscription subscription;
    final SagaCompensationRetrySweeper<State> sweeper;
    private final AtomicBoolean closed = new AtomicBoolean();

    LeasedNode(String name) {
      try {
        leadershipProxy = new SilentPeerProxy(POSTGRES.getHost(), POSTGRES.getMappedPort(5432));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      PGSimpleDataSource leaseSource = new PGSimpleDataSource();
      leaseSource.setUrl(leadershipProxy.jdbcUrl(POSTGRES.getDatabaseName()));
      leaseSource.setUser(POSTGRES.getUsername());
      leaseSource.setPassword(POSTGRES.getPassword());
      leaseSource.setSocketTimeout(2);
      leaseSource.setConnectTimeout(2);
      leaseSource.setApplicationName(name + "-lease");
      leadership = new LeaseBasedLeadership(leaseSource, LEASE_TTL);
      node = new Node(name, true, clock, TIMEOUT, true, leadership, POLLING);
      offsets = new CountingOffsetStore(new PostgresOffsetStore(node.source));
      subscription =
          PollingEventSubscription.builder()
              .subscriptionName(SUBSCRIPTION)
              .eventStore(node.eventStore)
              .offsetStore(offsets)
              .config(SubscriptionConfig.pollingOnly(POLLING))
              .sagaRunner(node.runner)
              .build();
      sweeper =
          SagaCompensationRetrySweeper.<State>builder()
              .sagaRunner(node.runner)
              .sagaStore(node.rows)
              .leadership(leadership)
              .retryInterval(POLLING)
              .giveUpAfter(Duration.ofHours(1))
              .clock(clock)
              .build();
    }

    void start() {
      subscription.start();
      node.timeoutRunner.start();
      sweeper.start();
    }

    LeaseRole role() {
      return node.runner.lease().role();
    }

    /**
     * The process dies: the lease table becomes unreachable from it, then everything it runs stops.
     * Every resign fails (and is swallowed), so its lease expires at its TTL.
     */
    void crash() {
      leadershipProxy.refuseNewConnections(true);
      leadershipProxy.silenceOpenConnections();
      close();
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      subscription.close();
      node.timeoutRunner.stop();
      sweeper.close();
      leadership.close();
      node.close();
      leadershipProxy.close();
    }
  }

  /** A leased process, closed after the test. */
  LeasedNode leased(String name) {
    LeasedNode leased = new LeasedNode(name);
    leasedNodes.add(leased);
    return leased;
  }

  @AfterEach
  void closeLeasedNodes() {
    leasedNodes.forEach(LeasedNode::close);
    leasedNodes.clear();
  }

  /**
   * Moves the nodes' clock past the deadline of every saga created so far. A row's start instant is
   * the database's time of its insert, and the clock stands still where the test began; so the
   * clock first catches up with the time that has passed since, then passes the deadline.
   */
  void passTheDeadlineOfEverySagaSoFar() {
    Duration sinceTheClocksStart = Duration.between(clock.instant(), Instant.now());
    clock.advance(sinceTheClocksStart.plus(TIMEOUT).plusSeconds(1));
  }

  /** Whether the database's clock is before the instant the lease row of {@code lease} expires. */
  boolean leaseRowStillLive(String lease, java.sql.Timestamp leaseUntil) throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT clock_timestamp() < ?")) {
      ps.setTimestamp(1, leaseUntil);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    }
  }

  /** When the lease row of {@code lease} expires, as the database stores it. */
  java.sql.Timestamp leaseUntil(String lease) throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT lease_until FROM subscription_leases WHERE consumer_name = ?")) {
      ps.setString(1, lease);
      try (var rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new AssertionError("no lease row for " + lease);
        }
        return rs.getTimestamp(1);
      }
    }
  }

  /** Whether the order's saga has a row with {@code status}. */
  boolean isRow(String orderId, SagaStatus status) {
    return probeRows
        .load(sagaOf(orderId), TYPE, State.class)
        .map(r -> r.status() == status)
        .orElse(false);
  }

  /** The cause the order's open or last compensation episode recorded; {@code null} for none. */
  String episodeCauseOf(String orderId) throws SQLException {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT episode_cause_event_id FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, sagaOf(orderId).value());
      try (var rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new AssertionError("no saga row for order " + orderId);
        }
        return rs.getString(1);
      }
    }
  }
}
