package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.StreamId;
import org.streamrune.postgres.PostgresOutboxStore;
import org.streamrune.runtime.OutboxPoller;

/**
 * Relay-level crash recovery via the claim lease. A first relay <em>claims</em> an outbox entry
 * (moving it to {@code IN_PROGRESS}) and then crashes before {@code markDelivered}. The claim is
 * aged past its lease. A SECOND relay — a real {@link OutboxPoller} running its actual background
 * poll loop with a capturing publisher — reclaims the expired entry, publishes it, and marks it
 * {@code DELIVERED}.
 *
 * <p>This exercises the runner/relay level: the real {@code OutboxPoller} loop drives loadPending →
 * publish → markDelivered. It is distinct from the store-level SKIP-LOCKED test, which asserts the
 * claim partitioning of {@code loadPending} alone with no relay involved.
 *
 * <p>Non-vacuity: while the first relay's claim is within its lease, the running second relay
 * publishes NOTHING (asserted across a one-second observation window). Only after the lease is aged
 * out and the reclaim path resets the row to {@code PENDING} does the relay deliver it. If lease
 * reclaim did not happen, the publish count would stay 0 and the final delivery assertion would
 * fail (timeout).
 */
@Timeout(120)
class RelayClaimRecoveryIT extends E2ETestBase {

  /** Captures every entry id handed to it; always succeeds so the poller marks it delivered. */
  private static final class CapturingPublisher implements OutboxPublisher {
    final ConcurrentLinkedQueue<String> published = new ConcurrentLinkedQueue<>();

    @Override
    public void publish(OutboxEntry entry) {
      published.add(entry.id().value());
    }
  }

  @Test
  void secondRelayReclaimsExpiredClaimAndDeliversExactlyOnce() throws Exception {
    // Lease long enough that it does NOT expire during the "still claimed" observation window, so
    // the only way relay #2 can ever see the entry is via the explicit aging below. Each store
    // instance is its own "relay process" (distinct claimed_by), modelling two separate relays.
    Duration lease = Duration.ofMinutes(5);
    var crashedRelayStore = new PostgresOutboxStore(dataSource, lease);
    var survivingRelayStore = new PostgresOutboxStore(dataSource, lease);

    var id = OutboxEntryId.of("relay-recovery-1");
    crashedRelayStore.save(
        OutboxEntry.pending(
            id,
            "{\"order\":\"42\"}",
            "OrderCreated",
            StreamId.of(AggregateType.of("order"), AggregateId.of("order-42"))));

    // ---- Relay #1 claims the entry, then "crashes" before markDelivered. ----
    List<OutboxEntry> claimed = crashedRelayStore.loadPending(10);
    assertThat(claimed).extracting(e -> e.id().value()).containsExactly(id.value());
    assertInProgress(id);

    // ---- Relay #2 runs its real poll loop while the claim is still alive: must deliver nothing.
    // ----
    var publisher = new CapturingPublisher();
    var poller =
        OutboxPoller.builder()
            .outboxStore(survivingRelayStore)
            .publisher(publisher)
            .batchSize(10)
            .pollInterval(Duration.ofMillis(100))
            .build();

    poller.start();
    try {
      // Observe for ~1s: the live, in-lease claim held by relay #1 is invisible to relay #2.
      Thread.sleep(1000);
      assertThat(publisher.published)
          .as("a live claim held by relay #1 must be invisible to the running relay #2")
          .isEmpty();
      assertInProgress(id);

      // ---- Age the abandoned claim past its lease (relay #1 never came back). ----
      try (var conn = dataSource.getConnection();
          var stmt = conn.createStatement()) {
        stmt.execute(
            "UPDATE outbox_events SET claimed_at = NOW() - INTERVAL '1 hour' WHERE entry_id = '"
                + id.value()
                + "'");
      }

      // ---- Relay #2's loop now reclaims the expired entry, publishes it, and marks it delivered.
      // Await the terminal DELIVERED status rather than the publish capture: publish() runs just
      // before markDelivered(), so polling the publisher queue could observe the row mid-cycle
      // (still IN_PROGRESS). The DELIVERED row is the proof the whole relay cycle completed.
      await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertStatus(id, "DELIVERED"));
    } finally {
      poller.close();
    }

    // Delivered EXACTLY ONCE, by relay #2, and the row is terminal DELIVERED.
    assertThat(publisher.published)
        .as("the reclaimed entry is delivered exactly once — never twice")
        .containsExactly(id.value());
    assertDelivered(id);

    // Give the (now-closed) loop no further chance: a delivered entry stays delivered, never
    // re-claimed or re-published. (Re-poll synchronously via a fresh store to be sure.)
    var verifier = new PostgresOutboxStore(dataSource, lease);
    assertThat(verifier.loadPending(10)).as("a DELIVERED entry is not re-claimable").isEmpty();
  }

  private static void assertStatus(OutboxEntryId id, String expectedStatus) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT status FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("status")).isEqualTo(expectedStatus);
      }
    }
  }

  private static void assertInProgress(OutboxEntryId id) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT status, claimed_at, claimed_by FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("status")).isEqualTo("IN_PROGRESS");
        assertThat(rs.getTimestamp("claimed_at")).isNotNull();
        assertThat(rs.getString("claimed_by")).isNotNull();
      }
    }
  }

  private static void assertDelivered(OutboxEntryId id) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT status, attempts, processed_at FROM outbox_events WHERE entry_id = ?")) {
      ps.setString(1, id.value());
      try (var rs = ps.executeQuery()) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getString("status")).isEqualTo("DELIVERED");
        assertThat(rs.getInt("attempts")).isEqualTo(1);
        assertThat(rs.getTimestamp("processed_at")).isNotNull();
      }
    }
  }
}
