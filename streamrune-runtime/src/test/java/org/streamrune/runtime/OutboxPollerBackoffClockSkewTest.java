package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxOrderingMode;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.test.MutableClock;

/**
 * Regression: the outbox retry backoff ({@code next_retry_at}) must be measured by the STORE clock
 * — the one {@link InMemoryOutboxStore#loadPending} evaluates eligibility against (the DB {@code
 * NOW()} for the Postgres store) — never by the relay JVM wall clock.
 *
 * <p>Before the fix the poller computed {@code Instant.now().plus(delay)} on its own wall clock and
 * passed that absolute instant to {@code markRetry}, but the claim gate compares {@code
 * next_retry_at <= }storeNow. A relay whose wall clock LAGS the store therefore wrote an
 * already-past {@code next_retry_at}, collapsing exponential backoff into a per-poll retry storm
 * and exhausting the retry ladder in a handful of polls under a broker outage.
 *
 * <p>Modelled here with a store clock set a full day AHEAD of the relay's real wall clock. The
 * failed entry must stay in its backoff window (not immediately re-claimable) and become eligible
 * only after the STORE clock advances past the backoff. Against the pre-fix code the first
 * assertion fails because the relay-clock {@code next_retry_at} (realNow+delay) is already in the
 * store's past, so the entry is re-claimed on the very next poll. The fix passes the backoff as a
 * {@link Duration} and stamps {@code next_retry_at} from the store clock, immune to relay/store
 * skew.
 */
class OutboxPollerBackoffClockSkewTest {

  @Test
  void backoffIsMeasuredByTheStoreClockNotTheRelayWallClock() {
    // Store (DB) clock a full day AHEAD of the relay's real wall clock.
    var storeClock = MutableClock.startingAt(Instant.now().plus(Duration.ofDays(1)));
    // A null-aggregate fixture, so the store is AVAILABILITY_FIRST (the strict default rejects
    // it); this pin is about the backoff clock, not ordering.
    var store =
        new InMemoryOutboxStore(
            storeClock,
            InMemoryOutboxStore.DEFAULT_CLAIM_LEASE,
            OutboxOrderingMode.AVAILABILITY_FIRST);
    var id = OutboxEntryId.of("skew-backoff");
    store.save(OutboxEntry.pending(id, "{}", "Event"));

    // 60s initial backoff, 5 attempts — attempt 1 is a retry (not terminal); no jitter for
    // determinism.
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                entry -> {
                  throw new RuntimeException("broker down");
                })
            .retryPolicy(new RetryPolicy(5, Duration.ofSeconds(60), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();

    poller.processBatch(); // attempt 1 fails -> markRetry(backoff = 60s)

    // The entry is backing off, measured by the STORE clock (next_retry_at = storeNow + 60s). With
    // the relay clock a full day behind the store, a relay-clock next_retry_at (realNow + 60s) is
    // already in the store's past and would be re-claimed immediately — the collapsed-backoff bug.
    assertTrue(
        store.loadPending(10).isEmpty(),
        "a failed entry must stay in its backoff window measured by the store clock, not be "
            + "immediately re-claimable off a lagging relay wall clock");

    // Still inside the backoff by the store clock.
    storeClock.advance(Duration.ofSeconds(30));
    assertTrue(store.loadPending(10).isEmpty(), "still inside the 60s backoff by the store clock");

    // Once the store clock advances past the backoff, the entry is eligible again.
    storeClock.advance(Duration.ofMinutes(5));
    assertEquals(
        1,
        store.loadPending(10).size(),
        "entry becomes eligible once the STORE clock advances past the backoff");
  }
}
