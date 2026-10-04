package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.LogSanitizer;
import org.streamrune.core.types.StreamId;
import org.streamrune.test.InMemoryOutboxStore;
import org.streamrune.test.MutableClock;

class OutboxPollerTest {

  private static final AggregateType TYPE = AggregateType.of("agg");

  InMemoryOutboxStore store;
  List<OutboxEntry> published;

  @BeforeEach
  void setUp() {
    store = afStore();
    published = new ArrayList<>();
  }

  // These pins cover relay mechanics (lease, CAS, backoff, deadline, streaks) on null-aggregate
  // fixtures, so the store is AVAILABILITY_FIRST; the strict default would reject every
  // null-aggregate save. Ordering-mode behaviour is pinned in OutboxStoreContract and in
  // the strict-specific tests below, which construct strictStore() explicitly.
  private static InMemoryOutboxStore afStore() {
    return afStore(java.time.Clock.systemUTC(), InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
  }

  private static InMemoryOutboxStore afStore(java.time.Clock clock, Duration lease) {
    return new InMemoryOutboxStore(
        clock, lease, org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST);
  }

  private static InMemoryOutboxStore strictStore() {
    return new InMemoryOutboxStore(); // strict by default
  }

  OutboxPoller pollerWith(
      RetryPolicy retryPolicy, org.streamrune.core.outbox.OutboxPublisher publisher) {
    return OutboxPoller.builder()
        .outboxStore(store)
        .publisher(publisher)
        .retryPolicy(retryPolicy)
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60)) // long interval — tests call processBatch() manually
        .build();
  }

  OutboxPoller pollerWithDefaults(org.streamrune.core.outbox.OutboxPublisher publisher) {
    return pollerWith(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false), publisher);
  }

  /**
   * A publisher whose {@link org.streamrune.core.outbox.OutboxPublisher#inFlightHorizon()} is set.
   */
  private static org.streamrune.core.outbox.OutboxPublisher publisherWithHorizon(Duration horizon) {
    return new org.streamrune.core.outbox.OutboxPublisher() {
      @Override
      public void publish(OutboxEntry entry) {}

      @Override
      public Duration inFlightHorizon() {
        return horizon;
      }
    };
  }

  @Test
  void build_rejects_lease_at_or_below_publisher_inflight_horizon() {
    // A claim lease that does not STRICTLY EXCEED the publisher's in-flight horizon
    // is refused at construction. The lease is when another relay may reclaim a still-IN_PROGRESS
    // entry and re-publish it; if it does not exceed the horizon, a reclaim could fire while an
    // IN_FLIGHT entry's original publish is still being delivered by the transport — a duplicate
    // AND
    // a same-aggregate reorder. This is the config StreamRune shipped before the fix: a 60s lease
    // against a Kafka publisher at its 120s delivery.timeout.ms default.
    var shortLeaseStore = afStore(java.time.Clock.systemUTC(), Duration.ofSeconds(60));
    var kafkaLikePublisher = publisherWithHorizon(Duration.ofSeconds(120));

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                OutboxPoller.builder()
                    .outboxStore(shortLeaseStore)
                    .publisher(kafkaLikePublisher)
                    .batchSize(10)
                    .pollInterval(Duration.ofSeconds(60))
                    .build());
    // The message must identify the cause and the fix.
    assertTrue(ex.getMessage().contains("in-flight horizon"), ex.getMessage());
    assertTrue(ex.getMessage().contains(kafkaLikePublisher.getClass().getName()), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("60000ms"), "names the configured lease: " + ex.getMessage());
    assertTrue(ex.getMessage().contains("120000ms"), "names the horizon: " + ex.getMessage());
    // Minimum safe lease = horizon (120s) + 30s margin = 150000ms.
    assertTrue(
        ex.getMessage().contains("150000ms"), "names the minimum safe lease: " + ex.getMessage());
  }

  @Test
  void build_accepts_lease_strictly_above_publisher_inflight_horizon() {
    // The framework's default outbox+Kafka wiring: the 4-minute (240s) default lease clears the
    // shipped Kafka horizon of 180s (max.block.ms 60s + delivery.timeout.ms 120s), so
    // out-of-the-box auto-config still boots.
    var adequateLeaseStore = afStore(java.time.Clock.systemUTC(), Duration.ofSeconds(240));
    var kafkaLikePublisher = publisherWithHorizon(Duration.ofSeconds(180));

    assertNotNull(
        OutboxPoller.builder()
            .outboxStore(adequateLeaseStore)
            .publisher(kafkaLikePublisher)
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build());
  }

  @Test
  void build_rejectsPreviousThreeMinuteLease_againstShippedKafka180sHorizon() {
    // The guard requires a STRICT excess, so the previous 3-minute (180s)
    // default lease is refused against the corrected 180s Kafka horizon — 180s is not > 180s. This
    // is exactly why DEFAULT_CLAIM_LEASE had to move to 4 minutes, and it is the operator-visible
    // breaking change: an existing config with a lease in (120s..180s] plus default Kafka producer
    // settings now fails at boot BY DESIGN (it was silently unsafe before).
    var previousDefaultStore = afStore(java.time.Clock.systemUTC(), Duration.ofSeconds(180));

    var ex =
        assertThrows(
            IllegalStateException.class,
            () ->
                OutboxPoller.builder()
                    .outboxStore(previousDefaultStore)
                    .publisher(publisherWithHorizon(Duration.ofSeconds(180)))
                    .batchSize(10)
                    .pollInterval(Duration.ofSeconds(60))
                    .build());
    // The remediation hint must name the recommended lease (180s + 30s margin) and the horizon
    // levers, including max.block.ms — the operator's cheapest way to shrink the horizon instead.
    assertTrue(
        ex.getMessage().contains("210000ms"), "names the minimum safe lease: " + ex.getMessage());
    assertTrue(
        ex.getMessage().contains("max.block.ms"),
        "names max.block.ms as a horizon lever: " + ex.getMessage());
  }

  @Test
  void build_acceptsDefaultInMemoryStoreLease_againstShippedKafka180sHorizon() {
    // `new InMemoryOutboxStore()` uses the 4-minute (240s)
    // default lease, which clears a shipped Kafka publisher's 180s in-flight horizon
    // (max.block.ms 60s + delivery.timeout.ms 120s) — so wiring the test double at its default
    // against a Kafka-like publisher builds instead of throwing. Both the earlier
    // 60s and the interim 180s test-kit defaults would have tripped the build() guard, leaving the
    // framework's own defaults internally inconsistent.
    assertNotNull(
        OutboxPoller.builder()
            .outboxStore(afStore()) // default lease = 4 minutes
            .publisher(publisherWithHorizon(Duration.ofSeconds(180)))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build());
  }

  @Test
  void build_allows_zero_horizon_publisher_with_any_positive_lease() {
    // A publisher that leaves nothing in flight once publish() returns (default horizon ZERO) never
    // trips the guard — a synchronous in-JVM handler or a test double.
    assertNotNull(
        OutboxPoller.builder()
            .outboxStore(afStore(java.time.Clock.systemUTC(), Duration.ofSeconds(1)))
            .publisher(entry -> {})
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build());
  }

  @Test
  void build_skips_guard_when_store_opts_out_with_nonpositive_lease() {
    // A store returning a non-positive lease opts out of deadline-bounding (see OutboxStore
    // .claimLease()) — there is no reclaim window to protect — so even a large in-flight horizon
    // does not trip the guard.
    var optOutStore =
        new org.streamrune.core.outbox.OutboxStore() {
          public String claimedBy() {
            return "test";
          }

          public void save(OutboxEntry e) {}

          public List<OutboxEntry> loadPending(int limit) {
            return List.of();
          }

          public boolean markDelivered(OutboxEntryId id, String c) {
            return true;
          }

          public boolean markFailed(OutboxEntryId id, int a, String e, String c) {
            return true;
          }

          public boolean markRetry(OutboxEntryId id, int a, String e, Duration n, String c) {
            return true;
          }

          public void delete(OutboxEntryId id) {}

          public List<OutboxEntry> findByStatus(OutboxStatus s, int limit) {
            return List.of();
          }

          public boolean resetFailedToPending(OutboxEntryId id) {
            return false;
          }

          public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
            return java.util.Optional.empty();
          }

          public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
            return false;
          }

          // build() reads the mode; a stub that enforces nothing claims the mode that
          // promises nothing.
          @Override
          public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
            return org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST;
          }

          @Override
          public Duration claimLease() {
            return Duration.ZERO;
          }
        };

    assertNotNull(
        OutboxPoller.builder()
            .outboxStore(optOutStore)
            .publisher(publisherWithHorizon(Duration.ofHours(1)))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build());
  }

  @Test
  void build_rejectsStoreThatInheritsThrowingClaimLeaseDefault() {
    // A store that does NOT override claimLease() inherits the throwing default. The
    // in-flight wiring guard reads store.claimLease() in build() before any poll, so an
    // un-overriding
    // store fails LOUD at wiring — it can never boot validating a fictional reclaim window it does
    // not actually honour (which would produce duplicate + out-of-aggregate-order deliveries).
    var noLeaseStore =
        new org.streamrune.core.outbox.OutboxStore() {
          public String claimedBy() {
            return "test";
          }

          public void save(OutboxEntry e) {}

          public List<OutboxEntry> loadPending(int limit) {
            return List.of();
          }

          public boolean markDelivered(OutboxEntryId id, String c) {
            return true;
          }

          public boolean markFailed(OutboxEntryId id, int a, String e, String c) {
            return true;
          }

          public boolean markRetry(OutboxEntryId id, int a, String e, Duration n, String c) {
            return true;
          }

          public void delete(OutboxEntryId id) {}

          public List<OutboxEntry> findByStatus(OutboxStatus s, int limit) {
            return List.of();
          }

          public boolean resetFailedToPending(OutboxEntryId id) {
            return false;
          }

          public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
            return java.util.Optional.empty();
          }

          public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
            return false;
          }

          // Overridden so the UnsupportedOperationException build() surfaces is claimLease's, not
          // orderingMode's (build() reads the mode first).
          @Override
          public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
            return org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST;
          }
          // deliberately does NOT override claimLease() — inherits the throwing default.
        };

    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                OutboxPoller.builder()
                    .outboxStore(noLeaseStore)
                    .publisher(publisherWithHorizon(Duration.ofSeconds(180)))
                    .batchSize(10)
                    .pollInterval(Duration.ofSeconds(60))
                    .build());
    assertTrue(ex.getMessage().contains("claimLease"), ex.getMessage());
  }

  @Test
  void build_rejectsStoreThatReturnsNullClaimLease() {
    // A null lease breaks the SPI contract. Reading it as the documented opt-out would skip the
    // lease-sizing guard and run the relay without a publish deadline over a store whose reclaim
    // window the poller cannot know, so build() refuses it and names the offending store. A
    // non-positive Duration remains the deliberate opt-out.
    var nullLeaseStore =
        new org.streamrune.test.ForwardingOutboxStore(afStore()) {
          @Override
          public Duration claimLease() {
            return null;
          }
        };
    var builder =
        OutboxPoller.builder()
            .outboxStore(nullLeaseStore)
            .publisher(publisherWithHorizon(Duration.ofSeconds(10)))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60));
    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("claimLease()"), ex.getMessage());
    assertTrue(ex.getMessage().contains(nullLeaseStore.getClass().getName()), ex.getMessage());
  }

  @Test
  void build_rejectsNullClaimLeaseEvenForAPublisherThatLeavesNothingInFlight() {
    // The refusal does not depend on the lease-sizing guard being applicable: a publisher with a
    // zero in-flight horizon never trips the guard, but the relay still reads the lease to bound
    // its publish deadline, so a store that cannot state one is refused at wiring.
    var nullLeaseStore =
        new org.streamrune.test.ForwardingOutboxStore(afStore()) {
          @Override
          public Duration claimLease() {
            return null;
          }
        };
    var builder =
        OutboxPoller.builder()
            .outboxStore(nullLeaseStore)
            .publisher(entry -> {})
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60));
    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("claimLease()"), ex.getMessage());
  }

  @Test
  void claimLease_isReadOnceAtBuild_andNotAgainPerCycle() throws Exception {
    // The lease the wiring guard validated is the lease the publish deadline is bounded by: it is
    // read once, at build(), so a store cannot pass the guard with one value and be relayed under
    // another (or under null) later.
    var reads = new java.util.concurrent.atomic.AtomicInteger();
    var countingStore =
        new org.streamrune.test.ForwardingOutboxStore(afStore()) {
          @Override
          public Duration claimLease() {
            reads.incrementAndGet();
            return super.claimLease();
          }
        };
    var poller =
        OutboxPoller.builder()
            .outboxStore(countingStore)
            .publisher(entry -> {})
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();
    assertEquals(1, reads.get(), "build() reads the lease");
    poller.processBatch();
    poller.processBatch();
    poller.start();
    poller.close();
    assertEquals(1, reads.get(), "no later cycle or start re-reads the lease");
  }

  @Test
  void processBatch_inFlightFailure_leavesEntryClaimed_notReleasedForReclaim() {
    // A delivery failure whose outcome is genuinely UNKNOWN (the message was handed
    // off to the broker but not confirmed — a send/confirm timeout; it may still be delivered) must
    // NOT be markRetry'd. markRetry releases the claim (status -> PENDING, claimed_by cleared), so
    // a
    // second HA relay reclaims and RE-PUBLISHES while our publish is still in-flight — a duplicate
    // AND a same-aggregate reorder. The entry must stay IN_PROGRESS (claimed) so only lease-reclaim
    // redelivers it, after the in-flight publish has resolved.
    var id = OutboxEntryId.of("inflight-1");
    store.save(OutboxEntry.pending(id, "{}", "Event"));

    var publisher =
        new org.streamrune.core.outbox.OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {
            throw new RuntimeException(
                new java.util.concurrent.TimeoutException("broker did not confirm within timeout"));
          }

          @Override
          public FailureKind classifyFailure(Exception failure) {
            return FailureKind.IN_FLIGHT;
          }
        };
    pollerWith(new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false), publisher).processBatch();

    var entries = store.all();
    assertEquals(1, entries.size());
    assertEquals(
        OutboxStatus.IN_PROGRESS,
        entries.get(0).status(),
        "an in-flight/unconfirmed failure must leave the entry IN_PROGRESS (claimed) for"
            + " lease-reclaim, not release it to PENDING for immediate re-claim (which would reorder)");
    assertEquals(
        0, entries.get(0).attempts(), "an in-flight failure must not burn a retry attempt");
    assertTrue(
        store.loadPending(10).isEmpty(),
        "the in-flight entry must not be immediately re-claimable by another relay");
  }

  /**
   * A publisher of the exact zero-horizon hazard shape: it classifies its failures {@code
   * IN_FLIGHT} ("the hand-off may still resolve at the transport") yet inherits the {@code
   * inFlightHorizon()} {@code ZERO} default ("nothing is ever left in flight"). The {@code
   * OutboxPoller.build()} lease guard skips a zero horizon, so this shape silently gets unsafe
   * lease sizing — the runtime guard below is what catches it.
   */
  private static org.streamrune.core.outbox.OutboxPublisher inFlightClassifyingZeroHorizon() {
    return new org.streamrune.core.outbox.OutboxPublisher() {
      @Override
      public void publish(OutboxEntry entry) {
        throw new RuntimeException(
            new java.util.concurrent.TimeoutException("broker did not confirm within timeout"));
      }

      @Override
      public FailureKind classifyFailure(Exception failure) {
        return FailureKind.IN_FLIGHT;
      }
      // deliberately does NOT override inFlightHorizon(): the ZERO default stands.
    };
  }

  @Test
  void inFlightWithZeroHorizon_isHeldFailSafe_andReportedAsContractViolation() {
    // classifyFailure == IN_FLIGHT with the ZERO inFlightHorizon()
    // default is a CONTRACT VIOLATION — the two defaults contradict each other, and the build()
    // lease guard (which early-returns on a zero horizon) never validated the lease against the
    // transport's real in-flight window. A build-time refusal is impossible in general (the
    // classification is behavioral, only observable when a failure is classified), so the poller
    // must catch it at delivery time: fail SAFE — hold the claim for the full lease exactly as for
    // a declared horizon, never release it early — and report LOUDLY (one-time ERROR log + a
    // metric per held entry) so the operator learns the lease guard is not protecting them.
    var id = OutboxEntryId.of("zero-horizon-1");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    var metrics = new RecordingMetrics();
    var poller =
        pollerWithMetrics(
            new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false),
            inFlightClassifyingZeroHorizon(),
            metrics);

    poller.processBatch();

    var entries = store.all();
    assertEquals(1, entries.size());
    assertEquals(
        OutboxStatus.IN_PROGRESS,
        entries.get(0).status(),
        "fail SAFE: the claim is held for the full lease (as if horizon == lease) — releasing it"
            + " early would let a second relay re-publish a possibly-still-in-flight entry");
    assertEquals(0, entries.get(0).attempts(), "the violation must not burn a retry attempt");
    assertEquals(
        1,
        metrics.inFlightHorizonViolations,
        "the contract violation must be reported on the"
            + " streamrune.outbox.in_flight_horizon_violation counter (once per held entry)");
  }

  @Test
  void inFlightWithDeclaredHorizon_doesNotReportContractViolation() {
    // The counterpart: a publisher that declares an honest nonzero horizon (validated against the
    // lease at build()) is the CORRECT in-flight shape — same held claim, no violation report.
    var id = OutboxEntryId.of("declared-horizon-1");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    var metrics = new RecordingMetrics();
    var publisher =
        new org.streamrune.core.outbox.OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {
            throw new RuntimeException(
                new java.util.concurrent.TimeoutException("broker did not confirm within timeout"));
          }

          @Override
          public FailureKind classifyFailure(Exception failure) {
            return FailureKind.IN_FLIGHT;
          }

          @Override
          public Duration inFlightHorizon() {
            return Duration.ofSeconds(30); // honest bound, well under the 4-minute default lease
          }
        };
    var poller =
        pollerWithMetrics(
            new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false), publisher, metrics);

    poller.processBatch();

    assertEquals(OutboxStatus.IN_PROGRESS, store.all().get(0).status(), "claim held as designed");
    assertEquals(
        0,
        metrics.inFlightHorizonViolations,
        "a declared-horizon IN_FLIGHT is the correct contract shape — no violation report");
  }

  @Test
  void processBatch_delivers_pending_entry_and_marks_delivered() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "OrderCreated"));

    pollerWithDefaults(entry -> published.add(entry)).processBatch();

    assertEquals(1, published.size());
    assertEquals("e1", published.get(0).id().value());
    assertTrue(store.loadPending(10).isEmpty());
    assertEquals(OutboxStatus.DELIVERED, store.all().get(0).status());
  }

  @Test
  void processBatch_delivers_multiple_entries() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("e2"), "{}", "Event"));

    pollerWithDefaults(entry -> published.add(entry)).processBatch();

    assertEquals(2, published.size());
    assertTrue(store.loadPending(10).isEmpty());
  }

  @Test
  void processBatch_failed_delivery_sets_nextRetryAt_and_keeps_pending() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("e3"), "{}", "Event"));
    var retryPolicy = new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false);
    pollerWith(
            retryPolicy,
            entryClassified(
                entry -> {
                  throw new RuntimeException("delivery failed");
                }))
        .processBatch();

    var entries = store.all();
    assertEquals(1, entries.size());
    assertEquals(OutboxStatus.PENDING, entries.get(0).status());
    assertEquals(1, entries.get(0).attempts());
    assertEquals("delivery failed", entries.get(0).lastError());
    assertTrue(
        entries.get(0).nextRetryAt().isAfter(Instant.now()),
        "nextRetryAt must be in the future after a failed delivery");
  }

  /**
   * {@code outbox_events.last_error} is a persisted-text sink — a broker's message is free text;
   * the stored copy is the sanitized one.
   */
  @Test
  void processBatch_failed_delivery_persistsTheErrorSanitized() {
    String raw = "broker said no\r\n2026-10-02 INFO outbox - delivered\u0000";
    store.save(OutboxEntry.pending(OutboxEntryId.of("e3s"), "{}", "Event"));
    var retryPolicy = new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false);
    pollerWith(
            retryPolicy,
            entryClassified(
                entry -> {
                  throw new RuntimeException(raw);
                }))
        .processBatch();

    var entries = store.all();
    assertEquals(1, entries.size());
    assertEquals(LogSanitizer.sanitizeFreeText(raw), entries.get(0).lastError());
    assertFalse(entries.get(0).lastError().chars().anyMatch(Character::isISOControl));
  }

  @Test
  void processBatch_marks_failed_after_maxAttempts_exhausted() throws InterruptedException {
    // The poller passes a Duration backoff and the store DB-stamps next_retry_at from its own clock
    // (here the real-clock InMemory store): a 1ms backoff releases the entry to PENDING with
    // next_retry_at ~1ms out, a short sleep makes it eligible again, and the poller RE-CLAIMS it
    // (mark* is now a CAS on IN_PROGRESS + claimed_by — the second processBatch reaches the
    // terminal
    // markFailed only via a fresh claim).
    var id = OutboxEntryId.of("e4");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    var retryPolicy = new RetryPolicy(2, Duration.ofMillis(1), 2.0, false);
    var poller =
        pollerWith(
            retryPolicy,
            entryClassified(
                entry -> {
                  throw new RuntimeException("always fails");
                }));

    poller.processBatch(); // attempt 1 — markRetry (releases to PENDING, nextRetryAt ~now+1ms)
    Thread.sleep(20); // let the backoff window elapse so loadPending re-claims it
    poller.processBatch(); // attempt 2 — re-claim then markFailed (terminal)

    var failed =
        store.all().stream()
            .filter(e -> e.status() == OutboxStatus.FAILED)
            .findFirst()
            .orElseThrow();
    assertEquals(2, failed.attempts());
    assertEquals("always fails", failed.lastError());
  }

  @Test
  void processBatch_skips_entries_with_nextRetryAt_in_future() {
    var id = OutboxEntryId.of("e-skip");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    var retryPolicy = new RetryPolicy(5, Duration.ofSeconds(60), 2.0, false);
    var poller =
        pollerWith(
            retryPolicy,
            entry -> {
              throw new RuntimeException("fail");
            });

    poller.processBatch(); // attempt 1 — sets nextRetryAt ~60s in future
    published.clear();

    var secondPoller = pollerWith(retryPolicy, entry -> published.add(entry));
    secondPoller.processBatch();

    assertTrue(published.isEmpty(), "entry with future nextRetryAt should not be delivered");
  }

  @Test
  void processBatch_respects_batchSize() {
    for (int i = 0; i < 5; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("e" + i), "{}", "Event"));
    }

    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(entry -> published.add(entry))
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
            .batchSize(3)
            .pollInterval(Duration.ofSeconds(60))
            .build();
    poller.processBatch();

    assertEquals(3, published.size());
    assertEquals(2, store.loadPending(10).size());
  }

  @Test
  void start_and_close_runs_without_error() throws Exception {
    store.save(OutboxEntry.pending(OutboxEntryId.of("bg"), "{}", "Event"));

    // The poll thread publishes while this thread waits, so the sink must be thread-safe.
    var delivered = new CopyOnWriteArrayList<OutboxEntry>();
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(entry -> delivered.add(entry))
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofMillis(50))
            .build();

    poller.start();
    try {
      Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> !delivered.isEmpty());
    } finally {
      poller.close();
    }

    assertEquals("bg", delivered.getFirst().id().value(), "poller must have delivered the entry");
  }

  @Test
  void poll_loop_survives_store_failure_and_recovers() throws Exception {
    store.save(OutboxEntry.pending(OutboxEntryId.of("resilient"), "{}", "Event"));

    var failuresLeft = new java.util.concurrent.atomic.AtomicInteger(2);
    var failingStore =
        new org.streamrune.core.outbox.OutboxStore() {
          @Override
          public String claimedBy() {
            return store.claimedBy(); // delegate: loadPending stamps the inner store's identity
          }

          @Override
          public void save(OutboxEntry entry) {
            store.save(entry);
          }

          @Override
          public List<OutboxEntry> loadPending(int limit) {
            if (failuresLeft.getAndDecrement() > 0) {
              throw new RuntimeException("store temporarily unreachable");
            }
            return store.loadPending(limit);
          }

          @Override
          public boolean markDelivered(
              org.streamrune.core.outbox.OutboxEntryId id, String claimedBy) {
            return store.markDelivered(id, claimedBy);
          }

          @Override
          public boolean markRetry(
              org.streamrune.core.outbox.OutboxEntryId id,
              int attempts,
              String lastError,
              Duration nextRetryAt,
              String claimedBy) {
            return store.markRetry(id, attempts, lastError, nextRetryAt, claimedBy);
          }

          @Override
          public boolean markFailed(
              org.streamrune.core.outbox.OutboxEntryId id,
              int attempts,
              String lastError,
              String claimedBy) {
            return store.markFailed(id, attempts, lastError, claimedBy);
          }

          @Override
          public void delete(org.streamrune.core.outbox.OutboxEntryId id) {
            store.delete(id);
          }

          @Override
          public List<OutboxEntry> findByStatus(
              org.streamrune.core.outbox.OutboxStatus status, int limit) {
            return store.findByStatus(status, limit);
          }

          @Override
          public boolean resetFailedToPending(org.streamrune.core.outbox.OutboxEntryId id) {
            return store.resetFailedToPending(id);
          }

          @Override
          public java.util.Optional<OutboxEntry> findById(
              org.streamrune.core.outbox.OutboxEntryId id) {
            return store.findById(id);
          }

          @Override
          public boolean skipFailed(
              org.streamrune.core.outbox.OutboxEntryId id, String skippedBy, String reason) {
            return store.skipFailed(id, skippedBy, reason);
          }

          @Override
          public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
            return store.orderingMode();
          }

          @Override
          public java.time.Duration claimLease() {
            return java.time.Duration.ofMinutes(4);
          }
        };

    var delivered = new java.util.concurrent.CopyOnWriteArrayList<OutboxEntry>();
    var poller =
        OutboxPoller.builder()
            .outboxStore(failingStore)
            .publisher(delivered::add)
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(10), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofMillis(10))
            .build();

    poller.start();
    try {
      long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
      while (delivered.isEmpty() && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertFalse(
          delivered.isEmpty(),
          "relay must survive transient store failures and deliver after recovery");
      assertEquals(0, poller.consecutiveFailures(), "failure counter resets after recovery");
    } finally {
      poller.close();
    }
  }

  @Test
  void processBatch_calls_markDeliveredAll_once_for_all_successes() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("batch1"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("batch2"), "{}", "Event"));

    // Wrap the real store to spy on markDelivered vs markDeliveredAll
    var deliveredSingleIds = new ArrayList<OutboxEntryId>();
    var deliveredAllCollections = new ArrayList<Collection<OutboxEntryId>>();
    var spyStore =
        new org.streamrune.core.outbox.OutboxStore() {
          @Override
          public String claimedBy() {
            return store.claimedBy();
          }

          @Override
          public void save(OutboxEntry entry) {
            store.save(entry);
          }

          @Override
          public List<OutboxEntry> loadPending(int limit) {
            return store.loadPending(limit);
          }

          @Override
          public boolean markDelivered(OutboxEntryId id, String claimedBy) {
            deliveredSingleIds.add(id);
            return store.markDelivered(id, claimedBy);
          }

          @Override
          public int markDeliveredAll(Collection<OutboxEntryId> ids, String claimedBy) {
            deliveredAllCollections.add(new ArrayList<>(ids));
            return store.markDeliveredAll(ids, claimedBy);
          }

          @Override
          public boolean markRetry(
              OutboxEntryId id,
              int attempts,
              String lastError,
              Duration nextRetryAt,
              String claimedBy) {
            return store.markRetry(id, attempts, lastError, nextRetryAt, claimedBy);
          }

          @Override
          public boolean markFailed(
              OutboxEntryId id, int attempts, String lastError, String claimedBy) {
            return store.markFailed(id, attempts, lastError, claimedBy);
          }

          @Override
          public void delete(OutboxEntryId id) {
            store.delete(id);
          }

          @Override
          public List<OutboxEntry> findByStatus(
              org.streamrune.core.outbox.OutboxStatus status, int limit) {
            return store.findByStatus(status, limit);
          }

          @Override
          public boolean resetFailedToPending(OutboxEntryId id) {
            return store.resetFailedToPending(id);
          }

          @Override
          public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
            return store.findById(id);
          }

          @Override
          public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
            return store.skipFailed(id, skippedBy, reason);
          }

          @Override
          public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
            return store.orderingMode();
          }

          @Override
          public java.time.Duration claimLease() {
            return java.time.Duration.ofMinutes(4);
          }
        };

    OutboxPoller.builder()
        .outboxStore(spyStore)
        .publisher(entry -> published.add(entry))
        .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .build()
        .processBatch();

    assertEquals(2, published.size(), "both entries must be published");
    assertEquals(0, deliveredSingleIds.size(), "markDelivered(id) must NOT be called per-entry");
    assertEquals(1, deliveredAllCollections.size(), "markDeliveredAll must be called exactly once");
    var deliveredIds = deliveredAllCollections.get(0);
    assertEquals(2, deliveredIds.size(), "markDeliveredAll must include both ids");
    assertTrue(
        deliveredIds.stream().anyMatch(id -> id.value().equals("batch1")),
        "batch1 must be in the delivered-all collection");
    assertTrue(
        deliveredIds.stream().anyMatch(id -> id.value().equals("batch2")),
        "batch2 must be in the delivered-all collection");
  }

  @Test
  void start_close_start_polls_again() throws Exception {
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(entry -> published.add(entry))
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofMillis(50))
            .build();

    store.save(OutboxEntry.pending(OutboxEntryId.of("first"), "{}", "Event"));
    poller.start();
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
    while (published.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertFalse(published.isEmpty(), "first entry must be delivered before close");
    poller.close();

    published.clear();
    store.save(OutboxEntry.pending(OutboxEntryId.of("second"), "{}", "Event"));
    assertDoesNotThrow(poller::start, "restart after close must not throw");
    deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
    while (published.isEmpty() && System.nanoTime() < deadline) {
      Thread.sleep(10);
    }
    assertFalse(published.isEmpty(), "second entry must be delivered after restart");
    poller.close();
  }

  @Test
  void builder_rejects_missing_outboxStore() {
    assertThrows(
        IllegalStateException.class, () -> OutboxPoller.builder().publisher(e -> {}).build());
  }

  @Test
  void builder_rejects_missing_publisher() {
    assertThrows(
        IllegalStateException.class, () -> OutboxPoller.builder().outboxStore(store).build());
  }

  @Test
  void builder_rejects_null_retryPolicy() {
    assertThrows(
        IllegalStateException.class,
        () ->
            OutboxPoller.builder().outboxStore(store).publisher(e -> {}).retryPolicy(null).build());
  }

  @Test
  void processBatch_publishBatch_partial_subset_only_confirmed_markDelivered_rest_markRetry() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("ok"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("fail"), "{}", "Event"));

    // Publisher that overrides the deadline-aware publishBatch to simulate a partial confirm from
    // the
    // broker. The poller calls the two-arg publishBatch (it passes a lease-bounded deadline,
    // the publish deadline),
    // so a batch publisher overrides THAT method (mirrors RabbitMqOutboxPublisher). Classifies its
    // failures ENTRY explicitly — this test asserts the counting markRetry path
    // (the SPI default is the non-counting TRANSPORT).
    var partialPublisher =
        new org.streamrune.core.outbox.OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {
            // unused in this test — publishBatch is overridden
          }

          @Override
          public FailureKind classifyFailure(Exception failure) {
            return FailureKind.ENTRY;
          }

          @Override
          public org.streamrune.core.outbox.OutboxPublisher.BatchResult publishBatch(
              java.util.List<OutboxEntry> entries, java.time.Instant deadline) {
            var confirmed = new java.util.LinkedHashSet<OutboxEntryId>();
            var failed = new java.util.LinkedHashMap<OutboxEntryId, Exception>();
            for (OutboxEntry e : entries) {
              if (e.id().value().equals("ok")) {
                confirmed.add(e.id());
              } else {
                failed.put(e.id(), new RuntimeException("broker nack"));
              }
            }
            return new BatchResult(confirmed, failed);
          }
        };

    pollerWithDefaults(partialPublisher).processBatch();

    var all = store.all();
    var okEntry = all.stream().filter(e -> e.id().value().equals("ok")).findFirst().orElseThrow();
    var failEntry =
        all.stream().filter(e -> e.id().value().equals("fail")).findFirst().orElseThrow();

    assertEquals(OutboxStatus.DELIVERED, okEntry.status(), "'ok' entry must be DELIVERED");
    assertEquals(OutboxStatus.PENDING, failEntry.status(), "'fail' entry must remain PENDING");
    assertEquals(1, failEntry.attempts(), "'fail' entry must have 1 attempt recorded");
    assertEquals("broker nack", failEntry.lastError());
    assertTrue(
        failEntry.nextRetryAt().isAfter(java.time.Instant.now()),
        "nextRetryAt must be in the future for the failed entry");
  }

  // ── Publish cycle bounded to the claim lease ─────────────────────────────────────────

  @Test
  void processBatch_stopsPublishingPastPublishDeadline_leavesRemainingEntriesClaimed()
      throws Exception {
    // A live relay whose publish cycle outlives the claim lease lets a second relay reclaim
    // and re-deliver the not-yet-published entries — duplicate + out-of-order same-aggregate
    // delivery. The poller must bound the batch to a deadline < lease: once the deadline passes it
    // stops publishing further entries and leaves them IN_PROGRESS (claimed) for the lease-reclaim
    // path, rather than blindly publishing the whole stale-claimed batch.
    var lease = Duration.ofSeconds(1); // deadline ≈ 0.8s
    var store = afStore(java.time.Clock.systemUTC(), lease);
    int total = 8;
    for (int i = 0; i < total; i++) {
      // NULL aggregate so all are claimable in one batch (no per-aggregate serialization).
      store.save(OutboxEntry.pending(OutboxEntryId.of("slow-" + i), "{}", "Event"));
    }

    var publishedIds = new java.util.concurrent.CopyOnWriteArrayList<OutboxEntryId>();
    org.streamrune.core.outbox.OutboxPublisher slow =
        entry -> {
          Thread.sleep(250); // each publish is slow; 0.8s deadline is hit after a few entries
          publishedIds.add(entry.id());
        };

    OutboxPoller.builder()
        .outboxStore(store)
        .publisher(slow)
        .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .build()
        .processBatch();

    assertTrue(
        publishedIds.size() < total,
        "the poller must stop publishing once the per-batch deadline (< lease) is reached, not "
            + "deliver the whole stale-claimed batch (published "
            + publishedIds.size()
            + " of "
            + total
            + ")");
    assertTrue(
        publishedIds.size() >= 1, "at least the first entry is attempted before the deadline");

    long delivered = store.all().stream().filter(e -> e.status() == OutboxStatus.DELIVERED).count();
    long stillClaimed =
        store.all().stream().filter(e -> e.status() == OutboxStatus.IN_PROGRESS).count();
    assertEquals(
        publishedIds.size(), delivered, "every published entry is marked DELIVERED this cycle");
    assertEquals(
        0,
        stillClaimed,
        "Entries not reached before the deadline were never handed to the"
            + " transport, so the poller RELEASES their claims at the end of the cycle instead of"
            + " holding them for a full lease — nothing is in flight for them, so there is nothing"
            + " a release could duplicate");
    long releasedPending =
        store.all().stream()
            .filter(e -> e.status() == OutboxStatus.PENDING)
            .filter(e -> e.attempts() == 0)
            .count();
    assertEquals(
        total - publishedIds.size(),
        releasedPending,
        "released entries are back to PENDING and must NOT be recorded as failures (which would"
            + " burn a retry attempt)");
    assertTrue(releasedPending >= 1, "at least one entry was left unattempted by the deadline");
  }

  /**
   * A publisher that models a deadline-truncated cycle deterministically, without sleeping: it
   * confirms the first {@code attemptLimit} entries and leaves every later one in NEITHER bucket —
   * exactly the shape {@code publishSequentially} produces when it breaks on the deadline, and the
   * shape {@code RabbitMqOutboxPublisher} produces when {@code stopPublishing} latches.
   */
  private static final class TruncatingPublisher
      implements org.streamrune.core.outbox.OutboxPublisher {
    private final int attemptLimit;
    final List<OutboxEntryId> attempted = new ArrayList<>();

    TruncatingPublisher(int attemptLimit) {
      this.attemptLimit = attemptLimit;
    }

    @Override
    public void publish(OutboxEntry entry) {
      attempted.add(entry.id());
    }

    @Override
    public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
      var confirmed = new java.util.LinkedHashSet<OutboxEntryId>();
      for (int i = 0; i < Math.min(attemptLimit, entries.size()); i++) {
        attempted.add(entries.get(i).id());
        confirmed.add(entries.get(i).id());
      }
      return new BatchResult(confirmed, java.util.Map.of());
    }
  }

  @Test
  void deadlineTruncatedBatch_releasesUnattemptedClaimsImmediately_withoutBurningAnAttempt() {
    // An entry the publish cycle never reached was never handed to the
    // transport — both the default publishSequentially and the RabbitMQ override break BEFORE the
    // publish call — so holding its claim for a full lease protects nothing while gating its
    // aggregate. Releasing the claim and burning a retry attempt are separable: the poller does the
    // first and not the second.
    //
    // The lease is the shipped 4-minute default here on purpose: lease-reclaim provably cannot be
    // what returns these entries to PENDING within the test.
    var store = afStore(java.time.Clock.systemUTC(), Duration.ofMinutes(4));
    for (int i = 0; i < 6; i++) {
      store.save(
          OutboxEntry.pending(
              OutboxEntryId.of("e" + i),
              "{}",
              "Event",
              StreamId.of(TYPE, AggregateId.of("agg-" + i))));
    }
    var publisher = new TruncatingPublisher(2);

    OutboxPoller.builder()
        .outboxStore(store)
        .publisher(publisher)
        .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .build()
        .processBatch();

    var unattempted =
        store.all().stream().filter(e -> !publisher.attempted.contains(e.id())).toList();
    assertEquals(4, unattempted.size(), "two of six entries were attempted");
    for (OutboxEntry e : unattempted) {
      assertEquals(
          OutboxStatus.PENDING,
          e.status(),
          "unattempted entry "
              + e.id()
              + " must be released back to PENDING, not held IN_PROGRESS");
      assertEquals(0, e.attempts(), "releasing must not burn a retry-ladder attempt");
      assertTrue(
          e.nextRetryAt() == null || !e.nextRetryAt().isAfter(Instant.now()),
          "a released entry is immediately eligible again — no backoff is stamped");
      assertNull(e.lastError(), "a never-attempted entry has no delivery error to record");
    }
  }

  @Test
  void deadlineTruncatedBatch_nextCycleCanReclaimTheSameAggregates() {
    // The operational consequence of holding the claims: agg_claimable's "no IN_PROGRESS row for
    // this aggregate" gate excludes every aggregate whose head this relay is holding, so the very
    // next cycle claims NOTHING and the relay idles for a full lease behind its own never-touched
    // entries. With the release in place the next cycle — same instant, lease nowhere near expiry —
    // claims and delivers the released heads.
    var store = afStore(java.time.Clock.systemUTC(), Duration.ofMinutes(4));
    for (int i = 0; i < 6; i++) {
      store.save(
          OutboxEntry.pending(
              OutboxEntryId.of("e" + i),
              "{}",
              "Event",
              StreamId.of(TYPE, AggregateId.of("agg-" + i))));
    }
    var publisher = new TruncatingPublisher(2);
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(publisher)
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();

    poller.processBatch();
    poller.processBatch();

    long delivered = store.all().stream().filter(e -> e.status() == OutboxStatus.DELIVERED).count();
    assertEquals(
        4,
        delivered,
        "the second cycle must make progress immediately (2 + 2 delivered); holding the"
            + " never-attempted claims gates every aggregate and stalls the relay for a full lease");
  }

  @Test
  void processBatch_publishDeadlineAnchoredBeforeLoadPending_slowClaimDoesNotExtendWindow() {
    // The publish deadline base is captured BEFORE loadPending, so a slow claim query
    // (RECLAIM + claim CTE on a large backlog) is charged AGAINST the publish budget rather than
    // pushing the deadline later. A store whose loadPending sleeps 400ms must NOT move the deadline
    // to ~base+0.4s+0.8s; it must stay anchored near base+0.8*lease.
    var lease = Duration.ofSeconds(1); // budget ≈ 0.8s
    var inner = afStore(java.time.Clock.systemUTC(), lease);
    inner.save(OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event"));
    long slowClaimMs = 400L;

    var slowStore =
        new org.streamrune.core.outbox.OutboxStore() {
          @Override
          public String claimedBy() {
            return inner.claimedBy();
          }

          @Override
          public Duration claimLease() {
            return inner.claimLease();
          }

          @Override
          public void save(OutboxEntry entry) {
            inner.save(entry);
          }

          @Override
          public List<OutboxEntry> loadPending(int limit) {
            try {
              Thread.sleep(slowClaimMs); // simulate a slow reclaim/claim query
            } catch (InterruptedException _) {
              Thread.currentThread().interrupt();
            }
            return inner.loadPending(limit);
          }

          @Override
          public boolean markDelivered(OutboxEntryId id, String claimedBy) {
            return inner.markDelivered(id, claimedBy);
          }

          @Override
          public int markDeliveredAll(Collection<OutboxEntryId> ids, String claimedBy) {
            return inner.markDeliveredAll(ids, claimedBy);
          }

          @Override
          public boolean markRetry(
              OutboxEntryId id,
              int attempts,
              String lastError,
              Duration backoff,
              String claimedBy) {
            return inner.markRetry(id, attempts, lastError, backoff, claimedBy);
          }

          @Override
          public boolean markFailed(
              OutboxEntryId id, int attempts, String lastError, String claimedBy) {
            return inner.markFailed(id, attempts, lastError, claimedBy);
          }

          @Override
          public void delete(OutboxEntryId id) {
            inner.delete(id);
          }

          @Override
          public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
            return inner.findByStatus(status, limit);
          }

          @Override
          public boolean resetFailedToPending(OutboxEntryId id) {
            return inner.resetFailedToPending(id);
          }

          @Override
          public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
            return inner.findById(id);
          }

          @Override
          public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
            return inner.skipFailed(id, skippedBy, reason);
          }

          @Override
          public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
            return inner.orderingMode();
          }
        };

    var capturedDeadline = new java.util.concurrent.atomic.AtomicReference<Instant>();
    var publisher =
        new org.streamrune.core.outbox.OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {}

          @Override
          public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
            capturedDeadline.set(deadline);
            var confirmed = new java.util.LinkedHashSet<OutboxEntryId>();
            for (OutboxEntry e : entries) {
              confirmed.add(e.id());
            }
            return new BatchResult(confirmed, new java.util.LinkedHashMap<>());
          }
        };

    var before = Instant.now();
    OutboxPoller.builder()
        .outboxStore(slowStore)
        .publisher(publisher)
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .build()
        .processBatch();

    var deadline = capturedDeadline.get();
    assertNotNull(deadline, "publisher must have received a lease-bounded deadline");
    // Fix: deadline ≈ before + 0.8*lease (~800ms). Bug: before + 400ms(claim) + 800ms (~1200ms).
    assertTrue(
        deadline.isBefore(before.plusMillis(950)),
        "publish deadline must be anchored before loadPending — a slow claim must not extend it "
            + "(was "
            + Duration.between(before, deadline).toMillis()
            + "ms after cycle start)");
    assertTrue(
        deadline.isAfter(before.plusMillis(600)),
        "publish deadline should still reflect ~0.8*lease of budget");
  }

  /**
   * A publisher with the given in-flight horizon that captures the publish deadline it is handed
   * and confirms every entry — for asserting the deadline arithmetic without real waiting.
   */
  private static org.streamrune.core.outbox.OutboxPublisher deadlineCapturingPublisher(
      Duration horizon, java.util.concurrent.atomic.AtomicReference<Instant> deadlineSink) {
    return new org.streamrune.core.outbox.OutboxPublisher() {
      @Override
      public void publish(OutboxEntry entry) {}

      @Override
      public Duration inFlightHorizon() {
        return horizon;
      }

      @Override
      public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
        deadlineSink.set(deadline);
        var confirmed = new java.util.LinkedHashSet<OutboxEntryId>();
        for (OutboxEntry e : entries) {
          confirmed.add(e.id());
        }
        return new BatchResult(confirmed, new java.util.LinkedHashMap<>());
      }
    };
  }

  @Test
  void publishDeadline_isCappedToLeaseMinusHorizon_whenHorizonLargeRelativeToLease() {
    // In-flight batch-tail coupling, restated at the Kafka max-block
    // defaults: 240s lease, 180s Kafka horizon (max.block.ms 60s + delivery.timeout.ms 120s). The
    // publish deadline is capped at lease - maxHorizon = 60s from the claim base — the same 60s
    // budget as before the horizon correction, since BOTH numbers moved by 60s — NOT the uncapped
    // 0.8*lease = 192s. So the LAST entry a batch may start still has >= the 180s horizon of lease
    // left for its worst-case in-flight duration to resolve before the lease could expire and a
    // second relay reclaim it (a duplicate + same-aggregate reorder).
    var store = afStore(java.time.Clock.systemUTC(), Duration.ofSeconds(240));
    store.save(OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event"));
    var capturedDeadline = new java.util.concurrent.atomic.AtomicReference<Instant>();
    var publisher = deadlineCapturingPublisher(Duration.ofSeconds(180), capturedDeadline);

    var before = Instant.now();
    OutboxPoller.builder()
        .outboxStore(store)
        .publisher(publisher)
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .build()
        .processBatch();

    var deadline = capturedDeadline.get();
    assertNotNull(deadline, "publisher must have received a lease-bounded deadline");
    assertTrue(
        deadline.isAfter(before.plusSeconds(58)),
        "deadline must be ~lease-horizon (60s), was "
            + Duration.between(before, deadline).toSeconds()
            + "s");
    assertTrue(
        deadline.isBefore(before.plusSeconds(64)),
        "deadline must be CAPPED at lease-horizon (60s), NOT the uncapped 0.8*lease (192s), was "
            + Duration.between(before, deadline).toSeconds()
            + "s");
  }

  @Test
  void publishDeadline_usesFractionOfLease_whenPublisherHorizonZero() {
    // Regression guard for the common/no-Kafka case: a zero-horizon publisher (the
    // default) leaves the deadline at the original 0.8*lease. With the shipped 240s lease that is
    // ~192s from the claim base — min(0.8*240, 240 - 0) = 192s — unchanged by the in-flight
    // horizon cap.
    var store = afStore(java.time.Clock.systemUTC(), Duration.ofSeconds(240));
    store.save(OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event"));
    var capturedDeadline = new java.util.concurrent.atomic.AtomicReference<Instant>();
    var publisher = deadlineCapturingPublisher(Duration.ZERO, capturedDeadline);

    var before = Instant.now();
    OutboxPoller.builder()
        .outboxStore(store)
        .publisher(publisher)
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .build()
        .processBatch();

    var deadline = capturedDeadline.get();
    assertNotNull(deadline, "publisher must have received a lease-bounded deadline");
    assertTrue(
        deadline.isAfter(before.plusSeconds(188)),
        "zero-horizon deadline must be ~0.8*lease (192s), was "
            + Duration.between(before, deadline).toSeconds()
            + "s");
    assertTrue(
        deadline.isBefore(before.plusSeconds(196)),
        "zero-horizon deadline must stay at 0.8*lease (192s), not be capped, was "
            + Duration.between(before, deadline).toSeconds()
            + "s");
  }

  @Test
  void publishDeadline_usesFractionOfLease_whenHorizonSmallRelativeToLease() {
    // Middle case: a RabbitMQ-class horizon (30s) against the
    // shipped 240s lease. min(0.8*240, 240-30) = min(192, 210) = 192 — the FRACTION branch must
    // win here, not the horizon branch, proving min() picks the correct side in both directions
    // (not just the two extremes: horizon=180s "big" and horizon=0 "zero" tested above).
    var store = afStore(java.time.Clock.systemUTC(), Duration.ofSeconds(240));
    store.save(OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event"));
    var capturedDeadline = new java.util.concurrent.atomic.AtomicReference<Instant>();
    var publisher = deadlineCapturingPublisher(Duration.ofSeconds(30), capturedDeadline);

    var before = Instant.now();
    OutboxPoller.builder()
        .outboxStore(store)
        .publisher(publisher)
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .build()
        .processBatch();

    var deadline = capturedDeadline.get();
    assertNotNull(deadline, "publisher must have received a lease-bounded deadline");
    assertTrue(
        deadline.isAfter(before.plusSeconds(188)),
        "small-horizon deadline must use the fraction (192s), not the horizon cap (210s), was "
            + Duration.between(before, deadline).toSeconds()
            + "s");
    assertTrue(
        deadline.isBefore(before.plusSeconds(196)),
        "small-horizon deadline must stay at 0.8*lease (192s), was "
            + Duration.between(before, deadline).toSeconds()
            + "s");
  }

  // ── Transport failures are non-counting; only per-entry rejections terminal-FAIL
  // ────────

  /**
   * Wraps a publisher body with an explicit {@code ENTRY} classification. The ladder tests below
   * exercise counting semantics, which require a publisher that EXPLICITLY attributes its failures
   * to the message — the SPI default is now the non-counting {@code TRANSPORT} (see {@code
   * defaultClassifiedFailure_isNonCounting_neverMassTerminalFails}).
   */
  private static org.streamrune.core.outbox.OutboxPublisher entryClassified(
      org.streamrune.core.outbox.OutboxPublisher delegate) {
    return new org.streamrune.core.outbox.OutboxPublisher() {
      @Override
      public void publish(OutboxEntry entry) throws Exception {
        delegate.publish(entry);
      }

      @Override
      public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
        return delegate.publishBatch(entries, deadline);
      }

      @Override
      public FailureKind classifyFailure(Exception failure) {
        return FailureKind.ENTRY;
      }
    };
  }

  @Test
  void defaultClassifiedFailure_isNonCounting_neverMassTerminalFails() {
    // A lambda publisher CANNOT override classifyFailure
    // (@FunctionalInterface), so whatever the default returns governs every custom
    // publisher-as-lambda in the wild. Under the old ENTRY default a broker outage seen through
    // such a publisher burned the ladder and terminal-FAILed the whole backlog in ~maxAttempts
    // cycles — permanent gap + silent same-aggregate reorder once the FAILED head stopped gating.
    // The default must be the non-counting TRANSPORT: retried forever with capped backoff, PENDING
    // throughout, delivery-failed metric never fired.
    var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("lambda-outage-1"), "{}", "Event"));
    var metrics = new RecordingMetrics();
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                entry -> { // lambda: the SPI default classification applies
                  throw new RuntimeException("broker unavailable");
                })
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(10), 2.0, false)) // ladder of 3
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .metrics(metrics)
            .build();

    for (int cycle = 0; cycle < 8; cycle++) { // far past maxAttempts=3
      poller.processBatch();
      clock.advance(Duration.ofMinutes(2)); // elapse the (capped) backoff so it is re-claimed
    }

    var entry = store.all().get(0);
    assertEquals(
        OutboxStatus.PENDING,
        entry.status(),
        "a default-classified (lambda-publisher) failure must be non-counting TRANSPORT — never"
            + " terminal-FAIL, however many cycles it repeats");
    assertEquals(
        0,
        entry.attempts(),
        "the persisted attempt counter must stay frozen for the default classification");
    assertEquals(
        0,
        metrics.deliveryFailed,
        "the delivery-failed metric must not fire for a default-classified failure");
  }

  /** Publisher that always fails, classifying every failure as the given {@link FailureKind}. */
  private static org.streamrune.core.outbox.OutboxPublisher alwaysFailingPublisher(
      org.streamrune.core.outbox.OutboxPublisher.FailureKind kind) {
    return new org.streamrune.core.outbox.OutboxPublisher() {
      @Override
      public void publish(OutboxEntry entry) {
        throw new RuntimeException("broker unavailable");
      }

      @Override
      public FailureKind classifyFailure(Exception failure) {
        return kind;
      }
    };
  }

  @Test
  void transportFailure_isNonCounting_neverTerminalFailsPastTheLadder() {
    // A broker-wide TRANSPORT outage (broker restart, network partition) must NEVER
    // terminal-FAIL the backlog, however long it lasts — otherwise a routine 30-60s restart FAILs
    // all in-flight traffic and a FAILED head silently un-gates its successors. Repeated far past
    // the retry ladder, the entry stays PENDING (retrying with capped backoff), never FAILED.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-13T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("outage-1"), "{}", "Event"));
    var metrics = new RecordingMetrics();
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                alwaysFailingPublisher(
                    org.streamrune.core.outbox.OutboxPublisher.FailureKind.TRANSPORT))
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(10), 2.0, false)) // ladder of 3
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .metrics(metrics)
            .build();

    for (int cycle = 0; cycle < 8; cycle++) { // far past maxAttempts=3
      poller.processBatch();
      clock.advance(Duration.ofMinutes(2)); // elapse the (capped) backoff so it is re-claimed
    }

    var entry = store.all().get(0);
    assertEquals(
        OutboxStatus.PENDING,
        entry.status(),
        "a transport outage must never terminal-FAIL the entry, no matter how many cycles it spans");
    assertEquals(
        0,
        metrics.deliveryFailed,
        "the delivery-failed metric must not fire for a transport outage");
  }

  /** Publisher that always fails, with a switchable {@link FailureKind} across cycles. */
  private static final class SwitchablePublisher
      implements org.streamrune.core.outbox.OutboxPublisher {
    volatile org.streamrune.core.outbox.OutboxPublisher.FailureKind kind;

    SwitchablePublisher(org.streamrune.core.outbox.OutboxPublisher.FailureKind initial) {
      this.kind = initial;
    }

    @Override
    public void publish(OutboxEntry entry) {
      throw new RuntimeException("fail");
    }

    @Override
    public FailureKind classifyFailure(Exception failure) {
      return kind;
    }
  }

  @Test
  void transportOutageThenEntryRejection_entryStillGetsItsFullLadder() {
    // TRANSPORT failures are non-counting, but they must ALSO not persist an inflated
    // attempt counter — otherwise outage-accrued attempts retroactively terminal-FAIL the entry on
    // its FIRST later ENTRY-classified rejection, before its real ladder is exhausted (permanent
    // gap + silent reorder). After a long outage the persisted counter stays flat, and the entry
    // still gets its full maxAttempts ENTRY ladder.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-13T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("e-1"), "{}", "Event"));
    var metrics = new RecordingMetrics();
    var publisher =
        new SwitchablePublisher(org.streamrune.core.outbox.OutboxPublisher.FailureKind.TRANSPORT);
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(publisher)
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(10), 2.0, false)) // ladder of 3
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .metrics(metrics)
            .build();

    // 15-cycle broker outage — every failure TRANSPORT (non-counting).
    for (int cycle = 0; cycle < 15; cycle++) {
      poller.processBatch();
      clock.advance(Duration.ofMinutes(2)); // elapse the capped backoff so it is re-claimed
    }
    var afterOutage = store.all().get(0);
    assertEquals(
        OutboxStatus.PENDING, afterOutage.status(), "a transport outage never terminal-FAILs");
    assertEquals(
        0,
        afterOutage.attempts(),
        "transport failures must NOT persist an incremented attempt counter");

    // Broker returns but the entry is now rejected per-ENTRY (e.g. unroutable). The FIRST such
    // failure must NOT terminal-FAIL — the outage did not pre-burn the ladder.
    publisher.kind = org.streamrune.core.outbox.OutboxPublisher.FailureKind.ENTRY;
    poller.processBatch();
    var afterFirstEntry = store.all().get(0);
    assertEquals(
        OutboxStatus.PENDING,
        afterFirstEntry.status(),
        "one entry rejection after a long outage must not terminal-FAIL — the ladder was intact");
    assertEquals(1, afterFirstEntry.attempts());
    assertEquals(0, metrics.deliveryFailed);

    // The entry terminal-FAILs only after its full maxAttempts ENTRY ladder.
    clock.advance(Duration.ofMinutes(2));
    poller.processBatch(); // ENTRY attempt 2
    clock.advance(Duration.ofMinutes(2));
    poller.processBatch(); // ENTRY attempt 3 → terminal
    var terminal = store.all().get(0);
    assertEquals(OutboxStatus.FAILED, terminal.status());
    assertEquals(3, terminal.attempts());
    assertEquals(1, metrics.deliveryFailed);
  }

  @Test
  void entryRejection_stillTerminalFailsAtMaxAttempts() {
    // The contrast to the transport case: a per-ENTRY rejection (unroutable/nack/serialization)
    // still burns the attempts ladder and terminal-FAILs at maxAttempts, so a genuinely poison
    // entry
    // is not retried forever.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-13T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("poison-1"), "{}", "Event"));
    var metrics = new RecordingMetrics();
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                alwaysFailingPublisher(
                    org.streamrune.core.outbox.OutboxPublisher.FailureKind.ENTRY))
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(10), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .metrics(metrics)
            .build();

    for (int cycle = 0; cycle < 3; cycle++) { // exactly maxAttempts cycles
      poller.processBatch();
      clock.advance(Duration.ofMinutes(2));
    }

    var entry = store.all().get(0);
    assertEquals(
        OutboxStatus.FAILED,
        entry.status(),
        "a per-entry rejection must still terminal-FAIL once the attempts ladder is exhausted");
    assertEquals(3, entry.attempts());
    assertEquals(
        1, metrics.deliveryFailed, "terminal FAILED emits the delivery-failed metric once");
  }

  @Test
  void retryBackoff_isCappedForLongTransportOutage() {
    // Transport retries use capped backoff so a long outage cannot push next_retry_at
    // arbitrarily far out (which would delay recovery for minutes/hours after the broker returns).
    // After many cycles the scheduled backoff must be at most the 60s cap, not initialDelay * 2^n.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-13T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("cap-1"), "{}", "Event"));
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                alwaysFailingPublisher(
                    org.streamrune.core.outbox.OutboxPublisher.FailureKind.TRANSPORT))
            .retryPolicy(new RetryPolicy(50, Duration.ofSeconds(1), 2.0, false)) // would blow up
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();

    for (int cycle = 0; cycle < 12; cycle++) { // 1s * 2^11 would be ~34 min uncapped
      if (cycle > 0) {
        clock.advance(
            Duration.ofMinutes(10)); // elapse the prior (capped) backoff BEFORE re-claiming
      }
      poller.processBatch();
    }

    var entry = store.all().get(0);
    // The clock was NOT advanced after the last markRetry, so clock.now() is that markRetry instant
    // and next_retry_at = clock.now() + backoff. The scheduled backoff must be <= the 60s cap,
    // proving it did not grow to initialDelay * 2^n.
    Duration scheduledBackoff = Duration.between(clock.instant(), entry.nextRetryAt());
    assertTrue(
        scheduledBackoff.compareTo(Duration.ofSeconds(61)) <= 0,
        "transport retry backoff must be capped (~60s), was " + scheduledBackoff);
  }

  // ── The TRANSPORT retry CADENCE escalates while the TERMINAL ladder stays frozen ───

  /**
   * The backoff the relay just scheduled for {@code id}: {@code markRetry} stamps {@code
   * next_retry_at = storeClock.now() + backoff}, so as long as the clock has not been advanced
   * since that cycle, this is exactly the deferral the poller asked for.
   */
  private static Duration stampedBackoff(InMemoryOutboxStore store, MutableClock clock, String id) {
    var entry =
        store.all().stream().filter(e -> e.id().value().equals(id)).findFirst().orElseThrow();
    assertNotNull(entry.nextRetryAt(), "entry " + id + " has no scheduled retry");
    return Duration.between(clock.instant(), entry.nextRetryAt());
  }

  /** Publisher that fails (kind {@code TRANSPORT}) only while {@code down}; otherwise publishes. */
  private static final class FlakyTransportPublisher
      implements org.streamrune.core.outbox.OutboxPublisher {
    volatile boolean down = true;

    @Override
    public void publish(OutboxEntry entry) {
      if (down) {
        throw new RuntimeException("broker down");
      }
    }

    @Override
    public FailureKind classifyFailure(Exception failure) {
      return FailureKind.TRANSPORT;
    }
  }

  @Test
  void transportOutage_retryCadenceEscalatesTowardTheCap_notPinnedAtInitialDelay() {
    // The PERSISTED attempts counter for non-counting TRANSPORT
    // failures (correct — an outage must never terminal-FAIL the backlog), but the backoff was
    // computed from that same frozen counter — delayForAttempt(entry.attempts() + 1) — so its input
    // never grew. For the whole outage the relay re-claimed and re-failed the entire batch every
    // ~1s (one WARN + two UPDATEs per entry per cycle: log storm, sustained DB write load,
    // lock-step
    // reconnects) instead of escalating toward the 60s cap the docs promise. The retry CADENCE must
    // escalate across cycles even though the TERMINAL ladder stays frozen.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("cadence-1"), "{}", "Event"));
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                alwaysFailingPublisher(
                    org.streamrune.core.outbox.OutboxPublisher.FailureKind.TRANSPORT))
            // jitter OFF so the ramp is exact: 1s, 2s, 4s, 8s, 16s, 32s, then the 60s cap.
            .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(1))
            .build();

    var stamped = new ArrayList<Duration>();
    for (int cycle = 0; cycle < 10; cycle++) {
      poller.processBatch();
      Duration backoff = stampedBackoff(store, clock, "cadence-1");
      stamped.add(backoff);
      clock.advance(backoff.plusMillis(1)); // elapse exactly that deferral, then poll again
    }

    assertEquals(
        Duration.ofSeconds(1),
        stamped.get(0),
        "the FIRST transport retry must still fire at initialDelay (fast re-probe): " + stamped);
    assertTrue(
        stamped.get(1).compareTo(stamped.get(0)) > 0,
        "the retry cadence must ESCALATE across the cycles of a sustained outage, not stay pinned at"
            + " initialDelay every cycle: "
            + stamped);
    for (int i = 1; i < stamped.size(); i++) {
      assertTrue(
          stamped.get(i).compareTo(stamped.get(i - 1)) >= 0,
          "the cadence must never shrink while the outage continues: " + stamped);
      assertTrue(
          stamped.get(i).compareTo(Duration.ofSeconds(60)) <= 0,
          "the cadence must stay clamped at the 60s cap so recovery is never delayed further: "
              + stamped);
    }
    assertEquals(
        Duration.ofSeconds(60),
        stamped.get(stamped.size() - 1),
        "a sustained outage must saturate at the 60s cap: " + stamped);

    // Pin: the escalating cadence must NOT be bought by counting attempts.
    var entry = store.all().get(0);
    assertEquals(
        OutboxStatus.PENDING, entry.status(), "a transport outage never terminal-FAILs the entry");
    assertEquals(
        0,
        entry.attempts(),
        "the persisted attempt counter must stay frozen for TRANSPORT failures — the"
            + " cadence escalation must not advance the terminal ladder");
  }

  @Test
  void escalatedTransportBackoff_stillJitters_soRelaysDoNotRetryInLockStep() {
    // Escalation must keep going through RetryPolicy.delayForAttempt so its [0.5,1.5)
    // jitter still applies to every rung of the ramp — de-correlating the retry instants of several
    // relays (and of a batch's entries). A fix that doubled the previous backoff itself, or read it
    // back from next_retry_at, would produce a perfectly lock-step ramp instead.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("jitter-1"), "{}", "Event"));
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                alwaysFailingPublisher(
                    org.streamrune.core.outbox.OutboxPublisher.FailureKind.TRANSPORT))
            .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, true)) // jitter ON
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(1))
            .build();

    // Six cycles: unjittered bases 1s, 2s, 4s, 8s, 16s, 32s — all still below the 60s cap even at
    // the 1.5x jitter ceiling (32 * 1.5 = 48s), so the clamp never hides the jitter here.
    var stamped = new ArrayList<Duration>();
    var bases = new ArrayList<Duration>();
    for (int cycle = 0; cycle < 6; cycle++) {
      poller.processBatch();
      Duration backoff = stampedBackoff(store, clock, "jitter-1");
      stamped.add(backoff);
      bases.add(Duration.ofSeconds(1L << cycle));
      clock.advance(backoff.plusMillis(1));
    }

    for (int i = 0; i < stamped.size(); i++) {
      Duration base = bases.get(i);
      assertTrue(
          stamped.get(i).compareTo(base.dividedBy(2)) >= 0
              && stamped.get(i).compareTo(base.multipliedBy(3).dividedBy(2)) <= 0,
          "cycle "
              + i
              + " backoff must be the escalating base "
              + base
              + " +/- jitter: "
              + stamped);
    }
    assertTrue(
        stamped.stream().anyMatch(d -> !bases.contains(d)),
        "at least one rung must differ from its unjittered base — jitter must survive the"
            + " escalation: "
            + stamped);
    // Escalation is unambiguous across the ramp even at the worst jitter draw: the 6th rung's floor
    // (32s * 0.5 = 16s) is far above the 1st rung's ceiling (1s * 1.5 = 1.5s).
    assertTrue(
        stamped.get(5).compareTo(stamped.get(0)) > 0,
        "the jittered cadence must still escalate across the outage: " + stamped);
  }

  @Test
  void transportStreak_resetsAfterASuccessfulPublish_soAFreshOutageStartsFastAgain() {
    // The escalation input is a relay-local streak of consecutive
    // all-transport-failed
    // cycles. Once the broker is back and a publish confirms, the streak must reset so the NEXT
    // failure starts at initialDelay again — a saturated relay must not keep deferring fresh
    // failures by a minute after recovery.
    var clock = MutableClock.startingAt(Instant.parse("2026-07-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("recover-a"), "{}", "Event"));
    var publisher = new FlakyTransportPublisher();
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(publisher)
            .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(1))
            .build();

    // Sustained outage: saturate the cadence at the cap.
    Duration backoff = Duration.ZERO;
    for (int cycle = 0; cycle < 8; cycle++) {
      poller.processBatch();
      backoff = stampedBackoff(store, clock, "recover-a");
      clock.advance(backoff.plusMillis(1));
    }
    assertEquals(Duration.ofSeconds(60), backoff, "the outage saturated the cadence at the cap");

    // Broker returns: the pending entry is delivered — a confirmed publish proves the transport
    // works, so the outage streak is over.
    publisher.down = false;
    poller.processBatch();
    assertEquals(
        OutboxStatus.DELIVERED,
        store.all().stream()
            .filter(e -> e.id().value().equals("recover-a"))
            .findFirst()
            .orElseThrow()
            .status(),
        "the recovered broker must deliver the backlog");

    // A brand-new entry now hits a fresh transport failure: it must be deferred by initialDelay,
    // NOT by the pre-recovery escalated cadence.
    publisher.down = true;
    store.save(OutboxEntry.pending(OutboxEntryId.of("recover-b"), "{}", "Event"));
    poller.processBatch();

    assertEquals(
        Duration.ofSeconds(1),
        stampedBackoff(store, clock, "recover-b"),
        "a successful cycle must reset the transport streak so a fresh failure retries fast again");
  }

  @Test
  void inFlightFailures_doNotFeedTheTransportStreak_theirCadenceIsLeaseReclaimPaced() {
    // Sibling semantics: an IN_FLIGHT (unconfirmed hand-off) failure is NOT released for
    // retry at all — it stays claimed and is redelivered only by the lease-reclaim path, so its
    // cadence is paced by the claim lease and it must not escalate the TRANSPORT streak. Otherwise
    // a Kafka-class publisher timing out its confirms would silently push unrelated transport
    // failures straight to the 60s cap.
    var lease = Duration.ofSeconds(30);
    var clock = MutableClock.startingAt(Instant.parse("2026-07-29T00:00:00Z"));
    var store = afStore(clock, lease);
    store.save(OutboxEntry.pending(OutboxEntryId.of("inflight-a"), "{}", "Event"));
    var publisher =
        new SwitchablePublisher(org.streamrune.core.outbox.OutboxPublisher.FailureKind.IN_FLIGHT);
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(publisher)
            .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(1))
            .build();

    // Six cycles of unconfirmed hand-offs, each re-attempted only after the claim lease lapses.
    for (int cycle = 0; cycle < 6; cycle++) {
      poller.processBatch();
      clock.advance(lease.plusSeconds(1)); // the abandoned claim is reclaimed next loadPending
    }
    var inflight =
        store.all().stream()
            .filter(e -> e.id().value().equals("inflight-a"))
            .findFirst()
            .orElseThrow();
    assertEquals(
        0, inflight.attempts(), "an in-flight failure must not burn a retry attempt (unchanged)");

    // A genuine transport failure now arrives: it must start at initialDelay — the in-flight cycles
    // contributed no escalation.
    publisher.kind = org.streamrune.core.outbox.OutboxPublisher.FailureKind.TRANSPORT;
    store.save(OutboxEntry.pending(OutboxEntryId.of("inflight-b"), "{}", "Event"));
    poller.processBatch();

    assertEquals(
        Duration.ofSeconds(1),
        stampedBackoff(store, clock, "inflight-b"),
        "IN_FLIGHT failures must not feed the TRANSPORT streak — their retry cadence is paced by"
            + " lease-reclaim, a different mechanism");
  }

  @Test
  void entryRejectionBackoff_followsItsOwnCountedLadder_notTheTransportStreak() {
    // Sibling semantics: the streak shapes ONLY the cadence of non-counting TRANSPORT
    // retries. A per-ENTRY rejection keeps its counted ladder untouched — its backoff stays
    // delayForAttempt(attempts) — so a relay that just weathered an outage does not defer a poison
    // entry's next attempt by a minute (nor reach terminal FAILED any later than before).
    var clock = MutableClock.startingAt(Instant.parse("2026-07-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("mixed-1"), "{}", "Event"));
    var publisher =
        new SwitchablePublisher(org.streamrune.core.outbox.OutboxPublisher.FailureKind.TRANSPORT);
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(publisher)
            .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(1))
            .build();

    for (int cycle = 0; cycle < 8; cycle++) { // saturate the transport cadence at the cap
      poller.processBatch();
      clock.advance(stampedBackoff(store, clock, "mixed-1").plusMillis(1));
    }

    // The broker is reachable again but rejects THIS entry on its own merits (unroutable/nack).
    publisher.kind = org.streamrune.core.outbox.OutboxPublisher.FailureKind.ENTRY;
    poller.processBatch();

    var entry =
        store.all().stream()
            .filter(e -> e.id().value().equals("mixed-1"))
            .findFirst()
            .orElseThrow();
    assertEquals(1, entry.attempts(), "the first ENTRY rejection burns ladder attempt 1");
    assertEquals(
        Duration.ofSeconds(1),
        stampedBackoff(store, clock, "mixed-1"),
        "an ENTRY rejection keeps its own counted ladder backoff — delayForAttempt(attempts) — and"
            + " must not inherit the transport outage streak");
  }

  // ── A poisoned entry amid healthy traffic escalates its OWN transport cadence ────────

  /** Publisher that TRANSPORT-fails every {@code poison-*} entry and confirms everything else. */
  private static final class PoisonedHeadPublisher
      implements org.streamrune.core.outbox.OutboxPublisher {
    @Override
    public void publish(OutboxEntry entry) {
      if (entry.id().value().startsWith("poison")) {
        // Id-free on purpose. The old message embedded entry.id().value() in its tail, so a
        // `warn.contains("poison-1")` assertion passed even with the WARN's entry-id argument
        // removed entirely — the exception's OWN message tail smuggled the id in and the
        // assertion never actually pinned the poller's new representative-entry parameter. An
        // id-free message forces that argument to be the only source of the id substring.
        throw new RuntimeException("connection refused");
      }
    }

    @Override
    public FailureKind classifyFailure(Exception failure) {
      return FailureKind.TRANSPORT;
    }
  }

  @Test
  void poisonedEntryAmidHealthyTraffic_escalatesItsOwnRetryCadence_notPinnedAtInitialDelay() {
    // The relay-wide transport streak resets whenever ANY delivery confirms in a cycle,
    // and TRANSPORT keeps the persisted attempts frozen — so before the per-entry
    // streak, a single entry that keeps failing TRANSPORT amid healthy confirming traffic computed
    // backoffAttempt = 1 every time and re-published at initialDelay (~1s) FOREVER: an unbounded
    // fast retry the escalation promise (and the SPI's default-TRANSPORT classification)
    // made the fate of EVERY unclassified custom-publisher
    // failure. The entry's own consecutive-transport streak must escalate its cadence toward the
    // 60s cap even while other traffic confirms every cycle.
    var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("poison-1"), "{}", "Event"));
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(new PoisonedHeadPublisher())
            // jitter OFF so the ramp is exact: 1s, 2s, 4s, 8s, 16s, 32s, then the 60s cap.
            .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(1))
            .build();

    var stamped = new ArrayList<Duration>();
    for (int cycle = 0; cycle < 10; cycle++) {
      // Fresh healthy traffic EVERY cycle: each cycle has a confirmed delivery, which resets the
      // relay-wide streak — the exact condition that used to pin the poisoned entry at 1s.
      store.save(OutboxEntry.pending(OutboxEntryId.of("healthy-" + cycle), "{}", "Event"));
      poller.processBatch();
      Duration backoff = stampedBackoff(store, clock, "poison-1");
      stamped.add(backoff);
      clock.advance(backoff.plusMillis(1)); // elapse exactly that deferral, then poll again
    }

    long delivered = store.all().stream().filter(e -> e.status() == OutboxStatus.DELIVERED).count();
    assertEquals(10, delivered, "the healthy traffic must confirm every cycle: " + stamped);

    assertEquals(
        Duration.ofSeconds(1),
        stamped.get(0),
        "the FIRST transport retry must still fire at initialDelay (fast re-probe): " + stamped);
    assertTrue(
        stamped.get(1).compareTo(stamped.get(0)) > 0,
        "a repeatedly transport-failing ENTRY must escalate its cadence even while other traffic"
            + " confirms — healthy confirms must not pin the poisoned head at initialDelay forever"
            + ": "
            + stamped);
    for (int i = 1; i < stamped.size(); i++) {
      assertTrue(
          stamped.get(i).compareTo(stamped.get(i - 1)) >= 0,
          "the poisoned entry's cadence must never shrink while it keeps failing: " + stamped);
      assertTrue(
          stamped.get(i).compareTo(Duration.ofSeconds(60)) <= 0,
          "the cadence must stay clamped at the 60s cap: " + stamped);
    }
    assertEquals(
        Duration.ofSeconds(60),
        stamped.get(stamped.size() - 1),
        "a persistently poisoned entry must saturate at the 60s cap: " + stamped);

    // Pin: the per-entry escalation is relay-local, never bought by counting attempts.
    var poison =
        store.all().stream()
            .filter(e -> e.id().value().equals("poison-1"))
            .findFirst()
            .orElseThrow();
    assertEquals(OutboxStatus.PENDING, poison.status(), "TRANSPORT never terminal-FAILs");
    assertEquals(
        0,
        poison.attempts(),
        "the persisted attempt counter must stay frozen for TRANSPORT failures — the"
            + " per-entry cadence escalation must not advance the terminal ladder");
  }

  @Test
  void confirmedDelivery_clearsThatEntrysTransportStreak() {
    // The per-entry streak is evidence about THAT entry's delivery path. Once the entry
    // confirms, the evidence is spent — the tracked streak must be dropped so the bounded map
    // holds only entries that are still failing, and a later fresh failure re-probes fast.
    var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("poison-1"), "{}", "Event"));
    var publisher = new FlakyTransportPublisher();
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(publisher)
            .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(1))
            .build();

    for (int cycle = 0; cycle < 3; cycle++) {
      poller.processBatch();
      clock.advance(stampedBackoff(store, clock, "poison-1").plusMillis(1));
    }
    assertEquals(
        1,
        poller.trackedTransportStreakEntries(),
        "the failing entry must be tracked while it keeps failing transport");

    publisher.down = false;
    poller.processBatch(); // the entry confirms
    assertEquals(
        0,
        poller.trackedTransportStreakEntries(),
        "a confirmed delivery must clear that entry's tracked transport streak");
  }

  @Test
  void perEntryTransportStreaks_areBounded_soTheTrackingMapCannotGrowWithoutLimit() {
    // Memory bound: the per-entry streak map is relay-local soft state and must stay
    // bounded however many distinct entries fail transport over the relay's lifetime. 1100
    // distinct transport-failing entries in one claimed batch must leave at most the 1024-entry
    // cap tracked (oldest evicted; an evicted entry merely restarts its escalation from the
    // relay-wide streak floor — safe, just one fast re-probe).
    var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    for (int i = 0; i < 1100; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("poison-" + i), "{}", "Event"));
    }
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(new PoisonedHeadPublisher())
            .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
            .batchSize(2000)
            .pollInterval(Duration.ofSeconds(1))
            .build();

    poller.processBatch();

    assertEquals(
        1024,
        poller.trackedTransportStreakEntries(),
        "the per-entry streak map must evict down to its 1024-entry cap");
  }

  // ── The aggregated transport WARN names an entry and does not over-claim scope ───────

  /**
   * Attaches a logback {@code ListAppender} to the {@code OutboxPoller} logger so a test can assert
   * on the aggregated transport WARN's actual text — its diagnosability IS the log line. Callers
   * must detach via {@link #detachFromOutboxPollerLogger}.
   */
  private static ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>
      captureOutboxPollerLogs() {
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(OutboxPoller.class);
    var appender =
        new ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>();
    appender.start();
    logger.addAppender(appender);
    return appender;
  }

  private static void detachFromOutboxPollerLogger(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender) {
    ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(OutboxPoller.class))
        .detachAppender(appender);
  }

  /** The formatted WARN lines the appender captured that mention a TRANSPORT outcome. */
  private static List<String> transportWarnLines(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .filter(m -> m.contains("TRANSPORT"))
        .toList();
  }

  /** The formatted messages the appender captured at exactly {@code level}. */
  private static List<String> linesAt(
      ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent> appender,
      ch.qos.logback.classic.Level level) {
    return appender.list.stream()
        .filter(e -> e.getLevel() == level)
        .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
        .toList();
  }

  @Test
  void aggregatedTransportWarn_namesARepresentativeEntry_andDoesNotClaimBrokerWide_whenMixed() {
    // With the SPI's default-TRANSPORT classification, the aggregated WARN may describe
    // ONE poisoned entry amid healthy confirming traffic — not a broker-wide outage. Asserting
    // "broker-wide TRANSPORT failure" while naming no entry sends the operator to check the
    // broker when the actionable fact is WHICH entry is stuck. The WARN must name a
    // representative entry id and must not claim broker-wide scope when other claimed entries
    // resolved differently.
    var appender = captureOutboxPollerLogs();
    try {
      var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
      var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
      store.save(OutboxEntry.pending(OutboxEntryId.of("poison-1"), "{}", "Event"));
      store.save(OutboxEntry.pending(OutboxEntryId.of("healthy-1"), "{}", "Event"));
      var poller =
          OutboxPoller.builder()
              .outboxStore(store)
              .publisher(new PoisonedHeadPublisher())
              .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
              .batchSize(10)
              .pollInterval(Duration.ofSeconds(1))
              .build();

      poller.processBatch();

      var warns = transportWarnLines(appender);
      assertEquals(1, warns.size(), "exactly one aggregated transport WARN per cycle: " + warns);
      String warn = warns.get(0);
      assertTrue(
          warn.contains("poison-1"),
          "the aggregated transport WARN must name a representative failing entry so the operator"
              + " can find the poisoned head: "
              + warn);
      assertTrue(
          warn.contains("NOT broker-wide"),
          "when other claimed entries confirmed, the WARN must say the failure is not provably"
              + " broker-wide: "
              + warn);
      assertFalse(
          warn.contains("a broker-wide TRANSPORT failure"),
          "the WARN must not assert a broker-wide outage when only some claimed entries failed"
              + " transport: "
              + warn);
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  @Test
  void aggregatedTransportWarn_keepsTheBrokerWideClaim_whenEveryClaimedEntryFailedTransport() {
    // Counterpart: when EVERY claimed entry failed transport this cycle, the broker-wide
    // wording is the honest and useful claim — keep it, and still name a representative entry.
    var appender = captureOutboxPollerLogs();
    try {
      var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
      var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
      store.save(OutboxEntry.pending(OutboxEntryId.of("poison-a"), "{}", "Event"));
      store.save(OutboxEntry.pending(OutboxEntryId.of("poison-b"), "{}", "Event"));
      var poller =
          OutboxPoller.builder()
              .outboxStore(store)
              .publisher(new PoisonedHeadPublisher())
              .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
              .batchSize(10)
              .pollInterval(Duration.ofSeconds(1))
              .build();

      poller.processBatch();

      var warns = transportWarnLines(appender);
      assertEquals(1, warns.size(), "exactly one aggregated transport WARN per cycle: " + warns);
      String warn = warns.get(0);
      assertTrue(
          warn.contains("a broker-wide TRANSPORT failure"),
          "when every claimed entry failed transport, the broker-wide claim is honest and must"
              + " stay: "
              + warn);
      assertTrue(
          warn.contains("poison-"),
          "the broker-wide WARN must still name a representative entry: " + warn);
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  // ── Broker-wide is keyed on RESOLVED outcomes, not raw claimed-entry count ─────────

  /**
   * Publisher that TRANSPORT-fails the first {@code failLimit} entries and leaves every later one
   * entirely unattempted — the deadline-truncation shape, but deterministic and without sleeping,
   * mirroring {@link #TruncatingPublisher} but with the attempted prefix TRANSPORT-failing instead
   * of confirming.
   */
  private static final class TruncatingTransportFailurePublisher
      implements org.streamrune.core.outbox.OutboxPublisher {
    private final int failLimit;

    TruncatingTransportFailurePublisher(int failLimit) {
      this.failLimit = failLimit;
    }

    @Override
    public void publish(OutboxEntry entry) {
      throw new AssertionError("the publishBatch override must be used, not per-entry publish");
    }

    @Override
    public FailureKind classifyFailure(Exception failure) {
      return FailureKind.TRANSPORT;
    }

    @Override
    public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
      var failed = new java.util.LinkedHashMap<OutboxEntryId, Exception>();
      for (int i = 0; i < Math.min(failLimit, entries.size()); i++) {
        failed.put(entries.get(i).id(), new RuntimeException("connect timed out"));
      }
      return new BatchResult(java.util.Set.of(), failed);
    }
  }

  @Test
  void aggregatedTransportWarn_keepsTheBrokerWideClaim_whenTruncationLeftEntriesUnattempted() {
    // brokerWide used to be `transportFailures == pending.size()`, which counts unattempted
    // (deadline-truncated) entries in the denominator. During a REAL blocking-connect
    // outage that also truncates the batch, every entry the cycle actually reached fails
    // transport, but transportFailures < pending.size() because the truncated tail was never
    // attempted — so the WARN wrongly said "NOT broker-wide ... possibly a single poisoned entry",
    // steering the operator AWAY from the broker that is, in fact, fully down. The fix keys
    // broker-wide on RESOLVED outcomes only (confirmed + failed), excluding the never-attempted
    // tail, mirroring the relay-streak's own evidence rule.
    var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    for (int i = 0; i < 6; i++) {
      store.save(OutboxEntry.pending(OutboxEntryId.of("e" + i), "{}", "Event"));
    }
    var appender = captureOutboxPollerLogs();
    try {
      var poller =
          OutboxPoller.builder()
              .outboxStore(store)
              // 3 of 6 entries are attempted and TRANSPORT-fail; 3 are truncated (never attempted)
              // — exactly the deadline-truncated shape mid broker-wide outage.
              .publisher(new TruncatingTransportFailurePublisher(3))
              .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
              .batchSize(10)
              .pollInterval(Duration.ofSeconds(1))
              .build();

      poller.processBatch();

      var warns = transportWarnLines(appender);
      assertEquals(1, warns.size(), "exactly one aggregated transport WARN per cycle: " + warns);
      String warn = warns.get(0);
      assertTrue(
          warn.contains("a broker-wide TRANSPORT failure"),
          "every entry the cycle actually resolved failed transport — the truncated"
              + " (never-attempted) tail carries no evidence either way and must not defeat the"
              + " broker-wide claim: "
              + warn);
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  @Test
  void aggregatedTransportWarn_hedgesOffBrokerWideClaim_whenOnlyOneEntryResolvedThisCycle() {
    // With exactly one resolved entry, `transportFailures == resolvedEntries` is vacuously
    // true — a lone poisoned entry would log "a broker-wide TRANSPORT failure ... 1 of 1" every
    // cycle, which is not a claim the relay has any evidence for (it never saw a second entry this
    // cycle to compare against). The WARN must fall back to the softened, non-broker-wide wording
    // whenever only one entry was claimed and resolved.
    var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("poison-1"), "{}", "Event"));
    var appender = captureOutboxPollerLogs();
    try {
      var poller =
          OutboxPoller.builder()
              .outboxStore(store)
              .publisher(new PoisonedHeadPublisher())
              .retryPolicy(new RetryPolicy(10, Duration.ofSeconds(1), 2.0, false))
              .batchSize(10)
              .pollInterval(Duration.ofSeconds(1))
              .build();

      poller.processBatch();

      var warns = transportWarnLines(appender);
      assertEquals(1, warns.size(), "exactly one aggregated transport WARN per cycle: " + warns);
      String warn = warns.get(0);
      assertFalse(
          warn.contains("a broker-wide TRANSPORT failure"),
          "a single resolved entry is not evidence of a broker-wide outage — the relay never saw"
              + " a second entry this cycle to corroborate it: "
              + warn);
      assertTrue(
          warn.contains("poison-1"),
          "the softened WARN must still name the representative entry: " + warn);
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  // ── Sibling log sites must sanitize entry.id() like the aggregated WARN already does ────

  @Test
  void entryRejectionWarn_sanitizesTheEntryId_stripsInjectedLineBreaks() {
    // entry.id() is a caller-supplied idempotency key (user-derived content, same as the
    // aggregate-seeded ids the aggregated-WARN test already calls out) and must never be rendered
    // RAW
    // into a log record — an unstripped '\n' followed by text shaped like a genuine log line lets
    // an attacker forge a fabricated entry indistinguishable from a real one. This pins
    // one of the five sibling sites the aggregated transport WARN's own LogSanitizer guard did not
    // cover: the per-entry ENTRY-retry WARN.
    var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    String forgedId = "poison-1\nERROR forged log line — operator, drop everything";
    store.save(OutboxEntry.pending(OutboxEntryId.of(forgedId), "{}", "Event"));
    var appender = captureOutboxPollerLogs();
    try {
      var poller =
          OutboxPoller.builder()
              .outboxStore(store)
              .publisher(
                  alwaysFailingPublisher(
                      org.streamrune.core.outbox.OutboxPublisher.FailureKind.ENTRY))
              .retryPolicy(new RetryPolicy(3, Duration.ofMillis(10), 2.0, false))
              .batchSize(10)
              .pollInterval(Duration.ofSeconds(60))
              .build();

      poller.processBatch();

      var warns =
          appender.list.stream()
              .filter(e -> e.getLevel() == ch.qos.logback.classic.Level.WARN)
              .map(ch.qos.logback.classic.spi.ILoggingEvent::getFormattedMessage)
              .filter(m -> m.contains("Outbox delivery failed for entry"))
              .toList();
      assertEquals(1, warns.size(), "exactly one ENTRY-retry WARN: " + warns);
      String warn = warns.get(0);
      assertFalse(
          warn.contains("\nERROR forged log line"),
          "the injected newline must be stripped before this entry id reaches the log sink —"
              + " LogSanitizer.sanitizeForLog must guard the ENTRY-retry WARN's entry-id argument"
              + ": "
              + warn);
      assertTrue(
          warn.contains("poison-1"),
          "sanitizing must strip only the unsafe code points, not the whole identifier: " + warn);
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  // ── A null-error failure from publishBatch is non-counting, never a terminal ladder ──

  /**
   * Publisher whose {@code publishBatch} reports every entry as failed with a {@code null}
   * exception — the "nacked / timed-out without a local exception" shape the {@code recordFailure}
   * javadoc described for a null error. {@code classifyFailure} fails the test if it is ever
   * invoked: there is no exception to classify, so the poller must bin the failure without calling
   * it (a call would mean the poller passed {@code null} into an SPI method whose contract says
   * never-null).
   */
  private static final class NullErrorBatchPublisher
      implements org.streamrune.core.outbox.OutboxPublisher {
    @Override
    public void publish(OutboxEntry entry) {
      throw new AssertionError("the publishBatch override must be used, not per-entry publish");
    }

    @Override
    public BatchResult publishBatch(List<OutboxEntry> entries, java.time.Instant deadline) {
      var failed = new java.util.LinkedHashMap<OutboxEntryId, Exception>();
      for (OutboxEntry e : entries) {
        failed.put(e.id(), null);
      }
      return new BatchResult(java.util.Set.of(), failed);
    }

    @Override
    public FailureKind classifyFailure(Exception failure) {
      throw new AssertionError(
          "classifyFailure must never be reached for a null error — there is no exception to"
              + " classify");
    }
  }

  @Test
  void nullErrorFromPublishBatch_isNonCountingTransport_neverTerminalFails() {
    // The recordFailure javadoc explicitly described a null error as "nacked / timed-out
    // without a local exception" — the exact broker-side/unresolved shapes now classified as
    // non-counting. A custom publishBatch
    // following that javadoc and mapping such failures to null must NOT walk the counting ladder
    // to terminal FAILED — that reproduces the exact mass-FAIL that was removed for typed failures.
    // A
    // null error is unclassifiable, so it takes the unclassifiable default: non-counting TRANSPORT
    // (NOT IN_FLIGHT — holding the claim on a shape the wiring never declared would trip the
    // zero-horizon violation guard by design).
    var clock = MutableClock.startingAt(Instant.parse("2026-08-29T00:00:00Z"));
    var store = afStore(clock, InMemoryOutboxStore.DEFAULT_CLAIM_LEASE);
    store.save(OutboxEntry.pending(OutboxEntryId.of("nullerr-1"), "{}", "Event"));
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(new NullErrorBatchPublisher())
            .retryPolicy(new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(1))
            .build();

    for (int cycle = 0; cycle < 6; cycle++) { // twice the 3-attempt ladder
      poller.processBatch();
      if (store.all().get(0).status() != OutboxStatus.PENDING) {
        break; // terminal — the assertions below report the regression
      }
      clock.advance(stampedBackoff(store, clock, "nullerr-1").plusMillis(1));
    }

    var entry = store.all().get(0);
    assertEquals(
        OutboxStatus.PENDING,
        entry.status(),
        "a null-error failure must never terminal-FAIL the entry — it is the nack/timeout shape"
            + " the SPI reclassified as non-counting");
    assertEquals(
        0,
        entry.attempts(),
        "the persisted attempt counter must stay frozen for a null-error failure — non-counting,"
            + " same as an unclassifiable typed TRANSPORT failure (frozen-counter semantics)");
  }

  // ── Lost-lease benign-skip contract ──────────────────────────────────────

  /**
   * A store double that hands out one entry to claim, then reports every mark* as a LOST LEASE
   * (markDelivered/markFailed/markRetry → false, markDeliveredAll → 0), as if another relay
   * recorded the outcome under a stolen lease. The poller must treat this as a benign no-op — it
   * must NOT throw out of processBatch.
   */
  private org.streamrune.core.outbox.OutboxStore lostLeaseStore(
      java.util.List<OutboxEntry> handOut) {
    return new org.streamrune.core.outbox.OutboxStore() {
      @Override
      public String claimedBy() {
        return "outbox-poller-identity";
      }

      @Override
      public void save(OutboxEntry entry) {}

      @Override
      public List<OutboxEntry> loadPending(int limit) {
        var batch = List.copyOf(handOut);
        handOut.clear(); // hand out once, then nothing (avoid a tight re-loop under start())
        return batch;
      }

      @Override
      public boolean markDelivered(OutboxEntryId id, String claimedBy) {
        return false; // lease lost
      }

      @Override
      public int markDeliveredAll(Collection<OutboxEntryId> ids, String claimedBy) {
        return 0; // none transitioned — every id's lease was lost
      }

      @Override
      public boolean markRetry(
          OutboxEntryId id, int attempts, String lastError, Duration backoff, String claimedBy) {
        return false;
      }

      @Override
      public boolean markFailed(
          OutboxEntryId id, int attempts, String lastError, String claimedBy) {
        return false;
      }

      @Override
      public void delete(OutboxEntryId id) {}

      @Override
      public List<OutboxEntry> findByStatus(
          org.streamrune.core.outbox.OutboxStatus status, int limit) {
        return List.of();
      }

      @Override
      public boolean resetFailedToPending(OutboxEntryId id) {
        return false;
      }

      @Override
      public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
        return java.util.Optional.empty();
      }

      @Override
      public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
        return false;
      }

      @Override
      public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
        // hands out null-aggregate entries, which only an AVAILABILITY_FIRST channel holds
        return org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST;
      }

      @Override
      public java.time.Duration claimLease() {
        return java.time.Duration.ofMinutes(4);
      }
    };
  }

  @Test
  void processBatch_lostLeaseOnDelivered_isBenignNoOp_doesNotThrow() {
    var handOut = new ArrayList<OutboxEntry>();
    handOut.add(OutboxEntry.pending(OutboxEntryId.of("lost-1"), "{}", "Event"));
    var poller =
        OutboxPoller.builder()
            .outboxStore(lostLeaseStore(handOut))
            .publisher(
                entry -> published.add(entry)) // publishes fine → confirmed → markDeliveredAll
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();

    // markDeliveredAll returns 0 (all leases lost) — the poller must swallow it, not throw.
    assertDoesNotThrow(poller::processBatch);
    assertEquals(1, published.size(), "the entry was still published");
  }

  @Test
  void processBatch_lostLeaseOnFailure_isBenignNoOp_doesNotThrow() {
    var handOut = new ArrayList<OutboxEntry>();
    handOut.add(OutboxEntry.pending(OutboxEntryId.of("lost-2"), "{}", "Event"));
    var poller =
        OutboxPoller.builder()
            .outboxStore(lostLeaseStore(handOut))
            .publisher(
                entry -> {
                  throw new RuntimeException("delivery failed"); // forces recordFailure → markRetry
                })
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();

    // markRetry returns false (lease lost) — recordFailure must swallow it, not throw.
    assertDoesNotThrow(poller::processBatch);
  }

  // ── Terminal FAILED metric + retry/terminal accounting ──────────────────────

  /** Records recordOutboxDeliveryFailed() calls; all other methods are no-ops. */
  private static final class DeliveryFailedMetrics
      implements org.streamrune.core.StreamRuneMetrics {
    int deliveryFailed = 0;

    @Override
    public void recordOutboxDeliveryFailed() {
      deliveryFailed++;
    }
  }

  @Test
  void terminalFailure_emitsDeliveryFailedMetric() throws Exception {
    // maxAttempts=2, 1ms backoff: attempt1 → markRetry (no metric), attempt2 → terminal markFailed
    // → recordOutboxDeliveryFailed exactly once.
    var id = OutboxEntryId.of("metric-fail");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    var metrics = new DeliveryFailedMetrics();
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                entryClassified(
                    entry -> {
                      throw new RuntimeException("always fails");
                    }))
            .retryPolicy(new RetryPolicy(2, Duration.ofMillis(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .metrics(metrics)
            .build();

    poller.processBatch(); // attempt 1 — markRetry, no metric
    assertEquals(0, metrics.deliveryFailed, "a scheduled retry must NOT emit the terminal metric");
    Thread.sleep(20); // let the backoff window elapse so loadPending re-claims it
    poller.processBatch(); // attempt 2 — terminal markFailed → metric

    assertEquals(
        1, metrics.deliveryFailed, "terminal FAILED emits recordOutboxDeliveryFailed once");
    assertTrue(
        store.all().stream().anyMatch(e -> e.status() == OutboxStatus.FAILED),
        "the entry is terminal FAILED");
  }

  @Test
  void scheduledRetry_doesNotEmitDeliveryFailedMetric() {
    // maxAttempts=5: a single failed delivery schedules a retry, never terminal → no metric.
    store.save(OutboxEntry.pending(OutboxEntryId.of("retry-only"), "{}", "Event"));
    var metrics = new DeliveryFailedMetrics();
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                entryClassified(
                    entry -> {
                      throw new RuntimeException("transient");
                    }))
            .retryPolicy(new RetryPolicy(5, Duration.ofSeconds(60), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .metrics(metrics)
            .build();

    poller.processBatch();
    assertEquals(0, metrics.deliveryFailed, "a scheduled retry must not emit the terminal metric");
    assertEquals(OutboxStatus.PENDING, store.all().get(0).status(), "entry is retrying (PENDING)");
  }

  // ── At-least-once crash window: publish SUCCEEDS then the process dies BEFORE markDelivered ──

  /**
   * Wraps a store so its {@code markDeliveredAll} throws on the first call, modeling a relay that
   * publishes successfully and then crashes in the tiny window before it can record DELIVERED (the
   * load-bearing at-least-once mechanic the docs rely on for "exactly-once combined with receiver
   * idempotency"). Every other call delegates to {@code inner}; {@code claimedBy} delegates too so
   * the entry stays claimed under the same relay identity the poller marks with.
   */
  private static final class CrashBeforeDeliveredStore
      implements org.streamrune.core.outbox.OutboxStore {
    private final org.streamrune.core.outbox.OutboxStore inner;
    private boolean crashed = false;

    CrashBeforeDeliveredStore(org.streamrune.core.outbox.OutboxStore inner) {
      this.inner = inner;
    }

    @Override
    public String claimedBy() {
      return inner.claimedBy();
    }

    @Override
    public void save(OutboxEntry entry) {
      inner.save(entry);
    }

    @Override
    public List<OutboxEntry> loadPending(int limit) {
      return inner.loadPending(limit);
    }

    @Override
    public int markDeliveredAll(Collection<OutboxEntryId> ids, String claimedBy) {
      if (!crashed) {
        crashed = true;
        // Publish already succeeded; the DELIVERED write never lands — the process dies here.
        throw new RuntimeException("simulated crash after publish, before DELIVERED");
      }
      return inner.markDeliveredAll(ids, claimedBy);
    }

    @Override
    public boolean markDelivered(OutboxEntryId id, String claimedBy) {
      return inner.markDelivered(id, claimedBy);
    }

    @Override
    public boolean markRetry(
        OutboxEntryId id, int attempts, String lastError, Duration backoff, String claimedBy) {
      return inner.markRetry(id, attempts, lastError, backoff, claimedBy);
    }

    @Override
    public boolean markFailed(OutboxEntryId id, int attempts, String lastError, String claimedBy) {
      return inner.markFailed(id, attempts, lastError, claimedBy);
    }

    @Override
    public void delete(OutboxEntryId id) {
      inner.delete(id);
    }

    @Override
    public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
      return inner.findByStatus(status, limit);
    }

    @Override
    public boolean resetFailedToPending(OutboxEntryId id) {
      return inner.resetFailedToPending(id);
    }

    @Override
    public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
      return inner.findById(id);
    }

    @Override
    public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
      return inner.skipFailed(id, skippedBy, reason);
    }

    @Override
    public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
      return inner.orderingMode();
    }

    @Override
    public java.time.Duration claimLease() {
      return java.time.Duration.ofMinutes(4);
    }
  }

  @Test
  void crashAfterPublishBeforeDelivered_entryStaysClaimable_andIsRedelivered_atLeastOnce() {
    // A short-lease store on a controllable clock so an abandoned (crashed) claim can be reclaimed
    // without real sleeping — modeling the production lease-reclaim path for a dead relay.
    var lease = Duration.ofSeconds(30);
    var clock = MutableClock.startingAt(Instant.parse("2026-07-06T00:00:00Z"));
    var innerStore = afStore(clock, lease);
    innerStore.save(OutboxEntry.pending(OutboxEntryId.of("crash-1"), "{}", "OrderPlaced"));

    var delivered = new ArrayList<OutboxEntryId>();

    // Relay #1: publishes, then crashes before it can mark DELIVERED.
    var crashingStore = new CrashBeforeDeliveredStore(innerStore);
    var relay1 =
        OutboxPoller.builder()
            .outboxStore(crashingStore)
            .publisher(entry -> delivered.add(entry.id()))
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();

    // The crash surfaces out of processBatch (the DELIVERED write threw).
    assertThrows(RuntimeException.class, relay1::processBatch);

    // The entry was published exactly once so far, but its outcome was NEVER recorded: it is not
    // DELIVERED, not FAILED — it stays IN_PROGRESS (a live, now-orphaned claim), i.e. NOT LOST.
    assertEquals(1, delivered.size(), "relay #1 published the entry once before crashing");
    var afterCrash = innerStore.all().get(0);
    assertEquals(
        OutboxStatus.IN_PROGRESS,
        afterCrash.status(),
        "a crash before markDelivered must leave the entry claimed (not DELIVERED, not FAILED, not "
            + "lost)");
    assertNotEquals(OutboxStatus.DELIVERED, afterCrash.status());
    assertNotEquals(OutboxStatus.FAILED, afterCrash.status());

    // The crashed relay's claim lease lapses (its process is gone). A recovered/second relay polls.
    clock.advance(lease.plusSeconds(1));

    // Relay #2 on the same underlying store (same claim identity). loadPending reclaims the expired
    // claim back to PENDING, re-claims it, and REDELIVERS — the receiver now sees the duplicate it
    // must idempotently handle. This is at-least-once, not at-most-once.
    var relay2 =
        OutboxPoller.builder()
            .outboxStore(innerStore)
            .publisher(entry -> delivered.add(entry.id()))
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .build();
    relay2.processBatch();

    assertEquals(
        2,
        delivered.size(),
        "the entry must be REDELIVERED after the crash — at-least-once, so the receiver sees the "
            + "duplicate");
    assertEquals(OutboxEntryId.of("crash-1"), delivered.get(0));
    assertEquals(OutboxEntryId.of("crash-1"), delivered.get(1), "same entry delivered twice");
    assertEquals(
        OutboxStatus.DELIVERED,
        innerStore.all().get(0).status(),
        "after the recovering relay records the outcome the entry is finally DELIVERED");
  }

  // ── Observability metrics (delivery-failed counter + backlog gauge) ───────────────

  /** Recording {@link org.streamrune.core.StreamRuneMetrics} double for the observability tests. */
  static final class RecordingMetrics implements org.streamrune.core.StreamRuneMetrics {
    int deliveryFailed = 0;
    int inFlightHorizonViolations = 0;
    long lastBacklog = -1;
    long lastInFlight = -1;
    volatile long lastRelayDegradation = -1;
    volatile int relayDegradationSamples = 0;
    volatile long lastBlockedAggregates = -1;
    volatile long lastBlockageAge = -1;

    @Override
    public void recordOutboxDeliveryFailed() {
      deliveryFailed++;
    }

    @Override
    public void recordOutboxInFlightHorizonViolation() {
      inFlightHorizonViolations++;
    }

    @Override
    public void recordOutboxBacklog(long pending) {
      lastBacklog = pending;
    }

    @Override
    public void recordOutboxInFlight(long inFlight) {
      lastInFlight = inFlight;
    }

    @Override
    public void recordOutboxRelayDegradation(long consecutiveFailures) {
      lastRelayDegradation = consecutiveFailures;
      relayDegradationSamples++;
    }

    @Override
    public void recordOutboxBlockedAggregates(long failedAggregates) {
      lastBlockedAggregates = failedAggregates;
    }

    @Override
    public void recordOutboxBlockageAge(long seconds) {
      lastBlockageAge = seconds;
    }
  }

  private OutboxPoller pollerWithMetrics(
      RetryPolicy retryPolicy,
      org.streamrune.core.outbox.OutboxPublisher publisher,
      org.streamrune.core.StreamRuneMetrics metrics) {
    return OutboxPoller.builder()
        .outboxStore(store)
        .publisher(publisher)
        .retryPolicy(retryPolicy)
        .batchSize(10)
        .pollInterval(Duration.ofSeconds(60))
        .metrics(metrics)
        .build();
  }

  @Test
  void processBatch_emits_deliveryFailed_metric_on_terminal_failure() throws InterruptedException {
    // The documented alert metric streamrune.outbox.delivery_failed must actually fire when an
    // entry exhausts its retry ladder and reaches terminal FAILED — this is what the auto-configs
    // silently never wired.
    var metrics = new RecordingMetrics();
    store.save(OutboxEntry.pending(OutboxEntryId.of("dead"), "{}", "Event"));
    var retryPolicy = new RetryPolicy(2, Duration.ofMillis(1), 2.0, false);
    var poller =
        pollerWithMetrics(
            retryPolicy,
            entryClassified(
                entry -> {
                  throw new RuntimeException("always fails");
                }),
            metrics);

    poller.processBatch(); // attempt 1 — markRetry (not terminal)
    assertEquals(0, metrics.deliveryFailed, "delivery-failed must not fire before retries exhaust");
    Thread.sleep(20); // let the backoff window elapse so the entry is re-claimed
    poller.processBatch(); // attempt 2 — terminal markFailed

    assertEquals(
        OutboxStatus.FAILED,
        store.all().get(0).status(),
        "entry must reach terminal FAILED after exhausting attempts");
    assertEquals(
        1, metrics.deliveryFailed, "streamrune.outbox.delivery_failed must fire exactly once");
  }

  @Test
  void processBatch_samples_pending_backlog_gauge() {
    // The poller samples the PENDING backlog each cycle (before claiming) and reports it as
    // the streamrune.outbox.pending gauge, giving operators a real-time view of a lagging relay.
    var metrics = new RecordingMetrics();
    store.save(OutboxEntry.pending(OutboxEntryId.of("p1"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("p2"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("p3"), "{}", "Event"));
    var poller =
        pollerWithMetrics(
            new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false),
            entry -> {
              throw new RuntimeException(
                  "broker down"); // keep them PENDING (backlog stays visible)
            },
            metrics);

    poller.processBatch();

    assertEquals(
        3,
        metrics.lastBacklog,
        "backlog gauge must reflect the 3 PENDING entries sampled at cycle");
  }

  @Test
  void processBatch_samples_in_flight_gauge_which_is_the_only_signal_for_a_post_handoff_outage() {
    // The observability blind spot the phase-based classification opened. An
    // IN_FLIGHT failure HOLDS the claim (no markRetry), so the backlog parks in IN_PROGRESS: the
    // PENDING gauge falls to 0, delivery_failed never fires (IN_FLIGHT burns no attempt) and the
    // transport streak stays 0. Without a dedicated gauge an operator alerting on
    // streamrune.outbox.pending sees a sustained outage as "healthy".
    var metrics = new RecordingMetrics();
    store.save(OutboxEntry.pending(OutboxEntryId.of("if1"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("if2"), "{}", "Event"));
    store.save(OutboxEntry.pending(OutboxEntryId.of("if3"), "{}", "Event"));
    var inFlightPublisher =
        new org.streamrune.core.outbox.OutboxPublisher() {
          @Override
          public void publish(OutboxEntry entry) {
            throw new RuntimeException(new java.io.IOException("connection reset after send"));
          }

          @Override
          public FailureKind classifyFailure(Exception failure) {
            return FailureKind.IN_FLIGHT;
          }
        };
    var poller =
        pollerWithMetrics(
            new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false), inFlightPublisher, metrics);

    poller.processBatch(); // claims all three; every publish fails post-hand-off
    poller.processBatch(); // samples the gauges with the batch parked IN_PROGRESS

    assertEquals(
        3,
        store.all().stream().filter(e -> e.status() == OutboxStatus.IN_PROGRESS).count(),
        "all three must be held IN_PROGRESS for lease-reclaim");
    assertEquals(3, metrics.lastInFlight, "the in_flight gauge must expose the parked backlog");
    assertEquals(
        0,
        metrics.lastBacklog,
        "the PENDING gauge deliberately reads 0 — that is the blind spot the in_flight gauge"
            + " closes, and why it is a separate series rather than being folded in");
    assertEquals(0, metrics.deliveryFailed, "IN_FLIGHT never terminal-fails");
  }

  @Test
  void processBatch_samples_relay_degradation_gauge_each_cycle() {
    // The relay degradation gauge is sampled every cycle so a stalled-but-alive relay is
    // visible on the metrics endpoint. A direct processBatch() call runs outside the resilient
    // loop,
    // so consecutiveFailures() is 0 here — the point is that the gauge is reported at all.
    var metrics = new RecordingMetrics();
    store.save(OutboxEntry.pending(OutboxEntryId.of("p1"), "{}", "Event"));
    var poller =
        pollerWithMetrics(new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false), e -> {}, metrics);

    poller.processBatch();

    assertTrue(metrics.relayDegradationSamples >= 1, "relay degradation gauge must be sampled");
    assertEquals(0, metrics.lastRelayDegradation, "no failures on a clean cycle");
  }

  @Test
  void relay_degradation_gauge_rises_under_sustained_store_failure() throws InterruptedException {
    // End-to-end: a relay whose store keeps throwing stays alive and retries with backoff;
    // consecutiveFailures() climbs and is surfaced on the degradation gauge (previously log-only).
    var metrics = new RecordingMetrics();
    var failingStore = new AlwaysFailingLoadStore();
    var poller =
        OutboxPoller.builder()
            .outboxStore(failingStore)
            .publisher(e -> {})
            .retryPolicy(new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofMillis(5))
            .metrics(metrics)
            .build();
    poller.start();
    try {
      long deadline = System.currentTimeMillis() + 5000;
      while (metrics.lastRelayDegradation < 1 && System.currentTimeMillis() < deadline) {
        Thread.sleep(20);
      }
      assertTrue(
          metrics.lastRelayDegradation >= 1,
          "a stalled relay's degradation must surface on the gauge (was "
              + metrics.lastRelayDegradation
              + ")");
      assertTrue(poller.isAlive(), "the relay thread must stay alive while degraded");
    } finally {
      poller.close();
    }
  }

  // ── A throwing metrics backend must abort neither the poll cycle nor a failure-
  // bookkeeping path ─────────────────────────────────────────────────────────────────────────────

  @Test
  void processBatch_survivesThrowingRelayDegradationGauge_andStillProcessesTheBatch() {
    // sampleObservability() called recordOutboxRelayDegradation BARE (a byte-identical twin
    // of the DLQ retry runner's degradation gauge, guarded the same way), and it runs
    // FIRST in every processBatch() — before loadPending claims anything. A throwing metrics
    // backend therefore aborted the ENTIRE cycle every time: no claim, no delivery, no retry,
    // indistinguishable from a dead relay except for the very gauge that never got the chance to
    // report it.
    var id = OutboxEntryId.of("degradation-throw");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    org.streamrune.core.StreamRuneMetrics throwing =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordOutboxRelayDegradation(long consecutiveFailures) {
            throw new IllegalStateException(
                "simulated metrics backend failure (degradation gauge)");
          }
        };
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(published::add)
            .retryPolicy(new RetryPolicy(3, Duration.ofMillis(50), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .metrics(throwing)
            .build();

    assertDoesNotThrow(poller::processBatch);

    assertEquals(
        1, published.size(), "the entry must still be published despite the metrics throw");
  }

  @Test
  void processBatch_survivesThrowingDeliveryFailedMetric_stillReachesTerminalFailed() {
    // recordOutboxDeliveryFailed ran bare inside recordFailure()'s terminal branch, AFTER
    // store.markFailed had already committed. A throwing metrics backend still propagated out of
    // processBatch()'s per-entry loop, silently skipping every remaining entry in the batch.
    var id = OutboxEntryId.of("delivery-failed-throw");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    org.streamrune.core.StreamRuneMetrics throwing =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordOutboxDeliveryFailed() {
            throw new IllegalStateException("simulated metrics backend failure (delivery failed)");
          }
        };
    var poller =
        OutboxPoller.builder()
            .outboxStore(store)
            .publisher(
                entryClassified(
                    entry -> {
                      throw new RuntimeException("always fails");
                    }))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .metrics(throwing)
            .build();

    assertDoesNotThrow(poller::processBatch); // maxAttempts=1: attempt 1 is immediately terminal

    assertEquals(
        OutboxStatus.FAILED,
        store.all().get(0).status(),
        "the entry must still reach terminal FAILED despite the metrics throw");
  }

  @Test
  void processBatch_survivesThrowingInFlightHorizonViolationMetric_stillHoldsClaimFailSafe() {
    // recordOutboxInFlightHorizonViolation ran bare inside reportZeroHorizonInFlight(),
    // called from recordFailure()'s IN_FLIGHT/zero-horizon branch. A throwing metrics backend
    // propagated out of recordFailure() and out of processBatch()'s per-entry loop, aborting the
    // fail-safe recording (never reaching the LOG.debug / FailureOutcome.IN_FLIGHT return) for this
    // entry and skipping every remaining entry in the batch this cycle.
    var id = OutboxEntryId.of("zero-horizon-throw");
    store.save(OutboxEntry.pending(id, "{}", "Event"));
    org.streamrune.core.StreamRuneMetrics throwing =
        new org.streamrune.core.StreamRuneMetrics() {
          @Override
          public void recordOutboxInFlightHorizonViolation() {
            throw new IllegalStateException(
                "simulated metrics backend failure (horizon violation)");
          }
        };
    var poller =
        pollerWithMetrics(
            new RetryPolicy(3, Duration.ofSeconds(1), 2.0, false),
            inFlightClassifyingZeroHorizon(),
            throwing);

    assertDoesNotThrow(poller::processBatch);

    assertEquals(
        OutboxStatus.IN_PROGRESS,
        store.all().get(0).status(),
        "fail SAFE: the claim must still be held for the full lease despite the metrics throw");
  }

  /** OutboxStore whose loadPending always throws, driving the relay into backoff-retry. */
  private static final class AlwaysFailingLoadStore
      implements org.streamrune.core.outbox.OutboxStore {
    @Override
    public void save(OutboxEntry entry) {}

    @Override
    public java.util.List<OutboxEntry> loadPending(int limit) {
      throw new org.streamrune.core.EventStoreException("store down");
    }

    @Override
    public String claimedBy() {
      return "test";
    }

    @Override
    public boolean markDelivered(OutboxEntryId id, String claimedBy) {
      return false;
    }

    @Override
    public java.util.List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
      return java.util.List.of();
    }

    @Override
    public boolean resetFailedToPending(OutboxEntryId id) {
      return false;
    }

    @Override
    public java.util.Optional<OutboxEntry> findById(OutboxEntryId id) {
      return java.util.Optional.empty();
    }

    @Override
    public boolean skipFailed(OutboxEntryId id, String skippedBy, String reason) {
      return false;
    }

    @Override
    public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
      return org.streamrune.core.outbox.OutboxOrderingMode.AVAILABILITY_FIRST;
    }

    @Override
    public boolean markFailed(OutboxEntryId id, int attempts, String error, String claimedBy) {
      return false;
    }

    @Override
    public boolean markRetry(
        OutboxEntryId id, int attempts, String error, Duration backoff, String claimedBy) {
      return false;
    }

    @Override
    public long countByStatus(OutboxStatus status) {
      throw new org.streamrune.core.EventStoreException("store down");
    }

    @Override
    public void delete(OutboxEntryId id) {}

    @Override
    public java.time.Duration claimLease() {
      return java.time.Duration.ofMinutes(4);
    }
  }

  // ── Ordering mode at build()/start(), mode-specific terminal ERROR, blockage gauges,
  // first-cycle legacy null-aggregate WARN ─────────────────────────────────────────────────────

  @Test
  void build_rejectsStoreThatInheritsThrowingOrderingModeDefault() {
    // A store that does not say which ordering it enforces fails at wiring, like the claimLease
    // precedent: the poller logs and reasons about the mode, so it must be stated.
    var noModeStore =
        new org.streamrune.test.ForwardingOutboxStore(afStore()) {
          @Override
          public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
            throw new UnsupportedOperationException("orderingMode is not implemented");
          }
        };
    var ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                OutboxPoller.builder()
                    .outboxStore(noModeStore)
                    .publisher(publisherWithHorizon(Duration.ofSeconds(10)))
                    .batchSize(10)
                    .pollInterval(Duration.ofSeconds(60))
                    .build());
    assertTrue(ex.getMessage().contains("orderingMode"), ex.getMessage());
  }

  @Test
  void build_rejectsStoreThatReturnsNullOrderingMode() {
    // A null mode breaks the SPI contract; an un-stubbed Mockito mock of OutboxStore returns
    // exactly that. Treating it as "not strict" would relay availability-first over a store whose
    // ordering the poller cannot know, so build() refuses it and names the offending store.
    var nullModeStore =
        new org.streamrune.test.ForwardingOutboxStore(afStore()) {
          @Override
          public org.streamrune.core.outbox.OutboxOrderingMode orderingMode() {
            return null;
          }
        };
    var builder =
        OutboxPoller.builder()
            .outboxStore(nullModeStore)
            .publisher(publisherWithHorizon(Duration.ofSeconds(10)))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60));
    var ex = assertThrows(IllegalStateException.class, builder::build);
    assertTrue(ex.getMessage().contains("orderingMode()"), ex.getMessage());
    assertTrue(ex.getMessage().contains(nullModeStore.getClass().getName()), ex.getMessage());
  }

  @Test
  void start_logsOrderingMode() throws Exception {
    // logback-test.xml keeps the root at WARN, so the capture only sees this INFO line when the
    // OutboxPoller logger is raised for the test's duration (restored in finally).
    var logger =
        (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(OutboxPoller.class);
    var previousLevel = logger.getLevel();
    logger.setLevel(ch.qos.logback.classic.Level.INFO);
    var appender = captureOutboxPollerLogs();
    try {
      var strict = strictStore();
      var poller =
          OutboxPoller.builder()
              .outboxStore(strict)
              .publisher(entry -> {})
              .pollInterval(Duration.ofSeconds(60))
              .build();
      poller.start();
      poller.close();
      var info = linesAt(appender, ch.qos.logback.classic.Level.INFO);
      assertTrue(
          info.stream()
              .anyMatch(
                  l ->
                      l.startsWith("Outbox relay started: orderingMode=STRICT_PER_AGGREGATE")
                          && l.contains("claimLease=PT4M")),
          info.toString());
    } finally {
      detachFromOutboxPollerLogger(appender);
      logger.setLevel(previousLevel);
    }
  }

  @Test
  void terminalFailed_strict_logsBlockedAggregate() {
    var appender = captureOutboxPollerLogs();
    try {
      var strict = strictStore();
      strict.save(
          OutboxEntry.pending(
              OutboxEntryId.of("bad-1"),
              "{}",
              "Event",
              StreamId.of(TYPE, AggregateId.of("agg-x"))));
      var poller =
          OutboxPoller.builder()
              .outboxStore(strict)
              .publisher(
                  entryClassified(
                      entry -> {
                        throw new RuntimeException("schema rejected");
                      }))
              .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 2.0, false))
              .batchSize(10)
              .pollInterval(Duration.ofSeconds(60))
              .build();
      poller.processBatch(); // attempt 1 of 1 → terminal FAILED
      var errors = linesAt(appender, ch.qos.logback.classic.Level.ERROR);
      assertEquals(1, errors.size(), errors.toString());
      assertTrue(errors.get(0).contains("of stream agg:agg-x"), errors.get(0));
      assertTrue(errors.get(0).contains("stream BLOCKED"), errors.get(0));
      assertTrue(
          errors.get(0).contains("replayed or skipped (OutboxFailedReplayer)"), errors.get(0));
      assertTrue(
          strict.loadPending(10).isEmpty(), "and the aggregate really is blocked for the relay");
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  @Test
  void terminalFailed_availabilityFirst_logsGap() {
    var appender = captureOutboxPollerLogs();
    try {
      store.save(
          OutboxEntry.pending(
              OutboxEntryId.of("bad-2"),
              "{}",
              "Event",
              StreamId.of(TYPE, AggregateId.of("agg-y"))));
      var poller =
          pollerWith(
              new RetryPolicy(1, Duration.ofMillis(1), 2.0, false),
              entryClassified(
                  entry -> {
                    throw new RuntimeException("schema rejected");
                  }));
      poller.processBatch();
      var errors = linesAt(appender, ch.qos.logback.classic.Level.ERROR);
      assertEquals(1, errors.size(), errors.toString());
      assertTrue(
          errors.get(0).contains("later entries of stream agg:agg-y continue"), errors.get(0));
      assertTrue(errors.get(0).contains("gap until replay"), errors.get(0));
      assertFalse(errors.get(0).contains("BLOCKED"), errors.get(0));
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  @Test
  void sampleObservability_recordsBlockedAggregatesAndAge() throws Exception {
    var strict = strictStore();
    var metrics = new RecordingMetrics();
    strict.save(
        OutboxEntry.pending(
            OutboxEntryId.of("h0"), "{}", "Event", StreamId.of(TYPE, AggregateId.of("agg-z"))));
    strict.save(
        OutboxEntry.pending(
            OutboxEntryId.of("h1"), "{}", "Event", StreamId.of(TYPE, AggregateId.of("agg-z"))));
    var poller =
        OutboxPoller.builder()
            .outboxStore(strict)
            .publisher(
                entryClassified(
                    entry -> {
                      throw new RuntimeException("poison");
                    }))
            .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 2.0, false))
            .batchSize(10)
            .pollInterval(Duration.ofSeconds(60))
            .metrics(metrics)
            .build();
    poller.processBatch(); // samples (0 blocked), then fails h0 terminally
    assertEquals(0, metrics.lastBlockedAggregates);
    assertEquals(0, metrics.lastBlockageAge);
    Thread.sleep(1100); // the age is in whole seconds
    poller.processBatch(); // samples: agg-z blocked, age >= 1s
    assertEquals(1, metrics.lastBlockedAggregates, "one blocked aggregate");
    assertTrue(
        metrics.lastBlockageAge >= 1, "age of the oldest FAILED row: " + metrics.lastBlockageAge);
    assertTrue(strict.skipFailed(OutboxEntryId.of("h0"), "ops", "poison"));
    poller.processBatch();
    assertEquals(0, metrics.lastBlockedAggregates, "released");
    assertEquals(0, metrics.lastBlockageAge, "0 when none");
  }

  @Test
  void sampleObservability_degradesWhenStoreCannotSampleBlockage_warnsOnceNamingTheConsequence() {
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    var noSample =
        new org.streamrune.test.ForwardingOutboxStore(afStore()) {
          @Override
          public BlockageSample sampleBlockage() {
            calls.incrementAndGet();
            throw new UnsupportedOperationException("no sampleBlockage");
          }
        };
    var metrics = new RecordingMetrics();
    var appender = captureOutboxPollerLogs();
    try {
      var poller =
          OutboxPoller.builder()
              .outboxStore(noSample)
              .publisher(entry -> {})
              .pollInterval(Duration.ofSeconds(60))
              .metrics(metrics)
              .build();
      poller.processBatch();
      poller.processBatch();
      var warns =
          linesAt(appender, ch.qos.logback.classic.Level.WARN).stream()
              .filter(l -> l.contains("sampleBlockage"))
              .toList();
      assertEquals(1, warns.size(), "WARN once, not DEBUG, not every cycle: " + warns);
      assertTrue(warns.get(0).contains("blockage alerts will NOT fire"), warns.get(0));
      assertTrue(warns.get(0).contains("AVAILABILITY_FIRST channel"), warns.get(0));
      assertEquals(1, calls.get(), "the second cycle does not call the store again");
      assertEquals(-1, metrics.lastBlockedAggregates, "gauges disabled");
      assertEquals(0, metrics.lastBacklog, "the backlog gauges are unaffected");
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  @Test
  void firstCycle_strict_warnsOnceAboutLegacyNullAggregateFailedRows_evenWithNoopMetrics()
      throws Exception {
    // FAILED rows with no aggregate_id can only have been written under
    // AVAILABILITY_FIRST. A strict relay over the same table WARNs once at its first poll and keeps
    // running; the WARN must not depend on a metrics bean being wired (NOOP here).
    store.save(OutboxEntry.pending(OutboxEntryId.of("legacy-1"), "{}", "Event"));
    pollerWith(
            new RetryPolicy(1, Duration.ofMillis(1), 2.0, false),
            entryClassified(
                entry -> {
                  throw new RuntimeException("poison");
                }))
        .processBatch(); // legacy-1 is FAILED, null aggregate
    var strict =
        store.withOrderingMode(org.streamrune.core.outbox.OutboxOrderingMode.STRICT_PER_AGGREGATE);
    var appender = captureOutboxPollerLogs();
    try {
      var poller =
          OutboxPoller.builder()
              .outboxStore(strict)
              .publisher(entry -> {})
              .pollInterval(Duration.ofSeconds(60))
              .build(); // metrics: NOOP
      poller.processBatch();
      poller.processBatch();
      var warns =
          linesAt(appender, ch.qos.logback.classic.Level.WARN).stream()
              .filter(l -> l.contains("FAILED entries with no aggregate_id"))
              .toList();
      assertEquals(1, warns.size(), warns.toString());
      assertTrue(warns.get(0).startsWith("Strict outbox channel holds 1 FAILED"), warns.get(0));
      assertTrue(warns.get(0).contains("not heads, never block"), warns.get(0));
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  @Test
  void firstCycle_availabilityFirst_doesNotWarnAboutNullAggregateFailedRows() {
    store.save(OutboxEntry.pending(OutboxEntryId.of("legacy-2"), "{}", "Event"));
    var appender = captureOutboxPollerLogs();
    try {
      var poller =
          pollerWith(
              new RetryPolicy(1, Duration.ofMillis(1), 2.0, false),
              entryClassified(
                  entry -> {
                    throw new RuntimeException("poison");
                  }));
      poller.processBatch();
      poller.processBatch();
      assertTrue(
          linesAt(appender, ch.qos.logback.classic.Level.WARN).stream()
              .noneMatch(l -> l.contains("no aggregate_id")),
          "null-aggregate rows are this mode's normal data");
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }

  @Test
  void legacyCheck_retriesNextCycleWhenTheReadThrows_armsAfterSuccess() {
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    var flaky =
        new org.streamrune.test.ForwardingOutboxStore(strictStore()) {
          @Override
          public BlockageSample sampleBlockage() {
            if (calls.incrementAndGet() == 1) {
              throw new IllegalStateException("db hiccup");
            }
            return new BlockageSample(0, null, 0);
          }
        };
    var appender = captureOutboxPollerLogs();
    try {
      var poller =
          OutboxPoller.builder()
              .outboxStore(flaky)
              .publisher(entry -> {})
              .pollInterval(Duration.ofSeconds(60))
              .build(); // metrics: NOOP, so the only sampleBlockage caller is the legacy check
      poller.processBatch(); // throws → not armed, WARN "retrying next cycle"
      poller.processBatch(); // succeeds → armed (0 legacy rows: no legacy WARN)
      poller.processBatch(); // armed → no call
      assertEquals(2, calls.get(), "retried once, then armed");
      var warns = linesAt(appender, ch.qos.logback.classic.Level.WARN);
      assertEquals(1, warns.size(), warns.toString());
      assertTrue(warns.get(0).contains("retrying next cycle"), warns.get(0));
    } finally {
      detachFromOutboxPollerLogger(appender);
    }
  }
}
