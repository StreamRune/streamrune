package org.streamrune.core.outbox;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

// OutboxStatus is in the same package (org.streamrune.core.outbox) — no import needed.

class OutboxStoreDefaultMethodsTest {

  @Test
  void markDeliveredAll_default_callsMarkDeliveredPerId_returningTransitionedCount() {
    List<OutboxEntryId> marked = new ArrayList<>();
    OutboxStore store =
        new OutboxStore() {
          public String claimedBy() {
            return "test";
          }

          public void save(OutboxEntry e) {}

          public List<OutboxEntry> loadPending(int limit) {
            return List.of();
          }

          public boolean markDelivered(OutboxEntryId id, String claimedBy) {
            marked.add(id);
            return true; // every per-id call transitions
          }

          public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
            return true;
          }

          public boolean markRetry(
              OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
            return true;
          }

          public void delete(OutboxEntryId id) {}

          public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
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
        };
    int transitioned =
        store.markDeliveredAll(List.of(OutboxEntryId.of("a"), OutboxEntryId.of("b")), "test");
    assertThat(marked).containsExactly(OutboxEntryId.of("a"), OutboxEntryId.of("b"));
    assertThat(transitioned).isEqualTo(2);
  }

  @Test
  void markDeliveredAll_default_countsOnlyTransitionedRows() {
    OutboxStore store =
        new OutboxStore() {
          public String claimedBy() {
            return "test";
          }

          public void save(OutboxEntry e) {}

          public List<OutboxEntry> loadPending(int limit) {
            return List.of();
          }

          public boolean markDelivered(OutboxEntryId id, String claimedBy) {
            // Only "a" still holds the lease; "b" was stolen — a benign no-op.
            return id.value().equals("a");
          }

          public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
            return true;
          }

          public boolean markRetry(
              OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
            return true;
          }

          public void delete(OutboxEntryId id) {}

          public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
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
        };
    int transitioned =
        store.markDeliveredAll(List.of(OutboxEntryId.of("a"), OutboxEntryId.of("b")), "test");
    assertThat(transitioned).isEqualTo(1);
  }

  @Test
  void claimLease_default_failsLoud() {
    // A store that does not override claimLease() must FAIL LOUD, not report a
    // plausible-looking 4-minute guess the OutboxPoller's lease-sizing guard would then validate
    // as if it were the store's real reclaim window. OutboxPoller.build() reads this before any
    // poll (see OutboxPollerTest.build_rejectsStoreThatInheritsThrowingClaimLeaseDefault), so an
    // un-overriding store fails at wiring, not silently in production. A store that deliberately
    // runs without deadline-bounding overrides claimLease() to return a non-positive Duration.
    OutboxStore store =
        new OutboxStore() {
          public String claimedBy() {
            return "test";
          }

          public void save(OutboxEntry e) {}

          public List<OutboxEntry> loadPending(int limit) {
            return List.of();
          }

          public boolean markDelivered(OutboxEntryId id, String claimedBy) {
            return true;
          }

          public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
            return true;
          }

          public boolean markRetry(
              OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
            return true;
          }

          public void delete(OutboxEntryId id) {}

          public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
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
        };
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, store::claimLease);
  }

  @Test
  void deleteDelivered_default_throwsUnsupported() {
    OutboxStore store =
        new OutboxStore() {
          public String claimedBy() {
            return "test";
          }

          public void save(OutboxEntry e) {}

          public List<OutboxEntry> loadPending(int limit) {
            return List.of();
          }

          public boolean markDelivered(OutboxEntryId id, String claimedBy) {
            return true;
          }

          public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
            return true;
          }

          public boolean markRetry(
              OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
            return true;
          }

          public void delete(OutboxEntryId id) {}

          public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
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
        };
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, () -> store.deleteDelivered(Instant.now()));
  }

  @Test
  void orderingMode_default_failsLoud() {
    // A store that does not say which ordering it enforces cannot be trusted to enforce it
    // (claimLease precedent). OutboxPoller.build() reads this before the first poll.
    OutboxStore store = minimalStore();
    var ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            UnsupportedOperationException.class, store::orderingMode);
    assertThat(ex.getMessage()).contains("orderingMode").contains("STRICT_PER_AGGREGATE");
  }

  @Test
  void sampleBlockage_default_throwsUnsupported() {
    OutboxStore store = minimalStore();
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, store::sampleBlockage);
  }

  @Test
  void deleteSkipped_default_throwsUnsupported() {
    OutboxStore store = minimalStore();
    org.junit.jupiter.api.Assertions.assertThrows(
        UnsupportedOperationException.class, () -> store.deleteSkipped(Instant.now()));
  }

  /** The least an OutboxStore can be: every abstract method, no default overridden. */
  private static OutboxStore minimalStore() {
    return new OutboxStore() {
      public String claimedBy() {
        return "test";
      }

      public void save(OutboxEntry e) {}

      public List<OutboxEntry> loadPending(int limit) {
        return List.of();
      }

      public boolean markDelivered(OutboxEntryId id, String claimedBy) {
        return true;
      }

      public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
        return true;
      }

      public boolean markRetry(OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
        return true;
      }

      public void delete(OutboxEntryId id) {}

      public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
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
    };
  }

  /** One markRetry invocation, captured argument-for-argument. */
  private record RetryCall(
      OutboxEntryId id, int attempts, String error, Duration backoff, String claimedBy) {}

  /** A store that records markRetry calls and reports which ids still hold their lease. */
  private static OutboxStore retryRecordingStore(
      List<RetryCall> calls, java.util.Set<String> held) {
    return new OutboxStore() {
      public String claimedBy() {
        return "test";
      }

      public void save(OutboxEntry e) {}

      public List<OutboxEntry> loadPending(int limit) {
        return List.of();
      }

      public boolean markDelivered(OutboxEntryId id, String claimedBy) {
        return true;
      }

      public boolean markFailed(OutboxEntryId id, int a, String e, String claimedBy) {
        return true;
      }

      public boolean markRetry(OutboxEntryId id, int a, String e, Duration n, String claimedBy) {
        calls.add(new RetryCall(id, a, e, n, claimedBy));
        return held.contains(id.value());
      }

      public void delete(OutboxEntryId id) {}

      public List<OutboxEntry> findByStatus(OutboxStatus status, int limit) {
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
    };
  }

  private static OutboxEntry entryWith(String id, int attempts, String lastError) {
    return new OutboxEntry(
        OutboxEntryId.of(id),
        "{}",
        "Event",
        null,
        OutboxStatus.IN_PROGRESS,
        attempts,
        lastError,
        Instant.now(),
        null,
        null,
        null);
  }

  @Test
  void releaseClaims_default_releasesViaMarkRetry_withoutBurningAnAttemptOrStampingBackoff() {
    // The SPI default must release the claim WITHOUT advancing the retry
    // ladder — it re-passes each entry's own attempts (so a relay whose deadline keeps truncating
    // cannot walk a never-attempted entry up to terminal FAILED and un-gate its aggregate) and its
    // own lastError (so the real last failure is not overwritten with a non-error), with a ZERO
    // backoff so the entry is immediately eligible again.
    var calls = new ArrayList<RetryCall>();
    OutboxStore store = retryRecordingStore(calls, java.util.Set.of("a", "b"));

    int released =
        store.releaseClaims(
            List.of(entryWith("a", 3, "earlier nack"), entryWith("b", 0, null)), "test");

    assertThat(released).isEqualTo(2);
    assertThat(calls)
        .containsExactly(
            new RetryCall(OutboxEntryId.of("a"), 3, "earlier nack", Duration.ZERO, "test"),
            new RetryCall(OutboxEntryId.of("b"), 0, null, Duration.ZERO, "test"));
  }

  @Test
  void releaseClaims_default_countsOnlyReleasedRows_lostLeaseIsABenignSkip() {
    // Only "a" still holds the lease; "b" was reclaimed by another relay while the cycle ran. The
    // CAS inside markRetry makes that a benign no-op, and the count reports it.
    var calls = new ArrayList<RetryCall>();
    OutboxStore store = retryRecordingStore(calls, java.util.Set.of("a"));

    int released =
        store.releaseClaims(List.of(entryWith("a", 0, null), entryWith("b", 0, null)), "test");

    assertThat(released).isEqualTo(1);
    assertThat(calls).hasSize(2);
  }
}
