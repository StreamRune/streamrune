package org.streamrune.core.outbox;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link OutboxPublisher#publishBatch} default implementation.
 *
 * <p>The default calls {@link OutboxPublisher#publish} per entry and collects results. These tests
 * confirm the invariant: every input entry appears in exactly one of confirmed or failed.
 */
class OutboxPublisherDefaultBatchTest {

  @Test
  void inFlightHorizon_default_isZero() {
    // A publisher that leaves nothing in flight once publish() returns (e.g. a
    // synchronous in-JVM handler) reports a ZERO horizon, so it never constrains the claim lease.
    OutboxPublisher publisher = entry -> {};
    assertEquals(Duration.ZERO, publisher.inFlightHorizon());
  }

  @Test
  void publishBatch_all_succeed_all_confirmed() throws Exception {
    OutboxPublisher publisher = entry -> {}; // no-op

    var e1 = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("e2"), "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("e3"), "{}", "Event");

    var result = publisher.publishBatch(List.of(e1, e2, e3));

    assertEquals(3, result.confirmed().size());
    assertTrue(result.confirmed().contains(OutboxEntryId.of("e1")));
    assertTrue(result.confirmed().contains(OutboxEntryId.of("e2")));
    assertTrue(result.confirmed().contains(OutboxEntryId.of("e3")));
    assertTrue(result.failed().isEmpty());
  }

  @Test
  void publishBatch_one_throws_appears_in_failed_rest_confirmed() throws Exception {
    var failId = OutboxEntryId.of("fail");
    var boom = new RuntimeException("delivery broke");
    OutboxPublisher publisher =
        entry -> {
          if (entry.id().equals(failId)) {
            throw boom;
          }
        };

    var e1 = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");
    var e2 = OutboxEntry.pending(failId, "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("e3"), "{}", "Event");

    var result = publisher.publishBatch(List.of(e1, e2, e3));

    assertEquals(2, result.confirmed().size());
    assertTrue(result.confirmed().contains(OutboxEntryId.of("e1")));
    assertTrue(result.confirmed().contains(OutboxEntryId.of("e3")));

    assertEquals(1, result.failed().size());
    assertTrue(result.failed().containsKey(failId));
    assertSame(boom, result.failed().get(failId));
  }

  @Test
  void publishBatch_all_fail_all_in_failed() {
    var boom = new RuntimeException("always down");
    OutboxPublisher publisher =
        entry -> {
          throw boom;
        };

    var e1 = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("e2"), "{}", "Event");

    var result = publisher.publishBatch(List.of(e1, e2));

    assertTrue(result.confirmed().isEmpty());
    assertEquals(2, result.failed().size());
  }

  @Test
  void publishBatch_empty_list_returns_empty_result() {
    OutboxPublisher publisher = entry -> {};

    var result = publisher.publishBatch(List.of());

    assertTrue(result.confirmed().isEmpty());
    assertTrue(result.failed().isEmpty());
  }

  @Test
  void publishBatch_withPastDeadline_attemptsNothing_leavesAllInNeitherBucket() {
    // A deadline already in the past stops the batch before the first publish. The
    // unattempted
    // entries appear in NEITHER bucket, so the poller leaves them claimed for lease-reclaim.
    var attempted = new java.util.ArrayList<OutboxEntryId>();
    OutboxPublisher publisher = entry -> attempted.add(entry.id());

    var e1 = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("e2"), "{}", "Event");

    var result =
        publisher.publishBatch(
            List.of(e1, e2), java.time.Instant.now().minusSeconds(1)); // deadline already passed

    assertTrue(attempted.isEmpty(), "no entry may be published once the deadline has passed");
    assertTrue(result.confirmed().isEmpty());
    assertTrue(result.failed().isEmpty(), "unattempted entries are in neither bucket, not failed");
  }

  @Test
  void classifyFailure_default_isTransport_neverCounting() {
    // The default classifies an unclassifiable failure as
    // TRANSPORT (non-counting, retried forever with capped backoff), NOT ENTRY. A lambda publisher
    // cannot override this method (@FunctionalInterface), so under the old ENTRY default ANY
    // repeated transport-ish exception (broker down, DNS, connection refused) burned the ladder and
    // mass-terminal-FAILed the backlog — permanent gaps plus the silent same-aggregate reorder a
    // FAILED head causes once it stops gating its successors. An unclassifiable failure must fail
    // toward the loud, recoverable stall (retry forever, aggregated WARN each cycle), never toward
    // silent loss — the same call the shipped publishers make for fleet-wide conditions.
    OutboxPublisher publisher = entry -> {};
    assertEquals(
        OutboxPublisher.FailureKind.TRANSPORT,
        publisher.classifyFailure(new RuntimeException("x")));
    assertEquals(
        OutboxPublisher.FailureKind.TRANSPORT,
        publisher.classifyFailure(new java.io.IOException("connection refused")));
  }

  @Test
  void publishBatch_withFutureDeadline_publishesAll() {
    // A comfortably-future deadline never trips: the batch behaves exactly like the no-deadline
    // path.
    OutboxPublisher publisher = entry -> {};

    var e1 = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("e2"), "{}", "Event");

    var result = publisher.publishBatch(List.of(e1, e2), java.time.Instant.now().plusSeconds(3600));

    assertEquals(2, result.confirmed().size());
    assertTrue(result.failed().isEmpty());
  }

  @Test
  void publishBatch_interruptedMidPublish_failsThatEntryAsInterrupted_leavesTheRestUnattempted() {
    // A shutdown interrupt that lands while publish(e2) is running: e2 may already be with the
    // transport (a Kafka record in the producer buffer, an HTTP request already written), so it is
    // reported in failed() carrying the interruption, which the poller holds as in flight. Only
    // the entries after it were provably never attempted: they are in neither bucket. The flag is
    // restored so the poll thread observes the shutdown.
    var attempted = new java.util.ArrayList<OutboxEntryId>();
    var interruptId = OutboxEntryId.of("e2");
    OutboxPublisher publisher =
        entry -> {
          attempted.add(entry.id());
          if (entry.id().equals(interruptId)) {
            throw new InterruptedException("shutdown");
          }
        };

    var e1 = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");
    var e2 = OutboxEntry.pending(interruptId, "{}", "Event");
    var e3 = OutboxEntry.pending(OutboxEntryId.of("e3"), "{}", "Event");

    OutboxPublisher.BatchResult result;
    boolean interrupted;
    try {
      result = publisher.publishBatch(List.of(e1, e2, e3));
    } finally {
      interrupted = Thread.interrupted(); // read AND clear so we do not pollute other tests
    }

    assertTrue(interrupted, "the interrupt flag must be restored after an interrupted publish");
    assertEquals(java.util.Set.of(OutboxEntryId.of("e1")), result.confirmed());
    assertEquals(java.util.Set.of(interruptId), result.failed().keySet());
    assertInstanceOf(InterruptedException.class, result.failed().get(interruptId));
    assertFalse(
        attempted.contains(OutboxEntryId.of("e3")), "the interrupt must stop the batch before e3");
  }

  @Test
  void publishBatch_alreadyInterrupted_attemptsNothing_leavesAllUnattempted() {
    // A batch entered with the interrupt flag already set (close() interrupted the poll thread
    // between cycles) attempts nothing and records no failures.
    var attempted = new java.util.ArrayList<OutboxEntryId>();
    OutboxPublisher publisher = entry -> attempted.add(entry.id());

    var e1 = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("e2"), "{}", "Event");

    OutboxPublisher.BatchResult result;
    boolean stillInterrupted;
    Thread.currentThread().interrupt();
    try {
      result = publisher.publishBatch(List.of(e1, e2));
    } finally {
      stillInterrupted = Thread.interrupted(); // read + clear
    }

    assertTrue(stillInterrupted, "the pre-existing interrupt flag is preserved");
    assertTrue(attempted.isEmpty(), "no entry is published once interrupted");
    assertTrue(result.confirmed().isEmpty());
    assertTrue(result.failed().isEmpty(), "unattempted entries are in neither bucket");
  }

  @Test
  void publishBatch_wrappedInterruption_failsThatEntryWithTheWrapper_leavesTheRestUnattempted() {
    // A publisher that rethrows the interrupt wrapped in a RuntimeException (caused by an
    // InterruptedException) is still a shutdown: the entry is reported with the wrapper, whose
    // cause chain the poller recognises, the batch stops, and the flag is restored.
    var wrapper = new RuntimeException("wrapped", new InterruptedException("shutdown"));
    OutboxPublisher publisher =
        entry -> {
          throw wrapper;
        };

    var e1 = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("e2"), "{}", "Event");

    OutboxPublisher.BatchResult result;
    boolean interrupted;
    try {
      result = publisher.publishBatch(List.of(e1, e2));
    } finally {
      interrupted = Thread.interrupted();
    }

    assertTrue(interrupted, "an InterruptedException cause restores the interrupt flag");
    assertTrue(result.confirmed().isEmpty());
    assertEquals(java.util.Set.of(OutboxEntryId.of("e1")), result.failed().keySet());
    assertSame(wrapper, result.failed().get(OutboxEntryId.of("e1")));
  }

  @Test
  void publishBatch_failureThrownWhileInterrupted_isReportedAsAnInterruption() {
    // Blocking socket I/O on a virtual thread does not throw InterruptedException when the thread
    // is interrupted: it closes the socket and throws an IOException with the flag still set. A
    // publish that fails while the flag is set was cut short by the shutdown, whatever the type,
    // so the entry is reported as interrupted (cause kept) and the batch stops.
    var socketClosed = new java.net.SocketException("Closed by interrupt");
    OutboxPublisher publisher =
        entry -> {
          Thread.currentThread().interrupt();
          throw socketClosed;
        };

    var e1 = OutboxEntry.pending(OutboxEntryId.of("e1"), "{}", "Event");
    var e2 = OutboxEntry.pending(OutboxEntryId.of("e2"), "{}", "Event");

    OutboxPublisher.BatchResult result;
    boolean interrupted;
    try {
      result = publisher.publishBatch(List.of(e1, e2));
    } finally {
      interrupted = Thread.interrupted();
    }

    assertTrue(interrupted, "the interrupt flag stays set");
    assertEquals(java.util.Set.of(OutboxEntryId.of("e1")), result.failed().keySet());
    Exception failure = result.failed().get(OutboxEntryId.of("e1"));
    assertInstanceOf(InterruptedException.class, failure);
    assertSame(socketClosed, failure.getCause(), "the original failure is kept as the cause");
  }

  @Test
  void publishBatch_every_entry_in_exactly_one_bucket() {
    var ids =
        List.of(
            OutboxEntryId.of("a"),
            OutboxEntryId.of("b"),
            OutboxEntryId.of("c"),
            OutboxEntryId.of("d"));
    OutboxPublisher publisher =
        entry -> {
          if (entry.id().value().equals("b") || entry.id().value().equals("d")) {
            throw new RuntimeException("odd ones out");
          }
        };

    var entries = ids.stream().map(id -> OutboxEntry.pending(id, "{}", "Event")).toList();
    var result = publisher.publishBatch(entries);

    // Confirmed + failed must equal the full input set (no overlap, no gaps)
    for (var id : ids) {
      boolean inConfirmed = result.confirmed().contains(id);
      boolean inFailed = result.failed().containsKey(id);
      assertTrue(
          inConfirmed ^ inFailed, "id " + id + " must be in exactly one bucket, not both or none");
    }
  }

  // isInterruption's self-cause guard (cause == t) only catches a 1-node
  // self-cycle; a 2-node cycle (a -> b -> a) is not equal to itself at either step and would spin
  // forever without a depth bound.

  /** A throwable whose getCause() returns a settable field, so two of them can form a cycle. */
  static final class CyclicThrowable extends RuntimeException {
    private transient Throwable next;

    void setNext(Throwable next) {
      this.next = next;
    }

    @Override
    public synchronized Throwable getCause() {
      return next;
    }
  }

  @Test
  void publishBatch_cyclicCauseChainWithoutInterruption_terminatesAndFailsTheEntry() {
    var a = new CyclicThrowable();
    var b = new CyclicThrowable();
    a.setNext(b);
    b.setNext(a); // a -> b -> a -> ... an unbounded isInterruption walk would never terminate

    var failId = OutboxEntryId.of("cyclic");
    OutboxPublisher publisher =
        entry -> {
          throw a;
        };
    var entry = OutboxEntry.pending(failId, "{}", "Event");

    assertTimeoutPreemptively(
        Duration.ofSeconds(2),
        () -> {
          var result = publisher.publishBatch(List.of(entry));
          assertTrue(
              result.failed().containsKey(failId),
              "no InterruptedException in the cycle — must be a counting failure, not swallowed"
                  + " as a shutdown signal");
        });
  }
}
