package org.streamrune.core.outbox;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pluggable delivery strategy for outbox entries.
 *
 * <p>Implement this interface to deliver outbox entries to any target (HTTP webhook, Kafka, etc.).
 * Throw any {@link Exception} to signal delivery failure; the OutboxPoller will retry up to the
 * configured {@code maxAttempts}.
 *
 * <p>The default {@link #publishBatch} implementation calls {@link #publish} per entry, preserving
 * today's per-entry semantics for transports that do not provide native batch confirms (HTTP,
 * Kafka, etc.). Transport-specific implementations (e.g. RabbitMQ) may override {@link
 * #publishBatch} to publish all entries first and then await a single round of broker confirms,
 * reducing round-trips from O(N) to O(1).
 */
@FunctionalInterface
public interface OutboxPublisher {

  /**
   * Delivers the given outbox entry to the target system.
   *
   * @throws Exception if delivery fails (will be retried by the poller)
   */
  void publish(OutboxEntry entry) throws Exception;

  /**
   * Delivers a batch of entries and returns the per-entry outcome.
   *
   * <p>The default implementation calls {@link #publish} for each entry in order, collecting
   * confirmed ids (publish succeeded) and failed ids with their cause (publish threw). Every input
   * entry appears in exactly one of {@link BatchResult#confirmed()} or {@link
   * BatchResult#failed()}.
   *
   * <p>Transport-specific implementations may override this method to publish all entries first and
   * then await a single confirmation round-trip (e.g. RabbitMQ publisher confirms).
   *
   * @param entries the entries to deliver; must not be null; may be empty
   * @return the per-entry outcome; never null
   */
  default BatchResult publishBatch(List<OutboxEntry> entries) {
    return publishSequentially(entries, null);
  }

  /**
   * Delivers a batch of entries under an optional publish {@code deadline} and returns the
   * per-entry outcome.
   *
   * <p>The {@code OutboxPoller} passes a deadline strictly earlier than the store's claim lease so
   * a live-but-slow relay's publish cycle can never outlive its claim — which would let a second
   * relay reclaim and re-deliver the same entries out of order. The default implementation
   * publishes entries in order, checking the deadline <em>before</em> each publish: once the
   * deadline passes it stops and returns, leaving every not-yet-attempted entry in <b>neither</b>
   * {@link BatchResult#confirmed()} nor {@link BatchResult#failed()}. The poller returns those
   * entries to {@code PENDING} at the end of the cycle (nothing for them reached the transport),
   * rather than recording a failure that would burn a retry attempt on an entry that was never
   * attempted.
   *
   * <p>A shutdown interrupt stops the default implementation the same way. When the calling thread
   * is already interrupted before an entry's turn, that entry and the rest are left in neither
   * bucket. When the interrupt arrives while {@link #publish} is running — {@code publish} throws
   * an {@link InterruptedException} (or an exception caused by one), or throws anything while the
   * interrupt flag is set — the entry may already be with the transport, so it is reported in
   * {@link BatchResult#failed()} with an {@code InterruptedException} in the failure's cause chain
   * (the {@code OutboxPoller} keeps such an entry claimed until its lease expires), the batch
   * stops, and the interrupt flag is left set.
   *
   * <p>A {@code null} deadline delegates to {@link #publishBatch(List)}, so a publisher that
   * overrides only the single-arg method keeps its batch semantics on the no-deadline path. A
   * single in-flight {@link #publish} can still run past the deadline — bound each {@code publish}
   * with its own per-entry timeout (see {@code KafkaOutboxPublisher} send timeout, the HTTP request
   * timeout) so the deadline check is reached within a bounded overrun.
   *
   * <p><b>Batch publishers that override {@link #publishBatch(List)} for a single confirm
   * round-trip (e.g. RabbitMQ) MUST override THIS method too</b> to participate in deadline
   * bounding — bounding their confirm wait by the deadline rather than looping per entry. A
   * publisher that overrides only the single-arg method falls back to per-entry {@link #publish}
   * once a non-null deadline is present.
   *
   * @param entries the entries to deliver; must not be null; may be empty
   * @param deadline the instant by which publishing must stop; {@code null} for no deadline
   * @return the per-entry outcome; never null. With a non-null deadline, entries not attempted
   *     before it appear in neither bucket; so do entries not attempted because of an interrupt.
   */
  default BatchResult publishBatch(List<OutboxEntry> entries, java.time.Instant deadline) {
    if (deadline == null) {
      // Honor a single-arg override (custom batch publishers) on the no-deadline path.
      return publishBatch(entries);
    }
    return publishSequentially(entries, deadline);
  }

  /**
   * Publishes entries one at a time via {@link #publish}, stopping before any entry whose turn
   * comes at or after {@code deadline} or once the thread is interrupted. Entries not attempted
   * appear in neither bucket; an entry whose publish was interrupted is failed with the
   * interruption. A {@code null} deadline attempts every entry.
   */
  private BatchResult publishSequentially(List<OutboxEntry> entries, java.time.Instant deadline) {
    var confirmed = new LinkedHashSet<OutboxEntryId>();
    var failed = new LinkedHashMap<OutboxEntryId, Exception>();
    for (OutboxEntry e : entries) {
      // A shutdown interrupt mid-batch must not burn a ladder attempt on the remaining unsent
      // entries. Stop exactly like the deadline cutoff — leave every not-yet-attempted entry in
      // NEITHER bucket so the poller releases it, instead of recording a counting failure on an
      // entry no broker ever rejected.
      if (Thread.currentThread().isInterrupted()) {
        break;
      }
      if (deadline != null && !java.time.Instant.now().isBefore(deadline)) {
        // Publish deadline reached — stop. Remaining entries stay claimed (poller leaves
        // them IN_PROGRESS for lease-reclaim), never recorded as failures.
        break;
      }
      try {
        publish(e);
        confirmed.add(e.id());
      } catch (Exception ex) {
        if (isInterruption(ex) || Thread.currentThread().isInterrupted()) {
          // The publish was interrupted (shutdown) after it started, not rejected by the broker.
          // It may already be with the transport — a Kafka record in the producer buffer, an HTTP
          // request already written — so it is not "never attempted": report it failed with the
          // interruption, which the poller holds as in flight rather than releasing it to a peer
          // relay that would publish it a second time. Blocking socket I/O on a virtual thread
          // reports an interrupt as an IOException with the flag set, hence the flag check.
          // Restore the flag (a thrown InterruptedException has cleared it) and stop: every later
          // entry stays unattempted.
          Thread.currentThread().interrupt();
          failed.put(e.id(), asInterruption(ex));
          break;
        }
        failed.put(e.id(), ex);
      }
    }
    return new BatchResult(confirmed, failed);
  }

  /**
   * {@code ex} itself when it is (or was caused by) an {@link InterruptedException}; otherwise a
   * new {@code InterruptedException} caused by {@code ex}, so the poller recognises the
   * interruption.
   */
  private static Exception asInterruption(Exception ex) {
    if (isInterruption(ex)) {
      return ex;
    }
    var interrupted = new InterruptedException("publish interrupted while in progress");
    interrupted.initCause(ex);
    return interrupted;
  }

  /**
   * Whether {@code ex} is (or was caused by) an {@link InterruptedException} — a shutdown signal,
   * not a per-entry broker rejection. Walks the cause chain with both a self-reference guard and a
   * depth bound: the self-cause guard alone only catches a 1-node self-cycle, not a longer cycle
   * (e.g. a -&gt; b -&gt; a), which would otherwise spin forever. Bounded to depth 50, matching
   * {@code PostgresEventStore.hasCryptoCause}.
   */
  private static boolean isInterruption(Throwable ex) {
    Throwable t = ex;
    for (int depth = 0; t != null && depth < 50; depth++) {
      if (t instanceof InterruptedException) {
        return true;
      }
      Throwable cause = t.getCause();
      if (cause == t) {
        break;
      }
      t = cause;
    }
    return false;
  }

  /**
   * Outcome of a {@link #publishBatch} call.
   *
   * <p>Invariant: every entry id passed to {@code publishBatch} appears in exactly one of {@link
   * #confirmed()} or {@link #failed()} — <em>unless</em> a non-null publish deadline or a shutdown
   * interrupt stopped the batch early (see {@link #publishBatch(List, java.time.Instant)}), in
   * which case entries not attempted appear in neither. The {@code OutboxPoller} returns those to
   * {@code PENDING} at once, so an entry may be left out only when nothing for it reached the
   * transport.
   *
   * @param confirmed ids whose delivery was acknowledged by the target (broker-confirmed, HTTP 2xx,
   *     etc.)
   * @param failed ids whose delivery failed, mapped to the exception that caused the failure. Map
   *     every id to a <b>typed exception</b> — a broker nack or a confirm/send timeout included:
   *     wrap the condition in an exception describing it (as the shipped publishers do), so {@link
   *     #classifyFailure} can bin the failure into its three-way split. A {@code null} value is
   *     tolerated but unclassifiable: the poller bins it as non-counting {@code TRANSPORT} —
   *     retried forever with capped backoff, never the counting ladder, never {@code IN_FLIGHT} —
   *     so a nack/timeout mapped to {@code null} loses its precise classification. An entry whose
   *     publish started and was then interrupted is reported with a failure whose cause chain holds
   *     an {@link InterruptedException}: the poller keeps its claim until the lease expires,
   *     whatever {@link #classifyFailure} would answer
   */
  record BatchResult(Set<OutboxEntryId> confirmed, Map<OutboxEntryId, Exception> failed) {}

  /**
   * Classifies a delivery failure for the {@code OutboxPoller}'s retry accounting.
   *
   * <p>Transport failures do <b>not</b> count against the retry ladder: a broker restart or network
   * partition retries forever with capped backoff and can never terminal-{@code FAILED} the whole
   * backlog (which would also silently break per-aggregate ordering, since a {@code FAILED} head
   * stops gating its successors). Only per-entry rejections burn attempts toward {@code FAILED}.
   *
   * <p><b>Split I/O failures by hand-off phase, not by exception type.</b> The same {@code
   * IOException} type can mean "the connection was never established" or "the peer reset while I
   * was awaiting the answer to a request it already has". Only the first is {@link
   * FailureKind#TRANSPORT}: releasing the claim for the second lets a peer relay re-deliver into a
   * request the receiver is still executing. When the phase is not provable from the failure alone,
   * classify {@link FailureKind#IN_FLIGHT} — that costs one claim lease of recovery latency, while
   * the other way round costs a duplicate and a same-aggregate reorder. Where the transport throws
   * the same type in both phases, record the phase at the throw site (as {@code
   * RabbitMqOutboxPublisher} does by wrapping a confirm-wait shutdown) rather than guessing here.
   *
   * <p><b>The default classifies every failure as {@link FailureKind#TRANSPORT}</b> — non-counting,
   * retried forever with capped backoff. A publisher wired as a lambda <em>cannot</em> override
   * this method ({@code @FunctionalInterface}), so the default governs every custom publisher that
   * does not classify its own failures — and the old {@code ENTRY} default meant ANY repeated
   * transport-ish exception (broker down, DNS failure, connection refused) burned the retry ladder
   * and mass-terminal-{@code FAILED} the backlog: a permanent gap plus the silent same-aggregate
   * reorder a {@code FAILED} head causes once it stops gating its successors. The trade this
   * default accepts is a genuinely poisoned entry retrying indefinitely (and holding its
   * aggregate's head): a <em>loud, recoverable stall</em> — the entry's retry cadence escalates
   * toward the poller's 60s backoff cap on the entry's OWN transport streak, even while surrounding
   * traffic confirms (the escalation once held only for broker-wide outages, and a poisoned entry
   * amid healthy traffic re-published at {@code initialDelay} every ~1s forever), the poller's
   * aggregated transport WARN fires on every cycle the entry fails, and the pending gauge holds the
   * backlog — versus the silent loss and silent ordering break the counting default produced. That
   * is the same call the shipped publishers make for fleet-wide non-retriable conditions. A custom
   * publisher that CAN attribute failures to a message should override this and return {@link
   * FailureKind#ENTRY} for them, restoring its terminal ladder; one whose transport can leave a
   * hand-off unresolved after {@code publish} throws should return {@link FailureKind#IN_FLIGHT}
   * for that phase AND override {@link #inFlightHorizon()} — the two are a contract pair (see
   * there).
   *
   * <p>The shipped publishers override this with the full three-way split: {@code
   * KafkaOutboxPublisher} (a bounded {@code send().get()} {@code
   * java.util.concurrent.TimeoutException} <em>and the post-hand-off-capable Kafka retriables —
   * Kafka's own {@code TimeoutException}, {@code NetworkException}, {@code
   * NotEnoughReplicasAfterAppendException}</em> = in-flight; a provably-pre-append Kafka {@code
   * RetriableException} <em>or a fleet-wide non-retriable condition — authentication,
   * authorization, invalid topic, closed producer</em> = transport; a non-retriable Kafka error
   * that is a property of the message = entry), {@code RabbitMqOutboxPublisher} (confirm timeout
   * and a confirm-wait connection loss = in-flight; a {@code basicPublish}-phase shutdown / {@code
   * IOException} <em>and a broker nack — the broker's own report that it could not enqueue, a
   * broker-side condition that leaves nothing in flight</em> = transport; unroutable = entry), and
   * {@code HttpOutboxPublisher} (request/response timeout and every not-provably- pre-hand-off
   * {@code IOException} = in-flight; connect timeout / DNS / bind / connect refused / TLS handshake
   * / 5xx / 429 <em>and the fleet-wide credential rejections 401/407</em> = transport; other
   * non-2xx, {@code 403} included, = entry).
   *
   * @param failure the exception a {@link #publish}/{@link #publishBatch} failure surfaced (never
   *     null)
   * @return {@link FailureKind#TRANSPORT} for a broker-wide outage, {@link FailureKind#IN_FLIGHT}
   *     for an unconfirmed hand-off that may still be delivered, or {@link FailureKind#ENTRY} for a
   *     per-entry rejection
   */
  default FailureKind classifyFailure(Exception failure) {
    return FailureKind.TRANSPORT;
  }

  /**
   * The publisher's <b>maximum in-flight horizon</b>: the longest wall-clock time a single {@link
   * #publish} attempt may still be resolving at the transport/broker <em>after</em> {@link
   * #publish} itself returns or throws — i.e. the worst-case time before the caller can be certain
   * the attempt is truly finished, success or failure.
   *
   * <p>This is deliberately <b>not</b> the method's own blocking timeout. A transport can keep an
   * attempt alive past the point where {@code publish} returns: {@code KafkaOutboxPublisher}'s
   * {@code send().get(sendTimeout)} throws at {@code sendTimeout} (10s default), but the record
   * stays buffered in the producer's accumulator and is retried internally until {@code
   * delivery.timeout.ms} (120s default) — so its horizon is {@code delivery.timeout.ms}, not {@code
   * sendTimeout}. That gap is exactly the {@link FailureKind#IN_FLIGHT} case: the message may still
   * be delivered after {@code publish} threw.
   *
   * <p>The {@code OutboxPoller} enforces at construction that the store's {@link
   * OutboxStore#claimLease() claim lease} <b>strictly exceeds</b> this horizon. The lease is the
   * point at which another relay may reclaim a still-{@code IN_PROGRESS} entry and re-publish it;
   * if the lease did not exceed the horizon, a reclaim could fire while an {@link
   * FailureKind#IN_FLIGHT} entry's original publish is still being delivered by the transport — a
   * duplicate <em>and</em> a same-aggregate reorder, the one thing the ordering contract forbids.
   * The poller refuses to build an unsafe combination rather than paper over it. See {@code
   * OutboxPoller.build()}.
   *
   * <p>The default is {@link Duration#ZERO} — correct for a publisher whose {@link #publish} leaves
   * nothing in flight once it returns (a synchronous in-JVM handler, or a test double). Every
   * shipped publisher that hands off to an external transport overrides this with a value derived
   * from its own configured timeouts. A transport with a <em>blocking pre-hand-off phase</em> must
   * report the SUM of both phases, because the horizon is measured from the moment the attempt
   * starts: {@code KafkaOutboxPublisher} = effective {@code max.block.ms} + effective {@code
   * delivery.timeout.ms}, {@code RabbitMqOutboxPublisher} = its {@code publishBlockBudget} (the
   * untimed {@code basicPublish} socket write under broker flow control) + its confirm timeout,
   * {@code HttpOutboxPublisher} = its request timeout. A custom transport-backed publisher
   * <b>should</b> override it too, or its unsafe lease sizing will not be caught.
   *
   * <p><b>Contract pair with {@link #classifyFailure}.</b> A publisher that ever returns {@link
   * FailureKind#IN_FLIGHT} is asserting "a hand-off can keep resolving at the transport after
   * {@code publish} throws" — which a {@code ZERO} horizon flatly contradicts, and the
   * contradiction disables the very guard IN_FLIGHT relies on: the lease validation above
   * <em>skips</em> a zero horizon, so nothing checks that the claim lease outlasts the transport's
   * real in-flight window before a reclaim may fire. The two defaults cannot enforce each other at
   * build time (the classification is behavioral, and a lambda cannot override either method), so
   * the {@code OutboxPoller} treats an IN_FLIGHT classification under a zero horizon as a
   * <b>runtime contract violation</b>: it still fails safe — the claim is held for the full lease,
   * exactly as for a declared horizon — and reports loudly (a one-time ERROR log naming the
   * publisher, plus the {@code streamrune.outbox.in_flight_horizon_violation} counter per held
   * entry). Override BOTH methods together, or neither.
   *
   * @return the maximum time a publish may remain in flight after {@link #publish} returns/throws;
   *     never null; {@link Duration#ZERO} if nothing is left in flight once {@code publish} returns
   */
  default Duration inFlightHorizon() {
    return Duration.ZERO;
  }

  /** How the {@code OutboxPoller} accounts for a delivery failure against the retry ladder. */
  enum FailureKind {
    /**
     * A broker-wide transport/connection outage detected <b>before hand-off</b> — connection
     * refused, DNS failure, connect timeout, TLS handshake failure, a dead channel — <em>or a
     * definitive broker refusal that provably leaves nothing in flight</em> (a RabbitMQ nack: the
     * broker reporting the message was NOT enqueued — a broker-side condition, not a message
     * property; a Kafka pre-append error response). Either way no message is in flight, so
     * re-arming cannot duplicate. <b>Non-counting</b>: the poller retries it forever with capped
     * backoff and never marks the entry terminal {@code FAILED}, so a routine broker restart (or a
     * transient broker-wide alarm) cannot terminal-fail the backlog or break per-aggregate
     * ordering. An outage that surfaces <em>after</em> hand-off with the outcome UNKNOWN belongs in
     * {@link #IN_FLIGHT}, not here: this kind releases the claim.
     */
    TRANSPORT,
    /**
     * The message was handed off to the broker/endpoint but its delivery is <b>unconfirmed</b> — a
     * send/confirm/request <em>timeout</em>, or a connection reset/EOF while awaiting the answer,
     * where the target may still deliver it. The poller must NOT re-arm (markRetry) such an entry:
     * markRetry releases the claim (status back to PENDING, {@code claimed_by} cleared), so a
     * second HA relay could reclaim and RE-PUBLISH while the first publish is still in-flight — a
     * duplicate AND a same-aggregate reorder, the one thing the ordering contract forbids. The
     * entry is left {@code IN_PROGRESS} (claimed) so ONLY lease-reclaim redelivers it, after the
     * lease (which must exceed the transport's maximum in-flight horizon) expires and the in-flight
     * publish has resolved. Distinct from {@link #TRANSPORT}, which is a failure <em>before</em>
     * hand-off (basicPublish threw / connection refused): no message is in flight there, so an
     * immediate re-arm is safe.
     */
    IN_FLIGHT,
    /**
     * A per-entry rejection the broker/endpoint attributes to <b>this specific message</b> (an
     * unroutable RabbitMQ message, a serialization error, an oversized record, an HTTP 4xx).
     * <b>Counting</b>: it advances the attempt counter and, once {@code retryPolicy.maxAttempts()}
     * is reached, the entry is marked terminal {@code FAILED} for operator replay. Membership
     * demands the rejection be a property of the MESSAGE: a RabbitMQ broker <em>nack</em> is
     * deliberately NOT here — it is the broker failing to enqueue (queue-process error, {@code
     * reject-publish} overflow), a broker-side condition that sweeps whatever is in flight and
     * belongs in {@link #TRANSPORT}; binning it ENTRY let a transient broker-wide alarm
     * mass-terminal-FAIL the in-flight set.
     */
    ENTRY
  }
}
