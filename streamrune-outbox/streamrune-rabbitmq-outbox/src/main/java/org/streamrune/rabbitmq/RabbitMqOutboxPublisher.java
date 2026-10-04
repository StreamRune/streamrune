package org.streamrune.rabbitmq;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ReturnListener;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxEntryId;
import org.streamrune.core.outbox.OutboxPublisher;

/**
 * {@link OutboxPublisher} that delivers outbox entries to a RabbitMQ exchange.
 *
 * <p>Publishes via {@link Channel#basicPublish} with:
 *
 * <ul>
 *   <li><b>Body:</b> {@link OutboxEntry#payload()} as UTF-8 bytes
 *   <li><b>Properties:</b> persistent delivery mode, {@code application/json} content type, {@code
 *       messageId} from entry ID, {@code type} from payload type
 *   <li><b>Headers:</b> {@code X-Outbox-Entry-Id} and {@code X-Outbox-Payload-Type}
 * </ul>
 *
 * <p>Publishes with <b>publisher confirms</b>. Two delivery paths are available, sharing one
 * confirm mechanism:
 *
 * <ul>
 *   <li>{@link #publishBatch(List)} — batch path (override of the default). Publishes all entries
 *       first, then awaits a single async-confirm round for the whole batch. Only broker-confirmed
 *       AND routable entries are returned in {@link BatchResult#confirmed()}; nacked, timed-out,
 *       {@code basicPublish}-failed, and unroutable-(returned) entries are returned in {@link
 *       BatchResult#failed()} and will be retried. This reduces confirm round-trips from O(N) to
 *       O(1) for a batch of N entries.
 *   <li>{@link #publish(OutboxEntry)} — single-entry path, implemented as a singleton batch. A
 *       nack, unroutable return, connection failure, or confirm timeout propagates as an exception
 *       (see the method javadoc for the exact exception-to-{@link FailureKind} mapping) so the
 *       OutboxPoller retries instead of marking the entry delivered — and <b>the channel survives
 *       every failed confirm</b>. This path deliberately never calls {@link
 *       Channel#waitForConfirmsOrDie(long)}: that method closes the channel before throwing on a
 *       nack or timeout, and a client-initiated close is never reopened by automatic recovery.
 * </ul>
 *
 * <p>A single {@link ConfirmListener} is registered at build time on the channel. It settles
 * confirms against a {@link ConcurrentNavigableMap} of outstanding (unconfirmed) sequence numbers
 * plus an immutable per-batch snapshot (entry ids, {@link CountDownLatch}, acked/nacked sets)
 * published through a single volatile field — the connection's confirm callback thread never reads
 * a collection the publishing thread might still be mutating.
 *
 * <p><b>Unroutable messages are retried, never dropped.</b> Every publish sets the {@code
 * mandatory} flag and a {@link ReturnListener} is registered at build time. A message the broker
 * cannot route (no queue bound to the exchange/routing key yet — e.g. a producer that started
 * before the consumer declared its queue, or a removed/mistyped binding) is <em>positively
 * acked</em> in confirm mode yet silently discarded, so a plain confirm would mark it delivered and
 * lose the integration event. With {@code mandatory=true} the broker first {@code basic.return}s
 * such a message; the return listener records its entry id, so {@link #publishBatch} moves it to
 * {@link BatchResult#failed()} (retried) instead of {@link BatchResult#confirmed()}, and {@link
 * #publish} throws — preserving the outbox's at-least-once contract until a queue is bound.
 *
 * <p>The channel must be <b>owned exclusively by this publisher</b>: sharing it with other
 * producers would couple their failures and latencies to this one.
 *
 * <p><b>The channel must stay in confirm mode, with the listeners {@link Builder#build()}
 * registered, for its whole lifetime.</b> A channel of an amqp-client {@code ConnectionFactory}
 * with automatic recovery enabled (the client's default) does: recovery re-applies {@code
 * confirmSelect} and re-registers the confirm and return listeners on the recovered channel. A
 * wrapper that silently replaces a closed channel with a fresh one does not — for example a Spring
 * AMQP {@code CachingConnectionFactory} channel proxy, which also turns automatic recovery off.
 * Messages published on such a replacement are never confirmed: each would time out, stay claimed,
 * and be published again after every claim lease. The publisher therefore checks the channel before
 * every publish: a channel that reports publish sequence number 0 (not in confirm mode) fails the
 * entry with {@link NotInConfirmModeException} without publishing it, and an unchecked exception
 * from the channel before the publish fails it with {@link ChannelFailureException}; both are
 * {@link FailureKind#TRANSPORT} (nothing was handed off) and repeat on every cycle until the
 * application rebuilds the publisher on a working channel. An unchecked exception thrown by {@code
 * basicPublish} itself is returned as a failure too, never thrown out of the batch. Give the
 * publisher a dedicated channel of its own amqp-client connection.
 *
 * <p>Does <b>not</b> declare exchanges or queues — that is an operational concern. Does <b>not</b>
 * close the channel — lifecycle is the caller's responsibility.
 */
public final class RabbitMqOutboxPublisher implements OutboxPublisher {

  /**
   * Default {@link Builder#publishBlockBudget(Duration) publish-block budget} — the assumed upper
   * bound on how long a single {@link Channel#basicPublish} can block before the message is handed
   * to the broker. See {@link #inFlightHorizon()}.
   */
  public static final Duration DEFAULT_PUBLISH_BLOCK_BUDGET = Duration.ofSeconds(30);

  private final Channel channel;
  private final String exchange;
  private final String routingKey;
  private final Duration confirmTimeout;
  private final Duration publishBlockBudget;

  // outstanding: seqNo -> entry id for every published-but-not-yet-settled message. The poller
  // thread writes (register-before-publish, publish-failure rollback, timeout sweep); the
  // connection's confirm thread reads and removes on settle. Being a concurrent map, it is the
  // single source of truth settleConfirms consults — no non-thread-safe collection shadows it.
  // (The former LinkedHashSet `batchSeqNos` did exactly that: the poller mutated it mid-publish
  // while the confirm thread read it, so a confirm could fail the membership check yet still
  // consume the outstanding entry — dropped confirm, latch never zero, batch stalled to timeout.)
  private final ConcurrentNavigableMap<Long, OutboxEntryId> outstanding =
      new ConcurrentSkipListMap<>();

  // FIX 2 (upgraded): per-batch state, bundled in one immutable holder behind a single volatile
  // field. One volatile read gives the confirm thread a mutually consistent snapshot of
  // (ids, latch, acked, nacked) — separate volatile fields could tear across a batch boundary
  // (the new latch observed together with the previous batch's accounting). publishBatch writes
  // the fully-built holder BEFORE its first basicPublish, so the volatile write happens-before
  // every confirm the batch can trigger.
  private volatile Batch currentBatch;

  // Entry ids the broker RETURNED as unroutable (mandatory publish, no
  // queue bound). The return listener populates it on the connection thread;
  // publish()/publishBatch() drain it to force the entry into failed()/throw instead of
  // confirmed(). An unroutable message is basic.return'ed BEFORE it is basic.ack'ed and both are
  // dispatched in frame order on the same channel thread, so by the time a confirm settles (or
  // waitForConfirmsOrDie returns) the id is already recorded here — the check after settlement is
  // race-free. Membership is drained per entry as each publish path builds its result, so the set
  // does not accumulate across calls.
  private final Set<OutboxEntryId> returned = ConcurrentHashMap.newKeySet();

  /**
   * Immutable per-batch confirm-tracking state.
   *
   * <p>{@code ids} is the complete set of entry ids in the batch, built and frozen before the first
   * publish — entry ids are known up front, unlike publish seqNos, which are handed out per message
   * right before its {@code basicPublish}. It scopes settlement: a leftover seqNo from an earlier
   * batch (e.g. abandoned by an interrupt during the confirm wait) maps to an id outside this set,
   * and its late confirm must not count down this batch's latch.
   *
   * <p>{@code acked}/{@code nacked} are concurrent sets keyed by entry id; recording an id in
   * exactly one of them exactly once guards the latch against double count-down (a retried entry
   * can settle via both its stale and its fresh seqNo).
   */
  private static final class Batch {
    final Set<OutboxEntryId> ids;
    final CountDownLatch latch;
    final Set<OutboxEntryId> acked;
    final Set<OutboxEntryId> nacked;

    Batch(
        Set<OutboxEntryId> ids,
        CountDownLatch latch,
        Set<OutboxEntryId> acked,
        Set<OutboxEntryId> nacked) {
      this.ids = ids;
      this.latch = latch;
      this.acked = acked;
      this.nacked = nacked;
    }
  }

  /**
   * A {@code mandatory} message the broker returned as unroutable (no queue bound to the
   * exchange/routing key). A distinct {@link IOException} subtype so {@link #classifyFailure} can
   * tell it apart from a connection-loss {@code IOException}: an unroutable message is a per-{@link
   * FailureKind#ENTRY ENTRY} routing condition (counts toward the retry ladder), whereas a bare
   * {@code IOException}/{@code ShutdownSignalException} is a broker-wide {@link
   * FailureKind#TRANSPORT TRANSPORT} outage.
   */
  static final class UnroutableException extends IOException {
    UnroutableException(String message) {
      super(message);
    }
  }

  /**
   * The publish may have reached the broker but its outcome is unknown, so the claim must be held
   * ({@link FailureKind#IN_FLIGHT}). Raised in two cases. First, {@code basicPublish} threw an
   * unchecked exception other than a connection shutdown (the original is the cause): a channel
   * wrapper raises such exceptions before it reaches a channel and the amqp-client its own before
   * writing a frame, but a metrics or observation collector the client calls after writing the
   * message can throw as well, so the phase is not provable. Second, the connection/channel died
   * while {@link #publish} was <em>awaiting the broker confirm</em> — i.e. strictly after {@code
   * basicPublish} handed the message off. A distinct subtype so {@link #classifyFailure} can split
   * the two phases of the same {@link ShutdownSignalException}: raised <em>by</em> {@code
   * basicPublish} nothing was handed off and re-arming is safe ({@link FailureKind#TRANSPORT}),
   * whereas raised while waiting for the confirm the broker may already hold the message, so the
   * claim must be held for lease-reclaim ({@link FailureKind#IN_FLIGHT}) exactly as on a confirm
   * timeout. classifyFailure alone cannot tell the phases apart — both throw the same type — so the
   * phase is recorded at the throw site.
   */
  static final class UnconfirmedPublishException extends IOException {
    UnconfirmedPublishException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * The broker <b>nacked</b> the publish: its {@code basic.nack} is the broker's own report that
   * <em>it</em> could not take responsibility for the message — RabbitMQ nacks when the queue
   * process hits an internal error, or on {@code reject-publish} overflow of a length-limited queue
   * — a <b>broker-side</b> condition that sweeps whatever happens to be in flight (often with
   * {@code multiple=true}), never a verdict about one message. Message-level problems surface
   * through other channels entirely (an unroutable message is {@code basic.return}ed — {@link
   * UnroutableException}; an over-limit or malformed frame is a channel error). A distinct {@link
   * IOException} subtype so {@link #classifyFailure} can bin it {@link FailureKind#TRANSPORT}: the
   * nack asserts the message was NOT enqueued, so an immediate re-arm cannot duplicate, and — being
   * broker-wide-transient shaped — it must never count toward terminal {@code FAILED} (under the
   * old bare-{@code RuntimeException} ENTRY binning, a transient broker-wide alarm
   * mass-terminal-FAILed the whole in-flight set in ~maxAttempts cycles: permanent gaps plus the
   * {@code FAILED}-head same-aggregate reorder). The residual trade is a nack condition that never
   * clears retrying forever — a loud stall (the poller's aggregated transport WARN fires every
   * cycle), consistent with how {@code HttpOutboxPublisher} bins a 5xx/429 and {@code
   * KafkaOutboxPublisher} a retriable broker error response.
   */
  static final class NackedPublishException extends IOException {
    NackedPublishException(String message) {
      super(message);
    }
  }

  /**
   * The channel is not in publisher-confirm mode: {@link Channel#getNextPublishSeqNo()} returned 0.
   * Raised before {@code basicPublish}, so nothing was handed off — {@link FailureKind#TRANSPORT}.
   * It means the channel this publisher was built on has been replaced by one that never had {@code
   * confirmSelect} applied (see the class javadoc); it repeats until the publisher is rebuilt on a
   * channel that keeps confirm mode.
   */
  public static final class NotInConfirmModeException extends IOException {
    NotInConfirmModeException(String message) {
      super(message);
    }
  }

  /**
   * The channel failed with an unchecked exception other than a connection shutdown before this
   * entry was published — typically a channel wrapper that could not obtain a working channel,
   * which fails the first call of a publish. Also reported for the entries of a batch that were not
   * attempted because {@code basicPublish} failed that way for an earlier entry. The original
   * exception is the cause. Nothing was handed off — {@link FailureKind#TRANSPORT}.
   */
  public static final class ChannelFailureException extends IOException {
    ChannelFailureException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  /**
   * The entry cannot be published at all: its id (the AMQP {@code message-id}) or its payload type
   * (the AMQP {@code type}) is longer than the protocol's short-string limit of 255 UTF-8 bytes.
   * The entry never reaches the channel — the amqp-client would reject it only after taking its
   * publish sequence number, leaving the channel's numbering ahead of the broker's confirms for the
   * rest of its life. A property of the message: {@link FailureKind#ENTRY}.
   */
  public static final class UnpublishableEntryException extends IllegalArgumentException {
    UnpublishableEntryException(String message) {
      super(message);
    }
  }

  /** The AMQP short-string limit, in UTF-8 bytes. */
  private static final int SHORT_STRING_MAX_BYTES = 255;

  private static final String NOT_IN_CONFIRM_MODE = "not in publisher-confirm mode";

  /** Whether {@code value} is longer than an AMQP short string allows. */
  private static boolean exceedsShortString(String value) {
    // A UTF-16 code unit encodes to at most 3 UTF-8 bytes, so 85 characters always fit.
    return value.length() > SHORT_STRING_MAX_BYTES / 3
        && value.getBytes(StandardCharsets.UTF_8).length > SHORT_STRING_MAX_BYTES;
  }

  private RabbitMqOutboxPublisher(
      Channel channel,
      String exchange,
      String routingKey,
      Duration confirmTimeout,
      Duration publishBlockBudget) {
    this.channel = channel;
    this.exchange = exchange;
    this.routingKey = routingKey;
    this.confirmTimeout = confirmTimeout;
    this.publishBlockBudget = publishBlockBudget;
  }

  /**
   * Publishes one entry via the shared batch confirm path — {@link #publishBatch(List, Instant)}
   * with a singleton list and no deadline — and maps the entry's outcome back onto this method's
   * throw-to-retry contract:
   *
   * <ul>
   *   <li>broker-confirmed and routable — returns normally;
   *   <li>broker nack — throws {@link NackedPublishException}, classified {@link
   *       FailureKind#TRANSPORT} (non-counting: the broker reported the message NOT enqueued — a
   *       broker-side condition, so re-arming cannot duplicate and a broker-wide alarm can never
   *       mass-terminal-FAIL the backlog), exactly matching {@link #publishBatch};
   *   <li>unroutable ({@code basic.return}ed) — throws {@link UnroutableException} ({@link
   *       FailureKind#ENTRY});
   *   <li>confirm timeout — throws {@link TimeoutException} ({@link FailureKind#IN_FLIGHT}: the
   *       broker may already hold the message, so the claim is held for lease-reclaim). If the
   *       channel/connection is dead by the time the confirm wait ends, the confirm can never
   *       arrive: the timeout is reported as {@link UnconfirmedPublishException} carrying the
   *       channel's close reason — the same post-hand-off {@link FailureKind#IN_FLIGHT}
   *       classification;
   *   <li>{@code basicPublish} failure — rethrows the original {@link IOException}/{@link
   *       ShutdownSignalException}: nothing was handed off, pre-hand-off {@link
   *       FailureKind#TRANSPORT};
   *   <li>channel not in confirm mode — throws {@link NotInConfirmModeException} without
   *       publishing; an unchecked exception from the channel before the publish — throws {@link
   *       ChannelFailureException} caused by it; both pre-hand-off {@link FailureKind#TRANSPORT};
   *       an unchecked exception from {@code basicPublish} itself — throws {@link
   *       UnconfirmedPublishException} caused by it ({@link FailureKind#IN_FLIGHT}: whether the
   *       message was written is not provable);
   *   <li>id or payload type longer than an AMQP short string — throws {@link
   *       UnpublishableEntryException} without touching the channel ({@link FailureKind#ENTRY});
   *   <li>interrupted — restores the interrupt flag and throws {@link InterruptedException}.
   * </ul>
   *
   * <p><b>The channel survives every failed confirm</b>. The previous implementation blocked on
   * {@link Channel#waitForConfirmsOrDie(long)}, which — verified against amqp-client 5.25.0 —
   * <em>closes the channel before throwing</em> on a nack ({@code close(200, "NACKS RECEIVED")} +
   * {@code IOException("nacks received")}) or a confirm timeout ({@code close(406, "TIMEOUT WAITING
   * FOR ACK")}). A client-initiated close is never reopened by automatic connection recovery, so
   * every subsequent publish on this publisher failed with {@code AlreadyClosedException} —
   * classified {@link FailureKind#TRANSPORT}, non-counting, retried forever — permanently stalling
   * the whole outbox backlog behind what logs reported as a transient broker outage. (The nack
   * itself IS non-counting TRANSPORT — see {@link NackedPublishException} — but the dead channel
   * turned one broker verdict into a permanent stall of everything after it.) Delegating to the
   * batch path removes the second, channel-killing confirm mechanism entirely: both SPI paths share
   * one listener-based settlement with per-entry attribution ({@link
   * Channel#waitForConfirms(long)}'s nack flag is channel-global and reset-on-read, so a late nack
   * from a previous timed-out publish would be misattributed to the current entry).
   *
   * <p>Same threading contract as {@link #publishBatch}: one publisher, one owning thread.
   */
  @Override
  public void publish(OutboxEntry entry) throws Exception {
    // An interrupted confirm wait comes back as this entry failed with the InterruptedException
    // (flag restored by publishBatch), which the `throw failure` below surfaces unchanged.
    BatchResult result = publishBatch(List.of(entry), null);
    if (result.confirmed().contains(entry.id())) {
      return;
    }
    Exception failure = result.failed().get(entry.id());
    if (failure == null) {
      // Neither bucket: with a null deadline, publishBatch leaves an entry unattempted only when
      // the calling thread was already interrupted before its publish — nothing was handed off.
      // Keep the flag set and honor the SPI contract with the checked exception.
      if (Thread.currentThread().isInterrupted()) {
        throw new InterruptedException(
            "interrupted before the publish was attempted: " + entry.id().value());
      }
      // Defensive: publishBatch guarantees every attempted entry lands in exactly one bucket.
      throw new IllegalStateException(
          "publishBatch returned no outcome for entry " + entry.id().value());
    }
    if (failure instanceof TimeoutException && !channel.isOpen()) {
      // The confirm wait timed out AND the channel is dead: the confirm can never arrive because
      // the connection died after basicPublish handed the message off — the broker may already
      // hold it. Report the post-hand-off phase precisely so classifyFailure keeps the claim
      // (IN_FLIGHT) with the actionable close reason as cause, instead of a bare timeout.
      ShutdownSignalException closeReason = channel.getCloseReason();
      throw new UnconfirmedPublishException(
          "RabbitMQ connection lost while awaiting the publisher confirm for "
              + entry.id().value()
              + " — the message may already be enqueued",
          closeReason != null ? closeReason : failure);
    }
    throw failure;
  }

  /**
   * Publishes all entries, then awaits a single batch of async broker confirms.
   *
   * <p>For each entry: captures the next publish sequence number, registers it in the outstanding
   * map, then calls {@code basicPublish}. If the channel fails for any entry — {@code basicPublish}
   * throws (an unchecked exception other than a shutdown as {@link UnconfirmedPublishException}),
   * an unchecked exception before it ({@link ChannelFailureException}), or the channel is not in
   * confirm mode ({@link NotInConfirmModeException}, checked before publishing) — that entry and
   * all subsequent unpublished entries are placed in {@link BatchResult#failed()}; no exception
   * escapes. An entry whose id or payload type exceeds an AMQP short string is failed alone with
   * {@link UnpublishableEntryException} and never reaches the channel.
   *
   * <p>After publishing, waits up to {@code confirmTimeout} for the broker to ack or nack every
   * sequence number in the batch. Acked entries go to {@link BatchResult#confirmed()}; nacked or
   * timed-out entries go to {@link BatchResult#failed()}.
   *
   * <p>If the calling thread is interrupted during the confirm wait, the method stops waiting and
   * returns what is known: entries the broker already acked are confirmed, nacked or unroutable
   * ones failed as usual, and every published entry still unsettled is failed with the {@link
   * InterruptedException} — the broker may already hold it, so the {@code OutboxPoller} keeps its
   * claim until the lease expires. The interrupt flag is left set. Entries not yet published when
   * the interrupt was seen are in neither bucket.
   *
   * @param entries the entries to deliver; must not be null; may be empty
   * @return the per-entry outcome
   */
  @Override
  public BatchResult publishBatch(List<OutboxEntry> entries) {
    return publishBatch(entries, null);
  }

  /**
   * Deadline-aware batch publish. Publishes all entries (the publish loop is fast — {@code
   * basicPublish} buffers), then awaits confirms for at most {@code min(confirmTimeout, time until
   * deadline)}. Bounding the confirm wait by the deadline keeps a slow broker from holding the
   * claim past its lease: entries unconfirmed by the deadline are returned in {@link
   * BatchResult#failed()} (retried) exactly as on a confirm timeout, and — being already published
   * — are at-least-once safe. A {@code null} deadline preserves the full {@code confirmTimeout}
   * wait.
   *
   * @param entries the entries to deliver; must not be null; may be empty
   * @param deadline the instant by which the confirm wait must end; {@code null} for no deadline
   * @return the per-entry outcome
   */
  @Override
  public BatchResult publishBatch(List<OutboxEntry> entries, Instant deadline) {
    if (entries.isEmpty()) {
      return new BatchResult(Set.of(), Map.of());
    }

    // --- Set up per-batch tracking ---
    // The batch's entry ids are all known up front, so the scoping set is built complete and
    // frozen BEFORE the first publish — the confirm thread never reads a collection this thread
    // is still mutating. (Publish seqNos cannot be pre-collected the same way:
    // getNextPublishSeqNo() must be called per message right before its basicPublish.)
    HashSet<OutboxEntryId> idsInBatch = HashSet.newHashSet(entries.size());
    for (OutboxEntry entry : entries) {
      idsInBatch.add(entry.id());
      // Mirror publish(): drain any stale return marker for this id BEFORE publishing, so the
      // post-confirm `returned` check only ever observes a basic.return produced by THIS batch. A
      // late return from a prior timed-out attempt of the same id would otherwise spuriously fail
      // an entry the broker delivers this time (one needless retry of an already-delivered entry).
      returned.remove(entry.id());
    }
    Set<OutboxEntryId> acked = Collections.newSetFromMap(new ConcurrentHashMap<>());
    Set<OutboxEntryId> nacked = Collections.newSetFromMap(new ConcurrentHashMap<>());
    var latch = new CountDownLatch(entries.size());
    // Single volatile write of the fully-built holder: happens-before every basicPublish below,
    // so every confirm this batch can trigger observes a consistent snapshot.
    currentBatch = new Batch(Set.copyOf(idsInBatch), latch, acked, nacked);

    // --- Publish all entries ---
    var failed = new LinkedHashMap<OutboxEntryId, Exception>();
    List<OutboxEntry> published = new ArrayList<>(entries.size());
    Exception publishError = null;
    boolean stopPublishing = false;
    for (OutboxEntry entry : entries) {
      // Bound the PUBLISH loop by the deadline (and honor a shutdown
      // interrupt), symmetric with the confirm wait below and with the default publishSequentially.
      // basicPublish blocks the calling thread while the broker is flow-controlled (a memory/disk
      // alarm); without this check the loop would keep attempting past the claim lease, so a second
      // relay would reclaim and re-publish the tail — a duplicate plus a same-aggregate reorder,
      // and A's poll thread stuck in basicPublish would also time out close()'s join. Once the
      // deadline is reached (or the thread is interrupted for shutdown), stop attempting: count
      // each remaining entry's latch slot down (no confirm will arrive for an unpublished entry)
      // and leave it in NEITHER bucket: nothing was handed off, so the poller releases it.
      if (!stopPublishing
          && publishError == null
          && ((deadline != null && !Instant.now().isBefore(deadline))
              || Thread.currentThread().isInterrupted())) {
        stopPublishing = true;
      }
      if (stopPublishing) {
        latch.countDown(); // not attempted; left in neither bucket for the poller to release
        continue;
      }
      if (publishError != null) {
        // Channel is dead after a publish failure; fail remaining entries with the same cause
        failed.put(entry.id(), publishError);
        latch.countDown(); // account for this entry in the latch
        continue;
      }
      Exception failure = handOff(entry);
      if (failure == null) {
        published.add(entry);
        continue;
      }
      failed.put(entry.id(), failure);
      latch.countDown(); // no confirm will arrive for this entry
      if (failure instanceof UnconfirmedPublishException) {
        // basicPublish threw an unchecked exception: this entry is held, but the entries after it
        // are never attempted on the failed channel, which is provably before any hand-off.
        publishError =
            new ChannelFailureException(
                "not published: the RabbitMQ channel failed an earlier publish of this batch",
                failure.getCause());
      } else if (!(failure instanceof UnpublishableEntryException)) {
        // The channel itself failed (only an over-long entry leaves it untouched): cascade the
        // same cause to every remaining entry instead of attempting each on a dead channel.
        publishError = failure;
      }
    }

    // --- Wait for confirms ---
    if (!published.isEmpty()) {
      try {
        // Never wait past the poller's publish deadline (which is < the claim lease), so a
        // slow broker cannot hold the claim until a second relay reclaims and re-delivers. Floor at
        // 0 — an already-passed deadline degrades to a non-blocking poll of what has settled.
        long confirmWaitMillis = confirmTimeout.toMillis();
        if (deadline != null) {
          confirmWaitMillis =
              Math.clamp(
                  Duration.between(Instant.now(), deadline).toMillis(), 0L, confirmWaitMillis);
        }
        boolean settled = latch.await(confirmWaitMillis, TimeUnit.MILLISECONDS);
        if (!settled) {
          // Timeout: every published entry not settled by now is unconfirmed. Mark it failed
          // and sweep its seqNo(s) out of `outstanding` by id -> seq reverse lookup, so a late
          // confirm finds nothing to settle (the Batch.ids gate in settleConfirms is the second
          // line of defense should a confirm race this sweep).
          var timedOut = new HashSet<OutboxEntryId>();
          for (OutboxEntry entry : published) {
            if (!acked.contains(entry.id()) && !nacked.contains(entry.id())) {
              failed.put(entry.id(), new TimeoutException("broker did not confirm within timeout"));
              timedOut.add(entry.id());
            }
          }
          // Weakly consistent removeIf tolerates a settle racing this sweep; it also clears any
          // stale seqNo a retried id may have left behind in an earlier abandoned batch.
          outstanding.values().removeIf(timedOut::contains);
        }
      } catch (InterruptedException e) {
        // Shutdown during the confirm wait: keep what the broker already settled (the poller must
        // record those acks before it stops) and report every unsettled published entry with the
        // interruption, which the poller holds as in flight. The result is built below as usual.
        Thread.currentThread().interrupt();
        for (OutboxEntry entry : published) {
          if (!acked.contains(entry.id()) && !nacked.contains(entry.id())) {
            failed.put(entry.id(), e);
          }
        }
      }
    }

    // --- Build result ---
    var confirmed = new LinkedHashSet<OutboxEntryId>();
    for (OutboxEntry entry : published) {
      // The broker positively ACKs an unroutable mandatory message (so it
      // would otherwise land in confirmed()), but it also basic.return'ed it first — recorded in
      // `returned`. Such an entry was never enqueued, so it must be FAILED (retried), not
      // confirmed. The return precedes the ack in frame order, so by now it is reliably visible.
      // Drain per id so the set does not accumulate across batches.
      if (returned.remove(entry.id())) {
        failed.putIfAbsent(
            entry.id(),
            new UnroutableException(
                "RabbitMQ returned message as unroutable (no queue bound to routing key '"
                    + routingKey
                    + "' on exchange '"
                    + exchange
                    + "'): "
                    + entry.id().value()));
      } else if (acked.contains(entry.id())) {
        confirmed.add(entry.id());
        // The confirm may have raced the timeout sweep and been recorded just after the sweep
        // marked this entry failed — the ack wins; an entry must never appear in both buckets.
        failed.remove(entry.id());
      } else if (!failed.containsKey(entry.id())) {
        // Nacked but not already in failed (the nack handler added it to the batch's nacked set).
        // Typed so classifyFailure bins it non-counting TRANSPORT — a nack
        // is the broker failing to enqueue (broker-side, often a multiple=true fleet sweep), not a
        // verdict about this message; a bare RuntimeException fell through to the counting bin and
        // let a transient broker-wide alarm mass-terminal-FAIL the in-flight set.
        failed.put(
            entry.id(), new NackedPublishException("broker nacked message: " + entry.id().value()));
      }
    }
    return new BatchResult(confirmed, failed);
  }

  /**
   * Hands one entry to the channel. Returns {@code null} once {@code basicPublish} returned (the
   * entry's sequence number stays registered in {@code outstanding} for its confirm), or the
   * failure with nothing left registered: {@link UnpublishableEntryException} (the channel was not
   * touched), {@link NotInConfirmModeException} or {@link ChannelFailureException} (nothing was
   * published), the {@link IOException}/{@link ShutdownSignalException} {@code basicPublish} threw,
   * or an {@link UnconfirmedPublishException} caused by any other unchecked exception it threw.
   */
  private Exception handOff(OutboxEntry entry) {
    if (exceedsShortString(entry.id().value())) {
      return new UnpublishableEntryException(
          "outbox entry id is longer than the 255 UTF-8 bytes an AMQP message-id allows");
    }
    if (exceedsShortString(entry.payloadType())) {
      return new UnpublishableEntryException(
          "outbox payload type is longer than the 255 UTF-8 bytes an AMQP type allows");
    }
    long seq;
    try {
      seq = channel.getNextPublishSeqNo();
    } catch (ShutdownSignalException e) {
      return e;
    } catch (RuntimeException e) {
      return new ChannelFailureException("RabbitMQ channel failed before publishing: " + e, e);
    }
    if (seq <= 0) {
      // Publishing anyway would deliver a message whose confirm never arrives: it would time out,
      // stay claimed and be published again after every claim lease.
      return new NotInConfirmModeException(
          "RabbitMQ channel is "
              + NOT_IN_CONFIRM_MODE
              + " (publish sequence number 0) — it was replaced by a channel"
              + " without confirmSelect; nothing was published. Build the publisher on a"
              + " channel that keeps confirm mode, such as one of an auto-recovering amqp-client"
              + " connection");
    }
    // Register in outstanding BEFORE basicPublish so a confirm arriving on the connection thread
    // during or immediately after the publish always finds its seqNo registered.
    outstanding.put(seq, entry.id());
    try {
      channel.basicPublish(
          exchange,
          routingKey,
          // mandatory: an unroutable message is RETURNED (see the ReturnListener in build())
          true,
          buildProperties(entry),
          entry.payload().getBytes(StandardCharsets.UTF_8));
      return null;
    } catch (IOException | ShutdownSignalException e) {
      // basicPublish fails either with a checked IOException or, when the channel/connection is
      // already closed (broker restart, prior missing-exchange publish, a network blip mid
      // auto-recovery), an unchecked AlreadyClosedException (a ShutdownSignalException). Undo the
      // seq registration so it cannot linger in `outstanding` forever (no confirm will arrive for
      // a publish that failed, and after channel recreation the seq counter resets so a later
      // `multiple` ack never sweeps the stale seq).
      outstanding.remove(seq);
      return e;
    } catch (RuntimeException e) {
      // Any other unchecked failure, returned as a per-entry outcome so the batch result stays
      // complete. Whether the message was written is not provable (see
      // UnconfirmedPublishException), so the claim is held. Its seq is unregistered all the same:
      // the entry is settled as failed here, and a late confirm must not count its latch slot
      // down a second time.
      outstanding.remove(seq);
      return new UnconfirmedPublishException(
          "RabbitMQ channel failed the publish with "
              + e
              + " — the message may have been handed off",
          e);
    }
  }

  /**
   * Package-private for testing: the number of published-but-not-yet-settled sequence numbers still
   * registered in {@code outstanding}. A regression must leave this at zero once a batch has fully
   * settled — a leaked entry (e.g. a {@code basicPublish} throw that never removed its seq) shows
   * up here as a non-zero residual.
   */
  int outstandingSize() {
    return outstanding.size();
  }

  /**
   * Classifies a delivery failure for the poller's retry accounting, by <b>hand-off phase</b>. A
   * <em>pre</em>-hand-off connection loss ({@link ShutdownSignalException}, including {@code
   * AlreadyClosedException}) or a bare {@code basicPublish} {@link IOException} means the message
   * never reached the broker: a broker-wide {@link FailureKind#TRANSPORT} outage, re-armed
   * immediately and retried forever so a broker restart never terminal-fails the backlog. A broker
   * {@link NackedPublishException nack} is {@code TRANSPORT} too: the broker's own report that
   * <em>it</em> could not enqueue the message — a broker-side condition (queue-process error,
   * {@code reject-publish} overflow) that sweeps whatever is in flight, and a definitive "not
   * enqueued", so re-arming cannot duplicate. A <em>post</em>-hand-off failure — a confirm-wait
   * {@link java.util.concurrent.TimeoutException}, or an {@link UnconfirmedPublishException} (the
   * connection died while awaiting the confirm) — leaves a message the broker may already hold:
   * {@link FailureKind#IN_FLIGHT}, so the claim is held and only lease-reclaim redelivers; an
   * {@link UnconfirmedPublishException} also wraps an unchecked exception thrown by {@code
   * basicPublish}, whose phase is not provable. A {@link NotInConfirmModeException} or {@link
   * ChannelFailureException} is raised before anything was handed off: {@code TRANSPORT}. An {@link
   * UnroutableException} (no queue bound) or {@link UnpublishableEntryException} (an over-long id
   * or payload type) is the per-{@link FailureKind#ENTRY} rejection that counts toward terminal
   * {@code FAILED}. The whole cause chain is inspected, and every {@code IOException} subtype is
   * tested before its supertype.
   */
  @Override
  public FailureKind classifyFailure(Exception failure) {
    // Bounded walk (depth 50) so a self-referential/cyclic cause chain
    // terminates instead of spinning forever, matching PostgresEventStore.hasCryptoCause.
    Throwable c = failure;
    for (int depth = 0; c != null && depth < 50; depth++, c = c.getCause()) {
      if (c instanceof UnroutableException || c instanceof UnpublishableEntryException) {
        return FailureKind.ENTRY;
      }
      if (c instanceof NotInConfirmModeException || c instanceof ChannelFailureException) {
        // Raised before basicPublish handed anything off. Explicit, like the nack below, so the
        // classification never depends on the order of the supertype branches.
        return FailureKind.TRANSPORT;
      }
      if (c instanceof UnconfirmedPublishException) {
        // Post-hand-off connection loss: same hazard as the confirm timeout below, and it must be
        // tested before the ShutdownSignalException/IOException clause, which would otherwise
        // classify its own wrapped cause (and its IOException supertype) as pre-hand-off TRANSPORT.
        return FailureKind.IN_FLIGHT;
      }
      if (c instanceof NackedPublishException) {
        // The broker reported the message NOT enqueued — a broker-side
        // condition, never a property of the message. Non-counting, safe to re-arm. Explicit
        // (rather than leaning on the IOException supertype clause below) so the classification
        // survives any future reordering of the supertype branches.
        return FailureKind.TRANSPORT;
      }
      if (c instanceof TimeoutException) {
        // A confirm timeout means the message was already published (handed off)
        // but not yet confirmed — the broker may still deliver it. Do not release the claim; only
        // lease-reclaim may redeliver, after the in-flight publish resolves.
        return FailureKind.IN_FLIGHT;
      }
      if (c instanceof ShutdownSignalException || c instanceof IOException) {
        // basicPublish threw: the message was NOT handed off — safe to re-arm (no in-flight
        // message).
        return FailureKind.TRANSPORT;
      }
    }
    // Anything unrecognized counts toward the ladder.
    return FailureKind.ENTRY;
  }

  /**
   * In-flight horizon = {@link Builder#publishBlockBudget(Duration) publishBlockBudget} + {@code
   * confirmTimeout}.
   *
   * <p>The horizon is measured from the moment an attempt <b>starts</b> — that is what the {@code
   * OutboxPoller} gates attempt starts against ({@code lease - horizon}) and what its {@code
   * build()} guard compares the claim lease to — so it must span every phase in which a message can
   * still reach the broker. A RabbitMQ attempt has two, exactly like Kafka's {@code max.block.ms +
   * delivery.timeout.ms}:
   *
   * <ol>
   *   <li><b>Pre-hand-off ({@code publishBlockBudget}).</b> {@link Channel#basicPublish} writes the
   *       frames to the socket synchronously and the AMQP client applies <b>no timeout</b> to that
   *       write. While the broker is flow-controlled — a memory/disk high-watermark alarm issues
   *       {@code connection.blocked} and the broker stops reading the socket, or the broker is
   *       simply slower than the producer and TCP backpressure fills the send buffer — the calling
   *       thread parks inside {@code basicPublish} for as long as the condition lasts. Nothing is
   *       handed off yet and no confirm is pending, so {@code confirmTimeout} bounds none of it,
   *       and neither does the loop: that check runs <em>before</em> each publish, never during
   *       one.
   *   <li><b>Post-hand-off ({@code confirmTimeout}).</b> The published-but-unconfirmed window. A
   *       confirm timeout is the {@link FailureKind#IN_FLIGHT} case: the message reached the broker
   *       and may still be delivered.
   * </ol>
   *
   * <p>Reporting only phase 2 (the earlier behaviour) understated the horizon by the whole of phase
   * 1, so a lease sized just above {@code confirmTimeout} passed the poller's {@code build()} guard
   * and was still short enough for a second relay to reclaim and re-publish an entry whose first
   * publish was merely buffered — a duplicate <b>and</b> a same-aggregate reorder, the one thing
   * the ordering contract forbids.
   *
   * <p><b>{@code publishBlockBudget} is an assumption, not an enforcement</b> — the AMQP client
   * offers no per-publish timeout to enforce it with. The one client-side mechanism that genuinely
   * bounds phase 1 is NIO mode: {@code ConnectionFactory.useNio()} with {@code
   * NioParams.setWriteEnqueuingTimeoutInMs(...)} makes an over-long write throw instead of park
   * (and that throw is already classified {@link FailureKind#TRANSPORT} — nothing was handed off,
   * so re-arming cannot duplicate). Size the budget at or above that timeout; on blocking-IO
   * connections size it at or above the longest broker alarm you are prepared to ride out, and
   * treat the claim lease as the real backstop.
   */
  @Override
  public Duration inFlightHorizon() {
    return publishBlockBudget.plus(confirmTimeout);
  }

  private AMQP.BasicProperties buildProperties(OutboxEntry entry) {
    return new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .deliveryMode(2)
        .messageId(entry.id().value())
        .type(entry.payloadType())
        .headers(
            Map.of(
                "X-Outbox-Entry-Id", entry.id().value(),
                "X-Outbox-Payload-Type", entry.payloadType()))
        .build();
  }

  /**
   * Returns a new builder for {@link RabbitMqOutboxPublisher}.
   *
   * @return a new builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link RabbitMqOutboxPublisher}. */
  public static final class Builder {

    private Channel channel;
    private String exchange;
    private String routingKey = "";
    private Duration confirmTimeout = Duration.ofSeconds(30);
    private Duration publishBlockBudget = DEFAULT_PUBLISH_BLOCK_BUDGET;

    private Builder() {}

    /**
     * Sets the AMQP channel to use for publishing. The channel is put into confirm mode by {@link
     * #build()}, must not be shared with other producers, and must keep confirm mode and the
     * listeners {@code build()} registers for its whole lifetime — use a channel of an
     * auto-recovering amqp-client connection, not a wrapper that replaces a closed channel with a
     * fresh one (see the class javadoc).
     *
     * @param channel the AMQP channel (required, not null)
     * @return this builder
     */
    public Builder channel(Channel channel) {
      this.channel = Objects.requireNonNull(channel, "channel is required");
      return this;
    }

    /**
     * Sets the target exchange name.
     *
     * @param exchange the exchange name (required, not null or blank, at most 255 UTF-8 bytes)
     * @return this builder
     */
    public Builder exchange(String exchange) {
      Objects.requireNonNull(exchange, "exchange is required");
      if (exchange.isBlank()) {
        throw new IllegalArgumentException("exchange must not be blank");
      }
      if (exceedsShortString(exchange)) {
        throw new IllegalArgumentException("exchange must not be longer than 255 UTF-8 bytes");
      }
      this.exchange = exchange;
      return this;
    }

    /**
     * Sets the routing key. Defaults to empty string (suitable for fanout exchanges).
     *
     * @param routingKey the routing key (not null, at most 255 UTF-8 bytes)
     * @return this builder
     */
    public Builder routingKey(String routingKey) {
      Objects.requireNonNull(routingKey, "routingKey must not be null");
      if (exceedsShortString(routingKey)) {
        throw new IllegalArgumentException("routingKey must not be longer than 255 UTF-8 bytes");
      }
      this.routingKey = routingKey;
      return this;
    }

    /**
     * Sets how long each {@link #publish} or {@link #publishBatch} waits for the broker to confirm
     * the message(s). Defaults to 30 seconds.
     *
     * @param confirmTimeout the confirm timeout (required, positive)
     * @return this builder
     */
    public Builder confirmTimeout(Duration confirmTimeout) {
      Objects.requireNonNull(confirmTimeout, "confirmTimeout must not be null");
      if (confirmTimeout.isZero() || confirmTimeout.isNegative()) {
        throw new IllegalArgumentException("confirmTimeout must be positive");
      }
      this.confirmTimeout = confirmTimeout;
      return this;
    }

    /**
     * Sets the assumed upper bound on how long a single {@link Channel#basicPublish} may block
     * <em>before</em> the message is handed to the broker. Defaults to {@link
     * #DEFAULT_PUBLISH_BLOCK_BUDGET 30 seconds}.
     *
     * <p>This is the pre-hand-off half of {@link RabbitMqOutboxPublisher#inFlightHorizon()} (see
     * that javadoc for why it is needed and how to enforce it). It is a <b>sizing declaration</b>:
     * raising it raises the minimum claim lease {@code OutboxPoller.build()} accepts and tightens
     * the per-cycle publish deadline, so an under-sized lease fails at wiring instead of silently
     * duplicating. {@link Duration#ZERO} opts out — legitimate only when the write provably cannot
     * block (NIO mode with a {@code writeEnqueuingTimeoutInMs} that turns the block into a thrown,
     * {@link FailureKind#TRANSPORT}-classified exception).
     *
     * @param publishBlockBudget the pre-hand-off block budget (required, not null, not negative;
     *     zero opts out)
     * @return this builder
     */
    public Builder publishBlockBudget(Duration publishBlockBudget) {
      Objects.requireNonNull(publishBlockBudget, "publishBlockBudget must not be null");
      if (publishBlockBudget.isNegative()) {
        throw new IllegalArgumentException("publishBlockBudget must not be negative");
      }
      this.publishBlockBudget = publishBlockBudget;
      return this;
    }

    /**
     * Builds the {@link RabbitMqOutboxPublisher}, puts the channel into confirm mode via {@link
     * Channel#confirmSelect()}, registers a {@link ConfirmListener} that drives the per-batch async
     * confirm tracking, and registers a {@link ReturnListener} so an unroutable {@code mandatory}
     * message is recorded and retried instead of being silently marked delivered.
     *
     * @return a new {@link RabbitMqOutboxPublisher}
     * @throws IOException if enabling publisher confirms on the channel fails
     * @throws IllegalStateException if the channel still reports publish sequence number 0 after
     *     {@code confirmSelect()} — it is not in confirm mode, so no publish could be confirmed
     */
    public RabbitMqOutboxPublisher build() throws IOException {
      Objects.requireNonNull(channel, "channel is required");
      Objects.requireNonNull(exchange, "exchange is required");
      channel.confirmSelect();
      if (channel.getNextPublishSeqNo() <= 0) {
        throw new IllegalStateException(
            "RabbitMQ channel is "
                + NOT_IN_CONFIRM_MODE
                + " after confirmSelect() (publish sequence number 0); pass the publisher a"
                + " plain amqp-client channel");
      }
      var publisher =
          new RabbitMqOutboxPublisher(
              channel, exchange, routingKey, confirmTimeout, publishBlockBudget);
      channel.addConfirmListener(
          new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) {
              publisher.settleConfirms(deliveryTag, multiple, true);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) {
              publisher.settleConfirms(deliveryTag, multiple, false);
            }
          });
      channel.addReturnListener(
          (replyCode, replyText, exchangeName, rKey, properties, body) ->
              publisher.handleReturn(properties));
      return publisher;
    }
  }

  /**
   * Called by the {@link ReturnListener} on the connection's I/O thread when the broker returns an
   * unroutable {@code mandatory} message. Records the entry id so the publishing path treats it as
   * a delivery failure (retried) rather than the delivered confirm that the broker still issues.
   *
   * @param properties the returned message's AMQP properties, carrying the entry id in {@code
   *     messageId} (with the {@code X-Outbox-Entry-Id} header as a fallback)
   */
  void handleReturn(AMQP.BasicProperties properties) {
    OutboxEntryId id = resolveEntryId(properties);
    if (id != null) {
      returned.add(id);
    }
  }

  /**
   * Resolves the {@link OutboxEntryId} of a returned message from its AMQP properties: {@code
   * messageId} first (set by {@link #buildProperties}), then the {@code X-Outbox-Entry-Id} header
   * as a fallback. Returns {@code null} if neither is present or usable, so a malformed return is
   * ignored rather than crashing the connection's return-dispatch thread.
   */
  private static OutboxEntryId resolveEntryId(AMQP.BasicProperties properties) {
    if (properties == null) {
      return null;
    }
    String candidate = properties.getMessageId();
    if (candidate == null || candidate.isBlank()) {
      Map<String, Object> headers = properties.getHeaders();
      Object headerValue = headers == null ? null : headers.get("X-Outbox-Entry-Id");
      candidate = headerValue == null ? null : headerValue.toString();
    }
    if (candidate == null || candidate.isBlank()) {
      return null;
    }
    return OutboxEntryId.of(candidate);
  }

  /**
   * Called by the {@link ConfirmListener} on the connection's I/O thread when the broker acks or
   * nacks one or more messages.
   *
   * @param deliveryTag the highest sequence number being confirmed
   * @param multiple if {@code true}, all sequence numbers up to and including {@code deliveryTag}
   *     are confirmed
   * @param ack {@code true} for ack, {@code false} for nack
   */
  void settleConfirms(long deliveryTag, boolean multiple, boolean ack) {
    Batch batch = currentBatch;
    if (batch == null) {
      // No batch has ever been active — a confirm arrived before the first publish on this
      // publisher (both SPI paths settle through the batch machinery). Drop the bookkeeping.
      outstanding.headMap(deliveryTag, true).clear();
      return;
    }

    // Settle by ATOMIC CLAIM: only the thread that removes an entry from `outstanding` settles
    // it, and nothing is removed unconditionally. A get-then-remove-later (or iterate-then-
    // clear) would let this thread consume an entry that another path registered or already
    // claimed in between — removing it without settling it, i.e. a dropped confirm.
    if (multiple) {
      for (Map.Entry<Long, OutboxEntryId> e : outstanding.headMap(deliveryTag, true).entrySet()) {
        if (outstanding.remove(e.getKey(), e.getValue())) {
          settle(batch, e.getValue(), ack);
        }
      }
    } else {
      OutboxEntryId id = outstanding.remove(deliveryTag);
      if (id != null) {
        settle(batch, id, ack);
      }
    }
  }

  /** Records one claimed confirm against the batch's accounting and latch. */
  private void settle(Batch batch, OutboxEntryId id, boolean ack) {
    if (!batch.ids.contains(id)) {
      // Leftover seqNo from an earlier batch — its late confirm must not touch this latch.
      return;
    }
    // Set.add returns false when the id already settled (a retried entry can settle via both
    // its stale and its fresh seqNo): count the latch down at most once per entry, and never
    // let a late ack overturn an already-recorded nack (or vice versa).
    boolean firstSettlement =
        ack
            ? !batch.nacked.contains(id) && batch.acked.add(id)
            : !batch.acked.contains(id) && batch.nacked.add(id);
    if (firstSettlement) {
      batch.latch.countDown();
    }
  }
}
