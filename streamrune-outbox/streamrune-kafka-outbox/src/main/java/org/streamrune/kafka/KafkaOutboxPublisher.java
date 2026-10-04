package org.streamrune.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.outbox.OutboxEntry;
import org.streamrune.core.outbox.OutboxPublisher;

/**
 * {@link OutboxPublisher} that delivers outbox entries to an Apache Kafka topic.
 *
 * <p>Sends a {@link ProducerRecord} per entry with:
 *
 * <ul>
 *   <li><b>Key:</b> the text form of {@link OutboxEntry#streamId()} ({@code
 *       <aggregateType>:<aggregateId>}, e.g. {@code order:o-1}) when present, otherwise {@link
 *       OutboxEntry#id()}. Kafka only guarantees ordering within a partition, and the partition is
 *       chosen by key — keying by stream keeps all events of one stream on the same partition, so
 *       consumers see them in publish order. Entries without a stream fall back to the entry id,
 *       which spreads them across partitions with <b>no ordering guarantee between entries</b>. A
 *       consumer that needs the two parts splits the key at the first {@code ':'} (an aggregate
 *       type never contains one) or reads them from the payload.
 *   <li><b>Value:</b> {@link OutboxEntry#payload()} — JSON string
 *   <li><b>Headers:</b> {@code X-Outbox-Entry-Id} and {@code X-Outbox-Payload-Type}
 * </ul>
 *
 * <p>Sends synchronously via {@code producer.send(record).get(sendTimeout)} so that the
 * OutboxPoller can retry on failure. The send is bounded by a configurable {@code sendTimeout}
 * (default 10s): Kafka's own {@code delivery.timeout.ms} defaults to 120s, and with a batch of up
 * to 100 sequential entries an unbounded {@code get()} could hold one relay's claim far past the
 * outbox claim lease, letting a second relay reclaim and re-deliver the same entries out of order.
 * On timeout a {@link java.util.concurrent.TimeoutException} is thrown so the poller retries the
 * entry; keep the send timeout well below the claim lease. Does <b>not</b> close the producer —
 * lifecycle is the caller's responsibility.
 *
 * <p><b>{@code sendTimeout} does not bound the whole call.</b> {@code send()} runs before {@code
 * get()} and blocks on its own for up to {@code max.block.ms} (60s default) while waiting for topic
 * metadata or accumulator space, so one {@code publish} can occupy the relay thread for {@code
 * max.block.ms + sendTimeout} — and can leave the record live at the transport for {@code
 * max.block.ms + delivery.timeout.ms} measured from the attempt's start. That sum is the {@link
 * #inFlightHorizon() in-flight horizon} the claim lease must strictly exceed.
 */
public final class KafkaOutboxPublisher implements OutboxPublisher {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaOutboxPublisher.class);

  /** Default bound on {@code producer.send(record).get(...)}. */
  static final Duration DEFAULT_SEND_TIMEOUT = Duration.ofSeconds(10);

  /**
   * Kafka's own default {@code delivery.timeout.ms} (120s). Contributes the <em>post</em>-{@code
   * send()} phase of the {@link #inFlightHorizon() in-flight horizon} when the producer config
   * passed to {@link Builder#producerConfig(Map)} does not set the key (or none was passed). Kafka
   * has defaulted {@code delivery.timeout.ms} to 120000 for years; see {@code
   * org.apache.kafka.clients.producer.ProducerConfig}.
   */
  static final Duration DEFAULT_DELIVERY_TIMEOUT = Duration.ofMillis(120_000L);

  /**
   * Kafka's own default {@code max.block.ms} (60s). Contributes the <em>pre</em>-{@code send()}
   * phase of the {@link #inFlightHorizon() in-flight horizon} when the producer config passed to
   * {@link Builder#producerConfig(Map)} does not set the key (or none was passed): {@code
   * KafkaProducer.send()} blocks the calling thread for up to this long waiting for topic metadata
   * or for buffer space in the record accumulator, <em>before</em> the record is enqueued and the
   * {@code delivery.timeout.ms} clock starts. Kafka has defaulted {@code max.block.ms} to 60000 for
   * years; see {@code org.apache.kafka.clients.producer.ProducerConfig}.
   */
  static final Duration DEFAULT_MAX_BLOCK = Duration.ofMillis(60_000L);

  private final KafkaProducer<String, String> producer;
  private final String topic;
  private final Duration sendTimeout;
  private final Duration inFlightHorizon;

  private KafkaOutboxPublisher(
      KafkaProducer<String, String> producer,
      String topic,
      Duration sendTimeout,
      Duration inFlightHorizon) {
    this.producer = producer;
    this.topic = topic;
    this.sendTimeout = sendTimeout;
    this.inFlightHorizon = inFlightHorizon;
  }

  /**
   * The effective {@code delivery.timeout.ms} from a producer config map — a user override when the
   * key is present, else Kafka's own {@link #DEFAULT_DELIVERY_TIMEOUT 120s default}.
   */
  private static Duration deliveryTimeoutFrom(Map<String, ?> config) {
    return millisFrom(config, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DEFAULT_DELIVERY_TIMEOUT);
  }

  /**
   * The effective {@code max.block.ms} from a producer config map — a user override when the key is
   * present, else Kafka's own {@link #DEFAULT_MAX_BLOCK 60s default}. Read from the SAME map as
   * {@link #deliveryTimeoutFrom}, so the same "pass the real producer config" contract on {@link
   * Builder#producerConfig(Map)} governs both.
   */
  private static Duration maxBlockFrom(Map<String, ?> config) {
    return millisFrom(config, ProducerConfig.MAX_BLOCK_MS_CONFIG, DEFAULT_MAX_BLOCK);
  }

  /**
   * Reads a millisecond-valued Kafka producer config key, falling back to {@code fallback} when it
   * is absent. Kafka config values arrive as either a {@link Number} or a {@link String}, so both
   * are accepted.
   */
  private static Duration millisFrom(Map<String, ?> config, String key, Duration fallback) {
    Object value = config.get(key);
    if (value == null) {
      return fallback;
    }
    long millis =
        (value instanceof Number number)
            ? number.longValue()
            : Long.parseLong(value.toString().trim());
    return Duration.ofMillis(millis);
  }

  @Override
  public void publish(OutboxEntry entry) throws Exception {
    String key = entry.streamId() != null ? entry.streamId().value() : entry.id().value();
    var record = new ProducerRecord<>(topic, key, entry.payload());
    record
        .headers()
        .add("X-Outbox-Entry-Id", entry.id().value().getBytes(StandardCharsets.UTF_8))
        .add("X-Outbox-Payload-Type", entry.payloadType().getBytes(StandardCharsets.UTF_8));
    try {
      // Bound the send so a degraded broker cannot block one entry for delivery.timeout.ms
      // (120s default) and push the whole publish cycle past the claim lease. This send bound
      // elapsing throws a java.util.concurrent.TimeoutException, which classifyFailure classes
      // IN_FLIGHT — the record stays buffered in the producer and may still be
      // delivered, so the poller leaves the entry claimed for lease-reclaim rather than re-arming
      // it.
      producer.send(record).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      // Restore the interrupt flag: get() consumed it, and the OutboxPoller's loop condition
      // checks Thread.currentThread().isInterrupted() — without this, close() would never stop
      // a poller that was blocked in send().get(), and the interrupt would be misrecorded as an
      // ordinary delivery failure.
      Thread.currentThread().interrupt();
      throw e;
    } catch (ExecutionException e) {
      throw (e.getCause() instanceof Exception ex) ? ex : new RuntimeException(e.getCause());
    }
  }

  /**
   * Classifies a send failure for the poller's retry accounting; the whole cause chain is inspected
   * so an unwrapped {@code ExecutionException} cause is classified correctly. The bounded {@code
   * send().get(...)} {@link java.util.concurrent.TimeoutException} (our own send wait elapsed) is
   * {@link FailureKind#IN_FLIGHT} — the record stays buffered in the producer and is retried
   * internally until {@code delivery.timeout.ms}, so it may still be delivered; the poller leaves
   * the entry claimed for lease-reclaim rather than re-arming it (which could duplicate and
   * reorder). {@link #isPostHandoffCapable Kafka's own post-hand-off-capable retriables} — its
   * {@code TimeoutException}, {@code NetworkException}, {@code
   * NotEnoughReplicasAfterAppendException} — are {@code IN_FLIGHT} too: each can surface while (or
   * after) the broker holds the record, so releasing the claim risks a duplicate + same-aggregate
   * reorder. Both checks run before the {@link org.apache.kafka.common.errors.RetriableException}
   * test, which classifies the remaining, provably-pre-append retriables (a leader-election or
   * not-enough-replicas <em>error response</em> — the broker answered without appending) as a
   * broker-wide {@link FailureKind#TRANSPORT} outage (retried forever, never terminal-fails the
   * backlog). {@link #isFleetWide A fleet-wide non-retriable condition} — an authentication or
   * authorization failure, an invalid topic, a closed producer — is {@code TRANSPORT} too. Anything
   * else — a non-retriable Kafka error such as {@code RecordTooLargeException} or a serialization
   * error — is a per-entry rejection: {@link FailureKind#ENTRY} (counts toward terminal {@code
   * FAILED}).
   */
  @Override
  public FailureKind classifyFailure(Exception failure) {
    // Bounded walk (depth 50) so a self-referential/cyclic cause chain
    // terminates instead of spinning forever, matching PostgresEventStore.hasCryptoCause.
    Throwable c = failure;
    for (int depth = 0; c != null && depth < 50; depth++, c = c.getCause()) {
      if (c instanceof java.util.concurrent.TimeoutException) {
        // send().get() timed out but the record remains buffered in the producer's
        // accumulator and is retried internally until delivery.timeout.ms — it may still be
        // delivered. Do not release the claim; only lease-reclaim may redeliver (the lease must
        // exceed delivery.timeout.ms).
        return FailureKind.IN_FLIGHT;
      }
      if (isPostHandoffCapable(c)) {
        // This failure does not prove the record is dead at the
        // broker. Tested BEFORE the RetriableException clause, which would bin these same types
        // TRANSPORT and release a claim the broker may still fulfill.
        return FailureKind.IN_FLIGHT;
      }
      if (c instanceof org.apache.kafka.common.errors.RetriableException || isFleetWide(c)) {
        return FailureKind.TRANSPORT;
      }
    }
    return FailureKind.ENTRY;
  }

  /**
   * Whether {@code c} is a Kafka {@link org.apache.kafka.common.errors.RetriableException} that can
   * surface <b>after the record was handed off</b> — so classifying it {@link
   * FailureKind#TRANSPORT} (which re-arms the entry immediately) would let a second relay
   * re-publish a record the broker may still land: a duplicate <em>and</em> a same-aggregate
   * reorder. Reachable at the shipped defaults simply by lowering {@code delivery.timeout.ms} below
   * {@code sendTimeout} — a tuning the claim-lease sizing docs recommend as a horizon lever — after
   * which every expiry surfaces Kafka's own {@code TimeoutException} instead of ours. All three
   * members verified against kafka-clients 3.9.0:
   *
   * <ul>
   *   <li>{@link org.apache.kafka.common.errors.TimeoutException} — surfaced through the send
   *       future when {@code delivery.timeout.ms} expires. Expiry fails the batch <em>locally</em>;
   *       a produce request carrying it may still be in flight, and the broker may append it after
   *       the client gave up. The same type is ALSO thrown synchronously by {@code send()} for a
   *       {@code max.block.ms} metadata wait — provably pre-enqueue — but by the time the exception
   *       reaches this classifier the two phases are indistinguishable from the type alone, and the
   *       SPI contract is explicit: an unprovable phase classifies {@code IN_FLIGHT}. The cost of
   *       that caution is bounded — a metadata outage parks the backlog on claim-lease cadence
   *       (visible on {@code streamrune.outbox.in_flight}) instead of the 60s-capped transport
   *       cadence — whereas the old TRANSPORT binning risked duplicates. Splitting the phases at
   *       the throw site (wrapping every future-surfaced failure in a marker type) was considered
   *       and rejected: it rewrites the operator-visible {@code last_error} cause chains of every
   *       async failure to buy back only that recovery latency.
   *   <li>{@link org.apache.kafka.common.errors.NetworkException} — the connection dropped after
   *       the produce request was sent; the broker may have processed it (Kafka's javadoc: the
   *       request "may or may not" have been acted on).
   *   <li>{@link org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException} — the broker
   *       APPENDED the record to the leader log and then failed the request on the ISR check; Kafka
   *       documents the write may still become visible. The definitional post-hand-off case. Its
   *       sibling {@code NotEnoughReplicasException} (no "AfterAppend") is the broker
   *       <em>refusing</em> to append — a definitive negative verdict, deliberately NOT a member:
   *       it stays a pre-append TRANSPORT via the RetriableException clause.
   * </ul>
   */
  private static boolean isPostHandoffCapable(Throwable c) {
    return c instanceof org.apache.kafka.common.errors.TimeoutException
        || c instanceof org.apache.kafka.common.errors.NetworkException
        || c instanceof org.apache.kafka.common.errors.NotEnoughReplicasAfterAppendException;
  }

  /**
   * Whether {@code c} is a <b>fleet</b> condition rather than a property of the message. Kafka
   * marks only <em>transient</em> errors {@link org.apache.kafka.common.errors.RetriableException},
   * so the classifier's old "not retriable ⇒ per-entry" deny-list swept a whole class of
   * broker-wide outages into the counting {@link FailureKind#ENTRY} bin: with the shipped ladder
   * (10 attempts, 60s cap) a ~5-minute credential rotation or ACL slip terminal-{@code FAILED}
   * <em>every</em> entry it touched — permanent, silent message loss, plus the silent
   * same-aggregate reorder that follows once a {@code FAILED} head stops gating its higher-seq
   * successors (they are then claimed and delivered past it). Membership is the same phase+scope
   * test the sibling publishers apply ({@code RabbitMqOutboxPublisher} bins a pre-hand-off {@code
   * ShutdownSignalException}/{@code AlreadyClosedException} TRANSPORT; {@code HttpOutboxPublisher}
   * bins an endpoint-wide 5xx/429 TRANSPORT), and every member is provably <em>pre</em>-hand-off,
   * so re-arming cannot duplicate:
   *
   * <ul>
   *   <li>{@link org.apache.kafka.common.errors.AuthenticationException} (e.g. {@code
   *       SaslAuthenticationException}) — the connection never authenticated, so no produce request
   *       was ever accepted. A credential is a property of the relay, not of a message.
   *   <li>{@link org.apache.kafka.common.errors.AuthorizationException} (e.g. {@code
   *       TopicAuthorizationException}, {@code ClusterAuthorizationException}) — the broker refused
   *       the produce request on an ACL check <em>before</em> appending. Every entry of this
   *       publisher targets the same topic under the same principal, so the refusal is identical
   *       for all of them.
   *   <li>{@link org.apache.kafka.common.errors.InvalidTopicException} — the topic is fixed at
   *       publisher construction, so an invalid or being-deleted topic can never be attributable to
   *       one message.
   *   <li>A closed-producer {@link IllegalStateException} — {@code KafkaProducer.send()} throws
   *       this from its {@code throwIfProducerClosed()} guard <em>before</em> the record is
   *       enqueued, and it will do so for every subsequent entry too. Matched on the message
   *       because Kafka ships no dedicated type for it; a generic {@code IllegalStateException}
   *       carries no fleet evidence and deliberately keeps the counting {@code ENTRY} default.
   * </ul>
   *
   * <p>The residual risk of this bin is a <em>stall</em>: a fleet condition that never clears
   * (revoked credentials nobody restores) retries forever with capped backoff, loudly — the
   * poller's aggregated transport WARN fires every cycle. That is the deliberate trade: a loud
   * stall is recoverable, whereas the ENTRY misclassification it replaces was silent loss plus a
   * silent ordering break. It matches the call {@code RabbitMqOutboxPublisher} already makes for a
   * dead channel.
   */
  private static boolean isFleetWide(Throwable c) {
    return c instanceof org.apache.kafka.common.errors.AuthenticationException
        || c instanceof org.apache.kafka.common.errors.AuthorizationException
        || c instanceof org.apache.kafka.common.errors.InvalidTopicException
        || (c instanceof IllegalStateException
            && String.valueOf(c.getMessage()).contains("producer has been closed"));
  }

  /**
   * In-flight horizon = effective {@code max.block.ms} + effective {@code delivery.timeout.ms},
   * both read from the producer config supplied to {@link Builder#producerConfig(Map)} (Kafka's
   * {@link #DEFAULT_MAX_BLOCK 60s} / {@link #DEFAULT_DELIVERY_TIMEOUT 120s} defaults when unset —
   * so the shipped default horizon is <b>180s</b>).
   *
   * <p>A Kafka publish is a <b>two-phase</b> envelope and the horizon must span both phases,
   * because the {@code OutboxPoller} gates attempt <em>starts</em> against it (the {@code lease -
   * horizon} publish-deadline cap) while the reclaim it protects against fires on wall time:
   *
   * <ol>
   *   <li><b>Before</b> {@code send()} returns: {@code KafkaProducer.send()} blocks the calling
   *       thread for up to {@code max.block.ms} waiting for topic metadata or for space in the
   *       record accumulator — precisely the degraded-broker condition this machinery targets. The
   *       record is not yet enqueued, so no delivery clock is running.
   *   <li><b>After</b> {@code send()} returns: the record sits in the accumulator and is retried
   *       internally until {@code delivery.timeout.ms} elapses — even after our own {@code
   *       send().get(sendTimeout)} throws a {@code TimeoutException}, it may still be delivered
   *       (the {@link FailureKind#IN_FLIGHT} case).
   * </ol>
   *
   * <p>Measured from the attempt's start — which is what the deadline cap and the {@code build()}
   * lease guard both need — the worst case is therefore the SUM, not {@code delivery.timeout.ms}
   * alone (and certainly not {@code sendTimeout}, which only bounds our own blocking wait). The
   * claim lease must strictly exceed this sum so a reclaim cannot re-publish a record the producer
   * is still delivering. Operators who want a shorter lease should lower {@code max.block.ms} (a
   * few seconds is ample for a healthy cluster): it shrinks the horizon one-for-one.
   */
  @Override
  public Duration inFlightHorizon() {
    return inFlightHorizon;
  }

  /** Returns a new builder for {@link KafkaOutboxPublisher}. */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link KafkaOutboxPublisher}. */
  public static final class Builder {

    private KafkaProducer<String, String> producer;
    private String topic;
    private Duration sendTimeout = DEFAULT_SEND_TIMEOUT;
    private Map<String, ?> producerConfig = Map.of();

    private Builder() {}

    /**
     * Supplies the producer configuration used to build the {@link #producer(KafkaProducer)} so the
     * publisher can derive its {@link KafkaOutboxPublisher#inFlightHorizon() in-flight horizon}
     * from the effective {@code max.block.ms} <b>+</b> {@code delivery.timeout.ms}. Pass the SAME
     * {@code Map} / {@code Properties} used to construct the {@code KafkaProducer}. When unset — or
     * when the map does not contain a key — Kafka's own defaults are assumed ({@link
     * KafkaOutboxPublisher#DEFAULT_MAX_BLOCK 60s} + {@link
     * KafkaOutboxPublisher#DEFAULT_DELIVERY_TIMEOUT 120s} = a 180s horizon).
     *
     * <p>Setting either key such that their sum reaches the outbox claim lease makes {@code
     * OutboxPoller.build()} fail fast, so always pass the real config when you raise one past its
     * default; the safe fallbacks can under-state a raised value if the config is withheld.
     * Conversely, lowering {@code max.block.ms} is the cheapest way to shrink the horizon (and thus
     * the minimum safe claim lease) on a healthy cluster.
     *
     * @param producerConfig the producer config map (required, not null; may be empty)
     * @return this builder
     */
    public Builder producerConfig(Map<String, ?> producerConfig) {
      this.producerConfig = Objects.requireNonNull(producerConfig, "producerConfig is required");
      return this;
    }

    /**
     * Sets the bound on each synchronous {@code producer.send(record).get(...)}. Defaults to 10
     * seconds. Keep it well below the outbox claim lease so a slow broker cannot hold a claim until
     * another relay reclaims it. On timeout the publish throws a {@link
     * java.util.concurrent.TimeoutException} and the poller retries the entry.
     *
     * @param sendTimeout the per-send timeout (required, not null, positive)
     * @return this builder
     */
    public Builder sendTimeout(Duration sendTimeout) {
      Objects.requireNonNull(sendTimeout, "sendTimeout is required");
      if (sendTimeout.isZero() || sendTimeout.isNegative()) {
        throw new IllegalArgumentException("sendTimeout must be positive");
      }
      this.sendTimeout = sendTimeout;
      return this;
    }

    /**
     * Sets the Kafka producer to use for sending records.
     *
     * @param producer the Kafka producer (required, not null)
     * @return this builder
     */
    public Builder producer(KafkaProducer<String, String> producer) {
      this.producer = Objects.requireNonNull(producer, "producer is required");
      return this;
    }

    /**
     * Sets the target Kafka topic.
     *
     * @param topic the topic name (required, not null or blank)
     * @return this builder
     */
    public Builder topic(String topic) {
      Objects.requireNonNull(topic, "topic is required");
      if (topic.isBlank()) {
        throw new IllegalArgumentException("topic must not be blank");
      }
      this.topic = topic;
      return this;
    }

    /**
     * Builds the {@link KafkaOutboxPublisher}.
     *
     * @return a new {@link KafkaOutboxPublisher}
     */
    public KafkaOutboxPublisher build() {
      Objects.requireNonNull(producer, "producer is required");
      Objects.requireNonNull(topic, "topic is required");
      // The horizon spans BOTH phases of a send — the pre-enqueue block
      // (max.block.ms) and the post-enqueue internal retry window (delivery.timeout.ms).
      Duration horizon = maxBlockFrom(producerConfig).plus(deliveryTimeoutFrom(producerConfig));
      Duration deliveryTimeout = deliveryTimeoutFrom(producerConfig);
      if (deliveryTimeout.compareTo(sendTimeout) < 0) {
        // With delivery.timeout.ms below sendTimeout, the
        // producer expires each attempt BEFORE our own bounded get() would, so every degraded-send
        // failure surfaces Kafka's own TimeoutException instead of ours. Classification stays safe
        // (both are IN_FLIGHT), but the tuning is almost certainly unintended: the sendTimeout
        // bound never fires, and every expiry parks its entry for a full claim lease. Only a WARN
        // — the config map is advisory (the builder cannot see the producer's true settings when
        // the caller withholds them), so refusing to build on it would punish an honest map more
        // than a withheld one.
        LOG.warn(
            "KafkaOutboxPublisher: effective delivery.timeout.ms ({}ms) is below sendTimeout"
                + " ({}ms), so the producer expires sends before the bounded send().get() ever"
                + " times out and every expiry is held IN_FLIGHT for a full claim lease. Keep"
                + " delivery.timeout.ms above sendTimeout (or lower sendTimeout) so the send bound"
                + " is the one that fires.",
            deliveryTimeout.toMillis(),
            sendTimeout.toMillis());
      }
      return new KafkaOutboxPublisher(producer, topic, sendTimeout, horizon);
    }
  }
}
