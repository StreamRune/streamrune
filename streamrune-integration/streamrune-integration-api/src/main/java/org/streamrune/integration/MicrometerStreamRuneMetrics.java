package org.streamrune.integration;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.metrics.MetricNames;
import org.streamrune.core.projection.ProjectionDeliveryMode;
import org.streamrune.core.types.ProjectionName;
import org.streamrune.core.types.SubscriptionName;

/**
 * Micrometer-backed implementation of {@link StreamRuneMetrics}. Records counters and timers using
 * the provided {@link MeterRegistry}. Meter names start with the configured prefix (default {@code
 * streamrune}), e.g. {@code streamrune.commands.dispatched}.
 *
 * <p><b>Names come from {@link MetricNames}, never from string literals</b>. Every registration
 * passes a {@code MetricNames} constant through {@link #meter(String)}, which rebases it onto the
 * configured prefix — so the operator-facing contract and the emitted series cannot drift apart.
 * See {@link #meter(String)} for the divergence that motivated this.
 *
 * <p><b>One meter name, one tag-key set.</b> Every series registered under a given meter name here
 * carries the SAME tag keys. This is not a style preference: a {@code PrometheusMeterRegistry} —
 * the registry behind {@code micrometer-registry-prometheus} and {@code
 * quarkus-micrometer-registry-prometheus}, i.e. the standard production monitoring stack — maps a
 * meter name onto ONE Prometheus metric family, and the Prometheus data model requires every series
 * in a family to carry the same label keys. Registering {@code streamrune.projections.processed}
 * bare and then {@code streamrune.projections.processed{projection.name=orders}} does NOT throw:
 * the second meter is accepted by Micrometer (a {@code registry.find(...)} even locates it, with
 * the right count) while the Prometheus collector keeps the FIRST registration's label set and the
 * tagged samples never reach {@code /actuator/prometheus} at all. The scrape then reads {@code
 * streamrune_projections_processed_total 0.0} forever while traffic flows — silently, with no
 * exception for the guarded metric call sites to log. {@code SimpleMeterRegistry} dedups by the
 * full id and happily shows both series, which is why a green suite on it proves nothing here;
 * {@code MicrometerStreamRuneMetricsPrometheusTest} pins the invariant on a real Prometheus
 * registry. The same registry constraint is documented at {@code SubscriptionHealthContributor}.
 *
 * <p>Entry points that carry no dimension value ({@link #recordCommandDispatched()}, {@link
 * #recordEventAppended()}, the no-arg projection methods) therefore emit under {@link
 * #UNKNOWN_TAG_VALUE} instead of opening a second, tag-less shape of the same name. Aggregate
 * queries ({@code sum(rate(streamrune_commands_dispatched_total[5m]))}) are unaffected, and a call
 * site that later starts supplying the real type simply moves traffic out of the {@code unknown}
 * bucket rather than changing the metric's shape — which after 1.0 would break every dashboard
 * written against it. The command-type family — {@code commands.retried}, {@code
 * commands.short.circuited}, {@code dlq.published}, {@code dlq.exhausted} — is tagged by {@code
 * command.type} on every series (the first three used to discard the type and emit bare), with a
 * null/blank type reported under the same sentinel.
 *
 * <p><b>Emission status:</b> the untagged meters are registered eagerly at construction so they are
 * discoverable; a tagged meter is registered lazily, one time series per tag value, the first time
 * its entry point records a sample. The command bus ({@code VirtualThreadCommandBus}) records the
 * command-side meters; the projection runners and query bus record the {@code projections.*} and
 * {@code queries.*} meters once the integration wires this collector into their builders (see the
 * {@code metrics(...)} builder methods on the runtime runners and query buses). The {@code
 * subscriptions.lag} gauge is registered lazily, one tagged time series per subscription name, the
 * first time {@link #recordSubscriptionLag(SubscriptionName, long)} is called (e.g. by the
 * runtime's subscription health contributor). The {@code projections.delivery_mode} gauge is
 * registered lazily as well, one series of value {@code 1} per {@code (projection.name,
 * delivery.mode)}, by {@link #recordProjectionDeliveryMode(ProjectionName, ProjectionDeliveryMode)}
 * — called once per projection registration at start, by the executing runner or, for an INLINE
 * registration, by the integration.
 */
public class MicrometerStreamRuneMetrics implements StreamRuneMetrics {

  /** Default meter name prefix. */
  public static final String DEFAULT_PREFIX = "streamrune";

  /**
   * Tag value used when a meter's dimension is not available at the call site.
   *
   * <p>A meter name commits to exactly one tag-key set — see the class javadoc for why a
   * PrometheusMeterRegistry makes that mandatory — so an entry point that has no {@code
   * command.type}/{@code event.type}/{@code projection.name} to report cannot simply omit the tag:
   * that would be a second, incompatible shape of the same metric family and its samples would be
   * dropped from the scrape. It reports {@code unknown} instead, which is the same convention
   * Micrometer's own instrumentation uses for an unavailable dimension.
   */
  public static final String UNKNOWN_TAG_VALUE = "unknown";

  private final MeterRegistry registry;
  private final String prefix;

  /**
   * One {@link Gauge} per subscription name, each backed by an {@link AtomicLong} holding the
   * latest reported lag. Registered lazily on first report so only observed subscriptions create a
   * meter, and updated in place on every report (gauge semantics, last write wins).
   */
  private final ConcurrentHashMap<SubscriptionName, AtomicLong> subscriptionLagValues =
      new ConcurrentHashMap<>();

  /**
   * Subscription names whose lag is served by a live supplier gauge (registered via {@link
   * #registerSubscriptionLagGauge}), mapped to the supplier that backs each. The push {@link
   * #recordSubscriptionLag} path skips names present here so it never registers a competing meter
   * with the same name+tag.
   *
   * <p><b>This map is what keeps each supplier gauge alive.</b> Micrometer's 3-arg {@code
   * Gauge.builder(name, stateObject, valueFn)} holds its state object — here the {@link
   * java.util.function.LongSupplier} — via a {@code WeakReference} by default, so without a
   * long-lived strong reference the supplier is collected on the first GC and the gauge reads
   * {@code Double.NaN} for the rest of the process. Retaining it in this instance field
   * (belt-and-braces with {@code .strongReference(true)} on the builder) makes the gauge durable
   * for the process lifetime, matching the sibling {@code AtomicLong}-field gauges in this class.
   */
  private final ConcurrentHashMap<SubscriptionName, java.util.function.LongSupplier>
      supplierBackedLagGauges = new ConcurrentHashMap<>();

  /**
   * Saga backlog gauges, one {@link AtomicLong} per saga type, each backing a {@code
   * streamrune.saga.compensating} gauge tagged by {@code saga.type}. Registered lazily on first
   * report and updated in place (gauge semantics, last write wins) — mirrors {@link
   * #subscriptionLagValues}.
   */
  private final ConcurrentHashMap<String, AtomicLong> sagaCompensatingBacklog =
      new ConcurrentHashMap<>();

  /** Saga backlog gauges, one per saga type, backing {@code streamrune.saga.timed_out}. */
  private final ConcurrentHashMap<String, AtomicLong> sagaTimedOutBacklog =
      new ConcurrentHashMap<>();

  /**
   * Saga backlog gauges, one {@link AtomicLong} per saga type, each backing a {@code
   * streamrune.saga.faulted_rows} gauge tagged by {@code saga.type} — the FAULTED {@code
   * saga_state} row count (entry-less automatic quarantines included), sampled by {@link
   * org.streamrune.core.StreamRuneMetrics#recordSagaFaultedRows(String, long)}. Registered lazily
   * on first report and updated in place (gauge semantics, last write wins) — mirrors {@link
   * #sagaCompensatingBacklog} and {@link #sagaTimedOutBacklog}.
   */
  private final ConcurrentHashMap<String, AtomicLong> sagaFaultedRowsBacklog =
      new ConcurrentHashMap<>();

  private final Counter commandsDispatched;
  private final Counter commandsSucceeded;
  private final Counter commandsFailed;
  private final Timer commandDuration;
  private final Timer locksWait;
  private final Counter commandsAsyncRejected;

  /**
   * Retains a strong reference to the {@code streamrune.commands.in_flight} gauge's backing
   * supplier — Micrometer's 2-arg {@code Gauge.builder(name, valueFn)} would hold a {@code
   * LongSupplier} lambda only weakly by default (see the note on {@link #supplierBackedLagGauges}),
   * so without this field the gauge would read {@code NaN} after the first GC. {@code null} until
   * {@link #registerInFlightCommandsGauge} is called.
   */
  private volatile java.util.function.LongSupplier inFlightCommandsSupplier;

  private final Counter eventsAppended;
  private final Counter eventsReplayed;
  private final Timer eventAppendDuration;

  private final Counter projectionsProcessed;
  private final Counter projectionsFailed;
  private final Counter projectionsDeadLettered;
  private final Timer projectionDuration;

  private final Counter subscriptionEventsReceived;
  private final Timer subscriptionDeliveryLatency;

  private final Counter snapshotsCreated;
  private final Counter snapshotsLoaded;
  private final Counter cryptoSubjectRedacted;
  private final Counter cryptoKeystoreSystemicFailure;

  /**
   * Backing value for the {@code outbox.pending} gauge, updated in place on every {@link
   * #recordOutboxBacklog(long)} report (gauge semantics, last write wins). Registered eagerly in
   * the constructor so the meter is discoverable even before the first poll cycle samples it.
   */
  private final AtomicLong outboxPending = new AtomicLong();

  /**
   * Backing value for the {@code outbox.in_flight} gauge — the claimed-but-unresolved backlog a
   * post-hand-off delivery outage parks while {@code outbox.pending} reads ~0. Registered eagerly
   * alongside {@code outbox.pending}, same last-write-wins gauge semantics.
   */
  private final AtomicLong outboxInFlight = new AtomicLong();

  /**
   * Backing values for the two blockage gauges: distinct aggregates with an unresolved FAILED
   * entry, and the age of the oldest one. Updated in place on each OutboxPoller sample (gauge
   * semantics, last write wins); registered eagerly like outboxPending so the alert series exists
   * before the first poll.
   */
  private final AtomicLong outboxBlockedAggregates = new AtomicLong();

  private final AtomicLong outboxBlockageAgeSeconds = new AtomicLong();

  /**
   * Backing values for the command-DLQ depth and the two relay/runner degradation gauges , updated
   * in place on each sample (gauge semantics, last write wins). Registered eagerly so the meters
   * are discoverable before the first poll cycle samples them.
   */
  private final AtomicLong dlqPending = new AtomicLong();

  /**
   * Projection dead-letter backlog: un-replayed dead-letter entries across all projections, sampled
   * once per cycle by the projection runners and updated in place (gauge semantics, last write
   * wins). Registered eagerly, mirroring {@link #dlqPending}.
   */
  private final AtomicLong projectionsDeadLetterBacklog = new AtomicLong();

  private final AtomicLong outboxRelayConsecutiveFailures = new AtomicLong();
  private final AtomicLong dlqRetryConsecutiveFailures = new AtomicLong();

  /**
   * Stranded-saga backlog: dead-letter entries whose owning saga row is still {@code FAULTED}.
   * Fleet-wide (untagged) because the retention sweeper that samples it prunes the dead-letter
   * table as a whole and has no saga type in scope. Registered eagerly so the meter is discoverable
   * before the first sweep cycle, and updated in place afterwards.
   */
  private final AtomicLong sagaFaultedBacklog = new AtomicLong();

  private final Counter inboxSweptRows;
  private final Counter sagaDeadLettersSwept;
  private final Counter outboxSweptRows;
  private final Counter outboxDeliveryFailed;
  private final Counter outboxInFlightHorizonViolation;
  private final Counter outboxSkipped;
  private final Counter outboxSkippedSwept;
  private final Counter outboxReplayed;
  private final Counter dlqSweptRows;

  private MicrometerStreamRuneMetrics(MeterRegistry registry, String prefix) {
    this.registry = registry;
    this.prefix = prefix;
    // Tagged with the UNKNOWN sentinel, not bare — recordCommandDispatched(String)
    // registers command.type-tagged series under this same name, and a Prometheus family cannot
    // hold both shapes (the bare one would win the scrape and the tagged samples would vanish).
    this.commandsDispatched =
        Counter.builder(meter(MetricNames.COMMANDS_DISPATCHED))
            .tag(MetricNames.TAG_COMMAND_TYPE, UNKNOWN_TAG_VALUE)
            .description("Total commands dispatched")
            .register(registry);
    this.commandsSucceeded =
        Counter.builder(meter(MetricNames.COMMANDS_SUCCEEDED))
            .description("Total commands succeeded")
            .register(registry);
    this.commandsFailed =
        Counter.builder(meter(MetricNames.COMMANDS_FAILED))
            .description("Total commands failed")
            .register(registry);
    this.commandDuration =
        Timer.builder(meter(MetricNames.COMMANDS_DURATION))
            .description("Command execution duration")
            .register(registry);
    // The command-type family — commands.retried, commands.short.circuited, dlq.published
    // (and dlq.exhausted, registered lazily per type) — carries command.type on EVERY series. These
    // three used to be registered BARE here while their record methods discarded the commandType
    // the bus hands them (only recordDeadLetterExhausted tagged), so the per-type
    // published↔exhausted join that DeadLetterRetryRunner and production.md describe was
    // impossible: the published side had no command.type label at all. Same shape rule as the
    // queries.* block below: eagerly registered under the UNKNOWN sentinel so the family is
    // discoverable from boot (rate() and its alert are evaluable before the first retry or
    // dead-letter) in its REAL tagged shape — never bare, which on a Prometheus registry would win
    // the family and drop every typed sample. No field is kept: every real sample goes through the
    // tagged registration in recordCommandRetried / recordCommandShortCircuited /
    // recordDeadLetterPublished, and Counter is registry-backed, so no strong reference is needed.
    Counter.builder(meter(MetricNames.COMMANDS_RETRIED))
        .tag(MetricNames.TAG_COMMAND_TYPE, UNKNOWN_TAG_VALUE)
        .description("Command retry attempts (optimistic-lock conflicts, lock failures)")
        .register(registry);
    Counter.builder(meter(MetricNames.COMMANDS_SHORT_CIRCUITED))
        .tag(MetricNames.TAG_COMMAND_TYPE, UNKNOWN_TAG_VALUE)
        .description("Commands short-circuited by an interceptor (not executed)")
        .register(registry);
    Counter.builder(meter(MetricNames.DLQ_PUBLISHED))
        .tag(MetricNames.TAG_COMMAND_TYPE, UNKNOWN_TAG_VALUE)
        .description("Commands published to the dead-letter queue")
        .register(registry);
    this.locksWait =
        Timer.builder(meter(MetricNames.LOCKS_WAIT))
            .description("Time spent waiting to acquire an aggregate lock")
            .register(registry);
    // Untagged — this is a bus-wide admission-control signal, not per-command-type.
    this.commandsAsyncRejected =
        Counter.builder(meter(MetricNames.COMMANDS_ASYNC_REJECTED))
            .description("Async command submissions refused admission (in-flight budget exhausted)")
            .register(registry);

    // See commandsDispatched above — recordEventAppended(String) tags this same name
    // with event.type, so the no-arg path reports the UNKNOWN sentinel rather than a bare shape.
    this.eventsAppended =
        Counter.builder(meter(MetricNames.EVENTS_APPENDED))
            .tag(MetricNames.TAG_EVENT_TYPE, UNKNOWN_TAG_VALUE)
            .description("Total events appended")
            .register(registry);
    this.eventsReplayed =
        Counter.builder(meter(MetricNames.EVENTS_REPLAYED))
            .description("Total events folded into aggregate state during reconstruction")
            .register(registry);
    this.eventAppendDuration =
        Timer.builder(meter(MetricNames.EVENTS_DURATION))
            .description("Event append duration")
            .register(registry);

    // All four projection meters carry projection.name. The three
    // projection runners emit the ProjectionName-tagged series under exactly these names, so a bare
    // registration here would claim the Prometheus family first and silently swallow every tagged
    // sample the runners produce — the flatline this sentinel exists to prevent. The no-arg SPI
    // entry points (which no framework call site uses) report UNKNOWN.
    this.projectionsProcessed =
        Counter.builder(meter(MetricNames.PROJECTIONS_PROCESSED))
            .tag(MetricNames.TAG_PROJECTION_NAME, UNKNOWN_TAG_VALUE)
            .description("Total events processed by projections")
            .register(registry);
    this.projectionsFailed =
        Counter.builder(meter(MetricNames.PROJECTIONS_FAILED))
            .tag(MetricNames.TAG_PROJECTION_NAME, UNKNOWN_TAG_VALUE)
            .description("Total projection failures")
            .register(registry);
    this.projectionsDeadLettered =
        Counter.builder(meter(MetricNames.PROJECTIONS_DEAD_LETTERED))
            .tag(MetricNames.TAG_PROJECTION_NAME, UNKNOWN_TAG_VALUE)
            .description("Projection batches dead-lettered (read model advanced past the failure)")
            .register(registry);
    this.projectionDuration =
        Timer.builder(meter(MetricNames.PROJECTIONS_DURATION))
            .tag(MetricNames.TAG_PROJECTION_NAME, UNKNOWN_TAG_VALUE)
            .description("Projection processing duration")
            .register(registry);

    this.subscriptionEventsReceived =
        Counter.builder(meter(MetricNames.SUBSCRIPTIONS_EVENTS_RECEIVED))
            .description("Total events received by subscriptions")
            .register(registry);
    this.subscriptionDeliveryLatency =
        Timer.builder(meter(MetricNames.SUBSCRIPTIONS_DELIVERY_LATENCY))
            .description("Event delivery latency")
            .register(registry);

    this.snapshotsCreated =
        Counter.builder(meter(MetricNames.SNAPSHOTS_CREATED))
            .description("Total snapshots created")
            .register(registry);
    this.snapshotsLoaded =
        Counter.builder(meter(MetricNames.SNAPSHOTS_LOADED))
            .description("Total snapshots loaded during aggregate reconstruction")
            .register(registry);
    this.cryptoSubjectRedacted =
        Counter.builder(meter(MetricNames.CRYPTO_SUBJECT_REDACTED))
            .description(
                "Decrypts that fell back to [REDACTED] (crypto-shred or systemic key-store failure)")
            .register(registry);
    this.cryptoKeystoreSystemicFailure =
        Counter.builder(meter(MetricNames.CRYPTO_KEYSTORE_SYSTEMIC_FAILURE))
            .description(
                "Detected systemic key-store failures (mass redaction, not lawful erasure)")
            .register(registry);

    // These four query meter names are registered eagerly for discoverability (matching
    // every other meter's "registered at construction" contract), but a BARE registration would
    // clash with the query bus, which makes every sample query.type-tagged — so on a Prometheus
    // registry the bare family won the scrape and every tagged sample was dropped: the queries.*
    // series read zero forever while traffic flowed. The eager registration is kept (a
    // counter family that exists from boot is what makes rate() and its alert evaluable before the
    // first query) but registers it in the family's REAL shape, under the UNKNOWN sentinel. No
    // field is kept: every real sample goes through the tagged registration in
    // recordQueryDispatched/Duration/CacheHit/CacheMiss below, and Counter/Timer are
    // registry-backed (not WeakReference-backed like the supplier gauges), so no strong reference
    // is needed to keep these alive.
    Counter.builder(meter(MetricNames.QUERIES_DISPATCHED))
        .tag(MetricNames.TAG_QUERY_TYPE, UNKNOWN_TAG_VALUE)
        .description("Total queries dispatched")
        .register(registry);
    Timer.builder(meter(MetricNames.QUERIES_DURATION))
        .tag(MetricNames.TAG_QUERY_TYPE, UNKNOWN_TAG_VALUE)
        .description("Query handling duration")
        .register(registry);
    Counter.builder(meter(MetricNames.QUERIES_CACHE_HITS))
        .tag(MetricNames.TAG_QUERY_TYPE, UNKNOWN_TAG_VALUE)
        .description("Total query cache hits")
        .register(registry);
    Counter.builder(meter(MetricNames.QUERIES_CACHE_MISSES))
        .tag(MetricNames.TAG_QUERY_TYPE, UNKNOWN_TAG_VALUE)
        .description("Total query cache misses")
        .register(registry);

    this.inboxSweptRows =
        Counter.builder(meter(MetricNames.INBOX_SWEPT_ROWS))
            .description("Inbox rows swept by the housekeeping job")
            .register(registry);
    this.sagaDeadLettersSwept =
        Counter.builder(meter(MetricNames.SAGA_DEAD_LETTERS_SWEPT))
            .description("Saga dead-letter rows swept by the housekeeping job")
            .register(registry);
    this.outboxSweptRows =
        Counter.builder(meter(MetricNames.OUTBOX_SWEPT_ROWS))
            .description("Outbox rows swept by the housekeeping job")
            .register(registry);
    this.outboxDeliveryFailed =
        Counter.builder(meter(MetricNames.OUTBOX_DELIVERY_FAILED))
            .description("Outbox entries that reached terminal FAILED after exhausting retries")
            .register(registry);
    this.outboxInFlightHorizonViolation =
        Counter.builder(meter(MetricNames.OUTBOX_IN_FLIGHT_HORIZON_VIOLATION))
            .description(
                "OutboxPublisher contract violations: classifyFailure returned IN_FLIGHT while"
                    + " inFlightHorizon() reports the ZERO default, so the lease guard never"
                    + " validated the claim lease")
            .register(registry);
    Gauge.builder(meter(MetricNames.OUTBOX_PENDING), outboxPending, AtomicLong::doubleValue)
        .description("Current PENDING outbox backlog awaiting delivery to the broker")
        .register(registry);
    Gauge.builder(meter(MetricNames.OUTBOX_IN_FLIGHT), outboxInFlight, AtomicLong::doubleValue)
        .description(
            "Current IN_PROGRESS outbox backlog — claimed by a relay but not yet resolved (a"
                + " sustained post-hand-off outage parks entries here, not in PENDING)")
        .register(registry);
    Gauge.builder(
            meter(MetricNames.OUTBOX_BLOCKED_AGGREGATES),
            outboxBlockedAggregates,
            AtomicLong::doubleValue)
        .description(
            "Distinct aggregates with an unresolved FAILED outbox entry — blocked on a strict"
                + " channel, gapped on an availability-first one")
        .register(registry);
    Gauge.builder(
            meter(MetricNames.OUTBOX_BLOCKAGE_AGE_SECONDS),
            outboxBlockageAgeSeconds,
            AtomicLong::doubleValue)
        .description(
            "Age in seconds of the oldest unresolved FAILED outbox entry (0 when none); the"
                + " primary blockage alert series")
        .register(registry);
    this.outboxSkipped =
        Counter.builder(meter(MetricNames.OUTBOX_SKIPPED))
            .description("FAILED outbox entries an operator skipped (FAILED -> SKIPPED)")
            .register(registry);
    this.outboxSkippedSwept =
        Counter.builder(meter(MetricNames.OUTBOX_SKIPPED_SWEPT))
            .description("SKIPPED outbox rows swept by the retention job")
            .register(registry);
    this.outboxReplayed =
        Counter.builder(meter(MetricNames.OUTBOX_REPLAYED))
            .description("FAILED outbox entries reset to PENDING by the operator replayer")
            .register(registry);
    this.dlqSweptRows =
        Counter.builder(meter(MetricNames.DLQ_SWEPT_ROWS))
            .description("Command dead-letter-queue rows swept by the housekeeping job")
            .register(registry);
    Gauge.builder(meter(MetricNames.DLQ_PENDING), dlqPending, AtomicLong::doubleValue)
        .description(
            "Current command dead-letter-queue depth (entries queued for retry/inspection)")
        .register(registry);
    Gauge.builder(
            meter(MetricNames.PROJECTIONS_DEAD_LETTER_BACKLOG),
            projectionsDeadLetterBacklog,
            AtomicLong::doubleValue)
        .description(
            "Current projection dead-letter backlog — un-replayed dead-letter entries (permanent"
                + " read-model holes) across all projections")
        .register(registry);
    Gauge.builder(
            meter(MetricNames.OUTBOX_RELAY_DEGRADED),
            outboxRelayConsecutiveFailures,
            AtomicLong::doubleValue)
        .description("Outbox relay consecutive poll-cycle failures (0 = healthy, >0 = degraded)")
        .register(registry);
    Gauge.builder(
            meter(MetricNames.DLQ_RETRY_DEGRADED),
            dlqRetryConsecutiveFailures,
            AtomicLong::doubleValue)
        .description("Command DLQ retry runner consecutive poll-cycle failures (0 = healthy)")
        .register(registry);
    Gauge.builder(
            meter(MetricNames.SAGA_FAULTED_BACKLOG), sagaFaultedBacklog, AtomicLong::doubleValue)
        .description(
            "Saga dead-letter entries retention protects (saga row FAULTED or shielded, saga row"
                + " absent, or a null-saga entry resolved to a target) — awaiting an operator"
                + " replay or discard")
        .register(registry);
  }

  /**
   * Rebases a canonical {@link MetricNames} constant onto this collector's configured {@link
   * Builder#prefix(String) prefix} — the ONLY way meter names are constructed here.
   *
   * <p>Names used to be assembled by string concatenation ({@code prefix + ".events.append
   * .duration"}), so nothing tied an emitted meter to the constant operators build dashboards and
   * PromQL alerts from: {@code MetricNames.EVENTS_DURATION} documented {@code
   * streamrune.events.duration} while the timer registered {@code streamrune.events.append
   * .duration}, and the documented series simply never existed — invisible to the compiler and to
   * the test suite, which pinned the emitted name. Routing every registration through this method
   * makes that class of drift impossible by construction: the constant IS the name, and a rename
   * moves both sides at once.
   *
   * <p>Package-private rather than private so the drift-guard test can exercise the rebasing and
   * the rejection directly; it is not part of the public API.
   *
   * @param canonicalName a {@link MetricNames} constant (always {@value #DEFAULT_PREFIX}-prefixed)
   * @return the same name with {@value #DEFAULT_PREFIX} replaced by the configured prefix
   * @throws IllegalArgumentException if the name is not a canonical {@code MetricNames} value — a
   *     programming error, caught eagerly at construction rather than emitting a malformed series
   */
  String meter(String canonicalName) {
    if (canonicalName == null || !canonicalName.startsWith(DEFAULT_PREFIX + ".")) {
      throw new IllegalArgumentException(
          "Meter names must be derived from a MetricNames constant (prefixed with '"
              + DEFAULT_PREFIX
              + ".'), got: "
              + canonicalName);
    }
    return prefix + canonicalName.substring(DEFAULT_PREFIX.length());
  }

  @Override
  public void recordCommandDispatched() {
    commandsDispatched.increment();
  }

  @Override
  public void recordCommandSucceeded() {
    commandsSucceeded.increment();
  }

  @Override
  public void recordCommandFailed() {
    commandsFailed.increment();
  }

  @Override
  public void recordCommandDuration(long durationNanos) {
    commandDuration.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  // The four command-type-tagged counters below share one shape (command.type on every
  // series) so a per-type join across them is possible — in particular dlq.published↔dlq.exhausted
  // (DeadLetterRetryRunner). The commandType the bus passes is Class#getSimpleName(); a null
  // or blank value (a custom bus/interceptor calling the SPI without one) lands in the UNKNOWN
  // sentinel series rather than throwing inside Micrometer (Tag.of rejects null) or opening a
  // second, tag-less shape of the family.

  @Override
  public void recordCommandRetried(String commandType) {
    Counter.builder(meter(MetricNames.COMMANDS_RETRIED))
        .tag(MetricNames.TAG_COMMAND_TYPE, commandTypeTag(commandType))
        .description("Command retry attempts (optimistic-lock conflicts, lock failures)")
        .register(registry)
        .increment();
  }

  @Override
  public void recordCommandShortCircuited(String commandType) {
    Counter.builder(meter(MetricNames.COMMANDS_SHORT_CIRCUITED))
        .tag(MetricNames.TAG_COMMAND_TYPE, commandTypeTag(commandType))
        .description("Commands short-circuited by an interceptor (not executed)")
        .register(registry)
        .increment();
  }

  @Override
  public void recordCommandAsyncRejected() {
    commandsAsyncRejected.increment();
  }

  @Override
  public void registerInFlightCommandsGauge(java.util.function.LongSupplier inFlightSupplier) {
    // Idempotent and retains the strong reference — see the inFlightCommandsSupplier field
    // javadoc for why a bare Gauge.builder(name, valueFn) lambda would be collected on the first
    // GC and freeze the gauge at NaN.
    if (inFlightCommandsSupplier != null) {
      return;
    }
    inFlightCommandsSupplier = inFlightSupplier;
    Gauge.builder(
            meter(MetricNames.COMMANDS_IN_FLIGHT),
            inFlightSupplier,
            java.util.function.LongSupplier::getAsLong)
        .strongReference(true)
        .description("Current in-flight command count (admitted, not yet completed)")
        .register(registry);
  }

  @Override
  public void recordDeadLetterPublished(String commandType) {
    Counter.builder(meter(MetricNames.DLQ_PUBLISHED))
        .tag(MetricNames.TAG_COMMAND_TYPE, commandTypeTag(commandType))
        .description("Commands published to the dead-letter queue")
        .register(registry)
        .increment();
  }

  @Override
  public void recordDeadLetterExhausted(String commandType) {
    Counter.builder(meter(MetricNames.DLQ_EXHAUSTED))
        .tag(MetricNames.TAG_COMMAND_TYPE, commandTypeTag(commandType))
        .description(
            "Commands that exhausted every DLQ retry; the retry runner stops retrying them (their"
                + " events were never produced)")
        .register(registry)
        .increment();
  }

  /**
   * The {@code command.type} tag value for the command-type family: the simple class name the bus
   * passes, or {@link #UNKNOWN_TAG_VALUE} when the caller supplied none.
   */
  private static String commandTypeTag(String commandType) {
    return commandType == null || commandType.isBlank() ? UNKNOWN_TAG_VALUE : commandType;
  }

  @Override
  public void recordLockWait(long durationNanos) {
    locksWait.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordCommandDispatched(String commandType) {
    // Emit ONLY the command.type series for this call — do NOT also bump the no-arg
    // sibling. Micrometer dedups by (name, tags), so the two are DISTINCT series that both survive
    // an unqualified sum(); calling both here made every dispatch through this SPI entry point
    // count twice. Framework call sites (VirtualThreadCommandBus) use the no-arg method
    // exclusively, so production emission is unaffected — this SPI overload is reachable only
    // through a custom bus/interceptor. The no-arg sibling now reports
    // command.type=UNKNOWN_TAG_VALUE rather than a bare series, so both shapes belong to one
    // Prometheus family and neither is dropped from the scrape.
    Counter.builder(meter(MetricNames.COMMANDS_DISPATCHED))
        .tag(MetricNames.TAG_COMMAND_TYPE, commandType)
        .description("Total commands dispatched")
        .register(registry)
        .increment();
  }

  @Override
  public void recordEventAppended() {
    eventsAppended.increment();
  }

  @Override
  public void recordEventAppended(String eventType) {
    // Same fix as recordCommandDispatched(String) above — this series only, no rollup
    // call, so sum(streamrune_events_appended_total) is not doubled. The no-arg sibling
    // reports event.type=UNKNOWN_TAG_VALUE, so both shapes are one Prometheus family.
    Counter.builder(meter(MetricNames.EVENTS_APPENDED))
        .tag(MetricNames.TAG_EVENT_TYPE, eventType)
        .description("Total events appended")
        .register(registry)
        .increment();
  }

  @Override
  public void recordEventReplayed() {
    eventsReplayed.increment();
  }

  @Override
  public void recordEventAppendDuration(long durationNanos) {
    eventAppendDuration.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordProjectionProcessed() {
    projectionsProcessed.increment();
  }

  @Override
  public void recordProjectionFailed() {
    projectionsFailed.increment();
  }

  @Override
  public void recordProjectionDeadLettered() {
    projectionsDeadLettered.increment();
  }

  @Override
  public void recordProjectionDuration(long durationNanos) {
    projectionDuration.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  // ProjectionName-tagged overrides. Without these, the StreamRuneMetrics interface
  // DEFAULTS (StreamRuneMetrics.java) delegate to the untagged no-arg methods above, so
  // ContinuousProjectionRunner's per-projection calls silently collapsed into one fleet-wide
  // series and "which projection is failing" was unanswerable. This series only:
  // do NOT also call the no-arg sibling, or the two series double-count under sum().
  // The no-arg siblings report projection.name=UNKNOWN_TAG_VALUE, so these tagged
  // series and theirs are one Prometheus family and the runners' samples actually reach a scrape.

  @Override
  public void recordProjectionProcessed(ProjectionName projectionName) {
    Counter.builder(meter(MetricNames.PROJECTIONS_PROCESSED))
        .tag(MetricNames.TAG_PROJECTION_NAME, projectionName.value())
        .description("Total events processed by projections")
        .register(registry)
        .increment();
  }

  @Override
  public void recordProjectionFailed(ProjectionName projectionName) {
    Counter.builder(meter(MetricNames.PROJECTIONS_FAILED))
        .tag(MetricNames.TAG_PROJECTION_NAME, projectionName.value())
        .description("Total projection failures")
        .register(registry)
        .increment();
  }

  @Override
  public void recordProjectionDeadLettered(ProjectionName projectionName) {
    Counter.builder(meter(MetricNames.PROJECTIONS_DEAD_LETTERED))
        .tag(MetricNames.TAG_PROJECTION_NAME, projectionName.value())
        .description("Projection batches dead-lettered (read model advanced past the failure)")
        .register(registry)
        .increment();
  }

  @Override
  public void recordProjectionDuration(ProjectionName projectionName, long durationNanos) {
    Timer.builder(meter(MetricNames.PROJECTIONS_DURATION))
        .tag(MetricNames.TAG_PROJECTION_NAME, projectionName.value())
        .description("Projection processing duration")
        .register(registry)
        .record(durationNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordSubscriptionEventReceived() {
    subscriptionEventsReceived.increment();
  }

  @Override
  public void recordSubscriptionDeliveryLatency(long durationNanos) {
    subscriptionDeliveryLatency.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordSnapshotCreated() {
    snapshotsCreated.increment();
  }

  @Override
  public void recordSnapshotLoaded() {
    snapshotsLoaded.increment();
  }

  @Override
  public void recordSnapshotDiscarded(String reason) {
    Counter.builder(meter(MetricNames.SNAPSHOTS_DISCARDED))
        .tag(MetricNames.TAG_REASON, reason)
        .description(
            "Stored snapshots discarded on load (undeserializable payload or failed snapshot"
                + " migration); the stream is replayed from the beginning")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSubjectRedacted() {
    cryptoSubjectRedacted.increment();
  }

  @Override
  public void recordSystemicKeyStoreFailure() {
    cryptoKeystoreSystemicFailure.increment();
  }

  @Override
  public void recordGdprPurgeFailed(String purgerName) {
    Counter.builder(meter(MetricNames.GDPR_PURGE_FAILED))
        .tag(MetricNames.TAG_PURGER, purgerName)
        .description("GDPR erasure read-model purges that failed after the key was crypto-shredded")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSubscriptionLag(SubscriptionName subscriptionName, long lag) {
    // A live supplier gauge (registerSubscriptionLagGauge) is the source of truth once registered;
    // this push variant must not register a competing meter with the same name+tag. Skip the push
    // for names already backed by a supplier gauge — the supplier already reflects current lag.
    if (supplierBackedLagGauges.containsKey(subscriptionName)) {
      return;
    }
    subscriptionLagValues
        .computeIfAbsent(
            subscriptionName,
            name -> {
              AtomicLong holder = new AtomicLong();
              Gauge.builder(meter(MetricNames.SUBSCRIPTIONS_LAG), holder, AtomicLong::doubleValue)
                  .tag(MetricNames.TAG_SUBSCRIPTION_NAME, name.value())
                  .description("Subscription processing lag (events behind the global head)")
                  .register(registry);
              return holder;
            })
        .set(lag);
  }

  @Override
  public void registerSubscriptionLagGauge(
      SubscriptionName subscriptionName, java.util.function.LongSupplier lagSupplier) {
    // A supplier-backed gauge recomputes lag on every scrape, so the metrics endpoint
    // always reads current lag independent of any health-endpoint scrape. Register once per name.
    // putIfAbsent retains the supplier in a long-lived field (keeps it strongly reachable so
    // it is not GC-collected — see supplierBackedLagGauges) AND makes registration idempotent.
    if (supplierBackedLagGauges.putIfAbsent(subscriptionName, lagSupplier) == null) {
      // A health scrape may have already pushed a lag value via recordSubscriptionLag,
      // registering an AtomicLong-backed push gauge under this same subscriptions.lag{name} id.
      // Micrometer dedups meters by id, so registering the supplier gauge below would just return
      // that stale push gauge and silently drop the supplier — and the push path then disables
      // itself (the containsKey guard above is now true), freezing the gauge at the last pushed
      // value. Remove any pre-existing meter with this id first so the supplier gauge becomes the
      // live source of truth (and drop the orphaned push holder).
      Gauge stalePushGauge =
          registry
              .find(meter(MetricNames.SUBSCRIPTIONS_LAG))
              .tag(MetricNames.TAG_SUBSCRIPTION_NAME, subscriptionName.value())
              .gauge();
      if (stalePushGauge != null) {
        registry.remove(stalePushGauge);
        subscriptionLagValues.remove(subscriptionName);
      }
      Gauge.builder(
              meter(MetricNames.SUBSCRIPTIONS_LAG),
              lagSupplier,
              java.util.function.LongSupplier::getAsLong)
          // Do not let Micrometer hold the supplier via a WeakReference (its default): a GC would
          // otherwise clear it and the gauge would read NaN for the rest of the process.
          .strongReference(true)
          .tag(MetricNames.TAG_SUBSCRIPTION_NAME, subscriptionName.value())
          .description("Subscription processing lag (events behind the global head)")
          .register(registry);
    }
  }

  @Override
  public void recordListenerReconnect(String channel) {
    Counter.builder(meter(MetricNames.SUBSCRIPTIONS_LISTENER_RECONNECTS))
        .tag(MetricNames.TAG_CHANNEL, channel)
        .description("LISTEN/NOTIFY listener reconnect attempts")
        .register(registry)
        .increment();
  }

  // queryType is the SOLE StreamRuneMetrics entry point for these four (there is no
  // no-arg sibling to fall back to, unlike commands/events/projections), so the queryType
  // parameter must actually reach the registered meter. It used to be silently discarded — every
  // call bumped the plain eagerly-registered counter/timer, and "which query is slow" or "which
  // query's cache is thrashing" was unanswerable. The eager registrations in the constructor stay
  // (so the meter names are discoverable from boot, matching every other meter in this class) but
  // are not incremented here, so sum() over the tagged series is not doubled —
  // and they carry the same query.type key, so they and these samples are one family.

  @Override
  public void recordQueryDispatched(String queryType) {
    Counter.builder(meter(MetricNames.QUERIES_DISPATCHED))
        .tag(MetricNames.TAG_QUERY_TYPE, queryType)
        .description("Total queries dispatched")
        .register(registry)
        .increment();
  }

  @Override
  public void recordQueryDuration(String queryType, long durationNanos) {
    Timer.builder(meter(MetricNames.QUERIES_DURATION))
        .tag(MetricNames.TAG_QUERY_TYPE, queryType)
        .description("Query handling duration")
        .register(registry)
        .record(durationNanos, TimeUnit.NANOSECONDS);
  }

  @Override
  public void recordQueryCacheHit(String queryType) {
    Counter.builder(meter(MetricNames.QUERIES_CACHE_HITS))
        .tag(MetricNames.TAG_QUERY_TYPE, queryType)
        .description("Total query cache hits")
        .register(registry)
        .increment();
  }

  @Override
  public void recordQueryCacheMiss(String queryType) {
    Counter.builder(meter(MetricNames.QUERIES_CACHE_MISSES))
        .tag(MetricNames.TAG_QUERY_TYPE, queryType)
        .description("Total query cache misses")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaQuarantined(String sagaType) {
    Counter.builder(meter(MetricNames.SAGA_QUARANTINED))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .description(
            "Saga dead-letter entries written (poison events quarantined and correlated events"
                + " held), one per write, so a still-poison replay counts again")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaFaulted(String sagaType) {
    Counter.builder(meter(MetricNames.SAGA_FAULTED))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .description("Sagas that transitioned into FAULTED (one per transition, from any path)")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaSkippedWhileFaulted(String sagaType) {
    Counter.builder(meter(MetricNames.SAGA_SKIPPED_WHILE_FAULTED))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .description(
            "Correlated events delivered to an already-FAULTED saga and recorded instead of"
                + " processed (a subset of saga.quarantined)")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaEventHeld(String sagaType, String reason) {
    Counter.builder(meter(MetricNames.SAGA_EVENT_HELD))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .tag(MetricNames.TAG_HOLD_REASON, reason)
        .description(
            "Correlated events held (recorded, not processed) because their saga cannot consume"
                + " them yet (a subset of saga.quarantined)")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaReplayDeferred(String sagaType) {
    Counter.builder(meter(MetricNames.SAGA_REPLAY_DEFERRED))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .description(
            "Saga dead-letter entries the replayer deferred without feeding them (saga row absent"
                + " or genesis not yet applied)")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaResumeFaulted(String sagaType, String outcome) {
    Counter.builder(meter(MetricNames.SAGA_RESUME_FAULTED))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .tag(MetricNames.TAG_OUTCOME, outcome)
        .description(
            "Operator resumes of give-up-faulted saga compensation episodes"
                + " (SagaDeadLetterReplayer.resumeFaulted), one per call, refusals included")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaCompensateFaulted(String sagaType, String outcome) {
    Counter.builder(meter(MetricNames.SAGA_COMPENSATE_FAULTED))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .tag(MetricNames.TAG_OUTCOME, outcome)
        .description(
            "Operator compensations of forward-faulted sagas with no dead-letter entry left"
                + " (SagaDeadLetterReplayer.compensateFaulted), one per call, refusals included")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaForcedStaleResume(String sagaType) {
    Counter.builder(meter(MetricNames.SAGA_FORCED_STALE_RESUME))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .description(
            "Stale compensation episodes an operator forced to resume past the executor's key-age"
                + " guard (at-least-once acknowledged; compensations may have re-executed)")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaCompensation(String sagaType, String outcome) {
    Counter.builder(meter(MetricNames.SAGA_COMPENSATION))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .tag(MetricNames.TAG_OUTCOME, outcome)
        .description("Saga compensation attempts")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaCompensationRetry(String sagaType) {
    Counter.builder(meter(MetricNames.SAGA_COMPENSATION_RETRIES))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .description(
            "Compensation episodes re-driven out of band by the compensation-retry sweeper")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaCasConflict(String sagaType) {
    Counter.builder(meter(MetricNames.SAGA_CAS_CONFLICTS))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .description("Optimistic-concurrency (CAS) conflicts while persisting saga state")
        .register(registry)
        .increment();
  }

  @Override
  public void recordSagaCompensatingBacklog(String sagaType, long count) {
    sagaCompensatingBacklog
        .computeIfAbsent(
            sagaType,
            type -> {
              AtomicLong holder = new AtomicLong();
              Gauge.builder(meter(MetricNames.SAGA_COMPENSATING), holder, AtomicLong::doubleValue)
                  .tag(MetricNames.TAG_SAGA_TYPE, type)
                  .description("Current COMPENSATING saga backlog (compensation not yet completed)")
                  .register(registry);
              return holder;
            })
        .set(count);
  }

  @Override
  public void recordSagaTimedOutBacklog(String sagaType, long count) {
    sagaTimedOutBacklog
        .computeIfAbsent(
            sagaType,
            type -> {
              AtomicLong holder = new AtomicLong();
              Gauge.builder(meter(MetricNames.SAGA_TIMED_OUT), holder, AtomicLong::doubleValue)
                  .tag(MetricNames.TAG_SAGA_TYPE, type)
                  .description("Timed-out sagas picked up in the most recent timeout poll cycle")
                  .register(registry);
              return holder;
            })
        .set(count);
  }

  @Override
  public void recordSagaFaultedRows(String sagaType, long count) {
    sagaFaultedRowsBacklog
        .computeIfAbsent(
            sagaType,
            type -> {
              AtomicLong holder = new AtomicLong();
              Gauge.builder(meter(MetricNames.SAGA_FAULTED_ROWS), holder, AtomicLong::doubleValue)
                  .tag(MetricNames.TAG_SAGA_TYPE, type)
                  .description(
                      "Current FAULTED saga_state rows (halted, awaiting an operator) —"
                          + " includes entry-less automatic quarantines the dead-letter-backed"
                          + " faulted_backlog gauge cannot see")
                  .register(registry);
              return holder;
            })
        .set(count);
  }

  @Override
  public void recordSagaFaultedBacklog(long count) {
    sagaFaultedBacklog.set(count);
  }

  @Override
  public void recordSagaReplayed(String sagaType, String outcome) {
    Counter.builder(meter(MetricNames.SAGA_REPLAYED))
        .tag(MetricNames.TAG_SAGA_TYPE, sagaType)
        .tag(MetricNames.TAG_OUTCOME, outcome)
        .description(
            "Saga dead-letter replay outcomes: one per replay call or drained entry, except"
                + " deferrals (counted in saga.replay_deferred)")
        .register(registry)
        .increment();
  }

  @Override
  public void recordInboxReplayHit(String commandType) {
    Counter.builder(meter(MetricNames.INBOX_REPLAY_HITS))
        .tag(MetricNames.TAG_COMMAND_TYPE, commandType)
        .description("Commands recognized as replays via the inbox and not re-executed")
        .register(registry)
        .increment();
  }

  @Override
  public void recordInboxSwept(long rows) {
    inboxSweptRows.increment(rows);
  }

  @Override
  public void recordSagaDeadLetterSwept(long rows) {
    sagaDeadLettersSwept.increment(rows);
  }

  @Override
  public void recordOutboxSwept(long rows) {
    outboxSweptRows.increment(rows);
  }

  @Override
  public void recordOutboxDeliveryFailed() {
    outboxDeliveryFailed.increment();
  }

  @Override
  public void recordOutboxInFlightHorizonViolation() {
    outboxInFlightHorizonViolation.increment();
  }

  @Override
  public void recordOutboxBacklog(long pending) {
    outboxPending.set(pending);
  }

  @Override
  public void recordOutboxInFlight(long inFlight) {
    outboxInFlight.set(inFlight);
  }

  @Override
  public void recordDlqBacklog(long pending) {
    dlqPending.set(pending);
  }

  @Override
  public void recordProjectionDeadLetterBacklog(long count) {
    projectionsDeadLetterBacklog.set(count);
  }

  @Override
  public void recordProjectionDeliveryMode(
      ProjectionName projectionName, ProjectionDeliveryMode mode) {
    // An info gauge, value 1, one series per (projection.name, delivery.mode). Micrometer returns
    // the existing meter for an identical id, so a second registration is a no-op; the strong
    // reference keeps the constant supplier alive for the registry's lifetime.
    Gauge.builder(meter(MetricNames.PROJECTIONS_DELIVERY_MODE), () -> 1.0)
        .tag(MetricNames.TAG_PROJECTION_NAME, projectionName.value())
        .tag(MetricNames.TAG_DELIVERY_MODE, mode.name())
        .description("Declared delivery mode of a projection registration (value 1)")
        .strongReference(true)
        .register(registry);
  }

  @Override
  public void recordOutboxRelayDegradation(long consecutiveFailures) {
    outboxRelayConsecutiveFailures.set(consecutiveFailures);
  }

  @Override
  public void recordDlqRetryDegradation(long consecutiveFailures) {
    dlqRetryConsecutiveFailures.set(consecutiveFailures);
  }

  @Override
  public void recordOutboxBlockedAggregates(long failedAggregates) {
    outboxBlockedAggregates.set(failedAggregates);
  }

  @Override
  public void recordOutboxBlockageAge(long seconds) {
    outboxBlockageAgeSeconds.set(seconds);
  }

  @Override
  public void recordOutboxSkipped() {
    outboxSkipped.increment();
  }

  @Override
  public void recordOutboxSkippedSwept(long rows) {
    outboxSkippedSwept.increment(rows);
  }

  @Override
  public void recordOutboxReplayed(long rows) {
    outboxReplayed.increment(rows);
  }

  @Override
  public void recordDeadLetterSwept(long rows) {
    dlqSweptRows.increment(rows);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private MeterRegistry registry;
    private String prefix = DEFAULT_PREFIX;

    public Builder registry(MeterRegistry registry) {
      this.registry = registry;
      return this;
    }

    /**
     * Sets the meter name prefix (default {@value #DEFAULT_PREFIX}).
     *
     * @param prefix non-blank prefix, e.g. {@code myapp.streamrune}
     * @return this builder
     */
    public Builder prefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public MicrometerStreamRuneMetrics build() {
      if (registry == null) {
        throw new IllegalArgumentException("MeterRegistry is required");
      }
      if (prefix == null || prefix.isBlank()) {
        throw new IllegalArgumentException("prefix must not be blank");
      }
      return new MicrometerStreamRuneMetrics(registry, prefix);
    }
  }
}
