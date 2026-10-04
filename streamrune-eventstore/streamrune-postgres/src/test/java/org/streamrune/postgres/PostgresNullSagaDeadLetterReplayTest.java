package org.streamrune.postgres;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventMetadata;
import org.streamrune.core.EventStoreException;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.saga.LoadedSaga;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.saga.SagaStore;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.SagaDeadLetterReplayer;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.MutableClock;
import org.streamrune.testsupport.PostgresTestImage;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/**
 * End-to-end {@link SagaDeadLetterReplayer} runs against the REAL {@link
 * PostgresSagaDeadLetterStore}, pinning the null-saga <b>duplicate-row</b> lifecycle that the
 * InMemory store cannot reproduce: PostgreSQL's NULL-distinct unique semantics make every
 * re-quarantine of a null-saga entry INSERT a fresh row instead of upserting, while {@code
 * findEntry} reads the NEWEST row.
 *
 * <p>Pre-fix, a re-quarantined row was born with {@code first_replay_started_at = NULL} and a fresh
 * {@code first_faulted_at}, so the newest row laundered a prior attempt's evidence — a plain replay
 * more than one inbox-retention window after the first attempt saw "first-EVER replay", skipped the
 * key-age guard, re-derived the same forward key, MISSED the pruned inbox, and re-executed an
 * already-committed forward command (the audited duplicate CapturePayment). The store now carries
 * the duplicate set's immutable evidence (oldest anchor, oldest first fault) onto every new
 * null-saga row, so the newest row always bears the set's true first-attempt age and the guard
 * refuses.
 */
@Testcontainers
class PostgresNullSagaDeadLetterReplayTest {

  @Container
  static final PostgreSQLContainer<?> PG =
      new PostgreSQLContainer<>(PostgresTestImage.NAME)
          .withDatabaseName("streamrune_nullsaga_replay_test");

  static PGSimpleDataSource dataSource;

  @BeforeAll
  static void initSchema() {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(PG.getJdbcUrl());
    dataSource.setUser(PG.getUsername());
    dataSource.setPassword(PG.getPassword());
    EventTypeRegistry typeRegistry =
        new EventTypeRegistry() {
          @Override
          public Class<?> resolveEventType(EventType eventType) {
            return Object.class;
          }

          @Override
          public Class<?> resolveStateType(String stateType) {
            return Object.class;
          }

          @Override
          public java.util.Collection<Class<?>> registeredTypes() {
            return List.of();
          }
        };
    new PostgresEventStoreFactory(dataSource, typeRegistry).create();
  }

  // =========================================================================
  // Minimal domain (mirrors SagaDeadLetterReplayerTest's start-poison harness)
  // =========================================================================

  /** Start event of the saga. */
  record Trigger(String orderId) implements DomainEvent {}

  /** Step-1 forward command dispatched by the saga's create path. */
  record DoWork(String orderId) implements Command {}

  record WorkDone(String orderId) implements DomainEvent {}

  record WorkState(int done) implements AggregateState {}

  static class WorkDecider implements Decider<DoWork, WorkState, DomainEvent> {
    @Override
    public WorkState initialState() {
      return new WorkState(0);
    }

    @Override
    public List<DomainEvent> decide(DoWork command, WorkState state) {
      return List.of(new WorkDone(command.orderId()));
    }

    @Override
    public WorkState evolve(WorkState state, DomainEvent event) {
      return new WorkState(state.done() + 1);
    }
  }

  record FulfillState(SagaStatus status, String orderId) implements SagaState {}

  /**
   * Start-event orchestrator whose ROUTER ({@code extractSagaId}) can be toggled poison -> healthy.
   * Router poison quarantines the start event as a NULL-saga entry (no saga id could be resolved,
   * no saga row exists); the healthy path dispatches the step-1 forward command through the
   * executor's create path.
   */
  static class RouterToggleStartOrchestrator implements SagaOrchestrator<FulfillState> {

    volatile boolean extractPoisoned = true;

    @Override
    public Class<FulfillState> stateType() {
      return FulfillState.class;
    }

    @Override
    public FulfillState initialState(SagaId sagaId) {
      return new FulfillState(SagaStatus.STARTED, null);
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return event.event() instanceof Trigger;
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      if (extractPoisoned) {
        throw new RuntimeException("simulated routing bug in extractSagaId()");
      }
      return SagaId.of("fulfill-" + ((Trigger) event.event()).orderId());
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.empty();
    }

    @Override
    public FulfillState evolve(FulfillState state, EventEnvelope event) {
      return new FulfillState(SagaStatus.RUNNING, ((Trigger) event.event()).orderId());
    }

    @Override
    public List<SagaCommand> handle(FulfillState state, EventEnvelope event) {
      return List.of(SagaCommand.of(new DoWork(state.orderId()), AggregateId.of(state.orderId())));
    }

    @Override
    public List<SagaCommand> compensate(
        FulfillState state, Throwable failure, SagaCommand failedCommand) {
      return List.of();
    }
  }

  /**
   * Delegating saga store whose genesis CAS ({@code applyEvent}) can be armed to throw once —
   * modeling a process crash after the feed's forward dispatch loop committed but before the
   * create-first genesis commits (the genesis-pending row exists, its command keys can be pruned).
   */
  static final class CreateCrashSagaStore implements SagaStore {
    private final SagaStore delegate;
    private volatile boolean failNextGenesisCommit;

    CreateCrashSagaStore(SagaStore delegate) {
      this.delegate = delegate;
    }

    /**
     * Arms a one-shot failure on the next {@link #applyEvent} call, thrown BEFORE delegating --
     * models a process crash after the feed's forward dispatch loop committed but before the
     * genesis CAS lands (the window whose command keys can be pruned). The four crash schedules
     * below run on this seam.
     */
    void failNextGenesisCommit() {
      this.failNextGenesisCommit = true;
    }

    @Override
    public void create(
        SagaId sagaId,
        org.streamrune.core.types.SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      delegate.create(sagaId, sagaType, state, status, deadLetterPending);
    }

    @Override
    public void update(
        SagaId sagaId,
        org.streamrune.core.types.SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion) {
      delegate.update(sagaId, sagaType, state, status, expectedVersion);
    }

    @Override
    public void claimCompensating(
        SagaId sagaId,
        org.streamrune.core.types.SagaType sagaType,
        SagaState state,
        long expectedVersion) {
      delegate.claimCompensating(sagaId, sagaType, state, expectedVersion);
    }

    @Override
    public void createGenesisPending(
        SagaId sagaId,
        org.streamrune.core.types.SagaType sagaType,
        SagaState state,
        SagaStatus status,
        boolean deadLetterPending) {
      delegate.createGenesisPending(sagaId, sagaType, state, status, deadLetterPending);
    }

    @Override
    public void createFaulted(
        SagaId sagaId, org.streamrune.core.types.SagaType sagaType, SagaState initialState) {
      delegate.createFaulted(sagaId, sagaType, initialState);
    }

    @Override
    public void applyEvent(
        SagaId sagaId,
        org.streamrune.core.types.SagaType sagaType,
        SagaState state,
        SagaStatus status,
        long expectedVersion,
        AppliedEvent applied) {
      if (failNextGenesisCommit) {
        failNextGenesisCommit = false;
        throw new EventStoreException(
            "simulated infra crash after the dispatch loop, before the genesis CAS");
      }
      delegate.applyEvent(sagaId, sagaType, state, status, expectedVersion, applied);
    }

    @Override
    public boolean markFaulted(
        SagaId sagaId, org.streamrune.core.types.SagaType sagaType, long expectedVersion) {
      return delegate.markFaulted(sagaId, sagaType, expectedVersion);
    }

    @Override
    public void setDeadLetterPending(
        SagaId sagaId, org.streamrune.core.types.SagaType sagaType, boolean pending) {
      delegate.setDeadLetterPending(sagaId, sagaType, pending);
    }

    @Override
    public <S extends SagaState> Optional<LoadedSaga<S>> load(
        SagaId sagaId, org.streamrune.core.types.SagaType sagaType, Class<S> stateType) {
      return delegate.load(sagaId, sagaType, stateType);
    }

    @Override
    public void delete(SagaId sagaId, org.streamrune.core.types.SagaType sagaType) {
      delegate.delete(sagaId, sagaType);
    }

    @Override
    public List<SagaId> findTimedOut(
        org.streamrune.core.types.SagaType sagaType, Instant cutoff, int limit) {
      return delegate.findTimedOut(sagaType, cutoff, limit);
    }

    @Override
    public List<SagaId> findByStatus(
        org.streamrune.core.types.SagaType sagaType, SagaStatus status, int limit) {
      return delegate.findByStatus(sagaType, status, limit);
    }

    @Override
    public long countByStatus(org.streamrune.core.types.SagaType sagaType, SagaStatus status) {
      return delegate.countByStatus(sagaType, status);
    }

    @Override
    public List<CompensatingSaga> findCompensating(
        org.streamrune.core.types.SagaType sagaType, Instant updatedBefore, int limit) {
      return delegate.findCompensating(sagaType, updatedBefore, limit);
    }
  }

  // =========================================================================
  // Per-test harness
  // =========================================================================

  InMemoryEventStore eventStore;
  PostgresSagaStore postgresSagaStore;
  CreateCrashSagaStore sagaStore;
  PostgresSagaDeadLetterStore dlq;
  InMemoryCommandInbox inbox;
  InMemoryEventStore workStore;
  RouterToggleStartOrchestrator orchestrator;
  SagaRunner<FulfillState> runner;
  MutableClock clock;

  @BeforeEach
  void setUp() throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_dead_letters");
      stmt.execute("DELETE FROM saga_state");
    }
    eventStore = new InMemoryEventStore();
    // The dead-letter store's retention guard AND its conditional
    // shield clear (clearShieldIfDrained) read and write saga_state at the DEAD-LETTER store, so
    // the
    // saga store must live on the SAME DataSource — the production pairing (all three
    // auto-configurations wire both onto the application DataSource). An in-memory saga store
    // beside a Postgres dead-letter store would keep its flag TRUE after a drained backlog.
    postgresSagaStore =
        new PostgresSagaStore(dataSource, new ObjectMapper().registerModule(new JavaTimeModule()));
    sagaStore = new CreateCrashSagaStore(postgresSagaStore);
    dlq = new PostgresSagaDeadLetterStore(dataSource);
    inbox = new InMemoryCommandInbox();
    workStore = new InMemoryEventStore().withCommandInbox(inbox);
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(workStore)
            .commandInbox(inbox)
            .register(
                TestStreams.TYPE,
                DoWork.class,
                cmd -> AggregateId.of(cmd.orderId()),
                new WorkDecider())
            .build();
    orchestrator = new RouterToggleStartOrchestrator();
    runner =
        SagaRunner.<FulfillState>builder()
            .orchestrator(orchestrator)
            .sagaStore(sagaStore)
            .commandBus(bus)
            .sagaDeadLetterStore(dlq)
            .build();
    clock = MutableClock.startingAt(Instant.parse("2026-07-01T00:00:00Z"));
  }

  private SagaDeadLetterReplayer replayer() {
    return SagaDeadLetterReplayer.builder()
        .sagaDeadLetterStore(dlq)
        .sagaStore(sagaStore)
        .eventStore(eventStore)
        .sagaRunner(runner)
        .inboxRetentionMaxAge(Duration.ofDays(7))
        .clock(clock)
        .build();
  }

  private EventEnvelope triggerEnvelope(String orderId) {
    Trigger event = new Trigger(orderId);
    var streamId = TestStreams.stream("order-stream-" + UUID.randomUUID());
    var appended =
        eventStore.append(
            streamId,
            List.of(
                new EventEnvelope(
                    GlobalOffset.initial(),
                    streamId,
                    Version.initial(),
                    EventType.fromClass(Trigger.class),
                    event,
                    new EventMetadata(
                        EventId.of("evt-" + UUID.randomUUID()),
                        CommandId.of("cmd-" + UUID.randomUUID()),
                        null,
                        null,
                        CorrelationId.of("fulfill-" + orderId),
                        null,
                        null,
                        Instant.now()))),
            Version.initial());
    return eventStore
        .readGlobalStream(GlobalOffset.of(appended.globalOffsets().get(0).value() - 1), 1)
        .get(0);
  }

  // =========================================================================
  // Tests
  // =========================================================================

  @Test
  void replay_nullSagaDuplicateRows_pastWindow_refusesInsteadOfLaunderingPrunedKeyRedrive() {
    // Repro — the exact audited three-attempt sequence, against the real Postgres store.
    // Day 0: a start event poisons in the ROUTER (extractSagaId throws) -> quarantined as a
    // NULL-saga entry, row R1. No saga row exists (markFaulted is skipped for a null saga id).
    var event = triggerEnvelope("order-nullsaga-launder");
    var sagaId = SagaId.of("fulfill-order-nullsaga-launder");
    var workStream = TestStreams.stream("order-nullsaga-launder");
    runner.asEventListener().onEvents(List.of(event));
    assertThat(dlq.findAll(10)).hasSize(1);
    assertThat(dlq.findAll(10).get(0).sagaId()).isNull();
    assertThat(sagaStore.load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class))
        .isEmpty();

    // Attempt 1 (t1): router fix ships. The replay establishes R1's immutable anchor, resolves and
    // shields the target, and the feed takes the create-first genesis path: genesis-pending row
    // (v1), forward command 0 dispatched (DoWork COMMITTED, inbox key written), then the process
    // dies before the genesis CAS lands.
    orchestrator.extractPoisoned = false;
    var replayer = replayer();
    Instant t1 = clock.instant();
    sagaStore.failNextGenesisCommit();
    assertThatThrownBy(() -> replayer.replay(null, event.globalOffset()))
        .isInstanceOf(EventStoreException.class);
    assertThat(workStore.readStream(workStream, Version.initial(), 100))
        .as("attempt 1 committed the forward command exactly once before dying")
        .hasSize(1);
    assertThat(dlq.findAll(10).get(0).firstReplayStartedAt()).isEqualTo(t1);
    var genesisPending =
        sagaStore
            .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(genesisPending.genesisApplied())
        .as("the crash landed after the dispatch loop, before the genesis CAS — create-first row")
        .isFalse();
    assertThat(genesisPending.version()).isEqualTo(1L);

    // Attempt 2 (t1+2d, within the 7d window): a router regression re-poisons PRE-dispatch. The
    // re-quarantine INSERTs a brand-new row R2 (NULL-distinct unique semantics) carrying the set's
    // immutable evidence (oldest anchor, oldest first fault).
    clock.advance(Duration.ofDays(2));
    orchestrator.extractPoisoned = true;
    assertThat(replayer.replay(null, event.globalOffset()))
        .isEqualTo(SagaDeadLetterReplayer.ReplayOutcome.STILL_POISON);
    var rowsAfterRequarantine = dlq.findAll(10);
    assertThat(rowsAfterRequarantine)
        .as("the null-saga re-quarantine accumulates a duplicate row")
        .hasSize(2);
    assertThat(workStore.readStream(workStream, Version.initial(), 100))
        .as("attempt 2 poisoned before any dispatch — still exactly one committed execution")
        .hasSize(1);

    // Attempt 3 (t1+10d, past the 7d window): the inbox sweeper pruned attempt 1's forward key;
    // the router fix re-ships; the operator runs a PLAIN (non-forced) replay. findEntry returns
    // the NEWEST row, R2 — pre-fix born with a NULL anchor, so the key-age guard was silently
    // skipped, the feed re-derived the SAME forward key, MISSED the pruned inbox, and re-executed
    // the already-committed forward command.
    clock.advance(Duration.ofDays(8));
    inbox.deleteProcessedBefore(Instant.now().plus(Duration.ofDays(365)));
    orchestrator.extractPoisoned = false;

    var outcome = replayer.replay(null, event.globalOffset());

    assertThat(workStore.readStream(workStream, Version.initial(), 100))
        .as(
            "the already-committed forward command must NOT execute a second time — the duplicate"
                + " null-saga row must not launder the prior attempt's first-replay anchor past"
                + " the key-age guard")
        .hasSize(1);
    assertThat(outcome)
        .as("a previously-attempted null-saga set past the window must be refused")
        .isEqualTo(SagaDeadLetterReplayer.ReplayOutcome.STALE_REDRIVE_BLOCKED);
    assertThat(dlq.findAll(10)).as("the refusal keeps the entry set for reconciliation").hasSize(2);
    // The refusal never moves the immutable anchor; the resolution
    // is stamped on the whole null-saga set and the target row is shielded BEFORE the refusal, so
    // the retention sweep (RESOLVED_NULL) cannot prune the set the operator was told to reconcile.
    // Pinned end-to-end, sweep included, by
    // replay_nullSagaRefusedPastWindow_retentionSweepMustNotPruneTheEntryBeingReconciled.
    assertThat(dlq.findAll(10))
        .as("the refusal stamps the resolved target on the set without moving the anchor")
        .allSatisfy(
            e -> {
              assertThat(e.targetSagaId()).isEqualTo(sagaId);
              assertThat(e.firstReplayStartedAt()).isEqualTo(t1);
            });
    var refused =
        sagaStore
            .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(refused.version()).as("the refused replay dispatches nothing").isEqualTo(1L);
    assertThat(refused.genesisApplied()).isFalse();
    assertThat(refused.deadLetterPending())
        .as("the target is shielded before the refusal")
        .isTrue();

    // Documented recovery: the operator verifies in the inbox / downstream which forward commands
    // executed, reconciles, then forces. The forced re-drive past the window re-executes the
    // pruned-key command (the operator's verified decision) and converges.
    var forced = replayer.replay(null, event.globalOffset(), true);

    assertThat(forced).isEqualTo(SagaDeadLetterReplayer.ReplayOutcome.REPLAYED);
    assertThat(workStore.readStream(workStream, Version.initial(), 100))
        .as("the forced re-drive re-executes under the pruned key — exactly once more")
        .hasSize(2);
    assertThat(dlq.findAll(10)).as("discard removes every row of the null-saga set").isEmpty();
    var converged =
        sagaStore
            .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
            .orElseThrow();
    assertThat(converged.status()).isEqualTo(SagaStatus.RUNNING);
    assertThat(converged.version()).as("genesis-pending v1 + the genesis CAS").isEqualTo(2L);
    assertThat(converged.genesisApplied()).isTrue();
    assertThat(converged.deadLetterPending()).as("backlog drained: shield cleared").isFalse();
  }

  @Test
  void replay_nullSagaRefusedPastWindow_retentionSweepMustNotPruneTheEntryBeingReconciled() {
    // Repro — the SAME audited sequence as the test above, but with the RETENTION SWEEP
    // actually running between the refusal and the operator's force (the production interleaving:
    // the sweeper's documented default interval is 1h, and no operator reconciles a duplicate
    // forward command in an hour).
    //
    // Structural claim being pinned: the first_faulted_at carry makes every row of the
    // null-saga set age from T0 (the FIRST quarantine). With the shipped defaults D == I == 7d and
    // the boot-time interlock D <= I (SagaRetentionValidator.validateDeadLetterRetention), the
    // STALE_REDRIVE refusal condition `now > anchor + I` together with `T0 <= anchor` implies
    // `now > T0 + D` — so EVERY null-saga STALE_REDRIVE refusal fires against a set that is
    // ALREADY past its retention cutoff. The refusal must therefore leave the set shielded, or the
    // very record it told the operator to reconcile is deleted out from under them and the eventual
    // force lands on ENTRY_NOT_FOUND (poison event never consumed, no surviving record,
    // countFaultedBacklog reports 0).
    //
    // The clock must be started at/after the quarantine instant: SagaRunner.quarantine stamps
    // faulted_at from Instant.now(), not from the injected clock, so a MutableClock anchored in the
    // past would put T0 AFTER the replay timeline and make the sweep vacuous.
    var event = triggerEnvelope("order-nullsaga-sweep");
    var sagaId = SagaId.of("fulfill-order-nullsaga-sweep");
    var workStream = TestStreams.stream("order-nullsaga-sweep");
    runner.asEventListener().onEvents(List.of(event)); // T0: router poison -> R1
    Instant firstFault = dlq.findAll(10).get(0).firstFaultedAt();
    assertThat(firstFault).as("the quarantine establishes the retention anchor T0").isNotNull();

    clock =
        MutableClock.startingAt(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MICROS));
    Instant t1 = clock.instant();
    assertThat(t1)
        .as("T0 <= t1 — the first replay attempt cannot precede the first fault")
        .isAfterOrEqualTo(firstFault);

    // Attempt 1 (t1): establishes the anchor, dispatches forward command 0, dies before the
    // genesis CAS.
    orchestrator.extractPoisoned = false;
    var replayer = replayer();
    sagaStore.failNextGenesisCommit();
    assertThatThrownBy(() -> replayer.replay(null, event.globalOffset()))
        .isInstanceOf(EventStoreException.class);
    assertThat(workStore.readStream(workStream, Version.initial(), 100)).hasSize(1);

    // Attempt 2 (t1+2d): re-poison -> R2 INSERTed carrying {anchor t1, first_faulted_at T0}.
    clock.advance(Duration.ofDays(2));
    orchestrator.extractPoisoned = true;
    assertThat(replayer.replay(null, event.globalOffset()))
        .isEqualTo(SagaDeadLetterReplayer.ReplayOutcome.STILL_POISON);
    assertThat(dlq.findAll(10)).hasSize(2);
    assertThat(dlq.findAll(10))
        .as("every row of the set carries the FIRST quarantine's retention anchor")
        .allSatisfy(e -> assertThat(e.firstFaultedAt()).isEqualTo(firstFault));

    // Attempt 3 (t1+10d, past the 7d window): the router is fixed, the operator runs a plain
    // replay and is told to reconcile and then force.
    clock.advance(Duration.ofDays(8));
    inbox.deleteProcessedBefore(Instant.now().plus(Duration.ofDays(365)));
    orchestrator.extractPoisoned = false;
    assertThat(replayer.replay(null, event.globalOffset()))
        .isEqualTo(SagaDeadLetterReplayer.ReplayOutcome.STALE_REDRIVE_BLOCKED);

    // The sweeper runs while the operator reconciles (default interval 1h). Cutoff = now - D with
    // the shipped default D = 7d, exactly what SagaDeadLetterRetentionSweeper passes.
    int pruned = dlq.deleteOlderThan(clock.instant().minus(Duration.ofDays(7)));

    assertThat(pruned)
        .as(
            "the retention sweep must not delete the null-saga set the STALE_REDRIVE refusal just"
                + " told the operator to reconcile — the carried first_faulted_at puts the whole"
                + " set past the cutoff by the time that refusal can fire, so the resolution the"
                + " replayer stamped BEFORE refusing (RESOLVED_NULL) is what shields it")
        .isZero();
    assertThat(dlq.findAll(10)).hasSize(2);
    assertThat(dlq.findAll(10))
        .as("the resolved target is stamped on every row of the set")
        .allSatisfy(e -> assertThat(e.targetSagaId()).isEqualTo(sagaId));
    assertThat(dlq.findAll(10))
        .as("a refusal never moves or forges the immutable first-attempt anchor")
        .allSatisfy(e -> assertThat(e.firstReplayStartedAt()).isEqualTo(t1));
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
                .orElseThrow()
                .deadLetterPending())
        .as("the genesis-pending target row is shielded")
        .isTrue();

    // The documented remedy stays reachable: reconcile, then force.
    var forced = replayer.replay(null, event.globalOffset(), true);

    assertThat(forced)
        .as("the operator's post-reconciliation force must still find the entry")
        .isEqualTo(SagaDeadLetterReplayer.ReplayOutcome.REPLAYED);
    assertThat(workStore.readStream(workStream, Version.initial(), 100)).hasSize(2);
    assertThat(dlq.findAll(10)).isEmpty();
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.RUNNING);
  }

  @Test
  void replay_nullSagaDuplicateRows_withinWindow_rerunRedrivesAndConvergesExactlyOnce() {
    // Crash-point convergence (dispatch committed, genesis CAS not landed): the rerun WITHIN
    // the inbox window must re-drive the genesis-pending row, dedup the surviving forward key,
    // commit the genesis, and discard the whole set — the guard must not over-block the legitimate
    // rerun.
    var event = triggerEnvelope("order-nullsaga-rerun");
    var sagaId = SagaId.of("fulfill-order-nullsaga-rerun");
    var workStream = TestStreams.stream("order-nullsaga-rerun");
    runner.asEventListener().onEvents(List.of(event));

    orchestrator.extractPoisoned = false;
    var replayer = replayer();
    sagaStore.failNextGenesisCommit();
    assertThatThrownBy(() -> replayer.replay(null, event.globalOffset()))
        .isInstanceOf(EventStoreException.class);
    assertThat(workStore.readStream(workStream, Version.initial(), 100)).hasSize(1);

    clock.advance(Duration.ofHours(2));
    var outcome = replayer.replay(null, event.globalOffset());

    assertThat(outcome).isEqualTo(SagaDeadLetterReplayer.ReplayOutcome.REPLAYED);
    assertThat(workStore.readStream(workStream, Version.initial(), 100))
        .as("the surviving inbox key dedups the re-driven dispatch — exactly once")
        .hasSize(1);
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(dlq.findAll(10)).isEmpty();
  }

  @Test
  void replay_nullSagaDuplicateRows_neverAttempted_firstReplayIsNotRefused() {
    // Two LIVE poison deliveries (at-least-once redelivery while the router is broken) accumulate
    // two rows with NO replay attempt ever started. The first-ever replay must not be refused —
    // there is no prior-attempt evidence to carry, however old the entry is — and must execute
    // the forward command exactly once.
    var event = triggerEnvelope("order-nullsaga-fresh");
    var sagaId = SagaId.of("fulfill-order-nullsaga-fresh");
    var workStream = TestStreams.stream("order-nullsaga-fresh");
    runner.asEventListener().onEvents(List.of(event));
    runner.asEventListener().onEvents(List.of(event)); // redelivery -> second row
    var rows = dlq.findAll(10);
    assertThat(rows).hasSize(2);
    assertThat(rows).allSatisfy(e -> assertThat(e.firstReplayStartedAt()).isNull());

    clock.advance(Duration.ofDays(30)); // far past the window — must STILL not refuse
    orchestrator.extractPoisoned = false;

    var outcome = replayer().replay(null, event.globalOffset());

    assertThat(outcome).isEqualTo(SagaDeadLetterReplayer.ReplayOutcome.REPLAYED);
    assertThat(workStore.readStream(workStream, Version.initial(), 100))
        .as("a first-ever replay writes its keys fresh — exactly once")
        .hasSize(1);
    assertThat(
            sagaStore
                .load(sagaId, SagaType.fromClass(FulfillState.class), FulfillState.class)
                .orElseThrow()
                .status())
        .isEqualTo(SagaStatus.RUNNING);
    assertThat(dlq.findAll(10)).isEmpty();
  }
}
