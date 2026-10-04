package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventTypeRegistry;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.saga.SagaCommand;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrchestrator;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.postgres.PostgresCommandInbox;
import org.streamrune.postgres.PostgresEventStore;
import org.streamrune.postgres.PostgresSagaStore;
import org.streamrune.runtime.LocalStripedLocker;
import org.streamrune.runtime.SagaCompensationRetrySweeper;
import org.streamrune.runtime.SagaRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemorySagaDeadLetterStore;

/**
 * Epoch-fenced-leases, Part B — spec B.5: the saga compensation-retry <em>sweeper</em> needs NO
 * epoch fencing because a stale/fenced-out replica's re-drive of an already-driven compensation
 * episode is deduplicated by the <b>command inbox</b> under the shared {@code
 * SagaCommandDispatch.episodeCompensationKey} — not double-applied.
 *
 * <p>This composes the whole loop on real infrastructure exactly as {@link SagaCrashResumeE2EIT}
 * does — a real {@link PostgresSagaStore}, a real {@link PostgresCommandInbox} shared by two {@link
 * VirtualThreadCommandBus}es (one per modeled replica), and a real {@link PostgresEventStore} — so
 * the dedup is proven end-to-end against PostgreSQL, not an in-memory double.
 *
 * <p><b>Scenario.</b>
 *
 * <ol>
 *   <li>A saga is seeded stuck {@code COMPENSATING} at episode version {@code V = 1} via the real
 *       store (a fresh {@code create(…, COMPENSATING)} — the state a compensation left behind when
 *       its command failed transiently, its {@code updated_at} back-dated so it is immediately due
 *       for the sweeper).
 *   <li>The <b>leader</b> replica's real {@link SagaCompensationRetrySweeper} (NOOP leadership =
 *       always leader) re-drives it: it dispatches {@code CancelOrder} under {@code
 *       saga:<id>:episode:1:comp:0} through the shared inbox → the compensation executes once (one
 *       {@code OrderCancelled} appended, the episode key recorded once) → the saga terminalizes
 *       {@code COMPENSATED}.
 *   <li>The <b>stale</b> replica (one that slipped past the lease gate) re-drives the <em>same</em>
 *       episode. Its sweep dispatches the compensation under the <em>same</em> {@code
 *       episodeCompensationKey}; the command inbox returns the prior result ({@link
 *       CommandBus.CommandResult#idempotentReplay()}), so the handler never runs a second time.
 * </ol>
 *
 * <p><b>Why the stale re-drive is modeled as a direct keyed dispatch (per the task brief).</b> The
 * point of B.5 is the <em>command-inbox</em> dedup layer. Once the leader has terminalized the saga
 * to {@code COMPENSATED}, a genuine second {@code SagaCompensationRetrySweeper.sweepOnce()} would
 * short-circuit one layer <em>earlier</em> — {@code findCompensating} no longer returns the row and
 * {@code SagaRunner.sweepCompensation} bails at the {@code status != COMPENSATING} gate — so it
 * would never reach, and therefore never exercise, the inbox. The genuine interleaving that reaches
 * the inbox is the one where the stale replica loaded the episode <em>while it was still
 * COMPENSATING at V</em> and dispatches afterwards; reproducing that as a real concurrent race
 * would be timing-dependent. So — exactly as the brief prescribes ("a stubbed/gate-bypassed second
 * sweep is the cleanest deterministic model") — the stale replica's sweep is modeled by issuing the
 * very dispatch its {@code compensateEpisode} would issue: the same compensation command, under the
 * same episode-scoped idempotency key, through a second bus over the shared inbox. That
 * deterministically pins the assertion on the inbox dedup, which is the seam B.5 is about. The
 * dedup is keyed purely on the {@link IdempotencyKey}, so the surrounding saga-owned dispatch
 * context is irrelevant to the outcome being proven.
 *
 * <p>Postgres-only — runs in the default {@code test} task.
 */
@Timeout(120)
class StaleEpochSweeperDedupIT extends E2ETestBase {

  // ---- domain: a compensation command that appends exactly one OrderCancelled and counts its own
  //      real executions through a SHARED counter, so a second (deduped) dispatch is observable
  // ----
  record OrderCancelled(String orderId) implements DomainEvent {}

  record CancelOrder(String orderId) implements Command {}

  record CancelState() implements AggregateState {}

  /**
   * Appends {@link OrderCancelled}; bumps {@code executions} once per <em>real</em> handler run.
   */
  static final class CancelDecider implements Decider<CancelOrder, CancelState, OrderCancelled> {
    private final AtomicInteger executions;

    CancelDecider(AtomicInteger executions) {
      this.executions = executions;
    }

    @Override
    public CancelState initialState() {
      return new CancelState();
    }

    @Override
    public List<OrderCancelled> decide(CancelOrder command, CancelState state) {
      executions.incrementAndGet();
      return List.of(new OrderCancelled(command.orderId()));
    }

    @Override
    public CancelState evolve(CancelState state, OrderCancelled event) {
      return state;
    }
  }

  // ---- saga: only compensate() matters here (seeded straight into COMPENSATING; no forward path,
  //      no subscription). The forward callbacks are never invoked and are minimal but valid. ----
  record OrderSagaState(SagaStatus status, String orderId) implements SagaState {
    @JsonCreator
    OrderSagaState(
        @JsonProperty("status") SagaStatus status, @JsonProperty("orderId") String orderId) {
      this.status = status;
      this.orderId = orderId;
    }

    @Override
    public SagaStatus status() {
      return status;
    }
  }

  static final class OrderSaga implements SagaOrchestrator<OrderSagaState> {
    @Override
    public Class<OrderSagaState> stateType() {
      return OrderSagaState.class;
    }

    @Override
    public OrderSagaState initialState(SagaId sagaId) {
      return new OrderSagaState(SagaStatus.STARTED, sagaId.value());
    }

    @Override
    public boolean isStartEvent(EventEnvelope event) {
      return false; // never routed here — the saga is seeded directly into COMPENSATING
    }

    @Override
    public SagaId extractSagaId(EventEnvelope event) {
      throw new UnsupportedOperationException("no forward routing in this test");
    }

    @Override
    public Optional<SagaId> correlate(EventEnvelope event) {
      return Optional.empty();
    }

    @Override
    public OrderSagaState evolve(OrderSagaState state, EventEnvelope event) {
      return state;
    }

    @Override
    public List<SagaCommand> handle(OrderSagaState state, EventEnvelope event) {
      return List.of();
    }

    @Override
    public List<SagaCommand> compensate(
        OrderSagaState state, Throwable failure, SagaCommand failedCommand) {
      // The compensation episode re-driven by BOTH replicas: one CancelOrder for this order.
      return List.of(
          SagaCommand.of(
              new CancelOrder(state.orderId()), AggregateId.of("cancel-" + state.orderId())));
    }
  }

  /** Registry resolving the compensation event for the store's payload (de)serialization. */
  private static final EventTypeRegistry SAGA_REGISTRY =
      new EventTypeRegistry() {
        @Override
        public Class<?> resolveEventType(EventType eventType) {
          if (EventType.fromClass(OrderCancelled.class).equals(eventType)) {
            return OrderCancelled.class;
          }
          throw new IllegalArgumentException("Unknown event type: " + eventType);
        }

        @Override
        public Class<?> resolveStateType(String stateType) {
          throw new IllegalArgumentException("No aggregate snapshots in this test: " + stateType);
        }
      };

  private final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

  private PostgresCommandInbox commandInbox;
  private PostgresEventStore store;
  private PostgresSagaStore sagaStore;

  @BeforeEach
  void cleanSagaTables() throws Exception {
    // E2ETestBase#cleanDatabase clears event_stream/offsets but NOT saga_state/command_inbox.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM saga_state");
      stmt.execute("DELETE FROM command_inbox");
    }
    commandInbox = new PostgresCommandInbox(dataSource);
    // ONE shared inbox + store, exactly as SagaCrashResumeE2EIT wires them.
    store =
        new PostgresEventStore(
            dataSource, mapper, SAGA_REGISTRY, null, null, null, null, commandInbox);
    sagaStore = new PostgresSagaStore(dataSource, mapper);
  }

  /**
   * A command bus over the SHARED inbox + store, with {@link CancelOrder} registered against a
   * decider that bumps {@code executions} on each real run. One per modeled replica; both dedup
   * against the same {@code command_inbox} table.
   */
  private VirtualThreadCommandBus busFor(AtomicInteger executions) {
    return VirtualThreadCommandBus.builder()
        .eventStore(store)
        .locker(new LocalStripedLocker(16))
        .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
        .objectMapper(mapper)
        .commandInbox(commandInbox)
        .register(
            AggregateType.of("cancel"),
            CancelOrder.class,
            cmd -> AggregateId.of("cancel-" + cmd.orderId()),
            new CancelDecider(executions))
        .build();
  }

  private long count(String sql, String param) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement(sql)) {
      ps.setString(1, param);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private Optional<String> sagaStatus(String sagaId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT status FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, sagaId);
      try (var rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getString(1)) : Optional.empty();
      }
    }
  }

  private long sagaVersion(String sagaId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT version FROM saga_state WHERE saga_id = ?")) {
      ps.setString(1, sagaId);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  @Test
  void staleReplicaSweepReDrive_isDedupedByCommandInbox_notDoubleApplied() throws Exception {
    String orderId = "o-" + UUID.randomUUID();
    String sagaId = "saga-" + orderId;
    SagaType sagaType = SagaType.fromClass(OrderSagaState.class);

    // The compensation side-effect counter is SHARED across both replicas' deciders: it counts
    // every
    // real CancelOrder handler run regardless of which bus dispatched it. Exactly-once == counter
    // 1.
    AtomicInteger compensationExecutions = new AtomicInteger();

    // 1. Seed a saga stuck COMPENSATING at episode version V=1 (fresh create → version 1), the row
    // a
    //    transiently-failed compensation leaves behind. Back-date updated_at so it is immediately
    //    past the sweeper's due-cutoff (robust to any container/JVM clock skew).
    sagaStore.create(
        SagaId.of(sagaId),
        sagaType,
        new OrderSagaState(SagaStatus.COMPENSATING, orderId),
        SagaStatus.COMPENSATING);
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE saga_state SET updated_at = NOW() - INTERVAL '60 seconds' "
                    + "WHERE saga_id = ?")) {
      ps.setString(1, sagaId);
      ps.executeUpdate();
    }

    // The shared, episode-scoped compensation key BOTH replicas derive for this episode — identical
    // to SagaCommandDispatch.episodeCompensationKey(CorrelationId.of(sagaId), V=1, index=0). It is
    // the seam B.5 relies on: same episode key ⇒ command-inbox dedup.
    IdempotencyKey episodeKey = IdempotencyKey.of("saga:" + sagaId + ":episode:1:comp:0");

    // 2. LEADER re-drive via the real SagaCompensationRetrySweeper (NOOP leadership = always
    // leader).
    //    A short retryInterval so the seeded (back-dated) row is swept promptly; a long giveUpAfter
    //    so the poison bound never fires. The sweeper re-drives the episode → CancelOrder
    // dispatches
    //    under episodeKey (executes once) → saga terminalizes COMPENSATED.
    AtomicInteger leaderExecutions = compensationExecutions;
    VirtualThreadCommandBus leaderBus = busFor(leaderExecutions);
    SagaRunner<OrderSagaState> leaderRunner =
        SagaRunner.<OrderSagaState>builder()
            .orchestrator(new OrderSaga())
            .sagaStore(sagaStore)
            .commandBus(leaderBus)
            // The dead-letter store must READ AND WRITE the saga rows it shields
            // (SagaDeadLetterStore's class javadoc). Wiring it to THIS Postgres saga store is what
            // makes a hold's shield, a drain's conditional clear and retention's owner guard answer
            // about the rows under test.
            .sagaDeadLetterStore(new InMemorySagaDeadLetterStore(sagaStore))
            .build();
    SagaCompensationRetrySweeper<OrderSagaState> leaderSweeper =
        SagaCompensationRetrySweeper.<OrderSagaState>builder()
            .sagaRunner(leaderRunner)
            .sagaStore(sagaStore)
            .retryInterval(Duration.ofMillis(50))
            .giveUpAfter(Duration.ofDays(1))
            .build();
    leaderSweeper.start();
    try {
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(sagaStatus(sagaId))
                      .as(
                          "the leader sweeper drives the stuck compensation to terminal COMPENSATED")
                      .contains(SagaStatus.COMPENSATED.name()));
    } finally {
      // Stop the leader sweep BEFORE the stale re-drive so nothing races the deterministic phase.
      leaderSweeper.close();
    }

    long versionAfterLeader = sagaVersion(sagaId);

    // Baseline after the leader: the compensation ran exactly once and recorded exactly one inbox
    // row + one domain event. (A pre-condition — the interesting assertion is that the STALE
    // re-drive
    // below changes none of these.)
    assertThat(compensationExecutions.get())
        .as("leader executed the compensation exactly once")
        .isEqualTo(1);
    assertThat(
            count(
                "SELECT COUNT(*) FROM command_inbox WHERE idempotency_key = ?", episodeKey.value()))
        .as("the compensation episode key is recorded exactly once by the leader")
        .isEqualTo(1L);
    assertThat(
            count(
                "SELECT COUNT(*) FROM event_stream WHERE event_type = ?",
                EventType.fromClass(OrderCancelled.class).name()))
        .as("exactly one OrderCancelled appended by the leader")
        .isEqualTo(1L);

    // 3. STALE replica re-drives the SAME episode (gate-bypassed model — see the class doc). Its
    //    sweep dispatches the SAME compensation command under the SAME episodeCompensationKey
    // through
    //    a SECOND bus over the SHARED inbox. The inbox MUST dedup: the handler does not run again.
    VirtualThreadCommandBus staleBus = busFor(compensationExecutions);
    try {
      CommandBus.CommandResult staleResult = staleBus.execute(new CancelOrder(orderId), episodeKey);

      assertThat(staleResult.idempotentReplay())
          .as(
              "the stale replica's re-drive under the same episode key is deduped by the command "
                  + "inbox (idempotent replay) — NOT a fresh execution, so the sweeper needs no "
                  + "epoch fence")
          .isTrue();
      assertThat(staleResult.events()).as("a deduped replay produces no new events").isEmpty();
    } finally {
      staleBus.close();
    }

    // 4. Exactly-once holds across the stale re-drive: the side-effect count, the domain event, and
    //    the inbox row are all UNCHANGED, and the saga is stable in its terminal state.
    assertThat(compensationExecutions.get())
        .as("compensation side-effect count REMAINS exactly 1 after the stale re-drive")
        .isEqualTo(1);
    assertThat(
            count(
                "SELECT COUNT(*) FROM event_stream WHERE event_type = ?",
                EventType.fromClass(OrderCancelled.class).name()))
        .as("no second OrderCancelled — the compensating event stays appended exactly once")
        .isEqualTo(1L);
    assertThat(
            count(
                "SELECT COUNT(*) FROM command_inbox WHERE idempotency_key = ?", episodeKey.value()))
        .as("the inbox row for the episode key exists exactly once")
        .isEqualTo(1L);
    // Only this one episode key was ever recorded for the saga — no stray second-episode dispatch.
    assertThat(
            count(
                "SELECT COUNT(*) FROM command_inbox WHERE idempotency_key LIKE ?",
                "saga:" + sagaId + ":%"))
        .as("only the single compensation episode key is present for this saga")
        .isEqualTo(1L);
    assertThat(sagaStatus(sagaId))
        .as("saga state unchanged — still terminal COMPENSATED after the stale re-drive")
        .contains(SagaStatus.COMPENSATED.name());
    assertThat(sagaVersion(sagaId))
        .as("the deduped stale re-drive wrote nothing to the saga row (version unchanged)")
        .isEqualTo(versionAfterLeader);
  }
}
