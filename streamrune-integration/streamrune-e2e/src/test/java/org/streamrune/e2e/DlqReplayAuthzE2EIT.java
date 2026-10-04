package org.streamrune.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.streamrune.core.AggregateState;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.DeadLetterRetryPolicy;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.RequireRole;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneContext.RequestContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.postgres.PostgresDeadLetterQueue;
import org.streamrune.runtime.AnnotationAuthorizationInterceptor;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.VirtualThreadCommandBus;

/**
 * End-to-end proof of the headline DLQ-replay claim: a {@code @RequireRole}-protected command that
 * originally passed authorization, failed, and landed in the dead letter queue is replayed <em>as
 * the original user</em> and succeeds — because the runner rebinds the request context the DLQ
 * persisted, and the fail-closed {@link AnnotationAuthorizationInterceptor} re-evaluates the replay
 * against that rebound user.
 *
 * <p>Everything below the test boundary is real, wired the way production wires it: a real {@link
 * PostgresDeadLetterQueue} on the Flyway-provisioned table (its context columns persist the request
 * context), a real {@link VirtualThreadCommandBus} with a real {@link
 * AnnotationAuthorizationInterceptor}, and a real {@link DeadLetterRetryRunner} background loop
 * polling the live queue. No mocks, no bridges: the bus publishes its own {@code cmd_<base36>}
 * command id straight into the live queue.
 *
 * <p>This test originally surfaced a real framework bug: {@code dead_letter_queue.command_id} was a
 * {@code UUID} column with {@code ?::uuid} casts, so the bus's {@code cmd_<base36>} ids (see {@code
 * IdGenerator.generateCommandId()}) were rejected with "invalid input syntax for type uuid" — the
 * command bus could not dead-letter a failed command at all. The column is now {@code
 * VARCHAR(255)}, matching how {@code audit_log} stores command ids, so this test exercises the
 * genuine end-to-end path with no id rewriting.
 *
 * <p>The whole proof hinges on a single fact being load-bearing: the persisted context. {@link
 * #replayRebindsAdminContextSoFailClosedAuthzAllowsReplay()} shows that <em>with</em> the persisted
 * admin context the replay is allowed and the entry is discarded; {@link
 * #replayWithoutPersistedUserIsRejectedByFailClosedAuthzAndStays()} shows the contrast — an
 * otherwise identical entry with <em>no</em> persisted user is rejected by the same fail-closed
 * interceptor and stays in the queue with its attempt count bumped. The second test is what makes
 * the first non-vacuous: the replay succeeds because of the rebind, not because authorization is
 * absent or permissive.
 *
 * <p>Postgres-only — no broker, no timing flakiness — so it runs in the default {@code test} task.
 */
@Timeout(120)
class DlqReplayAuthzE2EIT extends E2ETestBase {

  private static final String ADMIN_USER = "admin-user";
  // The bus persists the fully-qualified class name on the DLQ entry, and the retry runner
  // resolves it FQN-exactly (registerCommand keys by it), so the persisted-commandType assertion
  // must use the FQN, not the simple name.
  private static final String COMMAND_TYPE = ProtectedCommand.class.getName();

  private final ObjectMapper objectMapper = new ObjectMapper();

  /** The real queue the bus publishes through and the runner replays from — no wrapping. */
  private PostgresDeadLetterQueue dlq;

  @BeforeEach
  void setUpDlq() throws Exception {
    // The DLQ table is provisioned by the event-store baseline E2ETestBase applies: it carries the
    // correlation_id/user_id/trace_id context columns the rebind relies on, and a VARCHAR
    // command_id that accepts the bus's cmd_<base36> ids.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DELETE FROM dead_letter_queue");
    }
    dlq = new PostgresDeadLetterQueue(dataSource);
  }

  // ---- Domain under test ---------------------------------------------------------------------

  private static final AggregateType ORDER = AggregateType.of("order");

  /** A command gated by {@code @RequireRole("ADMIN")}; the interceptor enforces it in before(). */
  @RequireRole("ADMIN")
  record ProtectedCommand(String aggregateId) implements org.streamrune.core.Command {}

  /** Single-field event proving the command actually reached the decider and committed. */
  record ProtectedDone(String aggregateId) implements DomainEvent {}

  /** Trivial state — the decider does not branch on it. */
  record ProtectedState() implements AggregateState {}

  /**
   * Decider that fails {@code failuresBeforeSuccess} times before producing its event. This models
   * a handler whose transient downstream is broken on the first execution (the command lands in the
   * DLQ) and healthy by the time the runner replays it.
   */
  static final class FlakyDecider
      implements Decider<ProtectedCommand, ProtectedState, ProtectedDone> {
    // Atomic: the original execution runs on the caller thread, the replay on the runner's virtual
    // thread — the failure-countdown must be visible across that boundary.
    private final java.util.concurrent.atomic.AtomicInteger remainingFailures;

    FlakyDecider(int failuresBeforeSuccess) {
      this.remainingFailures = new java.util.concurrent.atomic.AtomicInteger(failuresBeforeSuccess);
    }

    @Override
    public ProtectedState initialState() {
      return new ProtectedState();
    }

    @Override
    public List<ProtectedDone> decide(ProtectedCommand command, ProtectedState state) {
      if (remainingFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        throw new IllegalStateException("transient handler failure");
      }
      return List.of(new ProtectedDone(command.aggregateId()));
    }

    @Override
    public ProtectedState evolve(ProtectedState state, ProtectedDone event) {
      return state;
    }
  }

  /**
   * Self-contained role resolver: it maps the bound caller's {@link UserId} to roles, granting
   * ADMIN only to {@link #ADMIN_USER} and nothing to anyone else (so an anonymous caller — {@code
   * userId == null} — is rejected upstream by the interceptor before {@code resolve} is even
   * called). This is the production shape: roles derive from the authenticated identity, which the
   * DLQ persists and the runner rebinds, NOT from per-request baggage (the framework does not
   * persist baggage on DLQ entries). A replay that rebinds {@link #ADMIN_USER} therefore
   * re-authorizes; a replay with no rebound user is rejected fail-closed.
   */
  static final class AdminUserRoleResolver implements UserRoleResolver {
    @Override
    public UserAuthority resolve(UserId userId) {
      if (ADMIN_USER.equals(userId.value())) {
        return new UserAuthority(java.util.Set.of("ADMIN"), java.util.Set.of());
      }
      return UserAuthority.EMPTY;
    }
  }

  private VirtualThreadCommandBus commandBus(FlakyDecider decider) {
    return VirtualThreadCommandBus.builder()
        .eventStore(eventStore(dataSource))
        .objectMapper(objectMapper)
        .deadLetterQueue(dlq)
        .interceptors(new AnnotationAuthorizationInterceptor(new AdminUserRoleResolver()))
        // No optimistic-lock retries: the decider failure is permanent for one execution, so the
        // command goes straight to the DLQ after a single attempt instead of being retried in-bus.
        .retryPolicy(new org.streamrune.core.RetryPolicy(1, Duration.ofMillis(1), 1.0, false))
        .register(
            ORDER,
            ProtectedCommand.class,
            cmd -> org.streamrune.core.types.AggregateId.of(cmd.aggregateId()),
            decider)
        .build();
  }

  private DeadLetterRetryRunner runner(VirtualThreadCommandBus bus) {
    return DeadLetterRetryRunner.builder()
        .deadLetterQueue(dlq)
        .commandBus(bus)
        .objectMapper(objectMapper)
        // ZERO initial delay => every entry is due immediately, so the very first poll cycle of the
        // background loop retries it without waiting out a backoff window.
        .policy(new DeadLetterRetryPolicy(5, Duration.ZERO, 2.0, false))
        // Short poll interval: the real background loop cycles fast under test (processBatch is
        // package-private, so the replay is driven through the actual start()/loop path, not poked
        // directly — this is the production code path end to end).
        .pollInterval(Duration.ofMillis(50))
        .registerCommand(ProtectedCommand.class)
        .build();
  }

  private static RequestContext adminContext() {
    return new RequestContext(
        null,
        UserId.of(ADMIN_USER),
        CorrelationId.of("corr-" + UUID.randomUUID()),
        Instant.now(),
        Map.of("role", "ADMIN"));
  }

  private long eventCount(String aggregateId) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "SELECT COUNT(*) FROM event_stream WHERE aggregate_type = ? AND aggregate_id = ?")) {
      ps.setString(1, ORDER.value());
      ps.setString(2, aggregateId);
      try (var rs = ps.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  // ---- Scenario 1: the rebind makes the replay succeed ---------------------------------------

  @Test
  void replayRebindsAdminContextSoFailClosedAuthzAllowsReplay() throws Exception {
    String aggregateId = "order-" + UUID.randomUUID();
    var decider = new FlakyDecider(/* failuresBeforeSuccess= */ 1);
    var bus = commandBus(decider);

    // ---- Original submission WITH the admin context bound. ----
    // before(): the interceptor reads ADMIN_USER + role=ADMIN from the bound context and ALLOWS.
    // decide(): the flaky handler throws its first time, so the bus publishes the command to the
    // DLQ, capturing the bound correlation/user from the context (its correlation_id / user_id
    // columns).
    assertThatThrownBy(
            () ->
                ScopedValue.where(StreamRuneContext.CURRENT, adminContext())
                    .run(() -> bus.execute(new ProtectedCommand(aggregateId))))
        .as("the first execution fails inside the decider and propagates")
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("transient handler failure");

    // The failed command is in the DLQ, and it persisted the originating admin user — without that
    // persisted user the replay below could never re-authorize.
    List<DeadLetterQueue.DeadLetterEntry> queued = dlq.read(10);
    assertThat(queued).hasSize(1);
    DeadLetterQueue.DeadLetterEntry entry = queued.getFirst();
    assertThat(entry.commandType()).isEqualTo(COMMAND_TYPE);
    assertThat(entry.userId()).isNotNull();
    assertThat(entry.userId().value()).isEqualTo(ADMIN_USER);
    assertThat(entry.correlationId()).isNotNull();
    // Authorization passed before the decider ran, so no event was committed yet.
    assertThat(eventCount(aggregateId)).isZero();

    // ---- Replay via the real runner's background loop. It rebinds the persisted admin context;
    // the interceptor re-evaluates the replay AS ADMIN_USER (role=ADMIN baggage) and ALLOWS; the
    // now-healthy decider produces its event; the entry is discarded. ----
    try (var runner = runner(bus)) {
      runner.start();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(dlq.read(10)).as("a successful replay discards the entry").isEmpty());
    }
    assertThat(eventCount(aggregateId))
        .as("the replay re-executed the command and committed exactly one event")
        .isEqualTo(1L);
  }

  // ---- Scenario 1b: the SHIPPED resolver shape, which the rebind alone cannot save -----------

  /**
   * Off-request authorization. {@link AdminUserRoleResolver} above is a pure function of the {@link
   * UserId}, so rebinding the persisted user is enough for it — which is exactly why scenario 1
   * could never exhibit the defect the shipped resolvers have. Every resolver StreamRune ships
   * reads its framework's request-scoped security state instead, and that state is gone on the
   * retry runner's poll thread: the rebound user resolves to {@link UserAuthority#EMPTY} and the
   * already-authorized command is rejected on every attempt until it ages out of the queue —
   * silently lost.
   *
   * <p>This scenario uses a resolver with that shape (backed by a {@link ThreadLocal}, exactly like
   * Spring's {@code SecurityContextHolder}) and declaring {@link
   * UserRoleResolver#requiresRequestContext()}, over the same real Postgres queue and the same real
   * background runner. The replay must complete under the recorded DLQ-replay system principal. The
   * end-to-end proof against the genuinely shipped {@code SpringSecurityUserRoleResolver}, driven
   * through a real Spring container, lives in {@code
   * org.streamrune.spring.OffRequestAuthorizationTest}.
   */
  @Test
  void replayUnderARequestScopedResolverCompletesInsteadOfBeingLost() throws Exception {
    String aggregateId = "order-" + UUID.randomUUID();
    var decider = new FlakyDecider(/* failuresBeforeSuccess= */ 1);
    var resolver = new RequestScopedUserRoleResolver();
    var bus =
        VirtualThreadCommandBus.builder()
            .eventStore(eventStore(dataSource))
            .objectMapper(objectMapper)
            .deadLetterQueue(dlq)
            .interceptors(new AnnotationAuthorizationInterceptor(resolver))
            .retryPolicy(new org.streamrune.core.RetryPolicy(1, Duration.ofMillis(1), 1.0, false))
            .register(
                ORDER,
                ProtectedCommand.class,
                cmd -> org.streamrune.core.types.AggregateId.of(cmd.aggregateId()),
                decider)
            .build();

    // The request thread has an authenticated ADMIN, exactly as a real HTTP request would.
    resolver.authenticate(ADMIN_USER);
    try {
      assertThatThrownBy(
              () ->
                  ScopedValue.where(StreamRuneContext.CURRENT, adminContext())
                      .run(() -> bus.execute(new ProtectedCommand(aggregateId))))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("transient handler failure");
    } finally {
      resolver.clear();
    }

    assertThat(dlq.read(10)).hasSize(1);
    assertThat(dlq.read(10).getFirst().userId().value()).isEqualTo(ADMIN_USER);
    assertThat(eventCount(aggregateId)).isZero();

    // The runner's poll thread never had that authentication and never can — before the fix this
    // rejected every attempt and the command was lost when the ladder ran out.
    try (var runner = runner(bus)) {
      runner.start();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () ->
                  assertThat(dlq.read(10))
                      .as(
                          "an already-authorized command must not be lost because the resolver"
                              + " cannot see a request on the retry thread")
                      .isEmpty());
    }
    assertThat(eventCount(aggregateId))
        .as("the replay re-executed the command and committed exactly one event")
        .isEqualTo(1L);
  }

  /**
   * A resolver with the shape of every resolver StreamRune ships: it answers from ambient
   * per-request state (here a {@link ThreadLocal}, the same mechanism as Spring's {@code
   * SecurityContextHolder}) rather than from the {@link UserId} argument, so it can only ever
   * return {@link UserAuthority#EMPTY} on a thread that never handled the request.
   */
  static final class RequestScopedUserRoleResolver implements UserRoleResolver {
    private final ThreadLocal<String> authenticated = new ThreadLocal<>();

    void authenticate(String user) {
      authenticated.set(user);
    }

    void clear() {
      authenticated.remove();
    }

    @Override
    public UserAuthority resolve(UserId userId) {
      String principal = authenticated.get();
      if (principal == null || !principal.equals(userId.value())) {
        return UserAuthority.EMPTY;
      }
      return new UserAuthority(java.util.Set.of("ADMIN"), java.util.Set.of());
    }

    @Override
    public boolean requiresRequestContext() {
      return true;
    }
  }

  // ---- Non-vacuity: without the rebound user, fail-closed authz rejects the replay -----------

  @Test
  void replayWithoutPersistedUserIsRejectedByFailClosedAuthzAndStays() throws Exception {
    String aggregateId = "order-" + UUID.randomUUID();
    // A decider that would SUCCEED immediately if it ever ran — so the only thing that can keep
    // this
    // entry in the DLQ is authorization rejecting the replay before the decider is reached.
    var alwaysSucceeds = new FlakyDecider(/* failuresBeforeSuccess= */ 0);
    var bus = commandBus(alwaysSucceeds);

    // Publish an entry directly with NO persisted user/trace (only a correlation id, which a
    // RequestContext requires). This is the entry shape produced by a command that ran with no
    // authenticated context — the runner rebinds correlation only, leaving the user anonymous.
    CommandId commandId = CommandId.of(UUID.randomUUID().toString());
    dlq.publish(
        new DeadLetterQueue.DeadLetterPublishRequest(
            objectMapper.writeValueAsString(new ProtectedCommand(aggregateId)),
            COMMAND_TYPE,
            commandId,
            StreamId.of(ORDER, org.streamrune.core.types.AggregateId.of(aggregateId)),
            "java.lang.IllegalStateException",
            "original failure",
            1,
            Instant.now(),
            CorrelationId.of("corr-" + UUID.randomUUID()),
            /* userId= */ null,
            /* traceId= */ null,
            null));

    assertThat(dlq.read(10)).hasSize(1);

    // ---- Replay. The runner rebinds the correlation but no user, so the interceptor sees an
    // anonymous caller for a @RequireRole("ADMIN") command and throws AuthorizationException. The
    // runner catches it, records a failed attempt, and the entry STAYS. Await the failed-attempt
    // bump (proof the loop ran and authz rejected it) rather than a fixed sleep. ----
    try (var runner = runner(bus)) {
      runner.start();
      await()
          .atMost(Duration.ofSeconds(30))
          .untilAsserted(
              () -> {
                List<DeadLetterQueue.DeadLetterEntry> entries = dlq.read(10);
                assertThat(entries)
                    .as("a replay rejected by fail-closed authz is NOT discarded")
                    .hasSize(1);
                assertThat(entries.getFirst().dlqAttempts())
                    .as("the rejected replay counts as a failed DLQ attempt")
                    .isGreaterThanOrEqualTo(1);
              });
    }

    assertThat(dlq.read(10))
        .as("the rejected entry is still queued after the runner stops")
        .hasSize(1);
    assertThat(eventCount(aggregateId))
        .as("the rejected command never reached the decider, so nothing was committed")
        .isZero();
  }

  /**
   * Pins the exact failure mode the previous test relies on: the fail-closed interceptor throws
   * {@link AuthorizationException} (not some unrelated error) when a {@code @RequireRole} command
   * runs with no bound user. This is the mechanism that keeps the un-rebound entry in the queue.
   */
  @Test
  void protectedCommandWithoutBoundUserThrowsAuthorizationException() {
    var bus = commandBus(new FlakyDecider(0));
    assertThatThrownBy(() -> bus.execute(new ProtectedCommand("order-" + UUID.randomUUID())))
        .isInstanceOf(AuthorizationException.class);
  }
}
