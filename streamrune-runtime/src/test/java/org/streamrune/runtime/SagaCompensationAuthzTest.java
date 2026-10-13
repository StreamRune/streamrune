package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AggregateState;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.Command;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventEnvelope;
import org.streamrune.core.EventStore;
import org.streamrune.core.RequireRole;
import org.streamrune.core.RetryPolicy;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryEventStore;
import org.streamrune.test.SagaEnvelopes;

/**
 * End-to-end: a saga's {@code @RequireRole}-gated compensation command, dispatched via {@link
 * SagaCommandDispatch#dispatch} on a thread with NO bound user (exactly how the subscription /
 * timeout / sweeper threads run), must reach its handler and commit — because {@code dispatch}
 * binds {@link StreamRuneContext#SAGA_OWNED} and {@link AnnotationAuthorizationInterceptor} honors
 * the saga-system principal. The contrast test proves the gate is real: the same command executed
 * directly (not saga-owned) with no user is rejected.
 */
class SagaCompensationAuthzTest {

  private static final AggregateType TYPE = AggregateType.of("payment");

  /** A compensation command gated behind a role no saga thread can present (no bound user). */
  @RequireRole("payments:refund")
  record RefundPayment(String orderId) implements Command {}

  record PaymentRefunded(String orderId) implements DomainEvent {}

  record RefundState() implements AggregateState {}

  static final class RefundDecider implements Decider<RefundPayment, RefundState, PaymentRefunded> {
    final AtomicInteger decideCalls = new AtomicInteger();

    @Override
    public RefundState initialState() {
      return new RefundState();
    }

    @Override
    public List<PaymentRefunded> decide(RefundPayment command, RefundState state) {
      decideCalls.incrementAndGet();
      return List.of(new PaymentRefunded(command.orderId()));
    }

    @Override
    public RefundState evolve(RefundState state, PaymentRefunded event) {
      return state;
    }
  }

  /** Grants nothing to anyone — so only the saga-system-principal bypass can let the refund run. */
  static final class DenyAllResolver implements UserRoleResolver {
    @Override
    public UserAuthority resolve(org.streamrune.core.types.UserId userId) {
      return UserAuthority.EMPTY;
    }
  }

  private VirtualThreadCommandBus buildBus(RefundDecider decider) {
    // Saga dispatch executes with an idempotency key → the keyed append path needs the same
    // inbox wired into the event store AND the bus.
    var inbox = new InMemoryCommandInbox();
    EventStore eventStore = new InMemoryEventStore().withCommandInbox(inbox);
    return VirtualThreadCommandBus.builder()
        .eventStore(eventStore)
        .locker(new LocalStripedLocker(16))
        .retryPolicy(new RetryPolicy(1, Duration.ofMillis(1), 1.0))
        .objectMapper(new ObjectMapper())
        .commandInbox(inbox)
        .interceptors(new AnnotationAuthorizationInterceptor(new DenyAllResolver()))
        .register(TYPE, RefundPayment.class, cmd -> AggregateId.of(cmd.orderId()), decider)
        .build();
  }

  @Test
  void sagaDispatchedCompensation_withNoBoundUser_runsAsSystemPrincipal() {
    var decider = new RefundDecider();
    var bus = buildBus(decider);

    // No StreamRuneContext user is bound (a subscription/timeout/sweeper thread). The refund is
    // @RequireRole-gated and the resolver grants nothing — the ONLY way this succeeds is the
    // saga-system-principal bypass wired to SAGA_OWNED (bound by saga dispatch).
    assertDoesNotThrow(
        () ->
            SagaCommandDispatch.dispatch(
                bus,
                SagaOrigins.of("saga-order-1"),
                null,
                new RefundPayment("order-1"),
                IdempotencyKey.of("saga:saga-order-1:episode:1:comp:0")));

    assertEquals(
        1,
        decider.decideCalls.get(),
        "the gated compensation command reached its handler as the saga-system principal");
  }

  @Test
  void directCompensationCommand_withNoBoundUser_isRejected() {
    // Contrast (non-vacuity): the SAME gated command executed directly (NOT saga-owned) with no
    // bound user is still rejected fail-closed — the bypass is scoped strictly to saga dispatch.
    var decider = new RefundDecider();
    var bus = buildBus(decider);

    var ctx =
        new StreamRuneContext.RequestContext(
            null, null, CorrelationId.of("direct"), Instant.now(), Map.of());
    ScopedValue.where(StreamRuneContext.CURRENT, ctx)
        .run(
            () ->
                assertThrows(
                    AuthorizationException.class, () -> bus.execute(new RefundPayment("order-2"))));
    assertEquals(0, decider.decideCalls.get(), "the rejected command never reached its handler");
  }

  private record FulfillmentState(SagaStatus status) implements SagaState {}

  /**
   * A listener's continuation of an event a saga caused carries that saga's origin and none of its
   * authority: the gated command is refused, on a plain thread and on the thread of a saga's own
   * dispatch, where the same command executed directly runs as the system.
   */
  @Test
  void aContinuationOfASagaCausedEvent_runsWithoutTheSagasAuthority() {
    var decider = new RefundDecider();
    var bus = buildBus(decider);
    EventEnvelope reply =
        SagaEnvelopes.causedBy(
            FulfillmentState.class,
            SagaId.of("saga-order-1"),
            StreamId.of(TYPE, AggregateId.of("order-1")),
            new PaymentRefunded("order-1"));

    assertThrows(
        AuthorizationException.class,
        () ->
            StreamRuneContext.continuing(reply)
                .run(() -> bus.execute(new RefundPayment("order-3"))));
    assertEquals(0, decider.decideCalls.get());

    // The thread of a saga's dispatch, bound as saga dispatch binds it — the saga's reserved
    // correlation id, the saga flag and the saga's origin — and holding a user whose captured
    // authority grants the role.
    SagaOrigin origin = reply.metadata().sagaOrigin();
    var privileged =
        new StreamRuneContext.RequestContext(
            null,
            UserId.of("operator"),
            origin.correlationId(),
            Instant.now(),
            Map.of("role", "payments:refund"),
            new UserAuthority(Set.of("payments:refund"), Set.of()));
    assertEquals("streamrune:saga:saga-order-1", privileged.correlationId().value());
    ScopedValue.where(StreamRuneContext.CURRENT, privileged)
        .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
        .where(StreamRuneContext.CAUSE, new StreamRuneContext.DispatchCause(null, origin))
        .run(
            () -> {
              // Control: directly on this thread the gated command passes.
              bus.execute(new RefundPayment("order-4"));
              assertEquals(1, decider.decideCalls.get());

              assertThrows(
                  AuthorizationException.class,
                  () ->
                      StreamRuneContext.continuing(reply)
                          .run(
                              () -> {
                                assertEquals(1, StreamRuneContext.cause().sagaOrigin().hop());
                                bus.execute(new RefundPayment("order-5"));
                              }));
            });
    assertEquals(
        1, decider.decideCalls.get(), "neither the saga flag nor the user reached the command");
  }
}
