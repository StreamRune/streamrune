package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaOrigin;
import org.streamrune.core.saga.SagaState;
import org.streamrune.core.saga.SagaStatus;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CausationId;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.EventId;
import org.streamrune.core.types.EventType;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.IdempotencyKey;
import org.streamrune.core.types.SagaType;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.test.SagaEnvelopes;

class StreamRuneContextTest {

  private static final Instant NOW = Instant.now();

  @Test
  void scopedValueShouldBeAvailableWithinScope() throws Exception {
    var ctx =
        new StreamRuneContext.RequestContext(
            TraceId.of("trace-1"),
            UserId.of("user-1"),
            CorrelationId.of("corr-1"),
            NOW,
            Map.of("key", "val"));

    ScopedValue.where(StreamRuneContext.CURRENT, ctx)
        .run(
            () -> {
              assertTrue(StreamRuneContext.CURRENT.isBound());
              var current = StreamRuneContext.CURRENT.get();
              assertEquals("trace-1", current.traceId().value());
              assertEquals("user-1", current.userId().value());
              assertEquals("corr-1", current.correlationId().value());
              assertEquals(NOW, current.timestamp());
              assertEquals(Map.of("key", "val"), current.baggage());
            });
  }

  @Test
  void scopedValueShouldNotBeBoundOutsideScope() {
    assertFalse(StreamRuneContext.CURRENT.isBound());
    assertThrows(NoSuchElementException.class, () -> StreamRuneContext.CURRENT.get());
  }

  @Test
  void requestContextShouldRejectNullCorrelationId() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new StreamRuneContext.RequestContext(null, null, null, NOW, null));
  }

  @Test
  void blankCorrelationIdIsRejectedByCorrelationIdBeforeRequestContextSeesIt() {
    // RequestContext checks only for null: CorrelationId's own constructor rejects a blank value,
    // so a blank correlation id cannot be constructed, let alone passed in.
    assertThrows(IllegalArgumentException.class, () -> CorrelationId.of("  "));
  }

  @Test
  void requestContextShouldRejectNullTimestamp() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamRuneContext.RequestContext(
                null, null, CorrelationId.of("corr-1"), null, null));
  }

  @Test
  void requestContextShouldDefaultBaggageToEmptyMap() {
    var ctx =
        new StreamRuneContext.RequestContext(null, null, CorrelationId.of("corr-1"), NOW, null);
    assertNotNull(ctx.baggage());
    assertTrue(ctx.baggage().isEmpty());
  }

  @Test
  void requestContextBaggageShouldBeImmutable() {
    var mutable = new java.util.HashMap<>(Map.of("k", "v"));
    var ctx =
        new StreamRuneContext.RequestContext(null, null, CorrelationId.of("corr-1"), NOW, mutable);

    // Mutating original should not affect context
    mutable.put("k2", "v2");
    assertEquals(1, ctx.baggage().size());

    // Returned map should be unmodifiable
    assertThrows(UnsupportedOperationException.class, () -> ctx.baggage().put("k3", "v3"));
  }

  @Test
  void captureShouldReturnNullWhenUnbound() {
    assertNull(StreamRuneContext.capture());
  }

  @Test
  void captureShouldReturnBoundContext() {
    var ctx =
        new StreamRuneContext.RequestContext(null, null, CorrelationId.of("corr-1"), NOW, null);
    ScopedValue.where(StreamRuneContext.CURRENT, ctx)
        .run(() -> assertSame(ctx, StreamRuneContext.capture()));
  }

  @Test
  void wrapRunnableShouldRebindContextAcrossThreadStart() throws Exception {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, UserId.of("user-1"), CorrelationId.of("corr-1"), NOW, null);
    var observedUser = new java.util.concurrent.atomic.AtomicReference<String>();

    Runnable wrapped =
        ScopedValue.where(StreamRuneContext.CURRENT, ctx)
            .call(
                () ->
                    StreamRuneContext.wrap(
                        () -> observedUser.set(StreamRuneContext.CURRENT.get().userId().value())));

    // ScopedValue bindings do not survive Thread.start(); the wrapper must re-bind.
    Thread thread = new Thread(wrapped);
    thread.start();
    thread.join();
    assertEquals("user-1", observedUser.get());
  }

  @Test
  void wrapRunnableShouldReturnTaskUnchangedWhenUnbound() {
    Runnable task = () -> {};
    assertSame(task, StreamRuneContext.wrap(task));
  }

  @Test
  void wrapRunnableShouldRejectNullTask() {
    assertThrows(NullPointerException.class, () -> StreamRuneContext.wrap((Runnable) null));
  }

  @Test
  void wrapCallableShouldRebindContextAcrossThreadBoundary() throws Exception {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, UserId.of("user-2"), CorrelationId.of("corr-2"), NOW, null);

    java.util.concurrent.Callable<String> wrapped =
        ScopedValue.where(StreamRuneContext.CURRENT, ctx)
            .call(
                () ->
                    StreamRuneContext.wrap(() -> StreamRuneContext.CURRENT.get().userId().value()));

    try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
      assertEquals("user-2", executor.submit(wrapped).get());
    }
  }

  @Test
  void wrapCallableShouldPropagateTaskException() {
    var ctx =
        new StreamRuneContext.RequestContext(null, null, CorrelationId.of("corr-3"), NOW, null);
    java.util.concurrent.Callable<String> wrapped =
        ScopedValue.where(StreamRuneContext.CURRENT, ctx)
            .call(
                () ->
                    StreamRuneContext.<String>wrap(
                        () -> {
                          throw new IllegalStateException("boom");
                        }));
    assertThrows(IllegalStateException.class, wrapped::call);
  }

  @Test
  void wrapCallableShouldReturnTaskUnchangedWhenUnbound() {
    java.util.concurrent.Callable<String> task = () -> "x";
    assertSame(task, StreamRuneContext.wrap(task));
  }

  @Test
  void wrapCallableShouldRejectNullTask() {
    assertThrows(
        NullPointerException.class,
        () -> StreamRuneContext.wrap((java.util.concurrent.Callable<String>) null));
  }

  @Test
  void nestedScopesShouldNotLeakToOuter() throws Exception {
    var outer =
        new StreamRuneContext.RequestContext(null, null, CorrelationId.of("outer"), NOW, null);
    var inner =
        new StreamRuneContext.RequestContext(null, null, CorrelationId.of("inner"), NOW, null);

    ScopedValue.where(StreamRuneContext.CURRENT, outer)
        .run(
            () -> {
              assertEquals("outer", StreamRuneContext.CURRENT.get().correlationId().value());

              ScopedValue.where(StreamRuneContext.CURRENT, inner)
                  .run(
                      () -> {
                        assertEquals(
                            "inner", StreamRuneContext.CURRENT.get().correlationId().value());
                      });

              // After inner scope exits, outer value should be restored
              assertEquals("outer", StreamRuneContext.CURRENT.get().correlationId().value());
            });
  }

  // ==== Baggage value policy at the request-context boundary ====

  @Test
  void requestContextSanitizesBaggageValuesAtTheBoundary() {
    // BaggageAllowlist.filter is the shared choke point the three framework request filters call —
    // but each of them then ADDS the X-User-Role header (trusted-gateway mode) into baggage
    // under the `role` key AFTER the filter has run, past its value policy. RequestContext is
    // the one boundary every path crosses on the way to EventMetadata and the append-only,
    // plaintext event_stream.metadata column, so the value policy is enforced here too.
    var ctx =
        new StreamRuneContext.RequestContext(
            null,
            null,
            CorrelationId.of("c-1"),
            NOW,
            java.util.Map.of(
                "role",
                "adm\r\nin",
                "smuggled",
                "p".repeat(BaggageAllowlist.MAX_VALUE_LENGTH + 1)));

    assertEquals("admin", ctx.baggage().get("role"), "control characters never reach metadata");
    assertFalse(
        ctx.baggage().containsKey("smuggled"),
        "an over-cap value is dropped at the boundary, however it got into the map");
  }

  // ==== The header-sourced IDs are bounded at the INGRESS DOOR ====
  //
  // The bound first sat in CorrelationId/TraceId themselves, which also run on the read path
  // (Jackson rebuilding EventMetadata from a stored row), so it made already-persisted events
  // permanently unreadable while preventing no write. The bound then moved to the RequestContext
  // constructor — which turned out to be the same mistake one call further down, because that
  // constructor is also the DLQ-replay and saga-dispatch RECONSTRUCTION path.
  //
  // It now lives on fromRequest: the door the three framework request filters take with values
  // they just read off the wire, and the only construction path an untrusted value can reach.

  @Test
  void fromRequestRejectsAnOversizedCorrelationIdAtIngress() {
    var oversized = CorrelationId.of("c".repeat(256));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                StreamRuneContext.RequestContext.fromRequest(null, null, oversized, NOW, Map.of()));
    assertTrue(ex.getMessage().contains("correlationId"), ex.getMessage());
    assertDoesNotThrow(
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                null, null, CorrelationId.of("c".repeat(255)), NOW, Map.of()));
  }

  @Test
  void fromRequestRejectsControlCharactersInTheHeaderSourcedIds() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                null, null, CorrelationId.of("corr\r\n_1"), NOW, Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                TraceId.of("trace" + (char) 0 + "_1"),
                null,
                CorrelationId.of("corr-1"),
                NOW,
                Map.of()));
  }

  @Test
  void fromRequestRejectsAnOversizedTraceIdButToleratesItsAbsence() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                TraceId.of("t".repeat(256)), null, CorrelationId.of("corr-1"), NOW, Map.of()));
    // traceId is nullable — the bound must not turn "no X-Trace-Id header" into a 500.
    assertDoesNotThrow(
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                null, null, CorrelationId.of("corr-1"), NOW, Map.of()));
  }

  /**
   * {@code userId} is the third header-sourced value crossing this door, and it was the one the
   * door did not check.
   *
   * <p>In header-identity mode ({@code X-User-Id} with no resolver wired, or {@code
   * trust-user-id-header=true}) it is attacker-supplied outright; in authenticated mode it is
   * whatever the identity provider asserted, which is not the same thing as charset-safe or
   * bounded. Either way it travels the identical path the correlation/trace bound exists to protect
   * — {@code EventMetadata.userId} into plaintext, immutable, unshreddable {@code
   * event_stream.metadata} — plus one channel those two share and one they do not: {@code
   * dead_letter_queue.user_id VARCHAR(255)}, where an over-long identity makes the INSERT fail.
   * That failure is caught and logged by {@code VirtualThreadCommandBus} (the original command
   * failure is what propagates), so an accepted command's only recovery channel was silently never
   * created.
   *
   * <p><b>Rejected, never truncated.</b> Truncation is the right answer for baggage (telemetry: a
   * capped value loses information and breaks nothing) and the wrong answer for an identity: {@code
   * alice-<250 chars>-1} and {@code alice-<250 chars>-2} truncate to the SAME principal, and {@code
   * Authorization.requireOwner} authorizes against exactly this field — so truncating would convert
   * an over-long identity into a different, existing user's. A rejected request is a 4xx an
   * operator can see; a truncated identity is an authorization decision made on a value nobody
   * chose.
   */
  @Test
  void fromRequestRejectsAnOversizedOrControlBearingUserIdAtIngress() {
    var oversized = UserId.of("u".repeat(256));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                StreamRuneContext.RequestContext.fromRequest(
                    null, oversized, CorrelationId.of("corr-1"), NOW, Map.of()));
    assertTrue(ex.getMessage().contains("userId"), ex.getMessage());

    // CR/LF or NUL in a principal name: the first forges log lines, the second breaks the jsonb
    // metadata write outright and would fail EVERY command by that user, after partial work.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                null, UserId.of("alice\r\nadmin"), CorrelationId.of("corr-1"), NOW, Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                null, UserId.of("alice" + (char) 0), CorrelationId.of("corr-1"), NOW, Map.of()));

    // The bound is the DLQ column width, and the column width is a legitimate identity's ceiling:
    // 255 characters is an order of magnitude above a UUID, an email address or a JWT `sub`.
    assertDoesNotThrow(
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                null, UserId.of("u".repeat(255)), CorrelationId.of("corr-1"), NOW, Map.of()));
    // userId is nullable — an anonymous request must not become a 500.
    assertDoesNotThrow(
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                null, null, CorrelationId.of("corr-1"), NOW, Map.of()));
  }

  /**
   * The identity half — and the reason this bound is on the door and NOT on {@link UserId}. {@code
   * UserId} is reconstructed from storage at three sites ({@code PostgresDeadLetterQueue}, {@code
   * PostgresCommandAuditQuery}, {@code PostgresEventAuditQuery}), and {@code
   * DeadLetterRetryRunner.replayContextFrom} rebuilds a whole {@code RequestContext} from those
   * columns. A constructor bound would reproduce the same defect exactly: a stored identity the
   * bound refuses would fail deterministically on every retry, burning the ladder and permanently
   * discarding an already-accepted business command — destroying the very recovery channel this
   * bound exists to protect.
   */
  @Test
  void theUserIdTypeAndTheReconstructionDoorStillAcceptWhatIsAlreadyAtRest() {
    assertDoesNotThrow(() -> UserId.of("u".repeat(4096)));
    assertDoesNotThrow(() -> UserId.of("alice\r\nadmin"));
    assertDoesNotThrow(
        () ->
            new StreamRuneContext.RequestContext(
                null, UserId.of("u".repeat(4096)), CorrelationId.of("corr-1"), NOW, Map.of()));
    assertDoesNotThrow(
        () ->
            new StreamRuneContext.RequestContext(
                    null, UserId.of("alice\r\nadmin"), CorrelationId.of("corr-1"), NOW, Map.of())
                .withAuthority(new UserAuthority(Set.of("ADMIN"), Set.of())));
  }

  @Test
  void fromRequestStillEnforcesTheOrdinaryInvariantsAndSanitizesBaggage() {
    // The ingress door is the constructor plus the bound, not a different set of rules.
    assertThrows(
        IllegalArgumentException.class,
        () -> StreamRuneContext.RequestContext.fromRequest(null, null, null, NOW, Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            StreamRuneContext.RequestContext.fromRequest(
                null, null, CorrelationId.of("corr-1"), null, Map.of()));
    var ctx =
        StreamRuneContext.RequestContext.fromRequest(
            null, null, CorrelationId.of("corr-1"), NOW, Map.of("role", "admin\r\nInjected"));
    assertEquals(
        "adminInjected",
        ctx.baggage().get("role"),
        "the baggage half of the value policy still sanitizes on the ingress door too");
    assertNull(ctx.authority(), "the ingress door captures no authority; withAuthority does that");
  }

  // ==== The CONSTRUCTOR is the reconstruction door and must accept what is at rest ====

  @Test
  void theConstructorAcceptsStoredIdsTheIngressBoundRefuses() {
    // These are the exact shapes DeadLetterRetryRunner.replayContextFrom reads out of
    // dead_letter_queue and SagaCommandDispatch derives from a stored saga_state.saga_id. The write
    // already happened through a path that never crossed the ingress door; rejecting them now
    // prevents nothing and only destroys the recovery path.
    var control = CorrelationId.of("corr\r\nFORGED");
    var ctx =
        assertDoesNotThrow(
            () -> new StreamRuneContext.RequestContext(null, null, control, NOW, Map.of()));
    assertEquals(
        "corr\r\nFORGED",
        ctx.correlationId().value(),
        "reconstruction is verbatim — a rewritten id is a different id");

    assertDoesNotThrow(
        () ->
            new StreamRuneContext.RequestContext(
                TraceId.of("trace" + (char) 0x01), null, control, NOW, Map.of()));
    assertDoesNotThrow(
        () ->
            new StreamRuneContext.RequestContext(
                null, null, CorrelationId.of("c".repeat(4096)), NOW, Map.of()));
  }

  @Test
  void withAuthorityDoesNotReApplyTheIngressBoundToAReconstructedContext() {
    // A copy is not an ingress. Were it treated as one, attaching an authority to a replayed
    // context would resurrect exactly the failure this design avoids.
    var reconstructed =
        new StreamRuneContext.RequestContext(
            TraceId.of("trace" + (char) 0x01),
            UserId.of("u1"),
            CorrelationId.of("corr\r\nFORGED"),
            NOW,
            Map.of());
    var withAuth =
        assertDoesNotThrow(
            () -> reconstructed.withAuthority(new UserAuthority(Set.of("ADMIN"), Set.of())));
    assertEquals(reconstructed.correlationId(), withAuth.correlationId());
    assertTrue(withAuth.authority().hasRole("ADMIN"));
  }

  @Test
  void theConstructorStillRefusesAMissingCorrelationIdOrTimestamp() {
    // Relaxing the ingress bound must not relax the record's own invariants: those are structural,
    // and no stored row can be missing them (a null correlation id short-circuits before the
    // context is built at all).
    assertThrows(
        IllegalArgumentException.class,
        () -> new StreamRuneContext.RequestContext(null, null, null, NOW, Map.of()));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new StreamRuneContext.RequestContext(
                null, null, CorrelationId.of("corr-1"), null, Map.of()));
  }

  // ==== The reserved correlation prefix ====

  @Test
  void fromRequest_refusesAReservedCorrelationId_withoutEchoingIt() {
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                StreamRuneContext.RequestContext.fromRequest(
                    null, null, CorrelationId.of("streamrune:saga:victim"), NOW, Map.of()));

    assertEquals("correlationId uses the reserved 'streamrune:' prefix", ex.getMessage());
    assertFalse(ex.getMessage().contains("victim"), "the refused value is not echoed");
  }

  @Test
  void fromRequest_refusesEveryValueWithThePrefix_notOnlyTheSagaForm() {
    for (String reserved : new String[] {"streamrune:", "streamrune:x", "streamrune:saga:"}) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              StreamRuneContext.RequestContext.fromRequest(
                  null, null, CorrelationId.of(reserved), NOW, Map.of()),
          reserved);
    }
  }

  @Test
  void fromRequest_keepsACorrelationIdThatMerelyResemblesTheReservedForm() {
    // The check is an exact, case-sensitive prefix match: a bare saga id, another case, the word
    // without its colon and the prefix further inside the value are an upstream's own ids.
    for (String ordinary :
        new String[] {
          "order-saga-42",
          "saga:order-saga-42",
          "Streamrune:saga:order-saga-42",
          "STREAMRUNE:saga:order-saga-42",
          "streamrune",
          "streamrune-saga-order-saga-42",
          "upstream/streamrune:saga:order-saga-42"
        }) {
      var ctx =
          StreamRuneContext.RequestContext.fromRequest(
              null, null, CorrelationId.of(ordinary), NOW, Map.of());
      assertEquals(ordinary, ctx.correlationId().value(), "kept verbatim");
    }
  }

  @Test
  void fromRequest_reportsTheLengthAndTheCharsetBeforeThePrefix() {
    // An over-long or control-bearing value that also starts with the prefix is refused either
    // way; which message it gets is pinned so the three refusals stay three distinct statements.
    assertEquals(
        "correlationId must be at most 255 characters, got 300",
        assertThrows(
                IllegalArgumentException.class,
                () ->
                    StreamRuneContext.RequestContext.fromRequest(
                        null,
                        null,
                        CorrelationId.of("streamrune:" + "c".repeat(289)),
                        NOW,
                        Map.of()))
            .getMessage());
  }

  @Test
  void fromRequest_acceptsATraceIdWithThePrefix() {
    // The prefix is reserved for correlation ids. A trace id is tracing data with no reserved form.
    var ctx =
        StreamRuneContext.RequestContext.fromRequest(
            TraceId.of("streamrune:saga:victim"),
            UserId.of("streamrune:saga:victim"),
            CorrelationId.of("corr-1"),
            NOW,
            Map.of("tenant", "streamrune:saga:victim"));

    assertEquals("streamrune:saga:victim", ctx.traceId().value());
    assertEquals("streamrune:saga:victim", ctx.userId().value());
    assertEquals("streamrune:saga:victim", ctx.baggage().get("tenant"));
  }

  @Test
  void theConstructorAcceptsAReservedCorrelationId() {
    // The reconstruction door: saga dispatch builds its context here, and StreamRuneContext
    // .continuing copies a stored event's correlation id through it.
    var reserved = CorrelationId.of("streamrune:saga:victim");

    var ctx = new StreamRuneContext.RequestContext(null, null, reserved, NOW, Map.of());
    var withAuthority = ctx.withAuthority(new UserAuthority(Set.of("ADMIN"), Set.of()));

    assertEquals(reserved, ctx.correlationId());
    assertEquals(reserved, withAuthority.correlationId());
  }

  // ==== The dispatch-side refusal of a reserved correlation id that is not its origin's ====

  private static StreamRuneContext.RequestContext contextWith(String correlationId) {
    return new StreamRuneContext.RequestContext(
        null, null, CorrelationId.of(correlationId), NOW, Map.of());
  }

  private static final String FOREIGN_RESERVED_ID =
      "the bound correlation id uses the reserved 'streamrune:' prefix and is not the bound saga"
          + " origin's; only a saga dispatch or StreamRuneContext.continuing binds it";

  private static void assertRefusedAsForeign(ScopedValue.Carrier bindings) {
    ReservedCorrelationIdException refused =
        assertThrows(
            ReservedCorrelationIdException.class,
            () -> bindings.run(StreamRuneContext::requireNoForeignReservedCorrelationId));
    assertEquals(FOREIGN_RESERVED_ID, refused.getMessage());
  }

  @Test
  void theRefusalOfAForeignReservedCorrelationId_isNeitherAnArgumentNorADomainRejection() {
    // A listener that takes a domain rejection as its command's answer catches DomainException or
    // IllegalArgumentException. This refusal is a wiring error of the caller and is neither.
    RuntimeException refused = new ReservedCorrelationIdException();

    assertFalse(refused instanceof IllegalArgumentException);
    assertFalse(refused instanceof DomainException);
    assertFalse(refused instanceof IllegalStateException);
    assertEquals(FOREIGN_RESERVED_ID, refused.getMessage());
    assertFalse(refused.getMessage().contains("saga:"), "the message names no bound value");
  }

  @Test
  void requireNoForeignReservedCorrelationId_passesWithNothingBoundOrAnOrdinaryId() {
    assertDoesNotThrow(StreamRuneContext::requireNoForeignReservedCorrelationId);
    for (String ordinary :
        new String[] {"client-corr-7", "order-saga-42", "Streamrune:saga:order-saga-42"}) {
      ScopedValue.where(StreamRuneContext.CURRENT, contextWith(ordinary))
          .run(StreamRuneContext::requireNoForeignReservedCorrelationId);
      // An ordinary id beside a saga origin is not this check's business.
      ScopedValue.where(StreamRuneContext.CURRENT, contextWith(ordinary))
          .where(StreamRuneContext.CAUSE, BOUND_CAUSE)
          .run(StreamRuneContext::requireNoForeignReservedCorrelationId);
    }
  }

  @Test
  void requireNoForeignReservedCorrelationId_passesForAContextBoundToNull() {
    // ScopedValue.where(CURRENT, StreamRuneContext.capture()) with nothing bound binds null.
    ScopedValue.where(StreamRuneContext.CURRENT, (StreamRuneContext.RequestContext) null)
        .run(StreamRuneContext::requireNoForeignReservedCorrelationId);
  }

  @Test
  void requireNoForeignReservedCorrelationId_refusesAReservedIdWithoutAnOrigin() {
    for (String reserved :
        new String[] {"streamrune:saga:order-saga-42", "streamrune:anything", "streamrune:"}) {
      assertRefusedAsForeign(ScopedValue.where(StreamRuneContext.CURRENT, contextWith(reserved)));
      assertRefusedAsForeign(
          ScopedValue.where(StreamRuneContext.CURRENT, contextWith(reserved))
              .where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE));
      assertRefusedAsForeign(
          ScopedValue.where(StreamRuneContext.CURRENT, contextWith(reserved))
              .where(
                  StreamRuneContext.CAUSE,
                  new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), null)));
    }
  }

  @Test
  void requireNoForeignReservedCorrelationId_passesForTheBoundOriginsOwnId_atAnyHop() {
    for (SagaOrigin origin :
        new SagaOrigin[] {ORIGIN, ORIGIN.nextHop(), ORIGIN.nextHop().nextHop()}) {
      ScopedValue.where(StreamRuneContext.CURRENT, contextWith("streamrune:saga:order-saga-42"))
          .where(StreamRuneContext.CAUSE, new StreamRuneContext.DispatchCause(null, origin))
          .run(StreamRuneContext::requireNoForeignReservedCorrelationId);
    }
  }

  @Test
  void requireNoForeignReservedCorrelationId_refusesAReservedIdThatIsNotTheBoundOrigins() {
    // Another saga's reserved id, and a reserved id that is no saga's, beside the origin of
    // order-saga-42: the bound correlation id and the origin would name two different things.
    for (String foreign :
        new String[] {
          "streamrune:saga:another-saga", "streamrune:saga:order-saga-4", "streamrune:anything"
        }) {
      assertRefusedAsForeign(
          ScopedValue.where(StreamRuneContext.CURRENT, contextWith(foreign))
              .where(StreamRuneContext.CAUSE, BOUND_CAUSE));
    }
  }

  @Test
  void continuing_anEventWithAnOriginAndAnotherCorrelationId_bindsTheOriginsCorrelationId() {
    // Metadata no command bus writes: an origin beside a correlation id that is not the origin's.
    // The continuation binds the origin's, so the context and the cause name the same saga.
    EventMetadata bare = sagaCausedEvent().metadata();
    for (String other : new String[] {"order-saga-42", "streamrune:saga:another-saga"}) {
      EventEnvelope event =
          envelopeWith(
              new EventMetadata(
                  bare.eventId(),
                  bare.commandId(),
                  null,
                  null,
                  CorrelationId.of(other),
                  null,
                  null,
                  NOW,
                  Map.of(),
                  bare.sagaOrigin()));

      StreamRuneContext.continuing(event)
          .run(
              () -> {
                assertEquals(
                    CorrelationId.of("streamrune:saga:order-saga-42"),
                    StreamRuneContext.capture().correlationId());
                StreamRuneContext.requireNoForeignReservedCorrelationId();
              });
    }
  }

  // ==== The cause of the dispatch in progress ====

  private static final SagaOrigin ORIGIN =
      SagaOrigin.dispatchedBy(SagaType.of("com.shop.OrderSaga$State"), SagaId.of("order-saga-42"));

  private static final StreamRuneContext.DispatchCause BOUND_CAUSE =
      new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), ORIGIN);

  @Test
  void cause_isNullWhenNothingIsBound() {
    assertFalse(StreamRuneContext.CAUSE.isBound());
    assertNull(StreamRuneContext.cause());
  }

  @Test
  void cause_returnsTheBoundValue() {
    ScopedValue.where(StreamRuneContext.CAUSE, BOUND_CAUSE)
        .run(() -> assertSame(BOUND_CAUSE, StreamRuneContext.cause()));
    assertNull(StreamRuneContext.cause());
  }

  @Test
  void aDispatchCauseNeedsAnEventOrAnOrigin() {
    var ex =
        assertThrows(
            IllegalArgumentException.class, () -> new StreamRuneContext.DispatchCause(null, null));
    assertEquals("a dispatch cause names a causing event, a saga origin, or both", ex.getMessage());

    var eventOnly = new StreamRuneContext.DispatchCause(CausationId.of("evt-1"), null);
    assertEquals(CausationId.of("evt-1"), eventOnly.causationId());
    assertNull(eventOnly.sagaOrigin());

    var originOnly = new StreamRuneContext.DispatchCause(null, ORIGIN);
    assertNull(originOnly.causationId());
    assertEquals(ORIGIN, originOnly.sagaOrigin());
  }

  @Test
  void wrapRunnable_carriesTheCauseToAnotherThread() throws Exception {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, UserId.of("user-1"), CorrelationId.of("corr-1"), NOW, null);
    var observedCause = new AtomicReference<StreamRuneContext.DispatchCause>();
    var observedContext = new AtomicReference<StreamRuneContext.RequestContext>();

    Runnable wrapped =
        ScopedValue.where(StreamRuneContext.CURRENT, ctx)
            .where(StreamRuneContext.CAUSE, BOUND_CAUSE)
            .call(
                () ->
                    StreamRuneContext.wrap(
                        () -> {
                          observedCause.set(StreamRuneContext.cause());
                          observedContext.set(StreamRuneContext.capture());
                        }));

    Thread thread = new Thread(wrapped);
    thread.start();
    thread.join();
    assertSame(BOUND_CAUSE, observedCause.get());
    assertSame(ctx, observedContext.get());
  }

  @Test
  void wrapCallable_carriesTheCauseToAnotherThread() throws Exception {
    var ctx =
        new StreamRuneContext.RequestContext(
            null, UserId.of("user-2"), CorrelationId.of("corr-2"), NOW, null);

    Callable<Object[]> wrapped =
        ScopedValue.where(StreamRuneContext.CURRENT, ctx)
            .where(StreamRuneContext.CAUSE, BOUND_CAUSE)
            .call(
                () ->
                    StreamRuneContext.wrap(
                        () ->
                            new Object[] {StreamRuneContext.cause(), StreamRuneContext.capture()}));

    try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
      Object[] observed = executor.submit(wrapped).get();
      assertSame(BOUND_CAUSE, observed[0]);
      assertSame(ctx, observed[1]);
    }
  }

  @Test
  void wrap_carriesACauseBoundWithoutAContext() throws Exception {
    var observedCause = new AtomicReference<StreamRuneContext.DispatchCause>();
    var contextBound = new AtomicReference<Boolean>();

    Runnable wrappedRunnable =
        ScopedValue.where(StreamRuneContext.CAUSE, BOUND_CAUSE)
            .call(
                () ->
                    StreamRuneContext.wrap(
                        () -> {
                          observedCause.set(StreamRuneContext.cause());
                          contextBound.set(StreamRuneContext.CURRENT.isBound());
                        }));
    Thread thread = new Thread(wrappedRunnable);
    thread.start();
    thread.join();
    assertSame(BOUND_CAUSE, observedCause.get());
    assertEquals(Boolean.FALSE, contextBound.get());

    Callable<Object[]> wrappedCallable =
        ScopedValue.where(StreamRuneContext.CAUSE, BOUND_CAUSE)
            .call(
                () ->
                    StreamRuneContext.wrap(
                        () ->
                            new Object[] {
                              StreamRuneContext.cause(), StreamRuneContext.CURRENT.isBound()
                            }));
    try (var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
      Object[] observed = executor.submit(wrappedCallable).get();
      assertSame(BOUND_CAUSE, observed[0]);
      assertEquals(Boolean.FALSE, observed[1]);
    }
  }

  @Test
  void wrap_carriesAContextBoundWithoutACause() throws Exception {
    var ctx =
        new StreamRuneContext.RequestContext(null, null, CorrelationId.of("corr-4"), NOW, null);
    var observedCause = new AtomicReference<Object>("unset");
    var observedContext = new AtomicReference<StreamRuneContext.RequestContext>();

    Runnable wrapped =
        ScopedValue.where(StreamRuneContext.CURRENT, ctx)
            .call(
                () ->
                    StreamRuneContext.wrap(
                        () -> {
                          observedCause.set(StreamRuneContext.cause());
                          observedContext.set(StreamRuneContext.capture());
                        }));
    Thread thread = new Thread(wrapped);
    thread.start();
    thread.join();
    assertNull(observedCause.get());
    assertSame(ctx, observedContext.get());
  }

  @Test
  void wrap_returnsTheTaskUnchangedWhenNothingIsBound() {
    Runnable runnable = () -> {};
    Callable<String> callable = () -> "x";

    assertSame(runnable, StreamRuneContext.wrap(runnable));
    assertSame(callable, StreamRuneContext.wrap(callable));
  }

  // ==== Continuing an event's flow ====

  private record ShipmentState(SagaStatus status) implements SagaState {}

  private record PaymentInitiated(String orderId) implements DomainEvent {}

  private static final SagaId SAGA_ID = SagaId.of("order-saga-42");

  private static final StreamId PAYMENT_STREAM =
      StreamId.of(AggregateType.of("payment"), AggregateId.of("pay-1"));

  /** The reply to a command the saga dispatched itself: origin at hop 0. */
  private static EventEnvelope sagaCausedEvent() {
    return SagaEnvelopes.causedBy(
        ShipmentState.class, SAGA_ID, PAYMENT_STREAM, new PaymentInitiated("o-1"));
  }

  /**
   * An event a user's request caused: a trace, a user, a correlation id of the client's choosing, a
   * baggage role, and no origin.
   */
  private static EventEnvelope requestCausedEvent() {
    return envelopeWith(
        new EventMetadata(
            EventId.of("evt-request-1"),
            CommandId.of("cmd-request-1"),
            TraceId.of("trace-7"),
            null,
            CorrelationId.of("client-corr-7"),
            CausationId.of("evt-earlier"),
            UserId.of("alice"),
            NOW,
            Map.of("role", "ADMIN", "tenant", "acme")));
  }

  private static EventEnvelope envelopeWith(EventMetadata metadata) {
    PaymentInitiated event = new PaymentInitiated("o-1");
    return new EventEnvelope(
        GlobalOffset.of(7),
        PAYMENT_STREAM,
        Version.initial(),
        EventType.fromClass(PaymentInitiated.class),
        event,
        metadata);
  }

  @Test
  void continuing_anEventWithAnOrigin_bindsItOneHopFurther() {
    EventEnvelope event = sagaCausedEvent();
    SagaOrigin stored = event.metadata().sagaOrigin();
    assertEquals(0, stored.hop());

    StreamRuneContext.continuing(event)
        .run(
            () -> {
              SagaOrigin bound = StreamRuneContext.cause().sagaOrigin();
              assertEquals(new SagaOrigin(stored.sagaType(), stored.sagaId(), 1), bound);
            });
  }

  @Test
  void continuing_anEventAlreadyAFewHopsAway_addsOneHop() {
    EventEnvelope event =
        SagaEnvelopes.causedBy(
            ShipmentState.class, SAGA_ID, 3, PAYMENT_STREAM, new PaymentInitiated("o-1"));
    SagaOrigin atHopThree = event.metadata().sagaOrigin();
    assertEquals(3, atHopThree.hop());
    assertNotNull(event.metadata().causationId());

    StreamRuneContext.continuing(event)
        .run(
            () ->
                assertEquals(
                    new SagaOrigin(atHopThree.sagaType(), atHopThree.sagaId(), 4),
                    StreamRuneContext.cause().sagaOrigin()));
  }

  @Test
  void continuing_anEventAtTheLargestHop_isRefusedAndBindsNothing() {
    EventMetadata hopZero = sagaCausedEvent().metadata();
    EventEnvelope event =
        envelopeWith(
            new EventMetadata(
                hopZero.eventId(),
                hopZero.commandId(),
                null,
                null,
                hopZero.correlationId(),
                null,
                null,
                NOW,
                Map.of(),
                new SagaOrigin(
                    hopZero.sagaOrigin().sagaType(),
                    hopZero.sagaOrigin().sagaId(),
                    Integer.MAX_VALUE)));

    var ex = assertThrows(IllegalStateException.class, () -> StreamRuneContext.continuing(event));

    assertTrue(ex.getMessage().contains("hop 2147483647"), ex.getMessage());
    assertNull(StreamRuneContext.cause());
  }

  @Test
  void continuing_anEventWithoutAnOrigin_bindsNoOrigin() {
    StreamRuneContext.continuing(requestCausedEvent())
        .run(
            () -> {
              assertNotNull(StreamRuneContext.cause());
              assertNull(StreamRuneContext.cause().sagaOrigin());
            });
  }

  @Test
  void continuing_bindsTheEventsIdAsTheCausationId() {
    EventEnvelope sagaCaused = sagaCausedEvent();
    StreamRuneContext.continuing(sagaCaused)
        .run(
            () ->
                assertEquals(
                    CausationId.of(sagaCaused.metadata().eventId().value()),
                    StreamRuneContext.cause().causationId()));

    // The event's own id, not the causation id the event itself carries.
    StreamRuneContext.continuing(requestCausedEvent())
        .run(
            () ->
                assertEquals(
                    CausationId.of("evt-request-1"), StreamRuneContext.cause().causationId()));
  }

  @Test
  void continuing_keepsTheEventsCorrelationAndTraceIds() {
    StreamRuneContext.continuing(requestCausedEvent())
        .run(
            () -> {
              StreamRuneContext.RequestContext context = StreamRuneContext.capture();
              assertEquals(CorrelationId.of("client-corr-7"), context.correlationId());
              assertEquals(TraceId.of("trace-7"), context.traceId());
            });

    EventEnvelope sagaCaused = sagaCausedEvent();
    assertEquals(
        CorrelationId.of("streamrune:saga:order-saga-42"), sagaCaused.metadata().correlationId());
    StreamRuneContext.continuing(sagaCaused)
        .run(
            () -> {
              StreamRuneContext.RequestContext context = StreamRuneContext.capture();
              assertEquals(
                  CorrelationId.of("streamrune:saga:order-saga-42"),
                  context.correlationId(),
                  "a continuation of a saga-caused event binds the reserved form it inherits");
              assertEquals(
                  StreamRuneContext.cause().sagaOrigin().correlationId(), context.correlationId());
              assertNull(context.traceId());
            });
  }

  @Test
  void continuing_bindsNoUserNoAuthorityAndNoBaggage() {
    Instant before = Instant.now();
    // Bound around the continuation: a caller with a user, a captured authority and baggage.
    var caller =
        new StreamRuneContext.RequestContext(
            TraceId.of("caller-trace"),
            UserId.of("bob"),
            CorrelationId.of("caller-corr"),
            NOW,
            Map.of("role", "OPERATOR"),
            new UserAuthority(Set.of("OPERATOR"), Set.of()));

    ScopedValue.where(StreamRuneContext.CURRENT, caller)
        .run(
            () ->
                StreamRuneContext.continuing(requestCausedEvent())
                    .run(
                        () -> {
                          StreamRuneContext.RequestContext context = StreamRuneContext.capture();
                          assertNull(context.userId(), "neither the event's user nor the caller's");
                          assertNull(context.authority());
                          assertTrue(context.baggage().isEmpty(), "a baggage role is authority");
                          assertFalse(context.timestamp().isBefore(before));
                        }));
  }

  @Test
  void continuing_insideASagaDispatch_isNotASagaDispatch() {
    StreamRuneContext.continuing(sagaCausedEvent())
        .run(() -> assertFalse(StreamRuneContext.isSagaDispatch()));

    // A listener called on the thread of a saga's dispatch: the continuation takes the saga
    // privileges away for its own commands and gives them back afterwards.
    ScopedValue.where(StreamRuneContext.SAGA_OWNED, Boolean.TRUE)
        .run(
            () -> {
              assertTrue(StreamRuneContext.isSagaDispatch());
              StreamRuneContext.continuing(sagaCausedEvent())
                  .run(
                      () -> {
                        assertFalse(StreamRuneContext.isSagaDispatch());
                        assertTrue(StreamRuneContext.SAGA_OWNED.isBound());
                        // The saga's pass does not extend to the continuation, and it has no user.
                        assertThrows(
                            AuthorizationException.class,
                            () -> Authorization.requireOwnerOrSaga(UserId.of("alice")));
                        assertNull(Authorization.currentUserId());
                      });
              assertTrue(StreamRuneContext.isSagaDispatch());
              Authorization.requireOwnerOrSaga(UserId.of("alice"));
            });
  }

  @Test
  void continuing_insideAnotherCause_bindsItsOwnEventsCause() {
    EventEnvelope event = requestCausedEvent();

    ScopedValue.where(StreamRuneContext.CAUSE, BOUND_CAUSE)
        .run(
            () -> {
              StreamRuneContext.continuing(event)
                  .run(
                      () ->
                          assertEquals(
                              new StreamRuneContext.DispatchCause(
                                  CausationId.of("evt-request-1"), null),
                              StreamRuneContext.cause()));
              assertSame(BOUND_CAUSE, StreamRuneContext.cause());
            });
  }

  @Test
  void continuing_bindsTheSameCauseEveryTimeItIsGivenTheSameEvent() {
    EventEnvelope event = sagaCausedEvent();
    var first = new AtomicReference<StreamRuneContext.DispatchCause>();
    var second = new AtomicReference<StreamRuneContext.DispatchCause>();

    StreamRuneContext.continuing(event).run(() -> first.set(StreamRuneContext.cause()));
    StreamRuneContext.continuing(event).run(() -> second.set(StreamRuneContext.cause()));

    assertEquals(first.get(), second.get());
  }

  @Test
  void continuing_leavesNothingBoundAfterwards() {
    StreamRuneContext.continuing(sagaCausedEvent()).run(() -> {});

    assertNull(StreamRuneContext.cause());
    assertNull(StreamRuneContext.capture());
    assertFalse(StreamRuneContext.SAGA_OWNED.isBound());
  }

  @Test
  void continuing_refusesANullEvent() {
    var ex = assertThrows(NullPointerException.class, () -> StreamRuneContext.continuing(null));
    assertEquals("cause is required", ex.getMessage());
  }

  @Test
  void wrap_insideContinuing_keepsTheCause() throws Exception {
    EventEnvelope event = sagaCausedEvent();
    var observedCause = new AtomicReference<StreamRuneContext.DispatchCause>();
    var observedContext = new AtomicReference<StreamRuneContext.RequestContext>();

    Runnable wrapped =
        StreamRuneContext.continuing(event)
            .call(
                () ->
                    StreamRuneContext.wrap(
                        () -> {
                          observedCause.set(StreamRuneContext.cause());
                          observedContext.set(StreamRuneContext.capture());
                        }));
    Thread thread = new Thread(wrapped);
    thread.start();
    thread.join();

    assertEquals(
        new StreamRuneContext.DispatchCause(
            CausationId.of(event.metadata().eventId().value()),
            new SagaOrigin(SagaType.fromClass(ShipmentState.class), SAGA_ID, 1)),
        observedCause.get());
    assertEquals(event.metadata().correlationId(), observedContext.get().correlationId());
    assertNull(observedContext.get().userId());
  }

  // ---------------------------------------------------------------------------------------------
  // The forward command of a saga in dispatch
  // ---------------------------------------------------------------------------------------------

  private static final IdempotencyKey FORWARD_KEY = IdempotencyKey.of("saga:fwd-1:cmd:0");

  @Test
  void sagaForwardOf_isNullWhenNothingIsBound() {
    assertFalse(StreamRuneContext.SAGA_FORWARD.isBound());
    assertNull(StreamRuneContext.sagaForwardOf(FORWARD_KEY));
  }

  @Test
  void sagaForwardOf_answersOnlyForTheBoundKey() {
    ScopedValue.where(
            StreamRuneContext.SAGA_FORWARD, new StreamRuneContext.SagaForward(ORIGIN, FORWARD_KEY))
        .run(
            () -> {
              assertSame(ORIGIN, StreamRuneContext.sagaForwardOf(FORWARD_KEY));
              assertSame(
                  ORIGIN,
                  StreamRuneContext.sagaForwardOf(IdempotencyKey.of(FORWARD_KEY.value())),
                  "keys compare by value");
              assertNull(
                  StreamRuneContext.sagaForwardOf(IdempotencyKey.of("nested-command-key")),
                  "a nested keyed command does not inherit the fence of the forward command");
            });
    ScopedValue.where(StreamRuneContext.SAGA_FORWARD, null)
        .run(() -> assertNull(StreamRuneContext.sagaForwardOf(FORWARD_KEY)));
  }

  @Test
  void sagaForwardOf_isNullForANullKey() {
    ScopedValue.where(
            StreamRuneContext.SAGA_FORWARD, new StreamRuneContext.SagaForward(ORIGIN, FORWARD_KEY))
        .run(() -> assertNull(StreamRuneContext.sagaForwardOf(null)));
  }

  @Test
  void aSagaForwardNeedsAnOriginAndAKey() {
    var noOrigin =
        assertThrows(
            NullPointerException.class, () -> new StreamRuneContext.SagaForward(null, FORWARD_KEY));
    assertEquals("origin is required", noOrigin.getMessage());
    var noKey =
        assertThrows(
            NullPointerException.class, () -> new StreamRuneContext.SagaForward(ORIGIN, null));
    assertEquals("key is required", noKey.getMessage());
  }

  @Test
  void wrap_doesNotCarryTheSagaForward() throws Exception {
    AtomicReference<Object> observed = new AtomicReference<>("unset");
    Runnable wrapped =
        ScopedValue.where(StreamRuneContext.CAUSE, BOUND_CAUSE)
            .where(
                StreamRuneContext.SAGA_FORWARD,
                new StreamRuneContext.SagaForward(ORIGIN, FORWARD_KEY))
            .call(
                () ->
                    StreamRuneContext.wrap(
                        () -> observed.set(StreamRuneContext.sagaForwardOf(FORWARD_KEY))));
    Thread thread = new Thread(wrapped);
    thread.start();
    thread.join();

    assertNull(observed.get(), "the marker names one dispatch on one thread");
  }
}
