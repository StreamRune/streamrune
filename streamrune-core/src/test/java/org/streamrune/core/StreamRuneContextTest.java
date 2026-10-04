package org.streamrune.core;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.TraceId;
import org.streamrune.core.types.UserId;

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
}
