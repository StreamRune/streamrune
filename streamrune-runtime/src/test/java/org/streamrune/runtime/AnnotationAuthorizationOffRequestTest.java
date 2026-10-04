package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.RequireRole;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * The off-request authorization rule — the decision ladder {@link
 * AnnotationAuthorizationInterceptor} applies when a command executes off the request thread.
 *
 * <p>The two failure modes have opposite risks, so every branch is pinned in both directions: an
 * authorized caller must not be denied off-request (an availability failure that silently loses
 * dead-lettered commands), and an unauthorized one must not be waved through off-request (an
 * authorization bypass). The end-to-end proof that the real Spring container, filter, bus and retry
 * runner behave this way lives in {@code org.streamrune.spring.OffRequestAuthorizationTest}; this
 * class pins the rules themselves.
 */
class AnnotationAuthorizationOffRequestTest {

  private static final AggregateType TYPE = AggregateType.of("order");

  private static final UserId ADMIN = UserId.of("admin-42");

  @RequireRole("ADMIN")
  record AdminCommand() implements Command {}

  /** Counts calls so a test can prove the resolver was, or was not, consulted. */
  private static final class CountingResolver implements UserRoleResolver {
    private final UserAuthority answer;
    private final boolean requestScoped;
    final AtomicInteger calls = new AtomicInteger();

    CountingResolver(UserAuthority answer, boolean requestScoped) {
      this.answer = answer;
      this.requestScoped = requestScoped;
    }

    @Override
    public UserAuthority resolve(UserId userId) {
      calls.incrementAndGet();
      return answer;
    }

    @Override
    public boolean requiresRequestContext() {
      return requestScoped;
    }
  }

  private static final UserAuthority ADMIN_AUTHORITY = new UserAuthority(Set.of("ADMIN"), Set.of());

  /** What every shipped resolver answers once the request thread is gone. */
  private static CountingResolver requestScopedResolverOffRequest() {
    return new CountingResolver(UserAuthority.EMPTY, true);
  }

  /** A resolver that is a pure function of the user id — it answers the same on any thread. */
  private static CountingResolver pureResolver(UserAuthority answer) {
    return new CountingResolver(answer, false);
  }

  private static CommandContext ctxFor(Command command) {
    return new CommandContext(
        command,
        command.getClass().getSimpleName(),
        CommandId.of("cmd-1"),
        TYPE,
        AggregateId.of("agg-1"),
        null,
        Instant.now());
  }

  private static StreamRuneContext.RequestContext context(UserId userId, UserAuthority authority) {
    return new StreamRuneContext.RequestContext(
        null, userId, CorrelationId.of("corr-1"), Instant.now(), null, authority);
  }

  private static void withContext(StreamRuneContext.RequestContext ctx, Runnable action) {
    ScopedValue.where(StreamRuneContext.CURRENT, ctx).run(action);
  }

  private static void withDlqReplay(StreamRuneContext.RequestContext ctx, Runnable action) {
    ScopedValue.where(VirtualThreadCommandBus.DLQ_REPLAY, Boolean.TRUE)
        .where(StreamRuneContext.CURRENT, ctx)
        .run(action);
  }

  // ---- captured authority (the executeAsync path) ---------------------------------------------

  @Test
  void capturedAuthorityIsUsedWhenTheResolverCannotAnswerOffRequest() {
    var resolver = requestScopedResolverOffRequest();
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    withContext(
        context(ADMIN, ADMIN_AUTHORITY),
        () -> assertTrue(interceptor.before(ctxFor(new AdminCommand()))));
    assertEquals(
        0,
        resolver.calls.get(),
        "the resolver must not be consulted off-request when the request edge already answered");
  }

  @Test
  void capturedAuthorityIsEnforcedNotBlindlyTrusted() {
    // The capture carries the caller's REAL authorities. Carrying one is not permission to run —
    // it is the same check, made where it could be made.
    var interceptor = new AnnotationAuthorizationInterceptor(requestScopedResolverOffRequest());
    withContext(
        context(ADMIN, new UserAuthority(Set.of("VIEWER"), Set.of())),
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new AdminCommand()))));
  }

  @Test
  void aDenialByARequestScopedResolverNamesTheOffRequestCause() {
    var interceptor = new AnnotationAuthorizationInterceptor(requestScopedResolverOffRequest());
    withContext(
        context(ADMIN, null),
        () -> {
          var thrown =
              assertThrows(
                  AuthorizationException.class,
                  () -> interceptor.before(ctxFor(new AdminCommand())));
          assertTrue(
              thrown.getMessage().contains("requires a request context"),
              "an operator must be able to tell 'the resolver could not see a request' apart from"
                  + " 'this user has no roles' — both look like EMPTY: "
                  + thrown.getMessage());
        });
  }

  @Test
  void aDenialByAPureResolverCarriesNoOffRequestHint() {
    var interceptor = new AnnotationAuthorizationInterceptor(pureResolver(UserAuthority.EMPTY));
    withContext(
        context(ADMIN, null),
        () -> {
          var thrown =
              assertThrows(
                  AuthorizationException.class,
                  () -> interceptor.before(ctxFor(new AdminCommand())));
          assertEquals("Required role: ADMIN", thrown.getMessage());
        });
  }

  // ---- DLQ replay ------------------------------------------------------------------------------

  @Test
  void dlqReplayUnderARequestScopedResolverRunsAsTheSystemPrincipal() {
    var resolver = requestScopedResolverOffRequest();
    var interceptor = new AnnotationAuthorizationInterceptor(resolver);
    withDlqReplay(
        context(ADMIN, null), () -> assertTrue(interceptor.before(ctxFor(new AdminCommand()))));
    assertEquals(
        0,
        resolver.calls.get(),
        "a resolver that provably cannot answer must not be asked and then obeyed");
  }

  @Test
  void dlqReplayUnderAPureResolverIsStillCheckedAsTheUser() {
    // THE anti-bypass case. The system principal exists because a request-scoped resolver cannot
    // answer — not because DLQ replays are exempt. A resolver that CAN answer is still consulted,
    // so a user whose role was revoked while the entry sat in the queue is denied on replay.
    var revoked = pureResolver(UserAuthority.EMPTY);
    var interceptor = new AnnotationAuthorizationInterceptor(revoked);
    withDlqReplay(
        context(ADMIN, null),
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new AdminCommand()))));
    assertEquals(1, revoked.calls.get(), "a resolver that can answer must be asked");
  }

  @Test
  void dlqReplayUnderAPureResolverPassesForAStillAuthorizedUser() {
    var interceptor = new AnnotationAuthorizationInterceptor(pureResolver(ADMIN_AUTHORITY));
    withDlqReplay(
        context(ADMIN, null), () -> assertTrue(interceptor.before(ctxFor(new AdminCommand()))));
  }

  @Test
  void dlqReplayIsDeniedWhenTheSystemPrincipalIsDisabled() {
    var interceptor =
        new AnnotationAuthorizationInterceptor(requestScopedResolverOffRequest(), true, false);
    withDlqReplay(
        context(ADMIN, null),
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new AdminCommand()))));
  }

  @Test
  void dlqReplayWithNoPersistedUserIsStillDeniedFailClosed() {
    // An entry with no persisted user means the original command ran with no bound identity. The
    // system principal is scoped to "the resolver cannot answer for THIS user", never to "no user
    // is needed" — so this stays a hard denial exactly as before.
    var interceptor = new AnnotationAuthorizationInterceptor(requestScopedResolverOffRequest());
    withDlqReplay(
        context(null, null),
        () -> {
          var thrown =
              assertThrows(
                  AuthorizationException.class,
                  () -> interceptor.before(ctxFor(new AdminCommand())));
          assertEquals("Authentication required", thrown.getMessage());
        });
  }

  @Test
  void anOrdinaryOffRequestExecutionIsNotTreatedAsADlqReplay() {
    // Only DLQ_REPLAY unlocks the system principal. The same request-scoped resolver on a plain
    // background thread (no marker, no capture) still fails closed.
    var interceptor = new AnnotationAuthorizationInterceptor(requestScopedResolverOffRequest());
    withContext(
        context(ADMIN, null),
        () ->
            assertThrows(
                AuthorizationException.class,
                () -> interceptor.before(ctxFor(new AdminCommand()))));
  }

  @Test
  void noContextAtAllIsStillDenied() {
    var interceptor = new AnnotationAuthorizationInterceptor(pureResolver(ADMIN_AUTHORITY));
    var thrown =
        assertThrows(
            AuthorizationException.class, () -> interceptor.before(ctxFor(new AdminCommand())));
    assertEquals("Authentication required", thrown.getMessage());
  }

  // ---- resolver capability plumbing ------------------------------------------------------------

  @Test
  void aLambdaResolverDefaultsToBeingUsableOffRequest() {
    UserRoleResolver lambda = userId -> ADMIN_AUTHORITY;
    assertFalse(
        lambda.requiresRequestContext(),
        "the default must keep every existing user-supplied resolver behaving exactly as before");
  }

  @Test
  void cachingResolverReportsItsDelegatesRequestScoping() {
    // Hiding a request-scoped delegate behind the default would let the framework consult the cache
    // on a background thread, where a miss resolves to EMPTY and then caches that empty answer
    // against the user for the whole TTL — denying them on the request thread too.
    assertTrue(
        new CachingUserRoleResolver(requestScopedResolverOffRequest()).requiresRequestContext());
    assertFalse(
        new CachingUserRoleResolver(pureResolver(ADMIN_AUTHORITY)).requiresRequestContext());
  }
}
