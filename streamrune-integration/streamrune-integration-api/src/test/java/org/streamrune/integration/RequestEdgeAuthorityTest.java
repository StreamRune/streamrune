package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.StreamRuneContext.RequestContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.UserId;

/**
 * Off-request authorization — the shared request-edge capture the three integrations call, so all
 * three behave identically instead of each hand-rolling the rule.
 */
class RequestEdgeAuthorityTest {

  private static final UserId USER = UserId.of("user-1");
  private static final UserAuthority ADMIN = new UserAuthority(Set.of("ADMIN"), Set.of());

  /** Any capture failure in a test that is not about failures is a bug in the test's premise. */
  private static final Consumer<RuntimeException> FAIL_ON_ERROR =
      e -> {
        throw new AssertionError("unexpected capture failure", e);
      };

  private static RequestContext contextFor(UserId userId) {
    return new RequestContext(null, userId, CorrelationId.of("corr-1"), Instant.now(), Map.of());
  }

  private static UserRoleResolver resolver(
      boolean requestScoped, UserAuthority answer, AtomicInteger calls) {
    return new UserRoleResolver() {
      @Override
      public UserAuthority resolve(UserId userId) {
        calls.incrementAndGet();
        return answer;
      }

      @Override
      public boolean requiresRequestContext() {
        return requestScoped;
      }
    };
  }

  @Test
  void capturesForARequestScopedResolver() {
    var calls = new AtomicInteger();
    var captured =
        RequestEdgeAuthority.capture(contextFor(USER), resolver(true, ADMIN, calls), FAIL_ON_ERROR);
    assertEquals(ADMIN, captured.authority());
    assertEquals(1, calls.get());
  }

  /**
   * Capture ordering — the repro. {@link UserRoleResolver#requiresRequestContext()} is documented
   * as "I read ambient per-request state", and {@link StreamRuneContext#CURRENT} is the framework's
   * own per-request state — the one piece of it that is framework-portable and therefore the
   * obvious thing such a resolver reads. The capture is the moment that predicate exists to serve,
   * so the context has to be bound while it runs; otherwise declaring {@code true} makes a working
   * resolver stop working, which is the opposite of what the declaration is for.
   */
  @Test
  void aResolverThatReadsTheRequestContextObservesItDuringTheCapture() {
    var ctx =
        new RequestContext(
            null, USER, CorrelationId.of("corr-1"), Instant.now(), Map.of("role", "ADMIN"));
    var seen = new AtomicReference<RequestContext>();
    UserRoleResolver readsTheRequest =
        new UserRoleResolver() {
          @Override
          public UserAuthority resolve(UserId userId) {
            RequestContext bound = StreamRuneContext.capture();
            seen.set(bound);
            if (bound == null) {
              return UserAuthority.EMPTY;
            }
            String role = bound.baggage().get("role");
            return role == null ? UserAuthority.EMPTY : new UserAuthority(Set.of(role), Set.of());
          }

          @Override
          public boolean requiresRequestContext() {
            return true;
          }
        };

    var captured = RequestEdgeAuthority.capture(ctx, readsTheRequest, FAIL_ON_ERROR);

    assertSame(
        ctx,
        seen.get(),
        "the resolver must observe the very context being built for this request — not null, and"
            + " not some other request's context");
    assertEquals(
        ADMIN,
        captured.authority(),
        "a resolver declaring requiresRequestContext() must be able to answer from the request"
            + " context; capturing before CURRENT is bound stores EMPTY, which the interceptor then"
            + " ENFORCES, denying a caller the request plainly authorizes");
  }

  /**
   * The binding must not outlive the capture: {@code RequestEdgeAuthority} exists to hand a value
   * back to the filter, which then binds the context around the chain itself. A binding that leaked
   * out of {@code capture} would shadow the filter's own (authority-carrying) one.
   */
  @Test
  void theCaptureBindingDoesNotOutliveTheResolver() {
    var calls = new AtomicInteger();
    RequestEdgeAuthority.capture(contextFor(USER), resolver(true, ADMIN, calls), FAIL_ON_ERROR);
    assertNull(StreamRuneContext.capture(), "the capture must leave no binding behind");
  }

  @Test
  void doesNotCallAResolverThatAlreadyWorksOffRequest() {
    // A pure-function resolver needs no capture and may be expensive (a database or LDAP lookup);
    // running it on every inbound request would be a tax for a feature it does not need.
    var calls = new AtomicInteger();
    var ctx = contextFor(USER);
    var captured = RequestEdgeAuthority.capture(ctx, resolver(false, ADMIN, calls), FAIL_ON_ERROR);
    assertSame(ctx, captured);
    assertNull(captured.authority());
    assertEquals(0, calls.get());
  }

  @Test
  void doesNotCaptureForAnAnonymousRequest() {
    var calls = new AtomicInteger();
    var ctx = contextFor(null);
    assertSame(ctx, RequestEdgeAuthority.capture(ctx, resolver(true, ADMIN, calls), FAIL_ON_ERROR));
    assertEquals(0, calls.get());
  }

  @Test
  void isANoOpWhenNoResolverIsWired() {
    var ctx = contextFor(USER);
    assertSame(ctx, RequestEdgeAuthority.capture(ctx, null, FAIL_ON_ERROR));
    assertNull(RequestEdgeAuthority.capture(null, null, FAIL_ON_ERROR));
  }

  @Test
  void aThrowingResolverDoesNotFailTheRequestButIsReported() {
    // The request must survive; authorization then resolves at command time exactly as it did
    // before the capture existed, and the same failure surfaces with the command it belongs to.
    UserRoleResolver boom =
        new UserRoleResolver() {
          @Override
          public UserAuthority resolve(UserId userId) {
            throw new IllegalStateException("security backend down");
          }

          @Override
          public boolean requiresRequestContext() {
            return true;
          }
        };
    var ctx = contextFor(USER);
    var reported = new AtomicReference<RuntimeException>();
    var captured = RequestEdgeAuthority.capture(ctx, boom, reported::set);
    assertSame(ctx, captured);
    assertNull(captured.authority());
    assertEquals(
        "security backend down",
        reported.get().getMessage(),
        "a swallowed capture failure must still reach the integration's logger");
  }
}
