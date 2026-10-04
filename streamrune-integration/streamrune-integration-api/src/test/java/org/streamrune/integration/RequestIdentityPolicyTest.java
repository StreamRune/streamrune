package org.streamrune.integration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.streamrune.core.types.UserId;
import org.streamrune.integration.RequestIdentityPolicy.Mode;

/**
 * The one rule every HTTP integration derives {@code RequestContext.userId} by. The header is an
 * identity only in the explicit trusted-gateway mode; without a resolver and without the flag the
 * request is anonymous.
 */
class RequestIdentityPolicyTest {

  private static final AuthenticatedUserResolver BOB = () -> Optional.of(UserId.of("bob"));
  private static final AuthenticatedUserResolver UNAUTHENTICATED = Optional::empty;
  private static final RequestIdentityPolicy.MismatchListener NO_MISMATCH_EXPECTED =
      (header, principal) -> {
        throw new AssertionError("no mismatch expected: " + header + " vs " + principal);
      };

  // ---- mode selection ---------------------------------------------------------------------------

  @Test
  void neitherAResolverNorTheTrustFlagIsAnonymous() {
    assertEquals(Mode.ANONYMOUS, RequestIdentityPolicy.of(null, false).mode());
  }

  @Test
  void aResolverWithoutTheTrustFlagIsTheAuthenticatedPrincipal() {
    assertEquals(Mode.AUTHENTICATED_PRINCIPAL, RequestIdentityPolicy.of(BOB, false).mode());
  }

  @Test
  void theTrustFlagIsTheTrustedGatewayWithOrWithoutAResolver() {
    assertEquals(Mode.TRUSTED_GATEWAY, RequestIdentityPolicy.of(null, true).mode());
    assertEquals(Mode.TRUSTED_GATEWAY, RequestIdentityPolicy.of(BOB, true).mode());
  }

  // ---- resolution -------------------------------------------------------------------------------

  @Test
  void anonymousIgnoresTheHeaderAndReportsNothing() {
    var policy = RequestIdentityPolicy.of(null, false);
    assertNull(policy.resolve(List.of("alice"), NO_MISMATCH_EXPECTED));
    assertNull(policy.resolve(null, NO_MISMATCH_EXPECTED));
  }

  @Test
  void theTrustedGatewayTakesTheHeader() {
    var policy = RequestIdentityPolicy.of(BOB, true);
    assertEquals(UserId.of("alice"), policy.resolve(List.of("alice"), NO_MISMATCH_EXPECTED));
  }

  @Test
  void aTrustedGatewayRequestWithoutAHeaderIsAnonymous() {
    var policy = RequestIdentityPolicy.of(null, true);
    assertNull(policy.resolve(null, NO_MISMATCH_EXPECTED));
    assertNull(policy.resolve(List.of("  "), NO_MISMATCH_EXPECTED));
  }

  @Test
  void aRepeatedHeaderIsAnonymousBehindATrustedGateway() {
    // A gateway that appends its own value after the client's instead of replacing it must not
    // let either value — let alone a comma-joined view of both — become the identity.
    var policy = RequestIdentityPolicy.of(null, true);
    assertNull(policy.resolve(List.of("victim", "real"), NO_MISMATCH_EXPECTED));
    assertNull(policy.resolve(List.of("alice", "alice"), NO_MISMATCH_EXPECTED));
    assertNull(policy.resolve(List.of(), NO_MISMATCH_EXPECTED));
  }

  @Test
  void aSingleValueContainingACommaIsOneValue() {
    var policy = RequestIdentityPolicy.of(null, true);
    assertEquals(UserId.of("a,b"), policy.resolve(List.of("a,b"), NO_MISMATCH_EXPECTED));
  }

  @Test
  void aRepeatedHeaderNeverDisplacesThePrincipalAndEachDisagreeingValueIsReportedOnce() {
    var policy = RequestIdentityPolicy.of(BOB, false);
    List<String> reported = new ArrayList<>();
    UserId resolved =
        policy.resolve(
            List.of("alice", "bob", " ", "alice", "carol"),
            (header, principal) -> reported.add(header.value() + "->" + principal.value()));
    assertEquals(UserId.of("bob"), resolved);
    assertEquals(List.of("alice->bob", "carol->bob"), reported);
  }

  @Test
  void thePrincipalWinsOverAMismatchingHeaderAndTheMismatchIsReported() {
    var policy = RequestIdentityPolicy.of(BOB, false);
    List<String> reported = new ArrayList<>();
    UserId resolved =
        policy.resolve(
            List.of("alice"),
            (header, principal) -> reported.add(header.value() + "->" + principal.value()));
    assertEquals(UserId.of("bob"), resolved);
    assertEquals(List.of("alice->bob"), reported);
  }

  @Test
  void aMatchingOrAbsentHeaderIsNotAMismatch() {
    var policy = RequestIdentityPolicy.of(BOB, false);
    assertEquals(UserId.of("bob"), policy.resolve(List.of("bob"), NO_MISMATCH_EXPECTED));
    assertEquals(UserId.of("bob"), policy.resolve(null, NO_MISMATCH_EXPECTED));
    assertEquals(UserId.of("bob"), policy.resolve(List.of(""), NO_MISMATCH_EXPECTED));
  }

  @Test
  void anUnauthenticatedRequestIsAnonymousWhateverTheHeaderSays() {
    var policy = RequestIdentityPolicy.of(UNAUTHENTICATED, false);
    assertNull(policy.resolve(List.of("alice"), NO_MISMATCH_EXPECTED));
  }

  @Test
  void theResolverIsAskedOncePerRequestAndNeverInTheTrustedGatewayMode() {
    var calls = new AtomicInteger();
    AuthenticatedUserResolver counting =
        () -> {
          calls.incrementAndGet();
          return Optional.of(UserId.of("bob"));
        };
    RequestIdentityPolicy.of(counting, false).resolve(List.of("alice"), (h, p) -> {});
    assertEquals(1, calls.get());
    RequestIdentityPolicy.of(counting, true).resolve(List.of("alice"), NO_MISMATCH_EXPECTED);
    assertEquals(1, calls.get());
  }

  @Test
  void aNullMismatchListenerIsRejected() {
    var policy = RequestIdentityPolicy.of(BOB, false);
    assertThrows(IllegalArgumentException.class, () -> policy.resolve(List.of("alice"), null));
  }

  // ---- startup rule -----------------------------------------------------------------------------

  @Test
  void authorizationWithoutAnIdentitySourceRefusesStartupNamingBothRemedies() {
    var policy = RequestIdentityPolicy.of(null, false);
    var refused =
        assertThrows(
            IllegalStateException.class,
            () ->
                policy.requireIdentitySourceFor(
                    List.of(
                        "AuthorizationCommandInterceptor", "AnnotationAuthorizationInterceptor")));
    String message = refused.getMessage();
    assertTrue(
        message.contains("AuthorizationCommandInterceptor, AnnotationAuthorizationInterceptor"),
        message);
    assertTrue(message.contains("AuthenticatedUserResolver"), message);
    assertTrue(
        message.contains(RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY + "=true"), message);
    assertTrue(message.contains("anonymous"), message);
  }

  @Test
  void anIdentitySourceOrNoAuthorizationPassesTheStartupRule() {
    assertDoesNotThrow(
        () -> RequestIdentityPolicy.of(null, false).requireIdentitySourceFor(List.of()));
    assertDoesNotThrow(
        () ->
            RequestIdentityPolicy.of(null, true)
                .requireIdentitySourceFor(List.of("AuthorizationCommandInterceptor")));
    assertDoesNotThrow(
        () ->
            RequestIdentityPolicy.of(UNAUTHENTICATED, false)
                .requireIdentitySourceFor(List.of("AuthorizationCommandInterceptor")));
  }

  @Test
  void aNullComponentListIsRejected() {
    var policy = RequestIdentityPolicy.of(null, false);
    assertThrows(IllegalArgumentException.class, () -> policy.requireIdentitySourceFor(null));
  }

  // ---- the role rule --------------------------------------------------------------------

  private static final String CAUSATION = "streamrune.causation-id";

  @Test
  void theTrustedGatewayTakesTheRoleFromASingleRoleHeader() {
    var policy = RequestIdentityPolicy.of(null, true);

    assertEquals(
        Map.of(CAUSATION, "evt-1", "role", "ADMIN"),
        policy.applyRole(Map.of(CAUSATION, "evt-1"), List.of("ADMIN")));
  }

  @Test
  void outsideTheTrustedGatewayTheRoleHeaderIsIgnored() {
    for (var policy :
        List.of(
            RequestIdentityPolicy.of(null, false),
            RequestIdentityPolicy.of(BOB, false),
            RequestIdentityPolicy.of(UNAUTHENTICATED, false))) {
      assertEquals(
          Map.of(CAUSATION, "evt-1"),
          policy.applyRole(Map.of(CAUSATION, "evt-1"), List.of("ADMIN")),
          policy.mode().name());
    }
  }

  @Test
  void aRoleEntryTheBaggageAlreadyCarriedIsNeverKept() {
    // A W3C `baggage: role=` entry (or an application that allow-listed `role`) must not reach the
    // key in any mode: behind a trusted gateway the header is the only source.
    Map<String, String> smuggled = Map.of("role", "ADMIN", CAUSATION, "evt-1");
    for (var policy :
        List.of(
            RequestIdentityPolicy.of(null, false),
            RequestIdentityPolicy.of(BOB, false),
            RequestIdentityPolicy.of(null, true))) {
      assertEquals(
          Map.of(CAUSATION, "evt-1"), policy.applyRole(smuggled, List.of()), policy.mode().name());
    }
    assertEquals(
        Map.of(CAUSATION, "evt-1", "role", "CUSTOMER"),
        RequestIdentityPolicy.of(null, true).applyRole(smuggled, List.of("CUSTOMER")));
  }

  @Test
  void aRepeatedOrBlankRoleHeaderIsNoRoleBehindATrustedGateway() {
    var policy = RequestIdentityPolicy.of(null, true);

    assertEquals(Map.of(), policy.applyRole(Map.of(), List.of("CUSTOMER", "ADMIN")));
    assertEquals(Map.of(), policy.applyRole(Map.of(), List.of("ADMIN", "ADMIN")));
    assertEquals(Map.of(), policy.applyRole(Map.of(), List.of("  ")));
    assertEquals(Map.of(), policy.applyRole(Map.of(), List.of()));
  }

  @Test
  void aSingleRoleValueContainingACommaIsOneValue() {
    assertEquals(
        Map.of("role", "ADMIN,CUSTOMER"),
        RequestIdentityPolicy.of(null, true).applyRole(Map.of(), List.of("ADMIN,CUSTOMER")));
  }

  @Test
  void theRoleRuleNeverConsultsTheAuthenticatedUserResolver() {
    // The principal mode does not lend the principal's roles to baggage `role`: the verified
    // authority is the request context's captured authority, never metadata baggage.
    AuthenticatedUserResolver mustNotBeAsked =
        () -> {
          throw new AssertionError("the role rule must not consult the resolver");
        };

    assertEquals(
        Map.of(), RequestIdentityPolicy.of(mustNotBeAsked, false).applyRole(null, List.of("A")));
    assertEquals(
        Map.of("role", "A"),
        RequestIdentityPolicy.of(mustNotBeAsked, true).applyRole(null, List.of("A")));
  }

  @Test
  void applyRoleTreatsNullsAsEmptyAndReturnsAnImmutableMap() {
    var applied = RequestIdentityPolicy.of(null, true).applyRole(null, null);

    assertEquals(Map.of(), applied);
    assertThrows(UnsupportedOperationException.class, () -> applied.put("role", "ADMIN"));
  }

  // ---- startup description ----------------------------------------------------------------------

  @Test
  void eachModeDescribesItselfForTheStartupLog() {
    String anonymous = RequestIdentityPolicy.of(null, false).describe();
    assertTrue(anonymous.contains("ANONYMOUS"), anonymous);
    assertTrue(anonymous.contains("X-User-Id header is ignored"), anonymous);
    assertTrue(anonymous.contains("X-User-Role header is ignored too"), anonymous);

    String principal = RequestIdentityPolicy.of(BOB, false).describe();
    assertTrue(principal.contains("AUTHENTICATED_PRINCIPAL"), principal);
    assertTrue(principal.contains("X-User-Id header is ignored"), principal);
    assertTrue(principal.contains("X-User-Role header is ignored too"), principal);

    String gateway = RequestIdentityPolicy.of(null, true).describe();
    assertTrue(gateway.contains("TRUSTED_GATEWAY"), gateway);
    assertTrue(gateway.contains("authenticate every caller"), gateway);
    assertTrue(gateway.contains("overwrite X-User-Id"), gateway);
    assertTrue(gateway.contains("strip any client-supplied value"), gateway);
    assertTrue(gateway.contains("more than one X-User-Id value is anonymous"), gateway);
    assertTrue(gateway.contains("X-User-Role header is baggage role"), gateway);
    assertTrue(gateway.contains("overwrite or strip X-User-Role"), gateway);
    assertFalse(gateway.contains("AuthenticatedUserResolver"), gateway);
  }

  @Test
  void theTrustedGatewayDescriptionNamesAResolverTheHeaderOverrides() {
    // The flag plus an authenticated principal is the risky combination: an authenticated user
    // who can reach the service directly acts as whoever the header names. Say so at startup.
    String gateway = RequestIdentityPolicy.of(new NamedResolver(), true).describe();
    assertTrue(gateway.contains(NamedResolver.class.getName()), gateway);
    assertTrue(gateway.contains("NOT consulted"), gateway);
    assertTrue(gateway.contains("overrides any authenticated principal"), gateway);
  }

  private static final class NamedResolver implements AuthenticatedUserResolver {
    @Override
    public Optional<UserId> currentUser() {
      throw new AssertionError("the trusted-gateway mode must not consult the resolver");
    }
  }

  @Test
  void theHeaderAndPropertyNamesAreTheOnesEveryIntegrationReads() {
    assertEquals("X-User-Id", RequestIdentityPolicy.USER_ID_HEADER);
    assertEquals("X-User-Role", RequestIdentityPolicy.USER_ROLE_HEADER);
    assertEquals(
        "streamrune.security.trust-user-id-header",
        RequestIdentityPolicy.TRUST_USER_ID_HEADER_PROPERTY);
  }
}
