# Authorization

## When to use

Use `CommandAuthorizationPolicy` when you need to enforce who can execute which commands.

Use it for coarse-grained authorization based on command type and user identity. For fine-grained resource-level checks, load aggregate state first and validate in the decider.

Do **not** use it for authentication — StreamRune assumes the HTTP layer has already authenticated the user and populated `StreamRuneContext.CURRENT` with a `UserId`.

Do **not** use `CommandAuthorizationPolicy.allowAll()` in production — it is a test utility.

## How it works

`CommandAuthorizationPolicy` is a `@FunctionalInterface` with `void authorize(UserId userId, Command command)`.

`AuthorizationCommandInterceptor` is a `CommandInterceptor` that calls the policy in `before(ctx)` before every command reaches the decider.

The `UserId` is read from `StreamRuneContext.CURRENT` (a `ScopedValue<RequestContext>`). If unbound (anonymous request), `userId` is `null`.

### Where the identity comes from (security)

`RequestContext.userId` is the identity every authorization check trusts — `CommandAuthorizationPolicy`, `@RequireRole`/`@RequirePermission` and `Authorization.requireOwner(...)` — and the user id the command audit records. The HTTP integrations derive it from each request with one rule, `RequestIdentityPolicy`, which **fails closed**: the client-supplied `X-User-Id` header is an identity only when you say so explicitly. The mode is fixed at startup by two inputs — whether an `AuthenticatedUserResolver` is available, and `streamrune.security.trust-user-id-header`:

| Mode | When | `RequestContext.userId` | `X-User-Id` header | `X-User-Role` header |
|---|---|---|---|---|
| **Authenticated principal** | an `AuthenticatedUserResolver` is available (Spring Security on the classpath, the Quarkus/Micronaut security integration, or your own bean) and the flag is `false` | the authenticated principal; `null` for an unauthenticated request | ignored; a header naming a different user is logged (sanitized) | ignored; no baggage `role` |
| **Trusted gateway** | `streamrune.security.trust-user-id-header=true` (with or without a resolver) | the header value; `null` when the request carries `X-User-Id` more than once | **the identity** | **baggage `role`**; none when the request carries it more than once |
| **Anonymous** | neither | always `null` | ignored, nothing logged per request | ignored; no baggage `role` |

The integration logs the mode once at startup. More than one `AuthenticatedUserResolver` bean (without a primary one) fails startup on every integration rather than leaving the mode to chance.

**Trusted-gateway mode** is for a service that sits behind a gateway which authenticates the caller and forwards the user. Turn the flag on only when **all** of these hold, because the framework cannot check them:

- the gateway authenticates every request it forwards;
- the gateway **overwrites** `X-User-Id` with the authenticated id, and **strips** any `X-User-Id` a client sent (including on unauthenticated routes);
- the gateway does the same for `X-User-Role`: it forwards the caller's role or strips the header, never a client's value (the header becomes baggage `role`, see below);
- the service is reachable only through the gateway (no direct port, no second ingress).

If any of them fails, any client can act as any user.

**A repeated `X-User-Id` is anonymous.** Every entry point — the request filter and the SSE endpoint, on all three integrations — hands the policy every `X-User-Id` value the request carried, as received (one value that contains a comma stays one value). In trusted-gateway mode a request with more than one value resolves to `null`, so a gateway that appends its value after the client's instead of replacing it fails closed instead of letting whichever value an API happens to return first become the identity. In authenticated-principal mode the principal still decides, and each disagreeing value is logged.

**The flag overrides an authenticated principal.** With the flag on, an `AuthenticatedUserResolver` is never consulted, even when one is configured: an authenticated user who can reach the service directly can act as whoever the header names. The startup line names the resolver the header overrides, so this combination is visible in the log.

**`X-User-Role` is read only behind a trusted gateway.** Baggage `role` is written into the plaintext, append-only metadata of every event a request produces, so one rule sets it — `RequestIdentityPolicy.applyRole`, identical in the three request filters. In trusted-gateway mode it is the `X-User-Role` header, which the gateway owns exactly as it owns `X-User-Id`; a header sent more than once is no role. In the authenticated-principal and anonymous modes the header is ignored and the request has no `role` — not even the principal's roles from your `UserRoleResolver`: verified authorities are the request context's captured `authority` (see [Commands that execute off the request thread](#commands-that-execute-off-the-request-thread)), which is never persisted. A W3C `baggage: role=` entry never reaches the key in any mode (`role` is not a default allow-listed baggage key, and listing it in `streamrune.metadata.baggage-allowlist` changes nothing). The framework never authorizes on baggage `role`; derive authorities from the authenticated identity through a `UserRoleResolver`. A resolver that reads baggage `role` trusts the gateway, which holds only in trusted-gateway mode.

**Anonymous mode is safe but cannot authorize.** Every request runs without a user, so `Authorization.requireOwner(...)` and a policy that checks `userId == null` deny. To catch the misconfiguration early, **startup is refused** when a command bus carries an identity-consuming authorization interceptor — `AuthorizationCommandInterceptor` (registered for a `CommandAuthorizationPolicy` bean) or `AnnotationAuthorizationInterceptor` (registered for a `UserRoleResolver` bean) — and the mode is anonymous. The check reads the interceptor chain of every `VirtualThreadCommandBus` bean, the one the integration builds or one you build yourself, so an interceptor you hand to your own bus counts and an interceptor bean your bus leaves out does not. The error names the interceptors and both remedies: add the security module (or an `AuthenticatedUserResolver` bean), or set the trusted-gateway flag. The rule applies only where the integration derives identities from HTTP requests (a servlet application for Spring, a Quarkus REST application for Quarkus); a headless worker is not checked. Micronaut checks wherever its `StreamRuneContextFilter` is a bean, and because `streamrune-micronaut` brings the Netty HTTP server that is every Micronaut application that has not replaced the filter. The SSE endpoint is not a startup signal: its `SseAuthorizer` receives `null` for an anonymous caller, which an ownership check already denies.

Every place that derives a user from a request resolves through the same policy: the request filter that binds `RequestContext`, and the SSE endpoint, whose `SseAuthorizer` receives the same caller a command from that request would run as. The rule is identical in the Spring, Quarkus and Micronaut integrations. On Micronaut, Micronaut Security with `micronaut.security.enabled=false` counts as absent — its beans are not created, so there is no principal to resolve.

If authorization fails, the policy throws `AuthorizationException`. The command never executes.

**Auto-registration:** in Spring, Quarkus, and Micronaut, registering a `CommandAuthorizationPolicy` bean causes `AuthorizationCommandInterceptor` to be auto-registered. No additional wiring needed.

## Quick example

```java
// Role-based: only ADMIN role can cancel orders
@Bean
public CommandAuthorizationPolicy authorizationPolicy(RoleRepository roles) {
    return (userId, command) -> {
        if (command instanceof CancelOrder) {
            if (userId == null) throw new AuthorizationException("Authentication required");
            if (!roles.hasRole(userId.value(), "ADMIN")) {
                throw new AuthorizationException("Only admins can cancel orders");
            }
        }
    };
}
```

## Full example

```java
// Users can only update their own profile
@Bean
public CommandAuthorizationPolicy authorizationPolicy() {
    return (userId, command) -> {
        if (userId == null) {
            throw new AuthorizationException("Authentication required to execute: "
                + command.getClass().getSimpleName());
        }
        if (command instanceof UpdateUserProfile update) {
            if (!update.userId().equals(userId.value())) {
                throw new AuthorizationException(
                    "User " + userId.value() + " cannot update profile of " + update.userId());
            }
        }
    };
}

// Binding the UserId yourself — only needed at an ingress the shipped request filters do not
// cover (they bind the context for every HTTP request already). Use fromRequest(...), never the
// RequestContext constructor, for values that just arrived from outside: it is the only factory
// that enforces the id bound (at most 255 characters, no control characters) on the trace,
// user and correlation ids, and rejects a violating value with IllegalArgumentException before
// any command runs. The constructor skips that check on purpose — it is the reconstruction path
// for ids already at rest (DLQ replay, saga dispatch).
StreamRuneContext.RequestContext context = StreamRuneContext.RequestContext.fromRequest(
    null,                                            // traceId (from OTel), nullable
    UserId.of(principal.getName()),                  // userId — the authenticated principal
    CorrelationId.of(UUID.randomUUID().toString()),  // correlationId (required)
    Instant.now(),                                   // timestamp (required)
    Map.of());                                       // baggage
ScopedValue.where(StreamRuneContext.CURRENT, context)
    .run(() -> commandBus.execute(new UpdateUserProfile(userId, newName)));
```

A context bound this way carries no captured authority, so `executeAsync` of an authz-gated command
fails closed — see [Commands that execute off the request thread](#commands-that-execute-off-the-request-thread).

Test utilities:

```java
CommandAuthorizationPolicy.allowAll()
CommandAuthorizationPolicy.denyAll("Not allowed in tests")
```

## Configuration

Authorization itself is code-based; the one property is where the request identity comes from:

| Property | Default | Meaning |
|---|---|---|
| `streamrune.security.trust-user-id-header` | `false` | `true` selects the trusted-gateway mode: `X-User-Id` is the identity. See [Where the identity comes from (security)](#where-the-identity-comes-from-security). |

| Framework | Trigger | What happens |
|---|---|---|
| Spring | `@Bean CommandAuthorizationPolicy` defined | `AuthorizationCommandInterceptor` auto-registered by `StreamRuneAutoConfiguration` |
| Quarkus | `@ApplicationScoped CommandAuthorizationPolicy` bean | `StreamRuneProducers` conditionally produces `AuthorizationCommandInterceptor` |
| Micronaut | `@Singleton CommandAuthorizationPolicy` bean | `StreamRuneMicronautModule` registers the interceptor automatically |

## Annotation-based authorization

For the common case — "this command type requires role X" or "permission Y" — you don't need to write a policy at all. Declare the requirement with an annotation on the command type.

### The annotations

Both live in `org.streamrune.core`, target `TYPE` (`@Target(ElementType.TYPE)`), and are retained at runtime:

- `@RequireRole(String... value)` — the caller must have **at least one** of the listed roles (OR logic).
- `@RequirePermission(String... value)` — the caller must have **at least one** of the listed permissions (OR logic).

When **both** are present on a command, **both** must be satisfied (AND across the two annotations; OR within each).

```java
// Single role
@RequireRole("ADMIN")
public record CancelOrder(String orderId, String reason) implements OrderCommand {}

// Either role suffices (OR)
@RequireRole({"ADMIN", "MANAGER"})
public record RefundOrder(String orderId) implements OrderCommand {}

// Permission-based
@RequirePermission("orders:cancel")
public record ForceCancelOrder(String orderId) implements OrderCommand {}

// Both required (AND): caller needs the ADMIN role *and* the orders:purge permission
@RequireRole("ADMIN")
@RequirePermission("orders:purge")
public record PurgeOrder(String orderId) implements OrderCommand {}
```

The annotation may sit on the command record **or on any interface or superclass in its hierarchy** — so a `@RequireRole("ADMIN")` on a shared sealed command interface applies to every permitted record. Lookup is performed via `AuthorizationAnnotations`; the declaration nearest to the command class wins.

```java
@RequireRole("ADMIN")
public sealed interface AdminCommand extends Command
    permits CloseAccount, PurgeData {}
```

### How it works

`AnnotationAuthorizationInterceptor` (a `CommandInterceptor`) enforces the annotations in `before(ctx)`, before the command reaches the decider:

1. If the command type carries no annotations, the interceptor is a no-op.
2. Otherwise it reads the caller's `UserId` from the request context. If no user is authenticated, it throws `AuthorizationException("Authentication required")`.
3. It resolves the caller's authorities via a `UserRoleResolver` and checks them against the required roles/permissions. On a mismatch it throws `AuthorizationException`.

**An annotation nothing enforces is refused.** Without the interceptor the annotations would be ignored and the command would run unguarded, so `VirtualThreadCommandBus.Builder.build()` refuses a bus whose registered command types declare `@RequireRole`/`@RequirePermission` — on the type, on a supertype, or on a permitted subtype of a sealed registered type — while no `AnnotationAuthorizationInterceptor` is among its interceptors. The check reads what that builder was given, so a bus you build by hand is held to it exactly like the one an integration assembles. Each integration also resolves every command bus at startup, so the refusal stops the application from starting instead of failing its first command. A subtype of a registered type that is not sealed cannot be enumerated and is not checked: annotate the registered type, or seal the hierarchy. The interceptor is recognised as a member of the chain only — hand it to `interceptors(...)` itself, not wrapped in `CommandInterceptor.compose(...)` or a decorator of your own.

`UserRoleResolver` (`org.streamrune.core`) is a `@FunctionalInterface`:

```java
@FunctionalInterface
public interface UserRoleResolver {
    UserAuthority resolve(UserId userId);   // userId is never null
}
```

`UserAuthority` (`org.streamrune.core`) is an immutable record of the caller's roles and permissions:

```java
public record UserAuthority(Set<String> roles, Set<String> permissions) { ... }
```

### Per-framework wiring

Each integration **auto-produces** a native `*SecurityUserRoleResolver` (reading its framework's security context) and the `AnnotationAuthorizationInterceptor` that consumes it — no wiring needed once the framework's security module is on the classpath.

| Framework | Default resolver | Interceptor |
|---|---|---|
| Spring | `SpringSecurityUserRoleResolver` (reads `SecurityContextHolder`) | `AnnotationAuthorizationInterceptor` auto-registered when a `UserRoleResolver` bean is present |
| Quarkus | `QuarkusSecurityUserRoleResolver` (reads `SecurityIdentity`) | produced when a `UserRoleResolver` is available |
| Micronaut | `MicronautSecurityUserRoleResolver` (reads `SecurityService`) | produced when a `UserRoleResolver` bean is available |

**To override**, supply your own `UserRoleResolver` bean — a database lookup, an external authorization service, whatever fits. The native resolvers are registered `@ConditionalOnMissingBean` (Spring) / `@DefaultBean` (Quarkus) / `@Requires(missingBeans = ...)` (Micronaut), so your bean takes precedence and the interceptor uses it instead.

```java
// Spring: override the default resolver with a database-backed one
@Bean
public UserRoleResolver userRoleResolver(RoleRepository roles) {
    return userId -> new UserAuthority(
        roles.rolesFor(userId.value()),
        roles.permissionsFor(userId.value()));
}
```

### Annotations vs. `CommandAuthorizationPolicy`

Reach for the annotations when the rule is "who may run this command type." Reach for a programmatic `CommandAuthorizationPolicy` when the check depends on the command's *payload* (e.g. "users may only update their own profile") — the annotations only see the command type, not its field values. The two layers are complementary and can both be active at once.

## Ownership checks in `Decider.guard`

The interceptors run before any state is loaded, so they cannot answer "does this caller own this aggregate". `Decider.guard(command, state)` can: it runs after the state is loaded and before `decide`, and an `AuthorizationException` it throws rejects the command. `Authorization` (in `streamrune-core`) provides the checks:

```java
@Override
public void guard(OrderCommand command, OrderState state) {
    if (command instanceof CreateOrder) {
        return; // aggregate does not exist yet, nobody owns it
    }
    Authorization.requireOwnerOrSaga(state.ownerId()); // the owner, or a saga
}
```

| Check | Passes when | Use it for |
|---|---|---|
| `Authorization.requireOwner(ownerId)` | the bound user equals `ownerId` | a command no saga may ever issue |
| `Authorization.requireOwnerOrSaga(ownerId)` | a saga dispatched the command, or the bound user equals `ownerId` | every command a saga may send to the aggregate, forward or compensation |

Both deny an unauthenticated caller and a `null` owner outside a saga, so branch on the creation command: `guard` also runs for it, on `initialState()`, where no owner is set yet.

**A saga-dispatched command carries no user** (next section), so `requireOwner` denies it, with a message naming that cause. That denial is a permanent rejection: a forward command's rejection compensates the saga's flow, and a rejected compensation leaves the saga `FAILED` with its undo never run. `StreamRuneContext.isSagaDispatch()` reports whether a saga dispatched the current command, for a guard that needs its own branch.

## Saga-dispatched commands run as the system principal

**A command a [saga](saga.md) dispatches — forward or compensation — runs with no bound user.** Sagas dispatch from a subscription poll thread, a `SagaTimeoutRunner` thread, or the `SagaCompensationRetrySweeper` thread; none of these bind a `StreamRuneContext` user, because no logged-in user drives a timeout or a crash-resume. A saga acts as the **system**, not an end user.

So **both** authorization interceptors treat commands dispatched under `StreamRuneContext.SAGA_OWNED` (bound by `SagaCommandDispatch` around every saga-issued command) as the **trusted saga-system principal**: `AnnotationAuthorizationInterceptor` bypasses the per-user `@RequireRole`/`@RequirePermission` check, and `AuthorizationCommandInterceptor` does not consult the `CommandAuthorizationPolicy` at all. This is the default, and it is what stops a `@RequirePermission("payments:refund")` gate — or a policy written in the documented `if (userId == null) throw new AuthorizationException(...)` shape — from rejecting a saga's `RefundPayment` compensation and silently abandoning the undo (no DLQ entry, looking like a legitimate rejection).

> Do not special-case anonymous callers in a `CommandAuthorizationPolicy` to let saga commands through: the marker-scoped bypass already covers them, and an anonymous-caller exception would admit *every* unauthenticated command. Both interceptors share one implementation of the marker test and the skip record.

- **Opt-out:** `new AnnotationAuthorizationInterceptor(resolver, /* honorSagaSystemPrincipal= */ false)` and `new AuthorizationCommandInterceptor(policy, /* honorSagaSystemPrincipal= */ false)` enforce per-user authz even for saga-dispatched commands. If you opt out, **saga commands must not carry `@RequireRole`/`@RequirePermission`** and your policy must permit `userId == null`, or their compensation fails closed. See [saga.md → Authorization and saga-dispatched commands](saga.md#authorization-and-saga-dispatched-commands).
- The bypass is scoped strictly to saga dispatch; an ordinary `bus.execute(...)` on an unauthenticated thread still fails closed.
- **Every skip is recorded.** The interceptor logs the command id, the command type and the system principal it ran under (`saga`), attributed to whichever interceptor skipped — WARN the first time a given command type executes unchecked in a process, DEBUG afterwards, so a saga-heavy deployment gets an inventory rather than a flood. If there is no such record, the per-user check ran.
- **The ownership check in `Decider.guard` is yours to choose.** The interceptors' bypass does not reach `guard`: use `Authorization.requireOwnerOrSaga(...)` for the commands a saga sends, as shown in [Ownership checks in `Decider.guard`](#ownership-checks-in-deciderguard). A saga acts with the system's authority, so authorize the user command that starts its flow — a saga that takes its target from an event payload acts on whichever aggregate the payload names.

## Commands that execute off the request thread

Two supported paths execute a command on a thread other than the one that submitted it, and both interact with authorization:

| Path | Thread | How authorization is decided |
|---|---|---|
| `bus.executeAsync(command)` | a fresh virtual thread per call | the authorities **captured at the request edge** and carried in `RequestContext` |
| `DeadLetterRetryRunner` replay | the runner's poll thread, or for `retry(CommandId)` a fresh virtual thread it waits for — never the caller's | the resolver, if it can answer there; otherwise the recorded **DLQ-replay system principal** |

**Why this needs machinery at all.** Each shipped `*SecurityUserRoleResolver` reads its framework's request-scoped security state — Spring's `SecurityContextHolder` (a `ThreadLocal`), Quarkus's request-scoped `SecurityIdentity`, Micronaut's `SecurityService`. None of that follows a command onto another thread, and Quarkus's request scope cannot be revived off-request at all. Calling such a resolver there returns `UserAuthority.EMPTY`, which is indistinguishable from an honest "this user has no roles": treating it as a denial rejects a caller who was authorized a millisecond earlier and, worse, makes a dead-lettered command fail every retry until it ages out — a legitimate business operation lost, with the logs blaming authorization. Treating it as permission would be an authorization bypass.

**`UserRoleResolver.requiresRequestContext()`** is how a resolver says which of the two it is. It defaults to `false` — a resolver that is a pure function of the `userId` (a database lookup, a static map, a claims decoder) already works on any thread and keeps its exact behaviour, including denying a DLQ replay for a user whose roles were revoked while the entry sat in the queue. The three shipped resolvers override it to `true`, which turns on two things:

1. **Request-edge capture.** Each integration's request filter calls the resolver once per authenticated request and stores the answer on `RequestContext.authority()`. `executeAsync` already re-binds the whole request context onto its virtual thread, so the decision travels with it and async authorizes exactly as synchronous execution does. A resolver returning `false` is never called at the request edge, so an expensive custom resolver pays nothing.
2. **DLQ-replay system principal.** On a replay the resolver provably cannot answer, so the command executes under a recorded system principal instead of being denied. This is safe because a command can only reach the command dead letter queue *after* passing this interceptor — `before()` throwing never reaches the code that publishes, and `AuthorizationException` is a `DomainException`, which the default DLQ-eligibility predicate excludes anyway. The replay completes work that was already authorized; it never admits new work.

The captured authority is **process-local and never persisted**. The dead letter queue stores the user, correlation and trace ids — not a role set — so a grant can never outlive the process that resolved it, be replayed from a database row after the user's roles changed, or be forged by anything that can write the table.

- **Opt-out:** `new AnnotationAuthorizationInterceptor(resolver, honorSagaSystemPrincipal, /* honorDlqReplaySystemPrincipal= */ false)` consults the resolver on the replay thread regardless. With a request-scoped resolver that means every authz-gated dead-lettered command is rejected on every attempt and lost when its ladder runs out. Choose it only if dead-letter rows can be written by something other than the command bus and that risk outweighs losing legitimate commands.
- **Every skip is recorded**, WARN on every occurrence: the replay's command id, the command type and the resolver that could not answer. The replay runs as the user its dead-letter entry recorded, and that command id joins the record to the events and the audit row (when command auditing is on) that carry the user. The dead-letter entry is no join target: it carries the original execution's command id, and a successful replay removes it.
- **A replay with no persisted user is still denied** (`Authentication required`). The system principal is scoped to "the resolver cannot answer for *this user*", never to "no user is needed".
- **A denial by a request-scoped resolver that returned nothing** carries a diagnostic naming that cause, so "the resolver could not see a request" is distinguishable from "this user has no roles".
- If you write your own filter or bind `StreamRuneContext.CURRENT` by hand, no authority is captured and `executeAsync` of an authz-gated command fails closed. Use the shipped filter, or make your resolver a pure function of the `userId`.

## Caveats

- `UserId` is `null` for unauthenticated requests. Always null-check before accessing `userId.value()`.
- `AuthorizationException` is not retried — it propagates immediately, bypassing the `RetryPolicy`.
- The `ScopedValueFilter` (Spring) / `StreamRuneContextFilter` (Micronaut) / `StreamRuneRequestFilter` (Quarkus) binds `RequestContext.userId` from the **authenticated principal** when a resolver is available; `X-User-Id` is the identity only with `streamrune.security.trust-user-id-header=true` (trusted gateway), and without either every request is anonymous — see [Where the identity comes from (security)](#where-the-identity-comes-from-security) above for the startup rule.
- `CommandAuthorizationPolicy` receives the command typed as the `Command` marker interface — use `instanceof` pattern matching for type-safe checks.
- Authorization runs before the decider loads aggregate state. For decisions that require current state, perform the check inside the decider.
- **Saga-dispatched commands bypass per-user authz by default** in both interceptors (they run as the system principal — see above). Gate them with `Decider.guard`/domain checks, not `@RequireRole` or a `CommandAuthorizationPolicy`, or opt out explicitly. In `guard`, `Authorization.requireOwner(...)` denies every saga-dispatched command; use `Authorization.requireOwnerOrSaga(...)` for the commands a saga sends.
- **A custom `UserRoleResolver` that reads ambient per-request state must override `requiresRequestContext()` to return `true`**, or `executeAsync` and DLQ replay will be denied for legitimately authorized callers — see [Commands that execute off the request thread](#commands-that-execute-off-the-request-thread).

## Related guides

| Guide | What it covers |
|---|---|
| [audit.md](audit.md) | Recording who did what, when — the audit trail behind authorized commands |
| [gdpr-erasure.md](gdpr-erasure.md) | Crypto-shredding and subject erasure for GDPR Article 17 |
| [retry-and-resilience.md](retry-and-resilience.md) | `RetryPolicy`, circuit breaker, and the command dead-letter queue |
