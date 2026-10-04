package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.streamrune.core.AggregateState;
import org.streamrune.core.AuthorizationException;
import org.streamrune.core.Command;
import org.streamrune.core.CommandInbox;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.RequireRole;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.UserAuthority;
import org.streamrune.core.UserRoleResolver;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.UserId;
import org.streamrune.core.types.Version;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCommandInbox;
import org.streamrune.test.InMemoryDeadLetterQueue;
import org.streamrune.test.InMemoryEventStore;

/**
 * Off-request authorization — both halves, driven through the REAL execution path with the REAL
 * shipped {@link SpringSecurityUserRoleResolver} resolved from a real Spring context.
 *
 * <p>Every collaborator that matters comes out of {@link StreamRuneAutoConfiguration}: the {@link
 * UserRoleResolver} (the shipped Spring Security one), the {@link
 * org.streamrune.runtime.AnnotationAuthorizationInterceptor}, the {@link ScopedValueFilter}, the
 * {@link VirtualThreadCommandBus} and the {@link DeadLetterRetryRunner}. Constructing the
 * interceptor by hand cannot observe a context-propagation defect at all — the defect is that the
 * ambient security state the resolver reads does not survive the hop off the request thread, which
 * only the real filter/bus/runner code paths exercise.
 *
 * <p><b>Path A — {@code executeAsync}.</b> Inside one real filter-bound request, the SAME
 * authenticated ADMIN whose {@code execute} succeeds must not be rejected by {@code executeAsync}.
 * Before the fix it was: Spring's {@code SecurityContextHolder} is a plain {@link ThreadLocal} that
 * does not follow the command onto the bus's virtual thread, so the resolver answered EMPTY and the
 * interceptor threw {@code Required role: ADMIN}.
 *
 * <p><b>Path B — DLQ replay.</b> A {@code @RequireRole("ADMIN")} command that passed authorization
 * and then failed on a transient downstream is dead-lettered. Every replay runs on a thread that
 * never had a {@code SecurityContext}. Before the fix the shipped resolver returned EMPTY there and
 * the replay was rejected until the entry exhausted its retry ladder — an already-authorized
 * business command permanently lost, with the logs blaming authorization rather than naming the
 * loss.
 */
class OffRequestAuthorizationTest {

  private static final AggregateType ORDER = AggregateType.of("order");

  private static final String ADMIN_USER = "admin-user";

  // ---- domain -------------------------------------------------------------------------------

  @RequireRole("ADMIN")
  record ArchiveOrder(String orderId) implements Command {}

  record OrderArchived(String orderId) implements DomainEvent {}

  record OrderState() implements AggregateState {}

  /**
   * Fails {@code failuresBeforeSuccess} times before producing its event — models a handler whose
   * downstream is broken on first execution (so the command is dead-lettered) and healthy by the
   * time the retry runner replays it.
   */
  static final class FlakyArchiveDecider
      implements Decider<ArchiveOrder, OrderState, OrderArchived> {
    private final AtomicInteger remainingFailures;

    FlakyArchiveDecider(int failuresBeforeSuccess) {
      this.remainingFailures = new AtomicInteger(failuresBeforeSuccess);
    }

    @Override
    public OrderState initialState() {
      return new OrderState();
    }

    @Override
    public List<OrderArchived> decide(ArchiveOrder command, OrderState state) {
      if (remainingFailures.getAndUpdate(n -> n > 0 ? n - 1 : 0) > 0) {
        // IllegalStateException is an INFRASTRUCTURE_FAILURE under DEFAULT_DLQ_ELIGIBLE, so it is
        // dead-lettered (a DomainException would be a permanent business rejection and never be).
        throw new IllegalStateException("transient downstream failure");
      }
      return List.of(new OrderArchived(command.orderId()));
    }

    @Override
    public OrderState evolve(OrderState state, OrderArchived event) {
      return state;
    }
  }

  // ---- container ----------------------------------------------------------------------------

  private final InMemoryCommandInbox commandInbox = new InMemoryCommandInbox();
  private final InMemoryEventStore eventStore =
      new InMemoryEventStore().withCommandInbox(commandInbox);
  private final InMemoryDeadLetterQueue deadLetterQueue = new InMemoryDeadLetterQueue();

  private static final String[] PROPERTIES = {
    // The command must reach the DLQ after ONE failed execution, not after in-bus retries.
    "streamrune.retry-max-attempts=1",
    "streamrune.dead-letter.max-retries=2",
    // The runner is driven explicitly via retry(commandId); no background polling.
    "streamrune.dead-letter.retry-interval-ms=3600000",
    "streamrune.subscription.single-active-consumer.enabled=false",
  };

  private <
          T extends
              org.springframework.boot.test.context.runner.AbstractApplicationContextRunner<
                      T, ?, ?>>
      T configure(T runner, int failuresBeforeSuccess) {
    return runner
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withPropertyValues(PROPERTIES)
        .withBean(DataSource.class, () -> mock(DataSource.class))
        .withBean(EventStore.class, () -> eventStore)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        // The DataSource is a mock, so the auto-configured Postgres inbox cannot open a connection;
        // an in-memory inbox keeps the keyed DLQ replay path (the production path) exercised.
        .withBean(CommandInbox.class, () -> commandInbox)
        .withBean(DeadLetterQueue.class, () -> deadLetterQueue)
        .withBean(
            "archiveOrderRegistration",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    ORDER,
                    ArchiveOrder.class,
                    (ArchiveOrder cmd) -> AggregateId.of(cmd.orderId()),
                    new FlakyArchiveDecider(failuresBeforeSuccess)));
  }

  private ApplicationContextRunner contextRunner(int failuresBeforeSuccess) {
    return configure(new ApplicationContextRunner(), failuresBeforeSuccess);
  }

  private WebApplicationContextRunner webContextRunner(int failuresBeforeSuccess) {
    return configure(new WebApplicationContextRunner(), failuresBeforeSuccess);
  }

  /** Authenticates the ADMIN principal exactly as Spring Security's filter chain would. */
  private static void authenticateAsAdmin() {
    var auth =
        new UsernamePasswordAuthenticationToken(
            ADMIN_USER, "n/a", List.of(new SimpleGrantedAuthority("ROLE_ADMIN")));
    SecurityContextHolder.getContext().setAuthentication(auth);
  }

  @AfterEach
  void clearSecurityContext() {
    SecurityContextHolder.clearContext();
  }

  /** The request context a caller would hand-build outside the filter — no captured authority. */
  private static StreamRuneContext.RequestContext handBuiltAdminContext() {
    return new StreamRuneContext.RequestContext(
        null,
        UserId.of(ADMIN_USER),
        CorrelationId.of("corr-" + UUID.randomUUID()),
        java.time.Instant.now(),
        Map.of());
  }

  private long archivedEventCount(String orderId) {
    return eventStore
        .readStream(
            StreamId.of(ORDER, AggregateId.of(orderId)), Version.initial(), Integer.MAX_VALUE)
        .size();
  }

  /**
   * The documented shape of a resolver that declares {@link
   * UserRoleResolver#requiresRequestContext()}: it derives the caller's authorities from the
   * StreamRune request context rather than from Spring Security's ThreadLocal. This is the
   * framework-portable version of "reads ambient per-request state" — the same class the
   * declaration exists for, and the one an application writes when it wants one resolver to behave
   * identically on all three integrations. The baggage {@code role} it reads is the {@code
   * X-User-Role} header a trusted gateway forwarded, the only mode in which that header becomes
   * baggage, so the tests that use it run in that mode.
   */
  static final class RequestContextRoleResolver implements UserRoleResolver {
    @Override
    public UserAuthority resolve(UserId userId) {
      var bound = StreamRuneContext.capture();
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
  }

  // ---- Path C: the capture must happen where the context exists -------------------------------

  /**
   * Capture ordering. Turning on the very mechanism that captures authorities at the request edge
   * broke live, synchronous authorization: {@code RequestEdgeAuthority.capture} ran before {@code
   * StreamRuneContext.CURRENT} was bound, so a resolver that reads the request answered EMPTY, and
   * the interceptor's captured-authority rung ENFORCES a captured answer — turning an ADMIN request
   * into "Required role: ADMIN" (HTTP 400) on the request thread itself, with the correct header
   * present.
   *
   * <p>Driven through the real auto-configured {@link ScopedValueFilter} and {@link
   * VirtualThreadCommandBus}: the whole point is the ordering between the filter's capture and the
   * filter's {@code ScopedValue} binding, which only the real filter exercises.
   */
  @Test
  void aResolverThatReadsTheRequestContextAuthorizesALiveRequest() {
    webContextRunner(0)
        .withPropertyValues("streamrune.security.trust-user-id-header=true")
        .withBean(UserRoleResolver.class, RequestContextRoleResolver::new)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(UserRoleResolver.class))
                  .isInstanceOf(RequestContextRoleResolver.class);
              var filter = ctx.getBean(ScopedValueFilter.class);
              var bus = ctx.getBean(VirtualThreadCommandBus.class);

              authenticateAsAdmin();
              var request = new MockHttpServletRequest();
              request.addHeader("X-User-Id", ADMIN_USER);
              request.addHeader("X-User-Role", "ADMIN");
              AtomicReference<Throwable> denial = new AtomicReference<>();
              filter.doFilter(
                  request,
                  new MockHttpServletResponse(),
                  (rq, rs) -> {
                    try {
                      bus.execute(new ArchiveOrder("order-live"));
                    } catch (RuntimeException e) {
                      denial.set(e);
                    }
                  });

              assertThat(denial.get())
                  .as(
                      "a resolver declaring requiresRequestContext() must be able to READ the"
                          + " request context during the capture; otherwise declaring the truth"
                          + " about the resolver is what breaks it")
                  .isNull();
              assertThat(archivedEventCount("order-live")).isEqualTo(1);
            });
  }

  /**
   * The same resolver, and the reason the capture exists at all: the decision must also survive the
   * hop onto {@code executeAsync}'s virtual thread, where nothing rebinds anything but the {@code
   * RequestContext} the bus carries.
   */
  @Test
  void aResolverThatReadsTheRequestContextAlsoAuthorizesExecuteAsync() {
    webContextRunner(0)
        .withPropertyValues("streamrune.security.trust-user-id-header=true")
        .withBean(UserRoleResolver.class, RequestContextRoleResolver::new)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var filter = ctx.getBean(ScopedValueFilter.class);
              var bus = ctx.getBean(VirtualThreadCommandBus.class);

              authenticateAsAdmin();
              var request = new MockHttpServletRequest();
              request.addHeader("X-User-Id", ADMIN_USER);
              request.addHeader("X-User-Role", "ADMIN");
              AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
              filter.doFilter(
                  request,
                  new MockHttpServletResponse(),
                  (rq, rs) -> {
                    try {
                      bus.executeAsync(new ArchiveOrder("order-live-async")).get();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      asyncFailure.set(e);
                    } catch (ExecutionException e) {
                      asyncFailure.set(e.getCause());
                    }
                  });

              assertThat(asyncFailure.get()).isNull();
              assertThat(archivedEventCount("order-live-async")).isEqualTo(1);
            });
  }

  // ---- Path A -------------------------------------------------------------------------------

  /**
   * Path A, driven through the REAL {@link ScopedValueFilter} bean: one authenticated request, one
   * synchronous {@code execute} (the baseline that makes the async assertion non-vacuous) and one
   * {@code executeAsync} of the same command by the same caller. Both must be authorized.
   */
  @Test
  void executeAsyncInsideARequestAuthorizesTheSameCallerAsExecute() {
    webContextRunner(0)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx.getBean(UserRoleResolver.class))
                  .as("the SHIPPED resolver must be the one under test")
                  .isInstanceOf(SpringSecurityUserRoleResolver.class);
              var filter = ctx.getBean(ScopedValueFilter.class);
              var bus = ctx.getBean(VirtualThreadCommandBus.class);

              authenticateAsAdmin();
              AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
              filter.doFilter(
                  new MockHttpServletRequest(),
                  new MockHttpServletResponse(),
                  (rq, rs) -> {
                    assertThatCode(() -> bus.execute(new ArchiveOrder("order-sync")))
                        .as("baseline: the synchronous execute of this caller is authorized")
                        .doesNotThrowAnyException();
                    try {
                      bus.executeAsync(new ArchiveOrder("order-async")).get();
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                      asyncFailure.set(e);
                    } catch (ExecutionException e) {
                      asyncFailure.set(e.getCause());
                    }
                  });

              assertThat(asyncFailure.get())
                  .as(
                      "executeAsync must authorize the same authenticated ADMIN that execute()"
                          + " authorizes — the decision has to survive the hop off the request"
                          + " thread")
                  .isNull();
              assertThat(archivedEventCount("order-sync")).isEqualTo(1);
              assertThat(archivedEventCount("order-async")).isEqualTo(1);
            });
  }

  /**
   * The capture is a request-edge act, not a blanket amnesty: a caller who never went through the
   * filter carries no captured authority, so an {@code executeAsync} from a hand-bound context is
   * still denied fail-closed rather than being waved through.
   */
  @Test
  void executeAsyncWithNoRequestEdgeCaptureIsStillDeniedFailClosed() {
    contextRunner(0)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var bus = ctx.getBean(VirtualThreadCommandBus.class);
              authenticateAsAdmin();
              AtomicReference<Throwable> failure = new AtomicReference<>();
              ScopedValue.where(StreamRuneContext.CURRENT, handBuiltAdminContext())
                  .run(
                      () -> {
                        try {
                          bus.executeAsync(new ArchiveOrder("order-uncaptured")).get();
                        } catch (InterruptedException _) {
                          Thread.currentThread().interrupt();
                        } catch (ExecutionException e) {
                          failure.set(e.getCause());
                        }
                      });
              assertThat(failure.get()).isInstanceOf(AuthorizationException.class);
              assertThat(failure.get())
                  .as(
                      "the denial must name the off-request cause instead of looking like an"
                          + " ordinary missing-role rejection")
                  .hasMessageContaining("requires a request context");
              assertThat(archivedEventCount("order-uncaptured")).isZero();
            });
  }

  /** Wiring proof for Path A: the auto-configured filter really receives the resolver bean. */
  @Test
  void scopedValueFilterIsWiredWithTheSameUserRoleResolverBeanAsTheInterceptor() {
    webContextRunner(0)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var filter = ctx.getBean(ScopedValueFilter.class);
              var resolverField = ScopedValueFilter.class.getDeclaredField("userRoleResolver");
              resolverField.setAccessible(true);
              assertThat(resolverField.get(filter))
                  .as(
                      "the filter must capture with the very resolver the interceptor enforces"
                          + " with, or the captured authority could differ from the synchronous"
                          + " decision")
                  .isSameAs(ctx.getBean(UserRoleResolver.class));
            });
  }

  // ---- Path B -------------------------------------------------------------------------------

  /**
   * Path B, the worse half: a legitimately authorized {@code @RequireRole("ADMIN")} command that
   * failed on a transient downstream must not be permanently lost by the retry runner. The replay
   * runs on a thread that never carried a {@code SecurityContext}, where the shipped resolver can
   * only ever answer EMPTY.
   */
  @Test
  void dlqReplayOfAnAlreadyAuthorizedCommandIsNotLostToAuthorization() {
    contextRunner(1)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var bus = ctx.getBean(VirtualThreadCommandBus.class);
              var retryRunner = ctx.getBean(DeadLetterRetryRunner.class);

              authenticateAsAdmin();
              ScopedValue.where(StreamRuneContext.CURRENT, handBuiltAdminContext())
                  .run(
                      () ->
                          assertThatThrownBy(() -> bus.execute(new ArchiveOrder("order-dlq")))
                              .hasMessageContaining("transient downstream failure"));
              SecurityContextHolder.clearContext();

              var entries = deadLetterQueue.all();
              assertThat(entries)
                  .as("the authorized-then-failed command must be dead-lettered")
                  .hasSize(1);
              var entry = entries.getFirst();
              assertThat(entry.userId()).isEqualTo(UserId.of(ADMIN_USER));

              // Replay on a thread that never had a SecurityContext — exactly what the runner's own
              // poll thread is. retry() runs the SAME processEntry path the poll loop runs.
              replayOnAForeignThread(retryRunner, entry.commandId());

              assertThat(deadLetterQueue.all())
                  .as(
                      "the replay must succeed and the entry be discarded — an already-authorized"
                          + " business command must not be lost because the resolver cannot see a"
                          + " request context on the retry thread")
                  .isEmpty();
              assertThat(archivedEventCount("order-dlq"))
                  .as("the replayed command must actually produce its domain event")
                  .isEqualTo(1);
            });
  }

  /**
   * The invariant the DLQ-replay system principal rests on: a command REJECTED by authorization can
   * never enter the dead letter queue at all. The interceptor's {@code before()} throws before
   * {@code executeInternal} — the only place that publishes — and {@code AuthorizationException} is
   * a {@code DomainException}, which {@code DEFAULT_DLQ_ELIGIBLE} excludes anyway. So every entry
   * in the queue is a command that already passed the authorization gate, which is exactly why
   * completing one under the system principal is not an admission of new, unauthorized work.
   */
  @Test
  void anAuthorizationRejectedCommandIsNeverDeadLettered() {
    contextRunner(0)
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var bus = ctx.getBean(VirtualThreadCommandBus.class);
              // Authenticated as nobody: the interceptor rejects before anything executes.
              ScopedValue.where(StreamRuneContext.CURRENT, handBuiltAdminContext())
                  .run(
                      () ->
                          assertThatThrownBy(() -> bus.execute(new ArchiveOrder("order-denied")))
                              .isInstanceOf(AuthorizationException.class));
              assertThat(deadLetterQueue.all())
                  .as("an authorization rejection must never reach the dead letter queue")
                  .isEmpty();
              assertThat(archivedEventCount("order-denied")).isZero();
            });
  }

  private static void replayOnAForeignThread(DeadLetterRetryRunner runner, Object commandId)
      throws Exception {
    AtomicReference<Throwable> error = new AtomicReference<>();
    var thread =
        new Thread(
            () -> {
              try {
                runner.retry((org.streamrune.core.types.CommandId) commandId);
              } catch (Throwable t) {
                error.set(t);
              }
            },
            "dlq-replay-probe");
    thread.start();
    thread.join(30_000);
    assertThat(error.get()).isNull();
  }
}
