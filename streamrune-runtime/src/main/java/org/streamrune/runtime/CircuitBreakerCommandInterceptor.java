package org.streamrune.runtime;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import org.streamrune.core.CircuitBreakerOpenException;
import org.streamrune.core.CommandFailureClassification;
import org.streamrune.core.CommandInterceptor;
import org.streamrune.core.DomainException;

/**
 * Circuit breaker implementation as a {@link CommandInterceptor}.
 *
 * <p>State machine: CLOSED → OPEN (after {@code failureThreshold} consecutive failures) → HALF_OPEN
 * (after {@code cooldown} elapsed) → CLOSED (on probe success) or OPEN (on probe failure).
 *
 * <p>Only failures matching the {@code failurePredicate} count toward the threshold. The default
 * predicate ({@link #INFRASTRUCTURE_FAILURES}) excludes client/domain rejections — {@link
 * DomainException} (including {@code AuthorizationException}) and {@link IllegalArgumentException}
 * (including {@code ValidationException} and {@code NoDeciderException}) — so a burst of malformed
 * or unauthorized requests cannot open the circuit and reject healthy traffic. Pass a custom
 * predicate to widen or narrow what trips the breaker.
 *
 * <p>The breaker is global for the bus it is registered on: failures of one command type open the
 * circuit for all commands on that bus. For per-domain partitioning, register a separate breaker
 * instance on each bus.
 *
 * <p>Thread-safe: state transitions use {@link AtomicReference#compareAndSet} to ensure exactly one
 * probe request transitions OPEN→HALF_OPEN. The {@link #probeStack} thread-local tracks whether the
 * current execution is the designated probe, so {@link #onError} can distinguish probe failure from
 * normal failure without re-reading the shared {@code state}; it is a stack so a nested command
 * execution on the same thread cannot steal an outer execution's designation.
 *
 * <p>If the designated probe never reports back (e.g. it hangs on a stuck connection), a new probe
 * is allowed after {@code probeTimeout} so the breaker cannot stay HALF_OPEN — rejecting all
 * traffic — forever. Either ordering of the two probes' callbacks is benign, because every
 * probe-driven transition is a compare-and-set on the HALF_OPEN episode it was designated for: a
 * probe success closes the circuit and a probe failure reopens it only while that episode is still
 * live, so neither can overwrite a verdict the other already recorded. A probe failure whose CAS is
 * declined still counts toward {@code failureThreshold} like any other.
 *
 * <p>A {@code cooldown} of {@link Duration#ZERO} is valid and means the circuit will immediately
 * allow a probe after opening.
 */
public final class CircuitBreakerCommandInterceptor implements CommandInterceptor {

  /**
   * Default failure predicate: counts only failures that carry actual infrastructure-health
   * evidence (event store connectivity, locking, unexpected errors) toward the threshold.
   *
   * <p>This is {@link CommandFailureClassification#BREAKER_ELIGIBLE} — derived from the same {@code
   * isPermanentRejection} definition {@code VirtualThreadCommandBus.DEFAULT_DLQ_ELIGIBLE} derives
   * from, not a second copy of it. The copy drifted: it excluded {@link DomainException} and {@link
   * IllegalArgumentException} but not a crypto-shredded ({@code SubjectForgottenException})
   * subject, so post-erasure traffic — a permanent rejection the DLQ correctly refuses to record —
   * counted toward the threshold and could open the circuit for every unrelated aggregate on the
   * bus.
   *
   * <p>Additionally excludes the deterministic per-aggregate stored-data family ({@link
   * CommandFailureClassification#isDeterministicPerAggregate} — a corrupt/unbindable stored
   * payload, an unregistered event type). Such a failure is a final fact about ONE aggregate's
   * stored bytes, produced by a perfectly healthy store: counting it let a single wedged aggregate,
   * hammered by a client retry loop or a DLQ replayer, open the circuit for the whole bus. It stays
   * DLQ-eligible — see the {@link CommandFailureClassification} matrix for the full split.
   */
  public static final Predicate<Throwable> INFRASTRUCTURE_FAILURES =
      CommandFailureClassification.BREAKER_ELIGIBLE;

  /** Default time after which a silent (never-reporting) probe is superseded by a new probe. */
  public static final Duration DEFAULT_PROBE_TIMEOUT = Duration.ofSeconds(30);

  private enum State {
    CLOSED,
    OPEN,
    HALF_OPEN
  }

  private final int failureThreshold;
  private final Duration cooldown;
  private final Duration probeTimeout;
  private final Predicate<Throwable> failurePredicate;
  private final LongSupplier nanoTimeSource;
  private final AtomicInteger failureCount = new AtomicInteger(0);
  private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
  private volatile long openedAtNano = 0;

  /**
   * When the current HALF_OPEN probe was designated. A thread takes over as the new probe by
   * CAS-ing this from the stale value it observed, so exactly one waiter wins per timeout.
   */
  private final AtomicLong probeStartedAtNano = new AtomicLong(0);

  /**
   * Tracks whether each in-flight execution ON THIS THREAD is the designated probe in HALF_OPEN.
   *
   * <p>A per-thread STACK, not a slot. The bus runs {@code after()} in reverse registration order,
   * so {@link InlineProjectionInterceptor} executes user projection code on the calling thread
   * while this interceptor's callback is still pending; a process-manager projection that
   * dispatches a follow-up command through the same bus runs a NESTED execution there. The nested
   * {@code before()} used to clear the flat flag before rejecting itself (a probe is already in
   * flight, so the nested command is turned away), which cost the OUTER execution its probe
   * designation — the designated probe then never reported back and the circuit stayed HALF_OPEN,
   * rejecting all traffic, until {@code probeTimeout} (30s by default) let another thread take
   * over.
   *
   * <p>A frame is pushed only on the paths that return {@code true}: a {@code before()} that throws
   * {@link CircuitBreakerOpenException} gets no callback from the bus, so it must leave the stack
   * exactly as it found it.
   */
  private final ThreadLocal<java.util.ArrayDeque<Boolean>> probeStack =
      ThreadLocal.withInitial(java.util.ArrayDeque::new);

  /**
   * Creates a breaker with the default failure predicate ({@link #INFRASTRUCTURE_FAILURES}) and
   * probe timeout ({@link #DEFAULT_PROBE_TIMEOUT}).
   */
  public CircuitBreakerCommandInterceptor(int failureThreshold, Duration cooldown) {
    this(failureThreshold, cooldown, DEFAULT_PROBE_TIMEOUT, INFRASTRUCTURE_FAILURES);
  }

  /**
   * Creates a breaker with full control over what counts as a failure and how long a probe may stay
   * silent before being superseded, reading elapsed time from {@link System#nanoTime()}.
   *
   * @param failureThreshold consecutive eligible failures that open the circuit, {@code >= 1}
   * @param cooldown how long the circuit stays OPEN before allowing a probe; zero means probe
   *     immediately
   * @param probeTimeout how long a HALF_OPEN probe may run without reporting back before another
   *     request may take over as the new probe; zero means every HALF_OPEN request probes
   * @param failurePredicate returns true for failures that count toward the threshold
   */
  public CircuitBreakerCommandInterceptor(
      int failureThreshold,
      Duration cooldown,
      Duration probeTimeout,
      Predicate<Throwable> failurePredicate) {
    this(failureThreshold, cooldown, probeTimeout, failurePredicate, System::nanoTime);
  }

  /**
   * Creates a breaker with a custom monotonic time source for the cooldown and probe-timeout elapse
   * checks. The source must be monotonic and is read in nanoseconds (like {@link
   * System#nanoTime()}); inject a controllable source to test the OPEN→HALF_OPEN cooldown and the
   * silent-probe takeover boundaries deterministically. Production code uses the {@code
   * System::nanoTime} default via the four-argument constructor.
   *
   * @param failureThreshold consecutive eligible failures that open the circuit, {@code >= 1}
   * @param cooldown how long the circuit stays OPEN before allowing a probe; zero means probe
   *     immediately
   * @param probeTimeout how long a HALF_OPEN probe may run without reporting back before another
   *     request may take over as the new probe; zero means every HALF_OPEN request probes
   * @param failurePredicate returns true for failures that count toward the threshold
   * @param nanoTimeSource monotonic nanosecond time source for elapse checks; must be non-null
   */
  public CircuitBreakerCommandInterceptor(
      int failureThreshold,
      Duration cooldown,
      Duration probeTimeout,
      Predicate<Throwable> failurePredicate,
      LongSupplier nanoTimeSource) {
    if (failureThreshold < 1) {
      throw new IllegalArgumentException("failureThreshold must be >= 1");
    }
    if (cooldown == null || cooldown.isNegative()) {
      throw new IllegalArgumentException("cooldown must be non-null and non-negative");
    }
    if (probeTimeout == null || probeTimeout.isNegative()) {
      throw new IllegalArgumentException("probeTimeout must be non-null and non-negative");
    }
    if (failurePredicate == null) {
      throw new IllegalArgumentException("failurePredicate must be non-null");
    }
    if (nanoTimeSource == null) {
      throw new IllegalArgumentException("nanoTimeSource must be non-null");
    }
    this.failureThreshold = failureThreshold;
    this.cooldown = cooldown;
    this.probeTimeout = probeTimeout;
    this.failurePredicate = failurePredicate;
    this.nanoTimeSource = nanoTimeSource;
  }

  /** Pushes this execution's probe designation. Called only on the paths that admit the command. */
  private void pushProbe(boolean designated) {
    probeStack.get().push(designated);
  }

  /**
   * Pops this execution's probe designation, removing the ThreadLocal once the stack drains so a
   * pooled platform thread carries no residue. Returns {@code false} when there is no matching
   * {@code before()} — the same default the pre-stack {@code ThreadLocal.withInitial(() -> false)}
   * produced.
   */
  private boolean popProbe() {
    java.util.ArrayDeque<Boolean> frames = probeStack.get();
    Boolean top = frames.poll();
    if (frames.isEmpty()) {
      probeStack.remove();
    }
    return Boolean.TRUE.equals(top);
  }

  @Override
  public boolean before(CommandContext ctx) {
    State current = state.get();
    if (current == State.CLOSED) {
      pushProbe(false);
      return true;
    }
    if (current == State.OPEN) {
      if (nanoTimeSource.getAsLong() - openedAtNano >= cooldown.toNanos()) {
        // Record the probe start before the state CAS: a thread that observes HALF_OPEN must
        // never see a stale timestamp from a previous probe episode. Concurrent writers all
        // write "now", so losing this race is harmless.
        probeStartedAtNano.set(nanoTimeSource.getAsLong());
        if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
          pushProbe(true); // this execution is the designated probe
          return true;
        }
        // CAS failed — another thread already transitioned to HALF_OPEN. Nothing was pushed, so an
        // outer execution on this thread keeps its own designation.
        throw new CircuitBreakerOpenException(
            "Circuit breaker HALF_OPEN — probe in flight, rejecting: " + ctx.commandType());
      }
      throw new CircuitBreakerOpenException(
          "Circuit breaker OPEN — rejecting command: " + ctx.commandType());
    }
    // HALF_OPEN — another execution is probing. If that probe has been silent longer than
    // probeTimeout (hung connection, thread death without callbacks), take over as the new
    // probe; the CAS designates exactly one taker per timeout window.
    long probeStarted = probeStartedAtNano.get();
    if (nanoTimeSource.getAsLong() - probeStarted >= probeTimeout.toNanos()
        && probeStartedAtNano.compareAndSet(probeStarted, nanoTimeSource.getAsLong())) {
      pushProbe(true);
      return true;
    }
    throw new CircuitBreakerOpenException(
        "Circuit breaker HALF_OPEN — probe in flight, rejecting: " + ctx.commandType());
  }

  @Override
  public void after(CommandContext ctx) {
    boolean thisThreadIsProbe = popProbe();

    var result = ctx.result();
    if (result != null && result.vetoed()) {
      // A VETOED result means a later interceptor's before() returned false — a
      // maintenance-mode / tenant-suspended / kill-switch / rate-limit rejection. Nothing was
      // executed and no infrastructure was touched, so this callback carries no evidence.
      //
      // Closing the circuit on it would reopen full traffic against a database that is still
      // down; resetting failureCount on it is worse, because a rate limiter or per-tenant kill
      // switch vetoing even one command in five would keep zeroing the consecutive-failure
      // counter, so the breaker could never reach its threshold again — the protection defeated
      // permanently.
      //
      // The probe slot is handed back unused. openedAtNano is deliberately NOT restamped: the
      // cooldown already elapsed, and a rejection that learned nothing is no reason to make
      // recovery wait out another full cooldown, so the next request probes immediately. An
      // IDEMPOTENT_REPLAY is NOT vetoed — it hit the CommandInbox, a real round trip to the very
      // store the breaker guards — and still counts as a probe success below.
      //
      // "carries no evidence" is the framework-wide meaning of a veto, not a
      // breaker-local convenience. The saga paths draw the same blank from it — see
      // SagaCommandVetoedException — so a veto is neither an infrastructure failure nor a
      // permanent business rejection anywhere: CommandFailureClassification deliberately does not
      // classify it, because there is nothing to classify. Any new consumer of a VETOED result
      // must abstain here too; concluding EITHER way from it is the defect both fixes closed.
      if (thisThreadIsProbe) {
        state.compareAndSet(State.HALF_OPEN, State.OPEN);
      }
      return;
    }

    if (thisThreadIsProbe) {
      // Probe succeeded — close the circuit
      state.compareAndSet(State.HALF_OPEN, State.CLOSED);
    }
    // Reset failure count only if circuit is currently CLOSED (not if it just opened for peers)
    if (state.get() == State.CLOSED) {
      failureCount.set(0);
    }
  }

  @Override
  public void onError(CommandContext ctx, Throwable error) {
    if (error instanceof CircuitBreakerOpenException) {
      popProbe();
      return;
    }
    boolean thisThreadIsProbe = popProbe();
    if (!failurePredicate.test(error)) {
      // Client/domain rejection — not a breaker-eligible failure. For a probe this still proves
      // the infrastructure is reachable (the command got far enough to be rejected), so treat it
      // like a probe success and close the circuit.
      if (thisThreadIsProbe) {
        state.compareAndSet(State.HALF_OPEN, State.CLOSED);
        if (state.get() == State.CLOSED) {
          failureCount.set(0);
        }
      }
      return;
    }
    int failures = failureCount.incrementAndGet();
    if (thisThreadIsProbe && tripFromHalfOpen()) {
      return;
    }
    if (failures >= failureThreshold) {
      openedAtNano = nanoTimeSource.getAsLong();
      failureCount.set(0);
      state.set(State.OPEN);
    }
  }

  /**
   * Reopens the circuit on behalf of a failing probe, but only while the HALF_OPEN episode the
   * probe was designated for is still live. Returns whether the trip happened.
   *
   * <p>This used to be an unconditional {@code state.set(State.OPEN)} guarded by {@code failures >=
   * threshold || thisThreadIsProbe}, and a probe designation is not self-invalidating: the
   * silent-probe takeover at {@link #before} CASes {@code probeStartedAtNano} alone, so a taker
   * keeps its designation even after the probe it superseded finally reports back and CASes the
   * circuit CLOSED. The taker's next single failure — one connection reset, with the
   * consecutive-failure threshold nowhere near reached — then slammed a healthy, already-CLOSED
   * circuit OPEN and rejected every command on the bus for a full cooldown. The class javadoc's "a
   * late callback from a superseded probe is benign" covered only the ordering where the superseded
   * probe reports first; this is the reverse one.
   *
   * <p>The CAS is the enforcement point rather than a {@code state} re-check inside {@link
   * #before}: re-checking there is a check-then-act that cannot close the window (the state can
   * still change between the read and the callback), and rejecting the takeover on a CLOSED circuit
   * would newly fail a command the circuit is healthy enough to serve.
   *
   * <p>A declined trip does <em>not</em> discard the failure: {@code failureCount} keeps the
   * increment and the caller falls through to the ordinary threshold rule, so a genuinely sick
   * dependency still opens the circuit after {@code failureThreshold} consecutive failures.
   */
  private boolean tripFromHalfOpen() {
    // Stamp before the CAS so a thread that observes OPEN can never read a stale openedAtNano and
    // probe immediately. A stamp left behind by a declined CAS is inert: every transition INTO
    // OPEN writes its own.
    openedAtNano = nanoTimeSource.getAsLong();
    if (state.compareAndSet(State.HALF_OPEN, State.OPEN)) {
      failureCount.set(0);
      return true;
    }
    return false;
  }

  /** Returns the current circuit state name. Useful for monitoring and tests. */
  public String circuitState() {
    return state.get().name();
  }
}
