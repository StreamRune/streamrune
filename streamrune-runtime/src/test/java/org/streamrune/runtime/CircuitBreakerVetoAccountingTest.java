package org.streamrune.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Command;
import org.streamrune.core.CommandBus;
import org.streamrune.core.CommandInterceptor.CommandContext;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.GlobalOffset;
import org.streamrune.core.types.StreamId;
import org.streamrune.core.types.Version;

/**
 * A VETOED short-circuit must not be accounted as a healthy execution.
 *
 * <p>An application interceptor that vetoes (maintenance mode, tenant-suspended, kill switch, rate
 * limit) sorts INNERMOST by default, so it runs after the circuit breaker. The bus then delivers
 * {@code after()} with a VETOED {@link CommandBus.CommandResult} to every interceptor whose {@code
 * before()} completed — including the breaker. Nothing was executed and no infrastructure was
 * touched, so that callback carries no evidence about downstream health: it must neither close a
 * HALF_OPEN circuit nor reset the consecutive-failure counter.
 */
class CircuitBreakerVetoAccountingTest {

  private static final AggregateType TYPE = AggregateType.of("agg");

  record TestCommand() implements Command {}

  private static CommandContext ctx(CommandBus.CommandResult result) {
    return new CommandContext(
        new TestCommand(),
        "TestCommand",
        CommandId.of("cmd-1"),
        TYPE,
        AggregateId.of("agg-1"),
        result,
        Instant.now());
  }

  /** The exact result the bus builds for an interceptor veto (VirtualThreadCommandBus). */
  private static CommandContext vetoed() {
    return ctx(
        new CommandBus.CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("agg-1")),
            Version.initial(),
            List.of(),
            List.of(),
            "MaintenanceModeInterceptor",
            CommandBus.ShortCircuitReason.VETOED));
  }

  /** An idempotent replay DID touch the inbox, so it is real evidence of a healthy store. */
  private static CommandContext idempotentReplay() {
    return ctx(
        new CommandBus.CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("agg-1")),
            new Version(1),
            List.of(GlobalOffset.of(1)),
            List.of(),
            "key-1",
            CommandBus.ShortCircuitReason.IDEMPOTENT_REPLAY));
  }

  private static CommandContext executed() {
    return ctx(
        new CommandBus.CommandResult(
            List.of(),
            StreamId.of(TYPE, AggregateId.of("agg-1")),
            new Version(1),
            List.of(GlobalOffset.of(1))));
  }

  /** Controllable monotonic nano source. */
  private static final class FakeNanoClock implements LongSupplier {
    private long now;

    FakeNanoClock(long start) {
      this.now = start;
    }

    void set(long value) {
      this.now = value;
    }

    @Override
    public long getAsLong() {
      return now;
    }
  }

  @Test
  void vetoedShortCircuit_doesNotCloseHalfOpenCircuit() {
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ZERO);
    cb.onError(ctx(null), new RuntimeException("database down")); // OPEN
    assertTrue(cb.before(ctx(null)), "zero cooldown admits a probe immediately");
    assertEquals("HALF_OPEN", cb.circuitState());

    // The user's maintenance-mode interceptor vetoed this command: nothing ran, the database was
    // never touched. The probe reported no verdict.
    cb.after(vetoed());

    assertEquals(
        "OPEN",
        cb.circuitState(),
        "a veto proves nothing about the infrastructure — it must not close the circuit");
  }

  @Test
  void vetoedProbe_returnsTheProbeSlotWithoutRestartingTheCooldown() {
    long start = 1_000_000_000L;
    long cooldownNanos = Duration.ofSeconds(30).toNanos();
    var clock = new FakeNanoClock(start);
    var cb =
        new CircuitBreakerCommandInterceptor(
            1,
            Duration.ofSeconds(30),
            Duration.ofMinutes(5),
            CircuitBreakerCommandInterceptor.INFRASTRUCTURE_FAILURES,
            clock);

    cb.onError(ctx(null), new RuntimeException("database down")); // OPEN at `start`
    clock.set(start + cooldownNanos);
    assertTrue(cb.before(ctx(null)), "cooldown elapsed — one probe admitted");
    cb.after(vetoed()); // the probe was vetoed before it could touch anything

    assertEquals("OPEN", cb.circuitState());
    // The slot is handed back unused: the very next request probes immediately instead of waiting
    // out a second cooldown that a rejection did nothing to justify.
    assertTrue(
        cb.before(ctx(null)),
        "the unused probe slot is returned — recovery is not delayed by a veto");
    assertEquals("HALF_OPEN", cb.circuitState());
  }

  @Test
  void vetoedShortCircuit_doesNotResetTheConsecutiveFailureCount() {
    // A rate limiter or per-tenant kill switch that vetoes even one command in five would
    // otherwise keep zeroing the counter, so the breaker could never reach its threshold again.
    var cb = new CircuitBreakerCommandInterceptor(5, Duration.ofMinutes(5));
    for (int i = 0; i < 4; i++) {
      cb.onError(ctx(null), new RuntimeException("database down"));
    }
    assertEquals("CLOSED", cb.circuitState(), "4 of 5 — not open yet");

    cb.after(vetoed()); // vetoed command: no execution, no evidence of recovery

    cb.onError(ctx(null), new RuntimeException("database down")); // the 5th consecutive failure
    assertEquals(
        "OPEN", cb.circuitState(), "a veto must not erase the consecutive-failure history");
  }

  @Test
  void executedCommand_stillResetsTheFailureCount() {
    var cb = new CircuitBreakerCommandInterceptor(5, Duration.ofMinutes(5));
    for (int i = 0; i < 4; i++) {
      cb.onError(ctx(null), new RuntimeException("database down"));
    }
    cb.after(executed()); // a real execution DID reach the store
    cb.onError(ctx(null), new RuntimeException("database down"));
    assertEquals("CLOSED", cb.circuitState(), "the counter restarts after a genuine success");
  }

  @Test
  void idempotentReplayResult_stillCountsAsProbeSuccess() {
    // A replay hit the CommandInbox — a real round trip to the very store the breaker guards.
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ZERO);
    cb.onError(ctx(null), new RuntimeException("database down")); // OPEN
    assertTrue(cb.before(ctx(null)));
    cb.after(idempotentReplay());
    assertEquals("CLOSED", cb.circuitState());
  }

  @Test
  void nullResult_isStillTreatedAsSuccess() {
    // A bus that supplies no result passes null; that means "the command ran".
    var cb = new CircuitBreakerCommandInterceptor(1, Duration.ZERO);
    cb.onError(ctx(null), new RuntimeException("database down")); // OPEN
    assertTrue(cb.before(ctx(null)));
    cb.after(ctx(null));
    assertEquals("CLOSED", cb.circuitState());
  }
}
