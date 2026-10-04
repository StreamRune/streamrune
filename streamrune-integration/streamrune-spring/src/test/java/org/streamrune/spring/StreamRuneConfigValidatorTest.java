package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;

class StreamRuneConfigValidatorTest {

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(javax.sql.DataSource.class, () -> mock(javax.sql.DataSource.class))
          .withBean(EventStore.class, () -> mock(EventStore.class))
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore);

  @Test
  void failsWhenSnapshotIntervalIsZero() {
    runner
        .withPropertyValues("streamrune.snapshot-every-n-events=0")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              // StreamRuneProperties is constructor-bound, so the configured 0 actually reaches
              // SnapshotPolicy and fails fast at command-bus creation — before the
              // StreamRuneConfigValidator (a SmartInitializingSingleton) gets a chance to run.
              assertThat(ctx.getStartupFailure())
                  .rootCause()
                  .hasMessageContaining("n must be >= 1");
            });
  }

  /**
   * A negative {@code streamrune.lock-timeout} is never a valid StreamRune value. It is not
   * rejected anywhere on the wiring path (the {@code VirtualThreadCommandBus.Builder} takes it
   * verbatim), so before this check it reached whichever {@code AggregateLocker} was wired and
   * meant two different things — {@code LocalStripedLocker} tried once without waiting, {@code
   * PgAdvisoryLocker} substituted a 30s default and blocked. The operator must learn at startup,
   * not under contention after moving to multi-replica.
   */
  @Test
  void failsWhenLockTimeoutIsNegative() {
    runner
        .withPropertyValues("streamrune.lock-timeout=-1s")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasMessageContaining("streamrune.lock-timeout")
                  .hasMessageContaining("must not be negative");
            });
  }

  /** Zero is the documented "do not wait at all" value and must boot. */
  @Test
  void bootsWhenLockTimeoutIsZero() {
    runner
        .withPropertyValues("streamrune.lock-timeout=0s")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  /**
   * Spring's DurationStyle accepts {@code us}/{@code ns}, so {@code 500us} binds to a positive
   * Duration the negative check waved through — and {@code toMillis()} then made it {@code SET
   * LOCAL lock_timeout = '0ms'} inside {@code PgAdvisoryLocker}, which PostgreSQL reads as
   * DISABLED: a configured 0.5 ms bound became an unbounded server-side wait, while {@code
   * LocalStripedLocker} on the same value made a single attempt. Identical rejection on Quarkus and
   * Micronaut ({@code PT0.0005S}).
   */
  @Test
  void failsWhenLockTimeoutIsPositiveButBelowOneMillisecond() {
    runner
        .withPropertyValues("streamrune.lock-timeout=500us")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasMessageContaining("streamrune.lock-timeout")
                  .hasMessageContaining("at least 1ms");
            });
  }

  /** The floor itself is a valid value and must boot. */
  @Test
  void bootsWhenLockTimeoutIsExactlyOneMillisecond() {
    runner
        .withPropertyValues("streamrune.lock-timeout=1ms")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @Test
  void failsWhenOutboxEnabledButNoStore() {
    runner
        .withPropertyValues("streamrune.outbox.enabled=true")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure()).hasMessageContaining("OutboxStore");
            });
  }

  @Test
  void failsWhenSagaDeadLetterRetentionExceedsInbox_evenWhenCompensationRetryDisabled() {
    // The dead-letter-retention <= inbox invariant is a
    // REPLAY-safety invariant, independent of the compensation-retry sweeper. Disabling the sweeper
    // (a legitimate config) must NOT skip the boot fail-fast — otherwise an operator boots silently
    // into the double-refund window. The always-on config validator fires whenever a
    // SagaDeadLetterStore bean is present (here: auto-configured from the mock DataSource).
    runner
        .withPropertyValues(
            "streamrune.saga.compensation-retry-enabled=false",
            "streamrune.saga.dead-letter-retention-max-age=30d",
            "streamrune.inbox.retention-max-age=7d")
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasMessageContaining("streamrune.saga.dead-letter-retention-max-age");
            });
  }

  @Test
  void bootsWhenCompensationRetryDisabledAndSagaRetentionConsistent() {
    runner
        .withPropertyValues(
            "streamrune.saga.compensation-retry-enabled=false",
            "streamrune.saga.dead-letter-retention-max-age=7d",
            "streamrune.inbox.retention-max-age=7d")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @Test
  void skipsSagaRetentionValidation_whenSagasDisabled_evenWithUserSuppliedDeadLetterStore() {
    // The saga kill switch must disarm the saga-only boot
    // invariants on every framework. Spring gates the check on bean PRESENCE, which the framework's
    // own store no longer satisfies when sagas are off — but a user-supplied SagaDeadLetterStore
    // (custom schema/store, kept wired while sagas are switched off during an incident) does, so
    // the replay invariant still fired for a replayer that can never run.
    runner
        .withBean(
            org.streamrune.core.saga.SagaDeadLetterStore.class,
            () -> mock(org.streamrune.core.saga.SagaDeadLetterStore.class))
        .withPropertyValues(
            "streamrune.saga.enabled=false",
            "streamrune.saga.dead-letter-retention-max-age=30d",
            "streamrune.inbox.retention-max-age=7d")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }

  @Test
  void skipsSagaRetentionValidation_whenNoDeadLetterStore() {
    // Gate: with sagas disabled there is no SagaDeadLetterStore bean (no replay support), so the
    // dead-letter>inbox config is not a hazard and must NOT fail fast.
    runner
        .withPropertyValues(
            "streamrune.saga.enabled=false",
            "streamrune.saga.dead-letter-retention-max-age=30d",
            "streamrune.inbox.retention-max-age=7d")
        .run(ctx -> assertThat(ctx).hasNotFailed());
  }
}
