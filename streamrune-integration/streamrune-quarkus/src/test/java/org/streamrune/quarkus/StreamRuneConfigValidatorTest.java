package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.runtime.StartupEvent;
import io.smallrye.config.SmallRyeConfig;
import io.smallrye.config.SmallRyeConfigBuilder;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Singleton;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

class StreamRuneConfigValidatorTest {

  @SuppressWarnings("unchecked")
  private <T> Instance<T> unsatisfied() {
    Instance<T> inst = mock(Instance.class);
    when(inst.isResolvable()).thenReturn(false);
    return inst;
  }

  @SuppressWarnings("unchecked")
  private <T> Instance<T> satisfied(T value) {
    Instance<T> inst = mock(Instance.class);
    when(inst.isResolvable()).thenReturn(true);
    when(inst.get()).thenReturn(value);
    return inst;
  }

  @Test
  void validConfigPassesWithoutException() {
    var validator =
        new StreamRuneConfigValidator(TestProperties.defaults(), unsatisfied(), unsatisfied());
    assertDoesNotThrow(() -> validator.validate(mock(StartupEvent.class)));
  }

  @Test
  void invalidSnapshotEveryNEventsThrows() {
    var props = TestProperties.of(Map.of("streamrune.snapshot-every-n-events", "0"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("snapshot-every-n-events"));
  }

  /**
   * A negative {@code streamrune.lock-timeout} is never a valid StreamRune value — nothing on the
   * wiring path rejects it, so it used to reach whichever {@code AggregateLocker} was wired and
   * mean two different things ({@code LocalStripedLocker}: try once, do not wait; {@code
   * PgAdvisoryLocker}: substitute a 30s default and block). Identical rejection on all three
   * frameworks.
   */
  @Test
  void negativeLockTimeoutThrows() {
    var props = TestProperties.of(Map.of("streamrune.lock-timeout", "PT-1S"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("streamrune.lock-timeout"));
    assertTrue(ex.getMessage().contains("must not be negative"));
  }

  /** Zero is the documented "do not wait at all" value and must pass. */
  @Test
  void zeroLockTimeoutPasses() {
    var props = TestProperties.of(Map.of("streamrune.lock-timeout", "PT0S"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    assertDoesNotThrow(() -> validator.validate(mock(StartupEvent.class)));
  }

  /**
   * {@code PT0.0005S} is positive, so it passed the negative check, and {@code toMillis()} then
   * made it {@code SET LOCAL lock_timeout = '0ms'} inside {@code PgAdvisoryLocker} — PostgreSQL's
   * spelling of DISABLED, so a configured 0.5 ms bound became an unbounded server-side wait, while
   * {@code LocalStripedLocker} on the same value made a single attempt. Identical rejection on
   * Spring ({@code 500us}) and Micronaut.
   */
  @Test
  void subMillisecondLockTimeoutThrows() {
    var props = TestProperties.of(Map.of("streamrune.lock-timeout", "PT0.0005S"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("streamrune.lock-timeout"));
    assertTrue(ex.getMessage().contains("at least 1ms"));
  }

  /** The floor itself is a valid value and must pass. */
  @Test
  void oneMillisecondLockTimeoutPasses() {
    var props = TestProperties.of(Map.of("streamrune.lock-timeout", "PT0.001S"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    assertDoesNotThrow(() -> validator.validate(mock(StartupEvent.class)));
  }

  @Test
  void outboxEnabledWithoutStoreThrows() {
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("OutboxStore"));
  }

  @Test
  void outboxEnabledWithStorePassesWithoutException() {
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    var validator =
        new StreamRuneConfigValidator(
            props, satisfied(mock(org.streamrune.core.outbox.OutboxStore.class)), unsatisfied());
    assertDoesNotThrow(() -> validator.validate(mock(StartupEvent.class)));
  }

  @Test
  void sagaDeadLetterRetentionExceedingInboxThrows_whenStorePresent_regardlessOfSweeper() {
    // The dead-letter-retention <= inbox invariant guards
    // SagaDeadLetterReplayer, so it must fire whenever a SagaDeadLetterStore is present — even with
    // the compensation-retry sweeper disabled (which short-circuits startSweepers() before its own
    // copy of this check ran).
    var props =
        TestProperties.of(
            Map.of(
                "streamrune.saga.compensation-retry-enabled", "false",
                "streamrune.saga.dead-letter-retention-max-age", "PT720H",
                "streamrune.inbox.retention-max-age", "PT168H"));
    var validator =
        new StreamRuneConfigValidator(
            props,
            unsatisfied(),
            satisfied(mock(org.streamrune.core.saga.SagaDeadLetterStore.class)));
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("streamrune.saga.dead-letter-retention-max-age"));
  }

  @Test
  void sagaDeadLetterRetentionExceedingInbox_ignored_whenNoStore() {
    // No SagaDeadLetterStore bean => no replay support => nothing to guard; must not fail fast.
    var props =
        TestProperties.of(
            Map.of(
                "streamrune.saga.dead-letter-retention-max-age", "PT720H",
                "streamrune.inbox.retention-max-age", "PT168H"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    assertDoesNotThrow(() -> validator.validate(mock(StartupEvent.class)));
  }

  @Test
  void sagaDeadLetterRetentionConsistentWithInbox_passes_whenStorePresent() {
    var props =
        TestProperties.of(
            Map.of(
                "streamrune.saga.compensation-retry-enabled", "false",
                "streamrune.saga.dead-letter-retention-max-age", "PT168H",
                "streamrune.inbox.retention-max-age", "PT168H"));
    var validator =
        new StreamRuneConfigValidator(
            props,
            unsatisfied(),
            satisfied(mock(org.streamrune.core.saga.SagaDeadLetterStore.class)));
    assertDoesNotThrow(() -> validator.validate(mock(StartupEvent.class)));
  }

  // ── the saga kill switch must also disarm the saga-only boot invariants ─────────────

  /** The validator's constructor arguments exactly as REAL Arc resolves them at its bean site. */
  public record ValidatorWiring(
      StreamRuneQuarkusProperties properties,
      Instance<org.streamrune.core.outbox.OutboxStore> outboxStore,
      Instance<org.streamrune.core.saga.SagaDeadLetterStore> sagaDeadLetterStore) {}

  /**
   * The two beans a real Quarkus application supplies that raw Arc cannot — the Agroal {@code
   * DataSource} and the SmallRye-bound {@code @ConfigMapping} — configured with sagas DISABLED and
   * an inbox retention window shorter than the saga dead-letter default (7d), plus a producer whose
   * parameters mirror {@link StreamRuneConfigValidator}'s injection points one-for-one.
   *
   * <p>The validator is captured through this producer rather than being indexed as a bean itself
   * because it is {@code @ApplicationScoped} with a constructor-injected, non-default constructor:
   * a client proxy for it needs the build-time bytecode transformer the {@code io.quarkus} Gradle
   * plugin supplies and this raw-Arc harness deliberately does not. The injection points under test
   * — the {@code Instance} handles — are still resolved by genuine Arc resolution against the real
   * {@link StreamRuneProducers} signatures, which is the whole point of the repro.
   */
  @ApplicationScoped
  public static class SagasDisabledInfrastructure {

    @Produces
    @Singleton
    public DataSource dataSource() {
      return mock(DataSource.class);
    }

    @Produces
    @Singleton
    public StreamRuneQuarkusProperties properties() {
      return bindWithQuarkusConverters(
          Map.of(
              "streamrune.saga.enabled", "false",
              "streamrune.inbox.retention-max-age", "PT72H"));
    }

    @Produces
    @Singleton
    public ValidatorWiring validatorWiring(
        StreamRuneQuarkusProperties properties,
        Instance<org.streamrune.core.outbox.OutboxStore> outboxStore,
        Instance<org.streamrune.core.saga.SagaDeadLetterStore> sagaDeadLetterStore) {
      return new ValidatorWiring(properties, outboxStore, sagaDeadLetterStore);
    }
  }

  /**
   * {@code streamrune.saga.enabled=false} plus a tuned {@code streamrune.inbox.retention-max-age}
   * must BOOT. The dead-letter-retention invariant protects saga replay; with sagas off there is no
   * replay to protect, and the identical config boots on Spring and Micronaut.
   *
   * <p>Only a real Arc container reproduces it: {@code StreamRuneProducers.sagaDeadLetterStore} is
   * a dependent-scoped {@code @DefaultBean} producer that returns {@code null} when sagas are
   * disabled, so the bean DEFINITION exists and {@code Instance#isResolvable()} answers {@code
   * true} while {@code get()} hands back {@code null} — the exact null-product contract documented
   * in {@link SagaCompensationRetryLifecycle}. A mocked {@code Instance} cannot express that; the
   * wiring has to come from real Arc resolution against the real producer signature.
   */
  @Test
  void sagasDisabled_withTunedInboxRetention_bootsThroughRealArcContainer() throws Exception {
    try (var arc =
        RealArcTestContainer.boot(
            List.of(StreamRuneProducers.class, SagasDisabledInfrastructure.class))) {
      ValidatorWiring wiring = arc.container().instance(ValidatorWiring.class).get();

      assertTrue(
          wiring.sagaDeadLetterStore().isResolvable(),
          "precondition: the @DefaultBean producer's DEFINITION resolves even with sagas disabled");
      assertNull(
          wiring.sagaDeadLetterStore().get(),
          "precondition: the resolved PRODUCT is null with sagas disabled — isResolvable() is"
              + " therefore not a presence proxy");

      var validator =
          new StreamRuneConfigValidator(
              wiring.properties(), wiring.outboxStore(), wiring.sagaDeadLetterStore());
      assertDoesNotThrow(
          () -> validator.validate(new StartupEvent()),
          "sagas are disabled, so the saga dead-letter retention invariant must not fail boot");
    }
  }

  /** Binds the real {@code @ConfigMapping} through SmallRye with Quarkus's Duration converter. */
  private static StreamRuneQuarkusProperties bindWithQuarkusConverters(
      Map<String, String> overrides) {
    SmallRyeConfig config =
        new SmallRyeConfigBuilder()
            .withMapping(StreamRuneQuarkusProperties.class)
            .withConverter(
                Duration.class, 100, new io.quarkus.runtime.configuration.DurationConverter())
            .withDefaultValues(overrides)
            .build();
    return config.getConfigMapping(StreamRuneQuarkusProperties.class);
  }
}
