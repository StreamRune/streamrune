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
    Instance.Handle<T> handle = mock(Instance.Handle.class);
    when(handle.get()).thenReturn(value);
    when(inst.getHandle()).thenReturn(handle);
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

  /**
   * An unknown spelling of {@code streamrune.subscription.single-active-consumer.enabled} fails
   * startup, with or without a {@code DataSource}: it used to read as {@code false}, silently
   * leaving every replica leading.
   */
  @Test
  void unknownSingleActiveConsumerSpellingFailsStartup() {
    var props =
        TestProperties.of(
            Map.of("streamrune.subscription.single-active-consumer.enabled", "maybe"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertEquals(
        "streamrune.subscription.single-active-consumer.enabled must be true or false"
            + " (case-insensitive; on/off, yes/no, y/n, t/f and 1/0 are accepted too); got:"
            + " 'maybe'",
        ex.getMessage());
  }

  /** An unknown {@code streamrune.gdpr.snapshot-purge} fails startup, crypto engine or not. */
  @Test
  void unknownSnapshotPurgeValueFailsStartup() {
    var props = TestProperties.of(Map.of("streamrune.gdpr.snapshot-purge", "maybe"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertEquals(
        "streamrune.gdpr.snapshot-purge must be required or none (case-insensitive); got: 'maybe'",
        ex.getMessage());
  }

  /**
   * The validator builds the forget service on the startup event, so a refused build (no snapshot
   * purger) stops startup instead of the first request that injects it.
   */
  @Test
  void aRefusedForgetServiceFailsStartup() {
    @SuppressWarnings("unchecked")
    Instance<org.streamrune.runtime.gdpr.ForgetSubjectService> forget = mock(Instance.class);
    when(forget.isResolvable()).thenReturn(true);
    when(forget.get()).thenThrow(new IllegalStateException("no snapshot purger"));
    var validator =
        new StreamRuneConfigValidator(
            TestProperties.defaults(), unsatisfied(), unsatisfied(), forget);
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertEquals("no snapshot purger", ex.getMessage());
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
    // The only OutboxStore is PostgresOutboxStore; the broker modules ship publishers.
    assertTrue(ex.getMessage().contains("no OutboxStore found"), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("Declare a PostgresOutboxStore bean (streamrune-postgres)"),
        ex.getMessage());
    assertTrue(ex.getMessage().contains("provide only the OutboxPublisher"), ex.getMessage());
  }

  @Test
  void outboxEnabledWithStoreAndPublisherPassesWithoutException() {
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    var validator =
        new StreamRuneConfigValidator(
            props,
            satisfied(mock(org.streamrune.core.outbox.OutboxStore.class)),
            satisfied(mock(org.streamrune.core.outbox.OutboxPublisher.class)),
            beanManagerWithOutboxPollers(),
            unsatisfied());
    assertDoesNotThrow(() -> validator.validate(mock(StartupEvent.class)));
  }

  /**
   * Without a publisher the framework's poller is never produced, so the entries the event store
   * writes would stay {@code PENDING} for ever. This was a WARN; it refuses the boot now, as on
   * Spring and Micronaut.
   */
  @Test
  void outboxEnabledWithStoreButNoPublisherThrows() {
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    var validator =
        new StreamRuneConfigValidator(
            props,
            satisfied(mock(org.streamrune.core.outbox.OutboxStore.class)),
            unsatisfied(),
            beanManagerWithOutboxPollers(frameworkPoller()),
            unsatisfied());
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("no OutboxPublisher found"), ex.getMessage());
    assertTrue(
        ex.getMessage().contains("no OutboxPoller can relay the outbox entries"), ex.getMessage());
  }

  @Test
  void outboxEnabledWithoutPublisherButWithAnApplicationPollerPasses() {
    var props = TestProperties.of(Map.of("streamrune.outbox.enabled", "true"));
    jakarta.enterprise.inject.spi.Bean<?> applicationPoller =
        mock(jakarta.enterprise.inject.spi.Bean.class);
    when(applicationPoller.getQualifiers())
        .thenReturn(java.util.Set.of(jakarta.enterprise.inject.Default.Literal.INSTANCE));
    var validator =
        new StreamRuneConfigValidator(
            props,
            satisfied(mock(org.streamrune.core.outbox.OutboxStore.class)),
            unsatisfied(),
            beanManagerWithOutboxPollers(frameworkPoller(), applicationPoller),
            unsatisfied());
    assertDoesNotThrow(() -> validator.validate(mock(StartupEvent.class)));
  }

  /** The framework's own poller definition: the {@code @Named} producer, always present. */
  private static jakarta.enterprise.inject.spi.Bean<?> frameworkPoller() {
    jakarta.enterprise.inject.spi.Bean<?> bean = mock(jakarta.enterprise.inject.spi.Bean.class);
    when(bean.getQualifiers())
        .thenReturn(
            java.util.Set.of(
                jakarta.enterprise.inject.literal.NamedLiteral.of(
                    StreamRuneProducers.FRAMEWORK_OUTBOX_POLLER)));
    return bean;
  }

  private static jakarta.enterprise.inject.spi.BeanManager beanManagerWithOutboxPollers(
      jakarta.enterprise.inject.spi.Bean<?>... beans) {
    var beanManager = mock(jakarta.enterprise.inject.spi.BeanManager.class);
    when(beanManager.getBeans(
            org.mockito.ArgumentMatchers.eq(org.streamrune.runtime.OutboxPoller.class),
            org.mockito.ArgumentMatchers.<java.lang.annotation.Annotation>any()))
        .thenReturn(java.util.Set.of(beans));
    return beanManager;
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

  // ── Command dead-letter retention vs inbox retention ──────────────────────────────────────────

  @Test
  void deadLetterRetentionLongerThanTheInboxWindowFailsStartup() {
    var props = TestProperties.of(Map.of("streamrune.dead-letter.retention-max-age", "PT720H"));
    var validator =
        new StreamRuneConfigValidator(
            props,
            unsatisfied(),
            unsatisfied(),
            satisfied(mock(org.streamrune.core.CommandInbox.class)),
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)));
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("streamrune.dead-letter.retention-max-age"));
    assertTrue(ex.getMessage().contains("streamrune.inbox.retention-max-age"));
  }

  @Test
  void deadLetterRetentionIsJudgedEvenWithSagasAndTheAutomaticRetryOff() {
    var props =
        TestProperties.of(
            Map.of(
                "streamrune.dead-letter.retention-max-age", "PT0S",
                "streamrune.dead-letter.enabled", "false",
                "streamrune.saga.enabled", "false"));
    var validator =
        new StreamRuneConfigValidator(
            props,
            unsatisfied(),
            unsatisfied(),
            satisfied(mock(org.streamrune.core.CommandInbox.class)),
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)));
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("pruning disabled"), ex.getMessage());
  }

  @Test
  void theDefaultDeadLetterAndInboxRetentionPass() {
    var validator =
        new StreamRuneConfigValidator(
            TestProperties.defaults(),
            unsatisfied(),
            unsatisfied(),
            satisfied(mock(org.streamrune.core.CommandInbox.class)),
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)));
    assertDoesNotThrow(() -> validator.validate(mock(StartupEvent.class)));
  }

  @Test
  void deadLetterRetentionIsNotJudgedWithoutBothStores() {
    var props = TestProperties.of(Map.of("streamrune.dead-letter.retention-max-age", "PT720H"));
    var noInbox =
        new StreamRuneConfigValidator(
            props,
            unsatisfied(),
            unsatisfied(),
            unsatisfied(),
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)));
    assertDoesNotThrow(() -> noInbox.validate(mock(StartupEvent.class)));
    // A producer that resolves but hands back null is no store.
    var nullQueue =
        new StreamRuneConfigValidator(
            props,
            unsatisfied(),
            unsatisfied(),
            satisfied(mock(org.streamrune.core.CommandInbox.class)),
            satisfied(null));
    assertDoesNotThrow(() -> nullQueue.validate(mock(StartupEvent.class)));
  }

  @Test
  void theRetentionCheckDestroysTheDependentInstancesItResolved() {
    @SuppressWarnings("unchecked")
    Instance<org.streamrune.core.CommandInbox> inbox = mock(Instance.class);
    @SuppressWarnings("unchecked")
    Instance.Handle<org.streamrune.core.CommandInbox> handle = mock(Instance.Handle.class);
    when(inbox.isResolvable()).thenReturn(true);
    when(inbox.getHandle()).thenReturn(handle);
    when(handle.get()).thenReturn(mock(org.streamrune.core.CommandInbox.class));
    var validator =
        new StreamRuneConfigValidator(
            TestProperties.defaults(),
            unsatisfied(),
            unsatisfied(),
            inbox,
            satisfied(mock(org.streamrune.core.DeadLetterQueue.class)));

    validator.validate(mock(StartupEvent.class));

    org.mockito.Mockito.verify(handle).close();
    org.mockito.Mockito.verify(inbox, org.mockito.Mockito.never()).get();
  }

  @Test
  void anUnknownUnkeyedReplayValueFailsStartup() {
    var props = TestProperties.of(Map.of("streamrune.dead-letter.unkeyed-replay", "sometimes"));
    var validator = new StreamRuneConfigValidator(props, unsatisfied(), unsatisfied());
    var ex =
        assertThrows(
            IllegalStateException.class, () -> validator.validate(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("streamrune.dead-letter.unkeyed-replay"));
  }
}
