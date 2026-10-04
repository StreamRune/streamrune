package org.streamrune.spring;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Instant;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.streamrune.core.AggregateState;
import org.streamrune.core.Command;
import org.streamrune.core.DeadLetterQueue;
import org.streamrune.core.Decider;
import org.streamrune.core.DomainEvent;
import org.streamrune.core.EventStore;
import org.streamrune.core.EventStoreFactory;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.AggregateId;
import org.streamrune.core.types.AggregateType;
import org.streamrune.core.types.CommandId;
import org.streamrune.core.types.StreamId;
import org.streamrune.runtime.DeadLetterRetryRunner;
import org.streamrune.runtime.DeciderRegistration;
import org.streamrune.runtime.VirtualThreadCommandBus;
import org.streamrune.test.InMemoryCryptoEngine;
import org.streamrune.test.InMemoryDeadLetterQueue;

/**
 * Proves the dead-letter pipeline is wired without requiring a Jackson 2 {@code
 * com.fasterxml.jackson.databind.ObjectMapper} bean (Spring Boot 4 auto-configures Jackson 3, so
 * that bean is usually absent), and that the auto-configured command bus actually publishes failed
 * commands to a user-defined {@link DeadLetterQueue}.
 */
class DeadLetterWiringAutoConfigurationTest {

  private static final AggregateType TYPE = AggregateType.of("test");

  private final DeadLetterQueue deadLetterQueue = mock(DeadLetterQueue.class);

  private final ApplicationContextRunner runner =
      new ApplicationContextRunner()
          .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
          .withBean(DataSource.class, () -> mock(DataSource.class))
          .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
          .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
          .withBean(DeadLetterQueue.class, () -> deadLetterQueue);

  @Test
  void deadLetterRetryRunnerRegisteredWithoutJackson2ObjectMapperBean() {
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          assertThat(ctx).hasSingleBean(DeadLetterRetryRunner.class);
        });
  }

  @Test
  void deadLetterRetryRunnerDefaultsToNoopLeadership_whenNoLeadershipBean() {
    // Disable the single-active-consumer knob so no SubscriptionLeadership bean is registered ->
    // the DLQ runner defaults to NOOP (always leader = single-instance behavior unchanged).
    runner
        .withPropertyValues("streamrune.subscription.single-active-consumer.enabled=false")
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx)
                  .doesNotHaveBean(org.streamrune.core.subscription.SubscriptionLeadership.class);
              var dlqRunner = ctx.getBean(DeadLetterRetryRunner.class);
              assertThat(getField(dlqRunner, "leadership"))
                  .as("with no leadership bean the DLQ runner must default to NOOP")
                  .isSameAs(org.streamrune.core.subscription.SubscriptionLeadership.NOOP);
            });
  }

  @Test
  void deadLetterRetryRunnerReceivesLeadership_whenBeanPresent() {
    // Wiring proof: the resolvable SubscriptionLeadership bean must be threaded into the DLQ retry
    // runner so only the leader replica polls/retries. Mirrors how the projection auto-config
    // threads the same bean into projection runners. The knob is on by default and a DataSource is
    // present, so the auto-configured LeaseBasedLeadership bean is the one wired in.
    runner.run(
        ctx -> {
          assertThat(ctx).hasNotFailed();
          var leadershipBean =
              ctx.getBean(org.streamrune.core.subscription.SubscriptionLeadership.class);
          assertThat(leadershipBean)
              .isInstanceOf(org.streamrune.postgres.LeaseBasedLeadership.class);
          var dlqRunner = ctx.getBean(DeadLetterRetryRunner.class);
          assertThat(getField(dlqRunner, "leadership"))
              .as("the DLQ runner must receive the resolvable SubscriptionLeadership bean")
              .isSameAs(leadershipBean);
        });
  }

  private static Object getField(Object target, String fieldName) {
    try {
      var field = target.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(target);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void commandBusPublishesFailedCommandsToDeadLetterQueue() {
    runner
        .withBean(
            "failingDeciderRegistration",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    TYPE,
                    FailingCommand.class,
                    cmd -> AggregateId.of(cmd.id()),
                    new FailingDecider()))
        .run(
            ctx -> {
              var bus = ctx.getBean(VirtualThreadCommandBus.class);
              var command = new FailingCommand("agg-1", Instant.parse("2026-06-12T00:00:00Z"));

              assertThatThrownBy(() -> bus.execute(command)).hasMessageContaining("boom");

              // The DLQ bean is wired into the auto-configured bus, and the fallback Jackson 2
              // mapper serialized the payload (including the java.time field) without a
              // user-provided ObjectMapper bean.
              verify(deadLetterQueue).publish(any(DeadLetterQueue.DeadLetterPublishRequest.class));
            });
  }

  /**
   * Regression coverage for the defect this task fixes: the auto-configured command bus built its
   * DLQ publish mapper via {@code defaultDeadLetterObjectMapper()} with NO {@link CryptoEngine}
   * wiring, so an {@code @Encrypted} command field was persisted to the DLQ as plaintext PII. With
   * a {@link CryptoEngine} bean present, the fallback mapper must now be crypto-aware and the
   * stored payload must be ciphertext.
   */
  @Test
  void dlqPayloadIsEncryptedWhenCryptoEngineBeanIsPresent() {
    var realDlq = new InMemoryDeadLetterQueue();
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(DeadLetterQueue.class, () -> realDlq)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .withBean(
            "piiFailingDeciderRegistration",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    TYPE,
                    PiiCommand.class,
                    cmd -> AggregateId.of(cmd.id()),
                    new PiiFailingDecider()))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var bus = ctx.getBean(VirtualThreadCommandBus.class);
              var command = new PiiCommand("agg-1", "alice@example.com");

              assertThatThrownBy(() -> bus.execute(command)).hasMessageContaining("boom");

              assertThat(realDlq.all()).hasSize(1);
              assertThat(realDlq.all().getFirst().commandPayload())
                  .as("the stored DLQ payload must be ciphertext, never the plaintext PII")
                  .doesNotContain("alice@example.com");
            });
  }

  /**
   * Critical GDPR proof: even when the application defines its OWN Jackson 2 {@code
   * com.fasterxml.jackson.databind.ObjectMapper} bean with NO {@code CryptoShreddingModule}, the
   * auto-configured DLQ mapper must STILL be crypto-aware when a {@link CryptoEngine} bean is
   * present, so an {@code @Encrypted} command field is persisted as ciphertext, never plaintext
   * PII.
   *
   * <p>Before the fix the bus honored the application {@code ObjectMapper} bean verbatim ({@code
   * objectMapperProvider.getIfAvailable(...)}), so the plaintext email leaked into the DLQ payload.
   * After the fix the DLQ mapper is always {@link
   * StreamRuneAutoConfiguration#defaultDeadLetterObjectMapper(CryptoEngine)}, regardless of any app
   * mapper.
   */
  @Test
  void dlqPayloadIsEncryptedEvenWhenApplicationObjectMapperBeanIsPresent() {
    var realDlq = new InMemoryDeadLetterQueue();
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(DeadLetterQueue.class, () -> realDlq)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        // A plain application Jackson 2 mapper with NO CryptoShreddingModule: the pre-fix bug
        // honored this verbatim for the DLQ, leaking @Encrypted fields as plaintext.
        .withBean(
            com.fasterxml.jackson.databind.ObjectMapper.class,
            com.fasterxml.jackson.databind.ObjectMapper::new)
        .withBean(
            "piiFailingDeciderRegistrationAppMapper",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    TYPE,
                    PiiCommand.class,
                    cmd -> AggregateId.of(cmd.id()),
                    new PiiFailingDecider()))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var bus = ctx.getBean(VirtualThreadCommandBus.class);
              var command = new PiiCommand("agg-1", "carol@example.com");

              assertThatThrownBy(() -> bus.execute(command)).hasMessageContaining("boom");

              assertThat(realDlq.all()).hasSize(1);
              assertThat(realDlq.all().getFirst().commandPayload())
                  .as(
                      "the DLQ mapper must be crypto-aware even when the app defines its own"
                          + " ObjectMapper bean — the payload must be ciphertext, never plaintext"
                          + " PII")
                  .doesNotContain("carol@example.com");
            });
  }

  /**
   * Fail-fast proof: an {@code @Encrypted} COMMAND field with NO {@link CryptoEngine} configured
   * must fail startup with an {@link IllegalStateException} naming the offending command class,
   * instead of silently allowing plaintext PII to be dead-lettered.
   */
  @Test
  void encryptedCommandWithoutCryptoEngineFailsStartup() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(
            "piiDeciderRegistrationNoCrypto",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    TYPE,
                    PiiCommand.class,
                    cmd -> AggregateId.of(cmd.id()),
                    new PiiFailingDecider()))
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("PiiCommand")
                  .hasStackTraceContaining("email");
            });
  }

  /**
   * The companion to {@link #encryptedCommandWithoutCryptoEngineFailsStartup}: the identical wiring
   * WITH a {@link CryptoEngine} bean starts cleanly.
   */
  @Test
  void encryptedCommandWithCryptoEngineStartsCleanly() {
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(CryptoEngine.class, InMemoryCryptoEngine::new)
        .withBean(
            "piiDeciderRegistrationWithCrypto",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    TYPE,
                    PiiCommand.class,
                    cmd -> AggregateId.of(cmd.id()),
                    new PiiFailingDecider()))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              assertThat(ctx).hasSingleBean(VirtualThreadCommandBus.class);
            });
  }

  @Test
  void ambiguousCryptoEnginesFailStartupInsteadOfPersistingDlqPayloadsAsPlaintext() {
    runner
        .withUserConfiguration(TwoCryptoEnginesConfig.class)
        .run(
            ctx -> {
              assertThat(ctx).hasFailed();
              assertThat(ctx.getStartupFailure())
                  .hasStackTraceContaining("PLAINTEXT")
                  .hasStackTraceContaining("@Primary");
            });
  }

  @Test
  void primaryCryptoEngineResolvesAmbiguityForDlq() {
    var realDlq = new InMemoryDeadLetterQueue();
    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(DeadLetterQueue.class, () -> realDlq)
        .withUserConfiguration(PrimaryCryptoEngineConfig.class)
        .withBean(
            "piiFailingDeciderRegistration2",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    TYPE,
                    PiiCommand.class,
                    cmd -> AggregateId.of(cmd.id()),
                    new PiiFailingDecider()))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var bus = ctx.getBean(VirtualThreadCommandBus.class);
              var command = new PiiCommand("agg-1", "bob@example.com");

              assertThatThrownBy(() -> bus.execute(command)).hasMessageContaining("boom");

              assertThat(realDlq.all()).hasSize(1);
              assertThat(realDlq.all().getFirst().commandPayload())
                  .doesNotContain("bob@example.com");
            });
  }

  @Configuration
  static class TwoCryptoEnginesConfig {
    @Bean
    CryptoEngine engineA() {
      return new InMemoryCryptoEngine();
    }

    @Bean
    CryptoEngine engineB() {
      return new InMemoryCryptoEngine();
    }
  }

  @Configuration
  static class PrimaryCryptoEngineConfig {
    @Bean
    @Primary
    CryptoEngine primaryEngine() {
      return new InMemoryCryptoEngine();
    }

    @Bean
    CryptoEngine secondaryEngine() {
      return new InMemoryCryptoEngine();
    }
  }

  record PiiCommand(String id, @Encrypted(subjectId = "id") String email) implements Command {}

  record PiiFailingState() implements AggregateState {}

  record PiiFailingEvent() implements DomainEvent {}

  static class PiiFailingDecider implements Decider<PiiCommand, PiiFailingState, PiiFailingEvent> {
    @Override
    public PiiFailingState initialState() {
      return new PiiFailingState();
    }

    @Override
    public List<PiiFailingEvent> decide(PiiCommand command, PiiFailingState state) {
      throw new IllegalStateException("boom");
    }

    @Override
    public PiiFailingState evolve(PiiFailingState state, PiiFailingEvent event) {
      return state;
    }
  }

  /**
   * Reproduces the "empty commandTypeRegistry" defect: the auto-configured {@link
   * DeadLetterRetryRunner} must resolve a DLQ entry's command type using the SAME {@link
   * DeciderRegistration} beans the auto-configured command bus already consumes, not a hand-built
   * registry. A DLQ entry is published directly (bypassing the bus) keyed exactly the way {@code
   * VirtualThreadCommandBus.publishToDeadLetterQueue} keys it — {@code
   * command.getClass().getName()} — then a single on-demand {@link
   * DeadLetterRetryRunner#retry(CommandId)} is invoked on the AUTO-CONFIGURED runner bean. Before
   * the fix, the runner's command-type registry is empty, so the entry is "unknown command type"
   * and only its failed-attempt count is bumped — the decider never runs and the entry survives.
   * After the fix, the registry is populated from the injected {@link DeciderRegistration} bean, so
   * the command resolves, the decider actually executes, and the entry is discarded on success.
   */
  @Test
  void autoConfiguredRunnerResolvesCommandTypeFromDeciderRegistry() {
    var realDlq = new InMemoryDeadLetterQueue();
    var executed = new java.util.concurrent.atomic.AtomicBoolean(false);

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        // Deliberately no DataSource bean: its presence auto-configures a PostgresCommandInbox,
        // which would make the runner's replay take the keyed execute(command, key) path and fail
        // on a real JDBC connection this unit-style test does not have. Without it,
        // supportsIdempotentExecution() is false and the unkeyed execute(command) path is used —
        // exactly like the existing commandBusPublishesFailedCommandsToDeadLetterQueue test above.
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(DeadLetterQueue.class, () -> realDlq)
        .withBean(
            "succeedingDeciderRegistration",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    TYPE,
                    SucceedingCommand.class,
                    cmd -> AggregateId.of(cmd.id()),
                    new SucceedingDecider(executed)))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var retryRunner = ctx.getBean(DeadLetterRetryRunner.class);
              var objectMapper =
                  ctx.getBeanProvider(com.fasterxml.jackson.databind.ObjectMapper.class)
                      .getIfAvailable(
                          () ->
                              StreamRuneAutoConfiguration.defaultDeadLetterObjectMapper(
                                  null, org.streamrune.core.StreamRuneMetrics.NOOP));

              var commandId = CommandId.of("dlq-cmd-1");
              var command = new SucceedingCommand("agg-1");
              realDlq.publish(
                  new DeadLetterQueue.DeadLetterPublishRequest(
                      objectMapper.writeValueAsString(command),
                      // Exactly what VirtualThreadCommandBus.publishToDeadLetterQueue stores:
                      // its fully-qualified class name, command.getClass().getName().
                      SucceedingCommand.class.getName(),
                      commandId,
                      StreamId.of(TYPE, AggregateId.of("agg-1")),
                      "java.lang.IllegalStateException",
                      "boom",
                      1,
                      Instant.now(),
                      null,
                      null,
                      null,
                      null));

              boolean retried = retryRunner.retry(commandId);

              assertThat(retried).isTrue();
              assertThat(executed)
                  .as(
                      "decider must actually execute — the command type must resolve via the"
                          + " decider registry, not be skipped as unknown")
                  .isTrue();
              assertThat(realDlq.all())
                  .as("a successfully-retried entry is discarded, not left behind as unresolvable")
                  .isEmpty();
            });
  }

  /**
   * The guides register a decider by its sealed command root ({@code OrderCommand.class}), while
   * the bus persists the CONCRETE command's fully-qualified name. Seeding the runner with the
   * root's name alone left every dead-lettered command of the hierarchy unresolvable — retried as
   * "Unknown command type" until exhausted, never executed. The auto-configured runner must resolve
   * a concrete permitted command of a sealed registration.
   */
  @Test
  void autoConfiguredRunnerResolvesAConcreteCommandOfASealedDeciderRegistration() {
    var realDlq = new InMemoryDeadLetterQueue();
    var executed = new java.util.concurrent.atomic.AtomicBoolean(false);

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        // No DataSource bean, as in autoConfiguredRunnerResolvesCommandTypeFromDeciderRegistry:
        // the replay takes the unkeyed execute(command) path.
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(DeadLetterQueue.class, () -> realDlq)
        .withBean(
            "sealedDeciderRegistration",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    TYPE,
                    SealedCommand.class,
                    cmd -> AggregateId.of(cmd.id()),
                    new SealedDecider(executed)))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var retryRunner = ctx.getBean(DeadLetterRetryRunner.class);
              var objectMapper =
                  StreamRuneAutoConfiguration.defaultDeadLetterObjectMapper(
                      null, org.streamrune.core.StreamRuneMetrics.NOOP);

              var commandId = CommandId.of("dlq-sealed-1");
              realDlq.publish(
                  new DeadLetterQueue.DeadLetterPublishRequest(
                      objectMapper.writeValueAsString(new SealedCommand.Ship("agg-2")),
                      // What the bus persists: the concrete class, not the registered root.
                      SealedCommand.Ship.class.getName(),
                      commandId,
                      StreamId.of(TYPE, AggregateId.of("agg-2")),
                      "java.lang.IllegalStateException",
                      "boom",
                      1,
                      Instant.now(),
                      null,
                      null,
                      null,
                      null));

              assertThat(retryRunner.retry(commandId)).isTrue();
              assertThat(executed)
                  .as("the concrete command of the sealed registration must resolve and execute")
                  .isTrue();
              assertThat(realDlq.all()).isEmpty();
            });
  }

  /**
   * Replay-path companion to the publish-path crypto proof: the auto-configured {@link
   * DeadLetterRetryRunner}'s deserialize/replay mapper must be crypto-aware even when the
   * application defines its own {@code ObjectMapper} bean. A DLQ entry whose {@code @Encrypted}
   * field was stored as CIPHERTEXT is replayed via {@link DeadLetterRetryRunner#retry(CommandId)};
   * a crypto-aware runner decrypts it back to the plaintext the decider observes. Before the fix
   * the runner honored the plain app mapper, so it would deserialize the ciphertext verbatim and
   * the decider would see ciphertext, never the PII plaintext.
   */
  @Test
  void autoConfiguredRetryRunnerDeserializesWithCryptoAwareMapperEvenWithAppObjectMapper()
      throws Exception {
    var realDlq = new InMemoryDeadLetterQueue();
    var cryptoEngine = new InMemoryCryptoEngine();
    var capturedEmail = new java.util.concurrent.atomic.AtomicReference<String>();

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(StreamRuneAutoConfiguration.class))
        .withBean(EventStore.class, org.streamrune.test.InMemoryEventStore::new)
        .withBean(EventStoreFactory.class, SpringTestMocks::eventStoreFactoryReturningMockStore)
        .withBean(DeadLetterQueue.class, () -> realDlq)
        .withBean(CryptoEngine.class, () -> cryptoEngine)
        // A plain application Jackson 2 mapper with NO CryptoShreddingModule: the pre-fix bug
        // honored this verbatim for the runner's replay mapper, so the @Encrypted field would be
        // read back as ciphertext instead of decrypted.
        .withBean(
            com.fasterxml.jackson.databind.ObjectMapper.class,
            com.fasterxml.jackson.databind.ObjectMapper::new)
        .withBean(
            "piiCapturingDeciderRegistration",
            DeciderRegistration.class,
            () ->
                new DeciderRegistration<>(
                    TYPE,
                    PiiCommand.class,
                    cmd -> AggregateId.of(cmd.id()),
                    new PiiCapturingDecider(capturedEmail)))
        .run(
            ctx -> {
              assertThat(ctx).hasNotFailed();
              var retryRunner = ctx.getBean(DeadLetterRetryRunner.class);

              // Store the entry exactly as VirtualThreadCommandBus.publishToDeadLetterQueue would:
              // payload serialized by a CRYPTO-AWARE mapper, so the @Encrypted email is CIPHERTEXT
              // at rest.
              var cryptoAwareMapper = DeadLetterRetryRunner.createObjectMapper(cryptoEngine);
              var command = new PiiCommand("agg-1", "carol@example.com");
              var payload = cryptoAwareMapper.writeValueAsString(command);
              assertThat(payload)
                  .as("precondition: the stored payload must be ciphertext, not plaintext PII")
                  .doesNotContain("carol@example.com");

              var commandId = CommandId.of("dlq-pii-1");
              realDlq.publish(
                  new DeadLetterQueue.DeadLetterPublishRequest(
                      payload,
                      PiiCommand.class.getName(),
                      commandId,
                      StreamId.of(TYPE, AggregateId.of("agg-1")),
                      "java.lang.IllegalStateException",
                      "boom",
                      1,
                      Instant.now(),
                      null,
                      null,
                      null,
                      null));

              boolean retried = retryRunner.retry(commandId);

              assertThat(retried).isTrue();
              assertThat(capturedEmail.get())
                  .as(
                      "the auto-configured runner's replay mapper must be crypto-aware even when the"
                          + " app defines its own ObjectMapper — it must DECRYPT the @Encrypted"
                          + " field back to plaintext, not deserialize the ciphertext verbatim")
                  .isEqualTo("carol@example.com");
            });
  }

  record FailingCommand(String id, Instant requestedAt) implements Command {}

  record FailingState() implements AggregateState {}

  record FailingEvent() implements DomainEvent {}

  static class FailingDecider implements Decider<FailingCommand, FailingState, FailingEvent> {
    @Override
    public FailingState initialState() {
      return new FailingState();
    }

    @Override
    public List<FailingEvent> decide(FailingCommand command, FailingState state) {
      throw new IllegalStateException("boom");
    }

    @Override
    public FailingState evolve(FailingState state, FailingEvent event) {
      return state;
    }
  }

  sealed interface SealedCommand extends Command {
    String id();

    record Ship(String id) implements SealedCommand {}

    record Hold(String id) implements SealedCommand {}
  }

  static class SealedDecider implements Decider<SealedCommand, SucceedingState, SucceedingEvent> {
    private final java.util.concurrent.atomic.AtomicBoolean executed;

    SealedDecider(java.util.concurrent.atomic.AtomicBoolean executed) {
      this.executed = executed;
    }

    @Override
    public SucceedingState initialState() {
      return new SucceedingState();
    }

    @Override
    public List<SucceedingEvent> decide(SealedCommand command, SucceedingState state) {
      executed.set(true);
      return List.of(new SucceedingEvent());
    }

    @Override
    public SucceedingState evolve(SucceedingState state, SucceedingEvent event) {
      return state;
    }
  }

  record SucceedingCommand(String id) implements Command {}

  record SucceedingState() implements AggregateState {}

  record SucceedingEvent() implements DomainEvent {}

  static class SucceedingDecider
      implements Decider<SucceedingCommand, SucceedingState, SucceedingEvent> {
    private final java.util.concurrent.atomic.AtomicBoolean executed;

    SucceedingDecider(java.util.concurrent.atomic.AtomicBoolean executed) {
      this.executed = executed;
    }

    @Override
    public SucceedingState initialState() {
      return new SucceedingState();
    }

    @Override
    public List<SucceedingEvent> decide(SucceedingCommand command, SucceedingState state) {
      executed.set(true);
      return List.of(new SucceedingEvent());
    }

    @Override
    public SucceedingState evolve(SucceedingState state, SucceedingEvent event) {
      return state;
    }
  }

  record PiiCapturingState() implements AggregateState {}

  record PiiCapturingEvent() implements DomainEvent {}

  /** Captures the {@code email} of the {@link PiiCommand} it receives, then produces one event. */
  static class PiiCapturingDecider
      implements Decider<PiiCommand, PiiCapturingState, PiiCapturingEvent> {
    private final java.util.concurrent.atomic.AtomicReference<String> capturedEmail;

    PiiCapturingDecider(java.util.concurrent.atomic.AtomicReference<String> capturedEmail) {
      this.capturedEmail = capturedEmail;
    }

    @Override
    public PiiCapturingState initialState() {
      return new PiiCapturingState();
    }

    @Override
    public List<PiiCapturingEvent> decide(PiiCommand command, PiiCapturingState state) {
      capturedEmail.set(command.email());
      return List.of(new PiiCapturingEvent());
    }

    @Override
    public PiiCapturingState evolve(PiiCapturingState state, PiiCapturingEvent event) {
      return state;
    }
  }
}
