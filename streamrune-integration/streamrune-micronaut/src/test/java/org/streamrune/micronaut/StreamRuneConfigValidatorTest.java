package org.streamrune.micronaut;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.event.ApplicationEventListener;
import io.micronaut.context.event.StartupEvent;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

class StreamRuneConfigValidatorTest {

  /**
   * The validator must listen on the context-level {@link StartupEvent}, which fires in every
   * Micronaut context including headless (non-HTTP) workers — not the HTTP-server-only
   * ServerStartupEvent, which would skip snapshot/outbox validation entirely in a headless app.
   * This matches the sibling SagaStateCryptoValidator / StreamRuneAuthorizationValidator.
   */
  @Test
  void listensOnContextLevelStartupEvent_soHeadlessAppsValidate() {
    ParameterizedType listenerIface =
        (ParameterizedType)
            Arrays.stream(StreamRuneConfigValidator.class.getGenericInterfaces())
                .filter(
                    t ->
                        t instanceof ParameterizedType p
                            && p.getRawType() == ApplicationEventListener.class)
                .findFirst()
                .orElseThrow(() -> new AssertionError("must implement ApplicationEventListener"));
    Type eventType = listenerIface.getActualTypeArguments()[0];
    assertEquals(
        StartupEvent.class,
        eventType,
        "must listen on context-level StartupEvent so headless (non-HTTP) apps validate");
  }

  @Test
  void validConfigPassesWithoutException() {
    var props = StreamRuneMicronautProperties.withDefaults();
    var ctx = mock(ApplicationContext.class);
    var validator = new StreamRuneConfigValidator(props, ctx);
    assertDoesNotThrow(() -> validator.onApplicationEvent(mock(StartupEvent.class)));
  }

  @Test
  void invalidSnapshotEveryNEventsThrows() {
    var props =
        new StreamRuneMicronautProperties(
            0,
            3,
            50,
            2.0,
            5000,
            1000,
            Duration.ofSeconds(5),
            1024,
            10_000,
            5,
            Duration.ofSeconds(30),
            false,
            false,
            true,
            Duration.ofDays(30),
            false,
            100,
            5000L,
            true,
            5,
            60000L,
            Duration.ofDays(30),
            true,
            "streamrune",
            true,
            true,
            true,
            Duration.ofDays(7),
            Duration.ofDays(7),
            java.util.List.of(),
            Duration.ofDays(30),
            true,
            10,
            1000L,
            2.0,
            Duration.ofSeconds(15),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            // event-store.statement-timeout
            java.time.Duration.ofSeconds(30),
            // sse.polling-interval
            Duration.ofSeconds(1));
    var ctx = mock(ApplicationContext.class);
    var validator = new StreamRuneConfigValidator(props, ctx);
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> validator.onApplicationEvent(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("snapshot-every-n-events"));
  }

  /**
   * withDefaults() with only {@code lockTimeout} (positional component 7) parameterized, so a
   * negative/zero argument is the sole deviation from an otherwise valid default config.
   */
  private static StreamRuneMicronautProperties propsWithLockTimeout(Duration lockTimeout) {
    return new StreamRuneMicronautProperties(
        100,
        3,
        50,
        2.0,
        5000,
        1000,
        lockTimeout, // component 7: lockTimeout
        1024,
        10_000,
        5,
        Duration.ofSeconds(30),
        false,
        false,
        true,
        Duration.ofDays(7),
        false,
        100,
        5000L,
        true,
        5,
        60000L,
        Duration.ofDays(30),
        true,
        "streamrune",
        true,
        true,
        true,
        Duration.ofDays(7),
        Duration.ofDays(7),
        java.util.List.of(),
        Duration.ofDays(30),
        true,
        10,
        1000L,
        2.0,
        Duration.ofSeconds(15),
        Duration.ofMinutes(5),
        Duration.ofSeconds(30),
        // event-store.statement-timeout
        java.time.Duration.ofSeconds(30),
        // sse.polling-interval
        Duration.ofSeconds(1));
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
    var props = propsWithLockTimeout(Duration.ofSeconds(-1));
    var ctx = mock(ApplicationContext.class);
    var validator = new StreamRuneConfigValidator(props, ctx);
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> validator.onApplicationEvent(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("streamrune.lock-timeout"));
    assertTrue(ex.getMessage().contains("must not be negative"));
  }

  /** Zero is the documented "do not wait at all" value and must pass. */
  @Test
  void zeroLockTimeoutPasses() {
    var props = propsWithLockTimeout(Duration.ZERO);
    var ctx = mock(ApplicationContext.class);
    var validator = new StreamRuneConfigValidator(props, ctx);
    assertDoesNotThrow(() -> validator.onApplicationEvent(mock(StartupEvent.class)));
  }

  /**
   * {@code PT0.0005S} is positive, so it passed the negative check, and {@code toMillis()} then
   * made it {@code SET LOCAL lock_timeout = '0ms'} inside {@code PgAdvisoryLocker} — PostgreSQL's
   * spelling of DISABLED, so a configured 0.5 ms bound became an unbounded server-side wait, while
   * {@code LocalStripedLocker} on the same value made a single attempt. Identical rejection on
   * Spring ({@code 500us}) and Quarkus.
   */
  @Test
  void subMillisecondLockTimeoutThrows() {
    var props = propsWithLockTimeout(Duration.ofNanos(500_000));
    var ctx = mock(ApplicationContext.class);
    var validator = new StreamRuneConfigValidator(props, ctx);
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> validator.onApplicationEvent(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("streamrune.lock-timeout"));
    assertTrue(ex.getMessage().contains("at least 1ms"));
  }

  /** The floor itself is a valid value and must pass. */
  @Test
  void oneMillisecondLockTimeoutPasses() {
    var props = propsWithLockTimeout(Duration.ofMillis(1));
    var ctx = mock(ApplicationContext.class);
    var validator = new StreamRuneConfigValidator(props, ctx);
    assertDoesNotThrow(() -> validator.onApplicationEvent(mock(StartupEvent.class)));
  }

  @Test
  void outboxEnabledWithoutStoreThrows() {
    var props =
        new StreamRuneMicronautProperties(
            100,
            3,
            50,
            2.0,
            5000,
            1000,
            Duration.ofSeconds(5),
            1024,
            10_000,
            5,
            Duration.ofSeconds(30),
            false,
            false,
            true,
            Duration.ofDays(30),
            true,
            100,
            5000L,
            true,
            5,
            60000L,
            Duration.ofDays(30),
            true,
            "streamrune",
            true,
            true,
            true,
            Duration.ofDays(7),
            Duration.ofDays(7),
            java.util.List.of(),
            Duration.ofDays(30),
            true,
            10,
            1000L,
            2.0,
            Duration.ofSeconds(15),
            Duration.ofMinutes(5),
            Duration.ofSeconds(30),
            // event-store.statement-timeout
            java.time.Duration.ofSeconds(30),
            // sse.polling-interval
            Duration.ofSeconds(1));
    var ctx = mock(ApplicationContext.class);
    when(ctx.containsBean(org.streamrune.core.outbox.OutboxStore.class)).thenReturn(false);
    var validator = new StreamRuneConfigValidator(props, ctx);
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> validator.onApplicationEvent(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("OutboxStore"));
  }

  /**
   * withDefaults() with only the saga dead-letter retention (positional component 14)
   * parameterized; inbox retention stays at its 7d default, so a {@code sagaDeadLetter > 7d}
   * argument violates the dead-letter-retention invariant while everything else is a valid default
   * config.
   */
  private static StreamRuneMicronautProperties propsWithSagaDeadLetterRetention(
      Duration sagaDeadLetter) {
    return new StreamRuneMicronautProperties(
        100,
        3,
        50,
        2.0,
        5000,
        1000,
        Duration.ofSeconds(5),
        1024,
        10_000,
        5,
        Duration.ofSeconds(30),
        false,
        false,
        true,
        sagaDeadLetter, // component 14: sagaDeadLetterRetentionMaxAge
        false,
        100,
        5000L,
        true,
        5,
        60000L,
        Duration.ofDays(30),
        true,
        "streamrune",
        true,
        true,
        true,
        Duration.ofDays(7), // inboxRetentionMaxAge stays 7d
        Duration.ofDays(7),
        java.util.List.of(),
        Duration.ofDays(30),
        true,
        10,
        1000L,
        2.0,
        Duration.ofSeconds(15),
        Duration.ofMinutes(5),
        Duration.ofSeconds(30),
        // event-store.statement-timeout
        java.time.Duration.ofSeconds(30),
        // sse.polling-interval
        Duration.ofSeconds(1));
  }

  @Test
  void sagaDeadLetterRetentionExceedingInboxThrows_whenStorePresent_regardlessOfSweeper() {
    // The dead-letter-retention <= inbox invariant guards
    // SagaDeadLetterReplayer, so it must fail fast whenever a SagaDeadLetterStore is present — even
    // with the compensation-retry sweeper disabled (its lifecycle is @Requires(notEquals="false")
    // on the knob, so its own copy of this check would never run).
    var props = propsWithSagaDeadLetterRetention(Duration.ofDays(30));
    var ctx = mock(ApplicationContext.class);
    when(ctx.containsBean(org.streamrune.core.saga.SagaDeadLetterStore.class)).thenReturn(true);
    var validator = new StreamRuneConfigValidator(props, ctx);
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> validator.onApplicationEvent(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("streamrune.saga.dead-letter-retention-max-age"));
  }

  @Test
  void sagaDeadLetterRetentionExceedingInbox_ignored_whenNoStore() {
    // No SagaDeadLetterStore bean => no replay support => nothing to guard; must not fail fast.
    var props = propsWithSagaDeadLetterRetention(Duration.ofDays(30));
    var ctx = mock(ApplicationContext.class);
    when(ctx.containsBean(org.streamrune.core.saga.SagaDeadLetterStore.class)).thenReturn(false);
    var validator = new StreamRuneConfigValidator(props, ctx);
    assertDoesNotThrow(() -> validator.onApplicationEvent(mock(StartupEvent.class)));
  }

  @Test
  void sagaDeadLetterRetentionConsistentWithInbox_passes_whenStorePresent() {
    var props = propsWithSagaDeadLetterRetention(Duration.ofDays(7));
    var ctx = mock(ApplicationContext.class);
    when(ctx.containsBean(org.streamrune.core.saga.SagaDeadLetterStore.class)).thenReturn(true);
    var validator = new StreamRuneConfigValidator(props, ctx);
    assertDoesNotThrow(() -> validator.onApplicationEvent(mock(StartupEvent.class)));
  }

  /**
   * The documented saga kill switch must disarm the saga-only boot invariants. Micronaut gates that
   * check on bean PRESENCE, which the framework's own store no longer satisfies when {@code
   * streamrune.saga.enabled=false} — but a user-supplied {@link
   * org.streamrune.core.saga.SagaDeadLetterStore} does, so the replay invariant still failed boot
   * for a replayer that can never run. Exercised through a REAL Micronaut context, so the validator
   * is discovered, wired and driven by the container's own StartupEvent.
   */
  @Test
  void sagasDisabled_withUserSuppliedDeadLetterStore_bootsRealContext() {
    assertDoesNotThrow(
        () -> {
          try (var ctx =
              io.micronaut.context.ApplicationContext.run(
                  java.util.Map.of(
                      "spec.name", "intg9-4-user-saga-dlq-store",
                      "streamrune.saga.enabled", "false",
                      "streamrune.saga.dead-letter-retention-max-age", "30d",
                      "streamrune.inbox.retention-max-age", "7d"))) {
            assertTrue(
                ctx.containsBean(org.streamrune.core.saga.SagaDeadLetterStore.class),
                "precondition: the user-supplied store is present — that is what opened the gate");
          }
        },
        "sagas are disabled, so the saga dead-letter retention invariant must not fail boot");
  }

  @io.micronaut.context.annotation.Factory
  @io.micronaut.context.annotation.Requires(
      property = "spec.name",
      value = "intg9-4-user-saga-dlq-store")
  static class UserSagaDeadLetterStoreFixture {

    @jakarta.inject.Singleton
    org.streamrune.core.saga.SagaDeadLetterStore userStore() {
      return mock(org.streamrune.core.saga.SagaDeadLetterStore.class);
    }
  }

  @Test
  void outboxEnabledWithStoreAndPublisherPassesWithoutException() {
    var ctx = mock(ApplicationContext.class);
    when(ctx.containsBean(org.streamrune.core.outbox.OutboxStore.class)).thenReturn(true);
    when(ctx.containsBean(org.streamrune.core.outbox.OutboxPublisher.class)).thenReturn(true);
    var validator = new StreamRuneConfigValidator(outboxEnabledProps(), ctx);
    assertDoesNotThrow(() -> validator.onApplicationEvent(mock(StartupEvent.class)));
  }

  @Test
  void outboxEnabledWithoutStoreNamesTheStoreTheApplicationMustDeclare() {
    var ctx = mock(ApplicationContext.class);
    var validator = new StreamRuneConfigValidator(outboxEnabledProps(), ctx);
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> validator.onApplicationEvent(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("PostgresOutboxStore"), ex.getMessage());
    assertFalse(
        ex.getMessage().contains("to your classpath"),
        "the publisher modules provide no store, so adding one cannot fix this: "
            + ex.getMessage());
  }

  /**
   * With the outbox enabled and no {@code OutboxPublisher} bean the framework poller is never
   * created: the event store keeps writing PENDING rows that nothing relays, while health stays UP.
   */
  @Test
  void outboxEnabledWithoutPublisherThrows() {
    var ctx = mock(ApplicationContext.class);
    when(ctx.containsBean(org.streamrune.core.outbox.OutboxStore.class)).thenReturn(true);
    var validator = new StreamRuneConfigValidator(outboxEnabledProps(), ctx);
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> validator.onApplicationEvent(mock(StartupEvent.class)));
    assertTrue(ex.getMessage().contains("OutboxPublisher"), ex.getMessage());
  }

  @Test
  void outboxEnabledWithAnApplicationPollerAndNoPublisherBeanPasses() {
    var ctx = mock(ApplicationContext.class);
    when(ctx.containsBean(org.streamrune.core.outbox.OutboxStore.class)).thenReturn(true);
    when(ctx.containsBean(org.streamrune.runtime.OutboxPoller.class)).thenReturn(true);
    var validator = new StreamRuneConfigValidator(outboxEnabledProps(), ctx);
    assertDoesNotThrow(() -> validator.onApplicationEvent(mock(StartupEvent.class)));
  }

  /** withDefaults() with streamrune.outbox.enabled=true. */
  private static StreamRuneMicronautProperties outboxEnabledProps() {
    return new StreamRuneMicronautProperties(
        100,
        3,
        50,
        2.0,
        5000,
        1000,
        Duration.ofSeconds(5),
        1024,
        10_000,
        5,
        Duration.ofSeconds(30),
        false,
        false,
        true,
        Duration.ofDays(30),
        true,
        100,
        5000L,
        true,
        5,
        60000L,
        Duration.ofDays(30),
        true,
        "streamrune",
        true,
        true,
        true,
        Duration.ofDays(7),
        Duration.ofDays(7),
        java.util.List.of(),
        Duration.ofDays(30),
        true,
        10,
        1000L,
        2.0,
        Duration.ofSeconds(15),
        Duration.ofMinutes(5),
        Duration.ofSeconds(30),
        // event-store.statement-timeout
        Duration.ofSeconds(30),
        // sse.polling-interval
        Duration.ofSeconds(1));
  }
}
