package org.streamrune.runtime.health;

/**
 * The names under which {@link org.streamrune.runtime.BackgroundRelayHealthContributor} reports
 * each background relay: the {@code relay.<component>} keys of the framework health details and the
 * {@code Relay component} table of the production guide. Operators alert on these names, so they
 * live in one place; {@code RelayComponentNamesDocLintTest} keeps the guide's table equal to them.
 *
 * <p>A name is a display name, not a coordination key: the leases the runners take ({@code
 * DeadLetterRetryRunner}, {@code SagaCompensationRetrySweeper}) have consumer names of their own,
 * shared by every replica, which stay put whatever is shown here.
 */
public final class RelayComponentNames {

  /** The outbox relay, {@link org.streamrune.runtime.OutboxPoller}. */
  public static final String OUTBOX_RELAY = "outbox-relay";

  /** The command dead-letter retry runner, {@link org.streamrune.runtime.DeadLetterRetryRunner}. */
  public static final String DEAD_LETTER_RETRY = "dead-letter-retry";

  /** The prefix of {@link #sagaCompensationRetry(String)}. */
  public static final String SAGA_COMPENSATION_RETRY_PREFIX = "saga-compensation-retry:";

  /**
   * The suffix of a compensation-retry sweeper built with {@code compensationRetryEnabled(false)}:
   * it re-drives nothing and only samples the saga backlog gauges.
   */
  public static final String SAMPLING_ONLY_SUFFIX = ":sampling-only";

  /** The prefix of {@link #sagaTimeout(String)}. */
  public static final String SAGA_TIMEOUT_PREFIX = "saga-timeout:";

  /** The command-inbox retention sweeper, {@link org.streamrune.runtime.InboxRetentionSweeper}. */
  public static final String INBOX_RETENTION_SWEEPER = "inbox-retention-sweeper";

  /**
   * The command dead-letter retention sweeper, {@link
   * org.streamrune.runtime.DeadLetterRetentionSweeper}.
   */
  public static final String DEAD_LETTER_RETENTION_SWEEPER = "dead-letter-retention-sweeper";

  /** The outbox retention sweeper, {@link org.streamrune.runtime.OutboxRetentionSweeper}. */
  public static final String OUTBOX_RETENTION_SWEEPER = "outbox-retention-sweeper";

  /**
   * The saga dead-letter retention sweeper, {@link
   * org.streamrune.runtime.SagaDeadLetterRetentionSweeper}.
   */
  public static final String SAGA_DEAD_LETTER_RETENTION_SWEEPER =
      "saga-dead-letter-retention-sweeper";

  /**
   * The listening side of the first crypto forget channel; a second, different channel is reported
   * with {@code -2} appended, a third with {@code -3}, and so on.
   */
  public static final String CRYPTO_FORGET_LISTEN = "crypto-forget-listen";

  /** The feed of the Server-Sent Events endpoint, {@link org.streamrune.runtime.SseEventFeed}. */
  public static final String SSE_EVENT_FEED = "sse-event-feed";

  private RelayComponentNames() {}

  /**
   * The compensation-retry sweeper of one saga type, {@link
   * org.streamrune.runtime.SagaCompensationRetrySweeper}: {@code saga-compensation-retry:<saga
   * type>}, the saga type being the saga state's fully qualified class name.
   */
  public static String sagaCompensationRetry(String sagaType) {
    return SAGA_COMPENSATION_RETRY_PREFIX + sagaType;
  }

  /**
   * The timeout runner of one saga type, {@link org.streamrune.runtime.SagaTimeoutRunner}: {@code
   * saga-timeout:<saga type>}, the saga type being the saga state's fully qualified class name.
   */
  public static String sagaTimeout(String sagaType) {
    return SAGA_TIMEOUT_PREFIX + sagaType;
  }
}
