package org.streamrune.integration;

/**
 * The messages the three framework integrations refuse startup with when {@code
 * streamrune.outbox.enabled=true} has nothing to store or relay the outbox, in one place so the
 * wording cannot drift between Spring, Quarkus and Micronaut.
 *
 * <p>{@code streamrune.outbox.enabled} switches the framework's relay (the poller, the retention
 * sweeper and the failed-entry replayer). The event store writes outbox entries whenever an {@code
 * OutboxEventMapper} and a {@code PostgresOutboxStore} are declared, whatever the property says, so
 * an application whose entries an external relay reads (change data capture on the outbox table)
 * runs with the property off.
 */
public final class OutboxWiringMessages {

  private static final String EXTERNAL_RELAY =
      " If an external relay reads the outbox table, set streamrune.outbox.enabled=false: the"
          + " event store still writes the entries when an OutboxEventMapper and a"
          + " PostgresOutboxStore are declared, and the framework's relay, retention sweeper and"
          + " failed-entry replayer stay off.";

  private OutboxWiringMessages() {}

  /**
   * The outbox is enabled and no {@code OutboxStore} bean exists.
   *
   * @return the message
   */
  public static String noStore() {
    return "Outbox enabled but no OutboxStore found. Declare a PostgresOutboxStore bean"
        + " (streamrune-postgres); streamrune-kafka-outbox and streamrune-rabbitmq-outbox"
        + " provide only the OutboxPublisher.";
  }

  /**
   * The outbox is enabled and neither an {@code OutboxPublisher} bean nor an application {@code
   * OutboxPoller} bean exists.
   *
   * @return the message
   */
  public static String noPublisher() {
    return "Outbox enabled but no OutboxPublisher found, so no OutboxPoller can relay the outbox"
        + " entries. Declare an OutboxPublisher bean (streamrune-kafka-outbox,"
        + " streamrune-rabbitmq-outbox or an HttpOutboxPublisher)."
        + EXTERNAL_RELAY;
  }

  /**
   * The outbox is enabled and the only {@code OutboxPublisher} beans carry a qualifier, so the
   * framework's poller, which resolves the default one, finds none; no application {@code
   * OutboxPoller} bean exists.
   *
   * @return the message
   */
  public static String qualifiedPublisherOnly() {
    return "Outbox enabled but an OutboxPublisher bean exists only with a qualifier, and the"
        + " framework's OutboxPoller resolves the OutboxPublisher without one, so nothing relays"
        + " the outbox entries. Declare the OutboxPublisher without a qualifier, or declare your"
        + " own OutboxPoller."
        + EXTERNAL_RELAY;
  }

  /**
   * The outbox is enabled and more than one {@code OutboxPublisher} bean matches, so the
   * framework's poller cannot pick one; no application {@code OutboxPoller} bean exists.
   *
   * @return the message
   */
  public static String ambiguousPublisher() {
    return "Outbox enabled but more than one OutboxPublisher bean matches, so no OutboxPoller can"
        + " relay the outbox entries. Keep exactly one OutboxPublisher bean without a qualifier,"
        + " or declare your own OutboxPoller."
        + EXTERNAL_RELAY;
  }
}
