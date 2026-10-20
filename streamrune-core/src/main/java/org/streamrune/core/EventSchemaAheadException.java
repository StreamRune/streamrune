package org.streamrune.core;

import java.util.Objects;
import org.streamrune.core.types.EventType;

/**
 * A stored event whose {@code schema_version} is above the version this reader's upcaster chain
 * reaches for its type: the event was written by a newer release of the application (or by a store
 * that has the upcasters this one lacks), and this reader cannot interpret it without risking a
 * silent mis-bind. The read is refused, and the refusal is about <em>this reader</em>, not about
 * the stored event: the same event reads correctly on a reader whose chain reaches the stored
 * version.
 *
 * <p>During a rolling deployment that bumps an event schema, this is what the replicas still on the
 * previous release see for every event the upgraded replicas wrote. It resolves by itself once the
 * replica is upgraded, so it is classified as <b>retry-later</b>, never as poison:
 *
 * <ul>
 *   <li>a command that fails with it is dead-lettered (a replay on an upgraded replica succeeds)
 *       but does <b>not</b> count toward the command bus's circuit breaker — it says nothing about
 *       the health of the store, the pool or the network, and opening the bus-global circuit would
 *       reject every command of the replica, for every aggregate, for the whole rollout (see {@link
 *       CommandFailureClassification});
 *   <li>a projection or subscription that reads it retries the read and stops at that event instead
 *       of quarantining it, so a replica on the previous release never dead-letters an event that
 *       the next release reads fine ({@code ReadPoisonClassifier} and {@link
 *       org.streamrune.core.projection.ProjectionErrorClassifier#DEFAULT} treat it as transient).
 * </ul>
 *
 * <p>Deliberately <b>not</b> an {@link EventDeserializationException}: that type means the stored
 * bytes can never be bound, and the read path quarantines it after a bounded poison streak. A
 * reader that is merely behind the stored schema must keep retrying until it is upgraded.
 */
public final class EventSchemaAheadException extends EventStoreException {

  private final EventType eventType;
  private final int storedVersion;
  private final int readerVersion;

  /**
   * Creates the refusal.
   *
   * @param message the full message, naming the event and both versions
   * @param eventType the stored event's type
   * @param storedVersion the {@code schema_version} the event was stored with
   * @param readerVersion the highest version this reader's upcaster chain reaches for the type (1
   *     without an upcaster for it)
   */
  public EventSchemaAheadException(
      String message, EventType eventType, int storedVersion, int readerVersion) {
    super(message);
    this.eventType = Objects.requireNonNull(eventType, "eventType is required");
    if (storedVersion <= readerVersion) {
      throw new IllegalArgumentException(
          "storedVersion ("
              + storedVersion
              + ") must be above readerVersion ("
              + readerVersion
              + ")");
    }
    this.storedVersion = storedVersion;
    this.readerVersion = readerVersion;
  }

  /**
   * The stored event's type.
   *
   * @return the event type
   */
  public EventType eventType() {
    return eventType;
  }

  /**
   * The {@code schema_version} the event was stored with.
   *
   * @return the stored version
   */
  public int storedVersion() {
    return storedVersion;
  }

  /**
   * The highest version this reader's upcaster chain reaches for the type: 1 without an upcaster.
   *
   * @return the reader's version
   */
  public int readerVersion() {
    return readerVersion;
  }
}
