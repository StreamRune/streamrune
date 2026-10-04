package org.streamrune.runtime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import org.streamrune.core.EventDeserializationException;
import org.streamrune.core.UnknownEventTypeException;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;

/**
 * Distinguishes a <b>deterministic read/deserialization poison</b> — an event the store can never
 * turn into an {@link org.streamrune.core.EventEnvelope} no matter how many times the read is
 * retried (an unregistered event type, or a corrupt / schema-incompatible payload) — from a genuine
 * <b>transient</b> store failure (a connection reset, lock/statement timeout, key-store outage, or
 * read-replica failover blip a bounded retry is likely to clear).
 *
 * <p>This is a <em>read-path</em> classifier and is deliberately separate from {@link
 * org.streamrune.core.projection.ProjectionErrorClassifier}, which classifies <em>processing</em>
 * failures. The processing classifier treats every {@link org.streamrune.core.EventStoreException}
 * as TRANSIENT — correct there, because a projection's {@code process()} typically wraps a
 * read-model SQL blip as an {@code EventStoreException}. On the READ path that same rule is wrong:
 * a read that fails because the stored event's type is unregistered, or its JSON cannot be bound to
 * the target class, is an {@code EventStoreException} too, but it is deterministic and will fail
 * identically forever. Classifying such a read as TRANSIENT is exactly what wedges a projection —
 * the runner retries the read with backoff indefinitely and never advances, dead-letters, or halts.
 *
 * <p><b>The whole cause chain is walked, flag-and-continue.</b> The walk used to early-return
 * POISON on the first {@link IOException} frame without ever looking underneath it. A transient
 * Vault/KMS outage during nested-{@code @Encrypted} decryption surfaces WRAPPED by Jackson into a
 * {@code JsonMappingException} (an IOException), so a routine key-store blip terminally halted
 * projections. The walk now mirrors {@link SagaStateConversion#isDeterministic}: deterministic
 * markers are flagged and the walk continues; engine/JDBC <em>transient</em> evidence anywhere in
 * the chain outranks every deterministic marker and short-circuits TRANSIENT. The rules, in the
 * order each frame is tested (bounded, to survive a pathological self-referential chain):
 *
 * <ul>
 *   <li>{@link SubjectForgottenException} anywhere ⇒ <b>POISON</b> (short-circuit). The subject was
 *       crypto-shredded; no retry can ever read it back — the same verdict every sibling classifier
 *       reaches.
 *   <li>{@link UnknownEventTypeException} / {@link EventDeserializationException} / {@link
 *       CryptoMappingException} ⇒ flag <b>deterministic</b>, keep walking. The first is a registry
 *       lookup that re-fails identically; the second is the store's own typed frame for a stored
 *       payload that deterministically cannot be bound (including a user upcaster's own throw,
 *       which would wedge the loop); the third is the crypto layer's deterministic mapping refusal.
 *   <li>a bare {@link CryptoOperationException} (i.e. NOT its {@code CryptoMappingException}
 *       subtype) or a {@link SQLException} ⇒ <b>TRANSIENT</b> (short-circuit). Engine/JDBC evidence
 *       outranks every deterministic marker: a key-store outage or DB connection blip heals by
 *       itself, and some JDBC drivers wrap a socket-level {@code IOException} under the {@code
 *       SQLException}.
 *   <li>an {@link IOException} / {@link UncheckedIOException} (Jackson's {@code
 *       JsonProcessingException} extends {@code IOException}) ⇒ flag <b>deterministic</b>, keep
 *       walking — the stored bytes cannot be parsed/bound to the target type, a schema/payload
 *       defect a retry cannot fix, <em>unless</em> a deeper frame proves the transient rule above.
 * </ul>
 *
 * <p>Anything not positively identified as poison defaults to <b>transient</b> (keep retrying): the
 * safe default never permanently kills a projection on a failure we could not prove is
 * deterministic.
 *
 * <p><b>This is public API.</b> {@code ContinuousProjectionRunner.SubscriptionFactory} documents
 * that a factory-built live subscription MUST honour the {@code readPoisonBound} it is handed, and
 * {@link ReadPoisonAware} is how the runner learns that the bound was exhausted. An implementer
 * outside this package therefore needs the same classification rule the framework's own readers
 * use; keeping it package-private is what left {@code PostgresNotificationSubscription} — a public,
 * documented production subscription — unable to comply, retrying a deterministic poison forever
 * while the projection reported healthy.
 */
public final class ReadPoisonClassifier {

  private ReadPoisonClassifier() {}

  /** Depth cap so a cyclic {@code getCause()} chain cannot loop forever. */
  private static final int MAX_CAUSE_DEPTH = 128;

  /**
   * Returns {@code true} if {@code readError} is a deterministic read/deserialization poison that a
   * retry can never clear, {@code false} if it is (or may be) a transient infrastructure failure.
   */
  public static boolean isDeterministicReadPoison(Throwable readError) {
    boolean sawDeterministic = false;
    int remaining = MAX_CAUSE_DEPTH;
    for (Throwable t = readError; t != null && remaining-- > 0; t = t.getCause()) {
      if (t instanceof SubjectForgottenException) {
        return true;
      }
      if (t instanceof UnknownEventTypeException
          || t instanceof EventDeserializationException
          || t instanceof CryptoMappingException) {
        sawDeterministic = true;
      } else if (t instanceof CryptoOperationException || t instanceof SQLException) {
        // Fail-safe short-circuit: engine/JDBC evidence outranks every deterministic marker, so a
        // key-store outage that Jackson wrapped in a JsonMappingException still retries.
        return false;
      } else if (t instanceof IOException || t instanceof UncheckedIOException) {
        sawDeterministic = true;
      }
    }
    return sawDeterministic;
  }
}
