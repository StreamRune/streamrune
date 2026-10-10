package org.streamrune.runtime;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.SQLException;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.saga.SagaId;
import org.streamrune.core.saga.SagaStateSerializationException;

/**
 * Decides whether a {@link SagaStateSerializationException} raised by a {@code SagaStore} is
 * <b>deterministic</b> — the same state converts to the same failure on every retry — or possibly
 * <b>transient</b>, and converts the deterministic ones into the runtime's own {@link
 * SagaPoisonException}.
 *
 * <p><b>Why the split has to happen here.</b> The store deliberately raises a type that carries no
 * verdict ("the conversion failed"), because only the cause chain distinguishes the two classes and
 * {@code SagaPoisonException} is package-private to this package, so the store cannot raise it.
 * Both halves are needed:
 *
 * <ul>
 *   <li>Treating <em>every</em> conversion failure as infrastructure (all that the store can do on
 *       its own) means a deterministic failure propagates out of {@code onEvents} forever: the
 *       subscription never advances its offset, {@code ResilientPollLoop} retries the identical
 *       batch with capped backoff, no event is quarantined, no saga is FAULTED, no dead-letter row
 *       is written, and the subscription still reports itself RUNNING. Every saga of every type on
 *       that subscription stops advancing.
 *   <li>Treating every conversion failure as poison is <em>worse</em>: a Vault/AWS-KMS outage or a
 *       pool timeout would dead-letter every event it touched and FAULT every saga, permanently,
 *       for a condition that heals by itself in minutes. The crypto backends deliberately make the
 *       backends fail closed with {@code CryptoOperationException} precisely so those operations
 *       block and retry rather than writing {@code [REDACTED]} tombstones — this classifier must
 *       not undo that.
 * </ul>
 *
 * <p><b>The rule.</b> The whole cause chain is scanned (bounded, so a self-referential chain cannot
 * spin) and the decision is made from the evidence found, not from the outermost frame — Jackson
 * wraps a property writer's exception into a {@code JsonMappingException} (an {@code IOException}),
 * so an engine's {@code CryptoOperationException} routinely arrives <em>underneath</em> an {@code
 * IOException} and an outermost-frame-wins walk would mis-bin every key-store outage as poison:
 *
 * <ol>
 *   <li>{@link SubjectForgottenException} anywhere ⇒ <b>deterministic</b>. The subject was
 *       crypto-shredded, so the engine refuses to re-encrypt and will keep refusing until an
 *       operator calls {@code reinstate}. Same verdict {@code VirtualThreadCommandBus} already
 *       reaches for commands ("that rejection is permanent — no retry can ever succeed"). Checked
 *       first because an engine may surface it wrapped.
 *   <li>Otherwise a plain {@link CryptoOperationException} (i.e. NOT a {@link
 *       CryptoMappingException}) or a {@link SQLException} anywhere ⇒ <b>transient</b>. The first
 *       is an engine-raised key-store failure; the second is JDBC, which reaches this path through
 *       {@code PostgresCryptoEngine} doing its own query inside the mapper. Both short-circuit the
 *       deterministic markers below, so the fail-safe direction always wins on mixed evidence.
 *   <li>Otherwise a {@link CryptoMappingException} anywhere ⇒ <b>deterministic</b>. That type has
 *       two sources, and both raise it only for a verdict no retry can change: {@code
 *       CryptoShreddingModule} raises it for a mapping refusal — an {@code @Encrypted} component
 *       whose {@code subjectId} is still null (the natural shape of a partially-populated saga
 *       state mid-flow), a shape/annotation combination that cannot be encrypted without leaking
 *       plaintext, a record whose constructor rejects the {@code [REDACTED]} tombstone — and every
 *       shipped {@code CryptoEngine} raises it for a verdict on the blob it was handed (e.g. too
 *       short, an unsupported format or key version, an AEAD tag mismatch). See {@link
 *       CryptoMappingException}.
 *   <li>Otherwise an {@link IOException} / {@link UncheckedIOException} anywhere ⇒
 *       <b>deterministic</b>. Jackson's {@code JsonProcessingException} extends {@code
 *       IOException}, so this is a pure mapping defect: an unmappable shape, an unknown enum
 *       constant, a self-referential structure. Same rule, and the same {@code SQLException}-first
 *       ordering, that {@link ReadPoisonClassifier} applies on the projection read path.
 *   <li>Anything else ⇒ <b>transient</b>. Never permanently quarantine on a failure that could not
 *       be proven deterministic.
 * </ol>
 */
final class SagaStateConversion {

  private SagaStateConversion() {}

  /** Depth cap so a cyclic {@code getCause()} chain cannot loop forever (matches the siblings). */
  private static final int MAX_CAUSE_DEPTH = 50;

  /**
   * Maps a store-raised conversion failure onto the exception the caller should throw: a {@link
   * SagaPoisonException} when the failure is provably deterministic (the adapter then quarantines
   * the triggering event and FAULTs the saga), otherwise {@code failure} itself so it keeps
   * propagating as an infrastructure failure and the batch is retried.
   *
   * @param sagaId the saga in scope at the call site; falls back to the id the store stamped on the
   *     exception when the call site has none
   * @param failure the store's conversion failure (never null)
   */
  static RuntimeException classify(SagaId sagaId, SagaStateSerializationException failure) {
    if (!isDeterministic(failure)) {
      return failure;
    }
    return new SagaPoisonException(sagaId != null ? sagaId : failure.sagaId(), failure);
  }

  /** Whether {@code failure}'s cause chain proves the conversion can never succeed on a retry. */
  static boolean isDeterministic(Throwable failure) {
    boolean sawMappingRefusal = false;
    boolean sawMappingIo = false;
    Throwable c = failure;
    for (int depth = 0; c != null && depth < MAX_CAUSE_DEPTH; depth++, c = c.getCause()) {
      if (c instanceof SubjectForgottenException) {
        return true;
      }
      if (c instanceof CryptoMappingException) {
        sawMappingRefusal = true;
      } else if (c instanceof CryptoOperationException || c instanceof SQLException) {
        // Fail-safe short-circuit: engine/JDBC evidence outranks every deterministic marker, so a
        // key-store outage that Jackson wrapped in a JsonMappingException still retries.
        return false;
      } else if (c instanceof IOException || c instanceof UncheckedIOException) {
        sawMappingIo = true;
      }
    }
    return sawMappingRefusal || sawMappingIo;
  }
}
