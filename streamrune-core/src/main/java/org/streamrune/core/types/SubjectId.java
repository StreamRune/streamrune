package org.streamrune.core.types;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Identifies the GDPR data subject whose personal data is being encrypted, exported, or erased.
 * Used as the per-subject key handle in {@link org.streamrune.core.crypto.CryptoEngine} and the
 * GDPR service SPIs.
 */
public record SubjectId(@JsonValue String value) {

  @JsonCreator
  public SubjectId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("subjectId is required");
    }
  }

  public static SubjectId of(String value) {
    return new SubjectId(value);
  }

  /**
   * A SHA-256 hex digest of {@link #value()} — the PII-safe form to embed anywhere a subject id
   * would otherwise be persisted or logged: crypto-engine exception messages, application logs, the
   * {@code dead_letter_queue.error_message} column, and GDPR {@code audit_log} rows.
   *
   * <p>A subject id may itself be personal data (an email address or username), so the raw {@link
   * #value()} must never land verbatim in a durable or logged string — otherwise the erasure flow
   * itself re-materializes the PII it was meant to remove. This is the <em>same</em> digest the
   * crypto engines store at rest ({@code PostgresCryptoEngine.storedSubjectId}, {@code
   * FileSystemCryptoEngine} and {@code VaultCryptoEngine} key names), so a redacted id in a log or
   * DLQ row still correlates to the subject's stored key and tombstone rows without exposing the
   * identifier.
   *
   * <p>{@link #toString()} is intentionally left returning the raw value: internal key derivation
   * and existing callers depend on it. Redaction is applied only at the sites that persist or log,
   * not on the type itself.
   */
  public String redacted() {
    try {
      byte[] digest =
          MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8));
      return HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException _) {
      // SHA-256 is a mandated JCA algorithm; if it is somehow unavailable, never fall back to the
      // raw value — a stable non-PII marker is always preferable to leaking the identifier.
      return "subject-hash-unavailable";
    }
  }

  @Override
  public String toString() {
    return value;
  }
}
