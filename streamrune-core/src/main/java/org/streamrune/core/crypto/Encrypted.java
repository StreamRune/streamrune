package org.streamrune.core.crypto;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a record component as containing personally identifiable information (PII) that should be
 * encrypted at rest using the crypto-shredding mechanism.
 *
 * <p>The {@code subjectId} attribute specifies which record component holds the data subject
 * identifier. The encryption key is derived from that subject's ID.
 *
 * <p>The annotated component must be a {@code String}. On read the value is decrypted
 * <em>before</em> the record's canonical/compact constructor runs, so the constructor always sees
 * the real plaintext — format/parse validation (an email/regex check, a UUID parse, a
 * minimum-length rule) is perfectly fine and validates the decrypted value like any other field.
 *
 * <p><b>Post-forget tombstone tolerance:</b> the one thing such validation <b>must</b> tolerate is
 * the {@code "[REDACTED]"} tombstone. After a subject is crypto-shredded (GDPR erasure), this field
 * decrypts to {@code "[REDACTED]"} on every subsequent load; because a Java record cannot be
 * reconstructed while bypassing its compact-constructor validation, a constructor that rejects the
 * sentinel makes the aggregate permanently unloadable after erasure. Guard any validation of an
 * {@code @Encrypted} field so it accepts the tombstone (for example, skip the check when the value
 * equals the redaction marker) — a rejection surfaces as a targeted, actionable {@code
 * CryptoOperationException} rather than an opaque failure.
 */
@Target(ElementType.RECORD_COMPONENT)
@Retention(RetentionPolicy.RUNTIME)
public @interface Encrypted {

  /**
   * The name of the record component that holds the data subject identifier. The value of that
   * component is used to look up the encryption key.
   */
  String subjectId();
}
