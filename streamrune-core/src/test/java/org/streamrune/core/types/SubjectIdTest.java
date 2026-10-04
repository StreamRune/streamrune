package org.streamrune.core.types;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

class SubjectIdTest {

  @Test
  void constructsWithValue() {
    assertEquals("customer-1", new SubjectId("customer-1").value());
  }

  @Test
  void ofIsAlias() {
    assertEquals(SubjectId.of("customer-1"), new SubjectId("customer-1"));
  }

  @Test
  void rejectsNull() {
    assertThrows(IllegalArgumentException.class, () -> new SubjectId(null));
  }

  @Test
  void rejectsBlank() {
    assertThrows(IllegalArgumentException.class, () -> new SubjectId(""));
  }

  @Test
  void serializesAsPlainStringAndRoundTrips() throws Exception {
    var mapper = new ObjectMapper();
    var id = SubjectId.of("customer-1");
    assertEquals("customer-1", id.toString());
    assertEquals("\"customer-1\"", mapper.writeValueAsString(id));
    assertEquals(id, mapper.readValue("\"customer-1\"", SubjectId.class));
  }

  @Test
  void redactedIsSha256HexNeverTheRawValue() throws Exception {
    // A subject id may itself be PII (an email/username). redacted() is the PII-safe
    // form
    // to embed in exception messages, logs, DLQ error_message and audit rows — the SAME SHA-256 hex
    // the crypto engines store at rest, so it still correlates to the stored key/tombstone rows.
    var id = SubjectId.of("alice@example.com");
    String redacted = id.redacted();

    assertFalse(
        redacted.contains("alice@example.com"), "redacted form must not contain the raw value");

    var md = java.security.MessageDigest.getInstance("SHA-256");
    String expected =
        java.util.HexFormat.of()
            .formatHex(
                md.digest("alice@example.com".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    assertEquals(expected, redacted, "must match the SHA-256 hex the engines store at rest");
    assertEquals(64, redacted.length(), "SHA-256 hex is 64 chars");
    assertEquals(redacted, SubjectId.of("alice@example.com").redacted(), "deterministic");

    // toString() intentionally stays the RAW value — key derivation and existing callers rely on
    // it.
    assertEquals("alice@example.com", id.toString());
  }
}
