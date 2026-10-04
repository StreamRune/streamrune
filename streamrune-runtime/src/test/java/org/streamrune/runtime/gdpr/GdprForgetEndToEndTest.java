package org.streamrune.runtime.gdpr;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.test.InMemoryCryptoEngine;

class GdprForgetEndToEndTest {

  record UserRegistered(
      String userId, @Encrypted(subjectId = "userId") String email, String locale) {}

  @Test
  void forget_makesEncryptedFieldRedacted() throws Exception {
    var crypto = new InMemoryCryptoEngine();
    var mapper = new ObjectMapper();
    mapper.registerModule(new CryptoShreddingModule(crypto));

    // Persist event — email gets encrypted under user-1's key.
    var event = new UserRegistered("user-1", "alice@example.com", "en_US");
    String storedJson = mapper.writeValueAsString(event);
    assertFalse(
        storedJson.contains("alice@example.com"),
        "email must not appear as plaintext in stored JSON — encryption must have occurred");

    // Pre-forget: round-trip restores plaintext.
    var pre = mapper.readValue(storedJson, UserRegistered.class);
    assertEquals("alice@example.com", pre.email());

    // Right to be forgotten — crypto-shred user-1's key.
    var forgetService = ForgetSubjectService.builder().cryptoEngine(crypto).build();
    forgetService.forget(SubjectId.of("user-1"), null);

    // Post-forget: stored JSON unchanged, but email reads as REDACTED.
    var post = mapper.readValue(storedJson, UserRegistered.class);
    assertEquals("user-1", post.userId());
    assertEquals(CryptoShreddingModule.REDACTED, post.email());
    assertEquals("en_US", post.locale());
  }

  @Test
  void forget_isIdempotent_secondCallSucceeds() {
    var crypto = new InMemoryCryptoEngine();
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).build();

    service.forget(SubjectId.of("user-2"), null);
    assertFalse(crypto.isKeyAvailable(SubjectId.of("user-2")));

    // Second call: no-op, must not throw.
    assertDoesNotThrow(() -> service.forget(SubjectId.of("user-2"), null));
  }
}
