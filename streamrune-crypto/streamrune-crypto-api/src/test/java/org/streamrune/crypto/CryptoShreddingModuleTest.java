package org.streamrune.crypto;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.streamrune.core.StreamRuneMetrics;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.test.InMemoryCryptoEngine;

class CryptoShreddingModuleTest {

  record UserRegistered(
      String userId, @Encrypted(subjectId = "userId") String email, String locale) {}

  record MultipleEncryptedFields(
      String customerId,
      @Encrypted(subjectId = "customerId") String email,
      @Encrypted(subjectId = "customerId") String fullName,
      String tier) {}

  @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
  @JsonSubTypes(@JsonSubTypes.Type(value = PolymorphicUserRegistered.class, name = "registered"))
  sealed interface PolymorphicEvent permits PolymorphicUserRegistered {}

  record PolymorphicUserRegistered(String userId, @Encrypted(subjectId = "userId") String email)
      implements PolymorphicEvent {}

  private InMemoryCryptoEngine cryptoEngine;
  private ObjectMapper mapper;

  @BeforeEach
  void setUp() {
    cryptoEngine = new InMemoryCryptoEngine();
    mapper = new ObjectMapper();
    mapper.registerModule(new CryptoShreddingModule(cryptoEngine));
  }

  @Test
  void serializesEncryptedFieldsAndDeserializesBack() throws Exception {
    var event = new UserRegistered("user-42", "alice@example.com", "en_US");

    String json = mapper.writeValueAsString(event);

    // JSON must not contain the plaintext email
    assertFalse(json.contains("alice@example.com"), "JSON should not contain plaintext email");
    // JSON must still contain non-encrypted fields in plain
    assertTrue(json.contains("user-42"));
    assertTrue(json.contains("en_US"));

    // Deserialize back — email should be decrypted
    var restored = mapper.readValue(json, UserRegistered.class);
    assertEquals("user-42", restored.userId());
    assertEquals("alice@example.com", restored.email());
    assertEquals("en_US", restored.locale());
  }

  @Test
  void shreddedFieldReturnsRedacted() throws Exception {
    var event = new UserRegistered("user-99", "bob@example.com", "cs_CZ");

    String json = mapper.writeValueAsString(event);

    // Delete the key — crypto shredding
    cryptoEngine.deleteKey(SubjectId.of("user-99"));

    var restored = mapper.readValue(json, UserRegistered.class);
    assertEquals("user-99", restored.userId());
    assertEquals(CryptoShreddingModule.REDACTED, restored.email());
    assertEquals("cs_CZ", restored.locale());
  }

  @Test
  void reinstatedSubject_preErasureFieldStaysRedacted_newFieldDecrypts() throws Exception {
    // The erase -> reinstate -> re-encrypt flow end to end: the re-minted key is one generation
    // above the destroyed one (deleteKey recorded it), so the pre-erasure event keeps reading as
    // the [REDACTED] placeholder instead of failing the tag check, and the new event decrypts.
    String preErasure =
        mapper.writeValueAsString(new UserRegistered("user-77", "old@example.com", "cs_CZ"));
    cryptoEngine.deleteKey(SubjectId.of("user-77"));
    cryptoEngine.reinstate(SubjectId.of("user-77"));
    String postReinstate =
        mapper.writeValueAsString(new UserRegistered("user-77", "new@example.com", "cs_CZ"));

    assertEquals(
        CryptoShreddingModule.REDACTED, mapper.readValue(preErasure, UserRegistered.class).email());
    assertEquals("new@example.com", mapper.readValue(postReinstate, UserRegistered.class).email());
  }

  @Test
  void nonEncryptedFieldsUnaffected() throws Exception {
    var event = new UserRegistered("user-7", "charlie@example.com", "de_DE");

    String json = mapper.writeValueAsString(event);

    // Parse as raw JSON to verify non-encrypted fields are plain text
    var tree = mapper.readTree(json);
    assertEquals("user-7", tree.get("userId").asText());
    assertEquals("de_DE", tree.get("locale").asText());
    // email should NOT be plaintext
    assertNotEquals("charlie@example.com", tree.get("email").asText());
  }

  @Test
  void multipleEncryptedFieldsAreAllEncrypted() throws Exception {
    var event = new MultipleEncryptedFields("cust-1", "eve@test.com", "Eve Smith", "gold");

    String json = mapper.writeValueAsString(event);

    assertFalse(json.contains("eve@test.com"));
    assertFalse(json.contains("Eve Smith"));
    assertTrue(json.contains("cust-1"));
    assertTrue(json.contains("gold"));

    var restored = mapper.readValue(json, MultipleEncryptedFields.class);
    assertEquals("cust-1", restored.customerId());
    assertEquals("eve@test.com", restored.email());
    assertEquals("Eve Smith", restored.fullName());
    assertEquals("gold", restored.tier());
  }

  @Test
  void multipleEncryptedFieldsShreddedAfterKeyDeletion() throws Exception {
    var event = new MultipleEncryptedFields("cust-2", "frank@test.com", "Frank Jones", "silver");

    String json = mapper.writeValueAsString(event);
    cryptoEngine.deleteKey(SubjectId.of("cust-2"));

    var restored = mapper.readValue(json, MultipleEncryptedFields.class);
    assertEquals("cust-2", restored.customerId());
    assertEquals(CryptoShreddingModule.REDACTED, restored.email());
    assertEquals(CryptoShreddingModule.REDACTED, restored.fullName());
    assertEquals("silver", restored.tier());
  }

  @Test
  void constructorRejectsNullCryptoEngine() {
    assertThrows(IllegalArgumentException.class, () -> new CryptoShreddingModule(null));
  }

  @Test
  void serializationFailsFastWhenSubjectIdIsNull() {
    // A null subjectId must never silently downgrade to plaintext serialization.
    var event = new UserRegistered(null, "leak@example.com", "en_US");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(event));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain");
    assertTrue(cause.getMessage().contains("email"), "message should name the encrypted field");
    assertTrue(cause.getMessage().contains("userId"), "message should name the subjectId field");
    // This refusal is raised BY THIS MODULE and is deterministic — re-serializing the
    // same state fails identically forever. It must therefore be distinguishable from the
    // engine-raised CryptoOperationException a Vault/KMS outage produces, which is transient and
    // must keep retrying. SagaRunner relies on exactly this split to decide "quarantine this
    // event" vs "retry the batch"; without it the only options are dead-lettering during a routine
    // KMS outage or wedging the subscription forever.
    assertInstanceOf(
        CryptoMappingException.class,
        cause,
        "a CryptoShreddingModule refusal must be a CryptoMappingException, not a bare"
            + " CryptoOperationException");
  }

  record NonStringSubjectIdRecord(int userId, @Encrypted(subjectId = "userId") String secret) {}

  record MissingSubjectIdRecord(@Encrypted(subjectId = "nope") String secret) {}

  @Test
  void everyRefusalThisModuleRaisesIsAMappingException() {
    // Generalized past the one scenario above: the module's own shape/config refusals
    // are deterministic too, so the whole class carries the marker. The complement — a BARE
    // CryptoOperationException — therefore always means "the engine's key store failed", which is
    // the transient half that must keep retrying.
    var nonStringSubject =
        assertThrows(
            CryptoMappingException.class,
            () -> mapper.writeValueAsString(new NonStringSubjectIdRecord(42, "secret")));
    assertTrue(nonStringSubject.getMessage().contains("must be of type String"));

    var missingSubject =
        assertThrows(
            CryptoMappingException.class,
            () -> mapper.writeValueAsString(new MissingSubjectIdRecord("secret")));
    assertTrue(missingSubject.getMessage().contains("does not exist on the record"));
  }

  @Test
  void nullEncryptedValueWithNullSubjectIdSerializesAsNull() throws Exception {
    // No PII present — a null value may serialize as plain null even without a subjectId.
    var event = new UserRegistered(null, null, "en_US");

    String json = mapper.writeValueAsString(event);

    var tree = mapper.readTree(json);
    assertTrue(tree.get("email").isNull());
  }

  @Test
  void nullEncryptedValueWithPresentSubjectIdRoundTripsAsNullWithoutTouchingEngine()
      throws Exception {
    // A null @Encrypted value carries no PII: it must serialize as plain null and round-trip
    // back to null — without creating a key for the subject as an encrypt call would.
    var event = new UserRegistered("user-100", null, "en_US");

    String json = mapper.writeValueAsString(event);

    var tree = mapper.readTree(json);
    assertTrue(tree.get("email").isNull(), "null value must serialize as JSON null");
    assertFalse(
        cryptoEngine.isKeyAvailable(SubjectId.of("user-100")),
        "serializing a null value must not create a key for the subject");

    var restored = mapper.readValue(json, UserRegistered.class);
    assertEquals("user-100", restored.userId());
    assertNull(restored.email());
  }

  @Test
  void transientDecryptFailurePropagatesInsteadOfRedacting() throws Exception {
    var event = new UserRegistered("user-50", "grace@example.com", "fr_FR");
    String json = mapper.writeValueAsString(event);

    // Simulate a transient backend outage: the key still exists, decrypt just fails.
    CryptoEngine failing =
        new CryptoEngine() {
          @Override
          public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
            return cryptoEngine.encrypt(subjectId, plaintext);
          }

          @Override
          public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
            throw new CryptoOperationException("backend unavailable");
          }

          @Override
          public void deleteKey(SubjectId subjectId) {
            cryptoEngine.deleteKey(subjectId);
          }

          @Override
          public boolean isKeyAvailable(SubjectId subjectId) {
            return cryptoEngine.isKeyAvailable(subjectId);
          }
        };
    var failingMapper = new ObjectMapper();
    failingMapper.registerModule(new CryptoShreddingModule(failing));

    Exception thrown =
        assertThrows(Exception.class, () -> failingMapper.readValue(json, UserRegistered.class));
    assertNotNull(
        findCryptoCause(thrown),
        "transient failures must propagate, not silently become [REDACTED]");
  }

  @Test
  void corruptedBase64PropagatesInsteadOfRedacting() {
    String json = "{\"userId\":\"user-60\",\"email\":\"not-valid-base64!!!\",\"locale\":\"en\"}";

    assertThrows(Exception.class, () -> mapper.readValue(json, UserRegistered.class));
  }

  private static CryptoOperationException findCryptoCause(Throwable thrown) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      if (t instanceof CryptoOperationException coe) {
        return coe;
      }
    }
    return null;
  }

  @Test
  void polymorphicReadAsInterfaceDecrypts() throws Exception {
    PolymorphicEvent event = new PolymorphicUserRegistered("user-70", "henry@example.com");

    String json = mapper.writerFor(PolymorphicEvent.class).writeValueAsString(event);
    assertFalse(json.contains("henry@example.com"), "JSON should not contain plaintext email");
    assertTrue(json.contains("registered"), "JSON should carry the type discriminator");

    PolymorphicEvent restored = mapper.readValue(json, PolymorphicEvent.class);
    assertEquals("henry@example.com", ((PolymorphicUserRegistered) restored).email());
  }

  @Test
  void polymorphicReadAsConcreteClassDecrypts() throws Exception {
    // Reading the concrete record while the type discriminator is present routes through
    // deserializeWithType — it must decrypt, not silently hand back raw base64 ciphertext.
    PolymorphicEvent event = new PolymorphicUserRegistered("user-71", "iris@example.com");
    String json = mapper.writerFor(PolymorphicEvent.class).writeValueAsString(event);

    var restored = mapper.readValue(json, PolymorphicUserRegistered.class);
    assertEquals("user-71", restored.userId());
    assertEquals("iris@example.com", restored.email());
  }

  @Test
  void polymorphicShreddedFieldReturnsRedacted() throws Exception {
    PolymorphicEvent event = new PolymorphicUserRegistered("user-72", "judy@example.com");
    String json = mapper.writerFor(PolymorphicEvent.class).writeValueAsString(event);

    cryptoEngine.deleteKey(SubjectId.of("user-72"));

    PolymorphicEvent restored = mapper.readValue(json, PolymorphicEvent.class);
    assertEquals(CryptoShreddingModule.REDACTED, ((PolymorphicUserRegistered) restored).email());
  }

  record NonStringEncrypted(String userId, @Encrypted(subjectId = "userId") int age) {}

  record DanglingSubjectId(String userId, @Encrypted(subjectId = "nope") String email) {}

  record NonStringSubjectId(int userId, @Encrypted(subjectId = "userId") String email) {}

  // An @Encrypted field whose subjectId reference is ITSELF @Encrypted.
  record EncryptedSubjectIdRef(
      @Encrypted(subjectId = "ssn") String email,
      @Encrypted(subjectId = "id") String ssn,
      String id) {}

  @Test
  void nonStringEncryptedComponentFailsFastOnWrite() {
    // String.valueOf would serialize fine but every subsequent read would fail to reconstruct
    // the record — permanently poisoned events. Reject the mapping before anything is written.
    Exception thrown =
        assertThrows(
            Exception.class, () -> mapper.writeValueAsString(new NonStringEncrypted("u-1", 42)));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain");
    assertTrue(cause.getMessage().contains("age"), "message should name the component");
    assertTrue(cause.getMessage().contains("int"), "message should name the offending type");
  }

  @Test
  void nonStringEncryptedComponentFailsFastOnRead() {
    String json = "{\"userId\":\"u-1\",\"age\":42}";

    Exception thrown =
        assertThrows(Exception.class, () -> mapper.readValue(json, NonStringEncrypted.class));

    assertNotNull(findCryptoCause(thrown), "expected a CryptoOperationException in the chain");
  }

  @Test
  void danglingSubjectIdReferenceFailsFast() {
    Exception thrown =
        assertThrows(
            Exception.class,
            () -> mapper.writeValueAsString(new DanglingSubjectId("u-2", "kate@example.com")));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain");
    assertTrue(cause.getMessage().contains("nope"), "message should name the missing component");
  }

  @Test
  void nonStringSubjectIdComponentFailsFastOnRegistration() {
    // The encrypt path derives the key subjectId via String.valueOf(subjectIdValue); the decrypt
    // path reads the subjectId property's text off the stored JSON. For a non-String subjectId
    // component
    // those two derivations could silently diverge, producing the WRONG key on read with no error —
    // a silent data-loss trap. Reject the mapping before anything is written, exactly like the
    // existing non-String @Encrypted-component guard.
    Exception thrown =
        assertThrows(
            Exception.class,
            () -> mapper.writeValueAsString(new NonStringSubjectId(1, "leo@example.com")));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain");
    assertTrue(
        cause.getMessage().contains("userId"), "message should name the subjectId component");
    assertTrue(cause.getMessage().contains("String"), "message should require the String type");
    assertTrue(cause.getMessage().contains("int"), "message should name the offending type");
  }

  @Test
  void encryptedSubjectIdReferenceFailsFastOnRegistration() {
    // An @Encrypted field keyed by a subjectId component that is ITSELF @Encrypted.
    // On encrypt the key is derived from the subjectId's PLAINTEXT (minting an encryption_keys row
    // keyed by sha256(plaintext-ssn) that no forget(id) ever deletes — an unshreddable stray key
    // tied to the subject's PII); on read the subjectId is read from the stored JSON where it is
    // still Base64 CIPHERTEXT until its own field decrypts, so the derived key — and whether the
    // value reads back or silently redacts to [REDACTED] — flips with HashMap field-iteration
    // order.
    // Reject the mapping at registration, mirroring the non-String-subjectId guard, rather than
    // shipping an order-dependent silent redaction.
    Exception thrown =
        assertThrows(
            Exception.class,
            () ->
                mapper.writeValueAsString(
                    new EncryptedSubjectIdRef("alice@example.com", "123-45-6789", "id-1")));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain");
    String msg = cause.getMessage();
    assertTrue(
        msg.contains("ssn"), "message should name the @Encrypted subjectId component: " + msg);
    assertTrue(msg.contains("email"), "message should name the field that references it: " + msg);
  }

  @Test
  void literalRedactedValueRoundTripsWhileKeyExists() throws Exception {
    // A user whose real field value equals the tombstone marker must not be confused with a
    // shredded subject — while the key exists the literal value round-trips intact.
    var event = new UserRegistered("user-80", CryptoShreddingModule.REDACTED, "en_US");

    String json = mapper.writeValueAsString(event);
    var restored = mapper.readValue(json, UserRegistered.class);

    assertEquals(CryptoShreddingModule.REDACTED, restored.email());
    assertTrue(cryptoEngine.isKeyAvailable(SubjectId.of("user-80")), "key must still exist");
  }

  @Test
  void reserializingAlreadyRedactedFieldSucceedsInsteadOfPropagatingSubjectForgotten()
      throws Exception {
    // Defect 6: after a subject is forgotten, replaying/snapshotting an aggregate whose
    // @Encrypted field already holds the [REDACTED] tombstone (produced by a prior decrypt) must
    // succeed instead of failing forever. The real CryptoEngine on the deleted key throws
    // SubjectForgottenException from encrypt; a test double reproduces exactly that failure mode
    // (proving the fix reacts to the real exception, not merely skipping encrypt on any value).
    CryptoEngine forgottenSubjectEngine =
        new CryptoEngine() {
          @Override
          public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
            throw new SubjectForgottenException(subjectId.value());
          }

          @Override
          public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void deleteKey(SubjectId subjectId) {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean isKeyAvailable(SubjectId subjectId) {
            return false;
          }
        };
    var redactedMapper = new ObjectMapper();
    redactedMapper.registerModule(new CryptoShreddingModule(forgottenSubjectEngine));

    var forgottenState = new UserRegistered("user-81", CryptoShreddingModule.REDACTED, "en_US");

    String json = redactedMapper.writeValueAsString(forgottenState);

    var tree = redactedMapper.readTree(json);
    assertEquals(
        CryptoShreddingModule.REDACTED,
        tree.get("email").asText(),
        "the tombstone marker must be written literally instead of propagating the exception");
  }

  @Test
  void newRealPiiValueForForgottenSubjectStillThrowsSubjectForgottenException() throws Exception {
    // The REDACTED passthrough must not weaken the guard: a genuinely NEW plaintext value (not
    // equal to the marker) for a subject whose key was deleted must still hit cryptoEngine.encrypt
    // and be rejected — new PII collection for a forgotten subject is never acceptable.
    var event = new UserRegistered("user-82", "kept@example.com", "en_US");
    mapper.writeValueAsString(event); // creates the key
    cryptoEngine.deleteKey(SubjectId.of("user-82"));

    var newPiiForForgottenSubject =
        new UserRegistered("user-82", "new-real-value@example.com", "en_US");

    Exception thrown =
        assertThrows(Exception.class, () -> mapper.writeValueAsString(newPiiForForgottenSubject));

    Throwable cause = thrown;
    boolean foundSubjectForgotten = false;
    while (cause != null) {
      if (cause instanceof SubjectForgottenException) {
        foundSubjectForgotten = true;
        break;
      }
      cause = cause.getCause();
    }
    assertTrue(
        foundSubjectForgotten,
        "expected SubjectForgottenException in the cause chain, got: " + thrown);
  }

  record RenamedEncrypted(
      @JsonProperty("emailAddress") @Encrypted(subjectId = "id") String email, String id) {}

  @Test
  void jsonPropertyRenamedEncryptedFieldIsCiphertextNotPlaintext() throws Exception {
    // A @JsonProperty-renamed @Encrypted component serializes under its JSON name; the
    // encrypt-writer
    // lookup must still match it (via the record component), or the PII leaks as plaintext.
    var event = new RenamedEncrypted("alice@example.com", "u-renamed");

    String json = mapper.writeValueAsString(event);

    assertFalse(
        json.contains("alice@example.com"),
        "@JsonProperty-renamed @Encrypted field must be encrypted at rest, not written as plaintext");
    var tree = mapper.readTree(json);
    assertTrue(tree.has("emailAddress"), "field must serialize under its @JsonProperty name");
    assertNotEquals(
        "alice@example.com",
        tree.get("emailAddress").asText(),
        "the renamed property must carry ciphertext, not the plaintext PII");

    // Round-trips: decrypts back through the renamed property.
    var restored = mapper.readValue(json, RenamedEncrypted.class);
    assertEquals("alice@example.com", restored.email());
    assertEquals("u-renamed", restored.id());
  }

  record MinLengthValidatedPii(
      String customerId, @Encrypted(subjectId = "customerId") String email) {
    MinLengthValidatedPii {
      // Validates the PII field but does NOT tolerate the crypto-shredding tombstone: "[REDACTED]"
      // (10 chars) fails this check, while ciphertext and real plaintext pass it.
      if (email != null && email.length() < 12) {
        throw new IllegalArgumentException("email too short");
      }
    }
  }

  record TolerantValidatedPii(
      String customerId, @Encrypted(subjectId = "customerId") String email) {
    TolerantValidatedPii {
      // Validates the PII field (non-blank) while tolerating the crypto-shredding tombstone —
      // "[REDACTED]" is non-blank, so a forgotten subject's field stays reconstructible (contract).
      if (email != null && email.isBlank()) {
        throw new IllegalArgumentException("email must not be blank");
      }
    }
  }

  @Test
  void forgottenValidatingRecord_surfacesActionableErrorNamingFieldAndSubject() throws Exception {
    // A record whose constructor validates the @Encrypted field and rejects
    // the tombstone becomes unloadable after erasure. Records cannot be built while bypassing their
    // compact-constructor validation, so the reconstruction must fail with an ACTIONABLE error
    // naming the record/field/subject — not the opaque "Failed to reconstruct record ...".
    var event = new MinLengthValidatedPii("cust-77", "alice@example.com");
    String json = mapper.writeValueAsString(event); // ciphertext (>= 12 chars) passes the guard

    cryptoEngine.deleteKey(SubjectId.of("cust-77")); // GDPR forget

    Exception thrown =
        assertThrows(Exception.class, () -> mapper.readValue(json, MinLengthValidatedPii.class));
    CryptoOperationException coe = findCryptoCause(thrown);
    assertNotNull(coe, "expected a CryptoOperationException in the cause chain");
    String msg = coe.getMessage();
    assertTrue(msg.contains("email"), "message must name the offending @Encrypted field: " + msg);
    // The subject is named by its SHA-256 hash, never the raw id (which may be PII).
    assertTrue(
        msg.contains(SubjectId.of("cust-77").redacted()),
        "message must name the forgotten subject by hash: " + msg);
    assertFalse(msg.contains("cust-77"), "message must not leak the raw subject id: " + msg);
    assertTrue(
        msg.contains(CryptoShreddingModule.REDACTED),
        "message must name the [REDACTED] tombstone: " + msg);
    assertTrue(
        msg.toLowerCase().contains("tolerate") || msg.toLowerCase().contains("must accept"),
        "message must give remediation guidance (tolerate the sentinel): " + msg);
  }

  @Test
  void forgottenValidatingButTolerantRecord_stillLoadsShowingRedacted() throws Exception {
    // A validating @Encrypted record that follows the sentinel-tolerance
    // contract must still LOAD after erasure, showing the [REDACTED] tombstone — replay keeps
    // working. This guards the central crypto-shredding promise for contract-following records.
    var event = new TolerantValidatedPii("cust-88", "bob@example.com");
    String json = mapper.writeValueAsString(event);

    cryptoEngine.deleteKey(SubjectId.of("cust-88")); // GDPR forget

    var restored = mapper.readValue(json, TolerantValidatedPii.class);
    assertEquals("cust-88", restored.customerId());
    assertEquals(CryptoShreddingModule.REDACTED, restored.email());
  }

  record FormatValidatedPii(String customerId, @Encrypted(subjectId = "customerId") String email) {
    FormatValidatedPii {
      // FORMAT validation (not a length check): the Base64 ciphertext never contains '@', so this
      // rejects ciphertext while accepting real plaintext. It documents the data-loss-class
      // bug — decrypt-before-construct is what lets this validate the decrypted plaintext on read.
      // It deliberately does NOT tolerate the "[REDACTED]" tombstone (no '@'), so the post-forget
      // path still exercises the actionable error.
      if (email != null && !email.contains("@")) {
        throw new IllegalArgumentException("email must contain '@'");
      }
    }
  }

  @Test
  void formatValidatingEncryptedField_loadsOnRead_validationRunsOnPlaintextNotCiphertext()
      throws Exception {
    // A @Encrypted field with FORMAT validation (email must contain '@') written
    // with a valid value must LOAD on read with the key present and no forget. Pre-fix the record
    // was constructed from the raw JSON (Base64 CIPHERTEXT, which has no '@') BEFORE decryption, so
    // the compact constructor threw and the record was permanently unreadable — data loss with the
    // key fully intact. Decrypt-before-construct makes validation run on the real plaintext.
    var event = new FormatValidatedPii("cust-mf2", "alice@example.com");
    String json = mapper.writeValueAsString(event);
    assertFalse(json.contains("alice@example.com"), "email must be encrypted at rest");
    assertFalse(mapper.readTree(json).get("email").asText().contains("@"), "ciphertext has no '@'");

    var restored = mapper.readValue(json, FormatValidatedPii.class);

    assertEquals("cust-mf2", restored.customerId());
    assertEquals("alice@example.com", restored.email());
  }

  @Test
  void formatValidatingEncryptedField_postForget_surfacesActionableError() throws Exception {
    // The post-forget path still works: once the subject is shredded the field decrypts to the
    // tombstone, which this format check rejects (the tombstone tolerance the field opted out of) —
    // so reconstruction must fail with the actionable error, not an opaque one.
    var event = new FormatValidatedPii("cust-mf2b", "bob@example.com");
    String json = mapper.writeValueAsString(event);

    cryptoEngine.deleteKey(SubjectId.of("cust-mf2b")); // GDPR forget

    Exception thrown =
        assertThrows(Exception.class, () -> mapper.readValue(json, FormatValidatedPii.class));
    CryptoOperationException coe = findCryptoCause(thrown);
    assertNotNull(coe, "expected a CryptoOperationException in the cause chain");
    assertTrue(coe.getMessage().contains("email"), "message must name the field: " + coe);
    // The subject is named by its SHA-256 hash, never the raw id.
    assertTrue(
        coe.getMessage().contains(SubjectId.of("cust-mf2b").redacted()),
        "message must name the subject by hash: " + coe);
    assertFalse(
        coe.getMessage().contains("cust-mf2b"), "message must not leak the raw subject id: " + coe);
    assertTrue(
        coe.getMessage().contains(CryptoShreddingModule.REDACTED),
        "message must name the tombstone: " + coe);
  }

  record Inner(String customerId, @Encrypted(subjectId = "customerId") String ssn) {}

  record Outer(String orderId, Inner customer) {}

  @Test
  void nestedRecordEncryptedFieldsRoundTrip() throws Exception {
    var event = new Outer("order-1", new Inner("cust-9", "123-45-6789"));

    String json = mapper.writeValueAsString(event);
    assertFalse(json.contains("123-45-6789"), "nested PII must be encrypted");

    var restored = mapper.readValue(json, Outer.class);
    assertEquals("123-45-6789", restored.customer().ssn());
  }

  @Test
  void nullSubjectIdOnReadKeepsCiphertextInsteadOfFailing() throws Exception {
    // Stored data may carry a null subjectId (e.g. produced before the field was required).
    // Without a subject the value cannot be decrypted — it stays as-is rather than crashing.
    var event = new UserRegistered("user-90", "mia@example.com", "en_US");
    String json = mapper.writeValueAsString(event);
    String base64 = mapper.readTree(json).get("email").asText();
    String jsonWithNullSubject =
        "{\"userId\":null,\"email\":\"" + base64 + "\",\"locale\":\"en_US\"}";

    var restored = mapper.readValue(jsonWithNullSubject, UserRegistered.class);

    assertNull(restored.userId());
    assertEquals(base64, restored.email(), "ciphertext must be preserved, not decrypted or lost");
  }

  @Test
  void nonRecordTypesAreUnaffected() throws Exception {
    // A plain POJO should serialize/deserialize normally
    String json = mapper.writeValueAsString(new PlainPojo("hello", 42));
    var restored = mapper.readValue(json, PlainPojo.class);
    assertEquals("hello", restored.name);
    assertEquals(42, restored.value);
  }

  static class PlainPojo {
    public String name;
    public int value;

    public PlainPojo() {}

    public PlainPojo(String name, int value) {
      this.name = name;
      this.value = value;
    }
  }

  // ---- The fail-closed @Encrypted serialization guard ----

  record IgnoredEncryptedField(
      String userId, @JsonIgnore @Encrypted(subjectId = "userId") String secret) {}

  @Test
  void unmatchedEncryptedComponentFailsClosedInsteadOfWritingPlaintext() {
    // Fail-closed safety net against a PII plaintext leak. findEncryptedFields() discovers
    // the
    // @Encrypted component by reflecting over record components, so it is in encryptedFields — but
    // because it is @JsonIgnore'd, Jackson drops it from the serialization properties, so the
    // encrypting writer never wraps it. The fail-closed guard is the SOLE line that then prevents
    // that PII field from being written unencrypted (or silently dropped): it must refuse to build
    // the serializer and THROW, never emit plaintext at rest.
    var event = new IgnoredEncryptedField("user-1", "top-secret-pii");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(event));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(
        cause,
        "serialization must fail CLOSED with a CryptoOperationException, never emit the PII field");
    assertTrue(
        cause.getMessage().contains("secret"),
        "message should name the unresolved @Encrypted component");
    assertTrue(
        cause.getMessage().contains("PLAINTEXT"),
        "message should state the fail-closed reason (avoid writing PII as PLAINTEXT)");
  }

  // ---- The subjectId component referenced by an @Encrypted field must itself be
  // serialized as a JSON property, or the ciphertext is permanently undecryptable ----

  record IgnoredSubjectId(
      @Encrypted(subjectId = "customerId") String email, @JsonIgnore String customerId) {}

  @Test
  void jsonIgnoredSubjectIdFailsClosedAtWriteInsteadOfWritingUndecryptableCiphertext() {
    // @JsonIgnore on the SUBJECT ID component — a natural,
    // privacy-motivated move ("keep the customer id off the wire"). Pre-fix the write SUCCEEDED
    // (EncryptingPropertyWriter reads the subjectId reflectively off the live bean, so encryption
    // worked) but the stored JSON carried no subjectId property: {"email":"<base64>"}. Every
    // subsequent read then found subjectIdNode == null, fell into the null-subjectId read
    // tolerance, and silently returned the Base64 ciphertext AS the field value — permanently
    // undecryptable data (the key reference was never persisted), with projections storing the
    // garbage as real data. Serializer construction must fail closed instead.
    var event = new IgnoredSubjectId("pii@example.com", "c1");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(event));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("email"), "message should name the @Encrypted component: " + msg);
    assertTrue(msg.contains("customerId"), "message should name the subjectId component: " + msg);
    assertTrue(msg.contains("undecryptable"), "message should state the consequence: " + msg);
    assertTrue(msg.contains("@JsonIgnore"), "message should give the remedy: " + msg);
  }

  record RenamedSubjectIdCustomer(
      @Encrypted(subjectId = "userId") String email, @JsonProperty("uid") String userId) {}

  @Test
  void jsonPropertyRenamedSubjectIdSerializesUnderRenameAndRoundTrips() throws Exception {
    // Edge (no-false-positive pin, and the suspected fourth bug that turned out NOT to
    // be one): a @JsonProperty-renamed subjectId is still a serialized property — the guard must
    // not fire — and BOTH sides resolve it through the same mapping: the write side reads the live
    // record by COMPONENT name (rename-proof by construction), the read side resolves the JSON
    // node through the component->JSON-name map built from findProperties (rename parity),
    // so the ciphertext decrypts through "uid". Renaming is the supported way to keep the Java
    // name off the wire; @JsonIgnore is not.
    var event = new RenamedSubjectIdCustomer("pii@example.com", "u-1");

    String json = mapper.writeValueAsString(event);

    assertFalse(json.contains("pii@example.com"), "PII must be ciphertext at rest: " + json);
    var tree = mapper.readTree(json);
    assertEquals("u-1", tree.get("uid").asText(), "subjectId serializes under its rename");
    assertFalse(tree.has("userId"), "subjectId must not also appear under the component name");

    var restored = mapper.readValue(json, RenamedSubjectIdCustomer.class);
    assertEquals("pii@example.com", restored.email());
    assertEquals("u-1", restored.userId());
  }

  record BothRenamedCustomer(
      @JsonProperty("emailAddress") @Encrypted(subjectId = "userId") String email,
      @JsonProperty("uid") String userId) {}

  @Test
  void bothEncryptedFieldAndSubjectIdRenamedRoundTrip() throws Exception {
    // The full rename matrix: encrypted field AND its subjectId reference both @JsonProperty-
    // renamed. The encrypt-writer lookup keys by component name, the subjectId
    // presence check resolves through the same component names, and the tree-level decrypt looks
    // both up under their external names — everything must compose.
    var event = new BothRenamedCustomer("pii@example.com", "u-2");

    String json = mapper.writeValueAsString(event);

    assertFalse(json.contains("pii@example.com"), "PII must be ciphertext at rest: " + json);
    var tree = mapper.readTree(json);
    assertTrue(tree.has("emailAddress"), "encrypted field serializes under its rename: " + json);
    assertEquals("u-2", tree.get("uid").asText(), "subjectId serializes under its rename");

    var restored = mapper.readValue(json, BothRenamedCustomer.class);
    assertEquals("pii@example.com", restored.email());
    assertEquals("u-2", restored.userId());
  }

  @Test
  void unwrappedSubjectIdResolvesUnderPrefixOnBothSides() throws Exception {
    // Unwrap interplay: when the @Encrypted-carrying record is embedded via
    // @JsonUnwrapped(prefix), the subjectId property serializes under its PREFIXED external name
    // (cust_customerId) — still a serialized property (guard silent), and the read side's
    // unwrappingDeserializer transforms the component->JSON map so the subjectId lookup follows
    // the prefix too (read-side symmetry). Pinned here from the subjectId's
    // perspective: the ciphertext must decrypt back through the prefixed subjectId, and after a
    // forget it must redact — proving the subjectId (not some fallback) drove the key lookup.
    var form =
        new OrderFormPojoContainer(
            "form-a92", new UnwrappedCustomer("unwrap-sid@example.com", "cust-a92"));

    String json = mapper.writeValueAsString(form);
    var tree = mapper.readTree(json);
    assertEquals("cust-a92", tree.get("cust_customerId").asText());

    var restored = mapper.readValue(json, OrderFormPojoContainer.class);
    assertEquals("unwrap-sid@example.com", restored.customer.email());

    cryptoEngine.deleteKey(SubjectId.of("cust-a92"));
    var afterForget = mapper.readValue(json, OrderFormPojoContainer.class);
    assertEquals(
        CryptoShreddingModule.REDACTED,
        afterForget.customer.email(),
        "post-forget redaction proves the prefixed subjectId drove the key lookup");
  }

  // ---- The redact fallback is observable and distinguishes systemic from per-subject
  // ----

  /** Capturing metrics double: counts the two crypto observability signals. */
  static final class CountingMetrics implements StreamRuneMetrics {
    final AtomicInteger redacted = new AtomicInteger();
    final AtomicInteger systemic = new AtomicInteger();

    @Override
    public void recordSubjectRedacted() {
      redacted.incrementAndGet();
    }

    @Override
    public void recordSystemicKeyStoreFailure() {
      systemic.incrementAndGet();
    }
  }

  // ---- @JsonUnwrapped(prefix/suffix) must never downgrade the
  // encrypting writer to a plaintext writer (Jackson's rename/_new derivation path) ----

  record UnwrappedCustomer(@Encrypted(subjectId = "customerId") String email, String customerId) {}

  record OrderPlacedRecordContainer(
      String orderId, @JsonUnwrapped(prefix = "cust_") UnwrappedCustomer customer) {}

  static class OrderFormPojoContainer {
    public String formId;

    @JsonUnwrapped(prefix = "cust_")
    public UnwrappedCustomer customer;

    OrderFormPojoContainer() {}

    OrderFormPojoContainer(String formId, UnwrappedCustomer customer) {
      this.formId = formId;
      this.customer = customer;
    }
  }

  @Test
  void unwrappedWithPrefix_recordContainer_writesCiphertextNotPlaintext() throws Exception {
    // A record with an @Encrypted component
    // embedded via @JsonUnwrapped(prefix = "cust_"). Jackson derives the container's unwrapping
    // serializer by calling BeanPropertyWriter.rename(NameTransformer) on each of the inner
    // record's writers; for a CHANGED name rename calls _new(PropertyName), whose base
    // implementation constructs a plain BeanPropertyWriter copy — silently discarding the
    // encrypting wrapper and appending "cust_email": "<plaintext PII>" into the event store.
    var event =
        new OrderPlacedRecordContainer(
            "order-1", new UnwrappedCustomer("alice@example.com", "cust-unwrap-1"));

    String json = mapper.writeValueAsString(event);

    assertFalse(
        json.contains("alice@example.com"),
        "prefixed @Encrypted field must be ciphertext at rest, never plaintext PII: " + json);
    var tree = mapper.readTree(json);
    assertTrue(tree.has("cust_email"), "field must serialize under its prefixed name: " + json);
    assertEquals("cust-unwrap-1", tree.get("cust_customerId").asText());
    // The strongest ciphertext proof: the stored value decrypts back to the plaintext under the
    // SAME subject key — the rename must not break the subjectId component binding either.
    byte[] decrypted =
        cryptoEngine.decrypt(
            SubjectId.of("cust-unwrap-1"),
            java.util.Base64.getDecoder().decode(tree.get("cust_email").asText()));
    assertEquals(
        "alice@example.com", new String(decrypted, java.nio.charset.StandardCharsets.UTF_8));

    // Full round trip through the record container (jackson-databind 2.19+ renames creator
    // properties of @JsonUnwrapped targets): the prefixed ciphertext must decrypt back.
    var restored = mapper.readValue(json, OrderPlacedRecordContainer.class);
    assertEquals("order-1", restored.orderId());
    assertEquals("alice@example.com", restored.customer().email());
    assertEquals("cust-unwrap-1", restored.customer().customerId());
  }

  @Test
  void unwrappedWithPrefix_pojoContainer_writesCiphertextNotPlaintext() throws Exception {
    // Same derivation path with a POJO container — the container may be a POJO
    // or a record; the downgrade happens in the INNER record's writers either way.
    var form =
        new OrderFormPojoContainer(
            "form-1", new UnwrappedCustomer("bob@example.com", "cust-unwrap-2"));

    String json = mapper.writeValueAsString(form);

    assertFalse(
        json.contains("bob@example.com"),
        "prefixed @Encrypted field must be ciphertext at rest, never plaintext PII: " + json);
    var tree = mapper.readTree(json);
    assertTrue(tree.has("cust_email"), "field must serialize under its prefixed name: " + json);
    byte[] decrypted =
        cryptoEngine.decrypt(
            SubjectId.of("cust-unwrap-2"),
            java.util.Base64.getDecoder().decode(tree.get("cust_email").asText()));
    assertEquals("bob@example.com", new String(decrypted, java.nio.charset.StandardCharsets.UTF_8));
  }

  @Test
  void unwrappedWithPrefix_roundTripsThroughPojoContainer() throws Exception {
    // Decrypt-side symmetry: the container's unwrapping deserializer buffers the prefixed
    // properties (cust_email, cust_customerId); the decrypting deserializer must resolve the
    // ciphertext and the subjectId under those TRANSFORMED names, decrypt in the tree, and hand
    // plaintext to the record constructor — proving the full write->read contract under prefix.
    var form =
        new OrderFormPojoContainer(
            "form-rt", new UnwrappedCustomer("carol@example.com", "cust-unwrap-3"));

    String json = mapper.writeValueAsString(form);
    assertFalse(json.contains("carol@example.com"), "PII must be ciphertext at rest: " + json);

    var restored = mapper.readValue(json, OrderFormPojoContainer.class);
    assertEquals("form-rt", restored.formId);
    assertEquals("carol@example.com", restored.customer.email());
    assertEquals("cust-unwrap-3", restored.customer.customerId());
  }

  record OrderPlacedSuffixContainer(
      String orderId, @JsonUnwrapped(suffix = "_cust") UnwrappedCustomer customer) {}

  static class OrderFormSuffixPojoContainer {
    public String formId;

    @JsonUnwrapped(suffix = "_cust")
    public UnwrappedCustomer customer;

    OrderFormSuffixPojoContainer() {}

    OrderFormSuffixPojoContainer(String formId, UnwrappedCustomer customer) {
      this.formId = formId;
      this.customer = customer;
    }
  }

  @Test
  void unwrappedWithSuffix_writesCiphertextAndRoundTrips() throws Exception {
    // The suffix transform exercises the same rename/_new derivation with a different
    // NameTransformer — the writer must keep encrypting under "email_cust" as well.
    var event =
        new OrderPlacedSuffixContainer(
            "order-sfx", new UnwrappedCustomer("dave@example.com", "cust-unwrap-4"));

    String json = mapper.writeValueAsString(event);

    assertFalse(json.contains("dave@example.com"), "PII must be ciphertext at rest: " + json);
    var tree = mapper.readTree(json);
    assertTrue(tree.has("email_cust"), "field must serialize under its suffixed name: " + json);
    byte[] decrypted =
        cryptoEngine.decrypt(
            SubjectId.of("cust-unwrap-4"),
            java.util.Base64.getDecoder().decode(tree.get("email_cust").asText()));
    assertEquals(
        "dave@example.com", new String(decrypted, java.nio.charset.StandardCharsets.UTF_8));

    var form =
        new OrderFormSuffixPojoContainer(
            "form-sfx", new UnwrappedCustomer("erin@example.com", "cust-unwrap-5"));
    String formJson = mapper.writeValueAsString(form);
    assertFalse(formJson.contains("erin@example.com"), "PII must be ciphertext: " + formJson);
    var restored = mapper.readValue(formJson, OrderFormSuffixPojoContainer.class);
    assertEquals("erin@example.com", restored.customer.email());
  }

  static class OrderFormNoPrefixPojoContainer {
    public String formId;

    @JsonUnwrapped public UnwrappedCustomer customer;

    OrderFormNoPrefixPojoContainer() {}

    OrderFormNoPrefixPojoContainer(String formId, UnwrappedCustomer customer) {
      this.formId = formId;
      this.customer = customer;
    }
  }

  @Test
  void unwrappedWithoutPrefix_regressionStillEncryptsAndRoundTrips() throws Exception {
    // Regression pin for the previously-working case: with no prefix/suffix the rename returns the
    // SAME writer (name unchanged), so this worked before the fix and must keep working after.
    var form =
        new OrderFormNoPrefixPojoContainer(
            "form-np", new UnwrappedCustomer("frank@example.com", "cust-unwrap-6"));

    String json = mapper.writeValueAsString(form);

    assertFalse(json.contains("frank@example.com"), "PII must be ciphertext at rest: " + json);
    var tree = mapper.readTree(json);
    assertTrue(tree.has("email"), "un-prefixed unwrapped field keeps its own name: " + json);

    var restored = mapper.readValue(json, OrderFormNoPrefixPojoContainer.class);
    assertEquals("frank@example.com", restored.customer.email());
    assertEquals("cust-unwrap-6", restored.customer.customerId());
  }

  @Test
  void unwrappedWithPrefix_afterForget_readsRedactedTombstone() throws Exception {
    // GDPR erasure must reach prefixed ciphertext exactly like plain ciphertext: after forget()
    // deletes the subject's key, the prefixed field decrypts to the [REDACTED] tombstone — the
    // stored bytes are unreadable, and replay keeps working.
    var form =
        new OrderFormPojoContainer(
            "form-gdpr", new UnwrappedCustomer("gone@example.com", "cust-unwrap-7"));
    String json = mapper.writeValueAsString(form);
    assertFalse(json.contains("gone@example.com"), "PII must be ciphertext at rest: " + json);

    cryptoEngine.deleteKey(SubjectId.of("cust-unwrap-7")); // GDPR forget

    var restored = mapper.readValue(json, OrderFormPojoContainer.class);
    assertEquals(CryptoShreddingModule.REDACTED, restored.customer.email());
    assertEquals("cust-unwrap-7", restored.customer.customerId());
  }

  @Test
  void unwrappedWithPrefix_nullEncryptedValueSerializesAsNullUnderPrefixedName() throws Exception {
    // The renamed writer's null path routes through super.serializeAsField — it must emit the
    // JSON null under the TRANSFORMED name and round-trip back to null (no PII, no engine call).
    var form =
        new OrderFormPojoContainer("form-null", new UnwrappedCustomer(null, "cust-unwrap-8"));

    String json = mapper.writeValueAsString(form);

    var tree = mapper.readTree(json);
    assertTrue(tree.has("cust_email"), "null must serialize under the prefixed name: " + json);
    assertTrue(tree.get("cust_email").isNull(), "null value must stay a JSON null: " + json);

    var restored = mapper.readValue(json, OrderFormPojoContainer.class);
    assertNull(restored.customer.email());
    assertEquals("cust-unwrap-8", restored.customer.customerId());
  }

  @Test
  void unwrappedWithPrefix_nullSubjectIdFailsFastNamingPrefixedField() {
    // The renamed writer keeps the fail-fast null-subjectId guard, and its error names the field
    // under its external (prefixed) name — the name an operator sees in the payload.
    var form =
        new OrderFormPojoContainer("form-nsid", new UnwrappedCustomer("leak@example.com", null));

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(form));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    assertTrue(
        cause.getMessage().contains("cust_email"),
        "message should name the prefixed field: " + cause.getMessage());
  }

  @com.fasterxml.jackson.annotation.JsonFormat(
      shape = com.fasterxml.jackson.annotation.JsonFormat.Shape.ARRAY)
  record ArrayShapedEncrypted(String userId, @Encrypted(subjectId = "userId") String email) {}

  @Test
  void arrayShapeOverEncryptedComponentFailsClosedInsteadOfWritingPlaintext() {
    // Sibling downgrade path found by generalizing the unwrapped-property guard:
    // @JsonFormat(shape=ARRAY)
    // serializes via BeanPropertyWriter.serializeAsElement, bypassing the encrypting
    // serializeAsField override — the base implementation writes the raw PLAINTEXT value as a
    // positional array element. A positional element carries no property name for the decrypt side
    // to resolve, so there is no sound encrypted round-trip either: serialization must fail
    // loudly, never emit plaintext.
    var event = new ArrayShapedEncrypted("u-arr", "leak@example.com");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(event));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    assertTrue(
        cause.getMessage().contains("ARRAY"),
        "message should name the unsupported shape: " + cause.getMessage());
    assertTrue(
        cause.getMessage().contains("PLAINTEXT"),
        "message should state the fail-closed reason: " + cause.getMessage());
  }

  // ---- Serializer shapes that replace the property-based bean serializer must
  // fail closed — they bypass changeProperties (and its fail-closed wrap check) entirely, so
  // the @Encrypted PII would be written verbatim as plaintext ----

  record JsonValueCustomer(@Encrypted(subjectId = "customerId") String email, String customerId) {
    @JsonValue
    public String wire() {
      return customerId + "|" + email;
    }
  }

  @Test
  void jsonValueOnEncryptedRecordFailsClosedInsteadOfWritingPlaintext() {
    // @JsonValue makes Jackson build a JsonValueSerializer
    // instead of a BeanSerializer, so changeProperties — and the line-322 "every @Encrypted
    // component must be wrapped" guard inside it — NEVER runs. Pre-fix the wire form was
    // "c1|pii@example.com": the raw PII verbatim in the append-only event store, unreachable by
    // any later GDPR forget. A @JsonValue wire form is a single opaque scalar with no property
    // slot to carry ciphertext + subjectId, so no sound encrypted round-trip exists — the only
    // safe behavior is to refuse serializer construction loudly.
    var event = new JsonValueCustomer("pii@example.com", "c1");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(event));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("JsonValueCustomer"), "message should name the class: " + msg);
    assertTrue(msg.contains("@JsonValue"), "message should name the offending mechanism: " + msg);
    assertTrue(msg.contains("PLAINTEXT"), "message should state the fail-closed reason: " + msg);
    assertTrue(msg.contains("email"), "message should name the @Encrypted component: " + msg);
  }

  static final class WireFormSerializer
      extends com.fasterxml.jackson.databind.JsonSerializer<CustomSerializedCustomer> {
    @Override
    public void serialize(
        CustomSerializedCustomer value,
        com.fasterxml.jackson.core.JsonGenerator gen,
        com.fasterxml.jackson.databind.SerializerProvider serializers)
        throws java.io.IOException {
      gen.writeString(value.customerId() + "|" + value.email());
    }
  }

  @com.fasterxml.jackson.databind.annotation.JsonSerialize(using = WireFormSerializer.class)
  record CustomSerializedCustomer(
      @Encrypted(subjectId = "customerId") String email, String customerId) {}

  @Test
  void classLevelJsonSerializeUsingOnEncryptedRecordFailsClosed() {
    // Sibling route found by generalizing the property-level shape guards: a class-level
    // @JsonSerialize(using = ...)
    // is returned directly from the serializer factory's annotation lookup — NO
    // BeanSerializerModifier hook (neither changeProperties nor modifySerializer) ever fires for
    // the record class (verified empirically on jackson-databind 2.19), so the user serializer
    // writes the PII plaintext with zero framework involvement. Only an annotation-introspection
    // guard can fail this closed.
    var event = new CustomSerializedCustomer("pii@example.com", "c1");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(event));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("CustomSerializedCustomer"), "message should name the class: " + msg);
    assertTrue(msg.contains("@JsonSerialize"), "message should name the mechanism: " + msg);
    assertTrue(msg.contains("using"), "message should name the offending attribute: " + msg);
    assertTrue(msg.contains("PLAINTEXT"), "message should state the fail-closed reason: " + msg);
  }

  static final class WireFormConverter
      extends com.fasterxml.jackson.databind.util.StdConverter<ConvertedCustomer, String> {
    @Override
    public String convert(ConvertedCustomer value) {
      return value.customerId() + "|" + value.email();
    }
  }

  @com.fasterxml.jackson.databind.annotation.JsonSerialize(converter = WireFormConverter.class)
  record ConvertedCustomer(@Encrypted(subjectId = "customerId") String email, String customerId) {}

  @Test
  void classLevelJsonSerializeConverterOnEncryptedRecordFailsClosed() {
    // Same family: @JsonSerialize(converter = ...) re-routes serializer construction to the
    // converter's OUTPUT type, so the record class never reaches any modifier hook either
    // (empirically the only modifySerializer call is for java.lang.String) — the converter reads
    // the live record's plaintext and it lands verbatim at rest. Fail closed at the annotation
    // lookup.
    var event = new ConvertedCustomer("pii@example.com", "c1");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(event));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("ConvertedCustomer"), "message should name the class: " + msg);
    assertTrue(msg.contains("converter"), "message should name the offending attribute: " + msg);
    assertTrue(msg.contains("PLAINTEXT"), "message should state the fail-closed reason: " + msg);
  }

  interface CustomerView {
    String email();

    String customerId();
  }

  @com.fasterxml.jackson.databind.annotation.JsonSerialize(as = CustomerView.class)
  record ViewSerializedCustomer(
      @Encrypted(subjectId = "customerId") String email, String customerId)
      implements CustomerView {}

  @Test
  void classLevelJsonSerializeAsOnEncryptedRecordFailsClosed() {
    // Same family: @JsonSerialize(as = ...) redirects to a DIFFERENT type's bean serializer
    // (here an interface view), whose plain property writers read this record's accessors —
    // including the @Encrypted ones — as ordinary plaintext getters. The record class itself
    // never reaches changeProperties, so the encrypting writers are never installed.
    var event = new ViewSerializedCustomer("pii@example.com", "c1");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(event));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("ViewSerializedCustomer"), "message should name the class: " + msg);
    assertTrue(msg.contains("as"), "message should name the offending attribute: " + msg);
  }

  @Test
  void encryptedRecordAsMapKeyFailsClosedInsteadOfLeakingToStringPlaintext() {
    // Same family, key path: a JSON object key is serialized via StdKeySerializer, which calls
    // toString() — for a record that is "UnwrappedCustomer[email=pii@example.com, ...]", a full
    // plaintext dump of every component. Keys route through modifyKeySerializer (never
    // changeProperties), and a key is a single opaque string with no slot for ciphertext +
    // subjectId, so no encrypted round-trip can exist. Refuse loudly.
    var map = java.util.Map.of(new UnwrappedCustomer("pii@example.com", "cust-key-1"), "v");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(map));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("UnwrappedCustomer"), "message should name the class: " + msg);
    assertTrue(msg.contains("KEY"), "message should name the key path: " + msg);
    assertTrue(msg.contains("toString"), "message should name the leak mechanism: " + msg);
  }

  @Test
  void collectionAndMapValuesOfEncryptedRecordsStillEncryptAtElementLevel() throws Exception {
    // No-false-positive pin for the shape guards: List elements and Map VALUES route
    // each element through the record's own (encrypting) bean serializer, and the container
    // classes themselves carry no @Encrypted components — both guards must stay silent and the
    // elements must be ciphertext at rest, round-tripping back to plaintext.
    var list = java.util.List.of(new UserRegistered("user-elem", "elem@example.com", "en_US"));
    String listJson = mapper.writeValueAsString(list);
    assertFalse(listJson.contains("elem@example.com"), "list element PII must be ciphertext");
    var restoredList =
        mapper.readValue(
            listJson,
            new com.fasterxml.jackson.core.type.TypeReference<java.util.List<UserRegistered>>() {});
    assertEquals("elem@example.com", restoredList.get(0).email());

    var map = java.util.Map.of("k", new UserRegistered("user-val", "val@example.com", "en_US"));
    String mapJson = mapper.writeValueAsString(map);
    assertFalse(mapJson.contains("val@example.com"), "map value PII must be ciphertext");
    var restoredMap =
        mapper.readValue(
            mapJson,
            new com.fasterxml.jackson.core.type.TypeReference<
                java.util.Map<String, UserRegistered>>() {});
    assertEquals("val@example.com", restoredMap.get("k").email());
  }

  record JsonValueEvolvedCustomer(
      @Encrypted(subjectId = "customerId") String email, String customerId) {
    @JsonValue
    public String wire() {
      return customerId + "|" + email;
    }

    @JsonCreator
    static JsonValueEvolvedCustomer parse(String wire) {
      int i = wire.indexOf('|');
      return new JsonValueEvolvedCustomer(wire.substring(i + 1), wire.substring(0, i));
    }
  }

  @Test
  void jsonValueStoredPlaintextStaysReadableForReplay() throws Exception {
    // Deliberate asymmetry (mirrors the null-subjectId read tolerance): the guard fails
    // the WRITE side only. Events of a type that gained @Encrypted through user schema evolution
    // are plaintext scalars in the append-only store; refusing to read them would turn the schema
    // change into a replay DoS. Reading stays possible (via the type's own delegating creator) so
    // replay keeps working while any RE-serialization (snapshot, DLQ copy) fails loudly until the
    // type is fixed.
    var restored = mapper.readValue("\"c1|pii@example.com\"", JsonValueEvolvedCustomer.class);

    assertEquals("pii@example.com", restored.email());
    assertEquals("c1", restored.customerId());
  }

  @Test
  void perSubjectForget_emitsRedactionMetricButNotSystemicAlarm() throws Exception {
    // A lawful per-subject GDPR forget must be OBSERVABLE (the redact fallback bumps the
    // subject_redacted metric — it is no longer silent) but must NOT trip the systemic-key-store
    // alarm, because every OTHER subject still decrypts successfully (which resets the detector).
    //
    // Fail-first: before redaction was instrumented the redact path emitted no metric at all, so
    // `redacted` would
    // stay 0 — the redaction was invisible/indistinguishable from a keystore outage.
    var metrics = new CountingMetrics();
    var engine = new InMemoryCryptoEngine();
    var m = new ObjectMapper();
    m.registerModule(new CryptoShreddingModule(engine, metrics));

    var forgotten = new UserRegistered("subject-forgotten", "gone@example.com", "en_US");
    String forgottenJson = m.writeValueAsString(forgotten);
    var live = new UserRegistered("subject-live", "here@example.com", "en_US");
    String liveJson = m.writeValueAsString(live);

    engine.deleteKey(SubjectId.of("subject-forgotten"));

    assertEquals(
        CryptoShreddingModule.REDACTED,
        m.readValue(forgottenJson, UserRegistered.class).email(),
        "the forgotten subject's field is redacted");
    assertEquals(
        "here@example.com",
        m.readValue(liveJson, UserRegistered.class).email(),
        "a live subject still decrypts");

    assertTrue(
        metrics.redacted.get() >= 1,
        "the redact fallback must be observable via a metric, not silent");
    assertEquals(
        0, metrics.systemic.get(), "a single lawful forget must NOT trip the systemic alarm");
  }

  @Test
  void systemicKeyStoreWipe_tripsSystemicAlarmExactlyOnce_distinctFromPerSubjectForget()
      throws Exception {
    // A wiped/misconfigured key store fails EVERY decrypt with KeyNotFound, so every
    // @Encrypted field redacts to [REDACTED] with no successful decrypt in between — the signature
    // of a systemic outage. That must be clearly separated from a lawful forget: the per-subject
    // metric fires on every redaction AND the crisp systemic alarm fires exactly once per episode.
    var metrics = new CountingMetrics();
    // Encrypt works (so we can produce real ciphertext to read back); decrypt always fails as if
    // the
    // key rows were restored-away / truncated.
    CryptoEngine wipedOnRead =
        new CryptoEngine() {
          private final InMemoryCryptoEngine backing = new InMemoryCryptoEngine();

          @Override
          public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
            return backing.encrypt(subjectId, plaintext);
          }

          @Override
          public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
            throw new KeyNotFoundException("key store wiped: " + subjectId);
          }

          @Override
          public void deleteKey(SubjectId subjectId) {}

          @Override
          public boolean isKeyAvailable(SubjectId subjectId) {
            return false;
          }
        };
    int threshold = 3;
    var m = new ObjectMapper();
    m.registerModule(new CryptoShreddingModule(wipedOnRead, metrics, threshold));

    int distinctSubjects = threshold + 2; // 5 distinct subjects, all redacting
    for (int i = 0; i < distinctSubjects; i++) {
      var event = new UserRegistered("subj-" + i, "pii-" + i + "@example.com", "en_US");
      String json = m.writeValueAsString(event); // encrypt succeeds
      var restored = m.readValue(json, UserRegistered.class); // decrypt fails -> redact
      assertEquals(CryptoShreddingModule.REDACTED, restored.email());
    }

    assertEquals(
        distinctSubjects, metrics.redacted.get(), "every redaction is metered (per-field)");
    assertEquals(
        1,
        metrics.systemic.get(),
        "the systemic-key-store alarm fires exactly once per outage episode, not per field");
  }

  @Test
  void systemicAlarmReArmsAfterRecovery_evenWhenTheRunSetWasBoundCleared() throws Exception {
    // "once per outage EPISODE" means the alarm must RE-ARM when the key store comes
    // back. The bound-clear branch (distinct > threshold*2) cleared the run set but left the
    // systemicAlarmRaised latch TRUE, and onDecryptSuccess only reset the latch when the run set
    // was non-empty — so after a large enough wipe the latch could never be reset again. A SECOND
    // wipe weeks later then emitted NO streamrune.crypto.keystore_systemic_failure and NO ERROR
    // (MetricNames documents it as "page on any nonzero value"), and every per-subject WARN stayed
    // suppressed for the rest of the process lifetime.
    var metrics = new CountingMetrics();
    var engine = new RecoverableWipedEngine();
    int threshold = 3;
    var m = new ObjectMapper();
    m.registerModule(new CryptoShreddingModule(engine, metrics, threshold));

    // Ciphertext for every subject is produced up front: encrypt always works, only reads fail.
    int beyondBound = threshold * 2 + 1; // 7 — crosses the `distinct > threshold * 2` clear branch
    var episodeOne = new String[beyondBound];
    for (int i = 0; i < beyondBound; i++) {
      episodeOne[i] =
          m.writeValueAsString(
              new UserRegistered("ep1-subj-" + i, "pii-" + i + "@x.test", "en_US"));
    }
    String healthy = m.writeValueAsString(new UserRegistered("healthy", "ok@x.test", "en_US"));
    var episodeTwo = new String[threshold];
    for (int i = 0; i < threshold; i++) {
      episodeTwo[i] =
          m.writeValueAsString(new UserRegistered("ep2-subj-" + i, "pii2-" + i + "@x.test", "en"));
    }

    // Episode 1: a big wipe that walks past the bound-clear branch.
    engine.keyStoreWiped = true;
    for (String json : episodeOne) {
      assertEquals(CryptoShreddingModule.REDACTED, m.readValue(json, UserRegistered.class).email());
    }
    assertEquals(1, metrics.systemic.get(), "episode 1 raises the alarm exactly once");

    // Operator restores the key rows; the very next decrypt proves the store is serving keys.
    engine.keyStoreWiped = false;
    assertEquals(
        "ok@x.test",
        m.readValue(healthy, UserRegistered.class).email(),
        "a successful decrypt proves the key store is reachable again");

    // Episode 2: the key store is wiped again.
    engine.keyStoreWiped = true;
    for (String json : episodeTwo) {
      assertEquals(CryptoShreddingModule.REDACTED, m.readValue(json, UserRegistered.class).email());
    }

    assertEquals(
        2,
        metrics.systemic.get(),
        "the alarm must re-arm per outage episode — a second key-store wipe has to page again");
  }

  /** A key store that can be wiped and restored, so a recovery episode is expressible. */
  static final class RecoverableWipedEngine implements CryptoEngine {
    private final InMemoryCryptoEngine backing = new InMemoryCryptoEngine();
    volatile boolean keyStoreWiped = true;

    @Override
    public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
      return backing.encrypt(subjectId, plaintext);
    }

    @Override
    public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
      if (keyStoreWiped) {
        throw new KeyNotFoundException("key store wiped: " + subjectId);
      }
      return backing.decrypt(subjectId, ciphertext);
    }

    @Override
    public void deleteKey(SubjectId subjectId) {}

    @Override
    public boolean isKeyAvailable(SubjectId subjectId) {
      return !keyStoreWiped;
    }
  }

  // ---- By-name property filtering applied AFTER the construction-time checks
  // (@JsonIgnoreProperties/@JsonIncludeProperties on a REFERENCING property, applied during
  // per-property contextualization; or class-level, applied in filterBeanProperties after
  // changeProperties) must never drop the subjectId writer — the ciphertext would be stored
  // without its key reference: permanently undecryptable — nor the encrypting writer itself
  // (silent loss of the PII field from the system of record) ----

  record ReferencedCustomer(@Encrypted(subjectId = "customerId") String email, String customerId) {}

  record OrderWithIgnoredSubjectRef(
      String customerId, @JsonIgnoreProperties("customerId") ReferencedCustomer customer) {}

  @Test
  void contextualIgnoralDroppingSubjectIdOnReferencingPropertyFailsClosed() {
    // A natural "don't duplicate the id on the wire"
    // slimming annotation on the CONTAINER property. The record's serializer passes every
    // construction-time check (the subjectId IS among the constructed properties); only when that
    // serializer is contextualized for THIS property does BeanSerializerBase.createContextual
    // apply the referencing member's ignorals (withByNameInclusion) and silently drop the
    // customerId writer while keeping the encrypting email writer. Pre-fix wire form:
    // {"customer":{"email":"<base64>"}} — ciphertext without its subjectId, so no later read can
    // ever derive the key (the null-subjectId read tolerance then hands the Base64 back as the
    // value). Must fail closed at contextualization instead.
    var order =
        new OrderWithIgnoredSubjectRef("c1", new ReferencedCustomer("pii@example.com", "c1"));

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(order));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("ReferencedCustomer"), "message should name the record: " + msg);
    assertTrue(msg.contains("customerId"), "message should name the filtered subjectId: " + msg);
    assertTrue(msg.contains("undecryptable"), "message should state the consequence: " + msg);
    assertTrue(
        msg.contains("@JsonIgnoreProperties") || msg.contains("@JsonIncludeProperties"),
        "message should name the offending mechanism: " + msg);
  }

  record OrderWithInclusionOmittingSubject(
      String customerId,
      @com.fasterxml.jackson.annotation.JsonIncludeProperties({"email"})
          ReferencedCustomer customer) {}

  @Test
  void contextualInclusionOmittingSubjectIdOnReferencingPropertyFailsClosed() {
    // The @JsonIncludeProperties dual: an inclusion list that omits the subjectId filters the
    // writer exactly like an ignoral naming it — same undecryptable-ciphertext corruption.
    var order =
        new OrderWithInclusionOmittingSubject(
            "c2", new ReferencedCustomer("pii@example.com", "c2"));

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(order));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("customerId"), "message should name the omitted subjectId: " + msg);
  }

  record OrderWithIgnoredEncryptedRef(
      String orderId, @JsonIgnoreProperties("email") ReferencedCustomer customer) {}

  @Test
  void contextualIgnoralDroppingTheEncryptedFieldItselfFailsClosed() {
    // Filtering the ENCRYPTED field itself writes no ciphertext at all: not undecryptable
    // garbage, but the PII silently VANISHES from the persisted event — for the event-store
    // mapper that is irreversible data loss on the system of record, discovered only when some
    // later read needs the value. Plain-Jackson DTO slimming intuition does not carry over to an
    // append-only store; the construction-time guards already refuse an @JsonIgnore'd @Encrypted
    // component (data loss + plaintext risk), so the contextual route must refuse identically:
    // loud beats silent data loss.
    var order = new OrderWithIgnoredEncryptedRef("o1", new ReferencedCustomer("pii@x.com", "c3"));

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(order));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("email"), "message should name the filtered @Encrypted field: " + msg);
  }

  record OrdersWithIgnoredSubjectListRef(
      String orderId,
      @JsonIgnoreProperties("customerId") java.util.List<ReferencedCustomer> customers) {}

  record OrdersWithIgnoredSubjectMapRef(
      String orderId,
      @JsonIgnoreProperties("customerId") java.util.Map<String, ReferencedCustomer> customers) {}

  record OrderWithIgnoredSubjectObjectRef(
      String orderId, @JsonIgnoreProperties("customerId") Object customer) {}

  @Test
  void contextualIgnoralPropagatesIntoContainerContentAndFailsClosed() {
    // The referencing member's ignorals flow into the CONTENT serializer of container-typed
    // properties (probe-verified on 2.19.2: List elements, Map VALUES — not just entries — and
    // even a declared-Object member resolved dynamically at write time all lose the subjectId
    // writer). The guard sits on the record's own serializer, so it fires wherever that
    // serializer ends up contextualized — including the declared-Object case, which no
    // member-declared-type inspection could ever see.
    var listOrder =
        new OrdersWithIgnoredSubjectListRef(
            "o1", java.util.List.of(new ReferencedCustomer("pii@x.com", "c4")));
    Exception listThrown =
        assertThrows(Exception.class, () -> mapper.writeValueAsString(listOrder));
    assertNotNull(
        findCryptoCause(listThrown),
        "List<ReferencedCustomer> content must be guarded, got: " + listThrown);

    var mapOrder =
        new OrdersWithIgnoredSubjectMapRef(
            "o2", java.util.Map.of("k", new ReferencedCustomer("pii@x.com", "c5")));
    Exception mapThrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(mapOrder));
    assertNotNull(
        findCryptoCause(mapThrown),
        "Map values of ReferencedCustomer must be guarded, got: " + mapThrown);

    var objOrder =
        new OrderWithIgnoredSubjectObjectRef("o3", new ReferencedCustomer("pii@x.com", "c6"));
    Exception objThrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(objOrder));
    assertNotNull(
        findCryptoCause(objThrown),
        "declared-Object member with runtime ReferencedCustomer must be guarded, got: "
            + objThrown);
  }

  record SnakeCustomer(
      @Encrypted(subjectId = "customerIdent") String emailAddr, String customerIdent) {}

  record SnakeOrder(
      String orderId, @JsonIgnoreProperties("customer_ident") SnakeCustomer customer) {}

  @Test
  void contextualIgnoralMatchesExternalNamesUnderNamingStrategy() {
    // Ignoral sets carry EXTERNAL (wire) names. Under SNAKE_CASE the subjectId component
    // customerIdent serializes as customer_ident, and @JsonIgnoreProperties("customer_ident")
    // drops it (probe-verified). The guard must therefore match on the writers' external names —
    // any component-name matching would silently miss this and let the corruption through.
    var snakeMapper = new ObjectMapper();
    snakeMapper.registerModule(new CryptoShreddingModule(cryptoEngine));
    snakeMapper.setPropertyNamingStrategy(
        com.fasterxml.jackson.databind.PropertyNamingStrategies.SNAKE_CASE);
    var order = new SnakeOrder("o1", new SnakeCustomer("pii@x.com", "c7"));

    Exception thrown = assertThrows(Exception.class, () -> snakeMapper.writeValueAsString(order));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    assertTrue(
        cause.getMessage().contains("customer_ident"),
        "message should name the external subjectId name: " + cause.getMessage());
  }

  record MixinOrder(String customerId, ReferencedCustomer customer) {}

  abstract static class MixinOrderIgnoral {
    @JsonIgnoreProperties("customerId")
    abstract ReferencedCustomer customer();
  }

  @Test
  void mixinDeclaredContextualIgnoralFailsClosed() {
    // Mixin-declared ignorals merge into the referencing member's annotation view and drop the
    // writer exactly like a directly-annotated property (probe-verified) — the guard must not
    // depend on where the annotation is declared.
    var mixinMapper = new ObjectMapper();
    mixinMapper.registerModule(new CryptoShreddingModule(cryptoEngine));
    mixinMapper.addMixIn(MixinOrder.class, MixinOrderIgnoral.class);
    var order = new MixinOrder("c8", new ReferencedCustomer("pii@x.com", "c8"));

    Exception thrown = assertThrows(Exception.class, () -> mixinMapper.writeValueAsString(order));

    assertNotNull(
        findCryptoCause(thrown),
        "mixin-declared ignoral of the subjectId must be guarded, got: " + thrown);
  }

  record UnwrappedIgnoredSubjectOrder(
      String orderId,
      @JsonUnwrapped(prefix = "cust_") @JsonIgnoreProperties("customerId")
          ReferencedCustomer customer) {}

  @Test
  void unwrappedMemberWithContextualIgnoralOfSubjectIdFailsClosed() {
    // @JsonUnwrapped + @JsonIgnoreProperties on the same member: the ignorals are applied to the
    // record's serializer when it is contextualized for the member, BEFORE the unwrapping variant
    // is derived — pre-fix the wire was {"orderId":..., "cust_email":"<base64>"} with
    // cust_customerId missing (probe-verified). Same guard, same fail-closed point.
    var order = new UnwrappedIgnoredSubjectOrder("o1", new ReferencedCustomer("pii@x.com", "c9"));

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(order));

    assertNotNull(
        findCryptoCause(thrown),
        "unwrapped member's ignoral of the subjectId must be guarded, got: " + thrown);
  }

  record UnwrappedPrefixIgnoralOrder(
      String orderId,
      @JsonUnwrapped(prefix = "cust_") @JsonIgnoreProperties("cust_customerId")
          ReferencedCustomer customer) {}

  @Test
  void unwrappedMemberIgnoralNamingPrefixedNameStaysSilentAndWritesBothFields() throws Exception {
    // No-false-positive pin: contextual ignorals are matched against the record's PRE-transform
    // external names (Jackson applies them before the unwrapping rename derivation), so an
    // ignoral naming the PREFIXED wire name matches nothing, drops nothing, and must not trip
    // the guard (probe-verified: both cust_ fields serialize).
    var order = new UnwrappedPrefixIgnoralOrder("o1", new ReferencedCustomer("pii@x.com", "c10"));

    String json = mapper.writeValueAsString(order);

    var tree = mapper.readTree(json);
    assertFalse(json.contains("pii@x.com"), "PII must be ciphertext at rest: " + json);
    assertTrue(tree.has("cust_email"), "encrypted field serializes under prefix: " + json);
    assertEquals("c10", tree.get("cust_customerId").asText(), "subjectId survives: " + json);
  }

  @JsonIgnoreProperties("customerId")
  record SelfIgnoredSubjectCustomer(
      @Encrypted(subjectId = "customerId") String email, String customerId) {}

  @Test
  void classLevelSelfIgnoralOfSubjectIdFailsClosed() {
    // Edge probed while hardening the subjectId guard, and NOT covered by the construction
    // check as assumed: class-level @JsonIgnoreProperties on the record ITSELF is applied by
    // BeanSerializerFactory.filterBeanProperties AFTER changeProperties ran, so the check
    // saw the subjectId writer and passed — pre-fix this wrote {"email":"<base64>"} silently
    // (probe-verified). The post-build writer-set check must refuse it.
    var customer = new SelfIgnoredSubjectCustomer("pii@x.com", "c11");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(customer));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(
        msg.contains("SelfIgnoredSubjectCustomer"), "message should name the record: " + msg);
    assertTrue(msg.contains("customerId"), "message should name the missing subjectId: " + msg);
  }

  @JsonIgnoreProperties("email")
  record SelfIgnoredEncryptedCustomer(
      @Encrypted(subjectId = "customerId") String email, String customerId) {}

  @Test
  void classLevelSelfIgnoralOfEncryptedFieldFailsClosed() {
    // Same post-changeProperties route, dropping the ENCRYPTED writer: pre-fix the PII field
    // silently vanished from the wire ({"customerId":"c1"}, probe-verified) — silent data loss on
    // the system of record. Refuse like the construction-time wrap check would have.
    var customer = new SelfIgnoredEncryptedCustomer("pii@x.com", "c12");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(customer));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    assertTrue(
        cause.getMessage().contains("email"),
        "message should name the missing @Encrypted field: " + cause.getMessage());
  }

  @com.fasterxml.jackson.annotation.JsonIncludeProperties({"email"})
  record SelfInclusionOmitsSubjectCustomer(
      @Encrypted(subjectId = "customerId") String email, String customerId) {}

  @Test
  void classLevelSelfInclusionOmittingSubjectIdFailsClosed() {
    // Inclusion dual of the class-level route (filterBeanProperties honors
    // @JsonIncludeProperties the same way).
    var customer = new SelfInclusionOmitsSubjectCustomer("pii@x.com", "c13");

    Exception thrown = assertThrows(Exception.class, () -> mapper.writeValueAsString(customer));

    assertNotNull(
        findCryptoCause(thrown),
        "class-level inclusion omitting the subjectId must be guarded, got: " + thrown);
  }

  record CustomerWithTier(
      @Encrypted(subjectId = "customerId") String email, String customerId, String tier) {}

  record OrderWithUnrelatedIgnoral(
      String orderId, @JsonIgnoreProperties("tier") CustomerWithTier customer) {}

  @Test
  void contextualIgnoralOfUnrelatedPropertyStaysSilentAndRoundTrips() throws Exception {
    // No-false-positive pin: an ignoral naming neither the encrypting writer nor its subjectId
    // filters only what it names — the guard must stay silent, the ciphertext + subjectId must
    // both serialize, and the read side must decrypt.
    var order =
        new OrderWithUnrelatedIgnoral("o1", new CustomerWithTier("pii@x.com", "c14", "gold"));

    String json = mapper.writeValueAsString(order);

    var tree = mapper.readTree(json);
    assertFalse(json.contains("pii@x.com"), "PII must be ciphertext at rest: " + json);
    assertFalse(tree.get("customer").has("tier"), "the named property IS filtered: " + json);
    assertEquals("c14", tree.get("customer").get("customerId").asText(), "subjectId survives");

    var restored = mapper.readValue(json, OrderWithUnrelatedIgnoral.class);
    assertEquals("pii@x.com", restored.customer().email(), "round-trip decrypts");
  }

  @Test
  void subjectIdLessNestedDataStaysReadableForReplay() throws Exception {
    // Deliberate asymmetry (mirrors the stance): stored data whose subjectId property
    // the current type cannot find (the component was renamed or removed through schema evolution,
    // or an upcaster dropped it) carries ciphertext no read can associate with a key, so reads keep
    // passing the Base64 through via the null-subjectId read tolerance instead of turning replay
    // into a DoS. The guard is serialize-side only.
    String cipher =
        java.util.Base64.getEncoder()
            .encodeToString(
                cryptoEngine.encrypt(
                    SubjectId.of("c15"),
                    "pii@x.com".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
    String stored = "{\"customerId\":\"c15\",\"customer\":{\"email\":\"" + cipher + "\"}}";

    var restored = mapper.readValue(stored, OrderWithIgnoredSubjectRef.class);

    assertEquals(
        cipher,
        restored.customer().email(),
        "subjectId-less ciphertext passes through as-is on read");
  }

  @Test
  void contextualIgnoralRouteConsultsIntrospectorBeforeGuardFires() {
    // Hook-map evidence (spy methodology): the contextual route consults
    // AnnotationIntrospector.findPropertyIgnoralByName with the REFERENCING member on the
    // serialization config (BeanSerializerBase.createContextual), and only then applies the
    // filtering the guard intercepts at withByNameInclusion — proving the guard sits on the
    // exact route verified in bytecode, one call later, where the effective
    // EXTERNAL names are authoritative.
    var calls = new java.util.ArrayList<String>();
    var spy =
        new com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector() {
          @Override
          public JsonIgnoreProperties.Value findPropertyIgnoralByName(
              com.fasterxml.jackson.databind.cfg.MapperConfig<?> config,
              com.fasterxml.jackson.databind.introspect.Annotated a) {
            if (a instanceof com.fasterxml.jackson.databind.introspect.AnnotatedMember m
                && m.hasAnnotation(JsonIgnoreProperties.class)) {
              calls.add(
                  (config instanceof com.fasterxml.jackson.databind.SerializationConfig
                          ? "SER "
                          : "DESER ")
                      + m.getDeclaringClass().getSimpleName()
                      + "."
                      + m.getName());
            }
            return super.findPropertyIgnoralByName(config, a);
          }
        };
    var spyMapper = new ObjectMapper();
    spyMapper.registerModule(new CryptoShreddingModule(cryptoEngine));
    spyMapper.registerModule(
        new com.fasterxml.jackson.databind.module.SimpleModule("introspector-spy") {
          @Override
          public void setupModule(SetupContext context) {
            super.setupModule(context);
            context.insertAnnotationIntrospector(spy);
          }
        });
    var order = new OrderWithIgnoredSubjectRef("c16", new ReferencedCustomer("pii@x.com", "c16"));

    Exception thrown = assertThrows(Exception.class, () -> spyMapper.writeValueAsString(order));

    assertNotNull(findCryptoCause(thrown), "the guard must fire on this route: " + thrown);
    assertTrue(
        calls.stream().anyMatch(c -> c.startsWith("SER OrderWithIgnoredSubjectRef.")),
        "findPropertyIgnoralByName must have been consulted with the referencing member on the"
            + " serialization config before the guard fired, saw: "
            + calls);
  }

  // ---- Class-level @JsonDeserialize(using/converter/builder) replaces the bean
  // deserializer BEFORE DecryptingDeserializerModifier can wrap it — every read (event replay,
  // aggregate load, projection rebuild, DLQ replay) would construct records holding raw Base64
  // ciphertext, silently: the deserialize-side sibling of the guarded @JsonSerialize routes ----

  static final class LenientCustomerDeserializer
      extends com.fasterxml.jackson.databind.JsonDeserializer<AnnotationReadCustomer> {
    @Override
    public AnnotationReadCustomer deserialize(
        com.fasterxml.jackson.core.JsonParser p,
        com.fasterxml.jackson.databind.DeserializationContext ctxt)
        throws java.io.IOException {
      com.fasterxml.jackson.databind.JsonNode n = p.readValueAsTree();
      return new AnnotationReadCustomer(
          n.path("email").asText(null), n.path("customerId").asText(null));
    }
  }

  @com.fasterxml.jackson.databind.annotation.JsonDeserialize(
      using = LenientCustomerDeserializer.class)
  record AnnotationReadCustomer(
      @Encrypted(subjectId = "customerId") String email, String customerId) {}

  @Test
  void classLevelJsonDeserializeUsingOnEncryptedRecordFailsClosedOnRead() throws Exception {
    // A class-level @JsonDeserialize(using = ...) — e.g.
    // a legacy tolerant-parsing deserializer, a common migration aid — is returned directly by
    // DeserializerCache._createDeserializer BEFORE the bean-factory path where
    // DecryptingDeserializerModifier hooks. Pre-fix the WRITE was correct (ciphertext + subjectId
    // at rest) but EVERY read handed the raw JSON to the user deserializer, which constructed the
    // record with email = "<base64>": ciphertext flowing into aggregates/read models/APIs, GDPR
    // forgets never showing [REDACTED] for the type, and each snapshot re-serialization
    // re-encrypting the ciphertext one nesting level deeper. Reads must fail closed at
    // deserializer construction instead.
    var event = new AnnotationReadCustomer("pii@example.com", "c20");
    String json = mapper.writeValueAsString(event);

    // The write side is NOT the defect: ciphertext + subjectId land correctly, so data written
    // while the annotation was present is fully recoverable once the override is removed.
    var tree = mapper.readTree(json);
    assertFalse(json.contains("pii@example.com"), "PII must be ciphertext at rest: " + json);
    assertEquals("c20", tree.get("customerId").asText(), "subjectId is persisted");

    Exception thrown =
        assertThrows(Exception.class, () -> mapper.readValue(json, AnnotationReadCustomer.class));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    String msg = cause.getMessage();
    assertTrue(msg.contains("AnnotationReadCustomer"), "message should name the record: " + msg);
    assertTrue(msg.contains("@JsonDeserialize"), "message should name the mechanism: " + msg);
    assertTrue(msg.contains("using"), "message should name the offending attribute: " + msg);
    assertTrue(msg.contains("ciphertext"), "message should state the consequence: " + msg);
  }

  static final class MapToCustomerConverter
      extends com.fasterxml.jackson.databind.util.StdConverter<
          java.util.Map<String, String>, ConverterReadCustomer> {
    @Override
    public ConverterReadCustomer convert(java.util.Map<String, String> v) {
      return new ConverterReadCustomer(v.get("email"), v.get("customerId"));
    }
  }

  @com.fasterxml.jackson.databind.annotation.JsonDeserialize(
      converter = MapToCustomerConverter.class)
  record ConverterReadCustomer(
      @Encrypted(subjectId = "customerId") String email, String customerId) {}

  @Test
  void classLevelJsonDeserializeConverterOnEncryptedRecordFailsClosedOnRead() throws Exception {
    // Same family: @JsonDeserialize(converter = ...) builds a StdDelegatingDeserializer for the
    // converter's INPUT type, so the record's own decrypting deserializer is never constructed —
    // the converter receives raw ciphertext values and bakes them into the record (probe-verified
    // on 2.19.2).
    var event = new ConverterReadCustomer("pii@example.com", "c21");
    String json = mapper.writeValueAsString(event);
    assertFalse(json.contains("pii@example.com"), "PII must be ciphertext at rest: " + json);

    Exception thrown =
        assertThrows(Exception.class, () -> mapper.readValue(json, ConverterReadCustomer.class));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    assertTrue(
        cause.getMessage().contains("converter"),
        "message should name the offending attribute: " + cause.getMessage());
  }

  @com.fasterxml.jackson.databind.annotation.JsonDeserialize(
      builder = BuilderReadCustomer.Builder.class)
  record BuilderReadCustomer(@Encrypted(subjectId = "customerId") String email, String customerId) {

    @com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder(withPrefix = "with")
    static final class Builder {
      private String email;
      private String customerId;

      Builder withEmail(String v) {
        this.email = v;
        return this;
      }

      Builder withCustomerId(String v) {
        this.customerId = v;
        return this;
      }

      BuilderReadCustomer build() {
        return new BuilderReadCustomer(email, customerId);
      }
    }
  }

  @Test
  void classLevelJsonDeserializeBuilderOnEncryptedRecordFailsClosedOnRead() throws Exception {
    // Same family: @JsonDeserialize(builder = ...) routes through createBuilderBasedDeserializer,
    // whose modifier hook receives the BUILDER's BeanDescription — no @Encrypted components — so
    // the decrypting wrapper is installed nowhere and the builder receives raw ciphertext
    // (probe-verified: read returned email = "<base64>").
    var event = new BuilderReadCustomer("pii@example.com", "c22");
    String json = mapper.writeValueAsString(event);
    assertFalse(json.contains("pii@example.com"), "PII must be ciphertext at rest: " + json);

    Exception thrown =
        assertThrows(Exception.class, () -> mapper.readValue(json, BuilderReadCustomer.class));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a CryptoOperationException in the cause chain, got: " + thrown);
    assertTrue(
        cause.getMessage().contains("builder"),
        "message should name the offending attribute: " + cause.getMessage());
  }

  static final class TrimmingStringDeserializer
      extends com.fasterxml.jackson.databind.JsonDeserializer<String> {
    @Override
    public String deserialize(
        com.fasterxml.jackson.core.JsonParser p,
        com.fasterxml.jackson.databind.DeserializationContext ctxt)
        throws java.io.IOException {
      String v = p.getValueAsString();
      return v == null ? null : v.trim();
    }
  }

  record CustomerWithPropertyLevelDeser(
      @Encrypted(subjectId = "customerId") String email,
      String customerId,
      @com.fasterxml.jackson.databind.annotation.JsonDeserialize(
              using = TrimmingStringDeserializer.class)
          String note) {}

  @Test
  void propertyLevelJsonDeserializeOnNonEncryptedComponentStaysSupported() throws Exception {
    // No-false-positive pin: a PROPERTY-level @JsonDeserialize(using = ...) on a component OF the
    // record reaches the introspector with an AnnotatedMember, not an AnnotatedClass — the guard
    // must stay silent, the record's own decrypting deserializer still wraps the bean
    // deserializer, and both the custom property deserializer and decryption work.
    var event = new CustomerWithPropertyLevelDeser("pii@example.com", "c23", "  padded  ");
    String json = mapper.writeValueAsString(event);

    var restored = mapper.readValue(json, CustomerWithPropertyLevelDeser.class);

    assertEquals("pii@example.com", restored.email(), "decryption still runs");
    assertEquals("padded", restored.note(), "the property-level deserializer still runs");
  }

  static final class PassthroughReferencedCustomerDeserializer
      extends com.fasterxml.jackson.databind.JsonDeserializer<ReferencedCustomer> {
    @Override
    public ReferencedCustomer deserialize(
        com.fasterxml.jackson.core.JsonParser p,
        com.fasterxml.jackson.databind.DeserializationContext ctxt)
        throws java.io.IOException {
      com.fasterxml.jackson.databind.JsonNode n = p.readValueAsTree();
      return new ReferencedCustomer(
          n.path("email").asText(null), n.path("customerId").asText(null));
    }
  }

  record OrderWithPropertyUsingRef(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonDeserialize(
              using = PassthroughReferencedCustomerDeserializer.class)
          ReferencedCustomer customer) {}

  @Test
  void propertyLevelJsonDeserializeUsingOnAnotherTypesFieldRemainsUnguardedResidual()
      throws Exception {
    // DOCUMENTED RESIDUAL (deliberately unguarded — the read-side twin of the
    // property-level write residual): @JsonDeserialize(using = ...) on ANOTHER type's field of the
    // record type replaces the record's deserializer for that one property, so the custom
    // deserializer receives raw ciphertext. The route IS observable — the introspector's
    // findDeserializer(AnnotatedMember) fires for the member (spy-verified here) — but guarding
    // members would break the legitimate pattern of a per-property deserializer that pre-processes
    // the JSON and then DELEGATES to ctxt.readValue(...), which decrypts correctly; a false
    // positive there is a replay DoS on a working type. Scope stays AnnotatedClass, the blind spot
    // is documented in the gdpr-erasure guide, and this test pins both the residual behavior and
    // its observability so any drift is caught.
    var memberCalls = new java.util.ArrayList<String>();
    var spy =
        new com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector() {
          @Override
          public Object findDeserializer(com.fasterxml.jackson.databind.introspect.Annotated a) {
            if (a instanceof com.fasterxml.jackson.databind.introspect.AnnotatedMember m
                && m.hasAnnotation(
                    com.fasterxml.jackson.databind.annotation.JsonDeserialize.class)) {
              memberCalls.add(m.getDeclaringClass().getSimpleName());
            }
            return null;
          }
        };
    var spyMapper = new ObjectMapper();
    spyMapper.registerModule(new CryptoShreddingModule(cryptoEngine));
    spyMapper.registerModule(
        new com.fasterxml.jackson.databind.module.SimpleModule("residual-spy") {
          @Override
          public void setupModule(SetupContext context) {
            super.setupModule(context);
            context.insertAnnotationIntrospector(spy);
          }
        });
    var order = new OrderWithPropertyUsingRef("o1", new ReferencedCustomer("pii@x.com", "c24"));
    String json = spyMapper.writeValueAsString(order);
    assertFalse(json.contains("pii@x.com"), "write side is correct: " + json);

    var restored = spyMapper.readValue(json, OrderWithPropertyUsingRef.class);

    assertNotEquals(
        "pii@x.com",
        restored.customer().email(),
        "residual: the property-level user deserializer receives raw ciphertext");
    assertTrue(
        memberCalls.contains("OrderWithPropertyUsingRef"),
        "the member route IS observable at findDeserializer(AnnotatedMember) — documented as"
            + " deliberately unguarded, saw: "
            + memberCalls);
  }

  // ---- Omission routes decided per SERIALIZATION rather than per contextualization
  // — an active @JsonView (which reads a SEPARATE _filteredProps array that the construction-time
  // check over serializer.properties() never sees) and a @JsonFilter (whose per-property decision
  // produces no callback at all for an excluded property in JSON). Both reproduce the same
  // harm exactly: drop the subjectId and the ciphertext is stored with no key reference
  // (permanently undecryptable, read back silently as raw Base64); drop the encrypted field and the
  // PII vanishes from the append-only store. Neither can be decided at construction time — the
  // "still serializes" pins below are configurations that must keep working. ----

  static final class PublicView {}

  static final class InternalView {}

  record ViewedCustomer(
      @com.fasterxml.jackson.annotation.JsonView(InternalView.class) String customerId,
      @Encrypted(subjectId = "customerId") String email) {}

  record PublicViewedCustomer(
      @com.fasterxml.jackson.annotation.JsonView(PublicView.class) String customerId,
      @Encrypted(subjectId = "customerId") String email) {}

  record OrderWithUnwrappedViewedCustomer(
      String orderId, @JsonUnwrapped(prefix = "cust_") ViewedCustomer customer) {}

  @Test
  void activeJsonViewOmittingTheSubjectIdFailsClosed() {
    // The subjectId carries @JsonView(InternalView) and the writer activates PublicView, so
    // BeanSerializerBase.serializeFields switches to _filteredProps and emits only the ciphertext:
    // {"email":"<Base64>"} — permanently undecryptable, and read back (verified pre-fix) as
    // ViewedCustomer[customerId=null, email=<raw Base64>] with no error at all.
    Exception thrown =
        assertThrows(
            Exception.class,
            () ->
                mapper
                    .writerWithView(PublicView.class)
                    .writeValueAsString(new ViewedCustomer("c40", "pii@example.com")));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a crypto failure, got: " + thrown);
    assertTrue(cause.getMessage().contains("@JsonView"), cause.getMessage());
    assertTrue(cause.getMessage().contains("customerId"), cause.getMessage());
    assertTrue(cause.getMessage().contains("permanently"), cause.getMessage());
  }

  @Test
  void activeJsonViewOmittingTheEncryptedFieldFailsClosed() throws Exception {
    // The second harm direction, reached through MapperFeature.DEFAULT_VIEW_INCLUSION being
    // disabled (Spring Boot's own default): the @Encrypted component carries no @JsonView, so
    // processViews leaves its _filteredProps slot null and the PII silently vanished from the
    // stored event ({"customerId":"c41"}).
    var strictViews =
        com.fasterxml.jackson.databind.json.JsonMapper.builder()
            .disable(com.fasterxml.jackson.databind.MapperFeature.DEFAULT_VIEW_INCLUSION)
            .addModule(new CryptoShreddingModule(cryptoEngine))
            .build();

    Exception thrown =
        assertThrows(
            Exception.class,
            () ->
                strictViews
                    .writerWithView(PublicView.class)
                    .writeValueAsString(new PublicViewedCustomer("c41", "pii@example.com")));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a crypto failure, got: " + thrown);
    assertTrue(cause.getMessage().contains("'email' (@Encrypted"), cause.getMessage());

    // No false positive: the SAME mapper and the SAME record write correctly with no active view.
    String json =
        strictViews.writeValueAsString(new PublicViewedCustomer("c41", "pii@example.com"));
    assertFalse(json.contains("pii@example.com"), json);
    assertTrue(json.contains("c41"), json);
  }

  @Test
  void jsonViewThatKeepsBothProtectedWritersStillSerializes() throws Exception {
    // No false positive, and the reason the check cannot run at construction time: this is the
    // identical annotation set as the failing case above, distinguished only by the ACTIVE view.
    String json =
        mapper
            .writerWithView(InternalView.class)
            .writeValueAsString(new ViewedCustomer("c42", "pii@example.com"));

    assertFalse(json.contains("pii@example.com"), json);
    assertTrue(json.contains("c42"), "the subjectId must survive: " + json);
    assertEquals("pii@example.com", mapper.readValue(json, ViewedCustomer.class).email());
  }

  @Test
  void jsonViewDeclaredButNeverActivatedStillSerializes() throws Exception {
    // A view-annotated record written through a mapper that sets no active view never reaches
    // _filteredProps at all — the guard must stay silent.
    String json = mapper.writeValueAsString(new ViewedCustomer("c43", "pii@example.com"));

    assertFalse(json.contains("pii@example.com"), json);
    assertEquals("pii@example.com", mapper.readValue(json, ViewedCustomer.class).email());
  }

  @Test
  void unwrappedRecordKeepsTheWriteTimeViewGuard() {
    // BeanSerializer.unwrappingSerializer was the one fluent copy that returned a stock
    // UnwrappingBeanSerializer, dropping the guard. Reachable: the unwrapped form runs the same
    // serializeFields path off renamed writer arrays, and pre-fix emitted
    // {"orderId":"o1","cust_email":"<Base64>"} with no subjectId. The prefixed names must appear in
    // the message, proving the guard reads the RENAMED writers.
    Exception thrown =
        assertThrows(
            Exception.class,
            () ->
                mapper
                    .writerWithView(PublicView.class)
                    .writeValueAsString(
                        new OrderWithUnwrappedViewedCustomer(
                            "o1", new ViewedCustomer("c44", "pii@example.com"))));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a crypto failure, got: " + thrown);
    assertTrue(cause.getMessage().contains("cust_customerId"), cause.getMessage());
    assertTrue(cause.getMessage().contains("cust_email"), cause.getMessage());
  }

  @Test
  void unwrappedRecordUnderAMatchingViewStillSerializes() throws Exception {
    String json =
        mapper
            .writerWithView(InternalView.class)
            .writeValueAsString(
                new OrderWithUnwrappedViewedCustomer(
                    "o1", new ViewedCustomer("c45", "pii@example.com")));

    assertFalse(json.contains("pii@example.com"), json);
    assertTrue(json.contains("cust_customerId"), json);
  }

  @com.fasterxml.jackson.annotation.JsonFilter("customer-filter")
  record FilteredCustomer(String customerId, @Encrypted(subjectId = "customerId") String email) {}

  private com.fasterxml.jackson.databind.ObjectWriter writerWithFilter(
      com.fasterxml.jackson.databind.ser.PropertyFilter filter) {
    return mapper.writer(
        new com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider()
            .addFilter("customer-filter", filter));
  }

  @Test
  void jsonFilterOmittingTheSubjectIdFailsClosed() {
    var filtered =
        writerWithFilter(
            com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter.serializeAllExcept(
                "customerId"));

    Exception thrown =
        assertThrows(
            Exception.class,
            () -> filtered.writeValueAsString(new FilteredCustomer("c46", "pii@example.com")));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a crypto failure, got: " + thrown);
    assertTrue(cause.getMessage().contains("@JsonFilter 'customer-filter'"), cause.getMessage());
    assertTrue(cause.getMessage().contains("permanently"), cause.getMessage());
  }

  @Test
  void jsonFilterOmittingTheEncryptedFieldFailsClosed() {
    var filtered =
        writerWithFilter(
            com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter.serializeAllExcept(
                "email"));

    Exception thrown =
        assertThrows(
            Exception.class,
            () -> filtered.writeValueAsString(new FilteredCustomer("c47", "pii@example.com")));

    CryptoOperationException cause = findCryptoCause(thrown);
    assertNotNull(cause, "expected a crypto failure, got: " + thrown);
    assertTrue(cause.getMessage().contains("'email' (@Encrypted"), cause.getMessage());
  }

  @Test
  void jsonFilterThatOmitsNothingProtectedStillSerializes() throws Exception {
    // No false positive — the filter is ASKED, per bean, rather than the annotation being refused:
    // a @JsonFilter that keeps both protected properties keeps working.
    var filtered =
        writerWithFilter(
            com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter.serializeAllExcept());

    String json = filtered.writeValueAsString(new FilteredCustomer("c48", "pii@example.com"));

    assertFalse(json.contains("pii@example.com"), json);
    assertTrue(json.contains("c48"), json);
    assertEquals("pii@example.com", mapper.readValue(json, FilteredCustomer.class).email());
  }

  // ---- Property-level WRITE residual: the mirror of the read residual above, and the
  // severe half — the encrypting writers never run, so the PII is stored as PLAINTEXT that no
  // later forget() can reach. Deliberately unguarded; these tests pin the behavior, its
  // observability, and the two legitimate shapes a guard could not tell apart from a bypass. ----

  static final class PlaintextReferencedCustomerSerializer
      extends com.fasterxml.jackson.databind.JsonSerializer<ReferencedCustomer> {
    @Override
    public void serialize(
        ReferencedCustomer value,
        com.fasterxml.jackson.core.JsonGenerator gen,
        com.fasterxml.jackson.databind.SerializerProvider provider)
        throws java.io.IOException {
      gen.writeStartObject();
      gen.writeStringField("email", value.email());
      gen.writeStringField("customerId", value.customerId());
      gen.writeEndObject();
    }
  }

  record OrderWithPropertySerializerUsing(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = PlaintextReferencedCustomerSerializer.class)
          ReferencedCustomer customer) {}

  record OrderWithPropertySerializerContentUsing(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              contentUsing = PlaintextReferencedCustomerSerializer.class)
          java.util.List<ReferencedCustomer> customers) {}

  @Test
  void propertyLevelJsonSerializeUsingOnAnotherTypesFieldRemainsUnguardedPlaintextResidual()
      throws Exception {
    // DOCUMENTED RESIDUAL, write side (gdpr-erasure.md "Never put a property-level
    // @JsonSerialize/@JsonDeserialize override on a field whose type carries @Encrypted"). A
    // property-level @JsonSerialize(using = ...) on ANOTHER type's field of the record type
    // replaces the record's serializer for that one property: no BeanSerializerModifier hook fires
    // for the record, so the encrypting property writers never run and the PII is written as
    // PLAINTEXT into the append-only store — unrecoverable, since no key was minted and no
    // forget(subjectId) can reach it. The route IS observable at findSerializer(AnnotatedMember)
    // (spy-verified below) and is deliberately left unguarded: see the two "still encrypts" pins
    // below for the legitimate shapes a member-level guard would break.
    var memberCalls = new java.util.ArrayList<String>();
    var spy =
        new com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector() {
          @Override
          public Object findSerializer(com.fasterxml.jackson.databind.introspect.Annotated a) {
            if (a instanceof com.fasterxml.jackson.databind.introspect.AnnotatedMember m
                && m.hasAnnotation(com.fasterxml.jackson.databind.annotation.JsonSerialize.class)) {
              memberCalls.add(m.getDeclaringClass().getSimpleName());
            }
            return null;
          }
        };
    var spyMapper = new ObjectMapper();
    spyMapper.registerModule(new CryptoShreddingModule(cryptoEngine));
    spyMapper.registerModule(
        new com.fasterxml.jackson.databind.module.SimpleModule("write-residual-spy") {
          @Override
          public void setupModule(SetupContext context) {
            super.setupModule(context);
            context.insertAnnotationIntrospector(spy);
          }
        });

    String json =
        spyMapper.writeValueAsString(
            new OrderWithPropertySerializerUsing(
                "o1", new ReferencedCustomer("pii@example.com", "c30")));

    assertTrue(
        json.contains("pii@example.com"),
        "residual: the property-level user serializer writes the PII as PLAINTEXT — " + json);
    assertTrue(
        memberCalls.contains("OrderWithPropertySerializerUsing"),
        "the member route IS observable at findSerializer(AnnotatedMember) — documented as"
            + " deliberately unguarded, saw: "
            + memberCalls);
  }

  @Test
  void propertyLevelJsonSerializeContentUsingOnAListOfEncryptedRecordsLeaksPlaintextToo()
      throws Exception {
    // Same residual through the container-content door: @JsonSerialize(contentUsing = ...) on a
    // List<record> replaces the ELEMENT serializer, so every element is written in plaintext.
    String json =
        mapper.writeValueAsString(
            new OrderWithPropertySerializerContentUsing(
                "o1", java.util.List.of(new ReferencedCustomer("pii@example.com", "c31"))));

    assertTrue(json.contains("pii@example.com"), "residual (contentUsing): " + json);
  }

  public static final class ReferencedCustomerToPlainMap
      extends com.fasterxml.jackson.databind.util.StdConverter<
          ReferencedCustomer, java.util.Map<String, String>> {
    @Override
    public java.util.Map<String, String> convert(ReferencedCustomer value) {
      return java.util.Map.of("email", value.email(), "customerId", value.customerId());
    }
  }

  record OrderWithPropertyConverter(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              converter = ReferencedCustomerToPlainMap.class)
          ReferencedCustomer customer) {}

  record OrderWithPropertyContentConverter(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              contentConverter = ReferencedCustomerToPlainMap.class)
          java.util.List<ReferencedCustomer> customers) {}

  @Test
  void propertyLevelJsonSerializeConverterRoutesAlsoLeakPlaintext() throws Exception {
    // The converter/contentConverter forms of the same residual: the value is converted to another
    // type BEFORE serialization, so the record's own (encrypting) serializer never runs.
    String viaConverter =
        mapper.writeValueAsString(
            new OrderWithPropertyConverter("o1", new ReferencedCustomer("pii@example.com", "c32")));
    String viaContentConverter =
        mapper.writeValueAsString(
            new OrderWithPropertyContentConverter(
                "o1", java.util.List.of(new ReferencedCustomer("pii@example.com", "c33"))));

    assertTrue(viaConverter.contains("pii@example.com"), "residual (converter): " + viaConverter);
    assertTrue(
        viaContentConverter.contains("pii@example.com"),
        "residual (contentConverter): " + viaContentConverter);
  }

  static final class DelegatingReferencedCustomerSerializer
      extends com.fasterxml.jackson.databind.JsonSerializer<ReferencedCustomer> {
    @Override
    public void serialize(
        ReferencedCustomer value,
        com.fasterxml.jackson.core.JsonGenerator gen,
        com.fasterxml.jackson.databind.SerializerProvider provider)
        throws java.io.IOException {
      // The legitimate pattern: pre-process, then DELEGATE BACK to the mapper, which resolves the
      // record's own encrypting serializer.
      provider.defaultSerializeValue(
          new ReferencedCustomer(value.email(), value.customerId().trim()), gen);
    }
  }

  record OrderWithDelegatingPropertySerializer(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              using = DelegatingReferencedCustomerSerializer.class)
          ReferencedCustomer customer) {}

  public static final class NormalizingReferencedCustomerConverter
      extends com.fasterxml.jackson.databind.util.StdConverter<
          ReferencedCustomer, ReferencedCustomer> {
    @Override
    public ReferencedCustomer convert(ReferencedCustomer value) {
      return new ReferencedCustomer(value.email(), value.customerId().trim());
    }
  }

  record OrderWithNormalizingPropertyConverter(
      String orderId,
      @com.fasterxml.jackson.databind.annotation.JsonSerialize(
              converter = NormalizingReferencedCustomerConverter.class)
          ReferencedCustomer customer) {}

  @Test
  void propertyLevelSerializerOverridesThatDelegateBackStillEncrypt() throws Exception {
    // WHY the member hook stays unguarded: these two carry the SAME annotations as the leaking
    // shapes above and are indistinguishable from them without running user code, yet they encrypt
    // correctly. A member-level guard would fail closed on both — and a write-side false positive
    // stops the application persisting events at all.
    String delegating =
        mapper.writeValueAsString(
            new OrderWithDelegatingPropertySerializer(
                "o1", new ReferencedCustomer("pii@example.com", " c34 ")));
    String normalizing =
        mapper.writeValueAsString(
            new OrderWithNormalizingPropertyConverter(
                "o1", new ReferencedCustomer("pii@example.com", " c35 ")));

    assertFalse(
        delegating.contains("pii@example.com"),
        "a custom serializer that delegates back to the mapper must keep encrypting: "
            + delegating);
    assertFalse(
        normalizing.contains("pii@example.com"),
        "a converter whose OUTPUT type is the record itself must keep encrypting: " + normalizing);
    assertEquals(
        "pii@example.com",
        mapper
            .readValue(delegating, OrderWithDelegatingPropertySerializer.class)
            .customer()
            .email());
  }

  @Test
  void classLevelDeserializeRouteConsultsIntrospectorBeforeCacheReturnsOverride() {
    // Hook-map evidence (spy methodology): DeserializerCache._createDeserializer consults
    // AnnotationIntrospector.findDeserializer with the ANNOTATED CLASS before returning the
    // annotation-provided deserializer (and before the factory/modifier path) — the pair asks the
    // inserted (primary) introspectors first, so the guard fails the read at deserializer
    // construction, before the override can ever run.
    var classCalls = new java.util.ArrayList<String>();
    var spy =
        new com.fasterxml.jackson.databind.introspect.NopAnnotationIntrospector() {
          @Override
          public Object findDeserializer(com.fasterxml.jackson.databind.introspect.Annotated a) {
            if (a instanceof com.fasterxml.jackson.databind.introspect.AnnotatedClass ac
                && ac.hasAnnotation(
                    com.fasterxml.jackson.databind.annotation.JsonDeserialize.class)) {
              classCalls.add(ac.getRawType().getSimpleName());
            }
            return null;
          }
        };
    var spyMapper = new ObjectMapper();
    spyMapper.registerModule(new CryptoShreddingModule(cryptoEngine));
    spyMapper.registerModule(
        new com.fasterxml.jackson.databind.module.SimpleModule("read-route-spy") {
          @Override
          public void setupModule(SetupContext context) {
            super.setupModule(context);
            context.insertAnnotationIntrospector(spy);
          }
        });
    String json = "{\"email\":\"aWdub3JlZA==\",\"customerId\":\"c25\"}";

    Exception thrown =
        assertThrows(
            Exception.class, () -> spyMapper.readValue(json, AnnotationReadCustomer.class));

    assertNotNull(findCryptoCause(thrown), "the guard must fire on this route: " + thrown);
    assertTrue(
        classCalls.contains("AnnotationReadCustomer"),
        "findDeserializer must have been consulted with the AnnotatedClass on read setup before"
            + " the guard fired, saw: "
            + classCalls);
  }

  // ---- Every component beside an @Encrypted one must read back exactly as written ----

  record PricedOrder(
      String customerId,
      @Encrypted(subjectId = "customerId") String shippingAddress,
      BigDecimal total,
      Instant placedAt,
      Duration leadTime) {}

  record BigDecimalBeside(
      String customerId, @Encrypted(subjectId = "customerId") String pii, BigDecimal value) {}

  record BigIntegerBeside(
      String customerId, @Encrypted(subjectId = "customerId") String pii, BigInteger value) {}

  // The encrypted component comes BEFORE its subjectId here: the decrypt side must find the
  // subjectId wherever it sits in the stored object.
  record InstantBeside(
      @Encrypted(subjectId = "customerId") String pii, Instant value, String customerId) {}

  record DurationBeside(
      String customerId, @Encrypted(subjectId = "customerId") String pii, Duration value) {}

  record OffsetDateTimeBeside(
      String customerId, @Encrypted(subjectId = "customerId") String pii, OffsetDateTime value) {}

  record LocalDateTimeBeside(
      String customerId, @Encrypted(subjectId = "customerId") String pii, LocalDateTime value) {}

  record DoubleBeside(
      String customerId, @Encrypted(subjectId = "customerId") String pii, double value) {}

  record FloatBeside(
      String customerId, @Encrypted(subjectId = "customerId") String pii, float value) {}

  record LongBeside(
      String customerId, @Encrypted(subjectId = "customerId") String pii, long value) {}

  /**
   * The mapper shape every StreamRune store builds: {@link JavaTimeModule} with its default numeric
   * timestamps (an Instant is written as {@code seconds.nanos}) plus this module.
   */
  private ObjectMapper storeMapper() {
    var storeMapper = new ObjectMapper();
    storeMapper.registerModule(new JavaTimeModule());
    storeMapper.registerModule(new CryptoShreddingModule(cryptoEngine));
    return storeMapper;
  }

  @Test
  void componentsBesideAnEncryptedOneReadBackExactlyAsWritten() throws Exception {
    // A record with an @Encrypted component takes the decrypt-before-construct path, which must
    // hand every OTHER component to the record exactly as the direct read would: a BigDecimal
    // keeps its scale, an Instant and a Duration keep all nine nanosecond digits. Anything less is
    // silent data drift that a snapshot then re-persists.
    var order =
        new PricedOrder(
            "customer-19",
            "1 Main St",
            new BigDecimal("19.90"),
            Instant.ofEpochSecond(1_700_000_000L, 123_456_789),
            Duration.ofSeconds(1_000_000_000L, 123_456_789));
    var storeMapper = storeMapper();

    String json = storeMapper.writeValueAsString(order);
    assertFalse(json.contains("1 Main St"), "the address must be ciphertext at rest: " + json);
    assertTrue(json.contains("19.90"), "the total is stored with its scale: " + json);

    PricedOrder restored = storeMapper.readValue(json, PricedOrder.class);

    assertEquals(order, restored, "read back from " + json);
    assertEquals(2, restored.total().scale(), "BigDecimal scale must survive the read");
    String reserialized = storeMapper.writeValueAsString(restored);
    assertTrue(
        reserialized.contains("\"total\":19.90,\"placedAt\":1700000000.123456789"),
        "a snapshot written from the restored record must carry the original literals: "
            + reserialized);
  }

  static Stream<Arguments> valueTypesJacksonHandlesSpecially() {
    return Stream.of(
        Arguments.of(
            "BigDecimal keeps its scale",
            new BigDecimalBeside("c-1", "secret-plaintext", new BigDecimal("19.90"))),
        Arguments.of(
            "BigDecimal with trailing zeros",
            new BigDecimalBeside("c-1", "secret-plaintext", new BigDecimal("100.00"))),
        Arguments.of(
            "BigDecimal zero with a scale",
            new BigDecimalBeside("c-1", "secret-plaintext", new BigDecimal("0.00"))),
        Arguments.of(
            "BigDecimal beyond double precision",
            new BigDecimalBeside(
                "c-1", "secret-plaintext", new BigDecimal("123456789012345678901.123456789"))),
        Arguments.of(
            "BigDecimal beyond double range",
            new BigDecimalBeside("c-1", "secret-plaintext", new BigDecimal("1E+400"))),
        Arguments.of(
            "BigInteger beyond long",
            new BigIntegerBeside(
                "c-1", "secret-plaintext", new BigInteger("123456789012345678901234567890"))),
        Arguments.of(
            "Instant with nanoseconds",
            new InstantBeside(
                "secret-plaintext", Instant.ofEpochSecond(1_700_000_000L, 123_456_789), "c-1")),
        Arguments.of(
            "Instant one nanosecond past the second",
            new InstantBeside(
                "secret-plaintext", Instant.parse("2026-10-03T12:34:56.000000001Z"), "c-1")),
        Arguments.of(
            "Duration with nanoseconds",
            new DurationBeside(
                "c-1", "secret-plaintext", Duration.ofSeconds(1_000_000_000L, 123_456_789))),
        Arguments.of(
            "OffsetDateTime with nanoseconds",
            new OffsetDateTimeBeside(
                "c-1",
                "secret-plaintext",
                OffsetDateTime.of(2026, 10, 3, 12, 34, 56, 123_456_789, ZoneOffset.UTC))),
        Arguments.of(
            "LocalDateTime with nanoseconds",
            new LocalDateTimeBeside(
                "c-1", "secret-plaintext", LocalDateTime.of(2026, 10, 3, 12, 34, 56, 123_456_789))),
        Arguments.of("double", new DoubleBeside("c-1", "secret-plaintext", 0.1)),
        Arguments.of(
            "smallest double", new DoubleBeside("c-1", "secret-plaintext", Double.MIN_VALUE)),
        Arguments.of("float", new FloatBeside("c-1", "secret-plaintext", 0.1f)),
        Arguments.of(
            "long beyond double precision",
            new LongBeside("c-1", "secret-plaintext", Long.MAX_VALUE)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("valueTypesJacksonHandlesSpecially")
  void valueTypesBesideAnEncryptedComponentRoundTripExactly(String label, Object record)
      throws Exception {
    var storeMapper = storeMapper();

    String json = storeMapper.writeValueAsString(record);
    assertFalse(json.contains("secret-plaintext"), label + ": PII must be ciphertext: " + json);

    Object restored = storeMapper.readValue(json, record.getClass());

    assertEquals(record, restored, label + ": read back from " + json);
  }

  @Test
  void polymorphicObjectWhoseOnlyPropertyWasTheTypeIdConstructsFromNoProperties() throws Exception {
    // The type-id reader consumes the opening brace and the type property before handing the
    // parser to the subtype's deserializer; with nothing else stored it arrives at the closing
    // brace. There is nothing to decrypt and the record is constructed from no properties,
    // exactly as a mapper without this module constructs it.
    PolymorphicEvent restored =
        mapper.readValue("{\"type\":\"registered\"}", PolymorphicEvent.class);

    assertEquals(new PolymorphicUserRegistered(null, null), restored);
  }
}
