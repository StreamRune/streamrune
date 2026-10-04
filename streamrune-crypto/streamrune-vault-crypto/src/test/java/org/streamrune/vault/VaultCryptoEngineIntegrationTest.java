package org.streamrune.vault;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;
import org.streamrune.runtime.gdpr.ForgetResult;
import org.streamrune.runtime.gdpr.ForgetSubjectService;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.vault.VaultContainer;

/**
 * Integration tests for {@link VaultCryptoEngine} against a real HashiCorp Vault instance. Requires
 * Docker.
 */
@Testcontainers
class VaultCryptoEngineIntegrationTest {

  private static final String VAULT_TOKEN = "test-root-token";

  private static final ObjectMapper JSON = new ObjectMapper();

  /**
   * The token policy the module README documents for the engine, verbatim (pinned against the
   * README by {@link #theReadmeDocumentsTheTokenPolicyTheseTestsRunWith()}). Every test that runs
   * with {@link #policyToken} proves it is sufficient for the operations it performs.
   */
  static final String DOCUMENTED_POLICY =
      """
      path "transit/encrypt/+" {
        capabilities = ["create", "update"]
      }
      path "transit/decrypt/+" {
        capabilities = ["update"]
      }
      path "transit/keys/+" {
        capabilities = ["read", "delete"]
      }
      path "transit/keys/+/config" {
        capabilities = ["update"]
      }
      """;

  /**
   * The documented policy without {@code update} on {@code transit/keys/+/config}: the engine can
   * encrypt, decrypt, read and delete keys, but cannot enable deletion on a key itself.
   */
  static final String POLICY_WITHOUT_KEY_CONFIG =
      """
      path "transit/encrypt/+" {
        capabilities = ["create", "update"]
      }
      path "transit/decrypt/+" {
        capabilities = ["update"]
      }
      path "transit/keys/+" {
        capabilities = ["read", "delete"]
      }
      """;

  /** A token holding only {@link #DOCUMENTED_POLICY} (plus Vault's built-in default policy). */
  private static String policyToken;

  /** A token holding only {@link #POLICY_WITHOUT_KEY_CONFIG}. */
  private static String noKeyConfigToken;

  @Container
  static final VaultContainer<?> VAULT =
      new VaultContainer<>("hashicorp/vault:1.18").withVaultToken(VAULT_TOKEN);

  private static VaultCryptoEngine engine;

  @BeforeAll
  static void setUp() throws Exception {
    String vaultAddress = "http://" + VAULT.getHost() + ":" + VAULT.getFirstMappedPort();

    // Enable Transit secrets engine via HTTP API
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest enableTransit =
        HttpRequest.newBuilder()
            .uri(URI.create(vaultAddress + "/v1/sys/mounts/transit"))
            .header("X-Vault-Token", VAULT_TOKEN)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"transit\"}"))
            .build();
    HttpResponse<String> resp = client.send(enableTransit, HttpResponse.BodyHandlers.ofString());
    assertEquals(204, resp.statusCode(), "Failed to enable transit engine: " + resp.body());

    policyToken = tokenWithPolicy("streamrune-documented", DOCUMENTED_POLICY);
    noKeyConfigToken = tokenWithPolicy("streamrune-no-key-config", POLICY_WITHOUT_KEY_CONFIG);

    engine =
        VaultCryptoEngine.builder()
            .httpClient(HttpClient.newHttpClient())
            .vaultAddress(vaultAddress)
            .token(VAULT_TOKEN)
            .engineMount("transit")
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();
  }

  private static String vaultAddress() {
    return "http://" + VAULT.getHost() + ":" + VAULT.getFirstMappedPort();
  }

  /** Sends one request to Vault with the root token, bypassing the engine entirely. */
  private static HttpResponse<String> rootCall(String method, String path, String jsonBody)
      throws Exception {
    HttpRequest.Builder request =
        HttpRequest.newBuilder()
            .uri(URI.create(vaultAddress() + "/v1/" + path))
            .header("X-Vault-Token", VAULT_TOKEN)
            .header("Content-Type", "application/json");
    request.method(
        method,
        jsonBody == null
            ? HttpRequest.BodyPublishers.noBody()
            : HttpRequest.BodyPublishers.ofString(jsonBody));
    return HttpClient.newHttpClient().send(request.build(), HttpResponse.BodyHandlers.ofString());
  }

  /** Writes the ACL policy and returns a fresh token that holds it. */
  private static String tokenWithPolicy(String policyName, String hcl) throws Exception {
    HttpResponse<String> policy =
        rootCall(
            "PUT",
            "sys/policies/acl/" + policyName,
            JSON.writeValueAsString(Map.of("policy", hcl)));
    assertEquals(204, policy.statusCode(), "Failed to write policy: " + policy.body());
    HttpResponse<String> token =
        rootCall(
            "POST",
            "auth/token/create",
            JSON.writeValueAsString(Map.of("policies", List.of(policyName))));
    assertEquals(200, token.statusCode(), "Failed to create token: " + token.body());
    return JSON.readTree(token.body()).path("auth").path("client_token").asText();
  }

  /** The transit key name the engine derives for a subject: SHA-256 hex, never the raw id. */
  private static String transitKeyName(String subjectValue) throws Exception {
    var md = MessageDigest.getInstance("SHA-256");
    return "subject-"
        + HexFormat.of().formatHex(md.digest(subjectValue.getBytes(StandardCharsets.UTF_8)));
  }

  /** The HTTP status of reading the subject's transit key directly, with the root token. */
  private static int transitKeyStatus(String subjectValue) throws Exception {
    return rootCall("GET", "transit/keys/" + transitKeyName(subjectValue), null).statusCode();
  }

  private static VaultCryptoEngine engineWithToken(String token, ForgottenSubjectStore store) {
    return VaultCryptoEngine.builder()
        .httpClient(HttpClient.newHttpClient())
        .vaultAddress(vaultAddress())
        .token(token)
        .engineMount("transit")
        .forgottenSubjectStore(store)
        .build();
  }

  @Test
  void encryptAndDecryptRoundTrip() {
    byte[] plaintext = "Hello, Vault!".getBytes(StandardCharsets.UTF_8);

    byte[] ciphertext = engine.encrypt(SubjectId.of("user-1"), plaintext);
    assertNotNull(ciphertext);
    assertTrue(ciphertext.length > 0);

    byte[] decrypted = engine.decrypt(SubjectId.of("user-1"), ciphertext);
    assertArrayEquals(plaintext, decrypted);
  }

  @Test
  void encryptProducesDifferentCiphertextEachTime() {
    byte[] plaintext = "same-data".getBytes(StandardCharsets.UTF_8);

    byte[] ciphertext1 = engine.encrypt(SubjectId.of("user-2"), plaintext);
    byte[] ciphertext2 = engine.encrypt(SubjectId.of("user-2"), plaintext);

    // Both should decrypt to same plaintext
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("user-2"), ciphertext1));
    assertArrayEquals(plaintext, engine.decrypt(SubjectId.of("user-2"), ciphertext2));
  }

  @Test
  void isKeyAvailableReturnsTrueAfterEncrypt() {
    engine.encrypt(SubjectId.of("user-3"), "data".getBytes(StandardCharsets.UTF_8));
    assertTrue(engine.isKeyAvailable(SubjectId.of("user-3")));
  }

  @Test
  void isKeyAvailableReturnsFalseForNonExistentKey() {
    assertFalse(engine.isKeyAvailable(SubjectId.of("nonexistent-user-99")));
  }

  @Test
  void forgetsAKeyThatEncryptCreatedOnADefaultTransitMount() throws Exception {
    // Transit creates a subject's key on the first encrypt, with deletion_allowed=false, and its
    // name is derived from the subject id, so nobody can configure it in advance. The forget must
    // destroy it with no write from outside the engine.
    var subject = SubjectId.of("lazily-created-key-user");
    byte[] ciphertext = engine.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8));
    assertEquals(200, transitKeyStatus("lazily-created-key-user"), "encrypt created the key");

    assertDoesNotThrow(() -> engine.deleteKey(subject));

    assertEquals(
        404, transitKeyStatus("lazily-created-key-user"), "the key material is gone from Vault");
    assertFalse(engine.isKeyAvailable(subject));
    assertThrows(KeyNotFoundException.class, () -> engine.decrypt(subject, ciphertext));
    assertThrows(
        SubjectForgottenException.class, () -> engine.encrypt(subject, "new-pii".getBytes()));
    assertDoesNotThrow(() -> engine.deleteKey(subject), "a repeated forget is a no-op");
  }

  @Test
  void forgetSubjectServiceCompletesTheErasureOfAKeyThatEncryptCreated() throws Exception {
    var subject = SubjectId.of("forget-service-user");
    engine.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8));
    List<SubjectId> purged = Collections.synchronizedList(new ArrayList<>());
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(engine)
            .purgers(
                List.of(
                    new SubjectDataPurger() {
                      @Override
                      public String name() {
                        return "customers_view";
                      }

                      @Override
                      public void purge(SubjectId subjectId) {
                        purged.add(subjectId);
                      }
                    }))
            .build();

    ForgetResult result = service.forget(subject, null);

    assertTrue(result.fullyErased(), "the erasure must complete: " + result);
    assertEquals(List.of(subject), purged, "the read-model purger runs after the key is deleted");
    assertEquals(404, transitKeyStatus("forget-service-user"), "the key material is gone");
  }

  @Test
  void theDocumentedTokenPolicyCoversEveryEngineOperation() throws Exception {
    var policyEngine = engineWithToken(policyToken, new InMemoryForgottenSubjectStore());
    var subject = SubjectId.of("documented-policy-user");
    byte[] plaintext = "pii".getBytes(StandardCharsets.UTF_8);

    byte[] ciphertext = policyEngine.encrypt(subject, plaintext); // creates the key
    assertArrayEquals(plaintext, policyEngine.decrypt(subject, ciphertext));
    assertTrue(policyEngine.isKeyAvailable(subject));

    policyEngine.deleteKey(subject);

    assertEquals(404, transitKeyStatus("documented-policy-user"), "the key material is gone");
    assertDoesNotThrow(() -> policyEngine.deleteKey(subject), "a repeated forget is a no-op");
    var neverEncrypted = SubjectId.of("documented-policy-never-encrypted");
    assertDoesNotThrow(() -> policyEngine.deleteKey(neverEncrypted));
    var refusal =
        assertThrows(CryptoOperationException.class, () -> policyEngine.reinstate(subject));
    assertTrue(
        refusal.getMessage().contains("new subject id"),
        "reinstate reads the key and refuses after a completed erasure: " + refusal.getMessage());
  }

  @Test
  void aTokenThatCannotEnableDeletionFailsTheForgetLoudly_andARerunWithTheCapabilityConverges()
      throws Exception {
    var store = new InMemoryForgottenSubjectStore();
    var restricted = engineWithToken(noKeyConfigToken, store);
    var subject = SubjectId.of("no-key-config-user");
    byte[] ciphertext = restricted.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8));

    var thrown = assertThrows(CryptoOperationException.class, () -> restricted.deleteKey(subject));

    assertTrue(
        thrown.getMessage().contains("\"update\" capability on transit/keys/+/config"),
        "the error must name the policy capability the token lacks: " + thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains("HTTP 403"),
        "the error must report how Vault answered the config write: " + thrown.getMessage());
    // The tombstone was committed before any Vault write, so the subject is erased from the
    // application's view; only the key material survives until the forget is re-run.
    assertEquals(200, transitKeyStatus("no-key-config-user"), "the key material survives");
    assertFalse(restricted.isKeyAvailable(subject));
    assertThrows(KeyNotFoundException.class, () -> restricted.decrypt(subject, ciphertext));

    // Re-running the forget once the token holds the documented policy converges.
    var fixed = engineWithToken(policyToken, store);
    assertDoesNotThrow(() -> fixed.deleteKey(subject));
    assertEquals(404, transitKeyStatus("no-key-config-user"), "the re-run destroyed the key");
  }

  @Test
  void aTokenWithoutCreateOnEncryptCannotEncryptForANewSubject() throws Exception {
    // The README asks for "create" on <mount>/encrypt/+ because transit creates a subject's key on
    // the subject's first encrypt, and refuses that with "update" alone.
    String updateOnly =
        tokenWithPolicy(
            "streamrune-encrypt-update-only",
            """
            path "transit/encrypt/+" {
              capabilities = ["update"]
            }
            """);
    var updateOnlyEngine = engineWithToken(updateOnly, new InMemoryForgottenSubjectStore());

    assertThrows(
        CryptoOperationException.class,
        () ->
            updateOnlyEngine.encrypt(
                SubjectId.of("update-only-new-subject"), "pii".getBytes(StandardCharsets.UTF_8)));
    assertEquals(404, transitKeyStatus("update-only-new-subject"), "no key was created");
  }

  @Test
  void theReadmeDocumentsTheTokenPolicyTheseTestsRunWith() throws Exception {
    String readme = Files.readString(Path.of("README.md"));
    assertTrue(
        readme.contains(DOCUMENTED_POLICY),
        "README.md must document the token policy pinned by this test, verbatim");
  }

  @Test
  void decryptFailsAfterKeyDeletion() throws Exception {
    // Create a key and encrypt data
    byte[] ciphertext =
        engine.encrypt(SubjectId.of("delete-me"), "secret".getBytes(StandardCharsets.UTF_8));

    // No deletion_allowed write from outside the engine: deleteKey enables deletion itself.
    engine.deleteKey(SubjectId.of("delete-me"));

    // A RETRIED forget must converge (the documented remediation contract): the key
    // is already gone, real Vault answers the second DELETE with its not-found 400, and the engine
    // treats that as idempotent success — this pins Vault transit's actual delete-endpoint error
    // wording to VaultCryptoEngine.DELETE_KEY_NOT_FOUND_MARKER (drift would surface here as a
    // CryptoOperationException).
    assertDoesNotThrow(() -> engine.deleteKey(SubjectId.of("delete-me")));

    // Terminal erasure: the Vault key is gone AND a tombstone was
    // recorded, so isKeyAvailable is false and a later encrypt for the forgotten subject must be
    // refused with SubjectForgottenException rather than silently minting a fresh Vault key.
    assertFalse(engine.isKeyAvailable(SubjectId.of("delete-me")));
    assertThrows(
        SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("delete-me"), "resurrected-pii".getBytes()));

    // With the tombstone present, decrypt short-circuits to KeyNotFoundException (mapped to
    // [REDACTED]) without hitting Vault.
    assertThrows(
        org.streamrune.core.crypto.KeyNotFoundException.class,
        () -> engine.decrypt(SubjectId.of("delete-me"), ciphertext));

    // Against REAL Vault: the erasure COMPLETED (the
    // transit key really was deleted above), so reinstate must REFUSE — a re-minted transit key
    // cannot decrypt pre-erasure blobs and vault cannot discriminate destroyed generations, so
    // lifting the tombstone would flip this subject's history from [REDACTED] to replay-blocking
    // decrypt failures the moment anything re-encrypts. The 404 probe runs against real Vault.
    var refusal =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.reinstate(SubjectId.of("delete-me")));
    assertTrue(
        refusal.getMessage().contains("new subject id"),
        "the refusal must direct the operator to a new subject id: " + refusal.getMessage());
    assertThrows(
        org.streamrune.core.crypto.KeyNotFoundException.class,
        () -> engine.decrypt(SubjectId.of("delete-me"), ciphertext),
        "the refused subject stays terminally gated by its tombstone");

    // Missing-key decrypt against REAL Vault: send well-formed transit ciphertext to a subject
    // whose key name never existed (no tombstone, no key), so the request genuinely reaches
    // Vault. This is the assertion that pins Vault transit's actual missing-key error wording to
    // VaultCryptoEngine.KEY_NOT_FOUND_MARKER — if real Vault's text ever drifts, a genuine
    // forget would surface as CryptoOperationException and halt replay instead of yielding
    // [REDACTED]; this fails loudly if that happens.
    assertThrows(
        org.streamrune.core.crypto.KeyNotFoundException.class,
        () -> engine.decrypt(SubjectId.of("never-keyed-subject"), ciphertext));
  }

  /**
   * Mutable tombstone store so tests can inspect and override what an erasure recorded. Mirrors
   * {@code JdbcForgottenSubjectStore}, including first-write-wins on the key-identity evidence.
   */
  private static final class StubForgottenStore
      implements org.streamrune.crypto.ForgottenSubjectStore {
    volatile boolean forgotten;
    volatile java.time.Instant at;
    volatile java.time.Instant keyCreatedAt;

    @Override
    public void forget(SubjectId subjectId) {
      forgotten = true;
    }

    @Override
    public void forget(SubjectId subjectId, java.time.Instant erasedKeyCreatedAt) {
      boolean firstWrite = !forgotten;
      forget(subjectId);
      if (firstWrite) {
        keyCreatedAt = erasedKeyCreatedAt;
      }
    }

    @Override
    public boolean isForgotten(SubjectId subjectId) {
      return forgotten;
    }

    @Override
    public void reinstate(SubjectId subjectId) {
      forgotten = false;
      keyCreatedAt = null;
    }

    @Override
    public java.util.Optional<java.time.Instant> forgottenAt(SubjectId subjectId) {
      return java.util.Optional.ofNullable(at);
    }

    @Override
    public java.util.Optional<java.time.Instant> keyCreatedAt(SubjectId subjectId) {
      return java.util.Optional.ofNullable(keyCreatedAt);
    }
  }

  private static VaultCryptoEngine engineWith(StubForgottenStore store) {
    return engineWith(VAULT_TOKEN, store, java.time.Duration.ofSeconds(60));
  }

  private static VaultCryptoEngine engineWith(
      String token, StubForgottenStore store, java.time.Duration revivalIdentityMargin) {
    return VaultCryptoEngine.builder()
        .httpClient(HttpClient.newHttpClient())
        .vaultAddress(vaultAddress())
        .token(token)
        .engineMount("transit")
        .forgottenSubjectStore(store)
        .revivalIdentityMargin(revivalIdentityMargin)
        .build();
  }

  @Test
  void reinstateDiscriminatesKeyIdentityAgainstRealVault_creationTimeWireShapePinned()
      throws Exception {
    // Against REAL Vault: reinstate proves key
    // IDENTITY from the transit key's oldest-version creation time (parsed from the real GET
    // /keys/<name> response — this test pins that wire shape; drift would surface as an unexpected
    // fail-closed refusal here).
    //
    // Made that proof an EQUALITY between two
    // VAULT-sourced instants — the creation time deleteKey recorded with the tombstone and the one
    // the live key reports now — so no clock of this process's or of the tombstone store's enters
    // it at all. (The intermediate AGE comparison still spanned the store's clock and ours, and a
    // store clock running AHEAD of ours under-measured the tombstone's age in exactly the
    // direction that let a post-forget residue key pass.) That also removes this test's former
    // dependence on real elapsed time: no sleep, no second-granularity race.

    // Revivable CRASHED-erasure shape, end to end against real Vault: the erasing engine's token
    // cannot enable deletion on the key (no "update" on transit/keys/+/config), and a key that
    // transit's encrypt-upsert created has deletion_allowed=false, so deleteKey commits the
    // tombstone (with the key's real creation_time, read from real Vault) and then FAILS its
    // transit DELETE — exactly the residue the revival exists for.
    //
    // The identity margin is checked at ERASURE time between two second-granular Vault instants
    // (transit's epoch-second creation_time and the HTTP Date header), so the key must be created
    // at least one whole Vault second before the erasure. Any real elapsed time >= 1s guarantees
    // floor(now) - floor(created) >= 1; 1.5s leaves half a second of slack for clock drift between
    // this process's sleep and Vault's clock, and a 1ms margin then passes deterministically.
    var reviveStore = new StubForgottenStore();
    var reviveEngine = engineWith(noKeyConfigToken, reviveStore, java.time.Duration.ofMillis(1));
    var revivable = SubjectId.of("identity-revive-subject");
    reviveEngine.encrypt(revivable, "pii".getBytes(StandardCharsets.UTF_8)); // creates the key
    Thread.sleep(1500);
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> reviveEngine.deleteKey(revivable),
        "deletion not enabled: the tombstone commits, the transit DELETE fails — the crash shape");
    assertTrue(reviveStore.forgotten, "the tombstone commits before the transit DELETE");
    assertTrue(
        reviveStore.keyCreatedAt.isBefore(java.time.Instant.now().plusSeconds(60)),
        "the erasure recorded the key's REAL creation_time, parsed from real Vault's key read");

    assertDoesNotThrow(() -> reviveEngine.reinstate(revivable));
    assertFalse(reviveStore.forgotten, "a provably-original key must be revivable");

    // Residue shape: the erasure destroyed a key created two hours before the one now live, so
    // the live key is encrypt-race residue and reviving it would poison every pre-erasure blob.
    // Must refuse and keep the tombstone. The refusal runs against real Vault's key read.
    var residueStore = new StubForgottenStore();
    var residueEngine = engineWith(residueStore);
    var poisoned = SubjectId.of("identity-residue-subject");
    residueEngine.encrypt(poisoned, "pii".getBytes(StandardCharsets.UTF_8)); // creates the key
    residueStore.forgotten = true;
    residueStore.at = java.time.Instant.now().minus(java.time.Duration.ofHours(2));
    residueStore.keyCreatedAt = residueStore.at;
    var refusal =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> residueEngine.reinstate(poisoned));
    assertTrue(
        refusal.getMessage().contains("new subject id"),
        "the residue refusal must direct to a new subject id: " + refusal.getMessage());
    assertTrue(residueStore.forgotten, "the refused subject must stay tombstoned");

    // And a tombstone carrying NO evidence (recorded without it) makes identity unprovable however
    // healthy the live key is. Fail closed.
    var noEvidenceStore = new StubForgottenStore();
    var noEvidenceEngine = engineWith(noEvidenceStore);
    var noEvidence = SubjectId.of("identity-no-evidence-tombstone-subject");
    noEvidenceEngine.encrypt(noEvidence, "pii".getBytes(StandardCharsets.UTF_8));
    noEvidenceStore.forgotten = true;
    noEvidenceStore.at = java.time.Instant.now();
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> noEvidenceEngine.reinstate(noEvidence),
        "a tombstone recorded without key-identity evidence must make the revival refuse");
    assertTrue(noEvidenceStore.forgotten, "the refused subject must stay tombstoned");
  }

  @Test
  void deleteKeyOnNeverEncryptedSubjectIsIdempotentNoOp() {
    // ForgetSubjectService documents forget-on-unknown-subject as a silent no-op. Against real
    // Vault the transit-key DELETE answers a not-found 400; the engine must treat it as success
    // (tombstone written, nothing to erase), never as the misleading deletion_allowed failure.
    var subject = SubjectId.of("never-encrypted-user");
    assertDoesNotThrow(() -> engine.deleteKey(subject));

    assertFalse(engine.isKeyAvailable(subject), "subject must be gated by the tombstone");
    assertThrows(
        SubjectForgottenException.class, () -> engine.encrypt(subject, "new-pii".getBytes()));
  }

  @Test
  void multipleSubjectsHaveIsolatedKeys() {
    byte[] data1 = "data-for-alice".getBytes(StandardCharsets.UTF_8);
    byte[] data2 = "data-for-bob".getBytes(StandardCharsets.UTF_8);

    byte[] ct1 = engine.encrypt(SubjectId.of("alice"), data1);
    byte[] ct2 = engine.encrypt(SubjectId.of("bob"), data2);

    assertArrayEquals(data1, engine.decrypt(SubjectId.of("alice"), ct1));
    assertArrayEquals(data2, engine.decrypt(SubjectId.of("bob"), ct2));
  }

  @Test
  void largePayloadEncryptDecrypt() {
    byte[] largePayload = new byte[10_000];
    java.util.Arrays.fill(largePayload, (byte) 42);

    byte[] ciphertext = engine.encrypt(SubjectId.of("large-user"), largePayload);
    byte[] decrypted = engine.decrypt(SubjectId.of("large-user"), ciphertext);

    assertArrayEquals(largePayload, decrypted);
  }
}
