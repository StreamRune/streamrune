package org.streamrune.vault;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.HexFormat;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;

/**
 * HTTP contract tests for {@link VaultCryptoEngine} using WireMock. Validates correct HTTP requests
 * and response parsing without Docker.
 */
class VaultCryptoEngineWireMockTest {

  private static WireMockServer wireMock;
  private VaultCryptoEngine engine;

  /** Mirrors the engine's key naming: subject IDs are hashed, never embedded in the URL. */
  private static String keyName(String subjectId) {
    try {
      var md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(subjectId.getBytes(StandardCharsets.UTF_8));
      return "subject-" + HexFormat.of().formatHex(digest);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
  }

  @BeforeAll
  static void startWireMock() {
    wireMock = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    wireMock.start();
  }

  @AfterAll
  static void stopWireMock() {
    wireMock.stop();
  }

  @BeforeEach
  void setUp() {
    wireMock.resetAll();
    engine =
        VaultCryptoEngine.builder()
            .httpClient(HttpClient.newHttpClient())
            .vaultAddress("http://localhost:" + wireMock.port())
            .token("test-token")
            .engineMount("transit")
            // Tiny backoff: retry tests exercise the full retry budget without slowing the suite.
            .retryDelay(java.time.Duration.ofMillis(5))
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();
  }

  @Test
  void encryptSendsCorrectRequestAndParsesResponse() {
    String b64Plaintext = Base64.getEncoder().encodeToString("hello".getBytes());
    String vaultCiphertext = "vault:v1:somecipherdata";

    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/encrypt/" + keyName("user-1")))
            .withHeader("X-Vault-Token", equalTo("test-token"))
            .withHeader("Content-Type", equalTo("application/json"))
            .withRequestBody(containing("\"plaintext\":\"" + b64Plaintext + "\""))
            .willReturn(okJson("{\"data\":{\"ciphertext\":\"" + vaultCiphertext + "\"}}")));

    byte[] result = engine.encrypt(SubjectId.of("user-1"), "hello".getBytes());
    // encrypt returns the raw vault ciphertext string as bytes
    assertEquals(vaultCiphertext, new String(result, StandardCharsets.UTF_8));

    wireMock.verify(
        postRequestedFor(urlEqualTo("/v1/transit/encrypt/" + keyName("user-1")))
            .withHeader("X-Vault-Token", equalTo("test-token")));
  }

  @Test
  void decryptSendsCorrectRequestAndParsesResponse() {
    String b64Plaintext = Base64.getEncoder().encodeToString("hello".getBytes());

    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/decrypt/" + keyName("user-1")))
            .withHeader("X-Vault-Token", equalTo("test-token"))
            .willReturn(okJson("{\"data\":{\"plaintext\":\"" + b64Plaintext + "\"}}")));

    byte[] result = engine.decrypt(SubjectId.of("user-1"), "vault:v1:abc".getBytes());
    assertArrayEquals("hello".getBytes(), result);

    wireMock.verify(
        postRequestedFor(urlEqualTo("/v1/transit/decrypt/" + keyName("user-1")))
            .withRequestBody(containing("\"ciphertext\":\"vault:v1:abc\"")));
  }

  @Test
  void decryptReturns400WithKeyNotFoundErrorThrowsKeyNotFoundException() {
    // Vault transit's actual decrypt error text for a missing key.
    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/decrypt/" + keyName("missing-user")))
            .willReturn(
                aResponse()
                    .withStatus(400)
                    .withBody("{\"errors\":[\"encryption key not found\"]}")));

    assertThrows(
        KeyNotFoundException.class,
        () -> engine.decrypt(SubjectId.of("missing-user"), "vault:v1:data".getBytes()));
  }

  @Test
  void decryptReturns400WithInvalidCiphertextErrorThrowsCryptoMappingException() {
    // Corrupt/tampered ciphertext also comes back as HTTP 400 from Vault transit, but must NOT
    // be reported as "key not found" — that would mask data-integrity incidents as a GDPR forget.
    // It is a deterministic blob verdict, so it is the CryptoMappingException subtype
    // (still a CryptoOperationException for every existing catch).
    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/decrypt/" + keyName("user-1")))
            .willReturn(
                aResponse()
                    .withStatus(400)
                    .withBody("{\"errors\":[\"invalid ciphertext: could not decode base64\"]}")));

    CryptoOperationException thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoMappingException.class,
            () -> engine.decrypt(SubjectId.of("user-1"), "vault:v1:abc".getBytes()));
    assertTrue(thrown.getMessage().contains("invalid ciphertext"));
  }

  @Test
  void decryptReturns400WithEmptyBodyThrowsCryptoOperationException() {
    // Unparseable/empty error body: default to the safe direction, never silently redact.
    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/decrypt/" + keyName("user-1")))
            .willReturn(aResponse().withStatus(400)));

    assertThrows(
        CryptoOperationException.class,
        () -> engine.decrypt(SubjectId.of("user-1"), "vault:v1:abc".getBytes()));
  }

  @Test
  void encryptReturns500ThrowsCryptoOperationException() {
    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/encrypt/" + keyName("user-1")))
            .willReturn(aResponse().withStatus(500)));

    assertThrows(
        CryptoOperationException.class,
        () -> engine.encrypt(SubjectId.of("user-1"), "data".getBytes()));
  }

  @Test
  void deleteKeyEnablesDeletionOnTheKeyConfigThenSendsDeleteRequest() {
    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/keys/" + keyName("user-1") + "/config"))
            .willReturn(aResponse().withStatus(204)));
    wireMock.stubFor(
        delete(urlEqualTo("/v1/transit/keys/" + keyName("user-1")))
            .withHeader("X-Vault-Token", equalTo("test-token"))
            .willReturn(aResponse().withStatus(204)));

    engine.deleteKey(SubjectId.of("user-1"));

    wireMock.verify(
        postRequestedFor(urlEqualTo("/v1/transit/keys/" + keyName("user-1") + "/config"))
            .withHeader("X-Vault-Token", equalTo("test-token"))
            .withHeader("Content-Type", equalTo("application/json"))
            .withRequestBody(equalToJson("{\"deletion_allowed\":true}")));
    wireMock.verify(
        deleteRequestedFor(urlEqualTo("/v1/transit/keys/" + keyName("user-1")))
            .withHeader("X-Vault-Token", equalTo("test-token")));
  }

  @Test
  void deleteKeyReturns500ThrowsCryptoOperationException() {
    wireMock.stubFor(
        delete(urlEqualTo("/v1/transit/keys/" + keyName("user-1")))
            .willReturn(aResponse().withStatus(500)));

    assertThrows(CryptoOperationException.class, () -> engine.deleteKey(SubjectId.of("user-1")));
  }

  @Test
  void isKeyAvailableReturns200ReturnsTrue() {
    wireMock.stubFor(
        get(urlEqualTo("/v1/transit/keys/" + keyName("user-1")))
            .willReturn(okJson("{\"data\":{\"name\":\"subject-user-1\"}}")));

    assertTrue(engine.isKeyAvailable(SubjectId.of("user-1")));
  }

  @Test
  void isKeyAvailableReturns404ReturnsFalse() {
    wireMock.stubFor(
        get(urlEqualTo("/v1/transit/keys/" + keyName("user-1")))
            .willReturn(aResponse().withStatus(404)));

    assertFalse(engine.isKeyAvailable(SubjectId.of("user-1")));
  }

  @Test
  void encryptReturnsCiphertextWithVaultPrefix() {
    String vaultCiphertext = "vault:v1:somecipherdata";

    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/encrypt/" + keyName("user-1")))
            .willReturn(okJson("{\"data\":{\"ciphertext\":\"" + vaultCiphertext + "\"}}")));

    byte[] result = engine.encrypt(SubjectId.of("user-1"), "data".getBytes());
    // Result preserves the full vault ciphertext including prefix
    assertEquals(vaultCiphertext, new String(result, StandardCharsets.UTF_8));
  }

  @Test
  void subjectIdWithPathCharactersCannotRedirectTheRequest() {
    // A subjectId like "x/../../other-key" must never become URL path segments — the hashed key
    // name keeps the request on the engine's own encrypt endpoint.
    String hostile = "x/../../other-key";

    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/encrypt/" + keyName(hostile)))
            .willReturn(okJson("{\"data\":{\"ciphertext\":\"vault:v1:ct\"}}")));

    engine.encrypt(SubjectId.of(hostile), "data".getBytes());

    wireMock.verify(postRequestedFor(urlEqualTo("/v1/transit/encrypt/" + keyName(hostile))));
  }

  @Test
  void decryptRejectsMalformedCiphertextWithoutCallingVault() {
    // Tampered data at rest containing '"' would otherwise be embedded verbatim in the JSON
    // request body (JSON injection). It must be rejected before any HTTP request is built.
    byte[] tampered = "vault:v1:abc\",\"context\":\"evil".getBytes(StandardCharsets.UTF_8);

    assertThrows(
        CryptoOperationException.class, () -> engine.decrypt(SubjectId.of("user-1"), tampered));

    assertTrue(wireMock.getAllServeEvents().isEmpty(), "no request may reach Vault");
  }

  @Test
  void encryptRetriesOn429ThenSucceeds() {
    String scenario = "encrypt-retry";
    String url = "/v1/transit/encrypt/" + keyName("user-retry");
    wireMock.stubFor(
        post(urlEqualTo(url))
            .inScenario(scenario)
            .whenScenarioStateIs(com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED)
            .willReturn(aResponse().withStatus(429))
            .willSetStateTo("second-attempt"));
    wireMock.stubFor(
        post(urlEqualTo(url))
            .inScenario(scenario)
            .whenScenarioStateIs("second-attempt")
            .willReturn(okJson("{\"data\":{\"ciphertext\":\"vault:v1:ct\"}}")));

    byte[] result = engine.encrypt(SubjectId.of("user-retry"), "data".getBytes());

    assertEquals("vault:v1:ct", new String(result, StandardCharsets.UTF_8));
    wireMock.verify(2, postRequestedFor(urlEqualTo(url)));
  }

  @Test
  void encryptExhaustsRetriesOnPersistentServerError() {
    String url = "/v1/transit/encrypt/" + keyName("user-down");
    wireMock.stubFor(post(urlEqualTo(url)).willReturn(aResponse().withStatus(500)));

    CryptoOperationException thrown =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.encrypt(SubjectId.of("user-down"), "data".getBytes()));

    assertTrue(thrown.getMessage().contains("500"), "error must carry the HTTP status");
    // Default maxRetries(2) → 1 initial attempt + 2 retries.
    wireMock.verify(3, postRequestedFor(urlEqualTo(url)));
  }

  @Test
  void decryptRetriesOnConnectionResetThenSucceeds() {
    String scenario = "decrypt-reset";
    String url = "/v1/transit/decrypt/" + keyName("user-reset");
    String b64Plaintext = Base64.getEncoder().encodeToString("hello".getBytes());
    wireMock.stubFor(
        post(urlEqualTo(url))
            .inScenario(scenario)
            .whenScenarioStateIs(com.github.tomakehurst.wiremock.stubbing.Scenario.STARTED)
            .willReturn(
                aResponse()
                    .withFault(com.github.tomakehurst.wiremock.http.Fault.CONNECTION_RESET_BY_PEER))
            .willSetStateTo("second-attempt"));
    wireMock.stubFor(
        post(urlEqualTo(url))
            .inScenario(scenario)
            .whenScenarioStateIs("second-attempt")
            .willReturn(okJson("{\"data\":{\"plaintext\":\"" + b64Plaintext + "\"}}")));

    byte[] result = engine.decrypt(SubjectId.of("user-reset"), "vault:v1:abc".getBytes());

    assertArrayEquals("hello".getBytes(), result);
  }

  @Test
  void isKeyAvailableThrowsOn403InsteadOfReportingAbsent() {
    // 403 is token expiry/permissions, not "no key" — callers may treat "no key" as
    // "already crypto-shredded", so the engine must throw instead of answering false.
    wireMock.stubFor(
        get(urlEqualTo("/v1/transit/keys/" + keyName("user-403")))
            .willReturn(aResponse().withStatus(403)));

    CryptoOperationException thrown =
        assertThrows(
            CryptoOperationException.class, () -> engine.isKeyAvailable(SubjectId.of("user-403")));
    assertTrue(thrown.getMessage().contains("403"));
  }

  @Test
  void deleteKeyRefusedBecauseTheTokenCannotEnableDeletionNamesTheCapability() {
    // The token may not write the key config, and the key still has deletion_allowed=false: the
    // error must name the policy capability instead of failing with an opaque status code.
    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/keys/" + keyName("user-locked") + "/config"))
            .willReturn(
                aResponse()
                    .withStatus(403)
                    .withBody(
                        "{\"errors\":[\"1 error occurred:\\n\\t* permission denied\\n\\n\"]}")));
    wireMock.stubFor(
        delete(urlEqualTo("/v1/transit/keys/" + keyName("user-locked")))
            .willReturn(
                aResponse()
                    .withStatus(400)
                    .withBody(
                        "{\"errors\":[\"error deleting policy "
                            + keyName("user-locked")
                            + ": deletion is not allowed for this key\"]}")));

    CryptoOperationException thrown =
        assertThrows(
            CryptoOperationException.class, () -> engine.deleteKey(SubjectId.of("user-locked")));
    assertTrue(
        thrown.getMessage().contains("deletion_allowed")
            && thrown.getMessage().contains("\"update\" capability on transit/keys/+/config"),
        "error must name deletion_allowed and the capability the token lacks: "
            + thrown.getMessage());
  }

  @Test
  void encryptThrowsWhenResponseLacksCiphertextField() {
    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/encrypt/" + keyName("user-1")))
            .willReturn(okJson("{\"data\":{}}")));

    CryptoOperationException thrown =
        assertThrows(
            CryptoOperationException.class,
            () -> engine.encrypt(SubjectId.of("user-1"), "data".getBytes()));
    assertTrue(thrown.getMessage().contains("ciphertext"));
  }

  @Test
  void decryptThrowsWhenResponseIsNotJson() {
    wireMock.stubFor(
        post(urlEqualTo("/v1/transit/decrypt/" + keyName("user-1")))
            .willReturn(aResponse().withStatus(200).withBody("<html>proxy error</html>")));

    assertThrows(
        CryptoOperationException.class,
        () -> engine.decrypt(SubjectId.of("user-1"), "vault:v1:abc".getBytes()));
  }

  @Test
  void customEngineMountUsesCorrectPath() {
    var customEngine =
        VaultCryptoEngine.builder()
            .httpClient(HttpClient.newHttpClient())
            .vaultAddress("http://localhost:" + wireMock.port())
            .token("test-token")
            .engineMount("custom-transit")
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();

    wireMock.stubFor(
        post(urlEqualTo("/v1/custom-transit/encrypt/" + keyName("user-1")))
            .willReturn(okJson("{\"data\":{\"ciphertext\":\"vault:v1:ct\"}}")));

    customEngine.encrypt(SubjectId.of("user-1"), "data".getBytes());

    wireMock.verify(
        postRequestedFor(urlEqualTo("/v1/custom-transit/encrypt/" + keyName("user-1"))));
  }
}
