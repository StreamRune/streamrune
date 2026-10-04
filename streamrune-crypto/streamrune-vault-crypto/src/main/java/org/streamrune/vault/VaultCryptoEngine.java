package org.streamrune.vault;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Objects;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.CryptoOperationException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.crypto.SubjectForgottenException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.ForgottenSubjectStore;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;

/**
 * HashiCorp Vault Transit Engine-backed {@link CryptoEngine}. Key material never leaves Vault.
 *
 * <p><b>Resilience:</b> every request uses a configurable {@link Builder#requestTimeout(Duration)}
 * (default 10s). Transient failures — connection errors and HTTP 429/5xx — are retried up to {@link
 * Builder#maxRetries(int)} times (default 2) with exponential backoff starting at {@link
 * Builder#retryDelay(Duration)} (default 200ms) and doubling per retry up to a 30s ceiling. All
 * operations used here are idempotent against Vault transit (encrypt/decrypt are pure
 * transformations with key upsert, key GET/DELETE are naturally idempotent), so retrying is safe.
 *
 * <p>{@link #isKeyAvailable(SubjectId)} only answers {@code false} on a definitive HTTP 404. Any
 * other non-200 status (403 token expiry, 5xx outage) throws — callers may treat "no key" as
 * "already crypto-shredded", so a live key must never be misreported as absent.
 *
 * <p><b>Terminal erasure via a per-subject tombstone.</b> Vault's transit engine re-creates a
 * deleted key on the next {@code encrypt} (transit upserts the named key), so on its own {@code
 * deleteKey} is <em>not</em> terminal — a later encrypt would silently resurrect the subject under
 * a fresh key version. To close that gap this engine records a durable tombstone in a {@link
 * ForgottenSubjectStore} on {@link #deleteKey(SubjectId)} <em>before</em> deleting the Vault key
 * (safe ordering: a tombstoned-but-still-live key fails closed, whereas a deleted key with no
 * tombstone would resurrect): afterwards {@link #encrypt(SubjectId, byte[])} throws {@link
 * SubjectForgottenException}, {@link #decrypt(SubjectId, byte[])} throws {@link
 * KeyNotFoundException} (mapped to the {@code [REDACTED]} tombstone by {@code
 * CryptoShreddingModule}), and {@link #isKeyAvailable(SubjectId)} returns {@code false} — matching
 * the terminal-erasure behavior of the filesystem/PostgreSQL/KMS backends. {@link
 * #reinstate(SubjectId)} lifts the tombstone ONLY for the documented crashed-erasure residue
 * (tombstone committed, the transit DELETE never landed: the safe residue this ordering
 * deliberately allows), proven by key IDENTITY — the live transit key's oldest-version {@code
 * creation_time} must EQUAL the one {@code deleteKey} recorded with the tombstone, both read from
 * Vault, so the proof carries no cross-clock term at all; after a COMPLETED erasure, or when the
 * live key is post-forget residue (left by an encrypt racing the erasure), or whenever identity
 * cannot be proven, it REFUSES (see the method javadoc) — including the ordinary GDPR case of a
 * subject that first encrypted and then exercised erasure within {@link
 * Builder#revivalIdentityMargin(Duration)}, for which {@code deleteKey} deliberately records no
 * evidence because that key's creation time could not later be told apart from a re-created one's.
 * Re-run {@link #deleteKey(SubjectId)} — idempotent, converges, sweeps residue — before reinstating
 * a subject whose erasure may have failed or may still be in flight.
 *
 * <p><b>Encrypt re-checks the tombstone after the Vault call.</b> The pre-check alone leaves a
 * TOCTOU window: an encrypt that passed the pre-check while a concurrent {@link
 * #deleteKey(SubjectId)} completed in full would land its POST after the transit DELETE — transit
 * upserts a brand-new live key and returns ciphertext for an erased subject, with nothing left to
 * ever delete that key. {@link #encrypt(SubjectId, byte[])} therefore re-checks {@code isForgotten}
 * after a successful Vault response; if the subject was forgotten mid-flight it best-effort deletes
 * the (possibly just-upserted) transit key and throws {@link SubjectForgottenException} instead of
 * returning ciphertext. Because {@code deleteKey} commits the tombstone before issuing the Vault
 * DELETE, any POST that landed after that DELETE necessarily observes the tombstone in its re-check
 * — no interleaving escapes both checks.
 *
 * <p><b>Durability is mandatory — the engine fails closed.</b> Erasure is only as durable as the
 * {@link ForgottenSubjectStore tombstone store}, so {@link Builder#build()} <b>throws</b> unless
 * one is supplied via {@link Builder#forgottenSubjectStore(ForgottenSubjectStore)}: there is no
 * silent in-memory default. Wire the durable {@code JdbcForgottenSubjectStore} (in {@code
 * streamrune-postgres-crypto}) in production (the framework auto-configs do this automatically when
 * a {@code DataSource} is present); {@link InMemoryForgottenSubjectStore} may be passed explicitly
 * for tests and single-process/ephemeral use.
 *
 * <p><b>400 disambiguation.</b> Vault transit's {@code /decrypt} returns HTTP 400 for several
 * distinct causes — a missing encryption key, invalid/malformed ciphertext, a bad key version,
 * general validation errors. Only a response body positively identifying a missing key (Vault's
 * {@code "encryption key not found"} error text) is mapped to {@link KeyNotFoundException}. A body
 * positively identifying a DETERMINISTIC verdict on the ciphertext itself — Vault's {@code "invalid
 * ciphertext ..."} family (shape, base64, length) or the AEAD failure text if forwarded ({@code
 * "message authentication failed"}) — throws the deterministic {@link CryptoMappingException}
 * subtype: retrying can never succeed, and the read-path classifiers must quarantine the one blob
 * instead of treating it as a Vault outage and retrying forever. Every other 400 — including an
 * unparseable or empty body, a request-validation error, a version refused by the key's {@code
 * min_decryption_version} policy ({@code "... disallowed by policy (too old)"}), and a ciphertext
 * version Vault reports as {@code "too new"} for this node (despite sharing the {@code "invalid
 * ciphertext"} prefix, this is reversible without touching the blob — a lagging secondary, a
 * restore from an older snapshot, or key material temporarily missing, not a verdict on the blob —
 * exactly like the policy refusal) — throws the bare {@link CryptoOperationException} with the
 * Vault error text preserved. This prevents ciphertext corruption or tampering from masquerading as
 * a GDPR forget, and a reversible node/policy state from masquerading as corruption.
 */
public final class VaultCryptoEngine implements CryptoEngine {

  private static final Logger log = LoggerFactory.getLogger(VaultCryptoEngine.class);

  /** The only ciphertext shape Vault's transit engine produces: {@code vault:v<N>:<base64>}. */
  private static final Pattern VAULT_CIPHERTEXT = Pattern.compile("vault:v\\d+:[A-Za-z0-9+/=]+");

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final HttpClient httpClient;
  private final String vaultAddress;
  private final String token;
  private final String engineMount;
  private final Duration requestTimeout;
  private final int maxRetries;
  private final Duration retryDelay;
  private final ForgottenSubjectStore forgottenSubjects;
  private final Duration revivalIdentityMargin;

  private VaultCryptoEngine(Builder builder) {
    this.httpClient = builder.httpClient;
    this.vaultAddress = builder.vaultAddress;
    this.token = builder.token;
    this.engineMount = builder.engineMount;
    this.requestTimeout = builder.requestTimeout;
    this.maxRetries = builder.maxRetries;
    this.retryDelay = builder.retryDelay;
    this.forgottenSubjects = builder.forgottenSubjects;
    this.revivalIdentityMargin = builder.revivalIdentityMargin;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public byte[] encrypt(SubjectId subjectId, byte[] plaintext) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (plaintext == null) {
      throw new IllegalArgumentException("plaintext must not be null");
    }
    if (forgottenSubjects.isForgotten(subjectId)) {
      // Terminal erasure: refuse to re-encrypt for a crypto-shredded subject, matching the
      // filesystem/PostgreSQL/KMS backends. Vault transit would otherwise silently upsert a fresh
      // key. Lift via reinstate(subjectId) for legitimate re-registration.
      throw new SubjectForgottenException(
          "Subject was crypto-shredded (GDPR erasure) and cannot be re-encrypted: "
              + subjectId.redacted()
              + " — call reinstate(subjectId) to re-register");
    }
    String url = vaultAddress + "/v1/" + engineMount + "/encrypt/" + keyName(subjectId.value());
    String body = "{\"plaintext\":\"" + Base64.getEncoder().encodeToString(plaintext) + "\"}";
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .header("X-Vault-Token", token)
              .header("Content-Type", "application/json")
              .timeout(requestTimeout)
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();
      HttpResponse<String> response = send(request);
      if (response.statusCode() != 200) {
        throw new CryptoOperationException(
            "Vault encrypt failed for subject: "
                + subjectId.redacted()
                + " — HTTP "
                + response.statusCode());
      }
      // Re-check the tombstone AFTER the Vault call succeeded. A
      // concurrent deleteKey() may have completed fully — tombstone committed, transit DELETE
      // succeeded, erasure reported complete — between the pre-check above and this POST reaching
      // Vault. Transit UPSERTS the named key, so in that interleaving the POST just resurrected a
      // brand-new live key for an erased subject, and no later operation would ever delete it.
      //
      // Soundness rests on deleteKey()'s ordering invariant (tombstone commit BEFORE the Vault
      // DELETE): if this POST executed after that DELETE (the resurrecting interleaving), the
      // tombstone was already durably committed before the DELETE was even issued — so this
      // re-check, which runs after the POST's response, must observe it. If the POST instead
      // executed before the DELETE, the DELETE itself removes whatever key the POST used or
      // upserted, and the ciphertext (whether returned here or refused below) is permanently
      // undecryptable — no resurrection either way. Every interleaving is caught by at least one
      // of {pre-check, deleteKey's DELETE, this post-check}.
      if (forgottenSubjects.isForgotten(subjectId)) {
        bestEffortDeleteResurrectedKey(subjectId);
        throw new SubjectForgottenException(
            "Subject was crypto-shredded (GDPR erasure) while this encrypt was in flight — the"
                + " ciphertext is discarded and the (possibly re-created) Vault transit key was"
                + " best-effort deleted: "
                + subjectId.redacted()
                + " — call reinstate(subjectId) to re-register");
      }
      String ciphertext = readDataField(response.body(), "ciphertext");
      return ciphertext.getBytes(StandardCharsets.UTF_8);
    } catch (CryptoOperationException | SubjectForgottenException e) {
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Vault encrypt failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * Best-effort removal of a transit key that an in-flight encrypt re-created (upserted) after the
   * subject's erasure — the cleanup half of the {@code encrypt} post-check. A key created by
   * transit's encrypt-upsert has {@code deletion_allowed=false}, so this first POSTs the key config
   * enabling deletion, then issues the DELETE. Both calls are deliberately best-effort and never
   * throw: the subject is already terminally gated by its tombstone (encrypt/decrypt/isKeyAvailable
   * all refuse), so a failure here leaves only un-deleted key material — the same documented safe
   * residue as a failed {@link #deleteKey(SubjectId)} Vault step — and re-running the forget
   * converges ({@code ForgetSubjectService.forget} and {@code deleteKey} are idempotent: the
   * tombstone re-write no-ops and the Vault DELETE is retried). On failure an ERROR names the exact
   * Vault path so an operator can also delete the key manually.
   */
  private void bestEffortDeleteResurrectedKey(SubjectId subjectId) {
    String keyPath = "/v1/" + engineMount + "/keys/" + keyName(subjectId.value());
    try {
      // The config POST's outcome is not checked on its own: the DELETE below is the decider (the
      // config may legitimately 403 under a restrictive policy while the key is still deletable).
      enableDeletion(keyPath);
      HttpResponse<String> response = deleteTransitKey(keyPath);
      // A not-found answer means deleteKey's own DELETE already removed the key after the late
      // POST: nothing was left behind, so this is success, not residue.
      if (!isTransitKeyGone(response)) {
        log.error(
            "GDPR erasure residue: an in-flight encrypt re-created (upserted) the Vault transit key"
                + " of an already-forgotten subject and the automatic cleanup DELETE failed with"
                + " HTTP {} (subject-hash={}, vault path={}). The subject stays terminally gated by"
                + " its tombstone, but live key material persists in Vault. Re-run the forget for"
                + " this subject (deleteKey is idempotent) or delete the key manually after setting"
                + " deletion_allowed=true on {}/config.",
            response.statusCode(),
            subjectId.redacted(),
            keyPath,
            keyPath);
      }
    } catch (Exception e) {
      log.error(
          "GDPR erasure residue: an in-flight encrypt re-created (upserted) the Vault transit key"
              + " of an already-forgotten subject and the automatic cleanup failed"
              + " (subject-hash={}, vault path={}). The subject stays terminally gated by its"
              + " tombstone, but live key material persists in Vault. Re-run the forget for this"
              + " subject (deleteKey is idempotent) or delete the key manually after setting"
              + " deletion_allowed=true on {}/config.",
          subjectId.redacted(),
          keyPath,
          keyPath,
          e);
    }
  }

  @Override
  public byte[] decrypt(SubjectId subjectId, byte[] ciphertext) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (ciphertext == null) {
      throw new IllegalArgumentException("ciphertext must not be null");
    }
    if (forgottenSubjects.isForgotten(subjectId)) {
      // Crypto-shredded subject: old ciphertext is unreadable. KeyNotFoundException is mapped to
      // the [REDACTED] tombstone by CryptoShreddingModule, so replay/read models surface no PII —
      // and we never send the ciphertext to Vault for a subject we already know is forgotten.
      throw new KeyNotFoundException(
          "Vault subject was crypto-shredded (GDPR erasure): " + subjectId.redacted());
    }
    String ciphertextString = new String(ciphertext, StandardCharsets.UTF_8);
    // The ciphertext comes from data at rest and may have been tampered with. It is embedded
    // verbatim in the JSON request body, so anything outside Vault's strict transit ciphertext
    // shape (e.g. a '"' enabling JSON injection) must be rejected before building the request.
    if (!VAULT_CIPHERTEXT.matcher(ciphertextString).matches()) {
      // A DETERMINISTIC blob-property failure (decided on data at rest, before any HTTP
      // call — retrying can never succeed), raised as the CryptoMappingException subtype the
      // read-path classifiers treat as poison; the bare CryptoOperationException reads to them as
      // a Vault OUTAGE and is retried forever.
      throw new CryptoMappingException(
          "Invalid Vault ciphertext for subject: "
              + subjectId.redacted()
              + " — expected vault:v<N>:<base64>; refusing to send malformed data to Vault");
    }
    String url = vaultAddress + "/v1/" + engineMount + "/decrypt/" + keyName(subjectId.value());
    String body = "{\"ciphertext\":\"" + ciphertextString + "\"}";
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .header("X-Vault-Token", token)
              .header("Content-Type", "application/json")
              .timeout(requestTimeout)
              .POST(HttpRequest.BodyPublishers.ofString(body))
              .build();
      HttpResponse<String> response = send(request);
      if (response.statusCode() == 400) {
        if (isKeyNotFoundError(response.body())) {
          throw new KeyNotFoundException(
              "Key not found in Vault for subject: " + subjectId.redacted());
        }
        if (isInvalidCiphertextError(response.body())) {
          // A DETERMINISTIC verdict on the blob itself — transit rejected its shape,
          // version, base64, length, or (via the AEAD open error it forwards verbatim) its
          // authentication tag. Retrying can never succeed, so raise the CryptoMappingException
          // subtype the read-path classifiers treat as poison; the bare type reads to them as a
          // Vault OUTAGE and is retried forever. Every OTHER 400 stays bare on purpose: a request
          // validation error is not a blob verdict, and a version refused by the key's
          // min_decryption_version policy ("... disallowed by policy (too old)") is reversible by
          // the operator — quarantining every old-version blob for a policy setting would be
          // worse than blocking until the policy is relaxed.
          throw new CryptoMappingException(
              "Vault rejected the ciphertext for subject: "
                  + subjectId.redacted()
                  + " — HTTP 400: "
                  + firstErrorOrBody(response.body())
                  + " (deterministic; retrying cannot succeed)");
        }
        throw new CryptoOperationException(
            "Vault decrypt failed for subject: "
                + subjectId.redacted()
                + " — HTTP 400: "
                + firstErrorOrBody(response.body()));
      }
      if (response.statusCode() != 200) {
        throw new CryptoOperationException(
            "Vault decrypt failed for subject: "
                + subjectId.redacted()
                + " — HTTP "
                + response.statusCode());
      }
      String plaintext = readDataField(response.body(), "plaintext");
      return Base64.getDecoder().decode(plaintext);
    } catch (CryptoOperationException | KeyNotFoundException e) {
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Vault decrypt failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * Records the subject's tombstone, then deletes its transit key.
   *
   * <p><b>Enabling deletion.</b> Vault transit refuses to delete a key whose config has {@code
   * deletion_allowed=false}, and that is how {@code encrypt} creates every subject's key: on first
   * use, under a name derived from the subject id, so no operator can configure it in advance. This
   * method therefore sets {@code deletion_allowed=true} on {@code <mount>/keys/<name>/config}
   * before the DELETE. The DELETE's answer decides the outcome, not the config write's: a token
   * that may not write the config still erases a key that is already deletable, and when the key is
   * not, the failure names the policy capability the token lacks. If the DELETE is refused although
   * deletion was enabled, an encrypt that passed its tombstone check before this erasure committed
   * has created the key since (transit creates a missing key on encrypt); one more round enables
   * deletion on that key and deletes it.
   *
   * <p><b>Vault token policy.</b> The engine's token needs {@code create} and {@code update} on
   * {@code <mount>/encrypt/+} ({@code create} lets the first encrypt create the subject's key),
   * {@code update} on {@code <mount>/decrypt/+}, {@code read} and {@code delete} on {@code
   * <mount>/keys/+}, and {@code update} on {@code <mount>/keys/+/config}. The module README lists
   * it as a Vault policy.
   *
   * <p><b>Deleting an already-absent key is an idempotent no-op.</b> Vault answers a DELETE of a
   * nonexistent transit key with HTTP 400 ({@code could not delete key; not found}); that response
   * is treated as success, because the tombstone is already durably committed and there is nothing
   * left to erase. A retried forget after a successful delete, a forget for a subject that never
   * encrypted, and this DELETE racing the {@code encrypt} post-check cleanup all complete normally.
   *
   * <p><b>Every crash point converges on a re-run.</b> Before the tombstone commit nothing has
   * changed. After it, the subject is gated (encrypt and decrypt refuse) and only key material
   * remains; the re-run's tombstone write is a no-op, and it enables deletion and deletes. After
   * the config write, the key is gated and deletable, and the re-run's config write changes
   * nothing. After the DELETE, the re-run's config write finds no key and its DELETE answers
   * not-found, which is success. A failed call leaves the same states, so the remedy for any
   * failure is to re-run the forget.
   */
  @Override
  public void deleteKey(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    // Terminal erasure: record the durable tombstone BEFORE any Vault write, matching
    // FileSystemCryptoEngine's documented safe ordering. If the tombstone write succeeds but a
    // Vault call then fails (transient error) or the JVM crashes between the steps, the subject is
    // STILL terminally gated: encrypt() throws SubjectForgottenException and decrypt() throws
    // KeyNotFoundException (both short-circuit on the tombstone before ever reaching Vault), so a
    // tombstoned-but-still-live key is the SAFE failure — the only residue is un-deleted key
    // material, and forget() is idempotent so a retried delete converges. The reverse order
    // (delete-then-tombstone) reopens a PII-resurrection window: a failed tombstone leaves the key
    // gone with no tombstone, and a later encrypt() silently upserts a fresh Vault transit key for
    // the "forgotten" subject.
    //
    // ORDERING INVARIANT — DO NOT REORDER: encrypt()'s post-Vault tombstone re-check is sound ONLY
    // because the tombstone commits before the Vault DELETE is issued. An encrypt POST that lands
    // after this DELETE (and would therefore resurrect the key via transit's upsert) is guaranteed
    // to find the tombstone already committed when it re-checks after its response — reordering
    // these two steps silently reopens that resurrection window.
    //
    // The identity-evidence read below happens BEFORE the tombstone and is read-only, so it does
    // not touch that invariant: it only widens the window in which an encrypt may still legally
    // run, and any key such an encrypt creates is destroyed by the DELETE that follows.
    forgottenSubjects.forget(subjectId, readErasureIdentityEvidence(subjectId));
    String keyPath = "/v1/" + engineMount + "/keys/" + keyName(subjectId.value());
    try {
      int configStatus = enableDeletion(keyPath);
      HttpResponse<String> response = deleteTransitKey(keyPath);
      if (isDeletionNotAllowedError(response)) {
        // Either the token cannot write the key config, or the config write found no key and an
        // encrypt that passed its tombstone pre-check before the tombstone above committed then
        // created one with deletion_allowed=false. Only encrypts already in flight can do that, so
        // one more round deletes the key they created.
        configStatus = enableDeletion(keyPath);
        response = deleteTransitKey(keyPath);
      }
      if (!isTransitKeyGone(response)) {
        throw deleteKeyFailure(subjectId, response, configStatus);
      }
    } catch (CryptoOperationException e) {
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Vault deleteKey failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * Sets {@code deletion_allowed=true} on the transit key at {@code keyPath} ({@code
   * /v1/<mount>/keys/<name>}) and returns Vault's HTTP status. The status is informational: Vault
   * rejects the write when the key does not exist, and answers 403 when the token may not write the
   * config; the DELETE that follows decides either way.
   */
  private int enableDeletion(String keyPath) throws IOException {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(vaultAddress + keyPath + "/config"))
            .header("X-Vault-Token", token)
            .header("Content-Type", "application/json")
            .timeout(requestTimeout)
            .POST(HttpRequest.BodyPublishers.ofString("{\"deletion_allowed\":true}"))
            .build();
    return send(request).statusCode();
  }

  /** Issues the transit key DELETE for {@code keyPath} ({@code /v1/<mount>/keys/<name>}). */
  private HttpResponse<String> deleteTransitKey(String keyPath) throws IOException {
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(vaultAddress + keyPath))
            .header("X-Vault-Token", token)
            .timeout(requestTimeout)
            .DELETE()
            .build();
    return send(request);
  }

  /**
   * Whether a transit key DELETE left no key behind: it deleted the key (204/200), or the key was
   * already gone (400 {@code could not delete key; not found} — never created, deleted by an
   * earlier forget, or removed by the encrypt post-check cleanup racing this DELETE).
   */
  private static boolean isTransitKeyGone(HttpResponse<String> response) {
    int status = response.statusCode();
    return status == 204
        || status == 200
        || (status == 400 && isDeleteKeyNotFoundError(response.body()));
  }

  /**
   * Whether Vault refused a transit key DELETE because the key has {@code deletion_allowed=false}.
   */
  private static boolean isDeletionNotAllowedError(HttpResponse<String> response) {
    return response.statusCode() == 400
        && errorsContain(response.body(), DELETION_NOT_ALLOWED_MARKER);
  }

  /**
   * The exception for a transit key DELETE that left the key in place, naming what the operator has
   * to change. The subject is already gated by its tombstone, so every case ends in "re-run the
   * forget".
   */
  private CryptoOperationException deleteKeyFailure(
      SubjectId subjectId, HttpResponse<String> response, int configStatus) {
    String failed =
        "Vault deleteKey failed for subject: "
            + subjectId.redacted()
            + " — HTTP "
            + response.statusCode();
    String rerun =
        " The subject is already gated by its tombstone; only the key material remains. Re-run the"
            + " forget once this is fixed (deleteKey is idempotent).";
    if (isDeletionNotAllowedError(response)) {
      if (configStatus == 200 || configStatus == 204) {
        return new CryptoOperationException(
            failed
                + ": Vault refused to delete the transit key although this engine had just set"
                + " deletion_allowed=true on it (HTTP "
                + configStatus
                + "), so an encrypt racing this erasure re-created the key after that write. The"
                + " subject is already gated by its tombstone. Re-run the forget: deleteKey is"
                + " idempotent.");
      }
      return new CryptoOperationException(
          failed
              + ": the transit key has deletion_allowed=false and this engine could not enable"
              + " deletion on it ("
              + engineMount
              + "/keys/"
              + keyName(subjectId.value())
              + "/config answered HTTP "
              + configStatus
              + "). Grant the engine's Vault token the \"update\" capability on "
              + engineMount
              + "/keys/+/config."
              + rerun);
    }
    if (response.statusCode() == 403) {
      return new CryptoOperationException(
          failed
              + ": permission denied. The engine's Vault token must be valid and hold the"
              + " \"delete\" capability on "
              + engineMount
              + "/keys/+ and the \"update\" capability on "
              + engineMount
              + "/keys/+/config."
              + rerun);
    }
    if (response.statusCode() == 400) {
      return new CryptoOperationException(
          failed + ": " + firstErrorOrBody(response.body()) + "." + rerun);
    }
    return new CryptoOperationException(failed + "." + rerun);
  }

  /**
   * Reads the identity evidence {@link #reinstate(SubjectId)} will later need — the transit key's
   * oldest-version creation time on VAULT's own clock — for the tombstone {@link
   * #deleteKey(SubjectId)} is about to write. Returns {@code null} when no provable evidence is
   * available, in which case the revival fails closed.
   *
   * <p><b>Never throws, and never fails the erasure.</b> The caller is exercising GDPR Article 17;
   * an erasure must not be blocked because the (optional) evidence for a possible future revival
   * could not be read. Every unavailable case is recorded as "no evidence" and WARN-logged with its
   * reason, so an operator who later meets a refusal can see why:
   *
   * <ul>
   *   <li>the key does not exist (nothing to record — a forget of a never-encrypted subject, or a
   *       repeated erasure whose first run already destroyed the key);
   *   <li>Vault is unreachable or answers a non-200/404 status;
   *   <li>the per-version creation time is unreadable, or Vault reported no parseable HTTP {@code
   *       Date} response header (its own clock reading);
   *   <li>the key does not yet out-age this erasure by {@link
   *       Builder#revivalIdentityMargin(Duration)}. Both instants come from Vault, so this test
   *       imports no foreign clock. It is what keeps the later equality check meaningful despite
   *       Vault's coarse creation-time granularity: a residue key can only carry the SAME creation
   *       timestamp as the original if it is created within one granule of it, which requires the
   *       subject's first encrypt and this erasure to fall in that same granule. Refusing to record
   *       evidence there costs the revival convenience for a subject that encrypted and erased
   *       within the margin (ordinary under GDPR) and buys an unambiguous proof for every other.
   * </ul>
   *
   * <p>The instant is truncated to MICROSECONDS: the durable tombstone column is a PostgreSQL
   * {@code TIMESTAMPTZ} (microsecond resolution), so a nanosecond-precision {@code creation_time}
   * would not round-trip and the equality check would refuse the very key it recorded. {@link
   * #reinstate(SubjectId)} truncates the live value identically.
   */
  private java.time.Instant readErasureIdentityEvidence(SubjectId subjectId) {
    String reason;
    try {
      TransitKeyProbe probe = probeTransitKey(subjectId);
      java.time.Instant created = probe.oldestVersionCreatedAt();
      java.time.Instant vaultNow = probe.vaultServerTime();
      if (!probe.exists()) {
        return null; // nothing to erase and nothing to revive — not worth a WARN
      } else if (created == null) {
        reason = "Vault did not report a parseable per-version creation time for the key";
      } else if (vaultNow == null) {
        reason =
            "Vault did not report its own server time (no parseable HTTP Date response header) —"
                + " a proxy that strips or rewrites Date must be fixed";
      } else if (!created.isBefore(vaultNow.minus(revivalIdentityMargin))) {
        reason =
            "the key ("
                + created
                + ") does not out-age this erasure ("
                + vaultNow
                + ", both on Vault's own clock) by the "
                + revivalIdentityMargin
                + " identity margin, so its creation time could not later be told apart from that"
                + " of a key an encrypt racing this erasure re-creates";
      } else {
        return created.truncatedTo(java.time.temporal.ChronoUnit.MICROS);
      }
    } catch (RuntimeException e) {
      log.warn(
          "GDPR erasure for subject {} recorded NO key-identity evidence: the Vault key read"
              + " failed. The erasure itself proceeds unaffected; only a later reinstate is"
              + " affected — it will refuse, because it cannot prove a surviving transit key is"
              + " the one this erasure targeted. Re-register the person under a new subject id if"
              + " they return.",
          subjectId.redacted(),
          e);
      return null;
    }
    log.warn(
        "GDPR erasure for subject {} recorded NO key-identity evidence: {}. The erasure itself"
            + " proceeds unaffected; only a later reinstate is affected — it will refuse, because"
            + " it cannot prove a surviving transit key is the one this erasure targeted."
            + " Re-register the person under a new subject id if they return.",
        subjectId.redacted(),
        reason);
    return null;
  }

  /**
   * Default {@link Builder#revivalIdentityMargin(Duration)}: the minimum margin by which the
   * transit key must out-age the erasure — measured wholly on VAULT's clock at {@link
   * #deleteKey(SubjectId)} time — before that erasure records the key-identity evidence a later
   * {@link #reinstate(SubjectId)} needs. Absorbs Vault's epoch-second creation-time granularity and
   * the round-trip latency between the two instants, which is what makes the later equality check
   * an unambiguous identity proof: a residue key can only carry the same creation timestamp as the
   * original if it is created within one granule of it. A genuine original was created when the
   * subject FIRST encrypted, out-aging the erasure by the subject's whole data lifetime.
   *
   * <p><b>A subject that first encrypts and then exercises erasure within this margin</b> (ordinary
   * under GDPR) has its crashed-erasure revival refused. The direction is fail-closed, so only the
   * revival convenience is lost: re-register under a new subject id.
   */
  static final Duration DEFAULT_REVIVAL_IDENTITY_MARGIN = Duration.ofSeconds(60);

  /**
   * Lifts the terminal-erasure tombstone — but <b>only for the documented crashed-erasure residue,
   * proven by key IDENTITY, not mere existence</b>: the live transit key's oldest-version {@code
   * creation_time} must EQUAL the creation time {@link #deleteKey(SubjectId)} recorded with the
   * tombstone. Both instants are read from Vault, so the proof carries <b>no cross-clock term at
   * all</b> and no clock disagreement anywhere can turn a refusal into an acceptance (the previous
   * AGE comparison measured the tombstone's age across this process's clock and the tombstone
   * store's — Postgres {@code NOW()} on the database host for the shipped JDBC store — and a store
   * clock running ahead of ours under-measured it in exactly the direction that let a post-forget
   * residue key pass; only its gross form, a {@code forgotten_at} in this process's future, was
   * detectable). Only a key that matches is the ORIGINAL (tombstone committed, the transit DELETE
   * never landed), whose revival returns its ciphertext to service. A bare "key exists" is NOT that
   * proof — the same codebase documents a second cause: an encrypt racing the erasure upserts a
   * FRESH key after the original was destroyed, and {@code bestEffortDeleteResurrectedKey} may have
   * failed to sweep it (it never throws; its ERROR-log remediation is advisory). Reviving that
   * residue serves the subject a DIFFERENT key: the first re-encrypt works, and every PRE-erasure
   * blob then decrypts as HTTP 400 "invalid ciphertext" — a {@link CryptoOperationException}
   * permanently blocking aggregate loads and full projection rebuilds — the exact poison this
   * refusal exists to prevent.
   *
   * <p><b>The evidence is written once, at erasure time, and never invented.</b> {@link
   * #deleteKey(SubjectId)} reads the key before destroying it and records its creation time (see
   * {@code readErasureIdentityEvidence}); the record is first-write-wins, so a repeated erasure
   * cannot overwrite it with a residue key's timestamp. A tombstone that carries NO evidence — one
   * from a {@link ForgottenSubjectStore} that does not implement {@code keyCreatedAt}, or from an
   * erasure whose key read failed — makes identity unprovable and this method REFUSES. There is
   * deliberately no fallback and no backfill: the only honest value for a destroyed key's creation
   * time is none.
   *
   * <p>After a <b>completed</b> erasure (key gone, 404) this method REFUSES with {@link
   * CryptoOperationException}: Vault's transit engine owns the ciphertext wire format, so this
   * backend cannot discriminate destroyed key generations the way the postgres/filesystem backends
   * do (a re-minted transit key restarts at {@code vault:v1}, indistinguishable from the destroyed
   * generation's blobs). Re-register the returning person under a NEW subject id instead.
   *
   * <p><b>Fails closed on every unprovable case</b>: Vault unreachable, a non-200/404 probe status,
   * an unparseable or absent per-version creation time, a tombstone carrying no key-identity
   * evidence, and a {@link ForgottenSubjectStore} that does not implement {@code keyCreatedAt} (the
   * SPI default) all refuse and keep the tombstone — the degradation costs only revival
   * convenience, never erasure safety. A failure to READ either the evidence or the tombstone's
   * {@code forgotten_at} propagates rather than being read as "no evidence".
   *
   * <p><b>Concurrent-forget verification, BEFORE the lift:</b> a {@code deleteKey} in flight
   * commits its tombstone BEFORE issuing the transit DELETE, so the identity probe can observe the
   * still-live original while that DELETE is about to land; lifting the tombstone would then leave
   * a completed erasure with no tombstone — the next encrypt would silently upsert a fresh key
   * (ungated resurrection + the same poison). This method therefore re-reads the key as its last
   * act before the decision becomes durable, and refuses if it has vanished. <b>The tombstone is
   * lifted last, and it is the ONLY durable write this method makes</b>.
   *
   * <p><b>Every crash converges, because there is no intermediate state to crash into.</b> A crash
   * before the lift leaves the tombstone and its evidence exactly as the erasure wrote them: the
   * subject stays gated, and a rerun re-probes and re-decides from scratch. A crash after the lift
   * means the revival completed — correctly, because the key was confirmed live microseconds
   * earlier — and a rerun agrees with it (an idempotent no-op). The earlier ordering ran this
   * verification AFTER the lift and restored the tombstone when the key had vanished; it detected
   * exactly the same interleavings (the undetected window is "the DELETE lands after this
   * verification" in both orderings, because this is the last observation in both) but opened a
   * window in which the subject was UNGATED and the decision half-made, and a process killed there
   * left a COMPLETED erasure with NO tombstone that a rerun could neither repair nor detect ({@code
   * isForgotten} was {@code false}, so {@code reinstate} returned an idempotent no-op).
   *
   * <p><b>ONE residual remains</b>, documented in {@code gdpr-erasure.md}: a transit DELETE landing
   * AFTER the verification still escapes detection, leaving a completed erasure with no tombstone.
   * No local ordering can close it — that would need a transaction spanning Vault and the tombstone
   * store. It is not self-healing, since a rerun then sees {@code isForgotten == false}. The
   * standing remedy is the one the class javadoc has always given: <b>re-run the idempotent {@code
   * deleteKey} whenever an erasure may be concurrent or crashed</b>; the WARN this method logs
   * immediately before the lift is the operator's breadcrumb.
   *
   * <p>Reinstating a never-forgotten subject remains an idempotent no-op with no Vault traffic.
   */
  @Override
  public void reinstate(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (!forgottenSubjects.isForgotten(subjectId)) {
      return; // idempotent no-op — nothing to lift, nothing to probe
    }
    TransitKeyProbe probe = probeTransitKey(subjectId);
    if (!probe.exists()) {
      throw new CryptoOperationException(
          "Refusing to reinstate subject "
              + subjectId.redacted()
              + ": the erasure COMPLETED (the Vault transit key was destroyed). A re-minted"
              + " transit key cannot decrypt the subject's pre-erasure ciphertext, and the vault"
              + " backend cannot discriminate destroyed key generations — after the first new"
              + " encrypt, every pre-erasure blob would fail decryption loudly (blocking replay"
              + " and rebuilds) instead of reading as [REDACTED]. Re-register this person under a"
              + " new subject id. (Only a CRASHED erasure — the ORIGINAL transit key still live —"
              + " is reinstatable on this backend; re-run deleteKey first if unsure, then use a"
              + " new subject id.)");
    }
    // The key exists — prove it IS the key this erasure destroyed, not residue left by an encrypt
    // racing the erasure.
    // The proof is an EQUALITY between two instants
    // that BOTH came from Vault — the creation time deleteKey recorded with the tombstone, and the
    // creation time the live key reports now. No clock of this process's and none of the tombstone
    // store's appears in it, so no clock disagreement anywhere can turn a refusal into an
    // acceptance. (The previous proof compared the key's Vault-measured AGE against the tombstone's
    // age — and the tombstone's age spans the store's clock, the JDBC store's Postgres NOW(), and
    // this process's. A store clock running AHEAD of ours under-measures that age, which is exactly
    // the direction that LOOSENS the check: with 3 minutes of ordinary NTP drift a post-forget
    // residue key passed it and was revived. Only its gross form — a forgotten_at in this
    // process's future — was detectable, and only when the skew exceeded the whole elapsed time.)
    // A live key that was destroyed and re-created reports the re-creation instant, so equality
    // holds only for the key that survived; deleteKey additionally refuses to record evidence for
    // a key that does not out-age the erasure by revivalIdentityMargin ON VAULT'S CLOCK, which is
    // what stops a re-creation inside one creation-time granule from colliding.
    java.time.Instant recordedKeyCreatedAt = forgottenSubjects.keyCreatedAt(subjectId).orElse(null);
    java.time.Instant liveKeyCreatedAt = probe.oldestVersionCreatedAt();
    // The recorded instant round-tripped through a microsecond-resolution TIMESTAMPTZ; truncate
    // the live one the same way so a nanosecond-precision creation_time cannot make a key fail to
    // match itself.
    java.time.Instant liveTruncated =
        liveKeyCreatedAt == null
            ? null
            : liveKeyCreatedAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS);
    boolean provenOriginal =
        recordedKeyCreatedAt != null
            && liveTruncated != null
            && liveTruncated.equals(
                recordedKeyCreatedAt.truncatedTo(java.time.temporal.ChronoUnit.MICROS));
    if (!provenOriginal) {
      // forgotten_at is diagnostic context only — never a term of the proof above. A read failure
      // here throws and reinstate fails closed, which is the correct direction.
      java.time.Instant forgottenAt = forgottenSubjects.forgottenAt(subjectId).orElse(null);
      String unprovenBecause;
      if (recordedKeyCreatedAt == null) {
        unprovenBecause =
            "that erasure recorded no key-identity evidence — the ForgottenSubjectStore does not"
                + " implement keyCreatedAt(subjectId), or the erasure could not read the key from"
                + " Vault (it WARNs the reason at the time)";
      } else if (liveTruncated == null) {
        unprovenBecause = "the live key's per-version creation time could not be read from Vault";
      } else {
        unprovenBecause =
            "the live key was created at "
                + liveTruncated
                + " but the erasure destroyed a key created at "
                + recordedKeyCreatedAt
                + " — both on Vault's own clock, so this is a DIFFERENT key: the residue of an"
                + " encrypt that raced the erasure after the original was destroyed";
      }
      throw new CryptoOperationException(
          "Refusing to reinstate subject "
              + subjectId.redacted()
              + " (erased at "
              + forgottenAt
              + "): a Vault transit key exists but cannot be proven to be the key that erasure"
              + " destroyed ("
              + unprovenBecause
              + "). Reviving a post-erasure key would make every pre-erasure blob fail decryption"
              + " loudly (blocking replay and rebuilds) instead of reading as [REDACTED]. Re-run"
              + " the idempotent deleteKey to sweep any residue, then re-register this person"
              + " under a new subject id.");
    }
    // Crashed-erasure residue, identity proven: tombstone committed, the transit DELETE never
    // landed. Clearing the tombstone returns the original key — and its ciphertext — to service
    // with no generation mismatch (the documented revival).
    //
    // CONCURRENT-FORGET VERIFICATION, BEFORE THE LIFT — DO NOT REORDER.
    // A deleteKey in flight commits its tombstone
    // BEFORE issuing the transit DELETE, so the identity probe above can observe the still-live
    // ORIGINAL while that DELETE is about to land. This second read is the last look at Vault
    // before the decision becomes durable.
    //
    // It used to run AFTER the lift, restoring the tombstone when the key had vanished. That
    // detected exactly the same interleavings — the undetected window is "the DELETE lands after
    // this read" either way, because this read is the last observation in both orderings — but it
    // opened a window in which the subject was UNGATED and the decision half-made. A process
    // killed there (OOM, pod eviction, SIGKILL) left a COMPLETED erasure with NO tombstone: the
    // next encrypt passed the pre-check and transit silently upserted a fresh key for an erased
    // person, and a rerun could neither repair nor DETECT it, because isForgotten was false and
    // reinstate returned an idempotent no-op.
    //
    // With the lift last, reinstate performs exactly ONE durable write and there is no
    // intermediate state to crash into: a crash before it leaves the tombstone (and its evidence)
    // untouched and the rerun re-decides from scratch; a crash after it means the revival
    // completed, which is correct — the key was confirmed alive microseconds earlier. It also
    // removes the restore write entirely, and with it the fail-OPEN case where that write itself
    // failed and the need to sweep a key an
    // encrypt could slip into the cleared window — the subject is never ungated, so no such
    // encrypt can succeed.
    boolean keyStillExists;
    try {
      keyStillExists = probeTransitKey(subjectId).exists();
    } catch (CryptoOperationException e) {
      throw new CryptoOperationException(
          "Reinstate of subject "
              + subjectId.redacted()
              + " could not verify the transit key still exists (the probe failed), so the"
              + " tombstone was NOT lifted (fail closed). Retry the reinstate once Vault is"
              + " reachable — the revival is safe to re-run.",
          e);
    }
    if (!keyStillExists) {
      throw new CryptoOperationException(
          "Refusing to reinstate subject "
              + subjectId.redacted()
              + ": a concurrent deleteKey completed while this reinstate was in flight (the"
              + " transit key vanished between the identity probe and the verification). The"
              + " erasure stands and its tombstone was never lifted. Re-register this person under"
              + " a new subject id.");
    }
    // The single durable write. The WARN is the operator's breadcrumb for the ONE residual this
    // ordering cannot close: a transit DELETE landing after the verification above, which no
    // amount of local ordering can detect without a transaction spanning Vault and the tombstone
    // store.
    log.warn(
        "Lifting the GDPR erasure tombstone for subject {} (crashed-erasure revival, key identity"
            + " proven and the key re-verified as still live). If a concurrent deleteKey's transit"
            + " DELETE lands after that verification, this subject ends up erased with no"
            + " tombstone; re-run the idempotent deleteKey whenever an erasure may have been"
            + " concurrent.",
        subjectId.redacted());
    forgottenSubjects.reinstate(subjectId);
  }

  /**
   * Result of the transit key read used by {@link #reinstate(SubjectId)}: whether the key exists, —
   * when it does — the creation instant of its OLDEST version, and VAULT's own view of "now" taken
   * from the same response. Either instant is {@code null} when Vault's response carried no
   * parseable value (callers must fail closed on that).
   *
   * <p>Both instants come from the SAME response and therefore the SAME clock, which is what lets
   * {@link #reinstate(SubjectId)} measure the key's age without importing Vault's absolute clock
   * offset into the comparison.
   */
  private record TransitKeyProbe(
      boolean exists,
      java.time.Instant oldestVersionCreatedAt,
      java.time.Instant vaultServerTime) {}

  /**
   * VAULT's own "now", read from the HTTP {@code Date} response header (Go's net/http emits it on
   * every response, and the transit key read is pinned against real Vault by {@code
   * VaultCryptoEngineIntegrationTest}). {@code null} when absent or unparseable — including any
   * response implementation that does not expose headers — so callers fail closed rather than
   * silently falling back to a cross-clock comparison.
   */
  private static java.time.Instant vaultServerTime(HttpResponse<String> response) {
    try {
      return response
          .headers()
          .firstValue("Date")
          .map(
              date ->
                  java.time.Instant.from(
                      java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.parse(date)))
          .orElse(null);
    } catch (RuntimeException _) {
      return null;
    }
  }

  /**
   * Reads the subject's transit key ({@code GET /v1/<mount>/keys/<name>}): {@code 200} → exists
   * (with the oldest version's creation time parsed from {@code data.keys}), definitive {@code 404}
   * → absent, anything else (token expiry, outage) → {@link CryptoOperationException} — a live key
   * must never be misreported as absent, and {@link #reinstate(SubjectId)} must not treat "unknown"
   * as either answer.
   *
   * <p>{@code data.keys} maps version → creation time in two shapes across Vault versions/key
   * types: a bare UNIX-epoch-seconds number, or an object with an RFC-3339 {@code creation_time}
   * string. Both are parsed; any version with an unrecognizable value makes the overall answer
   * "unknown" ({@code null}) rather than silently ignoring it — a partial minimum could misidentify
   * residue as the original. The exact wire shape is pinned against real Vault by {@code
   * VaultCryptoEngineIntegrationTest}.
   */
  private TransitKeyProbe probeTransitKey(SubjectId subjectId) {
    String url = vaultAddress + "/v1/" + engineMount + "/keys/" + keyName(subjectId.value());
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
              .uri(URI.create(url))
              .header("X-Vault-Token", token)
              .timeout(requestTimeout)
              .GET()
              .build();
      HttpResponse<String> response = send(request);
      if (response.statusCode() == 200) {
        return new TransitKeyProbe(
            true, oldestVersionCreation(response.body()), vaultServerTime(response));
      }
      if (response.statusCode() == 404) {
        return new TransitKeyProbe(false, null, null);
      }
      throw new CryptoOperationException(
          "Vault key lookup failed for subject: "
              + subjectId.redacted()
              + " — HTTP "
              + response.statusCode());
    } catch (CryptoOperationException e) {
      throw e;
    } catch (Exception e) {
      throw new CryptoOperationException(
          "Vault key lookup failed for subject: " + subjectId.redacted(), e);
    }
  }

  /**
   * The minimum creation instant across {@code data.keys} versions, or {@code null} when the map is
   * absent, empty, or any entry's creation time is unrecognizable (see {@link
   * #probeTransitKey(SubjectId)} for why partial answers are refused).
   */
  private static java.time.Instant oldestVersionCreation(String responseBody) {
    if (responseBody == null || responseBody.isBlank()) {
      return null;
    }
    JsonNode root;
    try {
      root = MAPPER.readTree(responseBody);
    } catch (IOException _) {
      return null;
    }
    JsonNode keys = root.path("data").path("keys");
    if (!keys.isObject() || keys.isEmpty()) {
      return null;
    }
    java.time.Instant oldest = null;
    for (var it = keys.properties().iterator(); it.hasNext(); ) {
      JsonNode version = it.next().getValue();
      java.time.Instant created = parseCreationTime(version);
      if (created == null) {
        return null; // one unreadable version poisons the minimum — fail closed upstream
      }
      if (oldest == null || created.isBefore(oldest)) {
        oldest = created;
      }
    }
    return oldest;
  }

  /** Parses one {@code data.keys} entry's creation time; {@code null} when unrecognizable. */
  private static java.time.Instant parseCreationTime(JsonNode version) {
    try {
      if (version.isNumber()) {
        return java.time.Instant.ofEpochSecond(version.asLong());
      }
      JsonNode creationTime = version.isObject() ? version.path("creation_time") : version;
      if (creationTime.isTextual()) {
        return java.time.OffsetDateTime.parse(creationTime.asText()).toInstant();
      }
      return null;
    } catch (java.time.format.DateTimeParseException _) {
      return null;
    }
  }

  /** Whether the subject's transit key currently exists — see {@link #probeTransitKey}. */
  private boolean transitKeyExists(SubjectId subjectId) {
    return probeTransitKey(subjectId).exists();
  }

  @Override
  public java.util.Set<String> requiredCryptoTables() {
    // Vault stores keys in its transit engine (no key table); only a durable, DB-backed
    // tombstone store needs the forgotten_subjects table provisioned/validated.
    return forgottenSubjects.requiresDatabaseSchema()
        ? java.util.Set.of("forgotten_subjects")
        : java.util.Set.of();
  }

  @Override
  public boolean isKeyAvailable(SubjectId subjectId) {
    if (subjectId == null) {
      throw new IllegalArgumentException("subjectId must not be null");
    }
    if (forgottenSubjects.isForgotten(subjectId)) {
      // A crypto-shredded subject has no usable key, regardless of Vault state.
      return false;
    }
    // 403 (token expiry), 5xx, ... throw — the key may well exist. Never report it as absent:
    // callers may treat "no key" as "already crypto-shredded".
    return transitKeyExists(subjectId);
  }

  /**
   * Sends the request, retrying transient failures: connection errors ({@link IOException},
   * including timeouts) and HTTP 429/5xx responses. Backoff doubles per attempt starting at {@code
   * retryDelay}, capped at {@link #MAX_RETRY_BACKOFF} (see {@link #retryBackoff}). After exhausting
   * retries the last retryable response is returned (callers raise their operation-specific error)
   * or the last {@link IOException} is rethrown. Never returns {@code null}.
   */
  private HttpResponse<String> send(HttpRequest request) throws IOException {
    IOException lastFailure = null;
    HttpResponse<String> lastResponse = null;
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      try {
        if (attempt > 0) {
          Thread.sleep(retryBackoff(retryDelay, attempt));
        }
        HttpResponse<String> response =
            httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (!isRetryableStatus(response.statusCode())) {
          return response;
        }
        lastResponse = response;
        lastFailure = null;
      } catch (IOException e) {
        lastFailure = e;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new CryptoOperationException("Interrupted during Vault request", e);
      }
    }
    if (lastFailure != null) {
      throw lastFailure;
    }
    // build() rejects maxRetries < 0, so the loop ran at least once, and an iteration that did not
    // return, throw or record lastFailure recorded a non-null lastResponse.
    return Objects.requireNonNull(lastResponse, "unreachable: maxRetries >= 0");
  }

  /**
   * Ceiling on one backoff sleep in {@link #send}: the doubling stops growing here, so a large
   * {@code maxRetries} keeps retrying at this cadence instead of sleeping for hours or years.
   */
  static final Duration MAX_RETRY_BACKOFF = Duration.ofSeconds(30);

  /**
   * Backoff before the given retry (1-based): {@code retryDelay * 2^(retry-1)}, capped at {@link
   * #MAX_RETRY_BACKOFF}. Computed on doubles in nanosecond precision, as {@code RetryPolicy} does,
   * so it can neither overflow nor shift by a wrapped distance: the former {@code long} shift slept
   * for years from about the 30th retry and, from about the 57th, overflowed to a negative delay or
   * wrapped its shift distance back to a short one. A {@code retryDelay} at or above the cap is
   * used as configured for every retry: the cap stops the doubling, it never shortens what the
   * operator asked for.
   */
  static Duration retryBackoff(Duration retryDelay, int retry) {
    if (retry < 1) {
      throw new IllegalArgumentException("retry must be >= 1, was " + retry);
    }
    if (retryDelay.compareTo(MAX_RETRY_BACKOFF) >= 0) {
      return retryDelay;
    }
    // Exponent bounded so a zero delay stays 0 (0 * +Infinity would be NaN); 2^62 nanoseconds is
    // far past the cap for any delay of at least 1ns.
    double nanos = retryDelay.toNanos() * Math.pow(2.0, Math.min(retry - 1, 62));
    return nanos < MAX_RETRY_BACKOFF.toNanos()
        ? Duration.ofNanos(Math.round(nanos))
        : MAX_RETRY_BACKOFF;
  }

  private static boolean isRetryableStatus(int status) {
    return status == 429 || status >= 500;
  }

  /**
   * Derives the Vault transit key name by hashing the subjectId (SHA-256 of its UTF-8 bytes, hex).
   *
   * <p>The subjectId is never embedded verbatim in the key name: it becomes part of the request
   * URL, so a value containing {@code '/'}, {@code '?'}, {@code '#'} or dot segments would redirect
   * the call to a different Vault path with this engine's token privileges (e.g. {@code deleteKey}
   * targeting another key). Hashing also keeps subject IDs that are themselves PII (emails,
   * usernames) out of Vault's key listing and audit logs, and mirrors {@code
   * FileSystemCryptoEngine}'s key naming.
   */
  private String keyName(String subjectValue) {
    try {
      var md = MessageDigest.getInstance("SHA-256");
      byte[] digest = md.digest(subjectValue.getBytes(StandardCharsets.UTF_8));
      return "subject-" + HexFormat.of().formatHex(digest);
    } catch (Exception e) {
      // The raw subjectValue may itself be PII — never embed it in the message. The
      // digest that would redact it is exactly what just failed, so omit the value entirely.
      throw new CryptoOperationException(
          "SHA-256 hashing failed while deriving the Vault transit key name for a subject", e);
    }
  }

  /**
   * Substring Vault transit's decrypt handler uses in its error message when the named encryption
   * key does not exist (checked case-insensitively). Vault also returns HTTP 400 for unrelated
   * causes — invalid/malformed ciphertext, a bad key version, general validation errors — which
   * must NOT be treated as "key not found": doing so would let corrupt or tampered ciphertext (or a
   * malformed request) silently masquerade as a GDPR forget.
   */
  private static final String KEY_NOT_FOUND_MARKER = "encryption key not found";

  /**
   * Substring Vault transit's key-delete handler uses when the named key does not exist ({@code
   * "error deleting policy <name>: could not delete key; not found"}) — observed against Vault 1.18
   * and pinned against a real Vault by {@code VaultCryptoEngineIntegrationTest}. Deliberately
   * distinct from the delete endpoint's other 400 cause ({@code "deletion is not allowed for this
   * key"}), which must keep failing loudly.
   */
  private static final String DELETE_KEY_NOT_FOUND_MARKER = "could not delete key; not found";

  /**
   * Substring of Vault transit's key-delete error for a key whose config has {@code
   * deletion_allowed=false} ({@code "error deleting policy <name>: deletion is not allowed for this
   * key"}), pinned against a real Vault by {@code VaultCryptoEngineIntegrationTest}.
   */
  private static final String DELETION_NOT_ALLOWED_MARKER = "deletion is not allowed";

  /**
   * The error texts Vault transit returns (HTTP 400) for a DETERMINISTIC verdict on the ciphertext
   * ITSELF. {@code keysutil} prefixes every shape/version/base64/length rejection with {@code
   * "invalid ciphertext"} ({@code no prefix}, {@code wrong number of fields}, {@code version number
   * could not be decoded}, {@code could not decode base64}, {@code invalid ciphertext length}) and
   * may forward the AEAD failure text if the open itself fails ( unverified for current Vault, so
   * treated as "if forwarded" rather than a guaranteed shape — Go's {@code cipher: message
   * authentication failed} / {@code chacha20poly1305: message authentication failed} is the
   * historically observed text). All are deterministic for the blob.
   *
   * <p>NOT included on purpose, both REVERSIBLE and therefore left on the bare (blocks-and-retries)
   * side: the {@code min_decryption_version} policy refusal ({@code "ciphertext or signature
   * version is disallowed by policy (too old)"}), which an operator can relax; and {@code "version
   * is too new"} ({@link #TOO_NEW_MARKER}) — despite {@code keysutil} giving it the SAME {@code
   * "invalid ciphertext"} prefix as the truly-deterministic members above, a ciphertext version
   * newer than what THIS Vault node currently knows about is a node-local, time-bound fact (a
   * secondary lagging a primary's key rotation, a cluster restored from an older snapshot, key
   * material temporarily missing) that resolves itself without touching the blob — quarantining it
   * would be exactly the mistake the policy-refusal exclusion above already avoids for "too old".
   */
  private static final String INVALID_CIPHERTEXT_MARKER = "invalid ciphertext";

  /**
   * {@code keysutil}'s error text when a ciphertext's key version is newer than this Vault node's
   * latest known version for the key — reversible (see {@link #INVALID_CIPHERTEXT_MARKER}'s
   * javadoc), so excluded from {@link #isInvalidCiphertextError(String)} even though it shares the
   * {@code "invalid ciphertext"} prefix.
   */
  private static final String TOO_NEW_MARKER = "version is too new";

  private static final String AEAD_FAILURE_MARKER = "message authentication failed";

  /**
   * Determines whether a Vault HTTP 400 body positively indicates a missing encryption key, as
   * opposed to any other 400 cause (invalid ciphertext, bad key version, validation errors).
   *
   * <p>Errors on the safe side: if the body is missing, empty, or not the expected {@code
   * {"errors": [...]}} shape, this returns {@code false} so the caller surfaces a {@link
   * CryptoOperationException} rather than silently reporting the subject as forgotten.
   */
  private static boolean isKeyNotFoundError(String responseBody) {
    return errorsContain(responseBody, KEY_NOT_FOUND_MARKER);
  }

  /**
   * Does the 400 body positively identify a DETERMINISTIC verdict on the ciphertext itself —
   * excluding the reversible "version is too new" case, which shares the "invalid ciphertext"
   * prefix but must stay bare (see {@link #TOO_NEW_MARKER}).
   */
  private static boolean isInvalidCiphertextError(String responseBody) {
    return (errorsContain(responseBody, INVALID_CIPHERTEXT_MARKER)
            && !errorsContain(responseBody, TOO_NEW_MARKER))
        || errorsContain(responseBody, AEAD_FAILURE_MARKER);
  }

  /**
   * Determines whether a Vault HTTP 400 body positively indicates a DELETE of a nonexistent transit
   * key. Same safe-side parsing as {@link #isKeyNotFoundError(String)}: an absent, empty, or
   * unrecognized body returns {@code false}, so an unexplained 400 still fails loudly.
   */
  private static boolean isDeleteKeyNotFoundError(String responseBody) {
    return errorsContain(responseBody, DELETE_KEY_NOT_FOUND_MARKER);
  }

  /** True when the Vault error body's {@code errors} array has an entry containing the marker. */
  private static boolean errorsContain(String responseBody, String marker) {
    if (responseBody == null || responseBody.isBlank()) {
      return false;
    }
    JsonNode root;
    try {
      root = MAPPER.readTree(responseBody);
    } catch (IOException _) {
      return false;
    }
    JsonNode errors = root.path("errors");
    if (!errors.isArray()) {
      return false;
    }
    for (JsonNode error : errors) {
      if (error.isTextual() && error.asText().toLowerCase(java.util.Locale.ROOT).contains(marker)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Renders a Vault error response body for inclusion in an exception message: the joined {@code
   * errors} array when present, else the raw body, else a placeholder if the body itself is
   * missing/unparseable.
   */
  private static String firstErrorOrBody(String responseBody) {
    if (responseBody == null || responseBody.isBlank()) {
      return "(no response body)";
    }
    try {
      JsonNode root = MAPPER.readTree(responseBody);
      JsonNode errors = root.path("errors");
      if (errors.isArray() && !errors.isEmpty()) {
        StringBuilder sb = new StringBuilder();
        for (JsonNode error : errors) {
          if (!sb.isEmpty()) {
            sb.append("; ");
          }
          sb.append(error.isTextual() ? error.asText() : error.toString());
        }
        return sb.toString();
      }
    } catch (IOException _) {
      // fall through to raw body
    }
    return responseBody;
  }

  /** Extracts {@code data.<field>} from a Vault JSON response using a real JSON parser. */
  private static String readDataField(String json, String field) {
    JsonNode root;
    try {
      root = MAPPER.readTree(json);
    } catch (IOException e) {
      throw new CryptoOperationException("Invalid Vault response: not parseable JSON", e);
    }
    JsonNode value = root.path("data").path(field);
    if (!value.isTextual()) {
      throw new CryptoOperationException("Invalid Vault response: missing data." + field);
    }
    return value.asText();
  }

  public static final class Builder {
    private HttpClient httpClient;
    private String vaultAddress = "http://localhost:8200";
    private String token;
    private String engineMount = "transit";
    private Duration requestTimeout = Duration.ofSeconds(10);
    private int maxRetries = 2;
    private Duration retryDelay = Duration.ofMillis(200);
    // No silent default — build() fails closed unless a tombstone
    // store is supplied, so Vault erasure can never silently be non-terminal (a post-erasure
    // encrypt would otherwise resurrect the subject under a fresh Vault transit key).
    private ForgottenSubjectStore forgottenSubjects = null;
    private Duration revivalIdentityMargin = DEFAULT_REVIVAL_IDENTITY_MARGIN;

    public Builder httpClient(HttpClient client) {
      this.httpClient = client;
      return this;
    }

    /**
     * Vault server address. The default {@code http://localhost:8200} is a plain-HTTP dev
     * convenience — production must use an {@code https://} address, otherwise the Vault token and
     * every plaintext sent to the transit engine cross the network unencrypted.
     */
    public Builder vaultAddress(String address) {
      this.vaultAddress = address;
      return this;
    }

    /**
     * The Vault token sent with every request. Its policy must grant the capabilities listed at
     * {@link VaultCryptoEngine#deleteKey(SubjectId)}: without {@code update} on {@code
     * <mount>/keys/+/config}, a forget fails for every key that {@code encrypt} created.
     */
    public Builder token(String token) {
      this.token = token;
      return this;
    }

    public Builder engineMount(String mount) {
      this.engineMount = mount;
      return this;
    }

    /** Per-request timeout. Default 10s. */
    public Builder requestTimeout(Duration timeout) {
      this.requestTimeout = timeout;
      return this;
    }

    /** Retries after the first attempt for connection errors and HTTP 429/5xx. Default 2. */
    public Builder maxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    /**
     * Initial backoff between attempts; doubles per retry up to a 30s ceiling. A delay above the
     * ceiling is used as configured for every retry. Default 200ms.
     */
    public Builder retryDelay(Duration retryDelay) {
      this.retryDelay = retryDelay;
      return this;
    }

    /**
     * Sets the crypto-shredding tombstone store used for terminal GDPR erasure. <b>Required :</b>
     * there is no default — {@link #build()} fails closed if none is supplied. Vault's transit
     * engine re-creates a deleted key on the next {@code encrypt}, so without a durable tombstone a
     * forgotten subject's NEW PII would be silently re-encrypted. Use the durable {@code
     * JdbcForgottenSubjectStore} (in {@code streamrune-postgres-crypto}) in production so erasure
     * survives restarts; {@link InMemoryForgottenSubjectStore} is acceptable only for tests /
     * single-process ephemeral use.
     *
     * @param forgottenSubjects the tombstone store; must not be null
     */
    public Builder forgottenSubjectStore(ForgottenSubjectStore forgottenSubjects) {
      if (forgottenSubjects == null) {
        throw new IllegalArgumentException("forgottenSubjectStore must not be null");
      }
      this.forgottenSubjects = forgottenSubjects;
      return this;
    }

    /**
     * Margin by which the transit key must OUT-AGE the erasure — measured wholly on VAULT's own
     * clock at {@link VaultCryptoEngine#deleteKey(SubjectId)} time — before that erasure records
     * the key-identity evidence {@link VaultCryptoEngine#reinstate(SubjectId)} needs. Default 60s.
     *
     * <p><b>This is a SAFETY parameter, not a convenience one</b>. Both instants it compares come
     * from Vault, so no other host's clock enters the decision. What the margin absorbs is Vault's
     * epoch-second creation-time granularity and request latency — which is what makes the later
     * equality check an unambiguous identity proof, since a key re-created within one granule of
     * the original would otherwise report the same creation time. RAISING it is always safe — it
     * only makes the revival stricter (fewer erasures record evidence). LOWERING it narrows the gap
     * an encrypt that raced the erasure needs in order to be mistaken for the original, and
     * reviving that residue poisons every pre-erasure blob. Lower it only where Vault's
     * creation-time resolution is provably fine, e.g. in tests.
     *
     * @param margin a positive duration
     */
    public Builder revivalIdentityMargin(Duration margin) {
      this.revivalIdentityMargin = margin;
      return this;
    }

    public VaultCryptoEngine build() {
      if (httpClient == null) throw new IllegalStateException("httpClient is required");
      if (token == null || token.isBlank()) throw new IllegalStateException("token is required");
      if (engineMount == null || engineMount.isBlank())
        throw new IllegalStateException("engineMount is required");
      if (requestTimeout == null || requestTimeout.isZero() || requestTimeout.isNegative())
        throw new IllegalArgumentException("requestTimeout must be positive");
      if (maxRetries < 0) throw new IllegalArgumentException("maxRetries must be non-negative");
      if (revivalIdentityMargin == null
          || revivalIdentityMargin.isZero()
          || revivalIdentityMargin.isNegative())
        throw new IllegalArgumentException("revivalIdentityMargin must be positive");
      if (retryDelay == null || retryDelay.isNegative())
        throw new IllegalArgumentException("retryDelay must be non-negative");
      if (forgottenSubjects == null) {
        // Fail-closed: refuse to build an engine whose GDPR erasure
        // would be non-terminal. Vault transit re-creates a deleted key on the next encrypt, so an
        // erased subject stays forgotten only as long as its tombstone does.
        throw new IllegalStateException(
            "Vault GDPR erasure requires a persistent ForgottenSubjectStore: Vault's transit engine"
                + " re-creates a deleted key on the next encrypt, so without a durable tombstone a"
                + " forgotten subject's new PII would be silently re-encrypted. Configure a"
                + " persistent ForgottenSubjectStore for durable, terminal GDPR erasure on Vault"
                + " (e.g. JdbcForgottenSubjectStore backed by your DataSource) via"
                + " forgottenSubjectStore(...); InMemoryForgottenSubjectStore is for tests only and"
                + " must never be the production default.");
      }
      return new VaultCryptoEngine(this);
    }
  }
}
