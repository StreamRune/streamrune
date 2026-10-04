package org.streamrune.vault;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.streamrune.core.crypto.CryptoMappingException;
import org.streamrune.core.crypto.KeyNotFoundException;
import org.streamrune.core.types.SubjectId;
import org.streamrune.crypto.InMemoryForgottenSubjectStore;

class VaultCryptoEngineTest {

  private HttpClient httpClient;
  private InMemoryForgottenSubjectStore store;
  private VaultCryptoEngine engine;

  @BeforeEach
  void setUp() {
    httpClient = mock(HttpClient.class);
    store = new InMemoryForgottenSubjectStore();
    engine =
        VaultCryptoEngine.builder()
            .httpClient(httpClient)
            .vaultAddress("http://localhost:8200")
            .token("test-token")
            .engineMount("transit")
            // Tiny backoff: failure-path tests exhaust retries without slowing the suite.
            .retryDelay(java.time.Duration.ofMillis(1))
            .forgottenSubjectStore(store)
            .build();
  }

  // ---------------------------------------------------------------------------
  // Reinstate must refuse after a COMPLETED
  // erasure. Vault's transit engine owns the ciphertext wire format, so this backend cannot
  // discriminate destroyed key generations the way postgres/filesystem do (a re-minted transit
  // key restarts at vault:v1, indistinguishable from the destroyed generation's blobs). Clearing
  // the tombstone would arm a trap: the first re-encrypt upserts a fresh transit key, and from
  // then on every PRE-forget blob decrypts as HTTP 400 "invalid ciphertext" ->
  // CryptoOperationException instead of the replay-tolerant KeyNotFoundException -> [REDACTED] —
  // the returning subject's streams and every full rebuild become permanently unloadable.
  // ---------------------------------------------------------------------------

  @Test
  void reinstateRefusesAfterCompletedErasure_transitKeyGone() throws Exception {
    var subject = SubjectId.of("vault-completed-erasure");
    store.forget(subject);
    HttpResponse<String> keyGone = mock(HttpResponse.class);
    when(keyGone.statusCode()).thenReturn(404);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(keyGone);

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.reinstate(subject));

    assertTrue(
        thrown.getMessage().contains("new subject id"),
        "the refusal must tell the operator to re-register under a NEW subject id: "
            + thrown.getMessage());
    assertTrue(
        store.isForgotten(subject),
        "a refused reinstate must leave the subject terminally gated by its tombstone");
  }

  @Test
  void reinstateAllowsCrashedErasureRevival_transitKeyStillLive() throws Exception {
    // The documented crashed-deleteKey residue: tombstone committed, the transit DELETE never
    // landed. The pre-erasure key is intact, so clearing the tombstone returns it — and its
    // ciphertext — to service with no generation mismatch. This documented recovery must survive.
    // Strengthened the probe from bare existence to
    // key IDENTITY, and a later change made that proof an EQUALITY
    // between two Vault-sourced creation times: the one deleteKey recorded with the tombstone and
    // the one the live key reports now. So the erasure must run through the engine — a tombstone
    // conjured straight into the store carries no evidence and (correctly) refuses.
    var subject = SubjectId.of("vault-crashed-erasure");
    java.time.Instant created = java.time.Instant.ofEpochSecond(1_000_000_000L); // 2001
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              // The crash: the transit DELETE never lands, so the ORIGINAL key survives.
              return "DELETE".equals(request.method())
                  ? status(500)
                  : keyRead(created, java.time.Instant.now());
            });
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.deleteKey(subject));
    assertTrue(store.isForgotten(subject), "the tombstone commits before the transit DELETE");

    engine.reinstate(subject);

    assertFalse(store.isForgotten(subject), "crashed-erasure revival must lift the tombstone");
  }

  // ---------------------------------------------------------------------------
  // "transit key exists" has a SECOND cause this
  // codebase documents — the encrypt-race resurrection whose best-effort cleanup
  // failed (bestEffortDeleteResurrectedKey never throws; its ERROR-log remediation is advisory).
  // That key is NOT the original: it was upserted fresh AFTER the erasure destroyed the original,
  // so reviving it serves the subject a DIFFERENT key and every PRE-erasure blob decrypts as a
  // hard HTTP 400 — the exact poison the completed-erasure refusal exists to prevent. The probe
  // must discriminate key IDENTITY (oldest version creation_time vs the tombstone's
  // forgotten_at), fail CLOSED when identity is unprovable, and re-check for a concurrently
  // completing erasure after lifting the tombstone.
  // ---------------------------------------------------------------------------

  @Test
  void reinstateRefusesResidueKey_createdAfterTheForget() throws Exception {
    // The plain case, no clock skew anywhere: the erasure destroys the ORIGINAL key and records
    // its creation time; an encrypt racing the erasure upserts a FRESH key whose best-effort sweep
    // failed. The live key exists but is a DIFFERENT key, so the revival must be refused.
    var subject = SubjectId.of("vault-a11-residue");
    java.time.Instant vaultNow = java.time.Instant.now();
    var liveKeyCreatedAt =
        new java.util.concurrent.atomic.AtomicReference<>(
            vaultNow.minus(java.time.Duration.ofDays(365)));
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if ("DELETE".equals(request.method())) {
                liveKeyCreatedAt.set(vaultNow);
                return status(204);
              }
              return keyRead(liveKeyCreatedAt.get(), vaultNow);
            });
    engine.deleteKey(subject);

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.reinstate(subject));

    assertTrue(
        thrown.getMessage().contains("new subject id"),
        "the refusal must direct the operator to a new subject id: " + thrown.getMessage());
    assertTrue(
        store.isForgotten(subject),
        "a refused reinstate must leave the subject terminally gated by its tombstone");
  }

  @Test
  void reinstateFailsClosedWhenStoreDoesNotTrackKeyIdentityEvidence() throws Exception {
    // A third-party ForgottenSubjectStore that has not overridden forget(subjectId, keyCreatedAt)
    // / keyCreatedAt() (the safe defaults drop and report nothing): key identity is unprovable, so
    // reinstate must refuse — the degradation costs only the crashed-erasure revival convenience,
    // never erasure safety. The same shape as a tombstone recorded without evidence.
    var forgotten = new java.util.concurrent.ConcurrentSkipListSet<String>();
    var evidenceBlindStore =
        new org.streamrune.crypto.ForgottenSubjectStore() {
          @Override
          public void forget(SubjectId subjectId) {
            forgotten.add(subjectId.value());
          }

          @Override
          public boolean isForgotten(SubjectId subjectId) {
            return forgotten.contains(subjectId.value());
          }

          @Override
          public void reinstate(SubjectId subjectId) {
            forgotten.remove(subjectId.value());
          }
        };
    var evidenceBlindEngine =
        VaultCryptoEngine.builder()
            .httpClient(httpClient)
            .vaultAddress("http://localhost:8200")
            .token("test-token")
            .engineMount("transit")
            .retryDelay(java.time.Duration.ofMillis(1))
            .forgottenSubjectStore(evidenceBlindStore)
            .build();
    var subject = SubjectId.of("vault-evidence-blind-store");
    evidenceBlindStore.forget(subject);
    HttpResponse<String> oldKey = mock(HttpResponse.class);
    when(oldKey.statusCode()).thenReturn(200);
    when(oldKey.body()).thenReturn("{\"data\":{\"keys\":{\"1\":1000000000}}}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(oldKey);

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> evidenceBlindEngine.reinstate(subject));
    assertTrue(
        evidenceBlindStore.isForgotten(subject),
        "an identity-unprovable reinstate must keep the tombstone");
  }

  @Test
  void reinstateFailsClosedWhenKeyCreationTimeIsUnreadable() throws Exception {
    // The erasure recorded good evidence, but the LIVE key's 200 carries no parseable per-version
    // creation time: existence is proven, identity is not — same fail-closed rule as an
    // unreachable Vault.
    var subject = SubjectId.of("vault-unreadable-creation");
    java.time.Instant vaultNow = java.time.Instant.now();
    var readable = new java.util.concurrent.atomic.AtomicBoolean(true);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if ("DELETE".equals(request.method())) {
                readable.set(false); // the crash: the key survives, but its body turns opaque
                return status(500);
              }
              if (readable.get()) {
                return keyRead(vaultNow.minus(java.time.Duration.ofDays(365)), vaultNow);
              }
              HttpResponse<String> opaque = mock(HttpResponse.class);
              when(opaque.statusCode()).thenReturn(200);
              when(opaque.body()).thenReturn("{\"data\":{\"name\":\"aes256-gcm96\"}}");
              when(opaque.headers()).thenReturn(vaultDate(vaultNow));
              return opaque;
            });
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.deleteKey(subject));
    assertTrue(store.keyCreatedAt(subject).isPresent(), "the erasure DID record evidence");

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.reinstate(subject));
    assertTrue(store.isForgotten(subject), "unprovable identity must keep the tombstone");
  }

  @Test
  void reinstateRefusesWhenTheKeyVanishesBetweenTheIdentityProofAndTheReProbe() throws Exception {
    // Second interleaving: the identity probe lands between a concurrent deleteKey's tombstone
    // commit and its transit DELETE — the still-live ORIGINAL key passes the identity proof, and
    // the DELETE then lands. Lifting the tombstone here would leave a COMPLETED erasure with no
    // tombstone, so the next encrypt would silently upsert a fresh key (ungated resurrection +
    // poison). reinstate must re-probe and, if the key vanished, refuse with the tombstone intact.
    var subject = SubjectId.of("vault-vanish-mid-reinstate");
    java.time.Instant vaultNow = java.time.Instant.now();
    java.time.Instant originalCreated = vaultNow.minus(java.time.Duration.ofDays(365));
    var keyReads = new java.util.concurrent.atomic.AtomicInteger();
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if ("DELETE".equals(request.method())) {
                return status(500); // this erasure crashes; the key survives it
              }
              // Reads 1 (the erasure's evidence probe) and 2 (reinstate's identity probe) see the
              // live original; by read 3 (the re-probe) the CONCURRENT deleteKey's DELETE landed.
              return keyReads.incrementAndGet() >= 3
                  ? status(404)
                  : keyRead(originalCreated, vaultNow);
            });
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.deleteKey(subject));

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.reinstate(subject));
    assertTrue(
        store.isForgotten(subject),
        "the concurrently completed erasure must stand — no tombstone-less erased state");
  }

  /**
   * An abrupt process death (OOM, pod eviction, SIGKILL). Deliberately an {@link Error}: {@code
   * probeTransitKey} catches {@link Exception}, so anything softer would be caught, wrapped and
   * handled instead of terminating the invocation the way a real kill does.
   */
  private static final class SimulatedProcessDeath extends Error {
    SimulatedProcessDeath() {
      super("simulated SIGKILL");
    }
  }

  @Test
  void aProcessDeathDuringTheReinstateVerificationLeavesTheErasureIntactAndRecoverable()
      throws Exception {
    // reinstate used to LIFT the tombstone and only then
    // re-probe, restoring it if the key had vanished. A process killed inside that window left a
    // COMPLETED erasure with NO tombstone: the next encrypt passed the pre-check and transit
    // silently upserted a fresh key for an erased person, and a rerun could neither repair nor
    // even DETECT it (isForgotten was false, so reinstate returned an idempotent no-op). The lift
    // is now the LAST step, after the verification, so no crash can produce that state.
    //
    // The kill is injected exactly in that window: at the verification key read, with a concurrent
    // deleteKey's transit DELETE landing right after it.
    var subject = SubjectId.of("vault-crash-during-verification");
    java.time.Instant vaultNow = java.time.Instant.now();
    java.time.Instant originalCreated = vaultNow.minus(java.time.Duration.ofDays(365));
    var keyReads = new java.util.concurrent.atomic.AtomicInteger();
    var concurrentDeleteLanded = new java.util.concurrent.atomic.AtomicBoolean();
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if ("DELETE".equals(request.method())) {
                return status(500); // this erasure crashes; the ORIGINAL key survives it
              }
              if ("POST".equals(request.method())) {
                HttpResponse<String> encrypted = mock(HttpResponse.class);
                when(encrypted.statusCode()).thenReturn(200);
                when(encrypted.body()).thenReturn("{\"data\":{\"ciphertext\":\"vault:v1:ct\"}}");
                return encrypted;
              }
              if (concurrentDeleteLanded.get()) {
                return status(404);
              }
              // Read 1 = the erasure's evidence probe. Read 2 = reinstate's identity probe.
              // Read 3 = the verification, and this process never survives it.
              if (keyReads.incrementAndGet() >= 3) {
                concurrentDeleteLanded.set(true); // the concurrent deleteKey completes meanwhile
                throw new SimulatedProcessDeath();
              }
              return keyRead(originalCreated, vaultNow);
            });
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.deleteKey(subject));

    assertThrows(SimulatedProcessDeath.class, () -> engine.reinstate(subject));

    // The whole point: the durable state the killed run left behind still gates the subject.
    assertTrue(
        store.isForgotten(subject),
        "a process killed during the reinstate verification must leave the erasure tombstone"
            + " INTACT — the lift is the last step, so there is no tombstone-less window to crash"
            + " in");
    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(subject, "pii".getBytes(StandardCharsets.UTF_8)),
        "and the erasure boundary holds: no silent re-encryption for an erased subject");

    // And the rerun CONVERGES rather than being blind to the state: the concurrent deleteKey
    // completed, so the key is gone and the erasure is refused as completed.
    var refusal =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.reinstate(subject));
    assertTrue(
        refusal.getMessage().contains("new subject id"),
        "the rerun must reach a decision, not an idempotent no-op: " + refusal.getMessage());
    assertTrue(store.isForgotten(subject), "and the erasure still stands");
  }

  @Test
  void aProcessDeathAtTheLiftItselfLeavesTheReinstateCOMPLETE_andTheRerunAgrees() throws Exception {
    // The crash point inside the NEW ordering: the verification passed, and the process dies as
    // the lift commits. That is the intended terminal state, not a hole — the key was confirmed
    // alive microseconds earlier, so the subject is legitimately reinstated. The rerun must agree
    // (an idempotent no-op) rather than trying to undo a decision that was correct.
    var subject = SubjectId.of("vault-crash-at-the-lift");
    java.time.Instant vaultNow = java.time.Instant.now();
    java.time.Instant originalCreated = vaultNow.minus(java.time.Duration.ofDays(365));
    var liftThenDie =
        new org.streamrune.crypto.ForgottenSubjectStore() {
          final InMemoryForgottenSubjectStore delegate = new InMemoryForgottenSubjectStore();

          @Override
          public void forget(SubjectId s) {
            delegate.forget(s);
          }

          @Override
          public void forget(SubjectId s, java.time.Instant keyCreatedAt) {
            delegate.forget(s, keyCreatedAt);
          }

          @Override
          public boolean isForgotten(SubjectId s) {
            return delegate.isForgotten(s);
          }

          @Override
          public void reinstate(SubjectId s) {
            delegate.reinstate(s); // the durable lift commits...
            throw new SimulatedProcessDeath(); // ...and the process dies before returning
          }

          @Override
          public java.util.Optional<java.time.Instant> forgottenAt(SubjectId s) {
            return delegate.forgottenAt(s);
          }

          @Override
          public java.util.Optional<java.time.Instant> keyCreatedAt(SubjectId s) {
            return delegate.keyCreatedAt(s);
          }
        };
    var crashingEngine = engineWith(liftThenDie);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              return "DELETE".equals(request.method())
                  ? status(500) // the erasure crashes; the ORIGINAL key survives
                  : keyRead(originalCreated, vaultNow);
            });
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> crashingEngine.deleteKey(subject));

    assertThrows(SimulatedProcessDeath.class, () -> crashingEngine.reinstate(subject));

    assertFalse(liftThenDie.isForgotten(subject), "the lift committed — the reinstate is COMPLETE");
    crashingEngine.reinstate(subject); // the rerun: an idempotent no-op, no Vault traffic
    assertFalse(liftThenDie.isForgotten(subject), "and the rerun agrees with it");
  }

  @Test
  void aRefusedReinstateWritesNothingToTheTombstoneStore_soNoStoreFailureCanUngateTheSubject()
      throws Exception {
    // Supersedes
    // By construction. The old ordering lifted the
    // tombstone and then RESTORED it when the key turned out to have vanished, so a store hiccup
    // in the milliseconds between those two writes left a completed erasure with NO tombstone —
    // fail-OPEN on erasure, and the earlier ordering could only make its MESSAGE unmistakable. With
    // the lift last there is no restore write at all, so that failure mode cannot occur: this
    // store THROWS on any write after the erasure's own, and the refusal is unaffected.
    var failOnAnyFurtherWrite =
        new org.streamrune.crypto.ForgottenSubjectStore() {
          final InMemoryForgottenSubjectStore delegate = new InMemoryForgottenSubjectStore();
          int writes;

          private void countWrite() {
            if (++writes > 1) {
              throw new org.streamrune.core.crypto.CryptoOperationException(
                  "Failed to record forgotten-subject tombstone");
            }
          }

          @Override
          public void forget(SubjectId s) {
            countWrite();
            delegate.forget(s);
          }

          @Override
          public void forget(SubjectId s, java.time.Instant keyCreatedAt) {
            countWrite();
            delegate.forget(s, keyCreatedAt);
          }

          @Override
          public boolean isForgotten(SubjectId s) {
            return delegate.isForgotten(s);
          }

          @Override
          public void reinstate(SubjectId s) {
            countWrite();
            delegate.reinstate(s);
          }

          @Override
          public java.util.Optional<java.time.Instant> forgottenAt(SubjectId s) {
            return delegate.forgottenAt(s);
          }

          @Override
          public java.util.Optional<java.time.Instant> keyCreatedAt(SubjectId s) {
            return delegate.keyCreatedAt(s);
          }
        };
    var engineWithFailingStore = engineWith(failOnAnyFurtherWrite);
    var subject = SubjectId.of("vault-restore-write-fails");
    java.time.Instant vaultNow = java.time.Instant.now();
    java.time.Instant originalCreated = vaultNow.minus(java.time.Duration.ofDays(365));
    var keyReads = new java.util.concurrent.atomic.AtomicInteger();
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if ("DELETE".equals(request.method())) {
                return status(500); // this erasure crashes; the key survives it
              }
              if (isKeyConfigWrite(request)) {
                return status(204); // the erasure enables deletion; not a key read
              }
              return keyReads.incrementAndGet() >= 3
                  ? status(404) // a CONCURRENT deleteKey's DELETE lands before the verification
                  : keyRead(originalCreated, vaultNow);
            });
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engineWithFailingStore.deleteKey(subject));

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engineWithFailingStore.reinstate(subject));

    assertTrue(
        thrown.getMessage().contains("never lifted"),
        "the refusal must be the concurrency refusal, not a store write error: "
            + thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains("new subject id"),
        "and must direct the operator to a new subject id: " + thrown.getMessage());
    assertEquals(
        1,
        failOnAnyFurtherWrite.writes,
        "a refused reinstate must write to the tombstone store ZERO times — the erasure's own"
            + " write is the only one");
    assertTrue(
        failOnAnyFurtherWrite.isForgotten(subject),
        "and the subject stays gated by the tombstone the erasure wrote");
  }

  @Test
  void reinstateFailsClosedWhenVaultProbeIsUnreachable() throws Exception {
    // If Vault cannot be reached, reinstate cannot prove the completed-vs-crashed distinction.
    // It must fail closed — keep the tombstone — rather than clear it on uncertainty and
    // possibly arm the destroyed-generation trap.
    var subject = SubjectId.of("vault-unreachable-reinstate");
    store.forget(subject);
    HttpResponse<String> outage = mock(HttpResponse.class);
    when(outage.statusCode()).thenReturn(500);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(outage);

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.reinstate(subject));
    assertTrue(store.isForgotten(subject), "an unprovable reinstate must keep the tombstone");
  }

  // ---------------------------------------------------------------------------
  // The identity proof compared the tombstone store's
  // clock (Postgres NOW()) with VAULT's clock (the transit key's creation_time) against a FIXED
  // 60 s margin. Two independent hosts — so a Vault clock >= 60 s BEHIND the tombstone store's
  // silently converts the refusal into an acceptance, and the residue key is revived.
  // The proof is now an AGE comparison whose Vault-sourced terms cancel that offset exactly.
  // ---------------------------------------------------------------------------

  /** An HTTP {@code Date} response header carrying Vault's own view of "now". */
  private static java.net.http.HttpHeaders vaultDate(java.time.Instant serverTime) {
    return java.net.http.HttpHeaders.of(
        java.util.Map.of(
            "Date",
            java.util.List.of(
                java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME.format(
                    serverTime.atZone(java.time.ZoneOffset.UTC)))),
        (a, b) -> true);
  }

  @Test
  void reinstateRefusesResidue_whenVaultsClockLagsTheTombstoneStore() throws Exception {
    // Vault's host runs 3 minutes behind the tombstone store's (a routine NTP misconfiguration,
    // well inside what the javadoc once called "modest"). An encrypt that passed the
    // pre-check lands its POST after the transit DELETE and upserts a FRESH key — but Vault stamps
    // it on ITS clock, i.e. 180 s before the tombstone's forgotten_at. The original absolute
    // comparison read that post-forget residue as a key "predating the forget by more than a
    // minute" and revived it. It must stay refused: the recorded creation time is the ORIGINAL
    // key's, and the residue's differs, whatever either host's clock says.
    var subject = SubjectId.of("vault-clock-skew-residue");
    java.time.Instant vaultNow = java.time.Instant.now().minusSeconds(180);
    java.time.Instant originalCreated = vaultNow.minus(java.time.Duration.ofDays(365));
    var liveKeyCreatedAt = new java.util.concurrent.atomic.AtomicReference<>(originalCreated);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if ("DELETE".equals(request.method())) {
                liveKeyCreatedAt.set(vaultNow); // original destroyed, residue key upserted
                return status(204);
              }
              return keyRead(liveKeyCreatedAt.get(), vaultNow);
            });
    engine.deleteKey(subject);

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.reinstate(subject));
    assertTrue(
        store.isForgotten(subject),
        "a key that is not the one the erasure destroyed must be refused however the two hosts'"
            + " clocks are set");
  }

  @Test
  void reinstateRevivesTheOriginal_evenWhenVaultsClockLagsTheTombstoneStore() throws Exception {
    // The reverse direction: the same 3-minute lag must NOT block the documented crashed-erasure
    // revival. Both instants of the identity proof come from Vault, so its offset is irrelevant.
    var subject = SubjectId.of("vault-clock-skew-original");
    java.time.Instant vaultNow = java.time.Instant.now().minusSeconds(180);
    java.time.Instant createdLongBefore = vaultNow.minus(java.time.Duration.ofDays(365));
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              return "DELETE".equals(request.method())
                  ? status(500) // the crash: the transit DELETE never lands
                  : keyRead(createdLongBefore, vaultNow);
            });
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.deleteKey(subject));

    engine.reinstate(subject);

    assertFalse(store.isForgotten(subject), "clock skew must not block the documented revival");
  }

  @Test
  void erasureRecordsNoIdentityEvidenceWhenVaultDoesNotReportItsOwnTime_soRevivalFailsClosed()
      throws Exception {
    // Vault's own "now" is what lets deleteKey check the identity margin without importing any
    // other host's clock. Without it (a proxy stripping or rewriting the HTTP Date header) the
    // erasure records NO evidence — it must still erase — and the later revival is refused.
    var subject = SubjectId.of("vault-no-server-date");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if ("DELETE".equals(request.method())) {
                return status(204);
              }
              HttpResponse<String> noDate = mock(HttpResponse.class);
              when(noDate.statusCode()).thenReturn(200);
              when(noDate.body()).thenReturn("{\"data\":{\"keys\":{\"1\":1000000000}}}");
              when(noDate.headers())
                  .thenReturn(java.net.http.HttpHeaders.of(java.util.Map.of(), (a, b) -> true));
              return noDate;
            });

    assertDoesNotThrow(
        () -> engine.deleteKey(subject), "an erasure must never fail for want of revival evidence");
    assertTrue(store.isForgotten(subject));
    assertTrue(
        store.keyCreatedAt(subject).isEmpty(),
        "no Vault-sourced 'now' means the identity margin cannot be checked, so nothing provable"
            + " may be recorded");

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.reinstate(subject));
    assertTrue(store.isForgotten(subject), "unprovable identity must keep the tombstone");
  }

  @Test
  void erasureRecordsNoIdentityEvidenceForAKeyYoungerThanTheMargin_soRevivalFailsClosed()
      throws Exception {
    // The margin is checked at ERASURE time and wholly on Vault's clock. A subject that first
    // encrypted less than a margin before erasing (ordinary under GDPR) leaves a key whose
    // creation time could not later be told apart from that of a key an encrypt racing this very
    // erasure re-creates — so no evidence is recorded and the revival is refused. Fail-closed: only
    // the revival convenience is lost.
    var subject = SubjectId.of("vault-key-younger-than-margin");
    java.time.Instant vaultNow = java.time.Instant.now();
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              return "DELETE".equals(request.method())
                  ? status(500) // the crash: the key survives, so only identity decides
                  : keyRead(vaultNow.minusSeconds(5), vaultNow);
            });
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.deleteKey(subject));
    assertTrue(store.keyCreatedAt(subject).isEmpty(), "a key inside the margin proves nothing");

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.reinstate(subject));
    assertTrue(store.isForgotten(subject), "unprovable identity must keep the tombstone");
  }

  @Test
  void aRepeatedErasureNeverOverwritesTheFirstErasuresIdentityEvidence() throws Exception {
    // The documented remediation is "re-run the idempotent deleteKey". By the second run the key
    // it can read may be residue — recording THAT would prove the wrong key and hand
    // the residue a revival. First-write-wins on the evidence is what prevents it.
    var subject = SubjectId.of("vault-repeated-erasure");
    java.time.Instant vaultNow = java.time.Instant.now();
    java.time.Instant originalCreated = vaultNow.minus(java.time.Duration.ofDays(365));
    var liveKeyCreatedAt = new java.util.concurrent.atomic.AtomicReference<>(originalCreated);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if ("DELETE".equals(request.method())) {
                // Destroyed, then an encrypt racing the erasure upserts residue whose sweep failed.
                liveKeyCreatedAt.set(vaultNow.minusSeconds(120));
                return status(204);
              }
              return keyRead(liveKeyCreatedAt.get(), vaultNow);
            });

    engine.deleteKey(subject);
    engine.deleteKey(subject); // the documented re-run, now reading the residue

    assertEquals(
        // Vault reports transit creation times as epoch SECONDS, so this is what the wire carries.
        java.util.Optional.of(java.time.Instant.ofEpochSecond(originalCreated.getEpochSecond())),
        store.keyCreatedAt(subject),
        "the FIRST erasure's evidence must survive every re-run");
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.reinstate(subject),
        "and the residue must therefore still be refused");
  }

  // ---------------------------------------------------------------------------
  // The AGE proof took VAULT's clock out of the
  // comparison but not the tombstone STORE's. tombstoneAge = now (THIS process's clock) minus
  // forgotten_at (the STORE's clock — Postgres NOW() on the database host for the shipped JDBC
  // store). A store clock running AHEAD of this process's UNDER-measures that age, which is
  // exactly the direction that LOOSENS `keyAge > tombstoneAge + margin`, and the only guard is a
  // NEGATIVE age — i.e. skew larger than the whole elapsed time since the forget. The proof must
  // instead compare two VAULT-sourced values with no cross-clock term at all: the key's creation
  // time recorded at deleteKey, and the live key's creation time now.
  // ---------------------------------------------------------------------------

  /**
   * A tombstone store whose {@code forgotten_at} is stamped by a DIFFERENT host's clock — the JDBC
   * store's real topology (Postgres {@code NOW()} on the database server vs {@code Instant.now()}
   * here). The instant is injected verbatim so skew can be modelled without waiting. Everything
   * else mirrors the shipped stores, including first-write-wins.
   */
  private static final class SkewedTombstoneStore
      implements org.streamrune.crypto.ForgottenSubjectStore {
    private final java.time.Instant forgottenAtOnTheStoresClock;
    private volatile boolean forgotten;
    private volatile java.time.Instant keyCreatedAt;

    SkewedTombstoneStore(java.time.Instant forgottenAtOnTheStoresClock) {
      this.forgottenAtOnTheStoresClock = forgottenAtOnTheStoresClock;
    }

    @Override
    public void forget(SubjectId subjectId) {
      forgotten = true;
    }

    @Override
    public void forget(SubjectId subjectId, java.time.Instant erasedKeyCreatedAt) {
      boolean firstWrite = !forgotten;
      forget(subjectId);
      if (firstWrite) {
        keyCreatedAt = erasedKeyCreatedAt; // first-write-wins, like ON CONFLICT DO NOTHING
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
      return forgotten
          ? java.util.Optional.of(forgottenAtOnTheStoresClock)
          : java.util.Optional.empty();
    }

    @Override
    public java.util.Optional<java.time.Instant> keyCreatedAt(SubjectId subjectId) {
      return java.util.Optional.ofNullable(keyCreatedAt);
    }
  }

  /** A {@code GET /keys/<name>} 200 carrying one version's creation time and Vault's own "now". */
  private static HttpResponse<String> keyRead(
      java.time.Instant created, java.time.Instant vaultNow) {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body())
        .thenReturn("{\"data\":{\"keys\":{\"1\":" + created.getEpochSecond() + "}}}");
    when(response.headers()).thenReturn(vaultDate(vaultNow));
    return response;
  }

  private static HttpResponse<String> status(int code) {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(code);
    return response;
  }

  private VaultCryptoEngine engineWith(org.streamrune.crypto.ForgottenSubjectStore tombstones) {
    return VaultCryptoEngine.builder()
        .httpClient(httpClient)
        .vaultAddress("http://localhost:8200")
        .token("test-token")
        .engineMount("transit")
        .retryDelay(java.time.Duration.ofMillis(1))
        .forgottenSubjectStore(tombstones)
        .build();
  }

  @Test
  void reinstateRefusesResidue_whenTheTombstoneStoresClockRunsAheadOfThisProcess()
      throws Exception {
    // Timeline, all modelled with real instants so nothing waits:
    //   R0 = now - 600s — the erasure. Vault destroys the ORIGINAL key; an encrypt that
    //        passed the pre-check lands its POST just after the DELETE, transit upserts a FRESH
    //        key that Vault stamps R0 on ITS clock, and the best-effort sweep failed (it never
    //        throws), so the residue is still live.
    //   The tombstone store's clock runs 180s AHEAD of this process's — a routine NTP
    //        misconfiguration between the application host and the database host — so the row
    //        reads forgotten_at = R0 + 180s.
    //   reinstate runs now.
    // Under the AGE proof: keyAge = 600s (measured wholly on Vault's clock, so Vault's own offset
    // genuinely cancels), tombstoneAge = now - (R0 + 180s) = 420s, and 600 > 420 + 60 — "identity
    // proven". The absolute conjunct passes too (R0 < R0 + 180s - 60s). The age is under-measured
    // but never negative, so the fail-closed guard never fires: the post-forget residue is
    // REVIVED, and from the first re-encrypt every pre-erasure blob decrypts as a hard HTTP 400.
    java.time.Instant erasure = java.time.Instant.now().minusSeconds(600);
    var store = new SkewedTombstoneStore(erasure.plusSeconds(180));
    var skewEngine = engineWith(store);
    var subject = SubjectId.of("vault-store-clock-ahead");
    java.time.Instant originalCreated = erasure.minus(java.time.Duration.ofDays(365));
    var liveKeyCreatedAt = new java.util.concurrent.atomic.AtomicReference<>(originalCreated);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if ("DELETE".equals(request.method())) {
                liveKeyCreatedAt.set(erasure); // original destroyed, residue key upserted at R0
                return status(204);
              }
              return keyRead(liveKeyCreatedAt.get(), java.time.Instant.now());
            });

    skewEngine.deleteKey(subject);

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> skewEngine.reinstate(subject),
        "a key created AFTER the erasure is residue and must be refused however the"
            + " tombstone store's clock is set — reviving it poisons every pre-erasure blob");
    assertTrue(store.isForgotten(subject), "a refused reinstate must keep the tombstone");
  }

  @Test
  void reinstateRevivesTheOriginal_whenTheTombstoneStoresClockRunsAheadOfThisProcess()
      throws Exception {
    // Reverse direction, must pass on BOTH sides of the fix: the same store-clock skew must not
    // block the documented crashed-erasure revival (tombstone committed, the transit DELETE never
    // landed, the ORIGINAL key still live).
    java.time.Instant erasure = java.time.Instant.now().minusSeconds(600);
    var store = new SkewedTombstoneStore(erasure.plusSeconds(180));
    var skewEngine = engineWith(store);
    var subject = SubjectId.of("vault-store-clock-ahead-original");
    java.time.Instant originalCreated = erasure.minus(java.time.Duration.ofDays(365));
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              // The crashed erasure: the transit DELETE never lands, so the ORIGINAL survives.
              return "DELETE".equals(request.method())
                  ? status(500)
                  : keyRead(originalCreated, java.time.Instant.now());
            });

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> skewEngine.deleteKey(subject),
        "the crashed erasure: the tombstone commits, then the transit DELETE fails");
    assertTrue(store.isForgotten(subject));

    skewEngine.reinstate(subject);

    assertFalse(store.isForgotten(subject), "the documented crashed-erasure revival must survive");
  }

  @Test
  void reinstateOfNeverForgottenSubjectIsNoOp_withoutVaultProbe() throws Exception {
    // Idempotency preserved: nothing to lift, nothing to probe — no HTTP traffic at all.
    engine.reinstate(SubjectId.of("vault-never-forgotten"));

    verifyNoInteractions(httpClient);
  }

  @Test
  void encryptSendsVaultEncryptRequest() throws Exception {
    String vaultCiphertext = "vault:v1:" + Base64.getEncoder().encodeToString("raw-ct".getBytes());
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body()).thenReturn("{\"data\":{\"ciphertext\":\"" + vaultCiphertext + "\"}}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    byte[] result = engine.encrypt(SubjectId.of("subject-1"), "plaintext".getBytes());

    assertEquals(vaultCiphertext, new String(result, StandardCharsets.UTF_8));
    verify(httpClient)
        .send(
            argThat(
                req ->
                    req.uri().toString().contains("/transit/encrypt/")
                        && req.method().equals("POST")),
            any(HttpResponse.BodyHandler.class));
  }

  @Test
  void decryptSendsVaultDecryptRequest() throws Exception {
    byte[] plaintext = "decrypted".getBytes();
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(response.body())
        .thenReturn(
            "{\"data\":{\"plaintext\":\"" + Base64.getEncoder().encodeToString(plaintext) + "\"}}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    byte[] result = engine.decrypt(SubjectId.of("subject-2"), "vault:v1:abc".getBytes());

    assertEquals("decrypted", new String(result));
    verify(httpClient)
        .send(
            argThat(
                req ->
                    req.uri().toString().contains("/transit/decrypt/")
                        && req.method().equals("POST")),
            any(HttpResponse.BodyHandler.class));
  }

  @Test
  void isKeyAvailableReturnsTrueOn200() throws Exception {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(200);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertTrue(engine.isKeyAvailable(SubjectId.of("subject-3")));
    verify(httpClient)
        .send(
            argThat(
                req ->
                    req.uri().toString().contains("/transit/keys/") && req.method().equals("GET")),
            any(HttpResponse.BodyHandler.class));
  }

  @Test
  void isKeyAvailableReturnsFalseOn404() throws Exception {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(404);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertFalse(engine.isKeyAvailable(SubjectId.of("unknown")));
  }

  @Test
  void encryptRejectsNullSubjectId() {
    assertThrows(IllegalArgumentException.class, () -> engine.encrypt(null, "data".getBytes()));
  }

  @Test
  void blankSubjectIdIsRejectedBySubjectIdBeforeTheEngineSeesIt() {
    // The engine checks only for null: SubjectId's own constructor rejects an empty or blank
    // value, so a blank subject id cannot be constructed, let alone passed to encrypt, decrypt,
    // deleteKey, reinstate or isKeyAvailable.
    assertThrows(IllegalArgumentException.class, () -> SubjectId.of("  "));
    assertThrows(IllegalArgumentException.class, () -> SubjectId.of(""));
  }

  @Test
  void encryptRejectsNullPlaintext() {
    assertThrows(
        IllegalArgumentException.class, () -> engine.encrypt(SubjectId.of("subject"), null));
  }

  @Test
  void decryptRejectsNullCiphertext() {
    assertThrows(
        IllegalArgumentException.class, () -> engine.decrypt(SubjectId.of("subject"), null));
  }

  @Test
  void decryptRejectsMalformedCiphertextAsDeterministic_beforeAnyVaultCall() throws Exception {
    // The shape check fires on data at rest BEFORE any HTTP call — a blob outside Vault's
    // strict transit shape can never decrypt, so it is a DETERMINISTIC blob-property failure and
    // must be raised as the CryptoMappingException subtype. A bare CryptoOperationException reads
    // as a Vault OUTAGE to the read-path classifiers (SagaStateConversion, ReadPoisonClassifier,
    // hasTransientCryptoCause) and is retried forever instead of quarantined.
    byte[] malformed = "not-vault-shaped\"".getBytes(StandardCharsets.UTF_8);

    var thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> engine.decrypt(SubjectId.of("subject-malformed"), malformed));

    assertTrue(thrown.getMessage().contains("expected vault:v<N>:<base64>"), thrown.getMessage());
    verify(httpClient, never()).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void isKeyAvailableThrowsOnServerError() throws Exception {
    // A 5xx says nothing about the key's existence. Reporting "absent" would let callers
    // treat a live key as already crypto-shredded.
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(503);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.isKeyAvailable(SubjectId.of("subject-503")));
  }

  @Test
  void isKeyAvailableThrowsOnForbidden() throws Exception {
    // 403 means token expiry/permissions, not "no key" — must never read as absent.
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(403);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.isKeyAvailable(SubjectId.of("subject-403")));
  }

  @Test
  void builderSetsAllFields() {
    var engine =
        VaultCryptoEngine.builder()
            .httpClient(httpClient)
            .vaultAddress("http://vault.example.com")
            .token("my-token")
            .engineMount("custom-engine")
            .forgottenSubjectStore(new InMemoryForgottenSubjectStore())
            .build();
    assertNotNull(engine);
  }

  @Test
  void builderThrowsWhenForgottenSubjectStoreMissing() {
    // Fail-closed: an engine with no tombstone store must not build,
    // otherwise Vault erasure would silently be non-terminal.
    IllegalStateException thrown =
        assertThrows(
            IllegalStateException.class,
            () -> VaultCryptoEngine.builder().httpClient(httpClient).token("valid-token").build());
    assertTrue(thrown.getMessage().contains("ForgottenSubjectStore"));
  }

  @Test
  void builderRejectsNullForgottenSubjectStore() {
    assertThrows(
        IllegalArgumentException.class,
        () -> VaultCryptoEngine.builder().forgottenSubjectStore(null));
  }

  @Test
  void deleteKeyCallsVaultDeleteEndpoint() throws Exception {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(204);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertDoesNotThrow(() -> engine.deleteKey(SubjectId.of("subject-del")));
    verify(httpClient)
        .send(
            argThat(
                req ->
                    req.uri().toString().contains("/transit/keys/")
                        && "DELETE".equals(req.method())),
            any(HttpResponse.BodyHandler.class));
    // The erasure also READS the key first, to record
    // the identity evidence a later reinstate needs. Read-only, and it never blocks the erasure —
    // here it sees a bare 204 with no key body, records nothing, and the DELETE still runs.
    verify(httpClient)
        .send(
            argThat(
                req ->
                    req.uri().toString().contains("/transit/keys/") && "GET".equals(req.method())),
            any(HttpResponse.BodyHandler.class));
  }

  @Test
  void encryptThrowsOnNon200Response() throws Exception {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(500);
    when(response.body()).thenReturn("{\"errors\":[\"internal error\"]}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.encrypt(SubjectId.of("subject-err"), "data".getBytes()));
  }

  @Test
  void decryptThrowsKeyNotFoundOn400WithEncryptionKeyNotFoundError() throws Exception {
    // Vault transit's actual decrypt error text for a missing key.
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(400);
    when(response.body()).thenReturn("{\"errors\":[\"encryption key not found\"]}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertThrows(
        KeyNotFoundException.class,
        () -> engine.decrypt(SubjectId.of("subject-nf"), "vault:v1:abc".getBytes()));
  }

  @Test
  void decryptThrowsCryptoMappingExceptionOn400WithInvalidCiphertextError() throws Exception {
    // A 400 caused by corrupt/tampered ciphertext must NOT be reported as "key not found" — that
    // would let a GDPR forget masquerade for a data-integrity incident. It is a
    // DETERMINISTIC verdict on the blob, so it is the CryptoMappingException subtype — the bare
    // type reads as a Vault outage to the read-path classifiers and is retried forever.
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(400);
    when(response.body())
        .thenReturn("{\"errors\":[\"invalid ciphertext: could not decode base64\"]}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    CryptoMappingException thrown =
        assertThrows(
            CryptoMappingException.class,
            () -> engine.decrypt(SubjectId.of("subject-bad-ct"), "vault:v1:abc".getBytes()));
    assertTrue(thrown.getMessage().contains("invalid ciphertext"));
  }

  @Test
  void decryptThrowsCryptoMappingExceptionOn400WithForwardedAeadFailure() throws Exception {
    // A well-formed blob whose tag no longer verifies (tampered, or written under destroyed key
    // material): transit forwards Go's AEAD error text verbatim — equally deterministic.
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(400);
    when(response.body()).thenReturn("{\"errors\":[\"cipher: message authentication failed\"]}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertThrows(
        CryptoMappingException.class,
        () -> engine.decrypt(SubjectId.of("subject-bad-tag"), "vault:v1:abc".getBytes()));
  }

  @Test
  void decryptKeepsA400PolicyRefusalBare() throws Exception {
    // A version refused by the key's min_decryption_version policy is NOT a blob verdict —
    // the operator can relax the policy — so it stays the bare (blocks-and-retries) type;
    // quarantining every old-version blob for a policy setting would be the worse outcome.
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(400);
    when(response.body())
        .thenReturn(
            "{\"errors\":[\"ciphertext or signature version is disallowed by policy (too old)\"]}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    org.streamrune.core.crypto.CryptoOperationException thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("subject-too-old"), "vault:v1:abc".getBytes()));
    assertFalse(thrown instanceof CryptoMappingException, thrown.getClass().getName());
    assertTrue(thrown.getMessage().contains("too old"));
  }

  @Test
  void decryptKeepsA400VersionTooNewRefusalBare() throws Exception {
    // A ciphertext version newer than what THIS Vault node currently knows about is
    // REVERSIBLE — a secondary lagging a primary's key rotation, a cluster restored from an older
    // snapshot, key material temporarily missing — NOT a verdict on the blob, even though
    // keysutil gives it the same "invalid ciphertext" prefix as the truly-deterministic members
    // of the family. By the fix's own rule (mirrors decryptKeepsA400PolicyRefusalBare above) it
    // must stay the bare (blocks-and-retries) type, not the CryptoMappingException poison type.
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(400);
    when(response.body()).thenReturn("{\"errors\":[\"invalid ciphertext: version is too new\"]}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    org.streamrune.core.crypto.CryptoOperationException thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.decrypt(SubjectId.of("subject-too-new"), "vault:v1:abc".getBytes()));
    assertFalse(thrown instanceof CryptoMappingException, thrown.getClass().getName());
    assertTrue(thrown.getMessage().contains("too new"));
  }

  @Test
  void decryptThrowsCryptoOperationExceptionOn400WithUnparseableBody() throws Exception {
    // Unparseable/empty error body: default to the safe direction (surface the error), never
    // silently redact.
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(400);
    when(response.body()).thenReturn("not json");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.decrypt(SubjectId.of("subject-badbody"), "vault:v1:abc".getBytes()));
  }

  // ----- Terminal erasure via the shared tombstone store -----

  @Test
  void forgetThenEncryptThrowsSubjectForgotten() throws Exception {
    HttpResponse<String> ok = mock(HttpResponse.class);
    when(ok.statusCode()).thenReturn(200);
    when(ok.body()).thenReturn("{\"data\":{\"ciphertext\":\"vault:v1:ct\"}}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(ok);

    engine.deleteKey(SubjectId.of("forget-me"));

    // Terminal erasure: a post-forget encrypt must be refused, never silently re-encrypted under a
    // fresh Vault transit key (the fail-open resurrection this fix closes).
    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("forget-me"), "new-pii".getBytes()));
    // The guard short-circuits before Vault: no encrypt request is ever sent for a forgotten
    // subject.
    verify(httpClient, never())
        .send(
            argThat(req -> req != null && req.uri().toString().contains("/transit/encrypt/")),
            any(HttpResponse.BodyHandler.class));
  }

  @Test
  void forgetThenDecryptThrowsKeyNotFound() throws Exception {
    HttpResponse<String> ok = mock(HttpResponse.class);
    when(ok.statusCode()).thenReturn(200);
    when(ok.body()).thenReturn("{\"data\":{\"plaintext\":\"cGxhaW4=\"}}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(ok);

    engine.deleteKey(SubjectId.of("forget-me-2"));

    // A forgotten subject's old ciphertext must surface as KeyNotFoundException (mapped to
    // [REDACTED]) without ever reaching Vault.
    assertThrows(
        KeyNotFoundException.class,
        () -> engine.decrypt(SubjectId.of("forget-me-2"), "vault:v1:abc".getBytes()));
    verify(httpClient, never())
        .send(
            argThat(req -> req != null && req.uri().toString().contains("/transit/decrypt/")),
            any(HttpResponse.BodyHandler.class));
  }

  @Test
  void forgetThenIsKeyAvailableFalse() throws Exception {
    HttpResponse<String> ok = mock(HttpResponse.class);
    when(ok.statusCode()).thenReturn(200);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(ok);

    engine.deleteKey(SubjectId.of("forget-me-3"));

    assertFalse(engine.isKeyAvailable(SubjectId.of("forget-me-3")));
  }

  @Test
  void reinstateAllowsEncryptAgain() throws Exception {
    // Crashed-erasure shape (the only reinstatable one): the stubbed key
    // read reports a version created long before the forget (epoch 1000000000 = 2001), so the
    // identity probe accepts it as the pre-erasure ORIGINAL; the same body carries the encrypt
    // response. (An earlier version of this test stubbed a bare 200 — which made a COMPLETED
    // erasure look crashed, the exact existence-vs-identity conflation the probe now rejects.)
    HttpResponse<String> ok = mock(HttpResponse.class);
    when(ok.statusCode()).thenReturn(200);
    when(ok.body())
        .thenReturn("{\"data\":{\"keys\":{\"1\":1000000000},\"ciphertext\":\"vault:v1:ct\"}}");
    // The identity proof measures the key's age on VAULT's clock, so the stub must
    // report Vault's own "now" through the HTTP Date header real Vault always sends.
    when(ok.headers()).thenReturn(vaultDate(java.time.Instant.now()));
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(ok);

    engine.deleteKey(SubjectId.of("reinstate-me"));
    // Re-registration is refused until the subject is explicitly reinstated.
    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(SubjectId.of("reinstate-me"), "pii".getBytes()));
    engine.reinstate(SubjectId.of("reinstate-me"));

    // Tombstone lifted: a fresh encrypt is allowed again.
    byte[] ct = engine.encrypt(SubjectId.of("reinstate-me"), "pii".getBytes());
    assertEquals("vault:v1:ct", new String(ct, StandardCharsets.UTF_8));
  }

  @Test
  void requiredCryptoTables_reflectsTombstoneStoreDurability() {
    // The engine (setUp uses an in-memory store) needs no crypto schema.
    assertTrue(engine.requiredCryptoTables().isEmpty());
    // A durable (DB-backed) tombstone store means forgotten_subjects must be provisioned/validated.
    var durable =
        VaultCryptoEngine.builder()
            .httpClient(httpClient)
            .token("tok")
            .forgottenSubjectStore(
                new org.streamrune.crypto.ForgottenSubjectStore() {
                  @Override
                  public void forget(SubjectId subjectId) {}

                  @Override
                  public boolean isForgotten(SubjectId subjectId) {
                    return false;
                  }

                  @Override
                  public void reinstate(SubjectId subjectId) {}

                  @Override
                  public boolean requiresDatabaseSchema() {
                    return true;
                  }
                })
            .build();
    assertEquals(java.util.Set.of("forgotten_subjects"), durable.requiredCryptoTables());
  }

  @Test
  void builderThrowsWhenHttpClientMissing() {
    assertThrows(
        IllegalStateException.class, () -> VaultCryptoEngine.builder().token("tok").build());
  }

  @Test
  void builderThrowsWhenTokenMissing() {
    assertThrows(
        IllegalStateException.class,
        () -> VaultCryptoEngine.builder().httpClient(httpClient).build());
  }

  @Test
  void builderThrowsWhenTokenBlank() {
    // Covers the token.isBlank() branch in build()
    assertThrows(
        IllegalStateException.class,
        () -> VaultCryptoEngine.builder().httpClient(httpClient).token("   ").build());
  }

  @Test
  void builderThrowsWhenEngineMountBlank() {
    // Covers the engineMount.isBlank() branch in build()
    assertThrows(
        IllegalStateException.class,
        () ->
            VaultCryptoEngine.builder()
                .httpClient(httpClient)
                .token("valid-token")
                .engineMount("  ")
                .build());
  }

  @Test
  void isKeyAvailableThrowsOnUnexpectedException() throws Exception {
    // Covers the catch (Exception) branch in isKeyAvailable
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new java.net.SocketTimeoutException("connection timeout"));

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.isKeyAvailable(SubjectId.of("subject-exc")));
  }

  @Test
  void deleteKeyTombstonesBeforeVaultDelete_soAFailedDeleteStillGatesTheSubject() throws Exception {
    // The tombstone must be recorded BEFORE the Vault transit DELETE, so a delete
    // that fails (or a crash) after the tombstone leaves the subject STILL terminally gated — the
    // documented safe failure (a tombstoned-but-still-live Vault key; encrypt/decrypt already
    // refuse). The reverse order (delete-then-tombstone) reopens a PII-resurrection window: the key
    // is gone, no tombstone is written, and a later encrypt silently mints a fresh Vault key.
    //
    // Vault transit DELETE fails (500) but encrypt would succeed (200) — so on the buggy
    // delete-first ordering the post-forget encrypt would RESURRECT the subject under a fresh key.
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest req = invocation.getArgument(0);
              @SuppressWarnings("unchecked")
              HttpResponse<String> r = mock(HttpResponse.class);
              if (req.uri().toString().contains("/transit/keys/")) {
                when(r.statusCode()).thenReturn(500); // Vault DELETE fails
              } else {
                when(r.statusCode()).thenReturn(200); // encrypt would succeed → resurrection risk
                when(r.body()).thenReturn("{\"data\":{\"ciphertext\":\"vault:v1:fresh\"}}");
              }
              return r;
            });

    var subject = SubjectId.of("forget-then-delete-fails");

    // deleteKey still surfaces the genuine Vault failure (the delete really failed)...
    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.deleteKey(subject));

    // ...but the subject is ALREADY terminally gated: a later encrypt is REFUSED, never silently
    // re-encrypted under a fresh Vault transit key (no resurrection window).
    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(subject, "resurrected-pii".getBytes()));
  }

  // ----- encrypt/deleteKey TOCTOU (post-Vault tombstone re-check) --

  /**
   * SHA-256-derived transit key name for a subject, mirroring {@code VaultCryptoEngine#keyName}.
   */
  private static String transitKeyName(String subjectValue) throws Exception {
    var md = java.security.MessageDigest.getInstance("SHA-256");
    byte[] digest = md.digest(subjectValue.getBytes(StandardCharsets.UTF_8));
    return "subject-" + java.util.HexFormat.of().formatHex(digest);
  }

  private static int firstIndexOf(
      java.util.List<HttpRequest> requests, String method, String uriPart) {
    for (int i = 0; i < requests.size(); i++) {
      HttpRequest r = requests.get(i);
      if (r.method().equals(method) && r.uri().toString().contains(uriPart)) {
        return i;
      }
    }
    return -1;
  }

  private static int lastIndexOf(
      java.util.List<HttpRequest> requests, String method, String uriPart) {
    for (int i = requests.size() - 1; i >= 0; i--) {
      HttpRequest r = requests.get(i);
      if (r.method().equals(method) && r.uri().toString().contains(uriPart)) {
        return i;
      }
    }
    return -1;
  }

  @Test
  void encryptRacingDeleteKey_refusesAndDeletesTheResurrectedTransitKey() throws Exception {
    var subject = SubjectId.of("race-resurrect");
    String keyName = transitKeyName("race-resurrect");
    var requests = java.util.Collections.synchronizedList(new java.util.ArrayList<HttpRequest>());
    var deleteKeyRan = new java.util.concurrent.atomic.AtomicBoolean(false);

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest req = invocation.getArgument(0);
              requests.add(req);
              @SuppressWarnings("unchecked")
              HttpResponse<String> r = mock(HttpResponse.class);
              if (req.uri().toString().contains("/transit/encrypt/")) {
                // The TOCTOU interleaving: T1's encrypt already passed the tombstone pre-check;
                // before its POST is processed by Vault, a FULL deleteKey completes (tombstone
                // committed, transit DELETE succeeded, erasure reported complete to the data
                // subject). Vault transit then UPSERTS a brand-new key for the name and answers
                // the late POST with fresh ciphertext — the documented resurrection hazard.
                if (deleteKeyRan.compareAndSet(false, true)) {
                  engine.deleteKey(subject);
                }
                when(r.statusCode()).thenReturn(200);
                when(r.body()).thenReturn("{\"data\":{\"ciphertext\":\"vault:v1:resurrected\"}}");
              } else {
                when(r.statusCode()).thenReturn(204);
              }
              return r;
            });

    // The caller must NEVER receive post-erasure ciphertext: the post-Vault tombstone re-check
    // sees the completed forget (deleteKey commits the tombstone BEFORE issuing the Vault DELETE,
    // so a POST that landed after that DELETE always observes the tombstone) and refuses.
    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(subject, "post-erasure PII".getBytes()));

    // ...and the just-upserted transit key must not be left live in Vault: after the late POST the
    // engine best-effort enables deletion_allowed (a fresh upserted key defaults to false) and
    // DELETEs the key again.
    int encryptIdx = firstIndexOf(requests, "POST", "/transit/encrypt/" + keyName);
    int configIdx = lastIndexOf(requests, "POST", "/transit/keys/" + keyName + "/config");
    int cleanupDeleteIdx = lastIndexOf(requests, "DELETE", "/transit/keys/" + keyName);
    assertTrue(encryptIdx >= 0, "the encrypt POST must have reached Vault (the race precondition)");
    assertTrue(
        configIdx > encryptIdx,
        "cleanup must set deletion_allowed on the resurrected key AFTER the late encrypt POST");
    assertTrue(
        cleanupDeleteIdx > configIdx,
        "the cleanup DELETE of the resurrected key must follow the deletion_allowed config");
  }

  @Test
  void encryptRacingDeleteKey_neverReturnsCiphertextEvenWhenCleanupIsDenied() throws Exception {
    var subject = SubjectId.of("race-cleanup-denied");
    var deleteKeyRan = new java.util.concurrent.atomic.AtomicBoolean(false);
    var postEncrypt = new java.util.concurrent.atomic.AtomicBoolean(false);

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest req = invocation.getArgument(0);
              @SuppressWarnings("unchecked")
              HttpResponse<String> r = mock(HttpResponse.class);
              if (req.uri().toString().contains("/transit/encrypt/")) {
                if (deleteKeyRan.compareAndSet(false, true)) {
                  engine.deleteKey(subject);
                }
                postEncrypt.set(true);
                when(r.statusCode()).thenReturn(200);
                when(r.body()).thenReturn("{\"data\":{\"ciphertext\":\"vault:v1:resurrected\"}}");
              } else if (postEncrypt.get()) {
                // The best-effort cleanup is denied (e.g. the token's policy does not grant
                // /config, and the fresh upserted key still has deletion_allowed=false → 400).
                when(r.statusCode()).thenReturn(400);
                when(r.body())
                    .thenReturn("{\"errors\":[\"deletion is not allowed for this key\"]}");
              } else {
                when(r.statusCode()).thenReturn(204);
              }
              return r;
            });

    // Cleanup failure must not change the outcome for the caller: SubjectForgottenException (the
    // terminal-erasure contract), never the resurrected ciphertext and never a wrapped
    // CryptoOperationException from the best-effort cleanup.
    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(subject, "post-erasure PII".getBytes()));
  }

  @Test
  void encryptRacingDeleteKey_cleanupConnectionFailureStillRefuses() throws Exception {
    var subject = SubjectId.of("race-cleanup-down");
    var deleteKeyRan = new java.util.concurrent.atomic.AtomicBoolean(false);
    var postEncrypt = new java.util.concurrent.atomic.AtomicBoolean(false);

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest req = invocation.getArgument(0);
              if (req.uri().toString().contains("/transit/encrypt/")) {
                if (deleteKeyRan.compareAndSet(false, true)) {
                  engine.deleteKey(subject);
                }
                postEncrypt.set(true);
                @SuppressWarnings("unchecked")
                HttpResponse<String> ok = mock(HttpResponse.class);
                when(ok.statusCode()).thenReturn(200);
                when(ok.body()).thenReturn("{\"data\":{\"ciphertext\":\"vault:v1:resurrected\"}}");
                return ok;
              }
              if (postEncrypt.get()) {
                // Vault becomes unreachable right after the late POST: the cleanup calls fail with
                // connection errors (exhausting the retry loop).
                throw new java.net.ConnectException("vault down");
              }
              @SuppressWarnings("unchecked")
              HttpResponse<String> noContent = mock(HttpResponse.class);
              when(noContent.statusCode()).thenReturn(204);
              return noContent;
            });

    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(subject, "post-erasure PII".getBytes()));
  }

  @Test
  void encryptRacingDeleteKey_cleanupFindingTheKeyAlreadyGoneIsSuccess() throws Exception {
    var subject = SubjectId.of("race-cleanup-already-gone");
    var deleteKeyRan = new java.util.concurrent.atomic.AtomicBoolean(false);
    var postEncrypt = new java.util.concurrent.atomic.AtomicBoolean(false);

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest req = invocation.getArgument(0);
              @SuppressWarnings("unchecked")
              HttpResponse<String> r = mock(HttpResponse.class);
              if (req.uri().toString().contains("/transit/encrypt/")) {
                if (deleteKeyRan.compareAndSet(false, true)) {
                  engine.deleteKey(subject);
                }
                postEncrypt.set(true);
                when(r.statusCode()).thenReturn(200);
                when(r.body()).thenReturn("{\"data\":{\"ciphertext\":\"vault:v1:resurrected\"}}");
              } else if (postEncrypt.get()) {
                // deleteKey's own DELETE landed after the late POST, so by the time the cleanup
                // runs the key no longer exists: Vault answers the delete-endpoint not-found 400.
                // That is success (no residue), not an error.
                when(r.statusCode()).thenReturn(400);
                when(r.body())
                    .thenReturn(
                        "{\"errors\":[\"error deleting policy subject-x: could not delete key; not"
                            + " found\"]}");
              } else {
                when(r.statusCode()).thenReturn(204);
              }
              return r;
            });

    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(subject, "post-erasure PII".getBytes()));
  }

  @Test
  void deleteKeyOnAlreadyAbsentVaultKeyIsIdempotentSuccess() throws Exception {
    // A retried forget after a successful delete, or a forget for a subject that never encrypted:
    // Vault answers the transit-key DELETE with 400 "could not delete key; not found". That must
    // be an idempotent no-op — the tombstone is durable and there is nothing left to erase — so
    // the documented "re-run the forget" remediation converges instead of failing forever.
    HttpResponse<String> notFound = mock(HttpResponse.class);
    when(notFound.statusCode()).thenReturn(400);
    when(notFound.body())
        .thenReturn(
            "{\"errors\":[\"error deleting policy subject-x: could not delete key; not found\"]}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(notFound);

    var subject = SubjectId.of("already-absent");
    assertDoesNotThrow(() -> engine.deleteKey(subject));

    // The tombstone was still written: the subject is terminally gated.
    assertThrows(
        org.streamrune.core.crypto.SubjectForgottenException.class,
        () -> engine.encrypt(subject, "new-pii".getBytes()));
  }

  @Test
  void deleteKeyStillFailsLoudlyWhenDeletionIsNotAllowed() throws Exception {
    // The delete endpoint's OTHER 400 cause — deletion_allowed=false — must keep failing loudly
    // (a GDPR forget must never silently no-op while the key material survives).
    HttpResponse<String> notAllowed = mock(HttpResponse.class);
    when(notAllowed.statusCode()).thenReturn(400);
    when(notAllowed.body())
        .thenReturn(
            "{\"errors\":[\"error deleting policy: deletion is not allowed for this key\"]}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(notAllowed);

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.deleteKey(SubjectId.of("locked-key")));
    assertTrue(thrown.getMessage().contains("deletion_allowed"), thrown.getMessage());
  }

  // ----- deleteKey enables deletion on the key itself ----------------------------------------
  // Transit creates a subject's key on the first encrypt with deletion_allowed=false, and the key
  // name is derived from the subject id, so no operator can configure it in advance: deleteKey sets
  // deletion_allowed=true on <mount>/keys/<name>/config itself, after the tombstone and before the
  // DELETE. The DELETE's answer decides the outcome.

  private static final String DELETION_NOT_ALLOWED_BODY =
      "{\"errors\":[\"error deleting policy subject-x: deletion is not allowed for this key\"]}";

  private static HttpResponse<String> statusWithBody(int code, String body) {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(code);
    when(response.body()).thenReturn(body);
    return response;
  }

  private static boolean isKeyConfigWrite(HttpRequest request) {
    return "POST".equals(request.method()) && request.uri().getPath().endsWith("/config");
  }

  private static boolean isKeyDelete(HttpRequest request) {
    return "DELETE".equals(request.method());
  }

  private static long count(
      java.util.List<HttpRequest> requests, java.util.function.Predicate<HttpRequest> kind) {
    return requests.stream().filter(kind).count();
  }

  @Test
  void deleteKeyEnablesDeletionOnTheKeyConfigAfterTheTombstoneAndBeforeTheDelete()
      throws Exception {
    var subject = SubjectId.of("lazily-created");
    String keyName = transitKeyName("lazily-created");
    var requests = java.util.Collections.synchronizedList(new java.util.ArrayList<HttpRequest>());
    var tombstonedAtConfigWrite = new java.util.concurrent.atomic.AtomicBoolean(false);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              requests.add(request);
              if (isKeyConfigWrite(request)) {
                tombstonedAtConfigWrite.set(store.isForgotten(subject));
                return status(200);
              }
              return status(204);
            });

    assertDoesNotThrow(() -> engine.deleteKey(subject));

    int configIdx = firstIndexOf(requests, "POST", "/transit/keys/" + keyName + "/config");
    int deleteIdx = firstIndexOf(requests, "DELETE", "/transit/keys/" + keyName);
    assertTrue(configIdx >= 0, "deleteKey must enable deletion on the key config: " + requests);
    assertTrue(deleteIdx > configIdx, "the DELETE must follow the config write: " + requests);
    assertTrue(
        tombstonedAtConfigWrite.get(), "the tombstone must be committed before any Vault write");
    assertEquals(1, count(requests, VaultCryptoEngineTest::isKeyDelete), "one round suffices");
    String body =
        requests.get(configIdx).bodyPublisher().map(VaultCryptoEngineTest::readBody).orElse("");
    assertEquals("{\"deletion_allowed\":true}", body);
  }

  /** Drains a request body publisher into a string. */
  private static String readBody(HttpRequest.BodyPublisher publisher) {
    var collected = new java.io.ByteArrayOutputStream();
    var done = new java.util.concurrent.CompletableFuture<Void>();
    publisher.subscribe(
        new java.util.concurrent.Flow.Subscriber<java.nio.ByteBuffer>() {
          @Override
          public void onSubscribe(java.util.concurrent.Flow.Subscription subscription) {
            subscription.request(Long.MAX_VALUE);
          }

          @Override
          public void onNext(java.nio.ByteBuffer item) {
            byte[] bytes = new byte[item.remaining()];
            item.get(bytes);
            collected.writeBytes(bytes);
          }

          @Override
          public void onError(Throwable throwable) {
            done.completeExceptionally(throwable);
          }

          @Override
          public void onComplete() {
            done.complete(null);
          }
        });
    done.join();
    return collected.toString(StandardCharsets.UTF_8);
  }

  @Test
  void deleteKeyStillDeletesAKeyThatIsAlreadyDeletableWhenTheConfigWriteIsForbidden()
      throws Exception {
    // A token without "update" on <mount>/keys/+/config still erases a key an operator made
    // deletable: the config write's 403 is not the verdict, the DELETE is.
    var requests = java.util.Collections.synchronizedList(new java.util.ArrayList<HttpRequest>());
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              requests.add(request);
              return status(isKeyConfigWrite(request) ? 403 : 204);
            });

    assertDoesNotThrow(() -> engine.deleteKey(SubjectId.of("operator-made-deletable")));
    assertEquals(1, count(requests, VaultCryptoEngineTest::isKeyDelete));
  }

  @Test
  void deleteKeyRunsASecondRoundWhenAnEncryptCreatedTheKeyAfterDeletionWasEnabled()
      throws Exception {
    // A forget of a subject whose FIRST encrypt is in flight: that encrypt passed its tombstone
    // pre-check before the tombstone committed. The config write finds no key yet (Vault 400), the
    // encrypt then creates one with deletion_allowed=false, and the DELETE is refused. One more
    // round enables deletion on the key that now exists and deletes it.
    var requests = java.util.Collections.synchronizedList(new java.util.ArrayList<HttpRequest>());
    var configWrites = new java.util.concurrent.atomic.AtomicInteger();
    var deletes = new java.util.concurrent.atomic.AtomicInteger();
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              requests.add(request);
              if (isKeyConfigWrite(request)) {
                return configWrites.incrementAndGet() == 1
                    ? statusWithBody(
                        400, "{\"errors\":[\"no existing key named subject-x could be found\"]}")
                    : status(204);
              }
              if (isKeyDelete(request)) {
                return deletes.incrementAndGet() == 1
                    ? statusWithBody(400, DELETION_NOT_ALLOWED_BODY)
                    : status(204);
              }
              return status(404); // the identity read: no key yet
            });

    assertDoesNotThrow(() -> engine.deleteKey(SubjectId.of("first-encrypt-in-flight")));
    assertEquals(2, count(requests, VaultCryptoEngineTest::isKeyConfigWrite));
    assertEquals(2, count(requests, VaultCryptoEngineTest::isKeyDelete));
  }

  @Test
  void deleteKeyNamesThePolicyCapabilityWhenTheTokenCannotEnableDeletion() throws Exception {
    var subject = SubjectId.of("config-forbidden");
    var requests = java.util.Collections.synchronizedList(new java.util.ArrayList<HttpRequest>());
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              requests.add(request);
              if (isKeyConfigWrite(request)) {
                return status(403);
              }
              return isKeyDelete(request)
                  ? statusWithBody(400, DELETION_NOT_ALLOWED_BODY)
                  : status(404);
            });

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.deleteKey(subject));

    assertTrue(
        thrown.getMessage().contains("\"update\" capability on transit/keys/+/config"),
        thrown.getMessage());
    assertTrue(thrown.getMessage().contains("HTTP 403"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains(subject.redacted()), thrown.getMessage());
    assertFalse(thrown.getMessage().contains("config-forbidden"), "never the raw subject id");
    assertTrue(store.isForgotten(subject), "the subject stays gated by its tombstone");
    assertEquals(2, count(requests, VaultCryptoEngineTest::isKeyDelete), "two rounds, then fail");
  }

  @Test
  void deleteKeyAsksForARerunWhenTheKeyIsRefusedAgainAfterDeletionWasEnabled() throws Exception {
    // Both rounds enabled deletion and both DELETEs were refused: the key was re-created after
    // each config write. Nothing is wrong with the policy; a re-run converges.
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if (isKeyConfigWrite(request)) {
                return status(204);
              }
              return isKeyDelete(request)
                  ? statusWithBody(400, DELETION_NOT_ALLOWED_BODY)
                  : status(404);
            });

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.deleteKey(SubjectId.of("recreated-twice")));

    assertTrue(thrown.getMessage().contains("Re-run the forget"), thrown.getMessage());
    assertFalse(thrown.getMessage().contains("capability"), thrown.getMessage());
  }

  @Test
  void deleteKeyNamesTheDeleteCapabilityWhenTheDeleteIsForbidden() throws Exception {
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if (isKeyConfigWrite(request)) {
                return status(204);
              }
              return status(isKeyDelete(request) ? 403 : 404);
            });

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.deleteKey(SubjectId.of("delete-forbidden")));

    assertTrue(thrown.getMessage().contains("HTTP 403"), thrown.getMessage());
    assertTrue(
        thrown.getMessage().contains("\"delete\" capability on transit/keys/+"),
        thrown.getMessage());
  }

  @Test
  void deleteKeyReportsVaultsErrorTextForAnyOther400() throws Exception {
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              if (isKeyDelete(request)) {
                return statusWithBody(400, "{\"errors\":[\"unexpected validation failure\"]}");
              }
              return status(isKeyConfigWrite(request) ? 204 : 404);
            });

    var thrown =
        assertThrows(
            org.streamrune.core.crypto.CryptoOperationException.class,
            () -> engine.deleteKey(SubjectId.of("other-400")));

    assertTrue(
        thrown.getMessage().contains("HTTP 400: unexpected validation failure"),
        thrown.getMessage());
  }

  @Test
  void deleteKeyFailsWithoutDeletingWhenTheConfigWriteCannotReachVault() throws Exception {
    // Crash point between the tombstone and the Vault writes: the subject is gated, the key
    // survives, and a re-run of the forget converges.
    var subject = SubjectId.of("config-unreachable");
    var requests = java.util.Collections.synchronizedList(new java.util.ArrayList<HttpRequest>());
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenAnswer(
            invocation -> {
              HttpRequest request = invocation.getArgument(0);
              requests.add(request);
              if (isKeyConfigWrite(request)) {
                throw new java.net.ConnectException("vault down");
              }
              return status(404);
            });

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class, () -> engine.deleteKey(subject));

    assertTrue(store.isForgotten(subject), "the subject stays gated by its tombstone");
    assertEquals(0, count(requests, VaultCryptoEngineTest::isKeyDelete));
  }

  @Test
  void deleteKeyThrowsOnUnexpectedHttpStatus() throws Exception {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(500);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(response);

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.deleteKey(SubjectId.of("subject-del-err")));
  }

  @Test
  void encryptThrowsOnConnectionError() throws Exception {
    // Covers the catch (Exception) path in encrypt
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new java.net.ConnectException("connection refused"));

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.encrypt(SubjectId.of("subject-conn"), "data".getBytes()));
  }

  @Test
  void decryptThrowsOnConnectionError() throws Exception {
    // Covers the catch (Exception) path in decrypt
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new java.net.SocketTimeoutException("read timed out"));

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.decrypt(SubjectId.of("subject-conn2"), "vault:v1:abc".getBytes()));
  }

  @Test
  void encryptRetriesConnectionErrorsBeforeFailing() throws Exception {
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new java.net.ConnectException("connection refused"));

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.encrypt(SubjectId.of("subject-retry"), "data".getBytes()));

    // Default maxRetries(2) → 1 initial attempt + 2 retries.
    verify(httpClient, times(3)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void encryptRecoversWhenRetrySucceeds() throws Exception {
    HttpResponse<String> ok = mock(HttpResponse.class);
    when(ok.statusCode()).thenReturn(200);
    when(ok.body()).thenReturn("{\"data\":{\"ciphertext\":\"vault:v1:ct\"}}");
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new java.net.ConnectException("connection refused"))
        .thenReturn(ok);

    byte[] result = engine.encrypt(SubjectId.of("subject-recover"), "data".getBytes());

    assertEquals("vault:v1:ct", new String(result, StandardCharsets.UTF_8));
    verify(httpClient, times(2)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void interruptedRequestRestoresInterruptFlag() throws Exception {
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new InterruptedException("interrupted"));

    assertThrows(
        org.streamrune.core.crypto.CryptoOperationException.class,
        () -> engine.encrypt(SubjectId.of("subject-int"), "data".getBytes()));
    assertTrue(Thread.interrupted(), "interrupt flag must be restored (and cleared for the suite)");
  }

  @Test
  void builderRejectsNegativeMaxRetries() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            VaultCryptoEngine.builder().httpClient(httpClient).token("tok").maxRetries(-1).build());
  }

  @Test
  void builderRejectsNonPositiveRequestTimeout() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            VaultCryptoEngine.builder()
                .httpClient(httpClient)
                .token("tok")
                .requestTimeout(java.time.Duration.ZERO)
                .build());
  }

  // ---------------------------------------------------------------------------
  // Retry backoff (capped like RetryPolicy). The
  // backoff used to be retryDelay.toMillis() << (attempt - 1): with a large maxRetries it slept
  // for years from about the 30th retry, and from about the 57th the long shift overflowed to a
  // negative delay (Thread.sleep then threw) or wrapped its distance mod 64 back to a short one.
  // ---------------------------------------------------------------------------

  @Test
  void retryBackoffDoublesFromTheConfiguredDelay() {
    var delay = java.time.Duration.ofMillis(200);
    assertEquals(java.time.Duration.ofMillis(200), VaultCryptoEngine.retryBackoff(delay, 1));
    assertEquals(java.time.Duration.ofMillis(400), VaultCryptoEngine.retryBackoff(delay, 2));
    assertEquals(java.time.Duration.ofMillis(800), VaultCryptoEngine.retryBackoff(delay, 3));
    // Sub-millisecond delays keep their precision instead of truncating to zero.
    assertEquals(
        java.time.Duration.ofNanos(1_000),
        VaultCryptoEngine.retryBackoff(java.time.Duration.ofNanos(500), 2));
  }

  @Test
  void retryBackoffIsCappedNeverOverflowsAndNeverShrinksAsRetriesGrow() {
    var delay = java.time.Duration.ofMillis(200);
    var cap = VaultCryptoEngine.MAX_RETRY_BACKOFF;
    // 200ms * 2^8 = 51.2s is the first rung past the 30s cap.
    assertEquals(cap, VaultCryptoEngine.retryBackoff(delay, 9));
    // Years under the old shift (retry 30), a negative delay (retry 58), a wrapped shift distance
    // (retry 65 shifted by 0 again) and the largest retry number: all land on the cap.
    for (int retry : new int[] {30, 58, 64, 65, 1_000, Integer.MAX_VALUE}) {
      assertEquals(cap, VaultCryptoEngine.retryBackoff(delay, retry), "retry " + retry);
    }
    var previous = java.time.Duration.ZERO;
    for (int retry = 1; retry <= 200; retry++) {
      var backoff = VaultCryptoEngine.retryBackoff(delay, retry);
      assertFalse(backoff.isNegative(), "retry " + retry + " backoff " + backoff);
      assertTrue(backoff.compareTo(cap) <= 0, "retry " + retry + " backoff " + backoff);
      assertTrue(backoff.compareTo(previous) >= 0, "retry " + retry + " shrank to " + backoff);
      previous = backoff;
    }
  }

  @Test
  void retryBackoffKeepsAZeroDelayAtZeroAndADelayAboveTheCapAsConfigured() {
    for (int retry : new int[] {1, 2, 64, 65, Integer.MAX_VALUE}) {
      assertEquals(
          java.time.Duration.ZERO,
          VaultCryptoEngine.retryBackoff(java.time.Duration.ZERO, retry),
          "retry " + retry);
    }
    // An operator who configured a delay above the cap gets that delay on every retry: the cap
    // stops the doubling, it never shortens what was asked for.
    var longDelay = VaultCryptoEngine.MAX_RETRY_BACKOFF.multipliedBy(2);
    for (int retry : new int[] {1, 2, 64, 65, Integer.MAX_VALUE}) {
      assertEquals(longDelay, VaultCryptoEngine.retryBackoff(longDelay, retry), "retry " + retry);
    }
  }

  @Test
  void builderRejectsNegativeRetryDelay() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            VaultCryptoEngine.builder()
                .httpClient(httpClient)
                .token("tok")
                .retryDelay(java.time.Duration.ofMillis(-1))
                .build());
  }
}
