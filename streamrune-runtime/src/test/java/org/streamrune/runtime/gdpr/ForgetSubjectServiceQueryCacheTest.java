package org.streamrune.runtime.gdpr;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.streamrune.core.Cacheable;
import org.streamrune.core.Query;
import org.streamrune.core.StreamRuneContext;
import org.streamrune.core.crypto.Encrypted;
import org.streamrune.core.gdpr.SubjectDataPurger;
import org.streamrune.core.types.CorrelationId;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.crypto.CryptoShreddingModule;
import org.streamrune.runtime.CachingQueryBus;
import org.streamrune.runtime.SimpleQueryBus;
import org.streamrune.test.InMemoryCryptoEngine;

/**
 * A forget must not leave the subject's decrypted PII in the query cache. Before this, a
 * {@code @Cacheable} query answered before the erasure kept serving the decrypted value, or the
 * purged read-model row, until its TTL ran out: the crypto-shred and the purge happened, and the
 * cache went on answering as if they had not.
 */
class ForgetSubjectServiceQueryCacheTest {

  record CustomerRegistered(String customerId, @Encrypted(subjectId = "customerId") String email) {}

  @Cacheable(scope = Cacheable.Scope.GLOBAL, ttlSeconds = 3600)
  record CustomerEmailQuery(String customerId) implements Query<String> {}

  @Cacheable(scope = Cacheable.Scope.USER, ttlSeconds = 3600)
  record CustomerRowQuery(String customerId) implements Query<String> {}

  private static final SubjectId ALICE = SubjectId.of("alice");

  /**
   * The canonical case: the handler decrypts the subject's event, so the cached answer is the
   * plaintext email. After the crypto-shred the event reads as {@code [REDACTED]}, and so must the
   * query — with no purger registered at all.
   */
  @Test
  void forget_evictsACachedAnswerHoldingTheSubjectsDecryptedPii() throws Exception {
    var crypto = new InMemoryCryptoEngine();
    var mapper = new ObjectMapper().registerModule(new CryptoShreddingModule(crypto));
    String storedEvent =
        mapper.writeValueAsString(new CustomerRegistered("alice", "alice@example.com"));
    var bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    bus.register(
        CustomerEmailQuery.class,
        q -> {
          try {
            return mapper.readValue(storedEvent, CustomerRegistered.class).email();
          } catch (Exception e) {
            throw new IllegalStateException(e);
          }
        });
    var service = ForgetSubjectService.builder().cryptoEngine(crypto).queryCache(bus).build();

    assertEquals("alice@example.com", bus.dispatch(new CustomerEmailQuery("alice")));

    service.forget(ALICE, UserId.of("dpo"));

    assertEquals(
        CryptoShreddingModule.REDACTED,
        bus.dispatch(new CustomerEmailQuery("alice")),
        "after the forget the cache must not serve the subject's decrypted email");
  }

  /**
   * A USER-scoped answer is cached under the caller who asked (a support agent), not under the
   * subject or the DPO who runs the forget. The forget clears every caller's partition.
   */
  @Test
  void forget_evictsTheSubjectsRowFromEveryCallersPartition_afterThePurge() {
    Map<String, String> readModel = new ConcurrentHashMap<>(Map.of("alice", "Alice, Main St 1"));
    var bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    bus.register(CustomerRowQuery.class, q -> readModel.getOrDefault(q.customerId(), "none"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(new InMemoryCryptoEngine())
            .purgers(List.of(purgerOf(readModel)))
            .queryCache(bus)
            .build();

    assertEquals(
        "Alice, Main St 1", dispatchAs("support-agent", bus, new CustomerRowQuery("alice")));

    ForgetResult result = service.forget(ALICE, UserId.of("dpo"));

    assertTrue(result.fullyErased());
    assertEquals(
        "none",
        dispatchAs("support-agent", bus, new CustomerRowQuery("alice")),
        "the purged row must not be served from the support agent's cache partition");
  }

  /**
   * The eviction runs after the last purge, not before it: a read that lands while a purge is still
   * running re-caches the row the purge is about to delete, and only an eviction after the purge
   * removes that answer.
   */
  @Test
  void forget_evictsAfterTheLastPurge_soAReadDuringThePurgeIsNotLeftCached() {
    Map<String, String> readModel = new ConcurrentHashMap<>(Map.of("alice", "Alice, Main St 1"));
    var bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    bus.register(CustomerEmailQuery.class, q -> readModel.getOrDefault(q.customerId(), "none"));
    AtomicReference<String> readDuringPurge = new AtomicReference<>();
    SubjectDataPurger racingPurger =
        new SubjectDataPurger() {
          @Override
          public String name() {
            return "customers";
          }

          @Override
          public void purge(SubjectId subjectId) {
            // A concurrent reader, modelled inline: it reads (and caches) the row before it goes.
            readDuringPurge.set(bus.dispatch(new CustomerEmailQuery(subjectId.value())));
            readModel.remove(subjectId.value());
          }
        };
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(new InMemoryCryptoEngine())
            .purgers(List.of(racingPurger))
            .queryCache(bus)
            .build();

    service.forget(ALICE, null);

    assertEquals("Alice, Main St 1", readDuringPurge.get(), "the read during the purge cached it");
    assertEquals(
        "none",
        bus.dispatch(new CustomerEmailQuery("alice")),
        "the eviction after the last purge must drop the answer cached during the purge");
  }

  /** A forget whose shred fails changes nothing, so it evicts nothing either. */
  @Test
  void forget_whoseShredFails_leavesTheCacheAlone() {
    var bus = CachingQueryBus.builder().delegate(new SimpleQueryBus()).build();
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    bus.register(CustomerEmailQuery.class, q -> "answer-" + calls.incrementAndGet());
    var failing = mock(org.streamrune.core.crypto.CryptoEngine.class);
    doThrow(new IllegalStateException("key store down")).when(failing).deleteKey(ALICE);
    var service = ForgetSubjectService.builder().cryptoEngine(failing).queryCache(bus).build();
    assertEquals("answer-1", bus.dispatch(new CustomerEmailQuery("bob")));

    assertThrows(IllegalStateException.class, () -> service.forget(ALICE, null));

    assertEquals("answer-1", bus.dispatch(new CustomerEmailQuery("bob")));
  }

  /**
   * An eviction that throws an exception never stops the erasure, which already happened, but the
   * result reports it as incomplete, like a failed purge.
   */
  @Test
  void forget_evictionThrowingAnException_reportsAnIncompleteErasure() {
    CachingQueryBus bus = mock(CachingQueryBus.class);
    doThrow(new IllegalStateException("cache broken")).when(bus).evictAll();
    Map<String, String> readModel = new ConcurrentHashMap<>(Map.of("alice", "Alice"));
    var audit = mock(org.streamrune.core.audit.AuditStore.class);
    var metrics = new ForgetSubjectServiceTest.CapturingMetrics();
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(new InMemoryCryptoEngine())
            .auditStore(audit)
            .metrics(metrics)
            .purgers(List.of(purgerOf(readModel)))
            .queryCache(bus)
            .build();

    ForgetResult result = service.forget(ALICE, null);

    // The key is gone and the read model purged, but a cached answer may still hold the subject's
    // decrypted data until its TTL runs out: the caller must not report a completed erasure.
    assertTrue(result.keyDeleted());
    assertTrue(readModel.isEmpty());
    assertTrue(result.failedPurgers().isEmpty(), "every purger succeeded");
    assertFalse(result.queryCacheEvicted());
    assertFalse(result.fullyErased());
    verify(bus).evictAll();
    assertEquals(1, metrics.purgeFailed.get(), "the eviction failure moves the alert metric");
    assertEquals(ForgetSubjectService.QUERY_CACHE_METRIC_NAME, metrics.lastPurger);
    var rows = org.mockito.ArgumentCaptor.forClass(org.streamrune.core.audit.AuditEntry.class);
    verify(audit, atLeast(1)).save(rows.capture());
    assertTrue(
        rows.getAllValues().stream()
            .anyMatch(
                e ->
                    e.outcome() == org.streamrune.core.audit.AuditOutcome.FAILURE
                        && e.errorMessage() != null
                        && e.errorMessage().contains("query-cache eviction failed")),
        "the eviction failure is recorded as a FAILURE audit row");
  }

  @Test
  void forget_withoutAQueryCache_reportsTheCacheAsEvicted() {
    Map<String, String> readModel = new ConcurrentHashMap<>(Map.of("alice", "Alice"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(new InMemoryCryptoEngine())
            .purgers(List.of(purgerOf(readModel)))
            .build();

    ForgetResult result = service.forget(ALICE, null);

    assertTrue(result.queryCacheEvicted(), "nothing is cached, so nothing can remain there");
    assertTrue(result.fullyErased());
  }

  /**
   * An Error from the eviction is thrown like every other post-shred Error: after the purgers ran,
   * carrying the note that the crypto-shred completed.
   */
  @Test
  void forget_evictionThrowingAnError_propagatesItAfterThePurgers() {
    CachingQueryBus bus = mock(CachingQueryBus.class);
    var error = new AssertionError("cache corrupted");
    doThrow(error).when(bus).evictAll();
    Map<String, String> readModel = new ConcurrentHashMap<>(Map.of("alice", "Alice"));
    var service =
        ForgetSubjectService.builder()
            .cryptoEngine(new InMemoryCryptoEngine())
            .purgers(List.of(purgerOf(readModel)))
            .queryCache(bus)
            .build();

    AssertionError thrown = assertThrows(AssertionError.class, () -> service.forget(ALICE, null));

    assertSame(error, thrown);
    assertTrue(readModel.isEmpty(), "the purger ran before the eviction");
    assertEquals(1, thrown.getSuppressed().length);
    assertTrue(thrown.getSuppressed()[0].getMessage().contains("crypto-shred completed"));
  }

  private static SubjectDataPurger purgerOf(Map<String, String> readModel) {
    return new SubjectDataPurger() {
      @Override
      public String name() {
        return "customers";
      }

      @Override
      public void purge(SubjectId subjectId) {
        readModel.remove(subjectId.value());
      }
    };
  }

  private static String dispatchAs(String user, CachingQueryBus bus, Query<String> query) {
    return ScopedValue.where(
            StreamRuneContext.CURRENT,
            new StreamRuneContext.RequestContext(
                null, UserId.of(user), CorrelationId.of("corr-1"), Instant.now(), null))
        .call(() -> bus.dispatch(query));
  }
}
