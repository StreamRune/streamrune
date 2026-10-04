package org.streamrune.quarkus;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.inject.Instance;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.streamrune.core.audit.AuditEntry;
import org.streamrune.core.audit.AuditStore;
import org.streamrune.core.crypto.CryptoEngine;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.test.InMemoryCryptoEngine;

class QuarkusGdprProducerTest {

  private final StreamRuneProducers producers = new StreamRuneProducers();

  @SuppressWarnings("unchecked")
  private <T> Instance<T> satisfied(T value) {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(false);
    when(inst.get()).thenReturn(value);
    return inst;
  }

  @SuppressWarnings("unchecked")
  private <T> Instance<T> unsatisfied() {
    Instance<T> inst = mock(Instance.class);
    when(inst.isUnsatisfied()).thenReturn(true);
    return inst;
  }

  @Test
  void forgetSubjectService_present_withCryptoEngine() {
    var service =
        producers.forgetSubjectService(
            satisfied(new InMemoryCryptoEngine()),
            unsatisfied(),
            List.of(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(service);
  }

  /**
   * Two CachingQueryBus beans with no default: the forget could evict only one of them, so the
   * producer refuses instead of leaving the other serving answers taken before an erasure.
   */
  @Test
  @SuppressWarnings("unchecked")
  void forgetSubjectService_ambiguousQueryCache_failsLoud() {
    Instance<org.streamrune.runtime.CachingQueryBus> ambiguous = mock(Instance.class);
    when(ambiguous.isAmbiguous()).thenReturn(true);

    var failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.forgetSubjectService(
                    satisfied(new InMemoryCryptoEngine()),
                    unsatisfied(),
                    List.of(),
                    unsatisfied(),
                    ambiguous));
    assertTrue(failure.getMessage().contains("CachingQueryBus"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void forgetSubjectService_resolvableQueryCache_isEvictedByAForget() {
    var bus =
        org.streamrune.runtime.CachingQueryBus.builder()
            .delegate(new org.streamrune.runtime.SimpleQueryBus())
            .build();
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    bus.register(GlobalQuery.class, q -> "answer-" + calls.incrementAndGet());
    Instance<org.streamrune.runtime.CachingQueryBus> resolvable = mock(Instance.class);
    when(resolvable.isResolvable()).thenReturn(true);
    when(resolvable.get()).thenReturn(bus);
    var service =
        producers.forgetSubjectService(
            satisfied(new InMemoryCryptoEngine()),
            unsatisfied(),
            List.of(),
            unsatisfied(),
            resolvable);
    assertEquals("answer-1", bus.dispatch(new GlobalQuery()));

    service.forget(SubjectId.of("user-1"), null);

    assertEquals("answer-2", bus.dispatch(new GlobalQuery()), "the forget evicted the cache");
  }

  @org.streamrune.core.Cacheable(scope = org.streamrune.core.Cacheable.Scope.GLOBAL)
  record GlobalQuery() implements org.streamrune.core.Query<String> {}

  @Test
  void forgetSubjectService_unsatisfiedCrypto_returnsNull() {
    Instance<CryptoEngine> crypto = unsatisfied();
    var service =
        producers.forgetSubjectService(
            crypto, unsatisfied(), List.of(), unsatisfied(), unsatisfied());
    assertNull(service);
  }

  @Test
  void forgetSubjectService_wiresOptionalAuditStore() {
    AuditStore auditStore = mock(AuditStore.class);
    var service =
        producers.forgetSubjectService(
            satisfied(new InMemoryCryptoEngine()),
            satisfied(auditStore),
            List.of(),
            unsatisfied(),
            unsatisfied());
    assertNotNull(service);
    service.forget(SubjectId.of("user-1"), UserId.of("admin"));
    verify(auditStore, atLeastOnce()).save(any(AuditEntry.class));
  }

  @Test
  void forgetSubjectService_auditsAReinstateBesideTheForgetItReverses() {
    AuditStore auditStore = mock(AuditStore.class);
    var service =
        producers.forgetSubjectService(
            satisfied(new InMemoryCryptoEngine()),
            satisfied(auditStore),
            List.of(),
            unsatisfied(),
            unsatisfied());

    service.forget(SubjectId.of("user-1"), UserId.of("dpo"));
    service.reinstate(SubjectId.of("user-1"), UserId.of("dpo"));

    var captor = org.mockito.ArgumentCaptor.forClass(AuditEntry.class);
    verify(auditStore, times(2)).save(captor.capture());
    assertEquals(
        List.of("GDPR_FORGET", "GDPR_REINSTATE"),
        captor.getAllValues().stream().map(AuditEntry::commandType).toList());
  }

  @Test
  void exportSubjectDataService_alwaysReturnsService_evenIfNoCollectors() {
    // Quarkus quirk: builder accepts empty list, export() throws lazily.
    var service = producers.exportSubjectDataService(List.of(), unsatisfied());
    assertNotNull(service);
    assertThrows(IllegalStateException.class, () -> service.export(SubjectId.of("user-1"), null));
  }

  @Test
  void exportSubjectDataService_withCollectors() {
    SubjectDataCollector profile =
        new SubjectDataCollector() {
          @Override
          public String name() {
            return "profile";
          }

          @Override
          public JsonNode collect(SubjectId subjectId) {
            return new ObjectMapper().createObjectNode();
          }
        };
    var service = producers.exportSubjectDataService(List.of(profile), unsatisfied());
    var export = service.export(SubjectId.of("user-1"), null);
    assertEquals(1, export.sections().size());
  }

  @Test
  void exportSubjectDataService_wiresOptionalAuditStore() {
    AuditStore auditStore = mock(AuditStore.class);
    SubjectDataCollector profile =
        new SubjectDataCollector() {
          @Override
          public String name() {
            return "profile";
          }

          @Override
          public JsonNode collect(SubjectId subjectId) {
            return new ObjectMapper().createObjectNode();
          }
        };
    var service = producers.exportSubjectDataService(List.of(profile), satisfied(auditStore));
    service.export(SubjectId.of("user-1"), UserId.of("admin"));
    verify(auditStore, atLeastOnce()).save(any(AuditEntry.class));
  }
}
