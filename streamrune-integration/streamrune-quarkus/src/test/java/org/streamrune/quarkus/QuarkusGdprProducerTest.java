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
import org.streamrune.core.gdpr.SnapshotPurger;
import org.streamrune.core.gdpr.SubjectDataCollector;
import org.streamrune.core.types.SubjectId;
import org.streamrune.core.types.UserId;
import org.streamrune.test.InMemoryCryptoEngine;

class QuarkusGdprProducerTest {

  private final StreamRuneProducers producers = new StreamRuneProducers();

  /**
   * The tests of everything but the snapshot purge state that their application stores no
   * snapshots; the snapshot purge is tested below with the defaults.
   */
  private static final StreamRuneQuarkusProperties NO_SNAPSHOTS =
      TestProperties.of(java.util.Map.of("streamrune.gdpr.snapshot-purge", "none"));

  /**
   * The produced forget needs a snapshot purger: a snapshot keeps every field of the aggregate's
   * state not marked {@code @Encrypted} in the clear. The producer refuses, saying what to declare
   * and which property opts out.
   */
  @Test
  void forgetSubjectService_withoutASnapshotPurger_isRefusedSayingWhatToAdd() {
    var failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.forgetSubjectService(
                    satisfied(new InMemoryCryptoEngine()),
                    unsatisfied(),
                    List.of(),
                    unsatisfied(),
                    unsatisfied(),
                    TestProperties.defaults()));
    assertTrue(
        failure
            .getMessage()
            .startsWith("The ForgetSubjectService StreamRune configures has no snapshot purger."),
        failure.getMessage());
    assertTrue(failure.getMessage().contains("or set streamrune.gdpr.snapshot-purge=none"));
  }

  /** A {@link SnapshotPurger} bean is run by the produced forget like any other purger. */
  @Test
  void forgetSubjectService_runsASnapshotPurger() {
    var purged = new java.util.concurrent.CopyOnWriteArrayList<SubjectId>();
    SnapshotPurger snapshots =
        new SnapshotPurger() {
          @Override
          public String name() {
            return "snapshots";
          }

          @Override
          public void purge(SubjectId subjectId) {
            purged.add(subjectId);
          }
        };
    var service =
        producers.forgetSubjectService(
            satisfied(new InMemoryCryptoEngine()),
            unsatisfied(),
            List.of(snapshots),
            unsatisfied(),
            unsatisfied(),
            TestProperties.defaults());

    service.forget(SubjectId.of("user-1"), null);

    assertEquals(List.of(SubjectId.of("user-1")), purged);
  }

  /** {@code none} beside a snapshot purger bean contradicts itself: the producer refuses. */
  @Test
  void forgetSubjectService_snapshotPurgeNoneWithASnapshotPurger_isRefused() {
    SnapshotPurger snapshots =
        new SnapshotPurger() {
          @Override
          public String name() {
            return "snapshots";
          }

          @Override
          public void purge(SubjectId subjectId) {}
        };
    var failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                producers.forgetSubjectService(
                    satisfied(new InMemoryCryptoEngine()),
                    unsatisfied(),
                    List.of(snapshots),
                    unsatisfied(),
                    unsatisfied(),
                    NO_SNAPSHOTS));
    assertTrue(
        failure
            .getMessage()
            .startsWith(
                "streamrune.gdpr.snapshot-purge=none says this application has never stored a"
                    + " snapshot"),
        failure.getMessage());
  }

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
            unsatisfied(),
            NO_SNAPSHOTS);
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
                    ambiguous,
                    NO_SNAPSHOTS));
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
            resolvable,
            NO_SNAPSHOTS);
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
            crypto, unsatisfied(), List.of(), unsatisfied(), unsatisfied(), NO_SNAPSHOTS);
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
            unsatisfied(),
            NO_SNAPSHOTS);
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
            unsatisfied(),
            NO_SNAPSHOTS);

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
